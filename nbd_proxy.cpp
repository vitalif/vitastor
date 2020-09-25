// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.0 or GNU GPL-2.0+ (see README.md for details)
// Similar to qemu-nbd, but sets timeout and uses io_uring

#include <linux/nbd.h>
#include <sys/ioctl.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <sys/un.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>

#include "epoll_manager.h"
#include "cluster_client.h"

class nbd_proxy
{
protected:
    uint64_t inode = 0;

    ring_loop_t *ringloop = NULL;
    epoll_manager_t *epmgr = NULL;
    cluster_client_t *cli = NULL;
    ring_consumer_t consumer;

    std::vector<iovec> send_list, next_send_list;
    std::vector<void*> to_free;
    int nbd_fd = -1;
    void *recv_buf = NULL;
    int receive_buffer_size = 9000;
    nbd_request cur_req;
    cluster_op_t *cur_op = NULL;
    void *cur_buf = NULL;
    int cur_left = 0;
    int read_state = 0;
    int read_ready = 0;
    msghdr read_msg = { 0 }, send_msg = { 0 };
    iovec read_iov = { 0 };

public:
    int start(json11::Json cfg)
    {
        // Parse options
        if (cfg["etcd_address"].string_value() == "")
        {
            fprintf(stderr, "etcd_address is missing\n");
            exit(1);
        }
        if (!cfg["size"].uint64_value())
        {
            fprintf(stderr, "device size is missing\n");
            exit(1);
        }
        inode = cfg["inode"].uint64_value();
        uint64_t pool = cfg["pool"].uint64_value();
        if (pool)
        {
            inode = (inode & ((1l << (64-POOL_ID_BITS)) - 1)) | (pool << (64-POOL_ID_BITS));
        }
        if (!(inode >> (64-POOL_ID_BITS)))
        {
            fprintf(stderr, "pool is missing\n");
            exit(1);
        }
        // Initialize NBD
        int sockfd[2];
        if (socketpair(AF_UNIX, SOCK_STREAM, 0, sockfd) < 0)
        {
            perror("socketpair");
            exit(1);
        }
        fcntl(sockfd[0], F_SETFL, fcntl(sockfd[0], F_GETFL, 0) | O_NONBLOCK);
        nbd_fd = sockfd[0];
        if (run_nbd(sockfd, cfg["nbd_device"].string_value().c_str(), cfg["size"].uint64_value(), NBD_FLAG_SEND_FLUSH, 30) < 0)
        {
            perror("run_nbd");
            exit(1);
        }
        // Create client
        ringloop = new ring_loop_t(512);
        epmgr = new epoll_manager_t(ringloop);
        cli = new cluster_client_t(ringloop, epmgr->tfd, cfg);
        // Initialize read state
        read_state = CL_READ_HDR;
        recv_buf = malloc_or_die(receive_buffer_size);
        cur_buf = &cur_req;
        cur_left = sizeof(nbd_request);
        consumer.loop = [this]()
        {
            submit_read();
            submit_send();
            ringloop->submit();
        };
        ringloop->register_consumer(&consumer);
        // Add FD to epoll
        epmgr->tfd->set_fd_handler(sockfd[0], false, [this](int peer_fd, int epoll_events)
        {
            read_ready++;
            submit_read();
        });
        while (1)
        {
            ringloop->loop();
            ringloop->wait();
        }
    }

protected:
    int run_nbd(int sockfd[2], const char *dev, uint64_t size, uint64_t flags, unsigned timeout)
    {
        // Check handle size
        assert(sizeof(cur_req.handle) == 8);
        int r, nbd = open(dev, O_RDWR);
        if (nbd < 0)
        {
            return -1;
        }
        r = ioctl(nbd, NBD_SET_SOCK, sockfd[1]);
        if (r < 0)
        {
            goto end_close;
        }
        r = ioctl(nbd, NBD_SET_BLKSIZE, 4096);
        if (r < 0)
        {
            goto end_close;
        }
        r = ioctl(nbd, NBD_SET_SIZE, size);
        if (r < 0)
        {
            goto end_close;
        }
        ioctl(nbd, NBD_SET_FLAGS, flags);
        if (timeout >= 0)
        {
            r = ioctl(nbd, NBD_SET_TIMEOUT, (unsigned long)timeout);
            if (r < 0)
            {
                goto end_close;
            }
        }
        if (!fork())
        {
            // Run in child
            close(sockfd[0]);
            r = ioctl(nbd, NBD_DO_IT);
            if (r < 0)
            {
                fprintf(stderr, "NBD device terminated with error: %s\n", strerror(errno));
                kill(getppid(), SIGTERM);
            }
            close(sockfd[1]);
            ioctl(nbd, NBD_CLEAR_QUE);
            ioctl(nbd, NBD_CLEAR_SOCK);
            exit(0);
        }
        close(sockfd[1]);
        close(nbd);
        return 0;
    end_close:
        int err = errno;
        ioctl(nbd, NBD_CLEAR_SOCK);
        close(nbd);
        errno = err;
        return -1;
    }

    void submit_send()
    {
        if (!send_list.size() || send_msg.msg_iovlen > 0)
        {
            return;
        }
        io_uring_sqe* sqe = ringloop->get_sqe();
        if (!sqe)
        {
            return;
        }
        ring_data_t* data = ((ring_data_t*)sqe->user_data);
        data->callback = [this](ring_data_t *data) { handle_send(data->res); };
        send_msg.msg_iov = send_list.data();
        send_msg.msg_iovlen = send_list.size();
        my_uring_prep_sendmsg(sqe, nbd_fd, &send_msg, 0);
    }

    void handle_send(int result)
    {
        send_msg.msg_iovlen = 0;
        if (result < 0 && result != -EAGAIN)
        {
            fprintf(stderr, "Socket disconnected: %s\n", strerror(-result));
            exit(1);
        }
        int to_eat = 0;
        while (result > 0 && to_eat < send_list.size())
        {
            if (result >= send_list[to_eat].iov_len)
            {
                free(to_free[to_eat]);
                result -= send_list[to_eat].iov_len;
                to_eat++;
            }
            else
            {
                send_list[to_eat].iov_base += result;
                send_list[to_eat].iov_len -= result;
                break;
            }
        }
        if (to_eat > 0)
        {
            send_list.erase(send_list.begin(), send_list.begin() + to_eat);
            to_free.erase(to_free.begin(), to_free.begin() + to_eat);
        }
        for (int i = 0; i < next_send_list.size(); i++)
        {
            send_list.push_back(next_send_list[i]);
        }
        next_send_list.clear();
        if (send_list.size() > 0)
        {
            ringloop->wakeup();
        }
    }

    void submit_read()
    {
        if (!read_ready || read_msg.msg_iovlen > 0)
        {
            return;
        }
        io_uring_sqe* sqe = ringloop->get_sqe();
        if (!sqe)
        {
            return;
        }
        ring_data_t* data = ((ring_data_t*)sqe->user_data);
        data->callback = [this](ring_data_t *data) { handle_read(data->res); };
        if (cur_left < receive_buffer_size)
        {
            read_iov.iov_base = recv_buf;
            read_iov.iov_len = receive_buffer_size;
        }
        else
        {
            read_iov.iov_base = cur_buf;
            read_iov.iov_len = cur_left;
        }
        read_msg.msg_iov = &read_iov;
        read_msg.msg_iovlen = 1;
        my_uring_prep_recvmsg(sqe, nbd_fd, &read_msg, 0);
    }

    void handle_read(int result)
    {
        read_msg.msg_iovlen = 0;
        if (result < 0 && result != -EAGAIN)
        {
            fprintf(stderr, "Socket disconnected: %s\n", strerror(-result));
            exit(1);
        }
        if (result == -EAGAIN || result < read_iov.iov_len)
        {
            read_ready--;
        }
        if (read_ready > 0)
        {
            ringloop->wakeup();
        }
        void *b = recv_buf;
        while (result > 0)
        {
            if (read_iov.iov_base == recv_buf)
            {
                int inc = result >= cur_left ? cur_left : result;
                memcpy(cur_buf, b, inc);
                cur_left -= inc;
                result -= inc;
                cur_buf += inc;
                b += inc;
            }
            else
            {
                assert(result <= cur_left);
                cur_left -= result;
                result = 0;
            }
            if (cur_left <= 0)
            {
                handle_finished_read();
            }
        }
    }

    void handle_finished_read()
    {
        if (read_state == CL_READ_HDR)
        {
            int req_type = be32toh(cur_req.type);
            if (be32toh(cur_req.magic) != NBD_REQUEST_MAGIC ||
                req_type != NBD_CMD_READ && req_type != NBD_CMD_WRITE && req_type != NBD_CMD_FLUSH)
            {
                printf("Unexpected request: magic=%x type=%x, terminating\n", cur_req.magic, req_type);
                exit(1);
            }
            uint64_t handle = *((uint64_t*)cur_req.handle);
#ifdef DEBUG
            printf("request %lx +%x %lx\n", be64toh(cur_req.from), be32toh(cur_req.len), handle);
#endif
            void *buf = NULL;
            cluster_op_t *op = new cluster_op_t;
            if (req_type == NBD_CMD_READ || req_type == NBD_CMD_WRITE)
            {
                op->opcode = req_type == NBD_CMD_READ ? OSD_OP_READ : OSD_OP_WRITE;
                op->inode = inode;
                op->offset = be64toh(cur_req.from);
                op->len = be32toh(cur_req.len);
                buf = malloc_or_die(sizeof(nbd_reply) + op->len);
                op->iov.push_back(buf + sizeof(nbd_reply), op->len);
            }
            else if (req_type == NBD_CMD_FLUSH)
            {
                op->opcode = OSD_OP_SYNC;
                buf = malloc_or_die(sizeof(nbd_reply));
            }
            op->callback = [this, buf, handle](cluster_op_t *op)
            {
#ifdef DEBUG
                printf("reply %lx e=%d\n", handle, op->retval);
#endif
                nbd_reply *reply = (nbd_reply*)buf;
                reply->magic = htobe32(NBD_REPLY_MAGIC);
                memcpy(reply->handle, &handle, 8);
                reply->error = htobe32(op->retval < 0 ? -op->retval : 0);
                auto & to_list = send_msg.msg_iovlen > 0 ? next_send_list : send_list;
                if (op->retval < 0 || op->opcode != OSD_OP_READ)
                    to_list.push_back({ .iov_base = buf, .iov_len = sizeof(nbd_reply) });
                else
                    to_list.push_back({ .iov_base = buf, .iov_len = sizeof(nbd_reply) + op->len });
                to_free.push_back(buf);
                delete op;
                ringloop->wakeup();
            };
            if (req_type == NBD_CMD_WRITE)
            {
                cur_op = op;
                cur_buf = buf + sizeof(nbd_reply);
                cur_left = op->len;
                read_state = CL_READ_DATA;
            }
            else
            {
                cur_op = NULL;
                cur_buf = &cur_req;
                cur_left = sizeof(nbd_request);
                read_state = CL_READ_HDR;
                cli->execute(op);
            }
        }
        else
        {
            cli->execute(cur_op);
            cur_op = NULL;
            cur_buf = &cur_req;
            cur_left = sizeof(nbd_request);
            read_state = CL_READ_HDR;
        }
    }
};

int main(int narg, char *args[])
{
    setvbuf(stdout, NULL, _IONBF, 0);
    setvbuf(stderr, NULL, _IONBF, 0);
    json11::Json::object cfg;
    for (int i = 1; i < narg; i++)
    {
        if (args[i][0] == '-' && args[i][1] == '-' && i < narg-1)
        {
            char *opt = args[i]+2;
            cfg[opt] = args[++i];
        }
    }
    nbd_proxy *p = new nbd_proxy();
    p->start(cfg);
    return 0;
}
