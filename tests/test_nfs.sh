#!/bin/bash -ex

PG_COUNT=16
. `dirname $0`/run_3osds.sh

build/src/vitastor-cli --etcd_address $ETCD_URL create -s 10G fsmeta
build/src/vitastor-nfs --fs fsmeta --etcd_address $ETCD_URL --portmap 0 --port 2050 --foreground 1 --trace 1 >>./testdata/nfs.log 2>&1 &
NFS_PID=$!

mkdir -p testdata/nfs
sudo mount localhost:/ ./testdata/nfs -o port=2050,mountport=2050,nfsvers=3,soft,nolock,tcp
MNT=$(pwd)/testdata/nfs
trap "sudo umount -f $MNT"' || true; kill -9 $(jobs -p)' EXIT

# write small file
ls -l ./testdata/nfs
dd if=/dev/urandom of=./testdata/f1 bs=100k count=1
cp testdata/f1 ./testdata/nfs/
sudo umount ./testdata/nfs/
sudo mount localhost:/ ./testdata/nfs -o port=2050,mountport=2050,nfsvers=3,soft,nolock,tcp
ls -l ./testdata/nfs | grep f1
diff ./testdata/f1 ./testdata/nfs/f1
format_green "100K file ok"

# overwrite it inplace
dd if=/dev/urandom of=./testdata/f1_90k bs=90k count=1
cp testdata/f1_90k ./testdata/nfs/f1
sudo umount ./testdata/nfs/
format_green "inplace overwrite 90K ok"
sudo mount localhost:/ ./testdata/nfs -o port=2050,mountport=2050,nfsvers=3,soft,nolock,tcp
ls -l ./testdata/nfs | grep f1
# create another copy
dd if=./testdata/f1_90k of=./testdata/nfs/f1_nfs bs=1M
diff ./testdata/f1_90k ./testdata/nfs/f1_nfs
sudo umount ./testdata/nfs/
format_green "another copy 90K ok"
sudo mount localhost:/ ./testdata/nfs -o port=2050,mountport=2050,nfsvers=3,soft,nolock,tcp
ls -l ./testdata/nfs | grep f1
cp ./testdata/nfs/f1 ./testdata/f1_nfs
diff ./testdata/f1_90k ./testdata/nfs/f1
format_green "90K data ok"

# test partial shared overwrite
dd if=/dev/urandom of=./testdata/f1_90k bs=9317 count=1 seek=5 conv=notrunc
dd if=./testdata/f1_90k of=./testdata/nfs/f1 bs=9317 count=1 skip=5 seek=5 conv=notrunc
sudo umount ./testdata/nfs/
sudo mount localhost:/ ./testdata/nfs -o port=2050,mountport=2050,nfsvers=3,soft,nolock,tcp
diff ./testdata/f1_90k ./testdata/nfs/f1
format_green "partial inplace shared overwrite ok"

# move it to a larger shared space
dd if=/dev/urandom of=./testdata/f1_110k bs=110k count=1
cp testdata/f1_110k ./testdata/nfs/f1
sudo umount ./testdata/nfs/
sudo mount localhost:/ ./testdata/nfs -o port=2050,mountport=2050,nfsvers=3,soft,nolock,tcp
ls -l ./testdata/nfs | grep f1
diff ./testdata/f1_110k ./testdata/nfs/f1
format_green "move shared 90K -> 110K ok"

# extend it to large file + rm
dd if=/dev/urandom of=./testdata/f1_2M bs=2M count=1
cp ./testdata/f1_2M ./testdata/nfs/f1
sudo umount ./testdata/nfs/
sudo mount localhost:/ ./testdata/nfs -o port=2050,mountport=2050,nfsvers=3,soft,nolock,tcp
ls -l ./testdata/nfs | grep f1
cp ./testdata/nfs/f1 ./testdata/f1_nfs
diff ./testdata/f1_2M ./testdata/nfs/f1
rm ./testdata/nfs/f1
format_green "extend to 2M + rm ok"

# mkdir
mkdir -p ./testdata/nfs/dir1/dir2
echo abcdef > ./testdata/nfs/dir1/dir2/hnpfls
# rename dir
mv ./testdata/nfs/dir1 ./testdata/nfs/dir3
sudo umount ./testdata/nfs/
sudo mount localhost:/ ./testdata/nfs -o port=2050,mountport=2050,nfsvers=3,soft,nolock,tcp
ls -l ./testdata/nfs | grep dir3
ls -l ./testdata/nfs/dir3 | grep dir2
ls -l ./testdata/nfs/dir3/dir2 | grep hnpfls
echo abcdef > ./testdata/hnpfls
diff ./testdata/hnpfls ./testdata/nfs/dir3/dir2/hnpfls
format_green "rename dir with file ok"

# touch
touch -t 202401011404 ./testdata/nfs/dir3/dir2/hnpfls
sudo chown 65534:65534 ./testdata/nfs/dir3/dir2/hnpfls
sudo chmod 755 ./testdata/nfs/dir3/dir2/hnpfls
sudo umount ./testdata/nfs/
sudo mount localhost:/ ./testdata/nfs -o port=2050,mountport=2050,nfsvers=3,soft,nolock,tcp
T=`stat -c '%a %u %g %y' ./testdata/nfs/dir3/dir2/hnpfls | perl -pe 's/(:\d+)(.*)/$1/'`
[[ "$T" = "755 65534 65534 2024-01-01 14:04" ]]
format_green "set attrs ok"

# move dir
mv ./testdata/nfs/dir3/dir2 ./testdata/nfs/
sudo umount ./testdata/nfs/
sudo mount localhost:/ ./testdata/nfs -o port=2050,mountport=2050,nfsvers=3,soft,nolock,tcp
ls -l ./testdata/nfs | grep dir3
ls -l ./testdata/nfs | grep dir2
format_green "move dir ok"

# symlink, readlink
ln -s dir2 ./testdata/nfs/sym2
[[ "`stat -c '%A' ./testdata/nfs/sym2`" = "lrwxrwxrwx" ]]
sudo umount ./testdata/nfs/
sudo mount localhost:/ ./testdata/nfs -o port=2050,mountport=2050,nfsvers=3,soft,nolock,tcp
[[ "`stat -c '%A' ./testdata/nfs/sym2`" = "lrwxrwxrwx" ]]
[[ "`readlink ./testdata/nfs/sym2`" = "dir2" ]]
format_green "symlink, readlink ok"

# mknod: chr, blk, sock, fifo + remove
sudo mknod ./testdata/nfs/nod_chr c 1 5
sudo mknod ./testdata/nfs/nod_blk b 2 6
mkfifo ./testdata/nfs/nod_fifo
perl -e 'use Socket; socket($sock, PF_UNIX, SOCK_STREAM, undef) || die $!; bind($sock, sockaddr_un("./testdata/nfs/nod_sock")) || die $!;'
chmod 777 ./testdata/nfs/nod_*
sudo umount ./testdata/nfs/
sudo mount localhost:/ ./testdata/nfs -o port=2050,mountport=2050,nfsvers=3,soft,nolock,tcp
[[ "`ls testdata|wc -l`" -ge 4 ]]
[[ "`stat -c '%A' ./testdata/nfs/nod_blk`" = "brwxrwxrwx" ]]
[[ "`stat -c '%A' ./testdata/nfs/nod_chr`" = "crwxrwxrwx" ]]
[[ "`stat -c '%A' ./testdata/nfs/nod_fifo`" = "prwxrwxrwx" ]]
[[ "`stat -c '%A' ./testdata/nfs/nod_sock`" = "srwxrwxrwx" ]]
sudo rm ./testdata/nfs/nod_*
format_green "mknod + rm ok"

# hardlink
echo ABCDEF > ./testdata/nfs/linked1
i=`stat -c '%i' ./testdata/nfs/linked1`
ln ./testdata/nfs/linked1 ./testdata/nfs/linked2
[[ "`stat -c '%i' ./testdata/nfs/linked2`" -eq $i ]]
echo BABABA > ./testdata/nfs/linked2
diff ./testdata/nfs/linked2 ./testdata/nfs/linked1
sudo umount ./testdata/nfs/
sudo mount localhost:/ ./testdata/nfs -o port=2050,mountport=2050,nfsvers=3,soft,nolock,tcp
diff ./testdata/nfs/linked2 ./testdata/nfs/linked1
[[ "`cat ./testdata/nfs/linked2`" = "BABABA" ]]
rm ./testdata/nfs/linked2
sudo umount ./testdata/nfs/
sudo mount localhost:/ ./testdata/nfs -o port=2050,mountport=2050,nfsvers=3,soft,nolock,tcp
[[ "`cat ./testdata/nfs/linked1`" = "BABABA" ]]
format_green "hardlink ok"

# rm small
ls -l ./testdata/nfs
dd if=/dev/urandom of=./testdata/nfs/smallfile bs=100k count=1
sudo umount ./testdata/nfs/
sudo mount localhost:/ ./testdata/nfs -o port=2050,mountport=2050,nfsvers=3,soft,nolock,tcp
rm ./testdata/nfs/smallfile
if ls ./testdata/nfs | grep smallfile; then false; fi
sudo umount ./testdata/nfs/
sudo mount localhost:/ ./testdata/nfs -o port=2050,mountport=2050,nfsvers=3,soft,nolock,tcp
if ls ./testdata/nfs | grep smallfile; then false; fi
format_green "rm small ok"

# rename over existing
echo ZXCVBN > ./testdata/nfs/over1
mv ./testdata/nfs/over1 ./testdata/nfs/linked2
sudo umount ./testdata/nfs/
sudo mount localhost:/ ./testdata/nfs -o port=2050,mountport=2050,nfsvers=3,soft,nolock,tcp
if ls ./testdata/nfs | grep over1; then false; fi
[[ "`cat ./testdata/nfs/linked2`" = "ZXCVBN" ]]
[[ "`cat ./testdata/nfs/linked1`" = "BABABA" ]]
format_green "rename over existing file ok"

format_green OK
