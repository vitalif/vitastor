FROM git.yourcmc.ru/vitalif/vitastor/buildenv

ADD . /root/vitastor

RUN set -e -x; \
    mkdir -p /root/fio-build/; \
    cd /root/fio-build/; \
    dpkg-source -x /root/fio*.dsc; \
    cd /root/vitastor; \
    ln -s /root/fio-build/fio-*/ ./fio; \
    ln -s /root/qemu-build/qemu-*/ ./qemu; \
    ls /usr/include/linux/raw.h || cp ./debian/raw.h /usr/include/linux/raw.h; \
    mkdir build; \
    cd build; \
    cmake .. -DWITH_ASAN=yes -DWITH_QEMU=yes; \
    make -j16
