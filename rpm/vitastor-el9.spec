Name:           vitastor
Version:        2.4.1
Release:        1%{?dist}
Summary:        Vitastor, a fast software-defined clustered block storage

License:        Vitastor Network Public License 1.1
URL:            https://vitastor.io/
Source0:        vitastor-2.4.1.el9.tar.gz

BuildRequires:  gperftools-devel
BuildRequires:  gcc-c++
BuildRequires:  nodejs >= 10
BuildRequires:  jerasure-devel
BuildRequires:  libisa-l-devel
BuildRequires:  gf-complete-devel
BuildRequires:  rdma-core-devel
BuildRequires:  cmake
BuildRequires:  libnl3-devel
Requires:       vitastor-osd = %{version}-%{release}
Requires:       vitastor-mon = %{version}-%{release}
Requires:       vitastor-client = %{version}-%{release}
Requires:       vitastor-client-devel = %{version}-%{release}
Requires:       vitastor-fio = %{version}-%{release}

%description
Vitastor is a small, simple and fast clustered block storage (storage for VM drives),
architecturally similar to Ceph which means strong consistency, primary-replication,
symmetric clustering and automatic data distribution over any number of drives of any
size with configurable redundancy (replication or erasure codes/XOR).


%package -n vitastor-osd
Summary:        Vitastor - OSD
Requires:       vitastor-client = %{version}-%{release}
Requires:       util-linux
Requires:       parted


%description -n vitastor-osd
Vitastor object storage daemon, i.e. server program that stores data.


%package -n vitastor-mon
Summary:        Vitastor - monitor
Requires:       nodejs >= 10
Requires:       lpsolve


%description -n vitastor-mon
Vitastor monitor, i.e. server program responsible for watching cluster state and
scheduling cluster-level operations.


%package -n vitastor-client
Summary:        Vitastor - client


%description -n vitastor-client
Vitastor client library and command-line interface.


%package -n vitastor-client-devel
Summary:        Vitastor - development files
Group:          Development/Libraries
Requires:       vitastor-client = %{version}-%{release}


%description -n vitastor-client-devel
Vitastor library headers for development.


%package -n vitastor-fio
Summary:        Vitastor - fio drivers
Group:          Development/Libraries
Requires:       vitastor-client = %{version}-%{release}
Requires:       fio = 3.35-1.el9


%description -n vitastor-fio
Vitastor fio drivers for benchmarking.


%package -n vitastor-opennebula
Summary:        Vitastor for OpenNebula
Group:          Development/Libraries
Requires:       vitastor-client
Requires:       jq
Requires:       python3-lxml
Requires:       patch
Requires:       qemu-kvm-block-vitastor


%description -n vitastor-opennebula
Vitastor storage plugin for OpenNebula.


%prep
%setup -q


%build
%cmake
%cmake_build


%install
rm -rf $RPM_BUILD_ROOT
%cmake_install
cd mon
npm install --production
cd ..
mkdir -p %buildroot/usr/lib/vitastor
cp -r mon %buildroot/usr/lib/vitastor
mv %buildroot/usr/lib/vitastor/mon/scripts/make-etcd %buildroot/usr/lib/vitastor/mon/
mkdir -p %buildroot/lib/systemd/system
cp mon/scripts/vitastor.target mon/scripts/vitastor-mon.service mon/scripts/vitastor-osd@.service %buildroot/lib/systemd/system
mkdir -p %buildroot/lib/udev/rules.d
cp mon/scripts/90-vitastor.rules %buildroot/lib/udev/rules.d
mkdir -p %buildroot/var/lib/one
cp -r opennebula/remotes %buildroot/var/lib/one
cp opennebula/install.sh %buildroot/var/lib/one/remotes/datastore/vitastor/
mkdir -p %buildroot/etc/
cp -r opennebula/sudoers.d %buildroot/etc/


%files
%doc GPL-2.0.txt VNPL-1.1.txt README.md README-ru.md


%files -n vitastor-osd
%_bindir/vitastor-osd
%_bindir/vitastor-disk
%_bindir/vitastor-dump-journal
/lib/systemd/system/vitastor-osd@.service
/lib/systemd/system/vitastor.target
/lib/udev/rules.d/90-vitastor.rules


%pre -n vitastor-osd
groupadd -r -f vitastor 2>/dev/null ||:
useradd -r -g vitastor -s /sbin/nologin -c "Vitastor daemons" -M -d /nonexistent vitastor 2>/dev/null ||:
install -o vitastor -g vitastor -d /var/log/vitastor
mkdir -p /etc/vitastor


%files -n vitastor-mon
/usr/lib/vitastor/mon
/lib/systemd/system/vitastor-mon.service


%pre -n vitastor-mon
groupadd -r -f vitastor 2>/dev/null ||:
useradd -r -g vitastor -s /sbin/nologin -c "Vitastor daemons" -M -d /nonexistent vitastor 2>/dev/null ||:
mkdir -p /etc/vitastor
mkdir -p /var/lib/vitastor
chown vitastor:vitastor /var/lib/vitastor


%files -n vitastor-client
%_bindir/vitastor-nbd
%_bindir/vitastor-ublk
%_bindir/vitastor-nfs
%_bindir/vitastor-cli
%_bindir/vitastor-rm
%_bindir/vitastor-kv
%_bindir/vitastor-kv-stress
%_bindir/vita
%_libdir/libvitastor_blk.so*
%_libdir/libvitastor_client.so*
%_libdir/libvitastor_kv.so*


%files -n vitastor-client-devel
%_includedir/vitastor_c.h
%_includedir/vitastor_kv.h
%_libdir/pkgconfig


%files -n vitastor-fio
%_libdir/libfio_vitastor.so
%_libdir/libfio_vitastor_blk.so
%_libdir/libfio_vitastor_sec.so


%files -n vitastor-opennebula
/var/lib/one
/etc/sudoers.d/opennebula-vitastor


%triggerin -n vitastor-opennebula -- opennebula
[ $2 = 0 ] || exit 0
/var/lib/one/remotes/datastore/vitastor/install.sh


# Turn off the brp-python-bytecompile script
%global __os_install_post %(echo '%{__os_install_post}' | sed -e 's!/usr/lib[^[:space:]]*/brp-python-bytecompile[[:space:]].*$!!g')


%changelog
