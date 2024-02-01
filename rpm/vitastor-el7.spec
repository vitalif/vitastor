Name:           vitastor
Version:        1.4.1
Release:        1%{?dist}
Summary:        Vitastor, a fast software-defined clustered block storage

License:        Vitastor Network Public License 1.1
URL:            https://vitastor.io/
Source0:        vitastor-1.4.1.el7.tar.gz

BuildRequires:  liburing-devel >= 0.6
BuildRequires:  gperftools-devel
BuildRequires:  devtoolset-9-gcc-c++
BuildRequires:  rh-nodejs12
BuildRequires:  rh-nodejs12-npm
BuildRequires:  jerasure-devel
BuildRequires:  libisa-l-devel
BuildRequires:  gf-complete-devel
BuildRequires:  libibverbs-devel
BuildRequires:  cmake3
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
Requires:       libJerasure2
Requires:       libisa-l
Requires:       liburing >= 0.6
Requires:       liburing < 2
Requires:       vitastor-client = %{version}-%{release}
Requires:       util-linux
Requires:       parted


%description -n vitastor-osd
Vitastor object storage daemon, i.e. server program that stores data.


%package -n vitastor-mon
Summary:        Vitastor - monitor
Requires:       rh-nodejs12
Requires:       rh-nodejs12-npm
Requires:       lpsolve


%description -n vitastor-mon
Vitastor monitor, i.e. server program responsible for watching cluster state and
scheduling cluster-level operations.


%package -n vitastor-client
Summary:        Vitastor - client
Requires:       liburing >= 0.6
Requires:       liburing < 2


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
Requires:       fio = 3.7-1.el7


%description -n vitastor-fio
Vitastor fio drivers for benchmarking.


%prep
%setup -q


%build
. /opt/rh/devtoolset-9/enable
%cmake3 .
%make_build


%install
rm -rf $RPM_BUILD_ROOT
%make_install
. /opt/rh/rh-nodejs12/enable
cd mon
npm install
cd ..
mkdir -p %buildroot/usr/lib/vitastor
cp -r mon %buildroot/usr/lib/vitastor
mkdir -p %buildroot/lib/systemd/system
cp mon/vitastor.target mon/vitastor-mon.service mon/vitastor-osd@.service %buildroot/lib/systemd/system
mkdir -p %buildroot/lib/udev/rules.d
cp mon/90-vitastor.rules %buildroot/lib/udev/rules.d


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


%files -n vitastor-client
%_bindir/vitastor-nbd
%_bindir/vitastor-nfs
%_bindir/vitastor-cli
%_bindir/vitastor-rm
%_bindir/vita
%_libdir/libvitastor_blk.so*
%_libdir/libvitastor_client.so*


%files -n vitastor-client-devel
%_includedir/vitastor_c.h
%_libdir/pkgconfig


%files -n vitastor-fio
%_libdir/libfio_vitastor.so
%_libdir/libfio_vitastor_blk.so
%_libdir/libfio_vitastor_sec.so


%changelog
