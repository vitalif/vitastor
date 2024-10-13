module vitastor.io/csi

go 1.15

require (
	github.com/container-storage-interface/spec v1.8.0
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b
	github.com/kubernetes-csi/csi-lib-utils v0.9.1
	golang.org/x/net v0.7.0
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	google.golang.org/grpc v1.33.1
	google.golang.org/protobuf v1.24.0
	k8s.io/klog v1.0.0
	k8s.io/utils v0.0.0-20210305010621-2afb4311ab10
)

replace github.com/coreos/bbolt => go.etcd.io/bbolt v1.3.5

replace go.etcd.io/bbolt => github.com/coreos/bbolt v1.3.5

replace google.golang.org/grpc => google.golang.org/grpc v1.25.1
