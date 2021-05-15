module vitastor.io/csi

go 1.15

require (
	github.com/container-storage-interface/spec v1.4.0
	github.com/coreos/bbolt v0.0.0-00010101000000-000000000000 // indirect
	github.com/coreos/etcd v3.3.25+incompatible // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20191104093116-d3cd4ed1dbcf // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b
	github.com/gorilla/websocket v1.4.2 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.16.0 // indirect
	github.com/jonboulle/clockwork v0.2.2 // indirect
	github.com/kubernetes-csi/csi-lib-utils v0.9.1
	github.com/soheilhy/cmux v0.1.5 // indirect
	github.com/tmc/grpc-websocket-proxy v0.0.0-20201229170055-e5319fda7802 // indirect
	github.com/xiang90/probing v0.0.0-20190116061207-43a291ad63a2 // indirect
	go.etcd.io/bbolt v0.0.0-00010101000000-000000000000 // indirect
	go.etcd.io/etcd v3.3.25+incompatible
	golang.org/x/net v0.0.0-20201202161906-c7110b5ffcbb
	google.golang.org/grpc v1.33.1
	k8s.io/klog v1.0.0
	k8s.io/utils v0.0.0-20210305010621-2afb4311ab10
)

replace github.com/coreos/bbolt => go.etcd.io/bbolt v1.3.5

replace go.etcd.io/bbolt => github.com/coreos/bbolt v1.3.5

replace google.golang.org/grpc => google.golang.org/grpc v1.25.1
