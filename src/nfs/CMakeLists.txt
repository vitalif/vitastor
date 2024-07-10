cmake_minimum_required(VERSION 2.8.12)

project(vitastor)

# vitastor-nfs
add_executable(vitastor-nfs
	nfs_proxy.cpp
	nfs_block.cpp
	nfs_kv.cpp
	nfs_kv_create.cpp
	nfs_kv_defrag.cpp
	nfs_kv_getattr.cpp
	nfs_kv_link.cpp
	nfs_kv_lookup.cpp
	nfs_kv_read.cpp
	nfs_kv_readdir.cpp
	nfs_kv_remove.cpp
	nfs_kv_rename.cpp
	nfs_kv_setattr.cpp
	nfs_kv_write.cpp
	nfs_fsstat.cpp
	nfs_mount.cpp
	nfs_portmap.cpp
	../util/sha256.c
	proto/xdr_impl.cpp
	proto/rpc_xdr.cpp
	proto/portmap_xdr.cpp
	proto/nfs_xdr.cpp
)
target_link_libraries(vitastor-nfs
	vitastor_client
	vitastor_kv
)
