#!/bin/bash

set -e

# 1) remove all extern non-xdr functions (service, client)
# 2) use xdr_string_t for strings instead of char*
# 3) remove K&R #ifdefs
# 4) remove register int32_t* buf
# 5) remove union names
# 6) use xdr_string_t for opaques instead of u_int + char*
# 7) TODO: generate normal procedure stubs
run_rpcgen() {
    rpcgen -h $1.x | \
        perl -e '
            { local $/ = undef; $_ = <>; }
            s/^extern(?!.*"C"|.*bool_t xdr.*XDR).*\n//gm;
            s/#include <rpc\/rpc.h>/#include "xdr_impl.h"/;
            s/^typedef char \*/typedef xdr_string_t /gm;
            s/^(\s*)char \*(?!.*_val)/$1xdr_string_t /gm;
            # remove union names
            s/ \w+_u;/;/gs;
            # use xdr_string_t for opaques
            s/struct\s*\{\s*u_int\s+\w+_len;\s*char\s+\*\w+_val;\s*\}\s*/xdr_string_t /gs;
            # remove stdc/k&r
            s/^#if.*__STDC__.*//gm;
            s/\n#else[^\n]*K&R.*?\n#endif[^\n]*K&R[^\n]*//gs;
            print;' > $1.h
    rpcgen -c $1.x | \
        perl -pe '
            s/register int32_t \*buf;\s*//g;
            s/\bbuf\s*=[^;]+;\s*//g;
            s/\bbuf\s*==\s*NULL/1/g;
            # remove union names
            s/(\.|->)\w+_u\./$1/g;
            # use xdr_string_t for opaques
            # xdr_bytes(xdrs, (char**)&objp->data.data_val, (char**)&objp->data.data_len, 400)
            #   -> xdr_bytes(xdrs, &objp->data, 400)
            # xdr_bytes(xdrs, (char**)&objp->data_val, (char**)&objp->data_len, 400)
            #   -> xdr_bytes(xdrs, objp, 400)
            s/xdr_bytes\s*\(\s*xdrs,\s*\(\s*char\s*\*\*\s*\)\s*([^()]+?)\.\w+_val\s*,\s*\(\s*u_int\s*\*\s*\)\s*\1\.\w+_len,/xdr_bytes(xdrs, $1,/gs;
            s/xdr_bytes\s*\(\s*xdrs,\s*\(\s*char\s*\*\*\s*\)\s*&\s*([^()]+?)->\w+_val\s*,\s*\(\s*u_int\s*\*\s*\)\s*&\s*\1->\w+_len,/xdr_bytes(xdrs, $1,/gs;
            # add include
            if (/#include/) { $_ .= "#include \"xdr_impl_inline.h\"\n"; }' > ${1}_xdr.cpp
}

run_rpcgen nfs
run_rpcgen rpc
run_rpcgen portmap
