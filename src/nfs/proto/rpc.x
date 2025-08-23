/* Based on RFC 5531 - RPC: Remote Procedure Call Protocol Specification Version 2 */

const RPC_MSG_VERSION = 2;

enum rpc_auth_flavor {
	RPC_AUTH_NONE  = 0,
	RPC_AUTH_SYS   = 1,
	RPC_AUTH_SHORT = 2,
	RPC_AUTH_DH    = 3,
	RPC_RPCSEC_GSS = 6
};

enum rpc_msg_type {
	RPC_CALL  = 0,
	RPC_REPLY = 1
};

enum rpc_reply_stat {
	RPC_MSG_ACCEPTED = 0,
	RPC_MSG_DENIED   = 1
};

enum rpc_accept_stat {
	RPC_SUCCESS       = 0,
	RPC_PROG_UNAVAIL  = 1,
	RPC_PROG_MISMATCH = 2,
	RPC_PROC_UNAVAIL  = 3,
	RPC_GARBAGE_ARGS  = 4,
	RPC_SYSTEM_ERR    = 5
};

enum rpc_reject_stat {
	RPC_MISMATCH   = 0,
	RPC_AUTH_ERROR = 1
};

enum rpc_auth_stat {
	RPC_AUTH_OK = 0,
	/*
	 * failed at remote end
	 */
	RPC_AUTH_BADCRED      = 1, /* bogus credentials (seal broken) */
	RPC_AUTH_REJECTEDCRED = 2, /* client should begin new session */
	RPC_AUTH_BADVERF      = 3, /* bogus verifier (seal broken) */
	RPC_AUTH_REJECTEDVERF = 4, /* verifier expired or was replayed */
	RPC_AUTH_TOOWEAK      = 5, /* rejected due to security reasons */
	/*
	 * failed locally
	 */
	RPC_AUTH_INVALIDRESP  = 6, /* bogus response verifier */
	RPC_AUTH_FAILED       = 7  /* some unknown reason */
};

struct rpc_opaque_auth {
	rpc_auth_flavor flavor;
	opaque body<400>;
};

struct rpc_call_body {
	u_int rpcvers;
	u_int prog;
	u_int vers;
	u_int proc;
	rpc_opaque_auth cred;
	rpc_opaque_auth verf;
	/* procedure-specific parameters start here */
};

struct rpc_mismatch_info {
	unsigned int min_version;
	unsigned int max_version;
};

union rpc_accepted_reply_body switch (rpc_accept_stat stat) {
case RPC_SUCCESS:
	void;
	/* procedure-specific results start here */
case RPC_PROG_MISMATCH:
	rpc_mismatch_info mismatch_info;
default:
	void;
};

struct rpc_accepted_reply {
	rpc_opaque_auth verf;
	rpc_accepted_reply_body reply_data;
};

union rpc_rejected_reply switch (rpc_reject_stat stat) {
case RPC_MISMATCH:
	rpc_mismatch_info mismatch_info;
case RPC_AUTH_ERROR:
	rpc_auth_stat auth_stat;
};

union rpc_reply_body switch (rpc_reply_stat stat) {
case RPC_MSG_ACCEPTED:
	rpc_accepted_reply areply;
case RPC_MSG_DENIED:
	rpc_rejected_reply rreply;
};

union rpc_msg_body switch (rpc_msg_type dir) {
case RPC_CALL:
	rpc_call_body cbody;
case RPC_REPLY:
	rpc_reply_body rbody;
};

struct rpc_msg {
	u_int xid;
	rpc_msg_body body;
};

struct authsys_parms {
	unsigned int stamp;
	string machinename<255>;
	unsigned int uid;
	unsigned int gid;
	unsigned int gids<16>;
};
