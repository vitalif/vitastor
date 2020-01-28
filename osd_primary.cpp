#include "osd.h"

void osd_t::exec_primary(osd_op_t *cur_op)
{
    // read: read directly or read paired stripe(s), reconstruct, return
    // write: read paired stripe(s), modify, write
    // nuance: take care to read the same version from paired stripes!
    // if there are no write requests in progress we're good (stripes must be in sync)
    // and... remember the last readable version during a write request
    // and... postpone other write requests to the same stripe until the completion of previous ones
    //
    // sync: sync peers, get unstable versions from somewhere, stabilize them
    
}

void osd_t::make_primary_reply(osd_op_t *op)
{
    op->reply.hdr.magic = SECONDARY_OSD_REPLY_MAGIC;
    op->reply.hdr.id = op->op.hdr.id;
    op->reply.hdr.opcode = op->op.hdr.opcode;
}
