void slice()
{
    // Slice the request into blockstore requests to individual objects
    // Primary OSD still operates individual stripes, except they're twice the size of the blockstore's stripe.
    std::vector read_parts;
    int block = bs->get_block_size();
    uint64_t stripe1 = cur_op->op.rw.offset / block / 2;
    uint64_t stripe2 = (cur_op->op.rw.offset + cur_op->op.rw.len + block*2 - 1) / block / 2 - 1;
    for (uint64_t s = stripe1; s <= stripe2; s++)
    {
        uint64_t start = s == stripe1 ? cur_op->op.rw.offset - stripe1*block*2 : 0;
        uint64_t end = s == stripe2 ? cur_op->op.rw.offset + cur_op->op.rw.len - stripe2*block*2 : block*2;
        if (start < block)
        {
            read_parts.push_back({
                .role = 1,
                .oid = {
                    .inode = cur_op->op.rw.inode,
                    .stripe = (s << STRIPE_ROLE_BITS) | 1,
                },
                .version = UINT64_MAX,
                .offset = start,
                .len = (block < end ? block : end) - start,
            });
        }
        if (end > block)
        {
            read_parts.push_back({
                .role = 2,
                .oid = {
                    .inode = cur_op->op.rw.inode,
                    .stripe = (s << STRIPE_ROLE_BITS) | 2,
                },
                .version = UINT64_MAX,
                .offset = (start > block ? start-block : 0),
                .len = end - (start > block ? start-block : 0),
            });
        }
    }
}
