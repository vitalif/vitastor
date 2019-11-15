#include <iostream>
#include "blockstore.h"

int main(int narg, char *args[])
{
    spp::sparse_hash_map<std::string, std::string> config;
    config["meta_device"] = "./test_meta.bin";
    config["journal_device"] = "./test_journal.bin";
    config["data_device"] = "./test_data.bin";
    ring_loop_t *ringloop = new ring_loop_t(512);
    blockstore *bs = new blockstore(config, ringloop);
    
    delete bs;
    delete ringloop;
    return 0;
}
