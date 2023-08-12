// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 or GNU GPL-2.0+ (see README.md for details)

#pragma once

#include <functional>

struct ring_consumer_t
{
    std::function<void(void)> loop;
};

class ring_loop_t
{
public:
    void register_consumer(ring_consumer_t *consumer)
    {
    }
    void unregister_consumer(ring_consumer_t *consumer)
    {
    }
    void submit()
    {
    }
    void wait()
    {
    }
    void loop()
    {
    }
};
