// Kill atomics in fio headers
#define _STDATOMIC_H
#include "fio/arch/arch.h"

#undef atomic_load_acquire
#undef atomic_store_release
#define atomic_load_acquire(p) *(p)
#define atomic_store_release(p, v) (*(p)) = (v)

#define CONFIG_HAVE_GETTID
#define CONFIG_SYNC_FILE_RANGE
#define CONFIG_PWRITEV2
extern "C" {
#include "fio/fio.h"
#include "fio/optgroup.h"
}
