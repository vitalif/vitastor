// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#ifndef NODE_VITASTOR_ADDON_H
#define NODE_VITASTOR_ADDON_H

#include <nan.h>
#include <vitastor_c.h>

#include "client.h"

#define ERRORF(format, ...) fprintf(stderr, format "\n", __VA_ARGS__);

//#define TRACEF(format, ...) fprintf(stderr, format "\n", __VA_ARGS__);
//#define TRACE(msg) fprintf(stderr, "%s\n", msg);

#define TRACEF(format, ...) ;
#define TRACE(msg) ;

#endif
