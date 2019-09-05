/*
 * Copyright 2015-2019 Leonid Yuriev <leo@yuriev.ru>
 * and other libmdbx authors: please see AUTHORS file.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in the file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>. */

/* Amalgamated build */
#define MDBX_ALLOY 1
#include "elements/internals.h" /* must be included fisrt */

#include "elements/core.c"
#include "elements/osal.c"
#include "elements/version.c"

#if defined(_WIN32) || defined(_WIN64)
#include "elements/lck-windows.c"
#else
#include "elements/lck-posix.c"
#endif
