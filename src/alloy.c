/*
 * Copyright 2015-2022 Leonid Yuriev <leo@yuriev.ru>
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

#define xMDBX_ALLOY 1  /* alloyed build */
#include "internals.h" /* must be included first */

#include "core.c"
#include "osal.c"
#include "version.c"

#if defined(_WIN32) || defined(_WIN64)
#include "lck-windows.c"
#else
#include "lck-posix.c"
#endif
