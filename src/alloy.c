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

#define MDBX_ALLOY 1 /* Amalgamated build */

/* Turn off formatters to avoid reordering the #includes */
/* *INDENT-OFF* */
/* clang-format off */

#include "elements/internals.h" /* must be included fisrt */
#include "../mdbx.h"
#include "elements/defs.h"
#include "elements/osal.h"

#if defined(__linux__) || defined(__gnu_linux__)
#include "elements/lck-linux.c"
#elif defined(_WIN32) || defined(_WIN64)
#include "elements/lck-windows.c"
#else
#include "elements/lck-posix.c"
#endif

#include "elements/osal.c"
#include "elements/core.c"
#include "elements/version.c.in"

/* *INDENT-ON* */
/* clang-format on */
