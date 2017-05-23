/*
 * Copyright 2015-2017 Leonid Yuriev <leo@yuriev.ru>
 * and other libmdbx authors: please see AUTHORS file.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in the file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 */

#include "./bits.h"

#if MDBX_VERSION_MAJOR != 0 || MDBX_VERSION_MINOR != 0
#error "API version mismatch!"
#endif

#define MDBX_VERSION_RELEASE 0
#define MDBX_VERSION_REVISION 0

const struct mdbx_version_info mdbx_version = {
    MDBX_VERSION_MAJOR,
    MDBX_VERSION_MINOR,
    MDBX_VERSION_RELEASE,
    MDBX_VERSION_REVISION,
    {"@MDBX_GIT_TIMESTAMP@", "@MDBX_GIT_TREE@", "@MDBX_GIT_COMMIT@",
     "@MDBX_GIT_DESCRIBE@"}};

const struct mdbx_build_info mdbx_build = {
    "@MDBX_BUILD_TIMESTAMP@", "@MDBX_BUILD_TAGRET@", "@MDBX_BUILD_OPTIONS@",
    "@MDBX_BUILD_COMPILER@", "@MDBX_BUILD_FLAGS@"};
