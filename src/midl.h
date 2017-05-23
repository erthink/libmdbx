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

/* IDL sizes - likely should be even bigger
 * limiting factors: sizeof(pgno_t), thread stack size */
#define MDB_IDL_LOGN 16 /* DB_SIZE is 2^16, UM_SIZE is 2^17 */
#define MDB_IDL_DB_SIZE (1 << MDB_IDL_LOGN)
#define MDB_IDL_UM_SIZE (1 << (MDB_IDL_LOGN + 1))

#define MDB_IDL_DB_MAX (MDB_IDL_DB_SIZE - 1)
#define MDB_IDL_UM_MAX (MDB_IDL_UM_SIZE - 1)

#define MDB_IDL_SIZEOF(ids) (((ids)[0] + 1) * sizeof(pgno_t))
#define MDB_IDL_IS_ZERO(ids) ((ids)[0] == 0)
#define MDB_IDL_CPY(dst, src) (memcpy(dst, src, MDB_IDL_SIZEOF(src)))
#define MDB_IDL_FIRST(ids) ((ids)[1])
#define MDB_IDL_LAST(ids) ((ids)[(ids)[0]])

/* Current max length of an #mdbx_midl_alloc()ed IDL */
#define MDB_IDL_ALLOCLEN(ids) ((ids)[-1])

/* Append ID to IDL. The IDL must be big enough. */
#define mdbx_midl_xappend(idl, id)                                             \
  do {                                                                         \
    pgno_t *xidl = (idl), xlen = ++(xidl[0]);                                  \
    xidl[xlen] = (id);                                                         \
  } while (0)
