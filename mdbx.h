/*
 * Copyright (c) 2015,2016 Leonid Yuriev <leo@yuriev.ru>.
 * Copyright (c) 2015,2016 Peter-Service R&D LLC.
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

/*
	This is solution to provide flexible compatibility with the original liblmdb.
	Yeah, this way is partially ugly and madness...

	But, on the other hand, only this way allows provide both API with
	minimal	changes the source code of an applications, and the source
	code of the library itself. Anyway, ideas are welcome!

	So,

	When needed drop-in replacement for liblmdb you should:
		- 'make lmdb' to build liblmdb.so and liblmdb.a;
		- #include <lmdb.h> and use mdb_* functions;
		- linking with liblmdb.so or liblmdb.a;

		= This provides nearly full compatibility with
		  original LMDB from Symas Corp.
		But you should be noted - such compatibility
		is not a goal for MDBX.

	When exactly the libmdbx is needed, you should:
		- 'make mdbx' to build libmdbx.so and libmdbx.a;
		- #include <mdbx.h> and use mdbx_* functions;
		- linking with libmdbx.so or libmdbx.a;

		= This allows using (linking) both MDBX and LMDB
		  simultaneously in the one application, for instance
		  to benchmarking and/or comparison.
*/

#ifndef _MDBX_H_
#define _MDBX_H_
#define MDBX_MODE_ENABLED 1

#ifndef _GNU_SOURCE
#	define _GNU_SOURCE
#endif

#define mdb_version	mdbx_version
#define mdb_strerror	mdbx_strerror
#define mdb_env_create	mdbx_env_create
#define mdb_env_open	mdbx_env_open
#define mdb_env_open_ex	mdbx_env_open_ex
#define mdb_env_copy	mdbx_env_copy
#define mdb_env_copyfd	mdbx_env_copyfd
#define mdb_env_copy2	mdbx_env_copy2
#define mdb_env_copyfd2	mdbx_env_copyfd2
#define mdb_env_sync	mdbx_env_sync
#define mdb_env_close	mdbx_env_close
#define mdb_env_close_ex	mdbx_env_close_ex
#define mdb_env_set_flags	mdbx_env_set_flags
#define mdb_env_get_flags	mdbx_env_get_flags
#define mdb_env_get_path	mdbx_env_get_path
#define mdb_env_get_fd	mdbx_env_get_fd
#define mdb_env_set_mapsize	mdbx_env_set_mapsize
#define mdb_env_set_maxreaders	mdbx_env_set_maxreaders
#define mdb_env_get_maxreaders	mdbx_env_get_maxreaders
#define mdb_env_set_maxdbs	mdbx_env_set_maxdbs
#define mdb_env_get_maxkeysize	mdbx_env_get_maxkeysize
#define mdb_env_set_userctx	mdbx_env_set_userctx
#define mdb_env_get_userctx	mdbx_env_get_userctx
#define mdb_env_set_assert	mdbx_env_set_assert
#define mdb_txn_begin	mdbx_txn_begin
#define mdb_txn_env	mdbx_txn_env
#define mdb_txn_id	mdbx_txn_id
#define mdb_txn_commit	mdbx_txn_commit
#define mdb_txn_abort	mdbx_txn_abort
#define mdb_txn_reset	mdbx_txn_reset
#define mdb_txn_renew	mdbx_txn_renew
#define mdb_dbi_open	mdbx_dbi_open
#define mdb_dbi_flags	mdbx_dbi_flags
#define mdb_dbi_close	mdbx_dbi_close
#define mdb_drop	mdbx_drop
#define mdb_set_compare	mdbx_set_compare
#define mdb_set_dupsort	mdbx_set_dupsort
#define mdb_set_relfunc	mdbx_set_relfunc
#define mdb_set_relctx	mdbx_set_relctx
#define mdb_get	mdbx_get
#define mdb_put	mdbx_put
#define mdb_del	mdbx_del
#define mdb_cursor_open	mdbx_cursor_open
#define mdb_cursor_close	mdbx_cursor_close
#define mdb_cursor_renew	mdbx_cursor_renew
#define mdb_cursor_txn	mdbx_cursor_txn
#define mdb_cursor_dbi	mdbx_cursor_dbi
#define mdb_cursor_get	mdbx_cursor_get
#define mdb_cursor_put	mdbx_cursor_put
#define mdb_cursor_del	mdbx_cursor_del
#define mdb_cursor_count	mdbx_cursor_count
#define mdb_cmp	mdbx_cmp
#define mdb_dcmp	mdbx_dcmp
#define mdb_reader_list	mdbx_reader_list
#define mdb_reader_check	mdbx_reader_check
#define mdb_dkey	mdbx_dkey

/** Compat with version <= 0.9.4, avoid clash with libmdb from MDB Tools project */
#define mdbx_open(txn,name,flags,dbi) mdbx_dbi_open(txn,name,flags,dbi)
#define mdbx_close(env,dbi) mdbx_dbi_close(env,dbi)

#include "./lmdb.h"

#endif /* _MDBX_H_ */
