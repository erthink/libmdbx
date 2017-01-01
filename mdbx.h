/*
 * Copyright 2015-2017 Leonid Yuriev <leo@yuriev.ru>.
 * Copyright 2015,2016 Peter-Service R&D LLC.
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

/** @defgroup mdbx MDBX API
 *	@{
 *	@brief libmdbx - Extended version of LMDB
 */

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

#ifdef __cplusplus
extern "C" {
#endif

int mdbx_env_open_ex(MDB_env *env, const char *path, unsigned flags, mode_t mode, int *exclusive);
int mdbx_env_stat(MDB_env *env, MDBX_stat *stat, size_t bytes);
int mdbx_stat(MDB_txn *txn, MDB_dbi dbi, MDBX_stat *stat, size_t bytes);
int mdbx_env_info(MDB_env *env, MDBX_envinfo *info, size_t bytes);
int mdbx_env_close_ex(MDB_env *env, int dont_sync);

	/** @brief Set threshold to force flush the data buffers to disk,
	 * even of #MDB_NOSYNC, #MDB_NOMETASYNC and #MDB_MAPASYNC flags
	 * in the environment.
	 *
	 * Data is always written to disk when #mdb_txn_commit() is called,
	 * but the operating system may keep it buffered. LMDB always flushes
	 * the OS buffers upon commit as well, unless the environment was
	 * opened with #MDB_NOSYNC or in part #MDB_NOMETASYNC.
	 *
	 * The default is 0, than mean no any threshold checked,
	 * and no additional flush will be made.
	 *
	 * @param[in] env An environment handle returned by #mdb_env_create()
	 * @param[in] bytes The size in bytes of summary changes
	 * when a synchronous flush would be made.
	 * @return A non-zero error value on failure and 0 on success.
	 */
int mdbx_env_set_syncbytes(MDB_env *env, size_t bytes);
	/** @brief Returns a lag of the reading.
	 *
	 * Returns an information for estimate how much given read-only
	 * transaction is lagging relative the to actual head.
	 *
	 * @param[in] txn A transaction handle returned by #mdb_txn_begin()
	 * @param[out] percent Percentage of page allocation in the database.
	 * @return Number of transactions committed after the given was started for read, or -1 on failure.
	 */
int mdbx_txn_straggler(MDB_txn *txn, int *percent);

	/** @brief A callback function for killing a laggard readers,
	 * but also could waiting ones. Called in case of MDB_MAP_FULL error.
	 *
	 * @param[in] env An environment handle returned by #mdb_env_create().
	 * @param[in] pid pid of the reader process.
	 * @param[in] thread_id thread_id of the reader thread.
	 * @param[in] txn Transaction number on which stalled.
	 * @param[in] gap a lag from the last commited txn.
	 * @param[in] retry a retry number, less that zero for notify end of OOM-loop.
	 * @return -1 on failure (reader is not killed),
	 * 	0 on a race condition (no such reader),
	 * 	1 on success (reader was killed),
	 * 	>1 on success (reader was SURE killed).
	 */
typedef int (MDBX_oom_func)(MDB_env *env, int pid, void* thread_id, size_t txn, unsigned gap, int retry);

	/** @brief Set the OOM callback.
	 *
	 * Callback will be called only on out-of-pages case for killing
	 * a laggard readers to allowing reclaiming of freeDB.
	 *
	 * @param[in] env An environment handle returned by #mdb_env_create().
	 * @param[in] oomfunc A #MDBX_oom_func function or NULL to disable.
	 */
void mdbx_env_set_oomfunc(MDB_env *env, MDBX_oom_func *oom_func);

	/** @brief Get the current oom_func callback.
	 *
	 * Callback will be called only on out-of-pages case for killing
	 * a laggard readers to allowing reclaiming of freeDB.
	 *
	 * @param[in] env An environment handle returned by #mdb_env_create().
	 * @return A #MDBX_oom_func function or NULL if disabled.
	 */
MDBX_oom_func* mdbx_env_get_oomfunc(MDB_env *env);

#define MDBX_DBG_ASSERT	1
#define MDBX_DBG_PRINT	2
#define MDBX_DBG_TRACE	4
#define MDBX_DBG_EXTRA	8
#define MDBX_DBG_AUDIT	16
#define MDBX_DBG_EDGE	32

/* LY: a "don't touch" value */
#define MDBX_DBG_DNT	(-1L)

typedef void MDBX_debug_func(int type, const char *function, int line,
								  const char *msg, va_list args);

int mdbx_setup_debug(int flags, MDBX_debug_func* logger, long edge_txn);

typedef int MDBX_pgvisitor_func(size_t pgno, unsigned pgnumber, void* ctx,
					const char* dbi, const char *type, int nentries,
					int payload_bytes, int header_bytes, int unused_bytes);
int mdbx_env_pgwalk(MDB_txn *txn, MDBX_pgvisitor_func* visitor, void* ctx);
/**	@} */

#ifdef __cplusplus
}
#endif

#endif /* _MDBX_H_ */
