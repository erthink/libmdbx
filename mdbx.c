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

#include "mdbx.h"

int mdb_runtime_flags = MDBX_DBG_PRINT
#if MDB_DEBUG
		| MDBX_DBG_ASSERT
#endif
#if MDB_DEBUG > 1
		| MDBX_DBG_TRACE
#endif
#if MDB_DEBUG > 2
		| MDBX_DBG_AUDIT
#endif
#if MDB_DEBUG > 3
		| MDBX_DBG_EXTRA
#endif
	;

static MDBX_debug_func *mdb_debug_logger;

int mdbx_setup_debug(int flags, MDBX_debug_func* logger, long edge_txn);

#include "mdb.c"

int __cold
mdbx_setup_debug(int flags, MDBX_debug_func* logger, long edge_txn) {
	unsigned ret = mdb_runtime_flags;
	if (flags != (int) MDBX_DBG_DNT)
		mdb_runtime_flags = flags;
	if (logger != (MDBX_debug_func*) MDBX_DBG_DNT)
		mdb_debug_logger = logger;
	if (edge_txn != (long) MDBX_DBG_DNT) {
#if MDB_DEBUG
		mdb_debug_edge = edge_txn;
#endif
	}
	return ret;
}

static txnid_t __cold
mdbx_oomkick(MDB_env *env, txnid_t oldest)
{
	mdb_debug("DB size maxed out");
#if MDBX_MODE_ENABLED
	int retry;
	txnid_t snap;
	mdb_debug("DB size maxed out");

	for(retry = 0; ; ++retry) {
		int reader;

		if (mdb_reader_check(env, NULL))
			break;

		snap = mdb_find_oldest(env, &reader);
		if (oldest < snap || reader < 0) {
			if (retry && env->me_oom_func) {
				/* LY: notify end of oom-loop */
				env->me_oom_func(env, 0, 0, oldest, snap - oldest, -retry);
			}
			return snap;
		}

		MDB_reader *r;
		pthread_t tid;
		pid_t pid;
		int rc;

		if (!env->me_oom_func)
			break;

		r = &env->me_txns->mti_readers[ reader ];
		pid = r->mr_pid;
		tid = r->mr_tid;
		if (r->mr_txnid != oldest || pid <= 0)
			continue;

		rc = env->me_oom_func(env, pid, (void*) tid, oldest,
			mdb_meta_head_w(env)->mm_txnid - oldest, retry);
		if (rc < 0)
			break;

		if (rc) {
			r->mr_txnid = ~(txnid_t)0;
			if (rc > 1) {
				r->mr_tid = 0;
				r->mr_pid = 0;
				mdbx_coherent_barrier();
			}
		}
	}

	if (retry && env->me_oom_func) {
		/* LY: notify end of oom-loop */
		env->me_oom_func(env, 0, 0, oldest, 0, -retry);
	}
#else
	(void) oldest;
	(void) mdb_reader_check(env, NULL);
#endif /* MDBX_MODE_ENABLED */
	return mdb_find_oldest(env, NULL);
}

int __cold
mdbx_env_set_syncbytes(MDB_env *env, size_t bytes)
{
	if (unlikely(!env))
		return EINVAL;

	if(unlikely(env->me_signature != MDBX_ME_SIGNATURE))
		return MDB_VERSION_MISMATCH;

	env->me_sync_threshold = bytes;
	return env->me_map ? mdb_env_sync(env, 0) : 0;
}

void __cold
mdbx_env_set_oomfunc(MDB_env *env, MDBX_oom_func *oomfunc)
{
	if (likely(env && env->me_signature == MDBX_ME_SIGNATURE))
		env->me_oom_func = oomfunc;
}

MDBX_oom_func* __cold
mdbx_env_get_oomfunc(MDB_env *env)
{
	return likely(env && env->me_signature == MDBX_ME_SIGNATURE)
		? env->me_oom_func : NULL;
}

struct mdb_walk_ctx {
	MDB_txn *mw_txn;
	void *mw_user;
	MDBX_pgvisitor_func *mw_visitor;
};

typedef struct mdb_walk_ctx mdb_walk_ctx_t;


/** Depth-first tree traversal. */
static int __cold
mdb_env_walk(mdb_walk_ctx_t *ctx, const char* dbi, pgno_t pg, int flags, int deep)
{
	MDB_page *mp;
	int rc, i, nkeys;
	unsigned header_size, unused_size, payload_size, align_bytes;
	const char* type;

	if (pg == P_INVALID)
		return MDB_SUCCESS; /* empty db */

	rc = mdb_page_get(ctx->mw_txn, pg, &mp, NULL);
	if (rc)
		return rc;
	if (pg != mp->mp_p.p_pgno)
		return MDB_CORRUPTED;

	nkeys = NUMKEYS(mp);
	header_size = IS_LEAF2(mp) ? PAGEHDRSZ : PAGEBASE + mp->mp_lower;
	unused_size = SIZELEFT(mp);
	payload_size = 0;

	/* LY: Don't use mask here, e.g bitwise (P_BRANCH|P_LEAF|P_LEAF2|P_META|P_OVERFLOW|P_SUBP).
	 * Pages should not me marked dirty/loose or otherwise. */
	switch (mp->mp_flags) {
	case P_BRANCH:
		type = "branch";
		if (nkeys < 1)
			return MDB_CORRUPTED;
		break;
	case P_LEAF:
		type = "leaf";
		break;
	case P_LEAF|P_SUBP:
		type = "dupsort-subleaf";
		break;
	case P_LEAF|P_LEAF2:
		type = "dupfixed-leaf";
		break;
	case P_LEAF|P_LEAF2|P_SUBP:
		type = "dupsort-dupfixed-subleaf";
		break;
	case P_META:
	case P_OVERFLOW:
	default:
		return MDB_CORRUPTED;
	}

	for (align_bytes = i = 0; i < nkeys;
		 align_bytes += ((payload_size + align_bytes) & 1), i++) {
		MDB_node *node;

		if (IS_LEAF2(mp)) {
			/* LEAF2 pages have no mp_ptrs[] or node headers */
			payload_size += mp->mp_leaf2_ksize;
			continue;
		}

		node = NODEPTR(mp, i);
		payload_size += NODESIZE + node->mn_ksize;

		if (IS_BRANCH(mp)) {
			rc = mdb_env_walk(ctx, dbi, NODEPGNO(node), flags, deep);
			if (rc)
				return rc;
			continue;
		}

		assert(IS_LEAF(mp));
		if (node->mn_ksize < 1)
			return MDB_CORRUPTED;
		if (node->mn_flags & F_BIGDATA) {
			MDB_page *omp;
			pgno_t *opg;
			size_t over_header, over_payload, over_unused;

			payload_size += sizeof(pgno_t);
			opg = NODEDATA(node);
			rc = mdb_page_get(ctx->mw_txn, *opg, &omp, NULL);
			if (rc)
				return rc;
			if (*opg != omp->mp_p.p_pgno)
				return MDB_CORRUPTED;
			/* LY: Don't use mask here, e.g bitwise (P_BRANCH|P_LEAF|P_LEAF2|P_META|P_OVERFLOW|P_SUBP).
			 * Pages should not me marked dirty/loose or otherwise. */
			if (P_OVERFLOW != omp->mp_flags)
				return MDB_CORRUPTED;

			over_header = PAGEHDRSZ;
			over_payload = NODEDSZ(node);
			over_unused = omp->mp_pages * ctx->mw_txn->mt_env->me_psize
					- over_payload - over_header;

			rc = ctx->mw_visitor(*opg, omp->mp_pages, ctx->mw_user, dbi,
					"overflow-data", 1, over_payload, over_header, over_unused);
			if (rc)
				return rc;
			continue;
		}

		payload_size += NODEDSZ(node);
		if (node->mn_flags & F_SUBDATA) {
			MDB_db *db = NODEDATA(node);
			char* name = NULL;

			if (NODEDSZ(node) < 1)
				return MDB_CORRUPTED;
			if (! (node->mn_flags & F_DUPDATA)) {
				name = NODEKEY(node);
				int namelen = (char*) db - name;
				name = memcpy(alloca(namelen + 1), name, namelen);
				name[namelen] = 0;
			}
			rc = mdb_env_walk(ctx, (name && name[0]) ? name : dbi,
					db->md_root, node->mn_flags & F_DUPDATA, deep + 1);
			if (rc)
				return rc;
		}
	}

	return ctx->mw_visitor(mp->mp_p.p_pgno, 1, ctx->mw_user, dbi, type,
		nkeys, payload_size, header_size, unused_size + align_bytes);
}

int __cold
mdbx_env_pgwalk(MDB_txn *txn, MDBX_pgvisitor_func* visitor, void* user)
{
	mdb_walk_ctx_t ctx;
	int rc;

	if (unlikely(!txn))
		return MDB_BAD_TXN;
	if (unlikely(txn->mt_signature != MDBX_MT_SIGNATURE))
		return MDB_VERSION_MISMATCH;

	ctx.mw_txn = txn;
	ctx.mw_user = user;
	ctx.mw_visitor = visitor;

	rc = visitor(0, 2, user, "lmdb", "meta", 2, sizeof(MDB_meta)*2, PAGEHDRSZ*2,
				 (txn->mt_env->me_psize - sizeof(MDB_meta) - PAGEHDRSZ) *2);
	if (! rc)
		rc = mdb_env_walk(&ctx, "free", txn->mt_dbs[FREE_DBI].md_root, 0, 0);
	if (! rc)
		rc = mdb_env_walk(&ctx, "main", txn->mt_dbs[MAIN_DBI].md_root, 0, 0);
	if (! rc)
		rc = visitor(P_INVALID, 0, user, NULL, NULL, 0, 0, 0, 0);
	return rc;
}

/* attribute support functions for Nexenta ***********************************/

static __inline int
mdbx_attr_peek(MDB_val *data, mdbx_attr_t *attrptr)
{
	if (unlikely(data->mv_size < sizeof(mdbx_attr_t)))
		return MDB_INCOMPATIBLE;

	if (likely(attrptr != NULL))
		*attrptr = *(mdbx_attr_t*) data->mv_data;
	data->mv_size -= sizeof(mdbx_attr_t);
	data->mv_data = likely(data->mv_size > 0)
		? ((mdbx_attr_t*) data->mv_data) + 1 : NULL;

	return MDB_SUCCESS;
}

static __inline int
mdbx_attr_poke(MDB_val *reserved, MDB_val *data, mdbx_attr_t attr, unsigned flags)
{
	mdbx_attr_t *space = reserved->mv_data;
	if (flags & MDB_RESERVE) {
		if (likely(data != NULL)) {
			data->mv_data = data->mv_size ? space + 1 : NULL;
		}
	} else {
		*space = attr;
		if (likely(data != NULL)) {
			memcpy(space + 1, data->mv_data, data->mv_size );
		}
	}

	return MDB_SUCCESS;
}

int
mdbx_cursor_get_attr(MDB_cursor *mc, MDB_val *key, MDB_val *data,
	mdbx_attr_t *attrptr, MDB_cursor_op op)
{
	int rc = mdbx_cursor_get(mc, key, data, op);
	if (unlikely(rc != MDB_SUCCESS))
		return rc;

	return mdbx_attr_peek(data, attrptr);
}

int
mdbx_get_attr(MDB_txn *txn, MDB_dbi dbi,
	MDB_val *key, MDB_val *data, uint64_t *attrptr)
{
	int rc = mdbx_get(txn, dbi, key, data);
	if (unlikely(rc != MDB_SUCCESS))
		return rc;

	return mdbx_attr_peek(data, attrptr);
}

int
mdbx_put_attr(MDB_txn *txn, MDB_dbi dbi,
	MDB_val *key, MDB_val *data, mdbx_attr_t attr, unsigned flags)
{
	MDB_val reserve = {
		.mv_data = NULL,
		.mv_size = (data ? data->mv_size : 0) + sizeof(mdbx_attr_t)
	};

	int rc = mdbx_put(txn, dbi, key, &reserve, flags | MDB_RESERVE);
	if (unlikely(rc != MDB_SUCCESS))
		return rc;

	return mdbx_attr_poke(&reserve, data, attr, flags);
}

int mdbx_cursor_put_attr(MDB_cursor *cursor, MDB_val *key, MDB_val *data,
	mdbx_attr_t attr, unsigned flags)
{
	MDB_val reserve = {
		.mv_data = NULL,
		.mv_size = (data ? data->mv_size : 0) + sizeof(mdbx_attr_t)
	};

	int rc = mdbx_cursor_put(cursor, key, &reserve, flags | MDB_RESERVE);
	if (unlikely(rc != MDB_SUCCESS))
		return rc;

	return mdbx_attr_poke(&reserve, data, attr, flags);
}

int mdbx_set_attr(MDB_txn *txn, MDB_dbi dbi,
	MDB_val *key, MDB_val *data, mdbx_attr_t attr)
{
	MDB_cursor      mc;
	MDB_xcursor     mx;
	MDB_val         old_data;
	mdbx_attr_t		old_attr;
	int             rc;

	if (unlikely(!key || !txn))
		return EINVAL;

	if (unlikely(txn->mt_signature != MDBX_MT_SIGNATURE))
		return MDB_VERSION_MISMATCH;

	if (unlikely(!TXN_DBI_EXIST(txn, dbi, DB_USRVALID)))
		return EINVAL;

	if (unlikely(txn->mt_flags & (MDB_TXN_RDONLY|MDB_TXN_BLOCKED)))
		return (txn->mt_flags & MDB_TXN_RDONLY) ? EACCES : MDB_BAD_TXN;

	mdb_cursor_init(&mc, txn, dbi, &mx);
	rc = mdb_cursor_set(&mc, key, &old_data, MDB_SET, NULL);
	if (unlikely(rc != MDB_SUCCESS)) {
		if (rc == MDB_NOTFOUND && data) {
			mc.mc_next = txn->mt_cursors[dbi];
			txn->mt_cursors[dbi] = &mc;
			rc = mdbx_cursor_put_attr(&mc, key, data, attr, 0);
			txn->mt_cursors[dbi] = mc.mc_next;
		}
		return rc;
	}

	rc = mdbx_attr_peek(&old_data, &old_attr);
	if (unlikely(rc != MDB_SUCCESS))
		return rc;

	if (old_attr == attr && (!data ||
			(data->mv_size == old_data.mv_size
				&& memcpy(data->mv_data, old_data.mv_data, old_data.mv_size) == 0)))
		return MDB_SUCCESS;

	mc.mc_next = txn->mt_cursors[dbi];
	txn->mt_cursors[dbi] = &mc;
	rc = mdbx_cursor_put_attr(&mc, key, data ? data : &old_data, attr, MDB_CURRENT);
	txn->mt_cursors[dbi] = mc.mc_next;
	return rc;
}
