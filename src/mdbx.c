/*
 * Copyright 2015-2017 Leonid Yuriev <leo@yuriev.ru>
 * and other libmdbx authors: please see AUTHORS file.
 * All rights reserved.
 *
 * This code is derived from "LMDB engine" written by
 * Howard Chu (Symas Corporation), which itself derived from btree.c
 * written by Martin Hedenfalk.
 *
 * ---
 *
 * Portions Copyright 2011-2017 Howard Chu, Symas Corp. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in the file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 *
 * ---
 *
 * Portions Copyright (c) 2009, 2010 Martin Hedenfalk <martin@bzero.se>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "./bits.h"
#include "./midl.h"

/*----------------------------------------------------------------------------*/
/* rthc (tls keys and destructors) */

typedef struct rthc_entry_t {
  MDB_reader *begin;
  MDB_reader *end;
  mdbx_thread_key_t key;
} rthc_entry_t;

#if MDB_DEBUG
#define RTHC_INITIAL_LIMIT 1
#else
#define RTHC_INITIAL_LIMIT 16
#endif

static unsigned rthc_count;
static unsigned rthc_limit = RTHC_INITIAL_LIMIT;
static rthc_entry_t rthc_table_static[RTHC_INITIAL_LIMIT];
static rthc_entry_t *rthc_table = rthc_table_static;

__cold void mdbx_rthc_dtor(void *ptr) {
  MDB_reader *rthc = (MDB_reader *)ptr;

  mdbx_rthc_lock();
  const mdbx_pid_t self_pid = mdbx_getpid();
  for (unsigned i = 0; i < rthc_count; ++i) {
    if (rthc >= rthc_table[i].begin && rthc < rthc_table[i].end) {
      if (rthc->mr_pid == self_pid) {
        rthc->mr_pid = 0;
        mdbx_coherent_barrier();
      }
      break;
    }
  }
  mdbx_rthc_unlock();
}

__cold void mdbx_rthc_cleanup(void) {
  mdbx_rthc_lock();
  const mdbx_pid_t self_pid = mdbx_getpid();
  for (unsigned i = 0; i < rthc_count; ++i) {
    mdbx_thread_key_t key = rthc_table[i].key;
    MDB_reader *rthc = mdbx_thread_rthc_get(key);
    if (rthc) {
      mdbx_thread_rthc_set(key, NULL);
      if (rthc->mr_pid == self_pid) {
        rthc->mr_pid = 0;
        mdbx_coherent_barrier();
      }
    }
  }
  mdbx_rthc_unlock();
}

__cold int mdbx_rthc_alloc(mdbx_thread_key_t *key, MDB_reader *begin,
                           MDB_reader *end) {
#ifndef NDEBUG
  *key = (mdbx_thread_key_t)0xBADBADBAD;
#endif /* NDEBUG */
  int rc = mdbx_thread_key_create(key);
  if (rc != MDB_SUCCESS)
    return rc;

  mdbx_rthc_lock();
  if (rthc_count == rthc_limit) {
    rthc_entry_t *new_table =
        realloc((rthc_table == rthc_table_static) ? NULL : rthc_table,
                sizeof(rthc_entry_t) * rthc_limit * 2);
    if (new_table == NULL) {
      rc = MDBX_ENOMEM;
      goto bailout;
    }
    if (rthc_table == rthc_table_static)
      memcpy(new_table, rthc_table_static, sizeof(rthc_table_static));
    rthc_table = new_table;
    rthc_limit *= 2;
  }

  rthc_table[rthc_count].key = *key;
  rthc_table[rthc_count].begin = begin;
  rthc_table[rthc_count].end = end;
  ++rthc_count;
  mdbx_rthc_unlock();
  return MDB_SUCCESS;

bailout:
  mdbx_thread_key_delete(*key);
  mdbx_rthc_unlock();
  return rc;
}

__cold void mdbx_rthc_remove(mdbx_thread_key_t key) {
  mdbx_rthc_lock();
  mdbx_thread_key_delete(key);

  for (unsigned i = 0; i < rthc_count; ++i) {
    if (key == rthc_table[i].key) {
      const mdbx_pid_t self_pid = mdbx_getpid();
      for (MDB_reader *rthc = rthc_table[i].begin; rthc < rthc_table[i].end;
           ++rthc)
        if (rthc->mr_pid == self_pid)
          rthc->mr_pid = 0;
      mdbx_coherent_barrier();
      if (--rthc_count > 0)
        rthc_table[i] = rthc_table[rthc_count];
      else if (rthc_table != rthc_table_static) {
        free(rthc_table);
        rthc_table = rthc_table_static;
        rthc_limit = RTHC_INITIAL_LIMIT;
      }
      break;
    }
  }

  mdbx_rthc_unlock();
}

/*----------------------------------------------------------------------------*/

/** Search for an ID in an IDL.
 * @param[in] ids	The IDL to search.
 * @param[in] id	The ID to search for.
 * @return	The index of the first ID greater than or equal to \b id.
 */
static unsigned mdbx_midl_search(MDB_IDL ids, MDB_ID id);

/** Allocate an IDL.
 * Allocates memory for an IDL of the given size.
 * @return	IDL on success, NULL on failure.
 */
static MDB_IDL mdbx_midl_alloc(int num);

/** Free an IDL.
 * @param[in] ids	The IDL to free.
 */
static void mdbx_midl_free(MDB_IDL ids);

/** Shrink an IDL.
 * Return the IDL to the default size if it has grown larger.
 * @param[in,out] idp	Address of the IDL to shrink.
 */
static void mdbx_midl_shrink(MDB_IDL *idp);

/** Make room for num additional elements in an IDL.
 * @param[in,out] idp	Address of the IDL.
 * @param[in] num	Number of elements to make room for.
 * @return	0 on success, MDBX_ENOMEM on failure.
 */
static int mdbx_midl_need(MDB_IDL *idp, unsigned num);

/** Append an ID onto an IDL.
 * @param[in,out] idp	Address of the IDL to append to.
 * @param[in] id	The ID to append.
 * @return	0 on success, MDBX_ENOMEM if the IDL is too large.
 */
static int mdbx_midl_append(MDB_IDL *idp, MDB_ID id);

/** Append an IDL onto an IDL.
 * @param[in,out] idp	Address of the IDL to append to.
 * @param[in] app	The IDL to append.
 * @return	0 on success, MDBX_ENOMEM if the IDL is too large.
 */
static int mdbx_midl_append_list(MDB_IDL *idp, MDB_IDL app);

/** Append an ID range onto an IDL.
 * @param[in,out] idp	Address of the IDL to append to.
 * @param[in] id	The lowest ID to append.
 * @param[in] n		Number of IDs to append.
 * @return	0 on success, MDBX_ENOMEM if the IDL is too large.
 */
static int mdbx_midl_append_range(MDB_IDL *idp, MDB_ID id, unsigned n);

/** Merge an IDL onto an IDL. The destination IDL must be big enough.
 * @param[in] idl	The IDL to merge into.
 * @param[in] merge	The IDL to merge.
 */
static void mdbx_midl_xmerge(MDB_IDL idl, MDB_IDL merge);

/** Sort an IDL.
 * @param[in,out] ids	The IDL to sort.
 */
static void mdbx_midl_sort(MDB_IDL ids);

/** Search for an ID in an ID2L.
 * @param[in] ids	The ID2L to search.
 * @param[in] id	The ID to search for.
 * @return	The index of the first ID2 whose \b mid member is greater than
 * or equal to \b id.
 */
static unsigned mdbx_mid2l_search(MDB_ID2L ids, MDB_ID id);

/** Insert an ID2 into a ID2L.
 * @param[in,out] ids	The ID2L to insert into.
 * @param[in] id	The ID2 to insert.
 * @return	0 on success, -1 if the ID was already present in the ID2L.
 */
static int mdbx_mid2l_insert(MDB_ID2L ids, MDB_ID2 *id);

/** Append an ID2 into a ID2L.
 * @param[in,out] ids	The ID2L to append into.
 * @param[in] id	The ID2 to append.
 * @return	0 on success, -2 if the ID2L is too big.
 */
static int mdbx_mid2l_append(MDB_ID2L ids, MDB_ID2 *id);

/*----------------------------------------------------------------------------*/

int mdbx_runtime_flags = MDBX_DBG_PRINT
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

MDBX_debug_func *mdbx_debug_logger;

int mdbx_setup_debug(int flags, MDBX_debug_func *logger, long edge_txn);

#if MDB_DEBUG
txnid_t mdbx_debug_edge;
#endif

/** Features under development */
#ifndef MDB_DEVEL
#define MDB_DEVEL 0
#endif

/* Internal error codes, not exposed outside liblmdb */
#define MDB_NO_ROOT (MDB_LAST_ERRCODE + 10)

/* Debuging output value of a cursor DBI: Negative in a sub-cursor. */
#define DDBI(mc)                                                               \
  (((mc)->mc_flags & C_SUB) ? -(int)(mc)->mc_dbi : (int)(mc)->mc_dbi)

/**	@brief The maximum size of a database page.
 *
 *	It is 32k or 64k, since value-PAGEBASE must fit in
 *	#MDB_page.%mp_upper.
 *
 *	LMDB will use database pages < OS pages if needed.
 *	That causes more I/O in write transactions: The OS must
 *	know (read) the whole page before writing a partial page.
 *
 *	Note that we don't currently support Huge pages. On Linux,
 *	regular data files cannot use Huge pages, and in general
 *	Huge pages aren't actually pageable. We rely on the OS
 *	demand-pager to read our data and page it out when memory
 *	pressure from other processes is high. So until OSs have
 *	actual paging support for Huge pages, they're not viable.
 */
#define MAX_PAGESIZE (PAGEBASE ? 0x10000 : 0x8000)

/** The minimum number of keys required in a database page.
 *	Setting this to a larger value will place a smaller bound on the
 *	maximum size of a data item. Data items larger than this size will
 *	be pushed into overflow pages instead of being stored directly in
 *	the B-tree node. This value used to default to 4. With a page size
 *	of 4096 bytes that meant that any item larger than 1024 bytes would
 *	go into an overflow page. That also meant that on average 2-3KB of
 *	each overflow page was wasted space. The value cannot be lower than
 *	2 because then there would no longer be a tree structure. With this
 *	value, items larger than 2KB will go into overflow pages, and on
 *	average only 1KB will be wasted.
 */
#define MDB_MINKEYS 2

/**	A stamp that identifies a file as an LMDB file.
 *	There's nothing special about this value other than that it is easily
 *	recognizable, and it will reflect any byte order mismatches.
 */
#define MDB_MAGIC 0xBEEFC0DE

/**	The version number for a database's datafile format. */
#define MDB_DATA_VERSION ((MDB_DEVEL) ? 999 : 1)
/**	The version number for a database's lockfile format. */
#define MDB_LOCK_VERSION ((MDB_DEVEL) ? 999 : 1)

#define DKBUF_MAXKEYSIZE 511 /* FIXME */
                             /**	Key size which fits in a #DKBUF.
                              *	@ingroup debug
                              */
#define DKBUF char kbuf[DKBUF_MAXKEYSIZE]
/**	Display a key in hex.
 *	@ingroup debug
 *	Invoke a function to display a key in hex.
 */
#define DKEY(x) mdbx_dkey(x, kbuf, sizeof(kbuf))

/** An invalid page number.
 *	Mainly used to denote an empty tree.
 */
#define P_INVALID (~(pgno_t)0)

/** Test if the flags \b f are set in a flag word \b w. */
#define F_ISSET(w, f) (((w) & (f)) == (f))

/** Round \b n up to an even number. */
#define EVEN(n) (((n) + 1U) & -2) /* sign-extending -2 to match n+1U */

/**	Default size of memory map.
 *	This is certainly too small for any actual applications. Apps should
 *always set
 *	the size explicitly using #mdbx_env_set_mapsize().
 */
#define DEFAULT_MAPSIZE 1048576

/**	@defgroup readers	Reader Lock Table
 *	Readers don't acquire any locks for their data access. Instead, they
 *	simply record their transaction ID in the reader table. The reader
 *	mutex is needed just to find an empty slot in the reader table. The
 *	slot's address is saved in thread-specific data so that subsequent
 *read
 *	transactions started by the same thread need no further locking to
 *proceed.
 *
 *	If #MDB_NOTLS is set, the slot address is not saved in thread-specific
 *data.
 *
 *	No reader table is used if the database is on a read-only filesystem,
 *or
 *	if #MDB_NOLOCK is set.
 *
 *	Since the database uses multi-version concurrency control, readers
 *don't
 *	actually need any locking. This table is used to keep track of which
 *	readers are using data from which old transactions, so that we'll know
 *	when a particular old transaction is no longer in use. Old
 *transactions
 *	that have discarded any data pages can then have those pages reclaimed
 *	for use by a later write transaction.
 *
 *	The lock table is constructed such that reader slots are aligned with
 *the
 *	processor's cache line size. Any slot is only ever used by one thread.
 *	This alignment guarantees that there will be no contention or cache
 *	thrashing as threads update their own slot info, and also eliminates
 *	any need for locking when accessing a slot.
 *
 *	A writer thread will scan every slot in the table to determine the
 *oldest
 *	outstanding reader transaction. Any freed pages older than this will
 *be
 *	reclaimed by the writer. The writer doesn't use any locks when
 *scanning
 *	this table. This means that there's no guarantee that the writer will
 *	see the most up-to-date reader info, but that's not required for
 *correct
 *	operation - all we need is to know the upper bound on the oldest
 *reader,
 *	we don't care at all about the newest reader. So the only consequence
 *of
 *	reading stale information here is that old pages might hang around a
 *	while longer before being reclaimed. That's actually good anyway,
 *because
 *	the longer we delay reclaiming old pages, the more likely it is that a
 *	string of contiguous pages can be found after coalescing old pages
 *from
 *	many old transactions together.
 *	@{
 */
/**	Number of slots in the reader table.
 *	This value was chosen somewhat arbitrarily. 126 readers plus a
 *	couple mutexes fit exactly into 8KB on my development machine.
 *	Applications should set the table size using
 *#mdbx_env_set_maxreaders().
 */
#define DEFAULT_READERS 126

/** Address of first usable data byte in a page, after the header */
#define PAGEDATA(p) ((void *)((char *)(p) + PAGEHDRSZ))

/** ITS#7713, change PAGEBASE to handle 65536 byte pages */
#define PAGEBASE ((MDB_DEVEL) ? PAGEHDRSZ : 0)

/** Number of nodes on a page */
#define NUMKEYS(p) (((p)->mp_lower - (PAGEHDRSZ - PAGEBASE)) >> 1)

/** The amount of space remaining in the page */
#define SIZELEFT(p) (indx_t)((p)->mp_upper - (p)->mp_lower)

/** The percentage of space used in the page, in tenths of a percent. */
#define PAGEFILL(env, p)                                                       \
  (1000L * ((env)->me_psize - PAGEHDRSZ - SIZELEFT(p)) /                       \
   ((env)->me_psize - PAGEHDRSZ))
/** The minimum page fill factor, in tenths of a percent.
 *	Pages emptier than this are candidates for merging.
 */
#define FILL_THRESHOLD 250

/** Test if a page is a leaf page */
#define IS_LEAF(p) F_ISSET((p)->mp_flags, P_LEAF)
/** Test if a page is a LEAF2 page */
#define IS_LEAF2(p) F_ISSET((p)->mp_flags, P_LEAF2)
/** Test if a page is a branch page */
#define IS_BRANCH(p) F_ISSET((p)->mp_flags, P_BRANCH)
/** Test if a page is an overflow page */
#define IS_OVERFLOW(p) F_ISSET((p)->mp_flags, P_OVERFLOW)
/** Test if a page is a sub page */
#define IS_SUBP(p) F_ISSET((p)->mp_flags, P_SUBP)

/** The number of overflow pages needed to store the given size. */
#define OVPAGES(size, psize) ((PAGEHDRSZ - 1 + (size)) / (psize) + 1)

/** Link in #MDB_txn.%mt_loose_pgs list.
 *  Kept outside the page header, which is needed when reusing the page.
 */
#define NEXT_LOOSE_PAGE(p) (*(MDB_page **)((p) + 2))

/** Header for a single key/data pair within a page.
 * Used in pages of type #P_BRANCH and #P_LEAF without #P_LEAF2.
 * We guarantee 2-byte alignment for 'MDB_node's.
 *
 * #mn_lo and #mn_hi are used for data size on leaf nodes, and for child
 * pgno on branch nodes.  On 64 bit platforms, #mn_flags is also used
 * for pgno.  (Branch nodes have no flags).  Lo and hi are in host byte
 * order in case some accesses can be optimized to 32-bit word access.
 *
 * Leaf node flags describe node contents.  #F_BIGDATA says the node's
 * data part is the page number of an overflow page with actual data.
 * #F_DUPDATA and #F_SUBDATA can be combined giving duplicate data in
 * a sub-page/sub-database, and named databases (just #F_SUBDATA).
 */
typedef struct MDB_node {
/* part of data size or pgno */
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
  uint16_t mn_lo, mn_hi;
#else
  uint16_t mn_hi, mn_lo;
#endif
/** @defgroup mdbx_node Node Flags
 *	@ingroup internal
 *	Flags for node headers.
 */
#define F_BIGDATA 0x01 /**< data put on overflow page */
#define F_SUBDATA 0x02 /**< data is a sub-database */
#define F_DUPDATA 0x04 /**< data has duplicates */

/** valid flags for #mdbx_node_add() */
#define NODE_ADD_FLAGS (F_DUPDATA | F_SUBDATA | MDB_RESERVE | MDB_APPEND)

  uint16_t mn_flags;  /**< @ref mdbx_node */
  uint16_t mn_ksize;  /**< key size */
  uint8_t mn_data[1]; /**< key and data are appended here */
} MDB_node;

/** Size of the node header, excluding dynamic data at the end */
#define NODESIZE offsetof(MDB_node, mn_data)

/** Bit position of top word in page number, for shifting mn_flags */
#define PGNO_TOPWORD ((pgno_t)-1 > 0xffffffffu ? 32 : 0)

/** Size of a node in a branch page with a given key.
 *	This is just the node header plus the key, there is no data.
 */
#define INDXSIZE(k) (NODESIZE + ((k) == NULL ? 0 : (k)->mv_size))

/** Size of a node in a leaf page with a given key and data.
 *	This is node header plus key plus data size.
 */
#define LEAFSIZE(k, d) (NODESIZE + (k)->mv_size + (d)->mv_size)

/* Address of node i in page p */
static __inline MDB_node *NODEPTR(MDB_page *p, unsigned i) {
  assert(NUMKEYS(p) > (unsigned)(i));
  return (MDB_node *)((char *)(p) + (p)->mp_ptrs[i] + PAGEBASE);
}

/** Address of the key for the node */
#define NODEKEY(node) (void *)((node)->mn_data)

/** Address of the data for a node */
#define NODEDATA(node) (void *)((char *)(node)->mn_data + (node)->mn_ksize)

/** Get the page number pointed to by a branch node */
#define NODEPGNO(node)                                                         \
  ((node)->mn_lo | ((pgno_t)(node)->mn_hi << 16) |                             \
   (PGNO_TOPWORD ? ((pgno_t)(node)->mn_flags << PGNO_TOPWORD) : 0))
/** Set the page number in a branch node */
#define SETPGNO(node, pgno)                                                    \
  do {                                                                         \
    (node)->mn_lo = (uint16_t)(pgno);                                          \
    (node)->mn_hi = (uint16_t)((pgno) >> 16);                                  \
    if (PGNO_TOPWORD)                                                          \
      (node)->mn_flags = (uint16_t)((pgno) >> PGNO_TOPWORD);                   \
  } while (0)

/** Get the size of the data in a leaf node */
#define NODEDSZ(node) ((node)->mn_lo | ((unsigned)(node)->mn_hi << 16))
/** Set the size of the data for a leaf node */
#define SETDSZ(node, size)                                                     \
  do {                                                                         \
    (node)->mn_lo = (uint16_t)(size);                                          \
    (node)->mn_hi = (uint16_t)((size) >> 16);                                  \
  } while (0)
/** The size of a key in a node */
#define NODEKSZ(node) ((node)->mn_ksize)

/** Copy a page number from src to dst */
#if UNALIGNED_OK
#define COPY_PGNO(dst, src) dst = src
#elif SIZE_MAX > 4294967295UL
#define COPY_PGNO(dst, src)                                                    \
  do {                                                                         \
    uint16_t *s, *d;                                                           \
    s = (uint16_t *)&(src);                                                    \
    d = (uint16_t *)&(dst);                                                    \
    *d++ = *s++;                                                               \
    *d++ = *s++;                                                               \
    *d++ = *s++;                                                               \
    *d = *s;                                                                   \
  } while (0)
#else
#define COPY_PGNO(dst, src)                                                    \
  do {                                                                         \
    uint16_t *s, *d;                                                           \
    s = (uint16_t *)&(src);                                                    \
    d = (uint16_t *)&(dst);                                                    \
    *d++ = *s++;                                                               \
    *d = *s;                                                                   \
  } while (0)
#endif /* UNALIGNED_OK */

/** The address of a key in a LEAF2 page.
         *	LEAF2 pages are used for #MDB_DUPFIXED sorted-duplicate
  *sub-DBs.
         *	There are no node headers, keys are stored contiguously.
         */
#define LEAF2KEY(p, i, ks) ((char *)(p) + PAGEHDRSZ + ((i) * (ks)))

/** Set the \b node's key into \b keyptr, if requested. */
#define MDB_GET_KEY(node, keyptr)                                              \
  {                                                                            \
    if ((keyptr) != NULL) {                                                    \
      (keyptr)->mv_size = NODEKSZ(node);                                       \
      (keyptr)->mv_data = NODEKEY(node);                                       \
    }                                                                          \
  }

/** Set the \b node's key into \b key. */
#define MDB_GET_KEY2(node, key)                                                \
  {                                                                            \
    key.mv_size = NODEKSZ(node);                                               \
    key.mv_data = NODEKEY(node);                                               \
  }

#define MDB_VALID 0x8000 /**< DB handle is valid, for me_dbflags */
#define PERSISTENT_FLAGS (0xffff & ~(MDB_VALID))
/** #mdbx_dbi_open() flags */
#define VALID_FLAGS                                                            \
  (MDB_REVERSEKEY | MDB_DUPSORT | MDB_INTEGERKEY | MDB_DUPFIXED |              \
   MDB_INTEGERDUP | MDB_REVERSEDUP | MDB_CREATE)

/** max number of pages to commit in one writev() call */
#define MDB_COMMIT_PAGES 64
#if defined(IOV_MAX) && IOV_MAX < MDB_COMMIT_PAGES /* sysconf(_SC_IOV_MAX) */
#undef MDB_COMMIT_PAGES
#define MDB_COMMIT_PAGES IOV_MAX
#endif

/** Check \b txn and \b dbi arguments to a function */
#define TXN_DBI_EXIST(txn, dbi, validity)                                      \
  ((dbi) < (txn)->mt_numdbs && ((txn)->mt_dbflags[dbi] & (validity)))

/** Check for misused \b dbi handles */
#define TXN_DBI_CHANGED(txn, dbi)                                              \
  ((txn)->mt_dbiseqs[dbi] != (txn)->mt_env->me_dbiseqs[dbi])

static int mdbx_page_alloc(MDB_cursor *mc, int num, MDB_page **mp, int flags);
static int mdbx_page_new(MDB_cursor *mc, uint32_t flags, int num,
                         MDB_page **mp);
static int mdbx_page_touch(MDB_cursor *mc);
static int mdbx_cursor_touch(MDB_cursor *mc);

#define MDB_END_NAMES                                                          \
  {                                                                            \
    "committed", "empty-commit", "abort", "reset", "reset-tmp", "fail-begin",  \
        "fail-beginchild"                                                      \
  }
enum {
  /* mdbx_txn_end operation number, for logging */
  MDB_END_COMMITTED,
  MDB_END_EMPTY_COMMIT,
  MDB_END_ABORT,
  MDB_END_RESET,
  MDB_END_RESET_TMP,
  MDB_END_FAIL_BEGIN,
  MDB_END_FAIL_BEGINCHILD
};
#define MDB_END_OPMASK 0x0F  /**< mask for #mdbx_txn_end() operation number */
#define MDB_END_UPDATE 0x10  /**< update env state (DBIs) */
#define MDB_END_FREE 0x20    /**< free txn unless it is #MDB_env.%me_txn0 */
#define MDB_END_EOTDONE 0x40 /**< txn's cursors already closed */
#define MDB_END_SLOT 0x80    /**< release any reader slot if #MDB_NOTLS */
static int mdbx_txn_end(MDB_txn *txn, unsigned mode);

static int mdbx_page_get(MDB_cursor *mc, pgno_t pgno, MDB_page **mp, int *lvl);
static int mdbx_page_search_root(MDB_cursor *mc, MDB_val *key, int modify);
#define MDB_PS_MODIFY 1
#define MDB_PS_ROOTONLY 2
#define MDB_PS_FIRST 4
#define MDB_PS_LAST 8
static int mdbx_page_search(MDB_cursor *mc, MDB_val *key, int flags);
static int mdbx_page_merge(MDB_cursor *csrc, MDB_cursor *cdst);

#define MDB_SPLIT_REPLACE MDB_APPENDDUP /**< newkey is not new */
static int mdbx_page_split(MDB_cursor *mc, MDB_val *newkey, MDB_val *newdata,
                           pgno_t newpgno, unsigned nflags);

static int mdbx_read_header(MDB_env *env, MDB_meta *meta);
static int mdbx_env_sync0(MDB_env *env, unsigned flags, MDB_meta *pending);
static void mdbx_env_close0(MDB_env *env);

static MDB_node *mdbx_node_search(MDB_cursor *mc, MDB_val *key, int *exactp);
static int mdbx_node_add(MDB_cursor *mc, indx_t indx, MDB_val *key,
                         MDB_val *data, pgno_t pgno, unsigned flags);
static void mdbx_node_del(MDB_cursor *mc, int ksize);
static void mdbx_node_shrink(MDB_page *mp, indx_t indx);
static int mdbx_node_move(MDB_cursor *csrc, MDB_cursor *cdst, int fromleft);
static int mdbx_node_read(MDB_cursor *mc, MDB_node *leaf, MDB_val *data);
static size_t mdbx_leaf_size(MDB_env *env, MDB_val *key, MDB_val *data);
static size_t mdbx_branch_size(MDB_env *env, MDB_val *key);

static int mdbx_rebalance(MDB_cursor *mc);
static int mdbx_update_key(MDB_cursor *mc, MDB_val *key);

static void mdbx_cursor_pop(MDB_cursor *mc);
static int mdbx_cursor_push(MDB_cursor *mc, MDB_page *mp);

static int mdbx_cursor_del0(MDB_cursor *mc);
static int mdbx_del0(MDB_txn *txn, MDB_dbi dbi, MDB_val *key, MDB_val *data,
                     unsigned flags);
static int mdbx_cursor_sibling(MDB_cursor *mc, int move_right);
static int mdbx_cursor_next(MDB_cursor *mc, MDB_val *key, MDB_val *data,
                            MDB_cursor_op op);
static int mdbx_cursor_prev(MDB_cursor *mc, MDB_val *key, MDB_val *data,
                            MDB_cursor_op op);
static int mdbx_cursor_set(MDB_cursor *mc, MDB_val *key, MDB_val *data,
                           MDB_cursor_op op, int *exactp);
static int mdbx_cursor_first(MDB_cursor *mc, MDB_val *key, MDB_val *data);
static int mdbx_cursor_last(MDB_cursor *mc, MDB_val *key, MDB_val *data);

static void mdbx_cursor_init(MDB_cursor *mc, MDB_txn *txn, MDB_dbi dbi,
                             MDB_xcursor *mx);
static void mdbx_xcursor_init0(MDB_cursor *mc);
static void mdbx_xcursor_init1(MDB_cursor *mc, MDB_node *node);
static void mdbx_xcursor_init2(MDB_cursor *mc, MDB_xcursor *src_mx, int force);

static int mdbx_drop0(MDB_cursor *mc, int subs);

/** @cond */
static MDB_cmp_func mdbx_cmp_memn, mdbx_cmp_memnr, mdbx_cmp_int_ai,
    mdbx_cmp_int_a2, mdbx_cmp_int_ua;
/** @endcond */

/** Return the library version info. */
const char *mdbx_version(int *major, int *minor, int *patch) {
  if (major)
    *major = MDBX_VERSION_MAJOR;
  if (minor)
    *minor = MDBX_VERSION_MINOR;
  if (patch)
    *patch = MDBX_VERSION_PATCH;
  return MDBX_VERSION_STRING;
}

static const char *__mdbx_strerr(int errnum) {
  /* Table of descriptions for LMDB errors */
  static const char *const tbl[] = {
      "MDB_KEYEXIST: Key/data pair already exists",
      "MDB_NOTFOUND: No matching key/data pair found",
      "MDB_PAGE_NOTFOUND: Requested page not found",
      "MDB_CORRUPTED: Located page was wrong data",
      "MDB_PANIC: Update of meta page failed or environment had fatal error",
      "MDB_VERSION_MISMATCH: DB version mismatch libmdbx",
      "MDB_INVALID: File is not an LMDB file",
      "MDB_MAP_FULL: Environment mapsize limit reached",
      "MDB_DBS_FULL: Too may DBI (maxdbs reached)",
      "MDB_READERS_FULL: Too many readers (maxreaders reached)",
      NULL /* -30789 unused in MDBX */,
      "MDB_TXN_FULL: Transaction has too many dirty pages - transaction too "
      "big",
      "MDB_CURSOR_FULL: Internal error - cursor stack limit reached",
      "MDB_PAGE_FULL: Internal error - page has no more space",
      "MDB_MAP_RESIZED: Database contents grew beyond environment mapsize",
      "MDB_INCOMPATIBLE: Operation and DB incompatible, or DB flags changed",
      "MDB_BAD_RSLOT: Invalid reuse of reader locktable slot",
      "MDB_BAD_TXN: Transaction must abort, has a child, or is invalid",
      "MDB_BAD_VALSIZE: Unsupported size of key/DB name/data, or wrong "
      "DUPFIXED size",
      "MDB_BAD_DBI: The specified DBI handle was closed/changed unexpectedly",
      "MDB_PROBLEM: Unexpected problem - txn should abort",
  };

  if (errnum >= MDB_KEYEXIST && errnum <= MDB_LAST_ERRCODE) {
    int i = errnum - MDB_KEYEXIST;
    return tbl[i];
  }

  switch (errnum) {
  case MDB_SUCCESS:
    return "MDB_SUCCESS: Successful";
  case MDBX_EMULTIVAL:
    return "MDBX_EMULTIVAL: Unable to update multi-value for the given key";
  case MDBX_EBADSIGN:
    return "MDBX_EBADSIGN: Wrong signature of a runtime object(s)";
  default:
    return NULL;
  }
}

const char *__cold mdbx_strerror_r(int errnum, char *buf, size_t buflen) {
  const char *msg = __mdbx_strerr(errnum);
  if (!msg) {
    if (!buflen)
      return NULL;
#ifdef _MSC_VER
    size_t size = FormatMessageA(
        FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS, NULL,
        errnum, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), buf, buflen, NULL);
    return size ? buf : NULL;
#elif defined(_GNU_SOURCE)
    /* GNU-specific */
    msg = strerror_r(errnum, buf, buflen);
#elif (_POSIX_C_SOURCE >= 200112L || _XOPEN_SOURCE >= 600)
    /* XSI-compliant */
    int rc = strerror_r(errnum, buf, buflen);
    if (rc) {
      rc = snprintf(buf, buflen, "error %d", errnum);
      assert(rc > 0);
    }
    return buf;
#else
    strncpy(buf, strerror(errnum), buflen);
    buf[buflen - 1] = '\0';
    return buf;
#endif
  }
  return msg;
}

const char *__cold mdbx_strerror(int errnum) {
  const char *msg = __mdbx_strerr(errnum);
  if (!msg) {
#ifdef _MSC_VER
    static __thread char buffer[1024];
    size_t size = FormatMessageA(
        FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS, NULL,
        errnum, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), buffer,
        sizeof(buffer), NULL);
    if (size)
      msg = buffer;
#else
    msg = strerror(errnum);
#endif
  }
  return msg;
}

static txnid_t mdbx_oomkick(MDB_env *env, txnid_t oldest);

void __cold mdbx_debug_log(int type, const char *function, int line,
                           const char *fmt, ...) {
  va_list args;

  va_start(args, fmt);
  if (mdbx_debug_logger)
    mdbx_debug_logger(type, function, line, fmt, args);
  else {
    if (function && line > 0)
      fprintf(stderr, "%s:%d ", function, line);
    else if (function)
      fprintf(stderr, "%s: ", function);
    else if (line > 0)
      fprintf(stderr, "%d: ", line);
    vfprintf(stderr, fmt, args);
  }
  va_end(args);
}

/** Return the page number of \b mp which may be sub-page, for debug output */
static __inline pgno_t mdbx_dbg_pgno(MDB_page *mp) {
  pgno_t ret;
  COPY_PGNO(ret, mp->mp_pgno);
  return ret;
}

/** Display a key in hexadecimal and return the address of the result.
* @param[in] key the key to display
* @param[in] buf the buffer to write into. Should always be #DKBUF.
* @return The key in hexadecimal form.
*/
char *mdbx_dkey(MDB_val *key, char *buf, const size_t bufsize) {
#ifdef _MSC_VER
  (void)key;
  (void)buf;
  return "FIXME: mdbx_dkey()";
#else
  char *ptr = buf;
  unsigned i;

  if (!key)
    return "";

  const uint8_t *const data = key->mv_data;
  bool is_ascii = true;
  for (i = 0; is_ascii && i < key->mv_size; i++)
    if (data[i] < ' ' || data[i] > 127)
      is_ascii = false;

  if (is_ascii)
    snprintf(buf, bufsize, "%.*s",
             (key->mv_size > INT_MAX) ? INT_MAX : (int)key->mv_size, data);
  else {
    buf[0] = '\0';
    for (i = 0; i < key->mv_size; i++) {
      int len = snprintf(ptr, bufsize - (ptr - buf), "%02x", data[i]);
      if (len < 1)
        break;
      ptr += len;
    }
  }
  return buf;
#endif /* _MSC_VER */
}

#if 0  /* LY: debug stuff */
static const char *
mdbx_leafnode_type(MDB_node *n)
{
	static char *const tp[2][2] = {{"", ": DB"}, {": sub-page", ": sub-DB"}};
	return F_ISSET(n->mn_flags, F_BIGDATA) ? ": overflow page" :
		tp[F_ISSET(n->mn_flags, F_DUPDATA)][F_ISSET(n->mn_flags, F_SUBDATA)];
}

/** Display all the keys in the page. */
static void
mdbx_page_list(MDB_page *mp)
{
	pgno_t pgno = mdbx_dbg_pgno(mp);
	const char *type, *state = (mp->mp_flags & P_DIRTY) ? ", dirty" : "";
	MDB_node *node;
	unsigned i, nkeys, nsize, total = 0;
	MDB_val key;
	DKBUF;

	switch (mp->mp_flags & (P_BRANCH|P_LEAF|P_LEAF2|P_META|P_OVERFLOW|P_SUBP)) {
	case P_BRANCH:              type = "Branch page";		break;
	case P_LEAF:                type = "Leaf page";			break;
	case P_LEAF|P_SUBP:         type = "Sub-page";			break;
	case P_LEAF|P_LEAF2:        type = "LEAF2 page";		break;
	case P_LEAF|P_LEAF2|P_SUBP: type = "LEAF2 sub-page";	break;
	case P_OVERFLOW:
		mdbx_print("Overflow page %" PRIuPTR " pages %u%s\n",
			pgno, mp->mp_pages, state);
		return;
	case P_META:
		mdbx_print("Meta-page %" PRIuPTR " txnid %" PRIuPTR "\n",
			pgno, ((MDB_meta *)PAGEDATA(mp))->mm_txnid);
		return;
	default:
		mdbx_print("Bad page %" PRIuPTR " flags 0x%X\n", pgno, mp->mp_flags);
		return;
	}

	nkeys = NUMKEYS(mp);
	mdbx_print("%s %" PRIuPTR " numkeys %u%s\n", type, pgno, nkeys, state);

	for (i=0; i<nkeys; i++) {
		if (IS_LEAF2(mp)) {	/* LEAF2 pages have no mp_ptrs[] or node headers */
			key.mv_size = nsize = mp->mp_leaf2_ksize;
			key.mv_data = LEAF2KEY(mp, i, nsize);
			total += nsize;
			mdbx_print("key %u: nsize %u, %s\n", i, nsize, DKEY(&key));
			continue;
		}
		node = NODEPTR(mp, i);
		key.mv_size = node->mn_ksize;
		key.mv_data = node->mn_data;
		nsize = NODESIZE + key.mv_size;
		if (IS_BRANCH(mp)) {
			mdbx_print("key %u: page %" PRIuPTR ", %s\n", i, NODEPGNO(node), DKEY(&key));
			total += nsize;
		} else {
			if (F_ISSET(node->mn_flags, F_BIGDATA))
				nsize += sizeof(pgno_t);
			else
				nsize += NODEDSZ(node);
			total += nsize;
			nsize += sizeof(indx_t);
			mdbx_print("key %u: nsize %u, %s%s\n",
				i, nsize, DKEY(&key), mdbx_leafnode_type(node));
		}
		total = EVEN(total);
	}
	mdbx_print("Total: header %u + contents %u + unused %u\n",
		IS_LEAF2(mp) ? PAGEHDRSZ : PAGEBASE + mp->mp_lower, total, SIZELEFT(mp));
}

static void
mdbx_cursor_chk(MDB_cursor *mc)
{
	unsigned i;
	MDB_node *node;
	MDB_page *mp;

	if (!mc->mc_snum || !(mc->mc_flags & C_INITIALIZED)) return;
	for (i=0; i<mc->mc_top; i++) {
		mp = mc->mc_pg[i];
		node = NODEPTR(mp, mc->mc_ki[i]);
		if (unlikely(NODEPGNO(node) != mc->mc_pg[i+1]->mp_pgno))
			mdbx_print("oops!\n");
	}
	if (unlikely(mc->mc_ki[i] >= NUMKEYS(mc->mc_pg[i])))
		mdbx_print("ack!\n");
	if (XCURSOR_INITED(mc)) {
		node = NODEPTR(mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top]);
		if (((node->mn_flags & (F_DUPDATA|F_SUBDATA)) == F_DUPDATA) &&
			mc->mc_xcursor->mx_cursor.mc_pg[0] != NODEDATA(node)) {
			mdbx_print("blah!\n");
		}
	}
}
#endif /* 0 */

/** Count all the pages in each DB and in the freelist
 *  and make sure it matches the actual number of pages
 *  being used.
 *  All named DBs must be open for a correct count.
 */
static void mdbx_audit(MDB_txn *txn) {
  MDB_cursor mc;
  MDB_val key, data;
  MDB_ID freecount, count;
  MDB_dbi i;
  int rc;

  freecount = 0;
  mdbx_cursor_init(&mc, txn, FREE_DBI, NULL);
  while ((rc = mdbx_cursor_get(&mc, &key, &data, MDB_NEXT)) == 0)
    freecount += *(MDB_ID *)data.mv_data;
  mdbx_tassert(txn, rc == MDB_NOTFOUND);

  count = 0;
  for (i = 0; i < txn->mt_numdbs; i++) {
    MDB_xcursor mx;
    if (!(txn->mt_dbflags[i] & DB_VALID))
      continue;
    mdbx_cursor_init(&mc, txn, i, &mx);
    if (txn->mt_dbs[i].md_root == P_INVALID)
      continue;
    count += txn->mt_dbs[i].md_branch_pages + txn->mt_dbs[i].md_leaf_pages +
             txn->mt_dbs[i].md_overflow_pages;
    if (txn->mt_dbs[i].md_flags & MDB_DUPSORT) {
      rc = mdbx_page_search(&mc, NULL, MDB_PS_FIRST);
      for (; rc == MDB_SUCCESS; rc = mdbx_cursor_sibling(&mc, 1)) {
        unsigned j;
        MDB_page *mp;
        mp = mc.mc_pg[mc.mc_top];
        for (j = 0; j < NUMKEYS(mp); j++) {
          MDB_node *leaf = NODEPTR(mp, j);
          if (leaf->mn_flags & F_SUBDATA) {
            MDB_db db;
            memcpy(&db, NODEDATA(leaf), sizeof(db));
            count +=
                db.md_branch_pages + db.md_leaf_pages + db.md_overflow_pages;
          }
        }
      }
      mdbx_tassert(txn, rc == MDB_NOTFOUND);
    }
  }
  if (freecount + count + NUM_METAS != txn->mt_next_pgno) {
    mdbx_print(
        "audit: %lu freecount: %lu count: %lu total: %lu next_pgno: %lu\n",
        txn->mt_txnid, freecount, count + NUM_METAS,
        freecount + count + NUM_METAS, txn->mt_next_pgno);
  }
}

int mdbx_cmp(MDB_txn *txn, MDB_dbi dbi, const MDB_val *a, const MDB_val *b) {
  mdbx_ensure(NULL, txn->mt_signature == MDBX_MT_SIGNATURE);
  return txn->mt_dbxs[dbi].md_cmp(a, b);
}

int mdbx_dcmp(MDB_txn *txn, MDB_dbi dbi, const MDB_val *a, const MDB_val *b) {
  mdbx_ensure(NULL, txn->mt_signature == MDBX_MT_SIGNATURE);
  return txn->mt_dbxs[dbi].md_dcmp(a, b);
}

/** Allocate memory for a page.
 * Re-use old malloc'd pages first for singletons, otherwise just malloc.
 * Set #MDB_TXN_ERROR on failure.
 */
static MDB_page *mdbx_page_malloc(MDB_txn *txn, unsigned num) {
  MDB_env *env = txn->mt_env;
  size_t size = env->me_psize;
  MDB_page *np = env->me_dpages;
  if (likely(num == 1 && np)) {
    ASAN_UNPOISON_MEMORY_REGION(np, size);
    VALGRIND_MEMPOOL_ALLOC(env, np, size);
    VALGRIND_MAKE_MEM_DEFINED(&np->mp_next, sizeof(np->mp_next));
    env->me_dpages = np->mp_next;
  } else {
    size *= num;
    np = malloc(size);
    if (unlikely(!np)) {
      txn->mt_flags |= MDB_TXN_ERROR;
      return np;
    }
    VALGRIND_MEMPOOL_ALLOC(env, np, size);
  }

  if ((env->me_flags & MDB_NOMEMINIT) == 0) {
    /* For a single page alloc, we init everything after the page header.
     * For multi-page, we init the final page; if the caller needed that
     * many pages they will be filling in at least up to the last page. */
    size_t skip = PAGEHDRSZ;
    if (num > 1)
      skip += (num - 1) * env->me_psize;
    memset((char *)np + skip, 0, size - skip);
  }
  VALGRIND_MAKE_MEM_UNDEFINED(np, size);
  np->mp_flags = 0;
  np->mp_pages = num;
  return np;
}

/** Free a single page.
 * Saves single pages to a list, for future reuse.
 * (This is not used for multi-page overflow pages.)
 */
static __inline void mdbx_page_free(MDB_env *env, MDB_page *mp) {
  mp->mp_next = env->me_dpages;
  VALGRIND_MEMPOOL_FREE(env, mp);
  env->me_dpages = mp;
}

/** Free a dirty page */
static void mdbx_dpage_free(MDB_env *env, MDB_page *dp) {
  if (!IS_OVERFLOW(dp) || dp->mp_pages == 1) {
    mdbx_page_free(env, dp);
  } else {
    /* large pages just get freed directly */
    VALGRIND_MEMPOOL_FREE(env, dp);
    free(dp);
  }
}

/**	Return all dirty pages to dpage list */
static void mdbx_dlist_free(MDB_txn *txn) {
  MDB_env *env = txn->mt_env;
  MDB_ID2L dl = txn->mt_u.dirty_list;
  size_t i, n = dl[0].mid;

  for (i = 1; i <= n; i++) {
    mdbx_dpage_free(env, dl[i].mptr);
  }
  dl[0].mid = 0;
}

static void __cold mdbx_kill_page(MDB_env *env, pgno_t pgno) {
  const size_t offs = env->me_psize * pgno;
  const size_t shift = offsetof(MDB_page, mp_pb);

  if (env->me_flags & MDB_WRITEMAP) {
    MDB_page *mp = (MDB_page *)(env->me_map + offs);
    memset(&mp->mp_pb, 0x6F /* 'o', 111 */, env->me_psize - shift);
    VALGRIND_MAKE_MEM_NOACCESS(&mp->mp_pb, env->me_psize - shift);
    ASAN_POISON_MEMORY_REGION(&mp->mp_pb, env->me_psize - shift);
  } else {
    ssize_t len = env->me_psize - shift;
    void *buf = alloca(len);
    memset(buf, 0x6F /* 'o', 111 */, len);
    (void)mdbx_pwrite(env->me_fd, buf, len, offs + shift);
  }
}

/** Loosen or free a single page.
 * Saves single pages to a list for future reuse
 * in this same txn. It has been pulled from the freeDB
 * and already resides on the dirty list, but has been
 * deleted. Use these pages first before pulling again
 * from the freeDB.
 *
 * If the page wasn't dirtied in this txn, just add it
 * to this txn's free list.
 */
static int mdbx_page_loose(MDB_cursor *mc, MDB_page *mp) {
  int loose = 0;
  pgno_t pgno = mp->mp_pgno;
  MDB_txn *txn = mc->mc_txn;

  if ((mp->mp_flags & P_DIRTY) && mc->mc_dbi != FREE_DBI) {
    if (txn->mt_parent) {
      MDB_ID2 *dl = txn->mt_u.dirty_list;
      /* If txn has a parent, make sure the page is in our
       * dirty list. */
      if (dl[0].mid) {
        unsigned x = mdbx_mid2l_search(dl, pgno);
        if (x <= dl[0].mid && dl[x].mid == pgno) {
          if (unlikely(mp != dl[x].mptr)) { /* bad cursor? */
            mc->mc_flags &= ~(C_INITIALIZED | C_EOF);
            txn->mt_flags |= MDB_TXN_ERROR;
            return MDB_PROBLEM;
          }
          /* ok, it's ours */
          loose = 1;
        }
      }
    } else {
      /* no parent txn, so it's just ours */
      loose = 1;
    }
  }
  if (loose) {
    mdbx_debug("loosen db %d page %" PRIuPTR "", DDBI(mc), mp->mp_pgno);
    MDB_page **link = &NEXT_LOOSE_PAGE(mp);
    if (unlikely(txn->mt_env->me_flags & MDBX_PAGEPERTURB)) {
      mdbx_kill_page(txn->mt_env, pgno);
      VALGRIND_MAKE_MEM_UNDEFINED(link, sizeof(MDB_page *));
      ASAN_UNPOISON_MEMORY_REGION(link, sizeof(MDB_page *));
    }
    *link = txn->mt_loose_pgs;
    txn->mt_loose_pgs = mp;
    txn->mt_loose_count++;
    mp->mp_flags |= P_LOOSE;
  } else {
    int rc = mdbx_midl_append(&txn->mt_free_pgs, pgno);
    if (unlikely(rc))
      return rc;
  }

  return MDB_SUCCESS;
}

/** Set or clear P_KEEP in dirty, non-overflow, non-sub pages watched by txn.
 * @param[in] mc A cursor handle for the current operation.
 * @param[in] pflags Flags of the pages to update:
 * P_DIRTY to set P_KEEP, P_DIRTY|P_KEEP to clear it.
 * @param[in] all No shortcuts. Needed except after a full #mdbx_page_flush().
 * @return 0 on success, non-zero on failure.
 */
static int mdbx_pages_xkeep(MDB_cursor *mc, unsigned pflags, int all) {
  const unsigned Mask = P_SUBP | P_DIRTY | P_LOOSE | P_KEEP;
  MDB_txn *txn = mc->mc_txn;
  MDB_cursor *m3, *m0 = mc;
  MDB_xcursor *mx;
  MDB_page *dp, *mp;
  MDB_node *leaf;
  unsigned i, j;
  int rc = MDB_SUCCESS, level;

  /* Mark pages seen by cursors: First m0, then tracked cursors */
  for (i = txn->mt_numdbs;;) {
    if (mc->mc_flags & C_INITIALIZED) {
      for (m3 = mc;; m3 = &mx->mx_cursor) {
        mp = NULL;
        for (j = 0; j < m3->mc_snum; j++) {
          mp = m3->mc_pg[j];
          if ((mp->mp_flags & Mask) == pflags)
            mp->mp_flags ^= P_KEEP;
        }
        mx = m3->mc_xcursor;
        /* Proceed to mx if it is at a sub-database */
        if (!(mx && (mx->mx_cursor.mc_flags & C_INITIALIZED)))
          break;
        if (!(mp && (mp->mp_flags & P_LEAF)))
          break;
        leaf = NODEPTR(mp, m3->mc_ki[j - 1]);
        if (!(leaf->mn_flags & F_SUBDATA))
          break;
      }
    }
    mc = mc->mc_next;
    for (; !mc || mc == m0; mc = txn->mt_cursors[--i])
      if (i == 0)
        goto mark_done;
  }

mark_done:
  if (all) {
    /* Mark dirty root pages */
    for (i = 0; i < txn->mt_numdbs; i++) {
      if (txn->mt_dbflags[i] & DB_DIRTY) {
        pgno_t pgno = txn->mt_dbs[i].md_root;
        if (pgno == P_INVALID)
          continue;
        if (unlikely((rc = mdbx_page_get(m0, pgno, &dp, &level)) !=
                     MDB_SUCCESS))
          break;
        if ((dp->mp_flags & Mask) == pflags && level <= 1)
          dp->mp_flags ^= P_KEEP;
      }
    }
  }

  return rc;
}

static int mdbx_page_flush(MDB_txn *txn, int keep);

/**	Spill pages from the dirty list back to disk.
 * This is intended to prevent running into #MDB_TXN_FULL situations,
 * but note that they may still occur in a few cases:
 *	1) our estimate of the txn size could be too small. Currently this
 *	 seems unlikely, except with a large number of #MDB_MULTIPLE items.
 *	2) child txns may run out of space if their parents dirtied a
 *	 lot of pages and never spilled them. TODO: we probably should do
 *	 a preemptive spill during #mdbx_txn_begin() of a child txn, if
 *	 the parent's dirty_room is below a given threshold.
 *
 * Otherwise, if not using nested txns, it is expected that apps will
 * not run into #MDB_TXN_FULL any more. The pages are flushed to disk
 * the same way as for a txn commit, e.g. their P_DIRTY flag is cleared.
 * If the txn never references them again, they can be left alone.
 * If the txn only reads them, they can be used without any fuss.
 * If the txn writes them again, they can be dirtied immediately without
 * going thru all of the work of #mdbx_page_touch(). Such references are
 * handled by #mdbx_page_unspill().
 *
 * Also note, we never spill DB root pages, nor pages of active cursors,
 * because we'll need these back again soon anyway. And in nested txns,
 * we can't spill a page in a child txn if it was already spilled in a
 * parent txn. That would alter the parent txns' data even though
 * the child hasn't committed yet, and we'd have no way to undo it if
 * the child aborted.
 *
 * @param[in] m0 cursor A cursor handle identifying the transaction and
 *	database for which we are checking space.
 * @param[in] key For a put operation, the key being stored.
 * @param[in] data For a put operation, the data being stored.
 * @return 0 on success, non-zero on failure.
 */
static int mdbx_page_spill(MDB_cursor *m0, MDB_val *key, MDB_val *data) {
  MDB_txn *txn = m0->mc_txn;
  MDB_page *dp;
  MDB_ID2L dl = txn->mt_u.dirty_list;
  unsigned i, j, need;
  int rc;

  if (m0->mc_flags & C_SUB)
    return MDB_SUCCESS;

  /* Estimate how much space this op will take */
  i = m0->mc_db->md_depth;
  /* Named DBs also dirty the main DB */
  if (m0->mc_dbi >= CORE_DBS)
    i += txn->mt_dbs[MAIN_DBI].md_depth;
  /* For puts, roughly factor in the key+data size */
  if (key)
    i += (LEAFSIZE(key, data) + txn->mt_env->me_psize) / txn->mt_env->me_psize;
  i += i; /* double it for good measure */
  need = i;

  if (txn->mt_dirty_room > i)
    return MDB_SUCCESS;

  if (!txn->mt_spill_pgs) {
    txn->mt_spill_pgs = mdbx_midl_alloc(MDB_IDL_UM_MAX);
    if (unlikely(!txn->mt_spill_pgs))
      return MDBX_ENOMEM;
  } else {
    /* purge deleted slots */
    MDB_IDL sl = txn->mt_spill_pgs;
    unsigned num = sl[0];
    j = 0;
    for (i = 1; i <= num; i++) {
      if (!(sl[i] & 1))
        sl[++j] = sl[i];
    }
    sl[0] = j;
  }

  /* Preserve pages which may soon be dirtied again */
  rc = mdbx_pages_xkeep(m0, P_DIRTY, 1);
  if (unlikely(rc != MDB_SUCCESS))
    goto bailout;

  /* Less aggressive spill - we originally spilled the entire dirty list,
   * with a few exceptions for cursor pages and DB root pages. But this
   * turns out to be a lot of wasted effort because in a large txn many
   * of those pages will need to be used again. So now we spill only 1/8th
   * of the dirty pages. Testing revealed this to be a good tradeoff,
   * better than 1/2, 1/4, or 1/10. */
  if (need < MDB_IDL_UM_MAX / 8)
    need = MDB_IDL_UM_MAX / 8;

  /* Save the page IDs of all the pages we're flushing */
  /* flush from the tail forward, this saves a lot of shifting later on. */
  for (i = dl[0].mid; i && need; i--) {
    MDB_ID pn = dl[i].mid << 1;
    dp = dl[i].mptr;
    if (dp->mp_flags & (P_LOOSE | P_KEEP))
      continue;
    /* Can't spill twice, make sure it's not already in a parent's
     * spill list. */
    if (txn->mt_parent) {
      MDB_txn *tx2;
      for (tx2 = txn->mt_parent; tx2; tx2 = tx2->mt_parent) {
        if (tx2->mt_spill_pgs) {
          j = mdbx_midl_search(tx2->mt_spill_pgs, pn);
          if (j <= tx2->mt_spill_pgs[0] && tx2->mt_spill_pgs[j] == pn) {
            dp->mp_flags |= P_KEEP;
            break;
          }
        }
      }
      if (tx2)
        continue;
    }
    rc = mdbx_midl_append(&txn->mt_spill_pgs, pn);
    if (unlikely(rc != MDB_SUCCESS))
      goto bailout;
    need--;
  }
  mdbx_midl_sort(txn->mt_spill_pgs);

  /* Flush the spilled part of dirty list */
  rc = mdbx_page_flush(txn, i);
  if (unlikely(rc != MDB_SUCCESS))
    goto bailout;

  /* Reset any dirty pages we kept that page_flush didn't see */
  rc = mdbx_pages_xkeep(m0, P_DIRTY | P_KEEP, i);

bailout:
  txn->mt_flags |= rc ? MDB_TXN_ERROR : MDB_TXN_SPILLS;
  return rc;
}

static __inline uint64_t mdbx_meta_sign(MDB_meta *meta) {
  uint64_t sign = MDB_DATASIGN_NONE;
#if 0 /* TODO */
	sign = hippeus_hash64(
				&meta->mm_mapsize,
				sizeof(MDB_meta) - offsetof(MDB_meta, mm_mapsize),
				meta->mm_version | (uint64_t) MDB_MAGIC << 32
			);
#else
  (void)meta;
#endif
  /* LY: newer returns MDB_DATASIGN_NONE or MDB_DATASIGN_WEAK */
  return (sign > MDB_DATASIGN_WEAK) ? sign : ~sign;
}

static __inline MDB_meta *mdbx_env_meta_flipflop(const MDB_env *env,
                                                 MDB_meta *meta) {
  return (meta == METAPAGE_1(env)) ? METAPAGE_2(env) : METAPAGE_1(env);
}

static __inline int mdbx_meta_lt(const MDB_meta *a, const MDB_meta *b) {
  return (META_IS_STEADY(a) == META_IS_STEADY(b)) ? a->mm_txnid < b->mm_txnid
                                                  : META_IS_STEADY(b);
}

/** Find oldest txnid still referenced. */
static txnid_t mdbx_find_oldest(MDB_env *env, int *laggard) {
  const MDB_meta *const a = METAPAGE_1(env);
  const MDB_meta *const b = METAPAGE_2(env);
  txnid_t oldest = mdbx_meta_lt(a, b) ? b->mm_txnid : a->mm_txnid;

  int i, reader;
  const MDB_reader *const r = env->me_lck->mti_readers;
  for (reader = -1, i = env->me_lck->mti_numreaders; --i >= 0;) {
    if (r[i].mr_pid) {
      mdbx_jitter4testing(true);
      txnid_t snap = r[i].mr_txnid;
      if (oldest > snap) {
        oldest = snap;
        reader = i;
      }
    }
  }

  if (laggard)
    *laggard = reader;
  return env->me_pgoldest = oldest;
}

/** Add a page to the txn's dirty list */
static void mdbx_page_dirty(MDB_txn *txn, MDB_page *mp) {
  MDB_ID2 mid;
  int rc, (*insert)(MDB_ID2L, MDB_ID2 *);

  if (txn->mt_flags & MDB_TXN_WRITEMAP) {
    insert = mdbx_mid2l_append;
  } else {
    insert = mdbx_mid2l_insert;
  }
  mid.mid = mp->mp_pgno;
  mid.mptr = mp;
  rc = insert(txn->mt_u.dirty_list, &mid);
  mdbx_tassert(txn, rc == 0);
  txn->mt_dirty_room--;
}

/** Allocate page numbers and memory for writing.  Maintain me_pglast,
 * me_pghead and mt_next_pgno.  Set #MDB_TXN_ERROR on failure.
 *
 * If there are free pages available from older transactions, they
 * are re-used first. Otherwise allocate a new page at mt_next_pgno.
 * Do not modify the freedB, just merge freeDB records into me_pghead[]
 * and move me_pglast to say which records were consumed.  Only this
 * function can create me_pghead and move me_pglast/mt_next_pgno.
 * @param[in] mc cursor A cursor handle identifying the transaction and
 *	database for which we are allocating.
 * @param[in] num the number of pages to allocate.
 * @param[out] mp Address of the allocated page(s). Requests for multiple
 *pages
 *  will always be satisfied by a single contiguous chunk of memory.
 * @return 0 on success, non-zero on failure.
 */

#define MDBX_ALLOC_CACHE 1
#define MDBX_ALLOC_GC 2
#define MDBX_ALLOC_NEW 4
#define MDBX_ALLOC_KICK 8
#define MDBX_ALLOC_ALL                                                         \
  (MDBX_ALLOC_CACHE | MDBX_ALLOC_GC | MDBX_ALLOC_NEW | MDBX_ALLOC_KICK)

static int mdbx_page_alloc(MDB_cursor *mc, int num, MDB_page **mp, int flags) {
  int rc;
  MDB_txn *txn = mc->mc_txn;
  MDB_env *env = txn->mt_env;
  pgno_t pgno, *mop = env->me_pghead;
  unsigned i = 0, j, mop_len = mop ? mop[0] : 0, n2 = num - 1;
  MDB_page *np;
  txnid_t oldest = 0, last = 0;
  MDB_cursor_op op;
  MDB_cursor m2;
  int found_oldest = 0;

  if (likely(flags & MDBX_ALLOC_GC)) {
    flags |= env->me_flags & (MDBX_COALESCE | MDBX_LIFORECLAIM);
    if (unlikely(mc->mc_flags & C_RECLAIMING)) {
      /* If mc is updating the freeDB, then the freelist cannot play
       * catch-up with itself by growing while trying to save it. */
      flags &=
          ~(MDBX_ALLOC_GC | MDBX_ALLOC_KICK | MDBX_COALESCE | MDBX_LIFORECLAIM);
    }
  }

  if (likely(flags & MDBX_ALLOC_CACHE)) {
    /* If there are any loose pages, just use them */
    assert(mp && num);
    if (likely(num == 1 && txn->mt_loose_pgs)) {
      np = txn->mt_loose_pgs;
      txn->mt_loose_pgs = NEXT_LOOSE_PAGE(np);
      txn->mt_loose_count--;
      mdbx_debug("db %d use loose page %" PRIuPTR "", DDBI(mc), np->mp_pgno);
      ASAN_UNPOISON_MEMORY_REGION(np, env->me_psize);
      *mp = np;
      return MDB_SUCCESS;
    }
  }

  /* If our dirty list is already full, we can't do anything */
  if (unlikely(txn->mt_dirty_room == 0)) {
    rc = MDB_TXN_FULL;
    goto fail;
  }

  for (;;) { /* oom-kick retry loop */
    for (op = MDB_FIRST;;
         op = (flags & MDBX_LIFORECLAIM) ? MDB_PREV : MDB_NEXT) {
      MDB_val key, data;
      MDB_node *leaf;
      pgno_t *idl;

      /* Seek a big enough contiguous page range. Prefer
       * pages at the tail, just truncating the list. */
      if (likely(flags & MDBX_ALLOC_CACHE) && mop_len > n2 &&
          (!(flags & MDBX_COALESCE) || op == MDB_FIRST)) {
        i = mop_len;
        do {
          pgno = mop[i];
          if (likely(mop[i - n2] == pgno + n2))
            goto done;
        } while (--i > n2);
      }

      if (op == MDB_FIRST) { /* 1st iteration */
        /* Prepare to fetch more and coalesce */
        if (unlikely(!(flags & MDBX_ALLOC_GC)))
          break;

        oldest = env->me_pgoldest;
        mdbx_cursor_init(&m2, txn, FREE_DBI, NULL);
        if (flags & MDBX_LIFORECLAIM) {
          if (!found_oldest) {
            oldest = mdbx_find_oldest(env, NULL);
            found_oldest = 1;
          }
          /* Begin from oldest reader if any */
          if (oldest > 2) {
            last = oldest - 1;
            op = MDB_SET_RANGE;
          }
        } else if (env->me_pglast) {
          /* Continue lookup from env->me_pglast to higher/last */
          last = env->me_pglast;
          op = MDB_SET_RANGE;
        }

        key.mv_data = &last;
        key.mv_size = sizeof(last);
      }

      if (!(flags & MDBX_LIFORECLAIM)) {
        /* Do not fetch more if the record will be too recent */
        if (op != MDB_FIRST && ++last >= oldest) {
          if (!found_oldest) {
            oldest = mdbx_find_oldest(env, NULL);
            found_oldest = 1;
          }
          if (oldest <= last)
            break;
        }
      }

      rc = mdbx_cursor_get(&m2, &key, NULL, op);
      if (rc == MDB_NOTFOUND && (flags & MDBX_LIFORECLAIM)) {
        if (op == MDB_SET_RANGE)
          continue;
        found_oldest = 1;
        if (oldest < mdbx_find_oldest(env, NULL)) {
          oldest = env->me_pgoldest;
          last = oldest - 1;
          key.mv_data = &last;
          key.mv_size = sizeof(last);
          op = MDB_SET_RANGE;
          rc = mdbx_cursor_get(&m2, &key, NULL, op);
        }
      }
      if (unlikely(rc)) {
        if (rc == MDB_NOTFOUND)
          break;
        goto fail;
      }

      last = *(txnid_t *)key.mv_data;
      if (oldest <= last) {
        if (!found_oldest) {
          oldest = mdbx_find_oldest(env, NULL);
          found_oldest = 1;
        }
        if (oldest <= last) {
          if (flags & MDBX_LIFORECLAIM)
            continue;
          break;
        }
      }

      if (flags & MDBX_LIFORECLAIM) {
        if (txn->mt_lifo_reclaimed) {
          for (j = txn->mt_lifo_reclaimed[0]; j > 0; --j)
            if (txn->mt_lifo_reclaimed[j] == last)
              break;
          if (j)
            continue;
        }
      }

      np = m2.mc_pg[m2.mc_top];
      leaf = NODEPTR(np, m2.mc_ki[m2.mc_top]);
      if (unlikely((rc = mdbx_node_read(&m2, leaf, &data)) != MDB_SUCCESS))
        goto fail;

      if ((flags & MDBX_LIFORECLAIM) && !txn->mt_lifo_reclaimed) {
        txn->mt_lifo_reclaimed = mdbx_midl_alloc(env->me_maxfree_1pg);
        if (unlikely(!txn->mt_lifo_reclaimed)) {
          rc = MDBX_ENOMEM;
          goto fail;
        }
      }

      idl = (MDB_ID *)data.mv_data;
      mdbx_tassert(txn, idl[0] == 0 ||
                            data.mv_size == (idl[0] + 1) * sizeof(MDB_ID));
      i = idl[0];
      if (!mop) {
        if (unlikely(!(env->me_pghead = mop = mdbx_midl_alloc(i)))) {
          rc = MDBX_ENOMEM;
          goto fail;
        }
      } else {
        if (unlikely((rc = mdbx_midl_need(&env->me_pghead, i)) != 0))
          goto fail;
        mop = env->me_pghead;
      }
      if (flags & MDBX_LIFORECLAIM) {
        if ((rc = mdbx_midl_append(&txn->mt_lifo_reclaimed, last)) != 0)
          goto fail;
      }
      env->me_pglast = last;

      if (mdbx_debug_enabled(MDBX_DBG_EXTRA)) {
        mdbx_debug_extra("IDL read txn %" PRIuPTR " root %" PRIuPTR
                         " num %u, IDL",
                         last, txn->mt_dbs[FREE_DBI].md_root, i);
        for (j = i; j; j--)
          mdbx_debug_extra_print(" %" PRIuPTR "", idl[j]);
        mdbx_debug_extra_print("\n");
      }

      /* Merge in descending sorted order */
      mdbx_midl_xmerge(mop, idl);
      mop_len = mop[0];

      if (unlikely((flags & MDBX_ALLOC_CACHE) == 0)) {
        /* force gc reclaim mode */
        return MDB_SUCCESS;
      }

      /* Don't try to coalesce too much. */
      if (mop_len > MDB_IDL_UM_SIZE / 2)
        break;
      if (flags & MDBX_COALESCE) {
        if (mop_len /* current size */ >= env->me_maxfree_1pg / 2 ||
            i /* prev size */ >= env->me_maxfree_1pg / 4)
          flags &= ~MDBX_COALESCE;
      }
    }

    if ((flags & (MDBX_COALESCE | MDBX_ALLOC_CACHE)) ==
            (MDBX_COALESCE | MDBX_ALLOC_CACHE) &&
        mop_len > n2) {
      i = mop_len;
      do {
        pgno = mop[i];
        if (mop[i - n2] == pgno + n2)
          goto done;
      } while (--i > n2);
    }

    /* Use new pages from the map when nothing suitable in the freeDB */
    i = 0;
    pgno = txn->mt_next_pgno;
    rc = MDB_MAP_FULL;
    if (likely(pgno + num <= env->me_maxpg)) {
      rc = MDB_NOTFOUND;
      if (likely(flags & MDBX_ALLOC_NEW))
        goto done;
    }

    if ((flags & MDBX_ALLOC_GC) &&
        ((flags & MDBX_ALLOC_KICK) || rc == MDB_MAP_FULL)) {
      MDB_meta *head = mdbx_meta_head(env);
      MDB_meta *tail = mdbx_env_meta_flipflop(env, head);

      if (oldest == tail->mm_txnid && META_IS_WEAK(head) &&
          !META_IS_WEAK(tail)) {
        MDB_meta meta = *head;
        /* LY: Here an oom was happened:
         *  - all pages had allocated;
         *  - reclaiming was stopped at the last steady-sync;
         *  - the head-sync is weak.
         * Now we need make a sync to resume reclaiming. If both
         * MDB_NOSYNC and MDB_MAPASYNC flags are set, then assume that
         * utterly no-sync write mode was requested. In such case
         * don't make a steady-sync, but only a legacy-mode checkpoint,
         * just for resume reclaiming only, not for data consistency. */

        mdbx_debug("kick-gc: head %" PRIuPTR "/%c, tail %" PRIuPTR
                   "/%c, oldest %" PRIuPTR "",
                   head->mm_txnid, META_IS_WEAK(head) ? 'W' : 'N',
                   tail->mm_txnid, META_IS_WEAK(tail) ? 'W' : 'N', oldest);

        int me_flags = env->me_flags & MDB_WRITEMAP;
        if ((env->me_flags & MDBX_UTTERLY_NOSYNC) == MDBX_UTTERLY_NOSYNC)
          me_flags |= MDBX_UTTERLY_NOSYNC;

        mdbx_assert(env, env->me_sync_pending > 0);
        if (mdbx_env_sync0(env, me_flags, &meta) == MDB_SUCCESS) {
          txnid_t snap = mdbx_find_oldest(env, NULL);
          if (snap > oldest) {
            continue;
          }
        }
      }

      if (rc == MDB_MAP_FULL) {
        txnid_t snap = mdbx_oomkick(env, oldest);
        if (snap > oldest) {
          oldest = snap;
          continue;
        }
      }
    }

  fail:
    if (mp) {
      *mp = NULL;
      txn->mt_flags |= MDB_TXN_ERROR;
    }
    assert(rc);
    return rc;
  }

done:
  assert(mp && num);
  if (env->me_flags & MDB_WRITEMAP) {
    np = (MDB_page *)(env->me_map + env->me_psize * pgno);
    /* LY: reset no-access flag from mdbx_kill_page() */
    VALGRIND_MAKE_MEM_UNDEFINED(np, env->me_psize * num);
    ASAN_UNPOISON_MEMORY_REGION(np, env->me_psize * num);
  } else {
    if (unlikely(!(np = mdbx_page_malloc(txn, num)))) {
      rc = MDBX_ENOMEM;
      goto fail;
    }
  }
  if (i) {
    mop[0] = mop_len -= num;
    /* Move any stragglers down */
    for (j = i - num; j < mop_len;)
      mop[++j] = mop[++i];
  } else {
    txn->mt_next_pgno = pgno + num;
  }

  if (env->me_flags & MDBX_PAGEPERTURB)
    memset(np, 0x71 /* 'q', 113 */, env->me_psize * num);
  VALGRIND_MAKE_MEM_UNDEFINED(np, env->me_psize * num);

  np->mp_pgno = pgno;
  np->mp_leaf2_ksize = 0;
  np->mp_flags = 0;
  np->mp_pages = num;
  mdbx_page_dirty(txn, np);
  *mp = np;

  return MDB_SUCCESS;
}

/** Copy the used portions of a non-overflow page.
 * @param[in] dst page to copy into
 * @param[in] src page to copy from
 * @param[in] psize size of a page
 */
static void mdbx_page_copy(MDB_page *dst, MDB_page *src, unsigned psize) {
  enum { Align = sizeof(pgno_t) };
  indx_t upper = src->mp_upper, lower = src->mp_lower, unused = upper - lower;

  /* If page isn't full, just copy the used portion. Adjust
   * alignment so memcpy may copy words instead of bytes. */
  if ((unused &= -Align) && !IS_LEAF2(src)) {
    upper = (upper + PAGEBASE) & -Align;
    memcpy(dst, src, (lower + PAGEBASE + (Align - 1)) & -Align);
    memcpy((pgno_t *)((char *)dst + upper), (pgno_t *)((char *)src + upper),
           psize - upper);
  } else {
    memcpy(dst, src, psize - unused);
  }
}

/** Pull a page off the txn's spill list, if present.
 * If a page being referenced was spilled to disk in this txn, bring
 * it back and make it dirty/writable again.
 * @param[in] txn the transaction handle.
 * @param[in] mp the page being referenced. It must not be dirty.
 * @param[out] ret the writable page, if any. ret is unchanged if
 * mp wasn't spilled.
 */
static int mdbx_page_unspill(MDB_txn *txn, MDB_page *mp, MDB_page **ret) {
  MDB_env *env = txn->mt_env;
  const MDB_txn *tx2;
  unsigned x;
  pgno_t pgno = mp->mp_pgno, pn = pgno << 1;

  for (tx2 = txn; tx2; tx2 = tx2->mt_parent) {
    if (!tx2->mt_spill_pgs)
      continue;
    x = mdbx_midl_search(tx2->mt_spill_pgs, pn);
    if (x <= tx2->mt_spill_pgs[0] && tx2->mt_spill_pgs[x] == pn) {
      MDB_page *np;
      int num;
      if (txn->mt_dirty_room == 0)
        return MDB_TXN_FULL;
      if (IS_OVERFLOW(mp))
        num = mp->mp_pages;
      else
        num = 1;
      if (env->me_flags & MDB_WRITEMAP) {
        np = mp;
      } else {
        np = mdbx_page_malloc(txn, num);
        if (unlikely(!np))
          return MDBX_ENOMEM;
        if (num > 1)
          memcpy(np, mp, num * env->me_psize);
        else
          mdbx_page_copy(np, mp, env->me_psize);
      }
      if (tx2 == txn) {
        /* If in current txn, this page is no longer spilled.
         * If it happens to be the last page, truncate the spill list.
         * Otherwise mark it as deleted by setting the LSB. */
        if (x == txn->mt_spill_pgs[0])
          txn->mt_spill_pgs[0]--;
        else
          txn->mt_spill_pgs[x] |= 1;
      } /* otherwise, if belonging to a parent txn, the
         * page remains spilled until child commits
         */

      mdbx_page_dirty(txn, np);
      np->mp_flags |= P_DIRTY;
      *ret = np;
      break;
    }
  }
  return MDB_SUCCESS;
}

/** Touch a page: make it dirty and re-insert into tree with updated pgno.
 * Set #MDB_TXN_ERROR on failure.
 * @param[in] mc cursor pointing to the page to be touched
 * @return 0 on success, non-zero on failure.
 */
static int mdbx_page_touch(MDB_cursor *mc) {
  MDB_page *mp = mc->mc_pg[mc->mc_top], *np;
  MDB_txn *txn = mc->mc_txn;
  MDB_cursor *m2, *m3;
  pgno_t pgno;
  int rc;

  if (!F_ISSET(mp->mp_flags, P_DIRTY)) {
    if (txn->mt_flags & MDB_TXN_SPILLS) {
      np = NULL;
      rc = mdbx_page_unspill(txn, mp, &np);
      if (unlikely(rc))
        goto fail;
      if (likely(np))
        goto done;
    }
    if (unlikely((rc = mdbx_midl_need(&txn->mt_free_pgs, 1)) ||
                 (rc = mdbx_page_alloc(mc, 1, &np, MDBX_ALLOC_ALL))))
      goto fail;
    pgno = np->mp_pgno;
    mdbx_debug("touched db %d page %" PRIuPTR " -> %" PRIuPTR "", DDBI(mc),
               mp->mp_pgno, pgno);
    mdbx_cassert(mc, mp->mp_pgno != pgno);
    mdbx_midl_xappend(txn->mt_free_pgs, mp->mp_pgno);
    /* Update the parent page, if any, to point to the new page */
    if (mc->mc_top) {
      MDB_page *parent = mc->mc_pg[mc->mc_top - 1];
      MDB_node *node = NODEPTR(parent, mc->mc_ki[mc->mc_top - 1]);
      SETPGNO(node, pgno);
    } else {
      mc->mc_db->md_root = pgno;
    }
  } else if (txn->mt_parent && !IS_SUBP(mp)) {
    MDB_ID2 mid, *dl = txn->mt_u.dirty_list;
    pgno = mp->mp_pgno;
    /* If txn has a parent, make sure the page is in our
     * dirty list. */
    if (dl[0].mid) {
      unsigned x = mdbx_mid2l_search(dl, pgno);
      if (x <= dl[0].mid && dl[x].mid == pgno) {
        if (unlikely(mp != dl[x].mptr)) { /* bad cursor? */
          mc->mc_flags &= ~(C_INITIALIZED | C_EOF);
          txn->mt_flags |= MDB_TXN_ERROR;
          return MDB_PROBLEM;
        }
        return 0;
      }
    }
    mdbx_cassert(mc, dl[0].mid < MDB_IDL_UM_MAX);
    /* No - copy it */
    np = mdbx_page_malloc(txn, 1);
    if (unlikely(!np))
      return MDBX_ENOMEM;
    mid.mid = pgno;
    mid.mptr = np;
    rc = mdbx_mid2l_insert(dl, &mid);
    mdbx_cassert(mc, rc == 0);
  } else {
    return 0;
  }

  mdbx_page_copy(np, mp, txn->mt_env->me_psize);
  np->mp_pgno = pgno;
  np->mp_flags |= P_DIRTY;

done:
  /* Adjust cursors pointing to mp */
  mc->mc_pg[mc->mc_top] = np;
  m2 = txn->mt_cursors[mc->mc_dbi];
  if (mc->mc_flags & C_SUB) {
    for (; m2; m2 = m2->mc_next) {
      m3 = &m2->mc_xcursor->mx_cursor;
      if (m3->mc_snum < mc->mc_snum)
        continue;
      if (m3->mc_pg[mc->mc_top] == mp)
        m3->mc_pg[mc->mc_top] = np;
    }
  } else {
    for (; m2; m2 = m2->mc_next) {
      if (m2->mc_snum < mc->mc_snum)
        continue;
      if (m2 == mc)
        continue;
      if (m2->mc_pg[mc->mc_top] == mp) {
        m2->mc_pg[mc->mc_top] = np;
        if (XCURSOR_INITED(m2) && IS_LEAF(np))
          XCURSOR_REFRESH(m2, np, m2->mc_ki[mc->mc_top]);
      }
    }
  }
  return 0;

fail:
  txn->mt_flags |= MDB_TXN_ERROR;
  return rc;
}

int mdbx_env_sync(MDB_env *env, int force) {
  int rc;
  MDB_meta *head;
  unsigned flags;

  if (unlikely(!env))
    return MDBX_EINVAL;

  if (unlikely(env->me_signature != MDBX_ME_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(!env->me_lck))
    return MDB_PANIC;

  flags = env->me_flags & ~MDB_NOMETASYNC;
  if (unlikely(flags & (MDB_RDONLY | MDB_FATAL_ERROR)))
    return MDBX_EACCESS;

  head = mdbx_meta_head(env);
  if (!META_IS_WEAK(head) && env->me_sync_pending == 0 &&
      env->me_mapsize == head->mm_mapsize)
    /* LY: nothing to do */
    return MDB_SUCCESS;

  if (force || head->mm_mapsize != env->me_mapsize ||
      (env->me_sync_threshold &&
       env->me_sync_pending >= env->me_sync_threshold))
    flags &= MDB_WRITEMAP;

  /* LY: early sync before acquiring the mutex to reduce writer's latency */
  if (env->me_sync_pending > env->me_psize * 16 && (flags & MDB_NOSYNC) == 0) {
    assert(((flags ^ env->me_flags) & MDB_WRITEMAP) == 0);
    if (flags & MDB_WRITEMAP) {
      size_t used_size = env->me_psize * (head->mm_last_pg + 1);
      rc = mdbx_msync(env->me_map, used_size, flags & MDB_MAPASYNC);
    } else {
      rc = mdbx_filesync(env->me_fd, false);
    }
    if (unlikely(rc != MDB_SUCCESS))
      return rc;
  }

  rc = mdbx_txn_lock(env);
  if (unlikely(rc != MDB_SUCCESS))
    return rc;

  /* LY: head may be changed while the mutex has been acquired. */
  head = mdbx_meta_head(env);
  rc = MDB_SUCCESS;
  if (META_IS_WEAK(head) || env->me_sync_pending != 0 ||
      env->me_mapsize != head->mm_mapsize) {
    MDB_meta meta = *head;
    rc = mdbx_env_sync0(env, flags, &meta);
  }

  mdbx_txn_unlock(env);
  return rc;
}

/** Back up parent txn's cursors, then grab the originals for tracking */
static int mdbx_cursor_shadow(MDB_txn *src, MDB_txn *dst) {
  MDB_cursor *mc, *bk;
  MDB_xcursor *mx;
  size_t size;
  int i;

  for (i = src->mt_numdbs; --i >= 0;) {
    if ((mc = src->mt_cursors[i]) != NULL) {
      size = sizeof(MDB_cursor);
      if (mc->mc_xcursor)
        size += sizeof(MDB_xcursor);
      for (; mc; mc = bk->mc_next) {
        bk = malloc(size);
        if (unlikely(!bk))
          return MDBX_ENOMEM;
        *bk = *mc;
        mc->mc_backup = bk;
        mc->mc_db = &dst->mt_dbs[i];
        /* Kill pointers into src to reduce abuse: The
         * user may not use mc until dst ends. But we need a valid
         * txn pointer here for cursor fixups to keep working. */
        mc->mc_txn = dst;
        mc->mc_dbflag = &dst->mt_dbflags[i];
        if ((mx = mc->mc_xcursor) != NULL) {
          *(MDB_xcursor *)(bk + 1) = *mx;
          mx->mx_cursor.mc_txn = dst;
        }
        mc->mc_next = dst->mt_cursors[i];
        dst->mt_cursors[i] = mc;
      }
    }
  }
  return MDB_SUCCESS;
}

/** Close this write txn's cursors, give parent txn's cursors back to parent.
 * @param[in] txn the transaction handle.
 * @param[in] merge true to keep changes to parent cursors, false to revert.
 * @return 0 on success, non-zero on failure.
 */
static void mdbx_cursors_eot(MDB_txn *txn, unsigned merge) {
  MDB_cursor **cursors = txn->mt_cursors, *mc, *next, *bk;
  MDB_xcursor *mx;
  int i;

  for (i = txn->mt_numdbs; --i >= 0;) {
    for (mc = cursors[i]; mc; mc = next) {
      unsigned stage = mc->mc_signature;
      mdbx_ensure(NULL,
                  stage == MDBX_MC_SIGNATURE || stage == MDBX_MC_WAIT4EOT);
      next = mc->mc_next;
      if ((bk = mc->mc_backup) != NULL) {
        if (merge) {
          /* Commit changes to parent txn */
          mc->mc_next = bk->mc_next;
          mc->mc_backup = bk->mc_backup;
          mc->mc_txn = bk->mc_txn;
          mc->mc_db = bk->mc_db;
          mc->mc_dbflag = bk->mc_dbflag;
          if ((mx = mc->mc_xcursor) != NULL)
            mx->mx_cursor.mc_txn = bk->mc_txn;
        } else {
          /* Abort nested txn */
          *mc = *bk;
          if ((mx = mc->mc_xcursor) != NULL)
            *mx = *(MDB_xcursor *)(bk + 1);
        }
        bk->mc_signature = 0;
        free(bk);
      }
      if (stage == MDBX_MC_WAIT4EOT) {
        mc->mc_signature = 0;
        free(mc);
      } else {
        mc->mc_signature = MDBX_MC_READY4CLOSE;
        mc->mc_flags = 0 /* reset C_UNTRACK */;
      }
    }
    cursors[i] = NULL;
  }
}

/* Common code for #mdbx_txn_begin() and #mdbx_txn_renew(). */
static int mdbx_txn_renew0(MDB_txn *txn, unsigned flags) {
  MDB_env *env = txn->mt_env;
  unsigned i, nr;
  int rc;

  if (unlikely(env->me_pid != mdbx_getpid())) {
    env->me_flags |= MDB_FATAL_ERROR;
    return MDB_PANIC;
  }

  if (flags & MDB_TXN_RDONLY) {
    txn->mt_flags = MDB_TXN_RDONLY;
    MDB_reader *r = txn->mt_u.reader;
    if (likely(env->me_flags & MDB_ENV_TXKEY)) {
      mdbx_assert(env, !(env->me_flags & MDB_NOTLS));
      r = mdbx_thread_rthc_get(env->me_txkey);
      if (likely(r)) {
        mdbx_assert(env, r->mr_pid == env->me_pid);
        mdbx_assert(env, r->mr_tid == mdbx_thread_self());
      }
    } else {
      mdbx_assert(env, env->me_flags & MDB_NOTLS);
    }

    if (likely(r)) {
      if (unlikely(r->mr_pid != env->me_pid || r->mr_txnid != ~(txnid_t)0))
        return MDB_BAD_RSLOT;
    } else {
      mdbx_pid_t pid = env->me_pid;
      mdbx_tid_t tid = mdbx_thread_self();

      rc = mdbx_rdt_lock(env);
      if (unlikely(MDBX_IS_ERROR(rc)))
        return rc;
      rc = MDB_SUCCESS;

      if (unlikely(env->me_live_reader != pid)) {
        rc = mdbx_rpid_set(env);
        if (unlikely(rc != MDB_SUCCESS)) {
          mdbx_rdt_unlock(env);
          return rc;
        }
        env->me_live_reader = pid;
      }

      for (;;) {
        nr = env->me_lck->mti_numreaders;
        for (i = 0; i < nr; i++)
          if (env->me_lck->mti_readers[i].mr_pid == 0)
            break;

        if (likely(i < env->me_maxreaders))
          break;

        rc = mdbx_reader_check0(env, 1, NULL);
        if (rc != MDBX_RESULT_TRUE) {
          mdbx_rdt_unlock(env);
          return (rc == MDB_SUCCESS) ? MDB_READERS_FULL : rc;
        }
      }

      r = &env->me_lck->mti_readers[i];
      /* Claim the reader slot, carefully since other code
       * uses the reader table un-mutexed: First reset the
       * slot, next publish it in mtb.mti_numreaders.  After
       * that, it is safe for mdbx_env_close() to touch it.
       * When it will be closed, we can finally claim it. */
      r->mr_pid = 0;
      r->mr_txnid = ~(txnid_t)0;
      r->mr_tid = tid;
      mdbx_coherent_barrier();
      if (i == nr)
        env->me_lck->mti_numreaders = ++nr;
      if (env->me_close_readers < nr)
        env->me_close_readers = nr;
      r->mr_pid = pid;
      mdbx_rdt_unlock(env);

      if (likely(env->me_flags & MDB_ENV_TXKEY))
        mdbx_thread_rthc_set(env->me_txkey, r);
    }

    while (1) {
      MDB_meta *const meta = mdbx_meta_head(txn->mt_env);
      mdbx_jitter4testing(false);
      const txnid_t snap = meta->mm_txnid;
      mdbx_jitter4testing(false);
      r->mr_txnid = snap;
      mdbx_jitter4testing(false);
      mdbx_coherent_barrier();
      mdbx_jitter4testing(true);

      /* Snap the state from current meta-head */
      txn->mt_txnid = snap;
      txn->mt_next_pgno = meta->mm_last_pg + 1;
      memcpy(txn->mt_dbs, meta->mm_dbs, CORE_DBS * sizeof(MDB_db));
      txn->mt_canary = meta->mm_canary;

      /* LY: Retry on a race, ITS#7970. */
      if (likely(meta == mdbx_meta_head(txn->mt_env) && snap == meta->mm_txnid))
        break;
    }

    txn->mt_u.reader = r;
    txn->mt_dbxs = env->me_dbxs; /* mostly static anyway */
  } else {
    /* Not yet touching txn == env->me_txn0, it may be active */
    mdbx_jitter4testing(false);
    rc = mdbx_txn_lock(env);
    if (unlikely(rc))
      return rc;

    mdbx_jitter4testing(false);
    MDB_meta *meta = mdbx_meta_head(env);
    mdbx_jitter4testing(false);
    txn->mt_canary = meta->mm_canary;
    txn->mt_txnid = meta->mm_txnid + 1;
#if MDB_DEBUG
    if (unlikely(txn->mt_txnid == mdbx_debug_edge)) {
      if (!mdbx_debug_logger)
        mdbx_runtime_flags |=
            MDBX_DBG_TRACE | MDBX_DBG_EXTRA | MDBX_DBG_AUDIT | MDBX_DBG_ASSERT;
      mdbx_debug_log(MDBX_DBG_EDGE, __FUNCTION__, __LINE__,
                     "on/off edge (txn %" PRIuPTR ")", txn->mt_txnid);
    }
#endif
    if (unlikely(txn->mt_txnid < meta->mm_txnid)) {
      mdbx_debug("txnid overflow!");
      rc = MDB_TXN_FULL;
      goto bailout;
    }

    txn->mt_flags = flags;
    txn->mt_child = NULL;
    txn->mt_loose_pgs = NULL;
    txn->mt_loose_count = 0;
    txn->mt_dirty_room = MDB_IDL_UM_MAX;
    txn->mt_u.dirty_list = env->me_dirty_list;
    txn->mt_u.dirty_list[0].mid = 0;
    txn->mt_free_pgs = env->me_free_pgs;
    txn->mt_free_pgs[0] = 0;
    txn->mt_spill_pgs = NULL;
    if (txn->mt_lifo_reclaimed)
      txn->mt_lifo_reclaimed[0] = 0;
    env->me_txn = txn;
    memcpy(txn->mt_dbiseqs, env->me_dbiseqs, env->me_maxdbs * sizeof(unsigned));
    /* Copy the DB info and flags */
    memcpy(txn->mt_dbs, meta->mm_dbs, CORE_DBS * sizeof(MDB_db));
    /* Moved to here to avoid a data race in read TXNs */
    txn->mt_next_pgno = meta->mm_last_pg + 1;
  }

  /* Setup db info */
  txn->mt_numdbs = env->me_numdbs;
  for (i = CORE_DBS; i < txn->mt_numdbs; i++) {
    unsigned x = env->me_dbflags[i];
    txn->mt_dbs[i].md_flags = x & PERSISTENT_FLAGS;
    txn->mt_dbflags[i] =
        (x & MDB_VALID) ? DB_VALID | DB_USRVALID | DB_STALE : 0;
  }
  txn->mt_dbflags[MAIN_DBI] = DB_VALID | DB_USRVALID;
  txn->mt_dbflags[FREE_DBI] = DB_VALID;

  if (unlikely(env->me_flags & MDB_FATAL_ERROR)) {
    mdbx_debug("environment had fatal error, must shutdown!");
    rc = MDB_PANIC;
  } else if (unlikely(env->me_maxpg < txn->mt_next_pgno)) {
    rc = MDB_MAP_RESIZED;
  } else {
    return MDB_SUCCESS;
  }
bailout:
  assert(rc != MDB_SUCCESS);
  mdbx_txn_end(txn, MDB_END_SLOT | MDB_END_FAIL_BEGIN);
  return rc;
}

int mdbx_txn_renew(MDB_txn *txn) {
  int rc;

  if (unlikely(!txn))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_signature != MDBX_MT_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(!F_ISSET(txn->mt_flags, MDB_TXN_RDONLY | MDB_TXN_FINISHED)))
    return MDBX_EINVAL;

  rc = mdbx_txn_renew0(txn, MDB_TXN_RDONLY);
  if (rc == MDB_SUCCESS) {
    mdbx_debug("renew txn %" PRIuPTR "%c %p on mdbenv %p, root page %" PRIuPTR
               "",
               txn->mt_txnid, (txn->mt_flags & MDB_TXN_RDONLY) ? 'r' : 'w',
               (void *)txn, (void *)txn->mt_env, txn->mt_dbs[MAIN_DBI].md_root);
  }
  return rc;
}

int mdbx_txn_begin(MDB_env *env, MDB_txn *parent, unsigned flags,
                   MDB_txn **ret) {
  MDB_txn *txn;
  MDB_ntxn *ntxn;
  int rc, size, tsize;

  if (unlikely(!env || !ret))
    return MDBX_EINVAL;

  if (unlikely(env->me_signature != MDBX_ME_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(env->me_pid != mdbx_getpid())) {
    env->me_flags |= MDB_FATAL_ERROR;
    return MDB_PANIC;
  }

  flags &= MDB_TXN_BEGIN_FLAGS;
  flags |= env->me_flags & MDB_WRITEMAP;

  if (unlikely(env->me_flags & MDB_RDONLY &
               ~flags)) /* write txn in RDONLY env */
    return MDBX_EACCESS;

  if (parent) {
    if (unlikely(parent->mt_signature != MDBX_MT_SIGNATURE))
      return MDBX_EINVAL;

    /* Nested transactions: Max 1 child, write txns only, no writemap */
    flags |= parent->mt_flags;
    if (unlikely(flags & (MDB_RDONLY | MDB_WRITEMAP | MDB_TXN_BLOCKED))) {
      return (parent->mt_flags & MDB_TXN_RDONLY) ? MDBX_EINVAL : MDB_BAD_TXN;
    }
    /* Child txns save MDB_pgstate and use own copy of cursors */
    size = env->me_maxdbs * (sizeof(MDB_db) + sizeof(MDB_cursor *) + 1);
    size += tsize = sizeof(MDB_ntxn);
  } else if (flags & MDB_RDONLY) {
    size = env->me_maxdbs * (sizeof(MDB_db) + 1);
    size += tsize = sizeof(MDB_txn);
  } else {
    /* Reuse preallocated write txn. However, do not touch it until
     * mdbx_txn_renew0() succeeds, since it currently may be active. */
    txn = env->me_txn0;
    goto renew;
  }
  if (unlikely((txn = calloc(1, size)) == NULL)) {
    mdbx_debug("calloc: %s", "failed");
    return MDBX_ENOMEM;
  }
  txn->mt_dbxs = env->me_dbxs; /* static */
  txn->mt_dbs = (MDB_db *)((char *)txn + tsize);
  txn->mt_dbflags = (unsigned char *)txn + size - env->me_maxdbs;
  txn->mt_flags = flags;
  txn->mt_env = env;

  if (parent) {
    unsigned i;
    txn->mt_cursors = (MDB_cursor **)(txn->mt_dbs + env->me_maxdbs);
    txn->mt_dbiseqs = parent->mt_dbiseqs;
    txn->mt_u.dirty_list = malloc(sizeof(MDB_ID2) * MDB_IDL_UM_SIZE);
    if (!txn->mt_u.dirty_list ||
        !(txn->mt_free_pgs = mdbx_midl_alloc(MDB_IDL_UM_MAX))) {
      free(txn->mt_u.dirty_list);
      free(txn);
      return MDBX_ENOMEM;
    }
    txn->mt_txnid = parent->mt_txnid;
    txn->mt_dirty_room = parent->mt_dirty_room;
    txn->mt_u.dirty_list[0].mid = 0;
    txn->mt_spill_pgs = NULL;
    txn->mt_next_pgno = parent->mt_next_pgno;
    parent->mt_flags |= MDB_TXN_HAS_CHILD;
    parent->mt_child = txn;
    txn->mt_parent = parent;
    txn->mt_numdbs = parent->mt_numdbs;
    memcpy(txn->mt_dbs, parent->mt_dbs, txn->mt_numdbs * sizeof(MDB_db));
    /* Copy parent's mt_dbflags, but clear DB_NEW */
    for (i = 0; i < txn->mt_numdbs; i++)
      txn->mt_dbflags[i] = parent->mt_dbflags[i] & ~DB_NEW;
    rc = 0;
    ntxn = (MDB_ntxn *)txn;
    ntxn->mnt_pgstate = env->me_pgstate; /* save parent me_pghead & co */
    if (env->me_pghead) {
      size = MDB_IDL_SIZEOF(env->me_pghead);
      env->me_pghead = mdbx_midl_alloc(env->me_pghead[0]);
      if (likely(env->me_pghead))
        memcpy(env->me_pghead, ntxn->mnt_pgstate.mf_pghead, size);
      else
        rc = MDBX_ENOMEM;
    }
    if (likely(!rc))
      rc = mdbx_cursor_shadow(parent, txn);
    if (unlikely(rc))
      mdbx_txn_end(txn, MDB_END_FAIL_BEGINCHILD);
  } else { /* MDB_RDONLY */
    txn->mt_dbiseqs = env->me_dbiseqs;
  renew:
    rc = mdbx_txn_renew0(txn, flags);
  }

  if (unlikely(rc)) {
    if (txn != env->me_txn0)
      free(txn);
  } else {
    txn->mt_signature = MDBX_MT_SIGNATURE;
    *ret = txn;
    mdbx_debug("begin txn %" PRIuPTR "%c %p on mdbenv %p, root page %" PRIuPTR
               "",
               txn->mt_txnid, (flags & MDB_RDONLY) ? 'r' : 'w', (void *)txn,
               (void *)env, txn->mt_dbs[MAIN_DBI].md_root);
  }

  return rc;
}

MDB_env *mdbx_txn_env(MDB_txn *txn) {
  if (unlikely(!txn || txn->mt_signature != MDBX_MT_SIGNATURE))
    return NULL;
  return txn->mt_env;
}

size_t mdbx_txn_id(MDB_txn *txn) {
  if (unlikely(!txn || txn->mt_signature != MDBX_MT_SIGNATURE))
    return ~(txnid_t)0;
  return txn->mt_txnid;
}

/** Export or close DBI handles opened in this txn. */
static void mdbx_dbis_update(MDB_txn *txn, int keep) {
  int i;
  MDB_dbi n = txn->mt_numdbs;
  MDB_env *env = txn->mt_env;
  unsigned char *tdbflags = txn->mt_dbflags;

  for (i = n; --i >= CORE_DBS;) {
    if (tdbflags[i] & DB_NEW) {
      if (keep) {
        env->me_dbflags[i] = txn->mt_dbs[i].md_flags | MDB_VALID;
      } else {
        char *ptr = env->me_dbxs[i].md_name.mv_data;
        if (ptr) {
          env->me_dbxs[i].md_name.mv_data = NULL;
          env->me_dbxs[i].md_name.mv_size = 0;
          env->me_dbflags[i] = 0;
          env->me_dbiseqs[i]++;
          free(ptr);
        }
      }
    }
  }
  if (keep && env->me_numdbs < n)
    env->me_numdbs = n;
}

/** End a transaction, except successful commit of a nested transaction.
 * May be called twice for readonly txns: First reset it, then abort.
 * @param[in] txn the transaction handle to end
 * @param[in] mode why and how to end the transaction
 */
static int mdbx_txn_end(MDB_txn *txn, unsigned mode) {
  MDB_env *env = txn->mt_env;
  static const char *const names[] = MDB_END_NAMES;

  if (unlikely(txn->mt_env->me_pid != mdbx_getpid())) {
    env->me_flags |= MDB_FATAL_ERROR;
    return MDB_PANIC;
  }

  /* Export or close DBI handles opened in this txn */
  mdbx_dbis_update(txn, mode & MDB_END_UPDATE);

  mdbx_debug("%s txn %" PRIuPTR "%c %p on mdbenv %p, root page %" PRIuPTR "",
             names[mode & MDB_END_OPMASK], txn->mt_txnid,
             (txn->mt_flags & MDB_TXN_RDONLY) ? 'r' : 'w', (void *)txn,
             (void *)env, txn->mt_dbs[MAIN_DBI].md_root);

  if (F_ISSET(txn->mt_flags, MDB_TXN_RDONLY)) {
    if (txn->mt_u.reader) {
      txn->mt_u.reader->mr_txnid = ~(txnid_t)0;
      if (mode & MDB_END_SLOT) {
        if ((env->me_flags & MDB_ENV_TXKEY) == 0)
          txn->mt_u.reader->mr_pid = 0;
        txn->mt_u.reader = NULL;
      }
    }
    mdbx_coherent_barrier();
    txn->mt_numdbs = 0; /* prevent further DBI activity */
    txn->mt_flags |= MDB_TXN_FINISHED;

  } else if (!F_ISSET(txn->mt_flags, MDB_TXN_FINISHED)) {
    pgno_t *pghead = env->me_pghead;

    if (!(mode & MDB_END_EOTDONE)) /* !(already closed cursors) */
      mdbx_cursors_eot(txn, 0);
    if (!(env->me_flags & MDB_WRITEMAP)) {
      mdbx_dlist_free(txn);
    }

    if (txn->mt_lifo_reclaimed) {
      txn->mt_lifo_reclaimed[0] = 0;
      if (txn != env->me_txn0) {
        mdbx_midl_free(txn->mt_lifo_reclaimed);
        txn->mt_lifo_reclaimed = NULL;
      }
    }
    txn->mt_numdbs = 0;
    txn->mt_flags = MDB_TXN_FINISHED;

    if (!txn->mt_parent) {
      mdbx_midl_shrink(&txn->mt_free_pgs);
      env->me_free_pgs = txn->mt_free_pgs;
      /* me_pgstate: */
      env->me_pghead = NULL;
      env->me_pglast = 0;

      env->me_txn = NULL;
      mode = 0; /* txn == env->me_txn0, do not free() it */

      /* The writer mutex was locked in mdbx_txn_begin. */
      mdbx_txn_unlock(env);
    } else {
      txn->mt_parent->mt_child = NULL;
      txn->mt_parent->mt_flags &= ~MDB_TXN_HAS_CHILD;
      env->me_pgstate = ((MDB_ntxn *)txn)->mnt_pgstate;
      mdbx_midl_free(txn->mt_free_pgs);
      mdbx_midl_free(txn->mt_spill_pgs);
      free(txn->mt_u.dirty_list);
    }

    mdbx_midl_free(pghead);
  }

  if (mode & MDB_END_FREE) {
    txn->mt_signature = 0;
    free(txn);
  }

  return MDB_SUCCESS;
}

int mdbx_txn_reset(MDB_txn *txn) {
  if (unlikely(!txn))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_signature != MDBX_MT_SIGNATURE))
    return MDBX_EBADSIGN;

  /* This call is only valid for read-only txns */
  if (unlikely(!(txn->mt_flags & MDB_TXN_RDONLY)))
    return MDBX_EINVAL;

  /* LY: don't close DBI-handles in MDBX mode */
  return mdbx_txn_end(txn, MDB_END_RESET | MDB_END_UPDATE);
}

int mdbx_txn_abort(MDB_txn *txn) {
  if (unlikely(!txn))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_signature != MDBX_MT_SIGNATURE))
    return MDBX_EBADSIGN;

  if (F_ISSET(txn->mt_flags, MDB_TXN_RDONLY))
    /* LY: don't close DBI-handles in MDBX mode */
    return mdbx_txn_end(txn, MDB_END_ABORT | MDB_END_UPDATE | MDB_END_SLOT |
                                 MDB_END_FREE);

  if (txn->mt_child)
    mdbx_txn_abort(txn->mt_child);

  return mdbx_txn_end(txn, MDB_END_ABORT | MDB_END_SLOT | MDB_END_FREE);
}

static __inline int mdbx_backlog_size(MDB_txn *txn) {
  int reclaimed = txn->mt_env->me_pghead ? txn->mt_env->me_pghead[0] : 0;
  return reclaimed + txn->mt_loose_count;
}

/* LY: Prepare a backlog of pages to modify FreeDB itself,
 * while reclaiming is prohibited. It should be enough to prevent search
 * in mdbx_page_alloc() during a deleting, when freeDB tree is unbalanced. */
static int mdbx_prep_backlog(MDB_txn *txn, MDB_cursor *mc) {
  /* LY: extra page(s) for b-tree rebalancing */
  const int extra = (txn->mt_env->me_flags & MDBX_LIFORECLAIM) ? 2 : 1;

  if (mdbx_backlog_size(txn) < mc->mc_db->md_depth + extra) {
    int rc = mdbx_cursor_touch(mc);
    if (unlikely(rc))
      return rc;

    while (unlikely(mdbx_backlog_size(txn) < extra)) {
      rc = mdbx_page_alloc(mc, 1, NULL, MDBX_ALLOC_GC);
      if (unlikely(rc)) {
        if (unlikely(rc != MDB_NOTFOUND))
          return rc;
        break;
      }
    }
  }

  return MDB_SUCCESS;
}

/** Save the freelist as of this transaction to the freeDB.
 * This changes the freelist. Keep trying until it stabilizes.
 */
static int mdbx_freelist_save(MDB_txn *txn) {
  /* env->me_pghead[] can grow and shrink during this call.
   * env->me_pglast and txn->mt_free_pgs[] can only grow.
   * Page numbers cannot disappear from txn->mt_free_pgs[]. */
  MDB_cursor mc;
  MDB_env *env = txn->mt_env;
  int rc, maxfree_1pg = env->me_maxfree_1pg, more = 1;
  txnid_t pglast = 0, head_id = 0;
  pgno_t freecnt = 0, *free_pgs, *mop;
  ssize_t head_room = 0, total_room = 0, mop_len, clean_limit;
  unsigned cleanup_idx = 0, refill_idx = 0;
  const int lifo = (env->me_flags & MDBX_LIFORECLAIM) != 0;

  mdbx_cursor_init(&mc, txn, FREE_DBI, NULL);

  /* MDB_RESERVE cancels meminit in ovpage malloc (when no WRITEMAP) */
  clean_limit = (env->me_flags & (MDB_NOMEMINIT | MDB_WRITEMAP)) ? SSIZE_MAX
                                                                 : maxfree_1pg;

again:
  for (;;) {
    /* Come back here after each Put() in case freelist changed */
    MDB_val key, data;
    pgno_t *pgs;
    ssize_t j;

    if (!lifo) {
      /* If using records from freeDB which we have not yet
       * deleted, delete them and any we reserved for me_pghead. */
      while (pglast < env->me_pglast) {
        rc = mdbx_cursor_first(&mc, &key, NULL);
        if (unlikely(rc))
          goto bailout;
        rc = mdbx_prep_backlog(txn, &mc);
        if (unlikely(rc))
          goto bailout;
        pglast = head_id = *(txnid_t *)key.mv_data;
        total_room = head_room = 0;
        more = 1;
        mdbx_tassert(txn, pglast <= env->me_pglast);
        mc.mc_flags |= C_RECLAIMING;
        rc = mdbx_cursor_del(&mc, 0);
        mc.mc_flags &= ~C_RECLAIMING;
        if (unlikely(rc))
          goto bailout;
      }
    } else if (txn->mt_lifo_reclaimed) {
      /* LY: cleanup reclaimed records. */
      while (cleanup_idx < txn->mt_lifo_reclaimed[0]) {
        pglast = txn->mt_lifo_reclaimed[++cleanup_idx];
        key.mv_data = &pglast;
        key.mv_size = sizeof(pglast);
        rc = mdbx_cursor_get(&mc, &key, NULL, MDB_SET);
        if (likely(rc != MDB_NOTFOUND)) {
          if (unlikely(rc))
            goto bailout;
          rc = mdbx_prep_backlog(txn, &mc);
          if (unlikely(rc))
            goto bailout;
          mc.mc_flags |= C_RECLAIMING;
          rc = mdbx_cursor_del(&mc, 0);
          mc.mc_flags &= ~C_RECLAIMING;
          if (unlikely(rc))
            goto bailout;
        }
      }
    }

    if (unlikely(!env->me_pghead) && txn->mt_loose_pgs) {
      /* Put loose page numbers in mt_free_pgs, since
       * we may be unable to return them to me_pghead. */
      MDB_page *mp = txn->mt_loose_pgs;
      if (unlikely((rc = mdbx_midl_need(&txn->mt_free_pgs,
                                        txn->mt_loose_count)) != 0))
        return rc;
      for (; mp; mp = NEXT_LOOSE_PAGE(mp))
        mdbx_midl_xappend(txn->mt_free_pgs, mp->mp_pgno);
      txn->mt_loose_pgs = NULL;
      txn->mt_loose_count = 0;
    }

    /* Save the IDL of pages freed by this txn, to a single record */
    if (freecnt < txn->mt_free_pgs[0]) {
      if (unlikely(!freecnt)) {
        /* Make sure last page of freeDB is touched and on freelist */
        rc = mdbx_page_search(&mc, NULL, MDB_PS_LAST | MDB_PS_MODIFY);
        if (unlikely(rc && rc != MDB_NOTFOUND))
          goto bailout;
      }
      free_pgs = txn->mt_free_pgs;
      /* Write to last page of freeDB */
      key.mv_size = sizeof(txn->mt_txnid);
      key.mv_data = &txn->mt_txnid;
      do {
        freecnt = free_pgs[0];
        data.mv_size = MDB_IDL_SIZEOF(free_pgs);
        rc = mdbx_cursor_put(&mc, &key, &data, MDB_RESERVE);
        if (unlikely(rc))
          goto bailout;
        /* Retry if mt_free_pgs[] grew during the Put() */
        free_pgs = txn->mt_free_pgs;
      } while (freecnt < free_pgs[0]);

      mdbx_midl_sort(free_pgs);
      memcpy(data.mv_data, free_pgs, data.mv_size);

      if (mdbx_debug_enabled(MDBX_DBG_EXTRA)) {
        unsigned i = free_pgs[0];
        mdbx_debug_extra("IDL write txn %" PRIuPTR " root %" PRIuPTR
                         " num %u, IDL",
                         txn->mt_txnid, txn->mt_dbs[FREE_DBI].md_root, i);
        for (; i; i--)
          mdbx_debug_extra_print(" %" PRIuPTR "", free_pgs[i]);
        mdbx_debug_extra_print("\n");
      }
      continue;
    }

    mop = env->me_pghead;
    mop_len = (mop ? mop[0] : 0) + txn->mt_loose_count;

    if (mop_len && refill_idx == 0)
      refill_idx = 1;

    /* Reserve records for me_pghead[]. Split it if multi-page,
     * to avoid searching freeDB for a page range. Use keys in
     * range [1,me_pglast]: Smaller than txnid of oldest reader. */
    if (total_room >= mop_len) {
      if (total_room == mop_len || --more < 0)
        break;
    } else if (head_room >= maxfree_1pg && head_id > 1) {
      /* Keep current record (overflow page), add a new one */
      head_id--;
      refill_idx++;
      head_room = 0;
    }

    if (lifo) {
      if (refill_idx >
          (txn->mt_lifo_reclaimed ? txn->mt_lifo_reclaimed[0] : 0)) {
        /* LY: need just a txn-id for save page list. */
        rc = mdbx_page_alloc(&mc, 0, NULL, MDBX_ALLOC_GC | MDBX_ALLOC_KICK);
        if (likely(rc == 0))
          /* LY: ok, reclaimed from freedb. */
          continue;
        if (unlikely(rc != MDB_NOTFOUND))
          /* LY: other troubles... */
          goto bailout;

        /* LY: freedb is empty, will look any free txn-id in high2low order.
         */
        if (unlikely(env->me_pglast < 1)) {
          /* LY: not any txn in the past of freedb. */
          rc = MDB_MAP_FULL;
          goto bailout;
        }

        if (unlikely(!txn->mt_lifo_reclaimed)) {
          txn->mt_lifo_reclaimed = mdbx_midl_alloc(env->me_maxfree_1pg);
          if (unlikely(!txn->mt_lifo_reclaimed)) {
            rc = MDBX_ENOMEM;
            goto bailout;
          }
        }
        /* LY: append the list. */
        rc = mdbx_midl_append(&txn->mt_lifo_reclaimed, env->me_pglast - 1);
        if (unlikely(rc))
          goto bailout;
        --env->me_pglast;
        /* LY: note that freeDB cleanup is not needed. */
        ++cleanup_idx;
      }
      head_id = txn->mt_lifo_reclaimed[refill_idx];
    }

    /* (Re)write {key = head_id, IDL length = head_room} */
    total_room -= head_room;
    head_room = mop_len - total_room;
    if (head_room > maxfree_1pg && head_id > 1) {
      /* Overflow multi-page for part of me_pghead */
      head_room /= head_id; /* amortize page sizes */
      head_room += maxfree_1pg - head_room % (maxfree_1pg + 1);
    } else if (head_room < 0) {
      /* Rare case, not bothering to delete this record */
      head_room = 0;
      continue;
    }
    key.mv_size = sizeof(head_id);
    key.mv_data = &head_id;
    data.mv_size = (head_room + 1) * sizeof(pgno_t);
    rc = mdbx_cursor_put(&mc, &key, &data, MDB_RESERVE);
    if (unlikely(rc))
      goto bailout;
    /* IDL is initially empty, zero out at least the length */
    pgs = (pgno_t *)data.mv_data;
    j = head_room > clean_limit ? head_room : 0;
    do {
      pgs[j] = 0;
    } while (--j >= 0);
    total_room += head_room;
  }

  mdbx_tassert(txn,
               cleanup_idx ==
                   (txn->mt_lifo_reclaimed ? txn->mt_lifo_reclaimed[0] : 0));

  /* Return loose page numbers to me_pghead, though usually none are
   * left at this point.  The pages themselves remain in dirty_list. */
  if (txn->mt_loose_pgs) {
    MDB_page *mp = txn->mt_loose_pgs;
    unsigned count = txn->mt_loose_count;
    MDB_IDL loose;
    /* Room for loose pages + temp IDL with same */
    if ((rc = mdbx_midl_need(&env->me_pghead, 2 * count + 1)) != 0)
      goto bailout;
    mop = env->me_pghead;
    loose = mop + MDB_IDL_ALLOCLEN(mop) - count;
    for (count = 0; mp; mp = NEXT_LOOSE_PAGE(mp))
      loose[++count] = mp->mp_pgno;
    loose[0] = count;
    mdbx_midl_sort(loose);
    mdbx_midl_xmerge(mop, loose);
    txn->mt_loose_pgs = NULL;
    txn->mt_loose_count = 0;
    mop_len = mop[0];
  }

  /* Fill in the reserved me_pghead records */
  rc = MDB_SUCCESS;
  if (mop_len) {
    MDB_val key, data;
    key.mv_size = data.mv_size = 0; /* avoid MSVC warning */
    key.mv_data = data.mv_data = NULL;

    mop += mop_len;
    if (!lifo) {
      rc = mdbx_cursor_first(&mc, &key, &data);
      if (unlikely(rc))
        goto bailout;
    }

    for (;;) {
      txnid_t id;
      ssize_t len;
      MDB_ID save;

      if (!lifo) {
        id = *(txnid_t *)key.mv_data;
        mdbx_tassert(txn, id <= env->me_pglast);
      } else {
        mdbx_tassert(txn,
                     refill_idx > 0 && refill_idx <= txn->mt_lifo_reclaimed[0]);
        id = txn->mt_lifo_reclaimed[refill_idx--];
        key.mv_data = &id;
        key.mv_size = sizeof(id);
        rc = mdbx_cursor_get(&mc, &key, &data, MDB_SET);
        if (unlikely(rc))
          goto bailout;
      }
      mdbx_tassert(
          txn, cleanup_idx ==
                   (txn->mt_lifo_reclaimed ? txn->mt_lifo_reclaimed[0] : 0));

      len = (ssize_t)(data.mv_size / sizeof(MDB_ID)) - 1;
      mdbx_tassert(txn, len >= 0);
      if (len > mop_len)
        len = mop_len;
      data.mv_size = (len + 1) * sizeof(MDB_ID);
      key.mv_data = &id;
      key.mv_size = sizeof(id);
      data.mv_data = mop -= len;

      save = mop[0];
      mop[0] = len;
      rc = mdbx_cursor_put(&mc, &key, &data, MDB_CURRENT);
      mdbx_tassert(
          txn, cleanup_idx ==
                   (txn->mt_lifo_reclaimed ? txn->mt_lifo_reclaimed[0] : 0));
      mop[0] = save;
      if (unlikely(rc || (mop_len -= len) == 0))
        goto bailout;

      if (!lifo) {
        rc = mdbx_cursor_next(&mc, &key, &data, MDB_NEXT);
        if (unlikely(rc))
          goto bailout;
      }
    }
  }

bailout:
  if (txn->mt_lifo_reclaimed) {
    mdbx_tassert(txn, rc || cleanup_idx == txn->mt_lifo_reclaimed[0]);
    if (rc == 0 && cleanup_idx != txn->mt_lifo_reclaimed[0]) {
      mdbx_tassert(txn, cleanup_idx < txn->mt_lifo_reclaimed[0]);
      /* LY: zeroed cleanup_idx to force cleanup & refill created freeDB
       * records. */
      cleanup_idx = 0;
      /* LY: restart filling */
      refill_idx = total_room = head_room = 0;
      more = 1;
      goto again;
    }
    txn->mt_lifo_reclaimed[0] = 0;
    if (txn != env->me_txn0) {
      mdbx_midl_free(txn->mt_lifo_reclaimed);
      txn->mt_lifo_reclaimed = NULL;
    }
  }

  return rc;
}

/** Flush (some) dirty pages to the map, after clearing their dirty flag.
 * @param[in] txn the transaction that's being committed
 * @param[in] keep number of initial pages in dirty_list to keep dirty.
 * @return 0 on success, non-zero on failure.
 */
static int mdbx_page_flush(MDB_txn *txn, int keep) {
  MDB_env *env = txn->mt_env;
  MDB_ID2L dl = txn->mt_u.dirty_list;
  unsigned psize = env->me_psize, j;
  int i, pagecount = dl[0].mid, rc;
  size_t size = 0, pos = 0;
  pgno_t pgno = 0;
  MDB_page *dp = NULL;
  struct iovec iov[MDB_COMMIT_PAGES];
  ssize_t wpos = 0, wsize = 0;
  size_t next_pos = 1; /* impossible pos, so pos != next_pos */
  int n = 0;

  j = i = keep;

  if (env->me_flags & MDB_WRITEMAP) {
    /* Clear dirty flags */
    while (++i <= pagecount) {
      dp = dl[i].mptr;
      /* Don't flush this page yet */
      if (dp->mp_flags & (P_LOOSE | P_KEEP)) {
        dp->mp_flags &= ~P_KEEP;
        dl[++j] = dl[i];
        continue;
      }
      dp->mp_flags &= ~P_DIRTY;
      env->me_sync_pending += IS_OVERFLOW(dp) ? psize * dp->mp_pages : psize;
    }
    goto done;
  }

  /* Write the pages */
  for (;;) {
    if (++i <= pagecount) {
      dp = dl[i].mptr;
      /* Don't flush this page yet */
      if (dp->mp_flags & (P_LOOSE | P_KEEP)) {
        dp->mp_flags &= ~P_KEEP;
        dl[i].mid = 0;
        continue;
      }
      pgno = dl[i].mid;
      /* clear dirty flag */
      dp->mp_flags &= ~P_DIRTY;
      pos = pgno * psize;
      size = psize;
      if (IS_OVERFLOW(dp))
        size *= dp->mp_pages;
      env->me_sync_pending += size;
    }
    /* Write up to MDB_COMMIT_PAGES dirty pages at a time. */
    if (pos != next_pos || n == MDB_COMMIT_PAGES || wsize + size > MAX_WRITE) {
      if (n) {
        /* Write previous page(s) */
        rc = mdbx_pwritev(env->me_fd, iov, n, wpos, wsize);
        if (unlikely(rc != MDB_SUCCESS)) {
          mdbx_debug("Write error: %s", strerror(rc));
          return rc;
        }
        n = 0;
      }
      if (i > pagecount)
        break;
      wpos = pos;
      wsize = 0;
    }
    mdbx_debug("committing page %" PRIuPTR "", pgno);
    next_pos = pos + size;
    iov[n].iov_len = size;
    iov[n].iov_base = (char *)dp;
    wsize += size;
    n++;
  }

  mdbx_invalidate_cache(env->me_map, txn->mt_next_pgno * env->me_psize);

  for (i = keep; ++i <= pagecount;) {
    dp = dl[i].mptr;
    /* This is a page we skipped above */
    if (!dl[i].mid) {
      dl[++j] = dl[i];
      dl[j].mid = dp->mp_pgno;
      continue;
    }
    mdbx_dpage_free(env, dp);
  }

done:
  i--;
  txn->mt_dirty_room += i - j;
  dl[0].mid = j;
  return MDB_SUCCESS;
}

int mdbx_txn_commit(MDB_txn *txn) {
  int rc;

  if (unlikely(txn == NULL))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_signature != MDBX_MT_SIGNATURE))
    return MDBX_EBADSIGN;

  MDB_env *env = txn->mt_env;
  if (unlikely(env->me_pid != mdbx_getpid())) {
    env->me_flags |= MDB_FATAL_ERROR;
    return MDB_PANIC;
  }

  if (txn->mt_child) {
    rc = mdbx_txn_commit(txn->mt_child);
    txn->mt_child = NULL;
    if (unlikely(rc != MDB_SUCCESS))
      goto fail;
  }

  /* mdbx_txn_end() mode for a commit which writes nothing */
  unsigned end_mode =
      MDB_END_EMPTY_COMMIT | MDB_END_UPDATE | MDB_END_SLOT | MDB_END_FREE;
  if (unlikely(F_ISSET(txn->mt_flags, MDB_TXN_RDONLY)))
    goto done;

  if (unlikely(txn->mt_flags & (MDB_TXN_FINISHED | MDB_TXN_ERROR))) {
    mdbx_debug("error flag is set, can't commit");
    if (txn->mt_parent)
      txn->mt_parent->mt_flags |= MDB_TXN_ERROR;
    rc = MDB_BAD_TXN;
    goto fail;
  }

  if (txn->mt_parent) {
    MDB_txn *parent = txn->mt_parent;
    MDB_page **lp;
    MDB_ID2L dst, src;
    MDB_IDL pspill;
    unsigned i, x, y, len, ps_len;

    /* Append our reclaim list to parent's */
    if (txn->mt_lifo_reclaimed) {
      if (parent->mt_lifo_reclaimed) {
        rc = mdbx_midl_append_list(&parent->mt_lifo_reclaimed,
                                   txn->mt_lifo_reclaimed);
        if (unlikely(rc != MDB_SUCCESS))
          goto fail;
        mdbx_midl_free(txn->mt_lifo_reclaimed);
      } else
        parent->mt_lifo_reclaimed = txn->mt_lifo_reclaimed;
      txn->mt_lifo_reclaimed = NULL;
    }

    /* Append our free list to parent's */
    rc = mdbx_midl_append_list(&parent->mt_free_pgs, txn->mt_free_pgs);
    if (unlikely(rc != MDB_SUCCESS))
      goto fail;
    mdbx_midl_free(txn->mt_free_pgs);
    /* Failures after this must either undo the changes
     * to the parent or set MDB_TXN_ERROR in the parent. */

    parent->mt_next_pgno = txn->mt_next_pgno;
    parent->mt_flags = txn->mt_flags;

    /* Merge our cursors into parent's and close them */
    mdbx_cursors_eot(txn, 1);

    /* Update parent's DB table. */
    memcpy(parent->mt_dbs, txn->mt_dbs, txn->mt_numdbs * sizeof(MDB_db));
    parent->mt_numdbs = txn->mt_numdbs;
    parent->mt_dbflags[FREE_DBI] = txn->mt_dbflags[FREE_DBI];
    parent->mt_dbflags[MAIN_DBI] = txn->mt_dbflags[MAIN_DBI];
    for (i = CORE_DBS; i < txn->mt_numdbs; i++) {
      /* preserve parent's DB_NEW status */
      x = parent->mt_dbflags[i] & DB_NEW;
      parent->mt_dbflags[i] = txn->mt_dbflags[i] | x;
    }

    dst = parent->mt_u.dirty_list;
    src = txn->mt_u.dirty_list;
    /* Remove anything in our dirty list from parent's spill list */
    if ((pspill = parent->mt_spill_pgs) && (ps_len = pspill[0])) {
      x = y = ps_len;
      pspill[0] = (pgno_t)-1;
      /* Mark our dirty pages as deleted in parent spill list */
      for (i = 0, len = src[0].mid; ++i <= len;) {
        MDB_ID pn = src[i].mid << 1;
        while (pn > pspill[x])
          x--;
        if (pn == pspill[x]) {
          pspill[x] = 1;
          y = --x;
        }
      }
      /* Squash deleted pagenums if we deleted any */
      for (x = y; ++x <= ps_len;)
        if (!(pspill[x] & 1))
          pspill[++y] = pspill[x];
      pspill[0] = y;
    }

    /* Remove anything in our spill list from parent's dirty list */
    if (txn->mt_spill_pgs && txn->mt_spill_pgs[0]) {
      for (i = 1; i <= txn->mt_spill_pgs[0]; i++) {
        MDB_ID pn = txn->mt_spill_pgs[i];
        if (pn & 1)
          continue; /* deleted spillpg */
        pn >>= 1;
        y = mdbx_mid2l_search(dst, pn);
        if (y <= dst[0].mid && dst[y].mid == pn) {
          free(dst[y].mptr);
          while (y < dst[0].mid) {
            dst[y] = dst[y + 1];
            y++;
          }
          dst[0].mid--;
        }
      }
    }

    /* Find len = length of merging our dirty list with parent's */
    x = dst[0].mid;
    dst[0].mid = 0; /* simplify loops */
    if (parent->mt_parent) {
      len = x + src[0].mid;
      y = mdbx_mid2l_search(src, dst[x].mid + 1) - 1;
      for (i = x; y && i; y--) {
        pgno_t yp = src[y].mid;
        while (yp < dst[i].mid)
          i--;
        if (yp == dst[i].mid) {
          i--;
          len--;
        }
      }
    } else { /* Simplify the above for single-ancestor case */
      len = MDB_IDL_UM_MAX - txn->mt_dirty_room;
    }
    /* Merge our dirty list with parent's */
    y = src[0].mid;
    for (i = len; y; dst[i--] = src[y--]) {
      pgno_t yp = src[y].mid;
      while (yp < dst[x].mid)
        dst[i--] = dst[x--];
      if (yp == dst[x].mid)
        free(dst[x--].mptr);
    }
    mdbx_tassert(txn, i == x);
    dst[0].mid = len;
    free(txn->mt_u.dirty_list);
    parent->mt_dirty_room = txn->mt_dirty_room;
    if (txn->mt_spill_pgs) {
      if (parent->mt_spill_pgs) {
        /* TODO: Prevent failure here, so parent does not fail */
        rc = mdbx_midl_append_list(&parent->mt_spill_pgs, txn->mt_spill_pgs);
        if (unlikely(rc != MDB_SUCCESS))
          parent->mt_flags |= MDB_TXN_ERROR;
        mdbx_midl_free(txn->mt_spill_pgs);
        mdbx_midl_sort(parent->mt_spill_pgs);
      } else {
        parent->mt_spill_pgs = txn->mt_spill_pgs;
      }
    }

    /* Append our loose page list to parent's */
    for (lp = &parent->mt_loose_pgs; *lp; lp = &NEXT_LOOSE_PAGE(*lp))
      ;
    *lp = txn->mt_loose_pgs;
    parent->mt_loose_count += txn->mt_loose_count;

    parent->mt_child = NULL;
    mdbx_midl_free(((MDB_ntxn *)txn)->mnt_pgstate.mf_pghead);
    txn->mt_signature = 0;
    free(txn);
    return rc;
  }

  if (unlikely(txn != env->me_txn)) {
    mdbx_debug("attempt to commit unknown transaction");
    rc = MDBX_EINVAL;
    goto fail;
  }

  mdbx_cursors_eot(txn, 0);
  end_mode |= MDB_END_EOTDONE;

  if (!txn->mt_u.dirty_list[0].mid &&
      !(txn->mt_flags & (MDB_TXN_DIRTY | MDB_TXN_SPILLS)))
    goto done;

  mdbx_debug(
      "committing txn %" PRIuPTR " %p on mdbenv %p, root page %" PRIuPTR "",
      txn->mt_txnid, (void *)txn, (void *)env, txn->mt_dbs[MAIN_DBI].md_root);

  /* Update DB root pointers */
  if (txn->mt_numdbs > CORE_DBS) {
    MDB_cursor mc;
    MDB_dbi i;
    MDB_val data;
    data.mv_size = sizeof(MDB_db);

    mdbx_cursor_init(&mc, txn, MAIN_DBI, NULL);
    for (i = CORE_DBS; i < txn->mt_numdbs; i++) {
      if (txn->mt_dbflags[i] & DB_DIRTY) {
        if (unlikely(TXN_DBI_CHANGED(txn, i))) {
          rc = MDB_BAD_DBI;
          goto fail;
        }
        data.mv_data = &txn->mt_dbs[i];
        rc = mdbx_cursor_put(&mc, &txn->mt_dbxs[i].md_name, &data, F_SUBDATA);
        if (unlikely(rc != MDB_SUCCESS))
          goto fail;
      }
    }
  }

  rc = mdbx_freelist_save(txn);
  if (unlikely(rc != MDB_SUCCESS))
    goto fail;

  mdbx_midl_free(env->me_pghead);
  env->me_pghead = NULL;
  mdbx_midl_shrink(&txn->mt_free_pgs);

  if (mdbx_audit_enabled())
    mdbx_audit(txn);

  rc = mdbx_page_flush(txn, 0);
  if (likely(rc == MDB_SUCCESS)) {
    MDB_meta meta;

    meta.mm_dbs[FREE_DBI] = txn->mt_dbs[FREE_DBI];
    meta.mm_dbs[MAIN_DBI] = txn->mt_dbs[MAIN_DBI];
    meta.mm_last_pg = txn->mt_next_pgno - 1;
    meta.mm_txnid = txn->mt_txnid;
    meta.mm_canary = txn->mt_canary;

    rc = mdbx_env_sync0(env, env->me_flags | txn->mt_flags, &meta);
  }
  if (unlikely(rc != MDB_SUCCESS))
    goto fail;
  end_mode = MDB_END_COMMITTED | MDB_END_UPDATE | MDB_END_EOTDONE;

done:
  return mdbx_txn_end(txn, end_mode);

fail:
  mdbx_txn_abort(txn);
  return rc;
}

/* Read the environment parameters of a DB environment
 * before mapping it into memory. */
static int __cold mdbx_read_header(MDB_env *env, MDB_meta *meta) {
  assert(offsetof(MDB_metabuf, mb_metabuf.mm_meta) == PAGEHDRSZ);
  memset(meta, 0, sizeof(MDB_meta));
  meta->mm_datasync_sign = MDB_DATASIGN_WEAK;
  unsigned offset = 0;

  /* Read both meta pages so we can use the latest one. */
  for (int loops_left = 2; --loops_left >= 0;) {
    MDB_metabuf buf;

    /* We don't know the page size on first time, so use a minimum value. */
    int rc = mdbx_pread(env->me_fd, &buf, sizeof(buf), offset);
    if (rc != MDB_SUCCESS) {
      mdbx_debug("read meta[%u,%u]: %i, %s", offset, (unsigned)sizeof(buf), rc,
                 mdbx_strerror(rc));
      return rc;
    }

    MDB_page *p = (MDB_page *)&buf;
    if (!F_ISSET(p->mp_flags, P_META)) {
      mdbx_debug("page %" PRIuPTR " not a meta-page", p->mp_pgno);
      return MDB_INVALID;
    }

    MDB_meta *m = PAGEDATA(p);
    if (m->mm_magic != MDB_MAGIC) {
      mdbx_debug("meta[%u] has invalid magic", offset);
      return MDB_INVALID;
    }

    if (m->mm_version != MDB_DATA_VERSION) {
      mdbx_debug("database is version %u, expected version %u", m->mm_version,
                 MDB_DATA_VERSION);
      return MDB_VERSION_MISMATCH;
    }

    /* LY: check signature as a checksum */
    if (META_IS_STEADY(m) && m->mm_datasync_sign != mdbx_meta_sign(m)) {
      mdbx_debug("steady-meta[%u] has invalid checksum", offset);
      continue;
    }

    if (mdbx_meta_lt(meta, m)) {
      *meta = *m;
      if (META_IS_WEAK(meta))
        loops_left += 1; /* LY: should re-read to avoid race */
    }

    if (offset)
      offset = 0;
    else {
      offset = meta->mm_psize;
      if (!offset)
        offset = m->mm_psize;
      if (!offset)
        offset = env->me_os_psize;
    }
  }

  if (META_IS_WEAK(meta)) {
    mdbx_debug("both meta-pages are weak, database is corrupted");
    return MDB_CORRUPTED;
  }

  return MDB_SUCCESS;
}

/* Fill in most of the zeroed MDB_meta for an empty database environment */
static void __cold mdbx_env_init_meta0(MDB_env *env, MDB_meta *meta) {
  meta->mm_magic = MDB_MAGIC;
  meta->mm_version = MDB_DATA_VERSION;
  meta->mm_mapsize = env->me_mapsize;
  meta->mm_psize = env->me_psize;
  meta->mm_last_pg = NUM_METAS - 1;
  meta->mm_flags = env->me_flags & 0xffff;
  meta->mm_flags |= MDB_INTEGERKEY; /* this is mm_dbs[FREE_DBI].md_flags */
  meta->mm_dbs[FREE_DBI].md_root = P_INVALID;
  meta->mm_dbs[MAIN_DBI].md_root = P_INVALID;
  meta->mm_datasync_sign = mdbx_meta_sign(meta);
}

/** Write the environment parameters of a freshly created DB environment.
 * @param[in] env the environment handle
 * @param[in] meta the #MDB_meta to write
 * @return 0 on success, non-zero on failure.
 */
static int __cold mdbx_env_init_meta(MDB_env *env, MDB_meta *meta) {
  MDB_page *p, *q;
  int rc;
  unsigned psize;

  mdbx_debug("writing new meta page");
  assert(offsetof(MDB_metabuf, mb_metabuf.mm_meta) == PAGEHDRSZ);

  psize = env->me_psize;

  p = calloc(NUM_METAS, psize);
  if (!p)
    return MDBX_ENOMEM;
  p->mp_pgno = 0;
  p->mp_flags = P_META;
  *(MDB_meta *)PAGEDATA(p) = *meta;

  q = (MDB_page *)((char *)p + psize);
  q->mp_pgno = 1;
  q->mp_flags = P_META;
  *(MDB_meta *)PAGEDATA(q) = *meta;

  rc = mdbx_pwrite(env->me_fd, p, psize * NUM_METAS, 0);

  free(p);
  return rc;
}

static int mdbx_env_sync0(MDB_env *env, unsigned flags, MDB_meta *pending) {
  int rc;
  MDB_meta *head = mdbx_meta_head(env);
  size_t prev_mapsize = head->mm_mapsize;
  size_t used_size = env->me_psize * (pending->mm_last_pg + 1);

  mdbx_assert(env, pending != METAPAGE_1(env) && pending != METAPAGE_2(env));
  mdbx_assert(env, (env->me_flags & (MDB_RDONLY | MDB_FATAL_ERROR)) == 0);
  mdbx_assert(env, META_IS_WEAK(head) || env->me_sync_pending != 0 ||
                       env->me_mapsize != prev_mapsize);

  pending->mm_mapsize = env->me_mapsize;
  mdbx_assert(env, pending->mm_mapsize >= used_size);
  if (unlikely(pending->mm_mapsize != prev_mapsize)) {
    if (pending->mm_mapsize < prev_mapsize) {
      /* LY: currently this can't happen, but force full-sync. */
      flags &= MDB_WRITEMAP;
    } else {
      /* Persist any increases of mapsize config */
    }
  }

  if (env->me_sync_threshold && env->me_sync_pending >= env->me_sync_threshold)
    flags &= MDB_WRITEMAP;

  /* LY: step#1 - sync previously written/updated data-pages */
  if (env->me_sync_pending && (flags & MDB_NOSYNC) == 0) {
    assert(((flags ^ env->me_flags) & MDB_WRITEMAP) == 0);
    if (flags & MDB_WRITEMAP) {
      rc = mdbx_msync(env->me_map, used_size, flags & MDB_MAPASYNC);
      if (unlikely(rc != MDB_SUCCESS))
        goto fail;
      if ((flags & MDB_MAPASYNC) == 0)
        env->me_sync_pending = 0;
    } else {
      bool fullsync = false;
      if (unlikely(prev_mapsize != pending->mm_mapsize)) {
        /* LY: It is no reason to use fdatasync() here, even in case
         * no such bug in a kernel. Because "no-bug" mean that a kernel
         * internally do nearly the same, e.g. fdatasync() == fsync()
         * when no-kernel-bug and file size was changed.
         *
         * So, this code is always safe and without appreciable
         * performance degradation.
         *
         * For more info about of a corresponding fdatasync() bug
         * see http://www.spinics.net/lists/linux-ext4/msg33714.html */
        fullsync = true;
      }
      rc = mdbx_filesync(env->me_fd, fullsync);
      if (unlikely(rc != MDB_SUCCESS))
        goto fail;
      env->me_sync_pending = 0;
    }
  }

  /* LY: step#2 - update meta-page. */
  if (env->me_sync_pending == 0) {
    pending->mm_datasync_sign = mdbx_meta_sign(pending);
  } else {
    pending->mm_datasync_sign =
        (flags & MDBX_UTTERLY_NOSYNC) == MDBX_UTTERLY_NOSYNC
            ? MDB_DATASIGN_NONE
            : MDB_DATASIGN_WEAK;
  }

  volatile MDB_meta *target =
      (pending->mm_txnid == head->mm_txnid || META_IS_WEAK(head))
          ? head
          : mdbx_env_meta_flipflop(env, head);
  off_t offset = (char *)target - env->me_map;

  MDB_meta *stay = mdbx_env_meta_flipflop(env, (MDB_meta *)target);
  mdbx_debug(
      "writing meta %d (%s, was %" PRIuPTR "/%s, stay %s %" PRIuPTR
      "/%s), root %" PRIuPTR ", "
      "txn_id %" PRIuPTR ", %s",
      offset >= (off_t)env->me_psize, target == head ? "head" : "tail",
      target->mm_txnid,
      META_IS_WEAK(target) ? "Weak" : META_IS_STEADY(target) ? "Steady"
                                                             : "Legacy",
      stay == head ? "head" : "tail", stay->mm_txnid,
      META_IS_WEAK(stay) ? "Weak" : META_IS_STEADY(stay) ? "Steady" : "Legacy",
      pending->mm_dbs[MAIN_DBI].md_root, pending->mm_txnid,
      META_IS_WEAK(pending) ? "Weak" : META_IS_STEADY(pending) ? "Steady"
                                                               : "Legacy");

  if (env->me_flags & MDB_WRITEMAP) {
    /* LY: 'invalidate' the meta,
     * but mdbx_meta_head_r() will be confused/retired in collision case. */
    target->mm_datasync_sign = MDB_DATASIGN_WEAK;
    target->mm_txnid = 0;
    /* LY: update info */
    target->mm_mapsize = pending->mm_mapsize;
    target->mm_dbs[FREE_DBI] = pending->mm_dbs[FREE_DBI];
    target->mm_dbs[MAIN_DBI] = pending->mm_dbs[MAIN_DBI];
    target->mm_last_pg = pending->mm_last_pg;
    target->mm_canary = pending->mm_canary;
    /* LY: 'commit' the meta */
    target->mm_txnid = pending->mm_txnid;
    target->mm_datasync_sign = pending->mm_datasync_sign;
  } else {
    pending->mm_magic = MDB_MAGIC;
    pending->mm_version = MDB_DATA_VERSION;
    rc = mdbx_pwrite(env->me_fd, pending, sizeof(MDB_meta), offset);
    if (unlikely(rc != MDB_SUCCESS)) {
    undo:
      mdbx_debug("write failed, disk error?");
      /* On a failure, the pagecache still contains the new data.
       * Try write some old data back, to prevent it from being used. */
      mdbx_pwrite(env->me_fd, (void *)target, sizeof(MDB_meta), offset);
      goto fail;
    }
    mdbx_invalidate_cache(env->me_map + offset, sizeof(MDB_meta));
  }

  /* Memory ordering issues are irrelevant; since the entire writer
   * is wrapped by wmutex, all of these changes will become visible
   * after the wmutex is unlocked. Since the DB is multi-version,
   * readers will get consistent data regardless of how fresh or
   * how stale their view of these values is.
   */

  /* LY: step#3 - sync meta-pages. */
  if ((flags & (MDB_NOSYNC | MDB_NOMETASYNC)) == 0) {
    assert(((flags ^ env->me_flags) & MDB_WRITEMAP) == 0);
    if (flags & MDB_WRITEMAP) {
      char *ptr = env->me_map + (offset & ~(env->me_os_psize - 1));
      rc = mdbx_msync(ptr, env->me_os_psize, flags & MDB_MAPASYNC);
      if (unlikely(rc != MDB_SUCCESS))
        goto fail;
    } else {
      rc = mdbx_filesync(env->me_fd, false);
      if (rc != MDB_SUCCESS)
        goto undo;
    }
  }

  /* LY: currently this can't happen, but... */
  if (unlikely(pending->mm_mapsize < prev_mapsize)) {
    mdbx_assert(env, pending->mm_mapsize == env->me_mapsize);
    rc = mdbx_ftruncate(env->me_fd, pending->mm_mapsize);
    if (unlikely(rc != MDB_SUCCESS))
      goto fail;
    rc = mdbx_mremap_size((void **)&env->me_map, prev_mapsize,
                          pending->mm_mapsize);
    if (unlikely(rc != MDB_SUCCESS))
      goto fail;
  }

  return MDB_SUCCESS;

fail:
  env->me_flags |= MDB_FATAL_ERROR;
  return rc;
}

int __cold mdbx_env_get_maxkeysize(MDB_env *env) {
  if (!env || env->me_signature != MDBX_ME_SIGNATURE || !env->me_maxkey_limit)
    return MDBX_EINVAL;
  return env->me_maxkey_limit;
}

static __inline ssize_t mdbx_calc_nodemax(ssize_t pagesize) {
  assert(pagesize > 0);
  return (((pagesize - PAGEHDRSZ) / MDB_MINKEYS) & -(ssize_t)2) -
         sizeof(indx_t);
}

static __inline ssize_t mdbx_calc_maxkey(ssize_t nodemax) {
  assert(nodemax > 0);
  return nodemax - (NODESIZE + sizeof(MDB_db));
}

int mdbx_get_maxkeysize(size_t pagesize) {
  if (pagesize == 0)
    pagesize = mdbx_syspagesize();

  ssize_t nodemax = mdbx_calc_nodemax(pagesize);
  if (nodemax < 0)
    return -MDBX_EINVAL;

  ssize_t maxkey = mdbx_calc_maxkey(nodemax);
  return (maxkey > 0 && maxkey < INT_MAX) ? (int)maxkey : -MDBX_EINVAL;
}

static void __cold mdbx_env_setup_limits(MDB_env *env, size_t pagesize) {
  env->me_maxfree_1pg = (pagesize - PAGEHDRSZ) / sizeof(pgno_t) - 1;
  env->me_maxpg = env->me_mapsize / pagesize;

  env->me_nodemax = mdbx_calc_nodemax(pagesize);
  env->me_maxkey_limit = mdbx_calc_maxkey(env->me_nodemax);
  assert(env->me_maxkey_limit > 42 && env->me_maxkey_limit < pagesize);
}

int __cold mdbx_env_create(MDB_env **env) {
  MDB_env *e;

  e = calloc(1, sizeof(MDB_env));
  if (!e)
    return MDBX_ENOMEM;

  e->me_maxreaders = DEFAULT_READERS;
  e->me_maxdbs = e->me_numdbs = CORE_DBS;
  e->me_fd = INVALID_HANDLE_VALUE;
  e->me_lfd = INVALID_HANDLE_VALUE;
  e->me_pid = mdbx_getpid();
  mdbx_env_setup_limits(e, e->me_os_psize = mdbx_syspagesize());
  if (!is_power2(e->me_os_psize))
    return MDB_INCOMPATIBLE;
  VALGRIND_CREATE_MEMPOOL(e, 0, 0);
  e->me_signature = MDBX_ME_SIGNATURE;
  *env = e;

  return MDB_SUCCESS;
}

static int __cold mdbx_env_map(MDB_env *env, void *addr, size_t usedsize) {
  unsigned flags = env->me_flags;
  int rc;

  if (flags & MDB_WRITEMAP) {
    rc = mdbx_ftruncate(env->me_fd, env->me_mapsize);
    if (unlikely(rc != MDB_SUCCESS))
      return rc;
  }

  env->me_map = addr;
  rc = mdbx_mmap((void **)&env->me_map, env->me_mapsize, flags & MDB_WRITEMAP,
                 env->me_fd);
  if (unlikely(rc != MDB_SUCCESS)) {
    env->me_map = NULL;
    return rc;
  }

#ifdef MADV_DONTFORK
  if (madvise(env->me_map, env->me_mapsize, MADV_DONTFORK))
    return errno;
#endif

#ifdef MADV_NOHUGEPAGE
  (void)madvise(env->me_map, env->me_mapsize, MADV_NOHUGEPAGE);
#endif

#ifdef MADV_DONTDUMP
  if (!(flags & MDBX_PAGEPERTURB))
    (void)madvise(env->me_map, env->me_mapsize, MADV_DONTDUMP);
#endif

#ifdef MADV_REMOVE
  if (flags & MDB_WRITEMAP)
    (void)madvise(env->me_map + usedsize, env->me_mapsize - usedsize,
                  MADV_REMOVE);
#else
  (void)usedsize;
#endif

#if defined(MADV_RANDOM) && defined(MADV_WILLNEED)
  /* Turn on/off readahead. It's harmful when the DB is larger than RAM. */
  if (madvise(env->me_map, env->me_mapsize,
              (flags & MDB_NORDAHEAD) ? MADV_RANDOM : MADV_WILLNEED))
    return errno;
#endif

  /* Lock meta pages to avoid unexpected write,
   *  before the data pages would be synchronized. */
  if (flags & MDB_WRITEMAP) {
    rc = mdbx_mlock(env->me_map, env->me_psize * 2);
    if (unlikely(rc != MDB_SUCCESS))
      return rc;
  }

#ifdef USE_VALGRIND
  env->me_valgrind_handle =
      VALGRIND_CREATE_BLOCK(env->me_map, env->me_mapsize, "lmdb");
#endif

  return MDB_SUCCESS;
}

int __cold mdbx_env_set_mapsize(MDB_env *env, size_t size) {
  if (unlikely(!env))
    return MDBX_EINVAL;

  if (unlikely(env->me_signature != MDBX_ME_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(size < env->me_psize * 8))
    return MDBX_EINVAL;

  /* If env is already open, caller is responsible for making
   * sure there are no active txns.
   */
  if (env->me_map) {
    int rc;
    MDB_meta *meta;
    if (env->me_txn)
      return MDBX_EINVAL;

    /* FIXME: lock/unlock */
    meta = mdbx_meta_head(env);
    if (!size)
      size = meta->mm_mapsize;
    /* Silently round up to minimum if the size is too small */
    const size_t usedsize = (meta->mm_last_pg + 1) * env->me_psize;
    if (size < usedsize)
      size = usedsize;

    mdbx_munmap(env->me_map, env->me_mapsize);
#ifdef USE_VALGRIND
    VALGRIND_DISCARD(env->me_valgrind_handle);
    env->me_valgrind_handle = -1;
#endif

    rc = mdbx_ftruncate(env->me_fd, size);
    if (unlikely(rc != MDB_SUCCESS))
      return rc;
    env->me_mapsize = size;
    /* FIXME: update meta */
    rc = mdbx_env_map(env, NULL, usedsize);
    if (unlikely(rc != MDB_SUCCESS))
      return rc;
  }

  env->me_mapsize = size;
  if (env->me_psize)
    env->me_maxpg = env->me_mapsize / env->me_psize;
  return MDB_SUCCESS;
}

int __cold mdbx_env_set_maxdbs(MDB_env *env, MDB_dbi dbs) {
  if (unlikely(!env))
    return MDBX_EINVAL;

  if (unlikely(env->me_signature != MDBX_ME_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(env->me_map))
    return MDBX_EINVAL;

  env->me_maxdbs = dbs + CORE_DBS;
  return MDB_SUCCESS;
}

int __cold mdbx_env_set_maxreaders(MDB_env *env, unsigned readers) {
  if (unlikely(!env || readers < 1))
    return MDBX_EINVAL;

  if (unlikely(env->me_signature != MDBX_ME_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(env->me_map || readers > INT16_MAX))
    return MDBX_EINVAL;

  env->me_maxreaders = readers;
  return MDB_SUCCESS;
}

int __cold mdbx_env_get_maxreaders(MDB_env *env, unsigned *readers) {
  if (!env || !readers)
    return MDBX_EINVAL;

  if (unlikely(env->me_signature != MDBX_ME_SIGNATURE))
    return MDBX_EBADSIGN;

  *readers = env->me_maxreaders;
  return MDB_SUCCESS;
}

/* Further setup required for opening an LMDB environment */
static int __cold mdbx_setup_dxb(MDB_env *env, MDB_meta *meta, int lck_rc) {
  int rc = MDBX_RESULT_FALSE;
  int err = mdbx_read_header(env, meta);
  if (unlikely(err != MDB_SUCCESS)) {
    if (lck_rc != /* lck exclusive */ MDBX_RESULT_TRUE || err != MDBX_ENODATA ||
        (env->me_flags & MDB_RDONLY) != 0)
      return err;

    mdbx_debug("create new database");
    rc = /* new database */ MDBX_RESULT_TRUE;

    env->me_psize = env->me_os_psize;
    if (env->me_psize > MAX_PAGESIZE)
      env->me_psize = MAX_PAGESIZE;
    memset(meta, 0, sizeof(*meta));
    mdbx_env_init_meta0(env, meta);
    meta->mm_mapsize = DEFAULT_MAPSIZE;
  } else {
    env->me_psize = meta->mm_psize;
  }

  /* Was a mapsize configured? */
  if (!env->me_mapsize)
    env->me_mapsize = meta->mm_mapsize;
  else {
    /* Make sure mapsize >= committed data size.  Even when using
     * mm_mapsize, which could be broken in old files (ITS#7789).
     */
    size_t minsize = (meta->mm_last_pg + 1) * meta->mm_psize;
    if (env->me_mapsize < minsize)
      env->me_mapsize = minsize;

    meta->mm_mapsize = env->me_mapsize;
  }

  if (rc == MDBX_RESULT_TRUE) {
    /* mdbx_env_map() may grow the datafile.  Write the metapages
     * first, so the file will be valid if initialization fails. */
    err = mdbx_env_init_meta(env, meta);
    if (unlikely(err != MDB_SUCCESS))
      return err;

    err = mdbx_ftruncate(env->me_fd, env->me_mapsize);
    if (unlikely(err != MDB_SUCCESS))
      return err;
  }

  const size_t usedsize = (meta->mm_last_pg + 1) * env->me_psize;
  err = mdbx_env_map(env, NULL, usedsize);
  if (err)
    return err;

  mdbx_env_setup_limits(env, env->me_psize);
  return rc;
}

/****************************************************************************/

/* Open and/or initialize the lock region for the environment. */
static int __cold mdbx_setup_lck(MDB_env *env, char *lck_pathname, int mode) {
  off_t size;
  assert(env->me_fd != INVALID_HANDLE_VALUE);
  assert(env->me_lfd == INVALID_HANDLE_VALUE);

  int err = mdbx_openfile(lck_pathname, O_RDWR | O_CREAT, mode, &env->me_lfd);
  if (err != MDB_SUCCESS) {
    if (err != MDBX_EROFS || (env->me_flags & MDB_RDONLY) == 0)
      return err;
    /* LY: without-lck mode (e.g. on read-only filesystem) */
    env->me_lfd = INVALID_HANDLE_VALUE;
  }

  /* Try to get exclusive lock. If we succeed, then
   * nobody is using the lock region and we should initialize it. */
  const int rc = mdbx_lck_seize(env);
  if (MDBX_IS_ERROR(rc))
    return rc;

  mdbx_debug("lck-setup: %s ",
             (rc == MDBX_RESULT_TRUE) ? "exclusive" : "shared");

  err = mdbx_filesize(env->me_lfd, &size);
  if (unlikely(err != MDB_SUCCESS))
    return err;

  if (rc == MDBX_RESULT_TRUE) {
    off_t wanna = roundup2((env->me_maxreaders - 1) * sizeof(MDB_reader) +
                               sizeof(MDBX_lockinfo),
                           env->me_os_psize);
#ifndef NDEBUG
    err = mdbx_ftruncate(env->me_lfd, size = 0);
    if (unlikely(err != MDB_SUCCESS))
      return err;
#endif
    mdbx_jitter4testing(false);

    if (size != wanna) {
      err = mdbx_ftruncate(env->me_lfd, wanna);
      if (unlikely(err != MDB_SUCCESS))
        return err;
      size = wanna;
    }
  }
  env->me_maxreaders = (size - sizeof(MDBX_lockinfo)) / sizeof(MDB_reader) + 1;

  void *addr = NULL;
  err = mdbx_mmap(&addr, size, true, env->me_lfd);
  if (unlikely(err != MDB_SUCCESS))
    return err;
  env->me_lck = addr;

#ifdef MADV_NOHUGEPAGE
  (void)madvise(env->me_lck, size, MADV_NOHUGEPAGE);
#endif

#ifdef MADV_DODUMP
  (void)madvise(env->me_lck, size, MADV_DODUMP);
#endif

#ifdef MADV_DONTFORK
  if (madvise(env->me_lck, size, MADV_DONTFORK) < 0)
    return errno;
#endif

#ifdef MADV_WILLNEED
  if (madvise(env->me_lck, size, MADV_WILLNEED) < 0)
    return errno;
#endif

#ifdef MADV_RANDOM
  if (madvise(env->me_lck, size, MADV_RANDOM) < 0)
    return errno;
#endif

  if (rc == MDBX_RESULT_TRUE) {
    /* LY: exlcusive mode, init lck */
    memset(env->me_lck, 0, sizeof(MDBX_lockinfo));
    err = mdbx_lck_init(env);
    if (err)
      return err;

    env->me_lck->mti_magic = MDB_MAGIC;
    env->me_lck->mti_format = MDB_LOCK_FORMAT;
  } else {
    if (env->me_lck->mti_magic != MDB_MAGIC) {
      mdbx_debug("lock region has invalid magic");
      return MDB_INVALID;
    }
    if (env->me_lck->mti_format != MDB_LOCK_FORMAT) {
      mdbx_debug("lock region has format+version 0x%" PRIx64
                 ", expected 0x%" PRIx64,
                 env->me_lck->mti_format, MDB_LOCK_FORMAT);
      return MDB_VERSION_MISMATCH;
    }
  }

  return rc;
}

/** Only a subset of the @ref mdbx_env flags can be changed
 *	at runtime. Changing other flags requires closing the
 *	environment and re-opening it with the new flags.
 */
#define CHANGEABLE                                                             \
  (MDB_NOSYNC | MDB_NOMETASYNC | MDB_MAPASYNC | MDB_NOMEMINIT |                \
   MDBX_COALESCE | MDBX_PAGEPERTURB)
#define CHANGELESS                                                             \
  (MDB_NOSUBDIR | MDB_RDONLY | MDB_WRITEMAP | MDB_NOTLS | MDB_NORDAHEAD |      \
   MDBX_LIFORECLAIM)

#if VALID_FLAGS & PERSISTENT_FLAGS & (CHANGEABLE | CHANGELESS)
#error "Persistent DB flags & env flags overlap, but both go in mm_flags"
#endif

int __cold mdbx_env_open_ex(MDB_env *env, const char *path, unsigned flags,
                            mode_t mode, int *exclusive) {
  int oflags, rc, len;
  char *lck_pathname, *dxb_pathname;

  if (unlikely(!env || !path))
    return MDBX_EINVAL;

  if (unlikely(env->me_signature != MDBX_ME_SIGNATURE))
    return MDBX_EBADSIGN;

  if (env->me_fd != INVALID_HANDLE_VALUE ||
      (flags & ~(CHANGEABLE | CHANGELESS)))
    return MDBX_EINVAL;

  len = strlen(path);
  if (flags & MDB_NOSUBDIR) {
    rc = len + sizeof(MDBX_LOCK_SUFFIX) + len + 1;
  } else {
    rc = len + sizeof(MDBX_LOCKNAME) + len + sizeof(MDBX_DATANAME);
  }
  lck_pathname = malloc(rc);
  if (!lck_pathname)
    return MDBX_ENOMEM;

  if (flags & MDB_NOSUBDIR) {
    dxb_pathname = lck_pathname + len + sizeof(MDBX_LOCK_SUFFIX);
    sprintf(lck_pathname, "%s" MDBX_LOCK_SUFFIX, path);
    strcpy(dxb_pathname, path);
  } else {
    dxb_pathname = lck_pathname + len + sizeof(MDBX_LOCKNAME);
    sprintf(lck_pathname, "%s" MDBX_LOCKNAME, path);
    sprintf(dxb_pathname, "%s" MDBX_DATANAME, path);
  }

  rc = MDB_SUCCESS;
  flags |= env->me_flags;
  if (flags & MDB_RDONLY) {
    /* LY: silently ignore irrelevant flags when we're only getting read
     * access */
    flags &= ~(MDB_WRITEMAP | MDB_MAPASYNC | MDB_NOSYNC | MDB_NOMETASYNC |
               MDBX_COALESCE | MDBX_LIFORECLAIM | MDB_NOMEMINIT);
  } else {
    if (!((env->me_free_pgs = mdbx_midl_alloc(MDB_IDL_UM_MAX)) &&
          (env->me_dirty_list = calloc(MDB_IDL_UM_SIZE, sizeof(MDB_ID2)))))
      rc = MDBX_ENOMEM;
  }
  env->me_flags = flags |= MDB_ENV_ACTIVE;
  if (rc)
    goto bailout;

  env->me_path = mdbx_strdup(path);
  env->me_dbxs = calloc(env->me_maxdbs, sizeof(MDB_dbx));
  env->me_dbflags = calloc(env->me_maxdbs, sizeof(uint16_t));
  env->me_dbiseqs = calloc(env->me_maxdbs, sizeof(unsigned));
  if (!(env->me_dbxs && env->me_path && env->me_dbflags && env->me_dbiseqs)) {
    rc = MDBX_ENOMEM;
    goto bailout;
  }
  env->me_dbxs[FREE_DBI].md_cmp = mdbx_cmp_int_ai; /* aligned MDB_INTEGERKEY */

  if (F_ISSET(flags, MDB_RDONLY))
    oflags = O_RDONLY;
  else
    oflags = O_RDWR | O_CREAT;

  rc = mdbx_openfile(dxb_pathname, oflags, mode, &env->me_fd);
  if (rc != MDB_SUCCESS)
    goto bailout;

  const int lck_rc = mdbx_setup_lck(env, lck_pathname, mode);
  if (MDBX_IS_ERROR(lck_rc)) {
    rc = lck_rc;
    goto bailout;
  }

  MDB_meta meta;
  const int dxb_rc = mdbx_setup_dxb(env, &meta, lck_rc);
  if (MDBX_IS_ERROR(dxb_rc)) {
    rc = dxb_rc;
    goto bailout;
  }

  mdbx_debug("opened dbenv %p", (void *)env);
  const unsigned mode_flags =
      MDB_WRITEMAP | MDB_NOSYNC | MDB_NOMETASYNC | MDB_MAPASYNC;
  if (lck_rc == MDBX_RESULT_TRUE) {
    env->me_lck->mti_envmode = env->me_flags & mode_flags;
    if (exclusive == NULL || *exclusive < 2) {
      /* LY: downgrade lock only if exclusive access not requested.
       *     in case exclusive==1, just leave value as is. */
      rc = mdbx_lck_downgrade(env);
      mdbx_debug("lck-downgrade: rc %i ", rc);
      if (rc != MDB_SUCCESS)
        goto bailout;
    }
  } else {
    if (exclusive) {
      /* LY: just indicate that is not an exclusive access. */
      *exclusive = 0;
    }
    if ((env->me_lck->mti_envmode ^ env->me_flags) & mode_flags) {
      /* LY: Current mode/flags incompatible with requested. */
      rc = MDB_INCOMPATIBLE;
      goto bailout;
    }
  }

  if ((env->me_flags & MDB_NOTLS) == 0) {
    rc = mdbx_rthc_alloc(&env->me_txkey, &env->me_lck->mti_readers[0],
                         &env->me_lck->mti_readers[env->me_maxreaders]);
    if (unlikely(rc != MDB_SUCCESS))
      return rc;
    env->me_flags |= MDB_ENV_TXKEY;
  }

  if ((flags & MDB_RDONLY) == 0) {
    MDB_txn *txn;
    int tsize = sizeof(MDB_txn),
        size = tsize +
               env->me_maxdbs * (sizeof(MDB_db) + sizeof(MDB_cursor *) +
                                 sizeof(unsigned) + 1);
    if ((env->me_pbuf = calloc(1, env->me_psize)) && (txn = calloc(1, size))) {
      txn->mt_dbs = (MDB_db *)((char *)txn + tsize);
      txn->mt_cursors = (MDB_cursor **)(txn->mt_dbs + env->me_maxdbs);
      txn->mt_dbiseqs = (unsigned *)(txn->mt_cursors + env->me_maxdbs);
      txn->mt_dbflags = (unsigned char *)(txn->mt_dbiseqs + env->me_maxdbs);
      txn->mt_env = env;
      txn->mt_dbxs = env->me_dbxs;
      txn->mt_flags = MDB_TXN_FINISHED;
      env->me_txn0 = txn;
    } else {
      rc = MDBX_ENOMEM;
    }
  }

#if MDB_DEBUG
  if (rc == MDB_SUCCESS) {
    MDB_meta *meta = mdbx_meta_head(env);
    MDB_db *db = &meta->mm_dbs[MAIN_DBI];
    int toggle = ((char *)meta == PAGEDATA(env->me_map)) ? 0 : 1;

    mdbx_debug("opened database version %u, pagesize %u", meta->mm_version,
               env->me_psize);
    mdbx_debug("using meta page %d, txn %" PRIuPTR "", toggle, meta->mm_txnid);
    mdbx_debug("depth: %u", db->md_depth);
    mdbx_debug("entries: %" PRIuPTR "", db->md_entries);
    mdbx_debug("branch pages: %" PRIuPTR "", db->md_branch_pages);
    mdbx_debug("leaf pages: %" PRIuPTR "", db->md_leaf_pages);
    mdbx_debug("overflow pages: %" PRIuPTR "", db->md_overflow_pages);
    mdbx_debug("root: %" PRIuPTR "", db->md_root);
  }
#endif

bailout:
  if (rc)
    mdbx_env_close0(env);
  free(lck_pathname);
  return rc;
}

int __cold mdbx_env_open(MDB_env *env, const char *path, unsigned flags,
                         mode_t mode) {
  return mdbx_env_open_ex(env, path, flags, mode, NULL);
}

/** Destroy resources from mdbx_env_open(), clear our readers & DBIs */
static void __cold mdbx_env_close0(MDB_env *env) {
  if (!(env->me_flags & MDB_ENV_ACTIVE))
    return;
  env->me_flags &= ~MDB_ENV_ACTIVE;

  /* Doing this here since me_dbxs may not exist during mdbx_env_close */
  if (env->me_dbxs) {
    for (unsigned i = env->me_maxdbs; --i >= CORE_DBS;)
      free(env->me_dbxs[i].md_name.mv_data);
    free(env->me_dbxs);
  }

  free(env->me_pbuf);
  free(env->me_dbiseqs);
  free(env->me_dbflags);
  free(env->me_path);
  free(env->me_dirty_list);
  if (env->me_txn0)
    mdbx_midl_free(env->me_txn0->mt_lifo_reclaimed);
  free(env->me_txn0);
  mdbx_midl_free(env->me_free_pgs);

  if (env->me_flags & MDB_ENV_TXKEY) {
    mdbx_rthc_remove(env->me_txkey);
    env->me_flags &= ~MDB_ENV_TXKEY;
  }

  if (env->me_map) {
    mdbx_munmap(env->me_map, env->me_mapsize);
#ifdef USE_VALGRIND
    VALGRIND_DISCARD(env->me_valgrind_handle);
    env->me_valgrind_handle = -1;
#endif
  }
  if (env->me_fd != INVALID_HANDLE_VALUE) {
    (void)mdbx_closefile(env->me_fd);
    env->me_fd = INVALID_HANDLE_VALUE;
  }

  mdbx_munmap((void *)env->me_lck,
              (env->me_maxreaders - 1) * sizeof(MDB_reader) +
                  sizeof(MDBX_lockinfo));
  env->me_lck = NULL;
  env->me_pid = 0;

  mdbx_lck_destroy(env);
  if (env->me_lfd != INVALID_HANDLE_VALUE) {
    (void)mdbx_closefile(env->me_lfd);
    env->me_lfd = INVALID_HANDLE_VALUE;
  }
}

int __cold mdbx_env_close_ex(MDB_env *env, int dont_sync) {
  MDB_page *dp;
  int rc = MDB_SUCCESS;

  if (unlikely(!env))
    return MDBX_EINVAL;
  if (unlikely(env->me_signature != MDBX_ME_SIGNATURE))
    return MDBX_EBADSIGN;

  if (!dont_sync && env->me_lck)
    rc = mdbx_env_sync(env, 1);

  VALGRIND_DESTROY_MEMPOOL(env);
  while ((dp = env->me_dpages) != NULL) {
    ASAN_UNPOISON_MEMORY_REGION(&dp->mp_next, sizeof(dp->mp_next));
    VALGRIND_MAKE_MEM_DEFINED(&dp->mp_next, sizeof(dp->mp_next));
    env->me_dpages = dp->mp_next;
    free(dp);
  }

  mdbx_env_close0(env);
  env->me_signature = 0;
  free(env);

  return rc;
}

void __cold mdbx_env_close(MDB_env *env) { mdbx_env_close_ex(env, 0); }

/* LY: fast enough on most arches
 *
 *                /
 *                | -1, a < b
 * cmp2int(a,b) = <  0, a == b
 *                |  1, a > b
 *                \
 */
#if 1
#define mdbx_cmp2int(a, b) (((b) > (a)) ? -1 : (a) > (b))
#else
#define mdbx_cmp2int(a, b) (((a) > (b)) - ((b) > (a)))
#endif

/** Compare two items pointing at aligned unsigned int's. */
static int __hot mdbx_cmp_int_ai(const MDB_val *a, const MDB_val *b) {
  mdbx_assert(NULL, a->mv_size == b->mv_size);
  mdbx_assert(NULL, 0 == (uintptr_t)a->mv_data % sizeof(int) &&
                        0 == (uintptr_t)b->mv_data % sizeof(int));
  switch (a->mv_size) {
  case 4:
    return mdbx_cmp2int(*(uint32_t *)a->mv_data, *(uint32_t *)b->mv_data);
  case 8:
    return mdbx_cmp2int(*(uint64_t *)a->mv_data, *(uint64_t *)b->mv_data);
  default:
    mdbx_assert_fail(NULL, "invalid size for INTEGERKEY/INTEGERDUP", mdbx_func_,
                     __LINE__);
    return 0;
  }
}

/** Compare two items pointing at 2-byte aligned unsigned int's. */
static int __hot mdbx_cmp_int_a2(const MDB_val *a, const MDB_val *b) {
  mdbx_assert(NULL, a->mv_size == b->mv_size);
  mdbx_assert(NULL, 0 == (uintptr_t)a->mv_data % sizeof(uint16_t) &&
                        0 == (uintptr_t)b->mv_data % sizeof(uint16_t));
#if UNALIGNED_OK
  switch (a->mv_size) {
  case 4:
    return mdbx_cmp2int(*(uint32_t *)a->mv_data, *(uint32_t *)b->mv_data);
  case 8:
    return mdbx_cmp2int(*(uint64_t *)a->mv_data, *(uint64_t *)b->mv_data);
  default:
    mdbx_assert_fail(NULL, "invalid size for INTEGERKEY/INTEGERDUP", mdbx_func_,
                     __LINE__);
    return 0;
  }
#else
  mdbx_assert(NULL, 0 == a->mv_size % sizeof(uint16_t));
  {
    int diff;
    const uint16_t *pa, *pb, *end;

#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    end = (const uint16_t *)a->mv_data;
    pa = (const uint16_t *)((char *)a->mv_data + a->mv_size);
    pb = (const uint16_t *)((char *)b->mv_data + a->mv_size);
    do {
      diff = *--pa - *--pb;
#else  /* __BYTE_ORDER__ */
    end = (const uint16_t *)((char *)a->mv_data + a->mv_size);
    pa = (const uint16_t *)a->mv_data;
    pb = (const uint16_t *)b->mv_data;
    do {
      diff = *pa++ - *pb++;
#endif /* __BYTE_ORDER__ */
      if (likely(diff != 0))
        break;
    } while (pa != end);
    return diff;
  }
#endif /* UNALIGNED_OK */
}

/** Compare two items pointing at unsigneds of unknown alignment.
 *
 *	This is also set as #MDB_INTEGERDUP|#MDB_DUPFIXED's #MDB_dbx.%md_dcmp.
 */
static int __hot mdbx_cmp_int_ua(const MDB_val *a, const MDB_val *b) {
  mdbx_assert(NULL, a->mv_size == b->mv_size);
#if UNALIGNED_OK
  switch (a->mv_size) {
  case 4:
    return mdbx_cmp2int(*(uint32_t *)a->mv_data, *(uint32_t *)b->mv_data);
  case 8:
    return mdbx_cmp2int(*(uint64_t *)a->mv_data, *(uint64_t *)b->mv_data);
  default:
    mdbx_assert_fail(NULL, "invalid size for INTEGERKEY/INTEGERDUP", mdbx_func_,
                     __LINE__);
    return 0;
  }
#else
  mdbx_assert(NULL, a->mv_size == sizeof(int) || a->mv_size == sizeof(size_t));
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
  {
    int diff;
    const uint8_t *pa, *pb;

    pa = (const uint8_t *)a->mv_data + a->mv_size;
    pb = (const uint8_t *)b->mv_data + a->mv_size;

    do {
      diff = *--pa - *--pb;
      if (likely(diff != 0))
        break;
    } while (pa != a->mv_data);
    return diff;
  }
#else  /* __BYTE_ORDER__ */
  return memcmp(a->mv_data, b->mv_data, a->mv_size);
#endif /* __BYTE_ORDER__ */
#endif /* UNALIGNED_OK */
}

/** Compare two items lexically */
static int __hot mdbx_cmp_memn(const MDB_val *a, const MDB_val *b) {
/* LY: assumes that length of keys are NOT equal for most cases,
 * if no then branch-prediction should mitigate the problem */
#if 0
	/* LY: without branch instructions on x86,
	 * but isn't best for equal length of keys */
	int diff_len = mdbx_cmp2int(a->mv_size, b->mv_size);
#else
  /* LY: best when length of keys are equal,
   * but got a branch-penalty otherwise */
  if (unlikely(a->mv_size == b->mv_size))
    return memcmp(a->mv_data, b->mv_data, a->mv_size);
  int diff_len = (a->mv_size < b->mv_size) ? -1 : 1;
#endif
  size_t shortest = (a->mv_size < b->mv_size) ? a->mv_size : b->mv_size;
  int diff_data = memcmp(a->mv_data, b->mv_data, shortest);
  return likely(diff_data) ? diff_data : diff_len;
}

/** Compare two items in reverse byte order */
static int __hot mdbx_cmp_memnr(const MDB_val *a, const MDB_val *b) {
  const uint8_t *pa, *pb, *end;

  pa = (const uint8_t *)a->mv_data + a->mv_size;
  pb = (const uint8_t *)b->mv_data + b->mv_size;
  size_t minlen = (a->mv_size < b->mv_size) ? a->mv_size : b->mv_size;
  end = pa - minlen;

  while (pa != end) {
    int diff = *--pa - *--pb;
    if (likely(diff))
      return diff;
  }
  return mdbx_cmp2int(a->mv_size, b->mv_size);
}

/** Search for key within a page, using binary search.
 * Returns the smallest entry larger or equal to the key.
 * If exactp is non-null, stores whether the found entry was an exact match
 * in *exactp (1 or 0).
 * Updates the cursor index with the index of the found entry.
 * If no entry larger or equal to the key is found, returns NULL.
 */
static MDB_node *__hot mdbx_node_search(MDB_cursor *mc, MDB_val *key,
                                        int *exactp) {
  unsigned i = 0, nkeys;
  int low, high;
  int rc = 0;
  MDB_page *mp = mc->mc_pg[mc->mc_top];
  MDB_node *node = NULL;
  MDB_val nodekey;
  MDB_cmp_func *cmp;
  DKBUF;

  nkeys = NUMKEYS(mp);

  mdbx_debug("searching %u keys in %s %spage %" PRIuPTR "", nkeys,
             IS_LEAF(mp) ? "leaf" : "branch", IS_SUBP(mp) ? "sub-" : "",
             mdbx_dbg_pgno(mp));

  low = IS_LEAF(mp) ? 0 : 1;
  high = nkeys - 1;
  cmp = mc->mc_dbx->md_cmp;

  /* Branch pages have no data, so if using integer keys,
   * alignment is guaranteed. Use faster mdbx_cmp_int_ai.
   */
  if (cmp == mdbx_cmp_int_a2 && IS_BRANCH(mp))
    cmp = mdbx_cmp_int_ai;

  if (IS_LEAF2(mp)) {
    nodekey.mv_size = mc->mc_db->md_xsize;
    node = NODEPTR(mp, 0); /* fake */
    while (low <= high) {
      i = (low + high) >> 1;
      nodekey.mv_data = LEAF2KEY(mp, i, nodekey.mv_size);
      rc = cmp(key, &nodekey);
      mdbx_debug("found leaf index %u [%s], rc = %i", i, DKEY(&nodekey), rc);
      if (rc == 0)
        break;
      if (rc > 0)
        low = i + 1;
      else
        high = i - 1;
    }
  } else {
    while (low <= high) {
      i = (low + high) >> 1;

      node = NODEPTR(mp, i);
      nodekey.mv_size = NODEKSZ(node);
      nodekey.mv_data = NODEKEY(node);

      rc = cmp(key, &nodekey);
      if (IS_LEAF(mp))
        mdbx_debug("found leaf index %u [%s], rc = %i", i, DKEY(&nodekey), rc);
      else
        mdbx_debug("found branch index %u [%s -> %" PRIuPTR "], rc = %i", i,
                   DKEY(&nodekey), NODEPGNO(node), rc);
      if (rc == 0)
        break;
      if (rc > 0)
        low = i + 1;
      else
        high = i - 1;
    }
  }

  if (rc > 0) /* Found entry is less than the key. */
    i++;      /* Skip to get the smallest entry larger than key. */

  if (exactp)
    *exactp = (rc == 0 && nkeys > 0);
  /* store the key index */
  mc->mc_ki[mc->mc_top] = i;
  if (i >= nkeys)
    /* There is no entry larger or equal to the key. */
    return NULL;

  /* nodeptr is fake for LEAF2 */
  return IS_LEAF2(mp) ? node : NODEPTR(mp, i);
}

#if 0
static void
mdbx_cursor_adjust(MDB_cursor *mc, func)
{
	MDB_cursor *m2;

	for (m2 = mc->mc_txn->mt_cursors[mc->mc_dbi]; m2; m2=m2->mc_next) {
		if (m2->mc_pg[m2->mc_top] == mc->mc_pg[mc->mc_top]) {
			func(mc, m2);
		}
	}
}
#endif

/** Pop a page off the top of the cursor's stack. */
static void mdbx_cursor_pop(MDB_cursor *mc) {
  if (mc->mc_snum) {
    mdbx_debug("popped page %" PRIuPTR " off db %d cursor %p",
               mc->mc_pg[mc->mc_top]->mp_pgno, DDBI(mc), (void *)mc);

    mc->mc_snum--;
    if (mc->mc_snum) {
      mc->mc_top--;
    } else {
      mc->mc_flags &= ~C_INITIALIZED;
    }
  }
}

/** Push a page onto the top of the cursor's stack.
 * Set #MDB_TXN_ERROR on failure.
 */
static int mdbx_cursor_push(MDB_cursor *mc, MDB_page *mp) {
  mdbx_debug("pushing page %" PRIuPTR " on db %d cursor %p", mp->mp_pgno,
             DDBI(mc), (void *)mc);

  if (unlikely(mc->mc_snum >= CURSOR_STACK)) {
    mc->mc_txn->mt_flags |= MDB_TXN_ERROR;
    return MDB_CURSOR_FULL;
  }

  mc->mc_top = mc->mc_snum++;
  mc->mc_pg[mc->mc_top] = mp;
  mc->mc_ki[mc->mc_top] = 0;

  return MDB_SUCCESS;
}

/** Find the address of the page corresponding to a given page number.
 * Set #MDB_TXN_ERROR on failure.
 * @param[in] mc the cursor accessing the page.
 * @param[in] pgno the page number for the page to retrieve.
 * @param[out] ret address of a pointer where the page's address will be
 * stored.
 * @param[out] lvl dirty_list inheritance level of found page. 1=current txn,
 * 0=mapped page.
 * @return 0 on success, non-zero on failure.
 */
static int mdbx_page_get(MDB_cursor *mc, pgno_t pgno, MDB_page **ret,
                         int *lvl) {
  MDB_txn *txn = mc->mc_txn;
  MDB_env *env = txn->mt_env;
  MDB_page *p = NULL;
  int level;

  if (!(txn->mt_flags & (MDB_TXN_RDONLY | MDB_TXN_WRITEMAP))) {
    MDB_txn *tx2 = txn;
    level = 1;
    do {
      MDB_ID2L dl = tx2->mt_u.dirty_list;
      unsigned x;
      /* Spilled pages were dirtied in this txn and flushed
       * because the dirty list got full. Bring this page
       * back in from the map (but don't unspill it here,
       * leave that unless page_touch happens again). */
      if (tx2->mt_spill_pgs) {
        MDB_ID pn = pgno << 1;
        x = mdbx_midl_search(tx2->mt_spill_pgs, pn);
        if (x <= tx2->mt_spill_pgs[0] && tx2->mt_spill_pgs[x] == pn)
          goto mapped;
      }
      if (dl[0].mid) {
        unsigned y = mdbx_mid2l_search(dl, pgno);
        if (y <= dl[0].mid && dl[y].mid == pgno) {
          p = dl[y].mptr;
          goto done;
        }
      }
      level++;
    } while ((tx2 = tx2->mt_parent) != NULL);
  }

  if (unlikely(pgno >= txn->mt_next_pgno)) {
    mdbx_debug("page %" PRIuPTR " not found", pgno);
    txn->mt_flags |= MDB_TXN_ERROR;
    return MDB_PAGE_NOTFOUND;
  }
  level = 0;

mapped:
  p = (MDB_page *)(env->me_map + env->me_psize * pgno);

done:
  *ret = p;
  if (lvl)
    *lvl = level;
  return MDB_SUCCESS;
}

/** Finish #mdbx_page_search() / #mdbx_page_search_lowest().
 *	The cursor is at the root page, set up the rest of it.
 */
static int mdbx_page_search_root(MDB_cursor *mc, MDB_val *key, int flags) {
  MDB_page *mp = mc->mc_pg[mc->mc_top];
  int rc;
  DKBUF;

  while (IS_BRANCH(mp)) {
    MDB_node *node;
    indx_t i;

    mdbx_debug("branch page %" PRIuPTR " has %u keys", mp->mp_pgno,
               NUMKEYS(mp));
    /* Don't assert on branch pages in the FreeDB. We can get here
     * while in the process of rebalancing a FreeDB branch page; we must
     * let that proceed. ITS#8336
     */
    mdbx_cassert(mc, !mc->mc_dbi || NUMKEYS(mp) > 1);
    mdbx_debug("found index 0 to page %" PRIuPTR "", NODEPGNO(NODEPTR(mp, 0)));

    if (flags & (MDB_PS_FIRST | MDB_PS_LAST)) {
      i = 0;
      if (flags & MDB_PS_LAST) {
        i = NUMKEYS(mp) - 1;
        /* if already init'd, see if we're already in right place */
        if (mc->mc_flags & C_INITIALIZED) {
          if (mc->mc_ki[mc->mc_top] == i) {
            mc->mc_top = mc->mc_snum++;
            mp = mc->mc_pg[mc->mc_top];
            goto ready;
          }
        }
      }
    } else {
      int exact;
      node = mdbx_node_search(mc, key, &exact);
      if (node == NULL)
        i = NUMKEYS(mp) - 1;
      else {
        i = mc->mc_ki[mc->mc_top];
        if (!exact) {
          mdbx_cassert(mc, i > 0);
          i--;
        }
      }
      mdbx_debug("following index %u for key [%s]", i, DKEY(key));
    }

    mdbx_cassert(mc, i < NUMKEYS(mp));
    node = NODEPTR(mp, i);

    if (unlikely((rc = mdbx_page_get(mc, NODEPGNO(node), &mp, NULL)) != 0))
      return rc;

    mc->mc_ki[mc->mc_top] = i;
    if (unlikely(rc = mdbx_cursor_push(mc, mp)))
      return rc;

  ready:
    if (flags & MDB_PS_MODIFY) {
      if (unlikely((rc = mdbx_page_touch(mc)) != 0))
        return rc;
      mp = mc->mc_pg[mc->mc_top];
    }
  }

  if (unlikely(!IS_LEAF(mp))) {
    mdbx_debug("internal error, index points to a %02X page!?", mp->mp_flags);
    mc->mc_txn->mt_flags |= MDB_TXN_ERROR;
    return MDB_CORRUPTED;
  }

  mdbx_debug("found leaf page %" PRIuPTR " for key [%s]", mp->mp_pgno,
             key ? DKEY(key) : "null");
  mc->mc_flags |= C_INITIALIZED;
  mc->mc_flags &= ~C_EOF;

  return MDB_SUCCESS;
}

/** Search for the lowest key under the current branch page.
 * This just bypasses a NUMKEYS check in the current page
 * before calling mdbx_page_search_root(), because the callers
 * are all in situations where the current page is known to
 * be underfilled.
 */
static int mdbx_page_search_lowest(MDB_cursor *mc) {
  MDB_page *mp = mc->mc_pg[mc->mc_top];
  MDB_node *node = NODEPTR(mp, 0);
  int rc;

  if (unlikely((rc = mdbx_page_get(mc, NODEPGNO(node), &mp, NULL)) != 0))
    return rc;

  mc->mc_ki[mc->mc_top] = 0;
  if (unlikely(rc = mdbx_cursor_push(mc, mp)))
    return rc;
  return mdbx_page_search_root(mc, NULL, MDB_PS_FIRST);
}

/** Search for the page a given key should be in.
 * Push it and its parent pages on the cursor stack.
 * @param[in,out] mc the cursor for this operation.
 * @param[in] key the key to search for, or NULL for first/last page.
 * @param[in] flags If MDB_PS_MODIFY is set, visited pages in the DB
 *   are touched (updated with new page numbers).
 *   If MDB_PS_FIRST or MDB_PS_LAST is set, find first or last leaf.
 *   This is used by #mdbx_cursor_first() and #mdbx_cursor_last().
 *   If MDB_PS_ROOTONLY set, just fetch root node, no further lookups.
 * @return 0 on success, non-zero on failure.
 */
static int mdbx_page_search(MDB_cursor *mc, MDB_val *key, int flags) {
  int rc;
  pgno_t root;

  /* Make sure the txn is still viable, then find the root from
   * the txn's db table and set it as the root of the cursor's stack.
   */
  if (unlikely(mc->mc_txn->mt_flags & MDB_TXN_BLOCKED)) {
    mdbx_debug("transaction has failed, must abort");
    return MDB_BAD_TXN;
  } else {
    /* Make sure we're using an up-to-date root */
    if (unlikely(*mc->mc_dbflag & DB_STALE)) {
      MDB_cursor mc2;
      if (unlikely(TXN_DBI_CHANGED(mc->mc_txn, mc->mc_dbi)))
        return MDB_BAD_DBI;
      mdbx_cursor_init(&mc2, mc->mc_txn, MAIN_DBI, NULL);
      rc = mdbx_page_search(&mc2, &mc->mc_dbx->md_name, 0);
      if (rc)
        return rc;
      {
        MDB_val data;
        int exact = 0;
        MDB_node *leaf = mdbx_node_search(&mc2, &mc->mc_dbx->md_name, &exact);
        if (!exact)
          return MDB_NOTFOUND;
        if (unlikely((leaf->mn_flags & (F_DUPDATA | F_SUBDATA)) != F_SUBDATA))
          return MDB_INCOMPATIBLE; /* not a named DB */
        rc = mdbx_node_read(&mc2, leaf, &data);
        if (rc)
          return rc;

        uint16_t md_flags;
        memcpy(&md_flags, ((char *)data.mv_data + offsetof(MDB_db, md_flags)),
               sizeof(uint16_t));
        /* The txn may not know this DBI, or another process may
         * have dropped and recreated the DB with other flags.
         */
        if (unlikely((mc->mc_db->md_flags & PERSISTENT_FLAGS) != md_flags))
          return MDB_INCOMPATIBLE;
        memcpy(mc->mc_db, data.mv_data, sizeof(MDB_db));
      }
      *mc->mc_dbflag &= ~DB_STALE;
    }
    root = mc->mc_db->md_root;

    if (unlikely(root == P_INVALID)) { /* Tree is empty. */
      mdbx_debug("tree is empty");
      return MDB_NOTFOUND;
    }
  }

  mdbx_cassert(mc, root > 1);
  if (!mc->mc_pg[0] || mc->mc_pg[0]->mp_pgno != root)
    if (unlikely((rc = mdbx_page_get(mc, root, &mc->mc_pg[0], NULL)) != 0))
      return rc;

  mc->mc_snum = 1;
  mc->mc_top = 0;

  mdbx_debug("db %d root page %" PRIuPTR " has flags 0x%X", DDBI(mc), root,
             mc->mc_pg[0]->mp_flags);

  if (flags & MDB_PS_MODIFY) {
    if (unlikely(rc = mdbx_page_touch(mc)))
      return rc;
  }

  if (flags & MDB_PS_ROOTONLY)
    return MDB_SUCCESS;

  return mdbx_page_search_root(mc, key, flags);
}

static int mdbx_ovpage_free(MDB_cursor *mc, MDB_page *mp) {
  MDB_txn *txn = mc->mc_txn;
  pgno_t pg = mp->mp_pgno;
  unsigned x = 0, ovpages = mp->mp_pages;
  MDB_env *env = txn->mt_env;
  MDB_IDL sl = txn->mt_spill_pgs;
  MDB_ID pn = pg << 1;
  int rc;

  mdbx_debug("free ov page %" PRIuPTR " (%u)", pg, ovpages);
  /* If the page is dirty or on the spill list we just acquired it,
   * so we should give it back to our current free list, if any.
   * Otherwise put it onto the list of pages we freed in this txn.
   *
   * Won't create me_pghead: me_pglast must be inited along with it.
   * Unsupported in nested txns: They would need to hide the page
   * range in ancestor txns' dirty and spilled lists.
   */
  if (env->me_pghead && !txn->mt_parent &&
      ((mp->mp_flags & P_DIRTY) ||
       (sl && (x = mdbx_midl_search(sl, pn)) <= sl[0] && sl[x] == pn))) {
    unsigned i, j;
    pgno_t *mop;
    MDB_ID2 *dl, ix, iy;
    rc = mdbx_midl_need(&env->me_pghead, ovpages);
    if (unlikely(rc))
      return rc;
    if (!(mp->mp_flags & P_DIRTY)) {
      /* This page is no longer spilled */
      if (x == sl[0])
        sl[0]--;
      else
        sl[x] |= 1;
      goto release;
    }
    /* Remove from dirty list */
    dl = txn->mt_u.dirty_list;
    x = dl[0].mid--;
    for (ix = dl[x]; ix.mptr != mp; ix = iy) {
      if (likely(x > 1)) {
        x--;
        iy = dl[x];
        dl[x] = ix;
      } else {
        mdbx_cassert(mc, x > 1);
        j = ++(dl[0].mid);
        dl[j] = ix; /* Unsorted. OK when MDB_TXN_ERROR. */
        txn->mt_flags |= MDB_TXN_ERROR;
        return MDB_PROBLEM;
      }
    }
    txn->mt_dirty_room++;
    if (!(env->me_flags & MDB_WRITEMAP))
      mdbx_dpage_free(env, mp);
  release:
    /* Insert in me_pghead */
    mop = env->me_pghead;
    j = mop[0] + ovpages;
    for (i = mop[0]; i && mop[i] < pg; i--)
      mop[j--] = mop[i];
    while (j > i)
      mop[j--] = pg++;
    mop[0] += ovpages;
  } else {
    rc = mdbx_midl_append_range(&txn->mt_free_pgs, pg, ovpages);
    if (unlikely(rc))
      return rc;
  }
  mc->mc_db->md_overflow_pages -= ovpages;
  return 0;
}

/** Return the data associated with a given node.
 * @param[in] mc The cursor for this operation.
 * @param[in] leaf The node being read.
 * @param[out] data Updated to point to the node's data.
 * @return 0 on success, non-zero on failure.
 */
static __inline int mdbx_node_read(MDB_cursor *mc, MDB_node *leaf,
                                   MDB_val *data) {
  MDB_page *omp; /* overflow page */
  pgno_t pgno;
  int rc;

  if (!F_ISSET(leaf->mn_flags, F_BIGDATA)) {
    data->mv_size = NODEDSZ(leaf);
    data->mv_data = NODEDATA(leaf);
    return MDB_SUCCESS;
  }

  /* Read overflow data.
   */
  data->mv_size = NODEDSZ(leaf);
  memcpy(&pgno, NODEDATA(leaf), sizeof(pgno));
  if (unlikely((rc = mdbx_page_get(mc, pgno, &omp, NULL)) != 0)) {
    mdbx_debug("read overflow page %" PRIuPTR " failed", pgno);
    return rc;
  }
  data->mv_data = PAGEDATA(omp);

  return MDB_SUCCESS;
}

int mdbx_get(MDB_txn *txn, MDB_dbi dbi, MDB_val *key, MDB_val *data) {
  MDB_cursor mc;
  MDB_xcursor mx;
  int exact = 0;
  DKBUF;

  mdbx_debug("===> get db %u key [%s]", dbi, DKEY(key));

  if (unlikely(!key || !data || !txn))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_signature != MDBX_MT_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(!TXN_DBI_EXIST(txn, dbi, DB_USRVALID)))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_flags & MDB_TXN_BLOCKED))
    return MDB_BAD_TXN;

  mdbx_cursor_init(&mc, txn, dbi, &mx);
  return mdbx_cursor_set(&mc, key, data, MDB_SET, &exact);
}

/** Find a sibling for a page.
 * Replaces the page at the top of the cursor's stack with the
 * specified sibling, if one exists.
 * @param[in] mc The cursor for this operation.
 * @param[in] move_right Non-zero if the right sibling is requested,
 * otherwise the left sibling.
 * @return 0 on success, non-zero on failure.
 */
static int mdbx_cursor_sibling(MDB_cursor *mc, int move_right) {
  int rc;
  MDB_node *indx;
  MDB_page *mp;

  if (unlikely(mc->mc_snum < 2)) {
    return MDB_NOTFOUND; /* root has no siblings */
  }

  mdbx_cursor_pop(mc);
  mdbx_debug("parent page is page %" PRIuPTR ", index %u",
             mc->mc_pg[mc->mc_top]->mp_pgno, mc->mc_ki[mc->mc_top]);

  if (move_right
          ? (mc->mc_ki[mc->mc_top] + 1u >= NUMKEYS(mc->mc_pg[mc->mc_top]))
          : (mc->mc_ki[mc->mc_top] == 0)) {
    mdbx_debug("no more keys left, moving to %s sibling",
               move_right ? "right" : "left");
    if (unlikely((rc = mdbx_cursor_sibling(mc, move_right)) != MDB_SUCCESS)) {
      /* undo cursor_pop before returning */
      mc->mc_top++;
      mc->mc_snum++;
      return rc;
    }
  } else {
    if (move_right)
      mc->mc_ki[mc->mc_top]++;
    else
      mc->mc_ki[mc->mc_top]--;
    mdbx_debug("just moving to %s index key %u", move_right ? "right" : "left",
               mc->mc_ki[mc->mc_top]);
  }
  mdbx_cassert(mc, IS_BRANCH(mc->mc_pg[mc->mc_top]));

  indx = NODEPTR(mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top]);
  if (unlikely((rc = mdbx_page_get(mc, NODEPGNO(indx), &mp, NULL)) != 0)) {
    /* mc will be inconsistent if caller does mc_snum++ as above */
    mc->mc_flags &= ~(C_INITIALIZED | C_EOF);
    return rc;
  }

  mdbx_cursor_push(mc, mp);
  if (!move_right)
    mc->mc_ki[mc->mc_top] = NUMKEYS(mp) - 1;

  return MDB_SUCCESS;
}

/** Move the cursor to the next data item. */
static int mdbx_cursor_next(MDB_cursor *mc, MDB_val *key, MDB_val *data,
                            MDB_cursor_op op) {
  MDB_page *mp;
  MDB_node *leaf;
  int rc;

  if ((mc->mc_flags & C_DEL) && op == MDB_NEXT_DUP)
    return MDB_NOTFOUND;

  if (!(mc->mc_flags & C_INITIALIZED))
    return mdbx_cursor_first(mc, key, data);

  mp = mc->mc_pg[mc->mc_top];
  if (mc->mc_flags & C_EOF) {
    if (mc->mc_ki[mc->mc_top] + 1u >= NUMKEYS(mp))
      return MDB_NOTFOUND;
    mc->mc_flags ^= C_EOF;
  }

  if (mc->mc_db->md_flags & MDB_DUPSORT) {
    leaf = NODEPTR(mp, mc->mc_ki[mc->mc_top]);
    if (F_ISSET(leaf->mn_flags, F_DUPDATA)) {
      if (op == MDB_NEXT || op == MDB_NEXT_DUP) {
        rc = mdbx_cursor_next(&mc->mc_xcursor->mx_cursor, data, NULL, MDB_NEXT);
        if (op != MDB_NEXT || rc != MDB_NOTFOUND) {
          if (likely(rc == MDB_SUCCESS))
            MDB_GET_KEY(leaf, key);
          return rc;
        }
      }
    } else {
      mc->mc_xcursor->mx_cursor.mc_flags &= ~(C_INITIALIZED | C_EOF);
      if (op == MDB_NEXT_DUP)
        return MDB_NOTFOUND;
    }
  }

  mdbx_debug("cursor_next: top page is %" PRIuPTR " in cursor %p",
             mdbx_dbg_pgno(mp), (void *)mc);
  if (mc->mc_flags & C_DEL) {
    mc->mc_flags ^= C_DEL;
    goto skip;
  }

  if (mc->mc_ki[mc->mc_top] + 1u >= NUMKEYS(mp)) {
    mdbx_debug("=====> move to next sibling page");
    if (unlikely((rc = mdbx_cursor_sibling(mc, 1)) != MDB_SUCCESS)) {
      mc->mc_flags |= C_EOF;
      return rc;
    }
    mp = mc->mc_pg[mc->mc_top];
    mdbx_debug("next page is %" PRIuPTR ", key index %u", mp->mp_pgno,
               mc->mc_ki[mc->mc_top]);
  } else
    mc->mc_ki[mc->mc_top]++;

skip:
  mdbx_debug("==> cursor points to page %" PRIuPTR
             " with %u keys, key index %u",
             mdbx_dbg_pgno(mp), NUMKEYS(mp), mc->mc_ki[mc->mc_top]);

  if (IS_LEAF2(mp)) {
    key->mv_size = mc->mc_db->md_xsize;
    key->mv_data = LEAF2KEY(mp, mc->mc_ki[mc->mc_top], key->mv_size);
    return MDB_SUCCESS;
  }

  mdbx_cassert(mc, IS_LEAF(mp));
  leaf = NODEPTR(mp, mc->mc_ki[mc->mc_top]);

  if (F_ISSET(leaf->mn_flags, F_DUPDATA)) {
    mdbx_xcursor_init1(mc, leaf);
  }
  if (data) {
    if (unlikely((rc = mdbx_node_read(mc, leaf, data)) != MDB_SUCCESS))
      return rc;

    if (F_ISSET(leaf->mn_flags, F_DUPDATA)) {
      rc = mdbx_cursor_first(&mc->mc_xcursor->mx_cursor, data, NULL);
      if (unlikely(rc != MDB_SUCCESS))
        return rc;
    }
  }

  MDB_GET_KEY(leaf, key);
  return MDB_SUCCESS;
}

/** Move the cursor to the previous data item. */
static int mdbx_cursor_prev(MDB_cursor *mc, MDB_val *key, MDB_val *data,
                            MDB_cursor_op op) {
  MDB_page *mp;
  MDB_node *leaf;
  int rc;

  if ((mc->mc_flags & C_DEL) && op == MDB_PREV_DUP)
    return MDB_NOTFOUND;

  if (!(mc->mc_flags & C_INITIALIZED)) {
    rc = mdbx_cursor_last(mc, key, data);
    if (unlikely(rc))
      return rc;
    mc->mc_ki[mc->mc_top]++;
  }

  mp = mc->mc_pg[mc->mc_top];
  if ((mc->mc_db->md_flags & MDB_DUPSORT) &&
      mc->mc_ki[mc->mc_top] < NUMKEYS(mp)) {
    leaf = NODEPTR(mp, mc->mc_ki[mc->mc_top]);
    if (F_ISSET(leaf->mn_flags, F_DUPDATA)) {
      if (op == MDB_PREV || op == MDB_PREV_DUP) {
        rc = mdbx_cursor_prev(&mc->mc_xcursor->mx_cursor, data, NULL, MDB_PREV);
        if (op != MDB_PREV || rc != MDB_NOTFOUND) {
          if (likely(rc == MDB_SUCCESS)) {
            MDB_GET_KEY(leaf, key);
            mc->mc_flags &= ~C_EOF;
          }
          return rc;
        }
      }
    } else {
      mc->mc_xcursor->mx_cursor.mc_flags &= ~(C_INITIALIZED | C_EOF);
      if (op == MDB_PREV_DUP)
        return MDB_NOTFOUND;
    }
  }

  mdbx_debug("cursor_prev: top page is %" PRIuPTR " in cursor %p",
             mdbx_dbg_pgno(mp), (void *)mc);

  mc->mc_flags &= ~(C_EOF | C_DEL);

  if (mc->mc_ki[mc->mc_top] == 0) {
    mdbx_debug("=====> move to prev sibling page");
    if ((rc = mdbx_cursor_sibling(mc, 0)) != MDB_SUCCESS) {
      return rc;
    }
    mp = mc->mc_pg[mc->mc_top];
    mc->mc_ki[mc->mc_top] = NUMKEYS(mp) - 1;
    mdbx_debug("prev page is %" PRIuPTR ", key index %u", mp->mp_pgno,
               mc->mc_ki[mc->mc_top]);
  } else
    mc->mc_ki[mc->mc_top]--;

  mdbx_debug("==> cursor points to page %" PRIuPTR
             " with %u keys, key index %u",
             mdbx_dbg_pgno(mp), NUMKEYS(mp), mc->mc_ki[mc->mc_top]);

  if (IS_LEAF2(mp)) {
    key->mv_size = mc->mc_db->md_xsize;
    key->mv_data = LEAF2KEY(mp, mc->mc_ki[mc->mc_top], key->mv_size);
    return MDB_SUCCESS;
  }

  mdbx_cassert(mc, IS_LEAF(mp));
  leaf = NODEPTR(mp, mc->mc_ki[mc->mc_top]);

  if (F_ISSET(leaf->mn_flags, F_DUPDATA)) {
    mdbx_xcursor_init1(mc, leaf);
  }
  if (data) {
    if (unlikely((rc = mdbx_node_read(mc, leaf, data)) != MDB_SUCCESS))
      return rc;

    if (F_ISSET(leaf->mn_flags, F_DUPDATA)) {
      rc = mdbx_cursor_last(&mc->mc_xcursor->mx_cursor, data, NULL);
      if (unlikely(rc != MDB_SUCCESS))
        return rc;
    }
  }

  MDB_GET_KEY(leaf, key);
  return MDB_SUCCESS;
}

/** Set the cursor on a specific data item. */
static int mdbx_cursor_set(MDB_cursor *mc, MDB_val *key, MDB_val *data,
                           MDB_cursor_op op, int *exactp) {
  int rc;
  MDB_page *mp;
  MDB_node *leaf = NULL;
  DKBUF;

  if ((mc->mc_db->md_flags & MDB_INTEGERKEY) &&
      unlikely(key->mv_size != sizeof(uint32_t) &&
               key->mv_size != sizeof(uint64_t))) {
    mdbx_cassert(mc, !"key-size is invalid for MDB_INTEGERKEY");
    return MDB_BAD_VALSIZE;
  }

  if (mc->mc_xcursor)
    mc->mc_xcursor->mx_cursor.mc_flags &= ~(C_INITIALIZED | C_EOF);

  /* See if we're already on the right page */
  if (mc->mc_flags & C_INITIALIZED) {
    MDB_val nodekey;

    mp = mc->mc_pg[mc->mc_top];
    if (!NUMKEYS(mp)) {
      mc->mc_ki[mc->mc_top] = 0;
      return MDB_NOTFOUND;
    }
    if (mp->mp_flags & P_LEAF2) {
      nodekey.mv_size = mc->mc_db->md_xsize;
      nodekey.mv_data = LEAF2KEY(mp, 0, nodekey.mv_size);
    } else {
      leaf = NODEPTR(mp, 0);
      MDB_GET_KEY2(leaf, nodekey);
    }
    rc = mc->mc_dbx->md_cmp(key, &nodekey);
    if (rc == 0) {
      /* Probably happens rarely, but first node on the page
       * was the one we wanted.
       */
      mc->mc_ki[mc->mc_top] = 0;
      if (exactp)
        *exactp = 1;
      goto set1;
    }
    if (rc > 0) {
      unsigned i;
      unsigned nkeys = NUMKEYS(mp);
      if (nkeys > 1) {
        if (mp->mp_flags & P_LEAF2) {
          nodekey.mv_data = LEAF2KEY(mp, nkeys - 1, nodekey.mv_size);
        } else {
          leaf = NODEPTR(mp, nkeys - 1);
          MDB_GET_KEY2(leaf, nodekey);
        }
        rc = mc->mc_dbx->md_cmp(key, &nodekey);
        if (rc == 0) {
          /* last node was the one we wanted */
          mc->mc_ki[mc->mc_top] = nkeys - 1;
          if (exactp)
            *exactp = 1;
          goto set1;
        }
        if (rc < 0) {
          if (mc->mc_ki[mc->mc_top] < NUMKEYS(mp)) {
            /* This is definitely the right page, skip search_page */
            if (mp->mp_flags & P_LEAF2) {
              nodekey.mv_data =
                  LEAF2KEY(mp, mc->mc_ki[mc->mc_top], nodekey.mv_size);
            } else {
              leaf = NODEPTR(mp, mc->mc_ki[mc->mc_top]);
              MDB_GET_KEY2(leaf, nodekey);
            }
            rc = mc->mc_dbx->md_cmp(key, &nodekey);
            if (rc == 0) {
              /* current node was the one we wanted */
              if (exactp)
                *exactp = 1;
              goto set1;
            }
          }
          rc = 0;
          mc->mc_flags &= ~C_EOF;
          goto set2;
        }
      }
      /* If any parents have right-sibs, search.
       * Otherwise, there's nothing further. */
      for (i = 0; i < mc->mc_top; i++)
        if (mc->mc_ki[i] < NUMKEYS(mc->mc_pg[i]) - 1)
          break;
      if (i == mc->mc_top) {
        /* There are no other pages */
        mc->mc_ki[mc->mc_top] = nkeys;
        return MDB_NOTFOUND;
      }
    }
    if (!mc->mc_top) {
      /* There are no other pages */
      mc->mc_ki[mc->mc_top] = 0;
      if (op == MDB_SET_RANGE && !exactp) {
        rc = 0;
        goto set1;
      } else
        return MDB_NOTFOUND;
    }
  } else {
    mc->mc_pg[0] = 0;
  }

  rc = mdbx_page_search(mc, key, 0);
  if (unlikely(rc != MDB_SUCCESS))
    return rc;

  mp = mc->mc_pg[mc->mc_top];
  mdbx_cassert(mc, IS_LEAF(mp));

set2:
  leaf = mdbx_node_search(mc, key, exactp);
  if (exactp != NULL && !*exactp) {
    /* MDB_SET specified and not an exact match. */
    return MDB_NOTFOUND;
  }

  if (leaf == NULL) {
    mdbx_debug("===> inexact leaf not found, goto sibling");
    if (unlikely((rc = mdbx_cursor_sibling(mc, 1)) != MDB_SUCCESS)) {
      mc->mc_flags |= C_EOF;
      return rc; /* no entries matched */
    }
    mp = mc->mc_pg[mc->mc_top];
    mdbx_cassert(mc, IS_LEAF(mp));
    leaf = NODEPTR(mp, 0);
  }

set1:
  mc->mc_flags |= C_INITIALIZED;
  mc->mc_flags &= ~C_EOF;

  if (IS_LEAF2(mp)) {
    if (op == MDB_SET_RANGE || op == MDB_SET_KEY) {
      key->mv_size = mc->mc_db->md_xsize;
      key->mv_data = LEAF2KEY(mp, mc->mc_ki[mc->mc_top], key->mv_size);
    }
    return MDB_SUCCESS;
  }

  if (F_ISSET(leaf->mn_flags, F_DUPDATA)) {
    mdbx_xcursor_init1(mc, leaf);
  }
  if (likely(data)) {
    if (F_ISSET(leaf->mn_flags, F_DUPDATA)) {
      if (op == MDB_SET || op == MDB_SET_KEY || op == MDB_SET_RANGE) {
        rc = mdbx_cursor_first(&mc->mc_xcursor->mx_cursor, data, NULL);
      } else {
        int ex2, *ex2p;
        if (op == MDB_GET_BOTH) {
          ex2p = &ex2;
          ex2 = 0;
        } else {
          ex2p = NULL;
        }
        rc = mdbx_cursor_set(&mc->mc_xcursor->mx_cursor, data, NULL,
                             MDB_SET_RANGE, ex2p);
        if (unlikely(rc != MDB_SUCCESS))
          return rc;
      }
    } else if (op == MDB_GET_BOTH || op == MDB_GET_BOTH_RANGE) {
      MDB_val olddata;
      if (unlikely((rc = mdbx_node_read(mc, leaf, &olddata)) != MDB_SUCCESS))
        return rc;
      rc = mc->mc_dbx->md_dcmp(data, &olddata);
      if (rc) {
        if (op == MDB_GET_BOTH || rc > 0)
          return MDB_NOTFOUND;
        rc = 0;
      }
      *data = olddata;
    } else {
      if (mc->mc_xcursor)
        mc->mc_xcursor->mx_cursor.mc_flags &= ~(C_INITIALIZED | C_EOF);
      if (unlikely((rc = mdbx_node_read(mc, leaf, data)) != MDB_SUCCESS))
        return rc;
    }
  }

  /* The key already matches in all other cases */
  if (op == MDB_SET_RANGE || op == MDB_SET_KEY)
    MDB_GET_KEY(leaf, key);
  mdbx_debug("==> cursor placed on key [%s]", DKEY(key));

  return rc;
}

/** Move the cursor to the first item in the database. */
static int mdbx_cursor_first(MDB_cursor *mc, MDB_val *key, MDB_val *data) {
  int rc;
  MDB_node *leaf;

  if (mc->mc_xcursor)
    mc->mc_xcursor->mx_cursor.mc_flags &= ~(C_INITIALIZED | C_EOF);

  if (!(mc->mc_flags & C_INITIALIZED) || mc->mc_top) {
    rc = mdbx_page_search(mc, NULL, MDB_PS_FIRST);
    if (unlikely(rc != MDB_SUCCESS))
      return rc;
  }
  mdbx_cassert(mc, IS_LEAF(mc->mc_pg[mc->mc_top]));

  leaf = NODEPTR(mc->mc_pg[mc->mc_top], 0);
  mc->mc_flags |= C_INITIALIZED;
  mc->mc_flags &= ~C_EOF;

  mc->mc_ki[mc->mc_top] = 0;

  if (IS_LEAF2(mc->mc_pg[mc->mc_top])) {
    key->mv_size = mc->mc_db->md_xsize;
    key->mv_data = LEAF2KEY(mc->mc_pg[mc->mc_top], 0, key->mv_size);
    return MDB_SUCCESS;
  }

  if (likely(data)) {
    if (F_ISSET(leaf->mn_flags, F_DUPDATA)) {
      mdbx_xcursor_init1(mc, leaf);
      rc = mdbx_cursor_first(&mc->mc_xcursor->mx_cursor, data, NULL);
      if (unlikely(rc))
        return rc;
    } else {
      if (unlikely((rc = mdbx_node_read(mc, leaf, data)) != MDB_SUCCESS))
        return rc;
    }
  }
  MDB_GET_KEY(leaf, key);
  return MDB_SUCCESS;
}

/** Move the cursor to the last item in the database. */
static int mdbx_cursor_last(MDB_cursor *mc, MDB_val *key, MDB_val *data) {
  int rc;
  MDB_node *leaf;

  if (mc->mc_xcursor)
    mc->mc_xcursor->mx_cursor.mc_flags &= ~(C_INITIALIZED | C_EOF);

  if (likely((mc->mc_flags & (C_EOF | C_DEL)) != C_EOF)) {
    if (!(mc->mc_flags & C_INITIALIZED) || mc->mc_top) {
      rc = mdbx_page_search(mc, NULL, MDB_PS_LAST);
      if (unlikely(rc != MDB_SUCCESS))
        return rc;
    }
    mdbx_cassert(mc, IS_LEAF(mc->mc_pg[mc->mc_top]));
  }

  mc->mc_ki[mc->mc_top] = NUMKEYS(mc->mc_pg[mc->mc_top]) - 1;
  mc->mc_flags |= C_INITIALIZED | C_EOF;
  leaf = NODEPTR(mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top]);

  if (IS_LEAF2(mc->mc_pg[mc->mc_top])) {
    key->mv_size = mc->mc_db->md_xsize;
    key->mv_data =
        LEAF2KEY(mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top], key->mv_size);
    return MDB_SUCCESS;
  }

  if (likely(data)) {
    if (F_ISSET(leaf->mn_flags, F_DUPDATA)) {
      mdbx_xcursor_init1(mc, leaf);
      rc = mdbx_cursor_last(&mc->mc_xcursor->mx_cursor, data, NULL);
      if (unlikely(rc))
        return rc;
    } else {
      if (unlikely((rc = mdbx_node_read(mc, leaf, data)) != MDB_SUCCESS))
        return rc;
    }
  }

  MDB_GET_KEY(leaf, key);
  return MDB_SUCCESS;
}

int mdbx_cursor_get(MDB_cursor *mc, MDB_val *key, MDB_val *data,
                    MDB_cursor_op op) {
  int rc;
  int exact = 0;
  int (*mfunc)(MDB_cursor * mc, MDB_val * key, MDB_val * data);

  if (unlikely(mc == NULL))
    return MDBX_EINVAL;

  if (unlikely(mc->mc_signature != MDBX_MC_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(mc->mc_txn->mt_flags & MDB_TXN_BLOCKED))
    return MDB_BAD_TXN;

  switch (op) {
  case MDB_GET_CURRENT: {
    if (unlikely(!(mc->mc_flags & C_INITIALIZED)))
      return MDBX_EINVAL;
    MDB_page *mp = mc->mc_pg[mc->mc_top];
    unsigned nkeys = NUMKEYS(mp);
    if (mc->mc_ki[mc->mc_top] >= nkeys) {
      mc->mc_ki[mc->mc_top] = nkeys;
      return MDB_NOTFOUND;
    }
    assert(nkeys > 0);

    rc = MDB_SUCCESS;
    if (IS_LEAF2(mp)) {
      key->mv_size = mc->mc_db->md_xsize;
      key->mv_data = LEAF2KEY(mp, mc->mc_ki[mc->mc_top], key->mv_size);
    } else {
      MDB_node *leaf = NODEPTR(mp, mc->mc_ki[mc->mc_top]);
      MDB_GET_KEY(leaf, key);
      if (data) {
        if (F_ISSET(leaf->mn_flags, F_DUPDATA)) {
          if (unlikely(!(mc->mc_xcursor->mx_cursor.mc_flags & C_INITIALIZED))) {
            mdbx_xcursor_init1(mc, leaf);
            rc = mdbx_cursor_first(&mc->mc_xcursor->mx_cursor, data, NULL);
            if (unlikely(rc))
              return rc;
          }
          rc = mdbx_cursor_get(&mc->mc_xcursor->mx_cursor, data, NULL,
                               MDB_GET_CURRENT);
        } else {
          rc = mdbx_node_read(mc, leaf, data);
        }
        if (unlikely(rc))
          return rc;
      }
    }
    break;
  }
  case MDB_GET_BOTH:
  case MDB_GET_BOTH_RANGE:
    if (unlikely(data == NULL))
      return MDBX_EINVAL;
    if (unlikely(mc->mc_xcursor == NULL))
      return MDB_INCOMPATIBLE;
  /* FALLTHRU */
  case MDB_SET:
  case MDB_SET_KEY:
  case MDB_SET_RANGE:
    if (unlikely(key == NULL))
      return MDBX_EINVAL;
    rc =
        mdbx_cursor_set(mc, key, data, op, op == MDB_SET_RANGE ? NULL : &exact);
    break;
  case MDB_GET_MULTIPLE:
    if (unlikely(data == NULL || !(mc->mc_flags & C_INITIALIZED)))
      return MDBX_EINVAL;
    if (unlikely(!(mc->mc_db->md_flags & MDB_DUPFIXED)))
      return MDB_INCOMPATIBLE;
    rc = MDB_SUCCESS;
    if (!(mc->mc_xcursor->mx_cursor.mc_flags & C_INITIALIZED) ||
        (mc->mc_xcursor->mx_cursor.mc_flags & C_EOF))
      break;
    goto fetchm;
  case MDB_NEXT_MULTIPLE:
    if (unlikely(data == NULL))
      return MDBX_EINVAL;
    if (unlikely(!(mc->mc_db->md_flags & MDB_DUPFIXED)))
      return MDB_INCOMPATIBLE;
    rc = mdbx_cursor_next(mc, key, data, MDB_NEXT_DUP);
    if (rc == MDB_SUCCESS) {
      if (mc->mc_xcursor->mx_cursor.mc_flags & C_INITIALIZED) {
        MDB_cursor *mx;
      fetchm:
        mx = &mc->mc_xcursor->mx_cursor;
        data->mv_size = NUMKEYS(mx->mc_pg[mx->mc_top]) * mx->mc_db->md_xsize;
        data->mv_data = PAGEDATA(mx->mc_pg[mx->mc_top]);
        mx->mc_ki[mx->mc_top] = NUMKEYS(mx->mc_pg[mx->mc_top]) - 1;
      } else {
        rc = MDB_NOTFOUND;
      }
    }
    break;
  case MDB_PREV_MULTIPLE:
    if (data == NULL)
      return MDBX_EINVAL;
    if (!(mc->mc_db->md_flags & MDB_DUPFIXED))
      return MDB_INCOMPATIBLE;
    rc = MDB_SUCCESS;
    if (!(mc->mc_flags & C_INITIALIZED))
      rc = mdbx_cursor_last(mc, key, data);
    if (rc == MDB_SUCCESS) {
      MDB_cursor *mx = &mc->mc_xcursor->mx_cursor;
      if (mx->mc_flags & C_INITIALIZED) {
        rc = mdbx_cursor_sibling(mx, 0);
        if (rc == MDB_SUCCESS)
          goto fetchm;
      } else {
        rc = MDB_NOTFOUND;
      }
    }
    break;
  case MDB_NEXT:
  case MDB_NEXT_DUP:
  case MDB_NEXT_NODUP:
    rc = mdbx_cursor_next(mc, key, data, op);
    break;
  case MDB_PREV:
  case MDB_PREV_DUP:
  case MDB_PREV_NODUP:
    rc = mdbx_cursor_prev(mc, key, data, op);
    break;
  case MDB_FIRST:
    rc = mdbx_cursor_first(mc, key, data);
    break;
  case MDB_FIRST_DUP:
    mfunc = mdbx_cursor_first;
  mmove:
    if (unlikely(data == NULL || !(mc->mc_flags & C_INITIALIZED)))
      return MDBX_EINVAL;
    if (unlikely(mc->mc_xcursor == NULL))
      return MDB_INCOMPATIBLE;
    {
      MDB_node *leaf = NODEPTR(mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top]);
      if (!F_ISSET(leaf->mn_flags, F_DUPDATA)) {
        MDB_GET_KEY(leaf, key);
        rc = mdbx_node_read(mc, leaf, data);
        break;
      }
    }
    if (unlikely(!(mc->mc_xcursor->mx_cursor.mc_flags & C_INITIALIZED)))
      return MDBX_EINVAL;
    rc = mfunc(&mc->mc_xcursor->mx_cursor, data, NULL);
    break;
  case MDB_LAST:
    rc = mdbx_cursor_last(mc, key, data);
    break;
  case MDB_LAST_DUP:
    mfunc = mdbx_cursor_last;
    goto mmove;
  default:
    mdbx_debug("unhandled/unimplemented cursor operation %u", op);
    return MDBX_EINVAL;
  }

  mc->mc_flags &= ~C_DEL;
  return rc;
}

/** Touch all the pages in the cursor stack. Set mc_top.
 *	Makes sure all the pages are writable, before attempting a write
 *operation.
 * @param[in] mc The cursor to operate on.
 */
static int mdbx_cursor_touch(MDB_cursor *mc) {
  int rc = MDB_SUCCESS;

  if (mc->mc_dbi >= CORE_DBS && !(*mc->mc_dbflag & (DB_DIRTY | DB_DUPDATA))) {
    /* Touch DB record of named DB */
    MDB_cursor mc2;
    MDB_xcursor mcx;
    if (TXN_DBI_CHANGED(mc->mc_txn, mc->mc_dbi))
      return MDB_BAD_DBI;
    mdbx_cursor_init(&mc2, mc->mc_txn, MAIN_DBI, &mcx);
    rc = mdbx_page_search(&mc2, &mc->mc_dbx->md_name, MDB_PS_MODIFY);
    if (unlikely(rc))
      return rc;
    *mc->mc_dbflag |= DB_DIRTY;
  }
  mc->mc_top = 0;
  if (mc->mc_snum) {
    do {
      rc = mdbx_page_touch(mc);
    } while (!rc && ++(mc->mc_top) < mc->mc_snum);
    mc->mc_top = mc->mc_snum - 1;
  }
  return rc;
}

/** Do not spill pages to disk if txn is getting full, may fail instead */
#define MDB_NOSPILL 0x8000

int mdbx_cursor_put(MDB_cursor *mc, MDB_val *key, MDB_val *data,
                    unsigned flags) {
  MDB_env *env;
  MDB_page *fp, *sub_root = NULL;
  uint16_t fp_flags;
  MDB_val xdata, *rdata, dkey, olddata;
  MDB_db dummy;
  int do_sub = 0, insert_key, insert_data;
  unsigned mcount = 0, dcount = 0, nospill;
  size_t nsize;
  int rc, rc2;
  unsigned nflags;
  DKBUF;

  if (unlikely(mc == NULL || key == NULL))
    return MDBX_EINVAL;

  if (unlikely(mc->mc_signature != MDBX_MC_SIGNATURE))
    return MDBX_EBADSIGN;

  env = mc->mc_txn->mt_env;

  /* Check this first so counter will always be zero on any
   * early failures.
   */
  if (flags & MDB_MULTIPLE) {
    dcount = data[1].mv_size;
    data[1].mv_size = 0;
    if (unlikely(!F_ISSET(mc->mc_db->md_flags, MDB_DUPFIXED)))
      return MDB_INCOMPATIBLE;
  }

  if (flags & MDB_RESERVE) {
    if (unlikely(mc->mc_db->md_flags & (MDB_DUPSORT | MDB_REVERSEDUP)))
      return MDB_INCOMPATIBLE;
  }

  nospill = flags & MDB_NOSPILL;
  flags &= ~MDB_NOSPILL;

  if (unlikely(mc->mc_txn->mt_flags & (MDB_TXN_RDONLY | MDB_TXN_BLOCKED)))
    return (mc->mc_txn->mt_flags & MDB_TXN_RDONLY) ? MDBX_EACCESS : MDB_BAD_TXN;

  if (unlikely(key->mv_size > env->me_maxkey_limit))
    return MDB_BAD_VALSIZE;

  if (unlikely(data->mv_size > ((mc->mc_db->md_flags & MDB_DUPSORT)
                                    ? env->me_maxkey_limit
                                    : MDBX_MAXDATASIZE)))
    return MDB_BAD_VALSIZE;

  if ((mc->mc_db->md_flags & MDB_INTEGERKEY) &&
      unlikely(key->mv_size != sizeof(uint32_t) &&
               key->mv_size != sizeof(uint64_t))) {
    mdbx_cassert(mc, !"key-size is invalid for MDB_INTEGERKEY");
    return MDB_BAD_VALSIZE;
  }

  if ((mc->mc_db->md_flags & MDB_INTEGERDUP) &&
      unlikely(data->mv_size != sizeof(uint32_t) &&
               data->mv_size != sizeof(uint64_t))) {
    mdbx_cassert(mc, !"data-size is invalid MDB_INTEGERDUP");
    return MDB_BAD_VALSIZE;
  }

  mdbx_debug("==> put db %d key [%s], size %" PRIuPTR ", data size %" PRIuPTR
             "",
             DDBI(mc), DKEY(key), key ? key->mv_size : 0, data->mv_size);

  int dupdata_flag = 0;
  if (flags & MDB_CURRENT) {
    if (unlikely(!(mc->mc_flags & C_INITIALIZED)))
      return MDBX_EINVAL;
    if (F_ISSET(mc->mc_db->md_flags, MDB_DUPSORT)) {
      MDB_node *leaf = NODEPTR(mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top]);
      if (F_ISSET(leaf->mn_flags, F_DUPDATA)) {
        mdbx_cassert(mc,
                     mc->mc_xcursor != NULL &&
                         (mc->mc_xcursor->mx_cursor.mc_flags & C_INITIALIZED));
        if (mc->mc_xcursor->mx_db.md_entries > 1) {
          rc = mdbx_cursor_del(mc, 0);
          if (rc != MDB_SUCCESS)
            return rc;
          flags -= MDB_CURRENT;
        }
      }
    }
    rc = MDB_SUCCESS;
  } else if (mc->mc_db->md_root == P_INVALID) {
    /* new database, cursor has nothing to point to */
    mc->mc_snum = 0;
    mc->mc_top = 0;
    mc->mc_flags &= ~C_INITIALIZED;
    rc = MDB_NO_ROOT;
  } else {
    int exact = 0;
    MDB_val d2;
    if (flags & MDB_APPEND) {
      MDB_val k2;
      rc = mdbx_cursor_last(mc, &k2, &d2);
      if (rc == 0) {
        rc = mc->mc_dbx->md_cmp(key, &k2);
        if (rc > 0) {
          rc = MDB_NOTFOUND;
          mc->mc_ki[mc->mc_top]++;
        } else {
          /* new key is <= last key */
          rc = MDB_KEYEXIST;
        }
      }
    } else {
      rc = mdbx_cursor_set(mc, key, &d2, MDB_SET, &exact);
    }
    if ((flags & MDB_NOOVERWRITE) && rc == 0) {
      mdbx_debug("duplicate key [%s]", DKEY(key));
      *data = d2;
      return MDB_KEYEXIST;
    }
    if (rc && unlikely(rc != MDB_NOTFOUND))
      return rc;
  }

  mc->mc_flags &= ~C_DEL;

  /* Cursor is positioned, check for room in the dirty list */
  if (!nospill) {
    if (flags & MDB_MULTIPLE) {
      rdata = &xdata;
      xdata.mv_size = data->mv_size * dcount;
    } else {
      rdata = data;
    }
    if (unlikely(rc2 = mdbx_page_spill(mc, key, rdata)))
      return rc2;
  }

  if (rc == MDB_NO_ROOT) {
    MDB_page *np;
    /* new database, write a root leaf page */
    mdbx_debug("allocating new root leaf page");
    if (unlikely(rc2 = mdbx_page_new(mc, P_LEAF, 1, &np))) {
      return rc2;
    }
    mdbx_cursor_push(mc, np);
    mc->mc_db->md_root = np->mp_pgno;
    mc->mc_db->md_depth++;
    *mc->mc_dbflag |= DB_DIRTY;
    if ((mc->mc_db->md_flags & (MDB_DUPSORT | MDB_DUPFIXED)) == MDB_DUPFIXED)
      np->mp_flags |= P_LEAF2;
    mc->mc_flags |= C_INITIALIZED;
  } else {
    /* make sure all cursor pages are writable */
    rc2 = mdbx_cursor_touch(mc);
    if (unlikely(rc2))
      return rc2;
  }

  insert_key = insert_data = rc;
  if (insert_key) {
    /* The key does not exist */
    mdbx_debug("inserting key at index %i", mc->mc_ki[mc->mc_top]);
    if ((mc->mc_db->md_flags & MDB_DUPSORT) &&
        LEAFSIZE(key, data) > env->me_nodemax) {
      /* Too big for a node, insert in sub-DB.  Set up an empty
       * "old sub-page" for prep_subDB to expand to a full page.
       */
      fp_flags = P_LEAF | P_DIRTY;
      fp = env->me_pbuf;
      fp->mp_leaf2_ksize = (uint16_t)data->mv_size; /* used if MDB_DUPFIXED */
      fp->mp_lower = fp->mp_upper = (PAGEHDRSZ - PAGEBASE);
      olddata.mv_size = PAGEHDRSZ;
      goto prep_subDB;
    }
  } else {
    /* there's only a key anyway, so this is a no-op */
    if (IS_LEAF2(mc->mc_pg[mc->mc_top])) {
      char *ptr;
      unsigned ksize = mc->mc_db->md_xsize;
      if (key->mv_size != ksize)
        return MDB_BAD_VALSIZE;
      ptr = LEAF2KEY(mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top], ksize);
      memcpy(ptr, key->mv_data, ksize);
    fix_parent:
      /* if overwriting slot 0 of leaf, need to
       * update branch key if there is a parent page
       */
      if (mc->mc_top && !mc->mc_ki[mc->mc_top]) {
        unsigned dtop = 1;
        mc->mc_top--;
        /* slot 0 is always an empty key, find real slot */
        while (mc->mc_top && !mc->mc_ki[mc->mc_top]) {
          mc->mc_top--;
          dtop++;
        }
        if (mc->mc_ki[mc->mc_top])
          rc2 = mdbx_update_key(mc, key);
        else
          rc2 = MDB_SUCCESS;
        mc->mc_top += dtop;
        if (rc2)
          return rc2;
      }
      return MDB_SUCCESS;
    }

  more:;
    MDB_node *leaf = NODEPTR(mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top]);
    olddata.mv_size = NODEDSZ(leaf);
    olddata.mv_data = NODEDATA(leaf);

    /* DB has dups? */
    if (F_ISSET(mc->mc_db->md_flags, MDB_DUPSORT)) {
      /* Prepare (sub-)page/sub-DB to accept the new item,
       * if needed.  fp: old sub-page or a header faking
       * it.  mp: new (sub-)page.  offset: growth in page
       * size.  xdata: node data with new page or DB.
       */
      unsigned i, offset = 0;
      MDB_page *mp = fp = xdata.mv_data = env->me_pbuf;
      mp->mp_pgno = mc->mc_pg[mc->mc_top]->mp_pgno;

      /* Was a single item before, must convert now */
      if (!F_ISSET(leaf->mn_flags, F_DUPDATA)) {
        /* Just overwrite the current item */
        if (flags & MDB_CURRENT) {
          if ((flags & MDB_NODUPDATA) && !mc->mc_dbx->md_dcmp(data, &olddata))
            return MDB_KEYEXIST;
          goto current;
        }

        /* does data match? */
        if (!mc->mc_dbx->md_dcmp(data, &olddata)) {
          if (unlikely(flags & (MDB_NODUPDATA | MDB_APPENDDUP)))
            return MDB_KEYEXIST;
          /* overwrite it */
          goto current;
        }

        /* Back up original data item */
        dupdata_flag = 1;
        dkey.mv_size = olddata.mv_size;
        dkey.mv_data = memcpy(fp + 1, olddata.mv_data, olddata.mv_size);

        /* Make sub-page header for the dup items, with dummy body */
        fp->mp_flags = P_LEAF | P_DIRTY | P_SUBP;
        fp->mp_lower = (PAGEHDRSZ - PAGEBASE);
        xdata.mv_size = PAGEHDRSZ + dkey.mv_size + data->mv_size;
        if (mc->mc_db->md_flags & MDB_DUPFIXED) {
          fp->mp_flags |= P_LEAF2;
          fp->mp_leaf2_ksize = (uint16_t)data->mv_size;
          xdata.mv_size += 2 * data->mv_size; /* leave space for 2 more */
        } else {
          xdata.mv_size += 2 * (sizeof(indx_t) + NODESIZE) +
                           (dkey.mv_size & 1) + (data->mv_size & 1);
        }
        fp->mp_upper = (uint16_t)(xdata.mv_size - PAGEBASE);
        olddata.mv_size = xdata.mv_size; /* pretend olddata is fp */
      } else if (leaf->mn_flags & F_SUBDATA) {
        /* Data is on sub-DB, just store it */
        flags |= F_DUPDATA | F_SUBDATA;
        goto put_sub;
      } else {
        /* Data is on sub-page */
        fp = olddata.mv_data;
        switch (flags) {
        default:
          if (!(mc->mc_db->md_flags & MDB_DUPFIXED)) {
            offset = EVEN(NODESIZE + sizeof(indx_t) + data->mv_size);
            break;
          }
          offset = fp->mp_leaf2_ksize;
          if (SIZELEFT(fp) < offset) {
            offset *= 4; /* space for 4 more */
            break;
          }
        /* FALLTHRU: Big enough MDB_DUPFIXED sub-page */
        case MDB_CURRENT | MDB_NODUPDATA:
        case MDB_CURRENT:
          fp->mp_flags |= P_DIRTY;
          COPY_PGNO(fp->mp_pgno, mp->mp_pgno);
          mc->mc_xcursor->mx_cursor.mc_pg[0] = fp;
          flags |= F_DUPDATA;
          goto put_sub;
        }
        xdata.mv_size = olddata.mv_size + offset;
      }

      fp_flags = fp->mp_flags;
      if (NODESIZE + NODEKSZ(leaf) + xdata.mv_size > env->me_nodemax) {
        /* Too big for a sub-page, convert to sub-DB */
        fp_flags &= ~P_SUBP;
      prep_subDB:
        dummy.md_xsize = 0;
        dummy.md_flags = 0;
        if (mc->mc_db->md_flags & MDB_DUPFIXED) {
          fp_flags |= P_LEAF2;
          dummy.md_xsize = fp->mp_leaf2_ksize;
          dummy.md_flags = MDB_DUPFIXED;
          if (mc->mc_db->md_flags & MDB_INTEGERDUP)
            dummy.md_flags |= MDB_INTEGERKEY;
        }
        dummy.md_depth = 1;
        dummy.md_branch_pages = 0;
        dummy.md_leaf_pages = 1;
        dummy.md_overflow_pages = 0;
        dummy.md_entries = NUMKEYS(fp);
        xdata.mv_size = sizeof(MDB_db);
        xdata.mv_data = &dummy;
        if ((rc = mdbx_page_alloc(mc, 1, &mp, MDBX_ALLOC_ALL)))
          return rc;
        offset = env->me_psize - olddata.mv_size;
        flags |= F_DUPDATA | F_SUBDATA;
        dummy.md_root = mp->mp_pgno;
        sub_root = mp;
      }
      if (mp != fp) {
        mp->mp_flags = fp_flags | P_DIRTY;
        mp->mp_leaf2_ksize = fp->mp_leaf2_ksize;
        mp->mp_lower = fp->mp_lower;
        mp->mp_upper = fp->mp_upper + offset;
        if (fp_flags & P_LEAF2) {
          memcpy(PAGEDATA(mp), PAGEDATA(fp), NUMKEYS(fp) * fp->mp_leaf2_ksize);
        } else {
          memcpy((char *)mp + mp->mp_upper + PAGEBASE,
                 (char *)fp + fp->mp_upper + PAGEBASE,
                 olddata.mv_size - fp->mp_upper - PAGEBASE);
          for (i = 0; i < NUMKEYS(fp); i++)
            mp->mp_ptrs[i] = fp->mp_ptrs[i] + offset;
        }
      }

      rdata = &xdata;
      flags |= F_DUPDATA;
      do_sub = 1;
      if (!insert_key)
        mdbx_node_del(mc, 0);
      goto new_sub;
    }
  current:
    /* LMDB passes F_SUBDATA in 'flags' to write a DB record */
    if (unlikely((leaf->mn_flags ^ flags) & F_SUBDATA))
      return MDB_INCOMPATIBLE;
    /* overflow page overwrites need special handling */
    if (F_ISSET(leaf->mn_flags, F_BIGDATA)) {
      MDB_page *omp;
      pgno_t pg;
      int level, ovpages, dpages = OVPAGES(data->mv_size, env->me_psize);

      memcpy(&pg, olddata.mv_data, sizeof(pg));
      if (unlikely((rc2 = mdbx_page_get(mc, pg, &omp, &level)) != 0))
        return rc2;
      ovpages = omp->mp_pages;

      /* Is the ov page large enough? */
      if (ovpages >= dpages) {
        if (!(omp->mp_flags & P_DIRTY) &&
            (level || (env->me_flags & MDB_WRITEMAP))) {
          rc = mdbx_page_unspill(mc->mc_txn, omp, &omp);
          if (unlikely(rc))
            return rc;
          level = 0; /* dirty in this txn or clean */
        }
        /* Is it dirty? */
        if (omp->mp_flags & P_DIRTY) {
          /* yes, overwrite it. Note in this case we don't
           * bother to try shrinking the page if the new data
           * is smaller than the overflow threshold. */
          if (unlikely(level > 1)) {
            /* It is writable only in a parent txn */
            MDB_page *np = mdbx_page_malloc(mc->mc_txn, ovpages);
            MDB_ID2 id2;
            if (unlikely(!np))
              return MDBX_ENOMEM;
            id2.mid = pg;
            id2.mptr = np;
            /* Note - this page is already counted in parent's dirty_room */
            rc2 = mdbx_mid2l_insert(mc->mc_txn->mt_u.dirty_list, &id2);
            mdbx_cassert(mc, rc2 == 0);

            /* Currently we make the page look as with put() in the
             * parent txn, in case the user peeks at MDB_RESERVEd
             * or unused parts. Some users treat ovpages specially. */
            size_t whole = (size_t)env->me_psize * ovpages;
            /* Skip the part where LMDB will put *data.
             * Copy end of page, adjusting alignment so
             * compiler may copy words instead of bytes. */
            size_t off = (PAGEHDRSZ + data->mv_size) & -(ssize_t)sizeof(size_t);
            memcpy((size_t *)((char *)np + off), (size_t *)((char *)omp + off),
                   whole - off);
            memcpy(np, omp, PAGEHDRSZ); /* Copy header of page */
            omp = np;
          }
          SETDSZ(leaf, data->mv_size);
          if (F_ISSET(flags, MDB_RESERVE))
            data->mv_data = PAGEDATA(omp);
          else
            memcpy(PAGEDATA(omp), data->mv_data, data->mv_size);
          return MDB_SUCCESS;
        }
      }
      if ((rc2 = mdbx_ovpage_free(mc, omp)) != MDB_SUCCESS)
        return rc2;
    } else if (data->mv_size == olddata.mv_size) {
      /* same size, just replace it. Note that we could
       * also reuse this node if the new data is smaller,
       * but instead we opt to shrink the node in that case.
       */
      if (F_ISSET(flags, MDB_RESERVE))
        data->mv_data = olddata.mv_data;
      else if (!(mc->mc_flags & C_SUB))
        memcpy(olddata.mv_data, data->mv_data, data->mv_size);
      else {
        memcpy(NODEKEY(leaf), key->mv_data, key->mv_size);
        goto fix_parent;
      }
      return MDB_SUCCESS;
    }
    mdbx_node_del(mc, 0);
  }

  rdata = data;

new_sub:
  nflags = flags & NODE_ADD_FLAGS;
  nsize = IS_LEAF2(mc->mc_pg[mc->mc_top]) ? key->mv_size
                                          : mdbx_leaf_size(env, key, rdata);
  if (SIZELEFT(mc->mc_pg[mc->mc_top]) < nsize) {
    if ((flags & (F_DUPDATA | F_SUBDATA)) == F_DUPDATA)
      nflags &= ~MDB_APPEND; /* sub-page may need room to grow */
    if (!insert_key)
      nflags |= MDB_SPLIT_REPLACE;
    rc = mdbx_page_split(mc, key, rdata, P_INVALID, nflags);
  } else {
    /* There is room already in this leaf page. */
    rc = mdbx_node_add(mc, mc->mc_ki[mc->mc_top], key, rdata, 0, nflags);
    if (likely(rc == 0)) {
      /* Adjust other cursors pointing to mp */
      MDB_cursor *m2, *m3;
      MDB_dbi dbi = mc->mc_dbi;
      unsigned i = mc->mc_top;
      MDB_page *mp = mc->mc_pg[i];

      for (m2 = mc->mc_txn->mt_cursors[dbi]; m2; m2 = m2->mc_next) {
        if (mc->mc_flags & C_SUB)
          m3 = &m2->mc_xcursor->mx_cursor;
        else
          m3 = m2;
        if (m3 == mc || m3->mc_snum < mc->mc_snum || m3->mc_pg[i] != mp)
          continue;
        if (m3->mc_ki[i] >= mc->mc_ki[i] && insert_key) {
          m3->mc_ki[i]++;
        }
        if (XCURSOR_INITED(m3))
          XCURSOR_REFRESH(m3, mp, m3->mc_ki[i]);
      }
    }
  }

  if (likely(rc == MDB_SUCCESS)) {
    /* Now store the actual data in the child DB. Note that we're
     * storing the user data in the keys field, so there are strict
     * size limits on dupdata. The actual data fields of the child
     * DB are all zero size. */
    if (do_sub) {
      int xflags;
      size_t ecount;
    put_sub:
      xdata.mv_size = 0;
      xdata.mv_data = "";
      MDB_node *leaf = NODEPTR(mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top]);
      if (flags & MDB_CURRENT) {
        xflags = (flags & MDB_NODUPDATA)
                     ? MDB_CURRENT | MDB_NOOVERWRITE | MDB_NOSPILL
                     : MDB_CURRENT | MDB_NOSPILL;
      } else {
        mdbx_xcursor_init1(mc, leaf);
        xflags = (flags & MDB_NODUPDATA) ? MDB_NOOVERWRITE | MDB_NOSPILL
                                         : MDB_NOSPILL;
      }
      if (sub_root)
        mc->mc_xcursor->mx_cursor.mc_pg[0] = sub_root;
      /* converted, write the original data first */
      if (dupdata_flag) {
        rc = mdbx_cursor_put(&mc->mc_xcursor->mx_cursor, &dkey, &xdata, xflags);
        if (unlikely(rc))
          goto bad_sub;
        /* we've done our job */
        dkey.mv_size = 0;
      }
      if (!(leaf->mn_flags & F_SUBDATA) || sub_root) {
        /* Adjust other cursors pointing to mp */
        MDB_cursor *m2;
        MDB_xcursor *mx = mc->mc_xcursor;
        unsigned i = mc->mc_top;
        MDB_page *mp = mc->mc_pg[i];
        int nkeys = NUMKEYS(mp);

        for (m2 = mc->mc_txn->mt_cursors[mc->mc_dbi]; m2; m2 = m2->mc_next) {
          if (m2 == mc || m2->mc_snum < mc->mc_snum)
            continue;
          if (!(m2->mc_flags & C_INITIALIZED))
            continue;
          if (m2->mc_pg[i] == mp) {
            if (m2->mc_ki[i] == mc->mc_ki[i]) {
              mdbx_xcursor_init2(m2, mx, dupdata_flag);
            } else if (!insert_key && m2->mc_ki[i] < nkeys) {
              XCURSOR_REFRESH(m2, mp, m2->mc_ki[i]);
            }
          }
        }
      }
      ecount = mc->mc_xcursor->mx_db.md_entries;
      if (flags & MDB_APPENDDUP)
        xflags |= MDB_APPEND;
      rc = mdbx_cursor_put(&mc->mc_xcursor->mx_cursor, data, &xdata, xflags);
      if (flags & F_SUBDATA) {
        void *db = NODEDATA(leaf);
        memcpy(db, &mc->mc_xcursor->mx_db, sizeof(MDB_db));
      }
      insert_data = mc->mc_xcursor->mx_db.md_entries - ecount;
    }
    /* Increment count unless we just replaced an existing item. */
    if (insert_data)
      mc->mc_db->md_entries++;
    if (insert_key) {
      /* Invalidate txn if we created an empty sub-DB */
      if (unlikely(rc))
        goto bad_sub;
      /* If we succeeded and the key didn't exist before,
       * make sure the cursor is marked valid. */
      mc->mc_flags |= C_INITIALIZED;
    }
    if (flags & MDB_MULTIPLE) {
      if (!rc) {
        mcount++;
        /* let caller know how many succeeded, if any */
        data[1].mv_size = mcount;
        if (mcount < dcount) {
          data[0].mv_data = (char *)data[0].mv_data + data[0].mv_size;
          insert_key = insert_data = 0;
          goto more;
        }
      }
    }
    return rc;
  bad_sub:
    if (unlikely(rc ==
                 MDB_KEYEXIST)) /* should not happen, we deleted that item */
      rc = MDB_PROBLEM;
  }
  mc->mc_txn->mt_flags |= MDB_TXN_ERROR;
  return rc;
}

int mdbx_cursor_del(MDB_cursor *mc, unsigned flags) {
  MDB_node *leaf;
  MDB_page *mp;
  int rc;

  if (unlikely(!mc))
    return MDBX_EINVAL;

  if (unlikely(mc->mc_signature != MDBX_MC_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(mc->mc_txn->mt_flags & (MDB_TXN_RDONLY | MDB_TXN_BLOCKED)))
    return (mc->mc_txn->mt_flags & MDB_TXN_RDONLY) ? MDBX_EACCESS : MDB_BAD_TXN;

  if (unlikely(!(mc->mc_flags & C_INITIALIZED)))
    return MDBX_EINVAL;

  if (unlikely(mc->mc_ki[mc->mc_top] >= NUMKEYS(mc->mc_pg[mc->mc_top])))
    return MDB_NOTFOUND;

  if (unlikely(!(flags & MDB_NOSPILL) &&
               (rc = mdbx_page_spill(mc, NULL, NULL))))
    return rc;

  rc = mdbx_cursor_touch(mc);
  if (unlikely(rc))
    return rc;

  mp = mc->mc_pg[mc->mc_top];
  if (IS_LEAF2(mp))
    goto del_key;
  leaf = NODEPTR(mp, mc->mc_ki[mc->mc_top]);

  if (F_ISSET(leaf->mn_flags, F_DUPDATA)) {
    if (flags & MDB_NODUPDATA) {
      /* mdbx_cursor_del0() will subtract the final entry */
      mc->mc_db->md_entries -= mc->mc_xcursor->mx_db.md_entries - 1;
      mc->mc_xcursor->mx_cursor.mc_flags &= ~C_INITIALIZED;
    } else {
      if (!F_ISSET(leaf->mn_flags, F_SUBDATA)) {
        mc->mc_xcursor->mx_cursor.mc_pg[0] = NODEDATA(leaf);
      }
      rc = mdbx_cursor_del(&mc->mc_xcursor->mx_cursor, MDB_NOSPILL);
      if (unlikely(rc))
        return rc;
      /* If sub-DB still has entries, we're done */
      if (mc->mc_xcursor->mx_db.md_entries) {
        if (leaf->mn_flags & F_SUBDATA) {
          /* update subDB info */
          void *db = NODEDATA(leaf);
          memcpy(db, &mc->mc_xcursor->mx_db, sizeof(MDB_db));
        } else {
          MDB_cursor *m2;
          /* shrink fake page */
          mdbx_node_shrink(mp, mc->mc_ki[mc->mc_top]);
          leaf = NODEPTR(mp, mc->mc_ki[mc->mc_top]);
          mc->mc_xcursor->mx_cursor.mc_pg[0] = NODEDATA(leaf);
          /* fix other sub-DB cursors pointed at fake pages on this page */
          for (m2 = mc->mc_txn->mt_cursors[mc->mc_dbi]; m2; m2 = m2->mc_next) {
            if (m2 == mc || m2->mc_snum < mc->mc_snum)
              continue;
            if (!(m2->mc_flags & C_INITIALIZED))
              continue;
            if (m2->mc_pg[mc->mc_top] == mp) {
              MDB_node *n2 = leaf;
              if (m2->mc_ki[mc->mc_top] != mc->mc_ki[mc->mc_top]) {
                n2 = NODEPTR(mp, m2->mc_ki[mc->mc_top]);
                if (n2->mn_flags & F_SUBDATA)
                  continue;
              }
              m2->mc_xcursor->mx_cursor.mc_pg[0] = NODEDATA(n2);
            }
          }
        }
        mc->mc_db->md_entries--;
        return rc;
      } else {
        mc->mc_xcursor->mx_cursor.mc_flags &= ~C_INITIALIZED;
      }
      /* otherwise fall thru and delete the sub-DB */
    }

    if (leaf->mn_flags & F_SUBDATA) {
      /* add all the child DB's pages to the free list */
      rc = mdbx_drop0(&mc->mc_xcursor->mx_cursor, 0);
      if (unlikely(rc))
        goto fail;
    }
  }
  /* LMDB passes F_SUBDATA in 'flags' to delete a DB record */
  else if (unlikely((leaf->mn_flags ^ flags) & F_SUBDATA)) {
    rc = MDB_INCOMPATIBLE;
    goto fail;
  }

  /* add overflow pages to free list */
  if (F_ISSET(leaf->mn_flags, F_BIGDATA)) {
    MDB_page *omp;
    pgno_t pg;

    memcpy(&pg, NODEDATA(leaf), sizeof(pg));
    if (unlikely((rc = mdbx_page_get(mc, pg, &omp, NULL)) ||
                 (rc = mdbx_ovpage_free(mc, omp))))
      goto fail;
  }

del_key:
  return mdbx_cursor_del0(mc);

fail:
  mc->mc_txn->mt_flags |= MDB_TXN_ERROR;
  return rc;
}

/** Allocate and initialize new pages for a database.
 * Set #MDB_TXN_ERROR on failure.
 * @param[in] mc a cursor on the database being added to.
 * @param[in] flags flags defining what type of page is being allocated.
 * @param[in] num the number of pages to allocate. This is usually 1,
 * unless allocating overflow pages for a large record.
 * @param[out] mp Address of a page, or NULL on failure.
 * @return 0 on success, non-zero on failure.
 */
static int mdbx_page_new(MDB_cursor *mc, uint32_t flags, int num,
                         MDB_page **mp) {
  MDB_page *np;
  int rc;

  if (unlikely((rc = mdbx_page_alloc(mc, num, &np, MDBX_ALLOC_ALL))))
    return rc;
  mdbx_debug("allocated new mpage %" PRIuPTR ", page size %u", np->mp_pgno,
             mc->mc_txn->mt_env->me_psize);
  np->mp_flags = flags | P_DIRTY;
  np->mp_lower = (PAGEHDRSZ - PAGEBASE);
  np->mp_upper = mc->mc_txn->mt_env->me_psize - PAGEBASE;

  if (IS_BRANCH(np))
    mc->mc_db->md_branch_pages++;
  else if (IS_LEAF(np))
    mc->mc_db->md_leaf_pages++;
  else if (IS_OVERFLOW(np)) {
    mc->mc_db->md_overflow_pages += num;
    np->mp_pages = num;
  }
  *mp = np;

  return 0;
}

/** Calculate the size of a leaf node.
 * The size depends on the environment's page size; if a data item
 * is too large it will be put onto an overflow page and the node
 * size will only include the key and not the data. Sizes are always
 * rounded up to an even number of bytes, to guarantee 2-byte alignment
 * of the #MDB_node headers.
 * @param[in] env The environment handle.
 * @param[in] key The key for the node.
 * @param[in] data The data for the node.
 * @return The number of bytes needed to store the node.
 */
static __inline size_t mdbx_leaf_size(MDB_env *env, MDB_val *key,
                                      MDB_val *data) {
  size_t sz;

  sz = LEAFSIZE(key, data);
  if (sz > env->me_nodemax) {
    /* put on overflow page */
    sz -= data->mv_size - sizeof(pgno_t);
  }

  return EVEN(sz + sizeof(indx_t));
}

/** Calculate the size of a branch node.
 * The size should depend on the environment's page size but since
 * we currently don't support spilling large keys onto overflow
 * pages, it's simply the size of the #MDB_node header plus the
 * size of the key. Sizes are always rounded up to an even number
 * of bytes, to guarantee 2-byte alignment of the #MDB_node headers.
 * @param[in] env The environment handle.
 * @param[in] key The key for the node.
 * @return The number of bytes needed to store the node.
 */
static __inline size_t mdbx_branch_size(MDB_env *env, MDB_val *key) {
  size_t sz;

  sz = INDXSIZE(key);
  if (unlikely(sz > env->me_nodemax)) {
    /* put on overflow page */
    /* not implemented */
    mdbx_assert_fail(env, "INDXSIZE(key) <= env->me_nodemax", __FUNCTION__,
                     __LINE__);
    sz -= key->mv_size - sizeof(pgno_t);
  }

  return sz + sizeof(indx_t);
}

/** Add a node to the page pointed to by the cursor.
 * Set #MDB_TXN_ERROR on failure.
 * @param[in] mc The cursor for this operation.
 * @param[in] indx The index on the page where the new node should be added.
 * @param[in] key The key for the new node.
 * @param[in] data The data for the new node, if any.
 * @param[in] pgno The page number, if adding a branch node.
 * @param[in] flags Flags for the node.
 * @return 0 on success, non-zero on failure. Possible errors are:
 * <ul>
 *	<li>MDBX_ENOMEM - failed to allocate overflow pages for the node.
 *	<li>MDB_PAGE_FULL - there is insufficient room in the page. This error
 *	should never happen since all callers already calculate the
 *	page's free space before calling this function.
 * </ul>
 */
static int mdbx_node_add(MDB_cursor *mc, indx_t indx, MDB_val *key,
                         MDB_val *data, pgno_t pgno, unsigned flags) {
  unsigned i;
  size_t node_size = NODESIZE;
  ssize_t room;
  unsigned ofs;
  MDB_node *node;
  MDB_page *mp = mc->mc_pg[mc->mc_top];
  MDB_page *ofp = NULL; /* overflow page */
  void *ndata;
  DKBUF;

  mdbx_cassert(mc, mp->mp_upper >= mp->mp_lower);

  mdbx_debug("add to %s %spage %" PRIuPTR " index %i, data size %" PRIuPTR
             " key size %" PRIuPTR " [%s]",
             IS_LEAF(mp) ? "leaf" : "branch", IS_SUBP(mp) ? "sub-" : "",
             mdbx_dbg_pgno(mp), indx, data ? data->mv_size : 0,
             key ? key->mv_size : 0, key ? DKEY(key) : "null");

  if (IS_LEAF2(mp)) {
    mdbx_cassert(mc, key);
    /* Move higher keys up one slot. */
    int ksize = mc->mc_db->md_xsize, dif;
    char *ptr = LEAF2KEY(mp, indx, ksize);
    dif = NUMKEYS(mp) - indx;
    if (dif > 0)
      memmove(ptr + ksize, ptr, dif * ksize);
    /* insert new key */
    memcpy(ptr, key->mv_data, ksize);

    /* Just using these for counting */
    mp->mp_lower += sizeof(indx_t);
    mp->mp_upper -= ksize - sizeof(indx_t);
    return MDB_SUCCESS;
  }

  room = (ssize_t)SIZELEFT(mp) - (ssize_t)sizeof(indx_t);
  if (key != NULL)
    node_size += key->mv_size;
  if (IS_LEAF(mp)) {
    mdbx_cassert(mc, key && data);
    if (unlikely(F_ISSET(flags, F_BIGDATA))) {
      /* Data already on overflow page. */
      node_size += sizeof(pgno_t);
    } else if (unlikely(node_size + data->mv_size >
                        mc->mc_txn->mt_env->me_nodemax)) {
      int ovpages = OVPAGES(data->mv_size, mc->mc_txn->mt_env->me_psize);
      int rc;
      /* Put data on overflow page. */
      mdbx_debug("data size is %" PRIuPTR ", node would be %" PRIuPTR
                 ", put data on overflow page",
                 data->mv_size, node_size + data->mv_size);
      node_size = EVEN(node_size + sizeof(pgno_t));
      if ((ssize_t)node_size > room)
        goto full;
      if ((rc = mdbx_page_new(mc, P_OVERFLOW, ovpages, &ofp)))
        return rc;
      mdbx_debug("allocated overflow page %" PRIuPTR "", ofp->mp_pgno);
      flags |= F_BIGDATA;
      goto update;
    } else {
      node_size += data->mv_size;
    }
  }
  node_size = EVEN(node_size);
  if (unlikely((ssize_t)node_size > room))
    goto full;

update:
  /* Move higher pointers up one slot. */
  for (i = NUMKEYS(mp); i > indx; i--)
    mp->mp_ptrs[i] = mp->mp_ptrs[i - 1];

  /* Adjust free space offsets. */
  ofs = mp->mp_upper - node_size;
  mdbx_cassert(mc, ofs >= mp->mp_lower + sizeof(indx_t));
  mp->mp_ptrs[indx] = (uint16_t)ofs;
  mp->mp_upper = (uint16_t)ofs;
  mp->mp_lower += sizeof(indx_t);

  /* Write the node data. */
  node = NODEPTR(mp, indx);
  node->mn_ksize = (key == NULL) ? 0 : (uint16_t)key->mv_size;
  node->mn_flags = flags;
  if (IS_LEAF(mp))
    SETDSZ(node, data->mv_size);
  else
    SETPGNO(node, pgno);

  if (key)
    memcpy(NODEKEY(node), key->mv_data, key->mv_size);

  if (IS_LEAF(mp)) {
    ndata = NODEDATA(node);
    if (unlikely(ofp == NULL)) {
      if (unlikely(F_ISSET(flags, F_BIGDATA)))
        memcpy(ndata, data->mv_data, sizeof(pgno_t));
      else if (F_ISSET(flags, MDB_RESERVE))
        data->mv_data = ndata;
      else if (likely(ndata != data->mv_data))
        memcpy(ndata, data->mv_data, data->mv_size);
    } else {
      memcpy(ndata, &ofp->mp_pgno, sizeof(pgno_t));
      ndata = PAGEDATA(ofp);
      if (F_ISSET(flags, MDB_RESERVE))
        data->mv_data = ndata;
      else if (likely(ndata != data->mv_data))
        memcpy(ndata, data->mv_data, data->mv_size);
    }
  }

  return MDB_SUCCESS;

full:
  mdbx_debug("not enough room in page %" PRIuPTR ", got %u ptrs",
             mdbx_dbg_pgno(mp), NUMKEYS(mp));
  mdbx_debug("upper-lower = %u - %u = %" PRIiPTR "", mp->mp_upper, mp->mp_lower,
             room);
  mdbx_debug("node size = %" PRIuPTR "", node_size);
  mc->mc_txn->mt_flags |= MDB_TXN_ERROR;
  return MDB_PAGE_FULL;
}

/** Delete the specified node from a page.
 * @param[in] mc Cursor pointing to the node to delete.
 * @param[in] ksize The size of a node. Only used if the page is
 * part of a #MDB_DUPFIXED database.
 */
static void mdbx_node_del(MDB_cursor *mc, int ksize) {
  MDB_page *mp = mc->mc_pg[mc->mc_top];
  indx_t indx = mc->mc_ki[mc->mc_top];
  unsigned sz;
  indx_t i, j, numkeys, ptr;
  MDB_node *node;
  char *base;

  mdbx_debug("delete node %u on %s page %" PRIuPTR "", indx,
             IS_LEAF(mp) ? "leaf" : "branch", mdbx_dbg_pgno(mp));
  numkeys = NUMKEYS(mp);
  mdbx_cassert(mc, indx < numkeys);

  if (IS_LEAF2(mp)) {
    int x = numkeys - 1 - indx;
    base = LEAF2KEY(mp, indx, ksize);
    if (x)
      memmove(base, base + ksize, x * ksize);
    mp->mp_lower -= sizeof(indx_t);
    mp->mp_upper += ksize - sizeof(indx_t);
    return;
  }

  node = NODEPTR(mp, indx);
  sz = NODESIZE + node->mn_ksize;
  if (IS_LEAF(mp)) {
    if (F_ISSET(node->mn_flags, F_BIGDATA))
      sz += sizeof(pgno_t);
    else
      sz += NODEDSZ(node);
  }
  sz = EVEN(sz);

  ptr = mp->mp_ptrs[indx];
  for (i = j = 0; i < numkeys; i++) {
    if (i != indx) {
      mp->mp_ptrs[j] = mp->mp_ptrs[i];
      if (mp->mp_ptrs[i] < ptr)
        mp->mp_ptrs[j] += sz;
      j++;
    }
  }

  base = (char *)mp + mp->mp_upper + PAGEBASE;
  memmove(base + sz, base, ptr - mp->mp_upper);

  mp->mp_lower -= sizeof(indx_t);
  mp->mp_upper += sz;
}

/** Compact the main page after deleting a node on a subpage.
 * @param[in] mp The main page to operate on.
 * @param[in] indx The index of the subpage on the main page.
 */
static void mdbx_node_shrink(MDB_page *mp, indx_t indx) {
  MDB_node *node;
  MDB_page *sp, *xp;
  char *base;
  unsigned nsize, delta, len, ptr;
  int i;

  node = NODEPTR(mp, indx);
  sp = (MDB_page *)NODEDATA(node);
  delta = SIZELEFT(sp);
  nsize = NODEDSZ(node) - delta;

  /* Prepare to shift upward, set len = length(subpage part to shift) */
  if (IS_LEAF2(sp)) {
    len = nsize;
    if (nsize & 1)
      return; /* do not make the node uneven-sized */
  } else {
    xp = (MDB_page *)((char *)sp + delta); /* destination subpage */
    for (i = NUMKEYS(sp); --i >= 0;)
      xp->mp_ptrs[i] = sp->mp_ptrs[i] - delta;
    len = PAGEHDRSZ;
  }
  sp->mp_upper = sp->mp_lower;
  COPY_PGNO(sp->mp_pgno, mp->mp_pgno);
  SETDSZ(node, nsize);

  /* Shift <lower nodes...initial part of subpage> upward */
  base = (char *)mp + mp->mp_upper + PAGEBASE;
  memmove(base + delta, base, (char *)sp + len - base);

  ptr = mp->mp_ptrs[indx];
  for (i = NUMKEYS(mp); --i >= 0;) {
    if (mp->mp_ptrs[i] <= ptr)
      mp->mp_ptrs[i] += delta;
  }
  mp->mp_upper += delta;
}

/** Initial setup of a sorted-dups cursor.
 * Sorted duplicates are implemented as a sub-database for the given key.
 * The duplicate data items are actually keys of the sub-database.
 * Operations on the duplicate data items are performed using a sub-cursor
 * initialized when the sub-database is first accessed. This function does
 * the preliminary setup of the sub-cursor, filling in the fields that
 * depend only on the parent DB.
 * @param[in] mc The main cursor whose sorted-dups cursor is to be
 * initialized.
 */
static void mdbx_xcursor_init0(MDB_cursor *mc) {
  MDB_xcursor *mx = mc->mc_xcursor;

  mx->mx_cursor.mc_xcursor = NULL;
  mx->mx_cursor.mc_txn = mc->mc_txn;
  mx->mx_cursor.mc_db = &mx->mx_db;
  mx->mx_cursor.mc_dbx = &mx->mx_dbx;
  mx->mx_cursor.mc_dbi = mc->mc_dbi;
  mx->mx_cursor.mc_dbflag = &mx->mx_dbflag;
  mx->mx_cursor.mc_snum = 0;
  mx->mx_cursor.mc_top = 0;
  mx->mx_cursor.mc_flags = C_SUB;
  mx->mx_dbx.md_name.mv_size = 0;
  mx->mx_dbx.md_name.mv_data = NULL;
  mx->mx_dbx.md_cmp = mc->mc_dbx->md_dcmp;
  mx->mx_dbx.md_dcmp = NULL;
}

/** Final setup of a sorted-dups cursor.
 *	Sets up the fields that depend on the data from the main cursor.
 * @param[in] mc The main cursor whose sorted-dups cursor is to be
 *initialized.
 * @param[in] node The data containing the #MDB_db record for the
 * sorted-dup database.
 */
static void mdbx_xcursor_init1(MDB_cursor *mc, MDB_node *node) {
  MDB_xcursor *mx = mc->mc_xcursor;

  if (node->mn_flags & F_SUBDATA) {
    memcpy(&mx->mx_db, NODEDATA(node), sizeof(MDB_db));
    mx->mx_cursor.mc_pg[0] = 0;
    mx->mx_cursor.mc_snum = 0;
    mx->mx_cursor.mc_top = 0;
    mx->mx_cursor.mc_flags = C_SUB;
  } else {
    MDB_page *fp = NODEDATA(node);
    mx->mx_db.md_xsize = 0;
    mx->mx_db.md_flags = 0;
    mx->mx_db.md_depth = 1;
    mx->mx_db.md_branch_pages = 0;
    mx->mx_db.md_leaf_pages = 1;
    mx->mx_db.md_overflow_pages = 0;
    mx->mx_db.md_entries = NUMKEYS(fp);
    COPY_PGNO(mx->mx_db.md_root, fp->mp_pgno);
    mx->mx_cursor.mc_snum = 1;
    mx->mx_cursor.mc_top = 0;
    mx->mx_cursor.mc_flags = C_INITIALIZED | C_SUB;
    mx->mx_cursor.mc_pg[0] = fp;
    mx->mx_cursor.mc_ki[0] = 0;
    if (mc->mc_db->md_flags & MDB_DUPFIXED) {
      mx->mx_db.md_flags = MDB_DUPFIXED;
      mx->mx_db.md_xsize = fp->mp_leaf2_ksize;
      if (mc->mc_db->md_flags & MDB_INTEGERDUP)
        mx->mx_db.md_flags |= MDB_INTEGERKEY;
    }
  }
  mdbx_debug("Sub-db -%u root page %" PRIuPTR "", mx->mx_cursor.mc_dbi,
             mx->mx_db.md_root);
  mx->mx_dbflag = DB_VALID | DB_USRVALID | DB_DUPDATA;
  /* #if UINT_MAX < SIZE_MAX
          if (mx->mx_dbx.md_cmp == mdbx_cmp_int && mx->mx_db.md_pad ==
  sizeof(size_t))
                  mx->mx_dbx.md_cmp = mdbx_cmp_clong;
  #endif */
}

/** Fixup a sorted-dups cursor due to underlying update.
 *	Sets up some fields that depend on the data from the main cursor.
 *	Almost the same as init1, but skips initialization steps if the
 *	xcursor had already been used.
 * @param[in] mc The main cursor whose sorted-dups cursor is to be fixed up.
 * @param[in] src_mx The xcursor of an up-to-date cursor.
 * @param[in] new_dupdata True if converting from a non-#F_DUPDATA item.
 */
static void mdbx_xcursor_init2(MDB_cursor *mc, MDB_xcursor *src_mx,
                               int new_dupdata) {
  MDB_xcursor *mx = mc->mc_xcursor;

  if (new_dupdata) {
    mx->mx_cursor.mc_snum = 1;
    mx->mx_cursor.mc_top = 0;
    mx->mx_cursor.mc_flags |= C_INITIALIZED;
    mx->mx_cursor.mc_ki[0] = 0;
    mx->mx_dbflag = DB_VALID | DB_USRVALID | DB_DUPDATA;
    mx->mx_dbx.md_cmp = src_mx->mx_dbx.md_cmp;
  } else if (!(mx->mx_cursor.mc_flags & C_INITIALIZED)) {
    return;
  }
  mx->mx_db = src_mx->mx_db;
  mx->mx_cursor.mc_pg[0] = src_mx->mx_cursor.mc_pg[0];
  mdbx_debug("Sub-db -%u root page %" PRIuPTR "", mx->mx_cursor.mc_dbi,
             mx->mx_db.md_root);
}

/** Initialize a cursor for a given transaction and database. */
static void mdbx_cursor_init(MDB_cursor *mc, MDB_txn *txn, MDB_dbi dbi,
                             MDB_xcursor *mx) {
  mc->mc_signature = MDBX_MC_SIGNATURE;
  mc->mc_next = NULL;
  mc->mc_backup = NULL;
  mc->mc_dbi = dbi;
  mc->mc_txn = txn;
  mc->mc_db = &txn->mt_dbs[dbi];
  mc->mc_dbx = &txn->mt_dbxs[dbi];
  mc->mc_dbflag = &txn->mt_dbflags[dbi];
  mc->mc_snum = 0;
  mc->mc_top = 0;
  mc->mc_pg[0] = 0;
  mc->mc_flags = 0;
  mc->mc_ki[0] = 0;
  mc->mc_xcursor = NULL;
  if (txn->mt_dbs[dbi].md_flags & MDB_DUPSORT) {
    mdbx_tassert(txn, mx != NULL);
    mx->mx_cursor.mc_signature = MDBX_MC_SIGNATURE;
    mc->mc_xcursor = mx;
    mdbx_xcursor_init0(mc);
  }
  if (unlikely(*mc->mc_dbflag & DB_STALE)) {
    mdbx_page_search(mc, NULL, MDB_PS_ROOTONLY);
  }
}

int mdbx_cursor_open(MDB_txn *txn, MDB_dbi dbi, MDB_cursor **ret) {
  MDB_cursor *mc;
  size_t size = sizeof(MDB_cursor);

  if (unlikely(!ret || !txn))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_signature != MDBX_MT_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(!TXN_DBI_EXIST(txn, dbi, DB_VALID)))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_flags & MDB_TXN_BLOCKED))
    return MDB_BAD_TXN;

  if (unlikely(dbi == FREE_DBI && !F_ISSET(txn->mt_flags, MDB_TXN_RDONLY)))
    return MDBX_EINVAL;

  if (txn->mt_dbs[dbi].md_flags & MDB_DUPSORT)
    size += sizeof(MDB_xcursor);

  if (likely((mc = malloc(size)) != NULL)) {
    mdbx_cursor_init(mc, txn, dbi, (MDB_xcursor *)(mc + 1));
    if (txn->mt_cursors) {
      mc->mc_next = txn->mt_cursors[dbi];
      txn->mt_cursors[dbi] = mc;
      mc->mc_flags |= C_UNTRACK;
    }
  } else {
    return MDBX_ENOMEM;
  }

  *ret = mc;

  return MDB_SUCCESS;
}

int mdbx_cursor_renew(MDB_txn *txn, MDB_cursor *mc) {
  if (unlikely(!mc || !txn))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_signature != MDBX_MT_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(mc->mc_signature != MDBX_MC_SIGNATURE &&
               mc->mc_signature != MDBX_MC_READY4CLOSE))
    return MDBX_EINVAL;

  if (unlikely(!TXN_DBI_EXIST(txn, mc->mc_dbi, DB_VALID)))
    return MDBX_EINVAL;

  if (unlikely(mc->mc_backup))
    return MDBX_EINVAL;

  if (unlikely((mc->mc_flags & C_UNTRACK) || txn->mt_cursors)) {
    MDB_cursor **prev = &mc->mc_txn->mt_cursors[mc->mc_dbi];
    while (*prev && *prev != mc)
      prev = &(*prev)->mc_next;
    if (*prev == mc)
      *prev = mc->mc_next;
    mc->mc_signature = MDBX_MC_READY4CLOSE;
  }

  if (unlikely(txn->mt_flags & MDB_TXN_BLOCKED))
    return MDB_BAD_TXN;

  mdbx_cursor_init(mc, txn, mc->mc_dbi, mc->mc_xcursor);
  return MDB_SUCCESS;
}

/* Return the count of duplicate data items for the current key */
int mdbx_cursor_count(MDB_cursor *mc, size_t *countp) {
  if (unlikely(mc == NULL || countp == NULL))
    return MDBX_EINVAL;

  if (unlikely(mc->mc_signature != MDBX_MC_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(mc->mc_txn->mt_flags & MDB_TXN_BLOCKED))
    return MDB_BAD_TXN;

  if (unlikely(!(mc->mc_flags & C_INITIALIZED)))
    return MDBX_EINVAL;

  if (!mc->mc_snum) {
    *countp = 0;
    return MDB_NOTFOUND;
  }

  MDB_page *mp = mc->mc_pg[mc->mc_top];
  if ((mc->mc_flags & C_EOF) && mc->mc_ki[mc->mc_top] >= NUMKEYS(mp)) {
    *countp = 0;
    return MDB_NOTFOUND;
  }

  *countp = 1;
  if (mc->mc_xcursor != NULL) {
    MDB_node *leaf = NODEPTR(mp, mc->mc_ki[mc->mc_top]);
    if (F_ISSET(leaf->mn_flags, F_DUPDATA)) {
      mdbx_cassert(mc, mc->mc_xcursor && (mc->mc_xcursor->mx_cursor.mc_flags &
                                          C_INITIALIZED));
      *countp = mc->mc_xcursor->mx_db.md_entries;
    }
  }
  return MDB_SUCCESS;
}

void mdbx_cursor_close(MDB_cursor *mc) {
  if (mc) {
    mdbx_ensure(NULL, mc->mc_signature == MDBX_MC_SIGNATURE ||
                          mc->mc_signature == MDBX_MC_READY4CLOSE);
    if (!mc->mc_backup) {
      /* Remove from txn, if tracked.
       * A read-only txn (!C_UNTRACK) may have been freed already,
       * so do not peek inside it.  Only write txns track cursors. */
      if ((mc->mc_flags & C_UNTRACK) && mc->mc_txn->mt_cursors) {
        MDB_cursor **prev = &mc->mc_txn->mt_cursors[mc->mc_dbi];
        while (*prev && *prev != mc)
          prev = &(*prev)->mc_next;
        if (*prev == mc)
          *prev = mc->mc_next;
      }
      mc->mc_signature = 0;
      free(mc);
    } else {
      /* cursor closed before nested txn ends */
      mdbx_cassert(mc, mc->mc_signature == MDBX_MC_SIGNATURE);
      mc->mc_signature = MDBX_MC_WAIT4EOT;
    }
  }
}

MDB_txn *mdbx_cursor_txn(MDB_cursor *mc) {
  if (unlikely(!mc || mc->mc_signature != MDBX_MC_SIGNATURE))
    return NULL;
  return mc->mc_txn;
}

MDB_dbi mdbx_cursor_dbi(MDB_cursor *mc) {
  if (unlikely(!mc || mc->mc_signature != MDBX_MC_SIGNATURE))
    return INT_MIN;
  return mc->mc_dbi;
}

/** Replace the key for a branch node with a new key.
 * Set #MDB_TXN_ERROR on failure.
 * @param[in] mc Cursor pointing to the node to operate on.
 * @param[in] key The new key to use.
 * @return 0 on success, non-zero on failure.
 */
static int mdbx_update_key(MDB_cursor *mc, MDB_val *key) {
  MDB_page *mp;
  MDB_node *node;
  char *base;
  size_t len;
  int delta, ksize, oksize;
  indx_t ptr, i, numkeys, indx;
  DKBUF;

  indx = mc->mc_ki[mc->mc_top];
  mp = mc->mc_pg[mc->mc_top];
  node = NODEPTR(mp, indx);
  ptr = mp->mp_ptrs[indx];
  {
    MDB_val k2;
    char kbuf2[DKBUF_MAXKEYSIZE * 2 + 1];
    k2.mv_data = NODEKEY(node);
    k2.mv_size = node->mn_ksize;
    mdbx_debug("update key %u (ofs %u) [%s] to [%s] on page %" PRIuPTR "", indx,
               ptr, mdbx_dkey(&k2, kbuf2, sizeof(kbuf2)), DKEY(key),
               mp->mp_pgno);
  }

  /* Sizes must be 2-byte aligned. */
  ksize = EVEN(key->mv_size);
  oksize = EVEN(node->mn_ksize);
  delta = ksize - oksize;

  /* Shift node contents if EVEN(key length) changed. */
  if (delta) {
    if (delta > 0 && SIZELEFT(mp) < delta) {
      pgno_t pgno;
      /* not enough space left, do a delete and split */
      mdbx_debug("Not enough room, delta = %d, splitting...", delta);
      pgno = NODEPGNO(node);
      mdbx_node_del(mc, 0);
      return mdbx_page_split(mc, key, NULL, pgno, MDB_SPLIT_REPLACE);
    }

    numkeys = NUMKEYS(mp);
    for (i = 0; i < numkeys; i++) {
      if (mp->mp_ptrs[i] <= ptr)
        mp->mp_ptrs[i] -= delta;
    }

    base = (char *)mp + mp->mp_upper + PAGEBASE;
    len = ptr - mp->mp_upper + NODESIZE;
    memmove(base - delta, base, len);
    mp->mp_upper -= delta;

    node = NODEPTR(mp, indx);
  }

  /* But even if no shift was needed, update ksize */
  if (node->mn_ksize != key->mv_size)
    node->mn_ksize = (uint16_t)key->mv_size;

  if (key->mv_size)
    memcpy(NODEKEY(node), key->mv_data, key->mv_size);

  return MDB_SUCCESS;
}

static void mdbx_cursor_copy(const MDB_cursor *csrc, MDB_cursor *cdst);

/** Perform \b act while tracking temporary cursor \b mn */
#define WITH_CURSOR_TRACKING(mn, act)                                          \
  do {                                                                         \
    MDB_cursor mc_dummy, *tracked, **tp = &(mn).mc_txn->mt_cursors[mn.mc_dbi]; \
    if ((mn).mc_flags & C_SUB) {                                               \
      mc_dummy.mc_flags = C_INITIALIZED;                                       \
      mc_dummy.mc_xcursor = (MDB_xcursor *)&(mn);                              \
      tracked = &mc_dummy;                                                     \
    } else {                                                                   \
      tracked = &(mn);                                                         \
    }                                                                          \
    tracked->mc_next = *tp;                                                    \
    *tp = tracked;                                                             \
    { act; }                                                                   \
    *tp = tracked->mc_next;                                                    \
  } while (0)

/** Move a node from csrc to cdst.
 */
static int mdbx_node_move(MDB_cursor *csrc, MDB_cursor *cdst, int fromleft) {
  MDB_node *srcnode;
  MDB_val key, data;
  pgno_t srcpg;
  MDB_cursor mn;
  int rc;
  unsigned flags;

  DKBUF;

  /* Mark src and dst as dirty. */
  if (unlikely((rc = mdbx_page_touch(csrc)) || (rc = mdbx_page_touch(cdst))))
    return rc;

  if (IS_LEAF2(csrc->mc_pg[csrc->mc_top])) {
    key.mv_size = csrc->mc_db->md_xsize;
    key.mv_data = LEAF2KEY(csrc->mc_pg[csrc->mc_top], csrc->mc_ki[csrc->mc_top],
                           key.mv_size);
    data.mv_size = 0;
    data.mv_data = NULL;
    srcpg = 0;
    flags = 0;
  } else {
    srcnode = NODEPTR(csrc->mc_pg[csrc->mc_top], csrc->mc_ki[csrc->mc_top]);
    mdbx_cassert(csrc, !((size_t)srcnode & 1));
    srcpg = NODEPGNO(srcnode);
    flags = srcnode->mn_flags;
    if (csrc->mc_ki[csrc->mc_top] == 0 &&
        IS_BRANCH(csrc->mc_pg[csrc->mc_top])) {
      unsigned snum = csrc->mc_snum;
      MDB_node *s2;
      /* must find the lowest key below src */
      rc = mdbx_page_search_lowest(csrc);
      if (unlikely(rc))
        return rc;
      if (IS_LEAF2(csrc->mc_pg[csrc->mc_top])) {
        key.mv_size = csrc->mc_db->md_xsize;
        key.mv_data = LEAF2KEY(csrc->mc_pg[csrc->mc_top], 0, key.mv_size);
      } else {
        s2 = NODEPTR(csrc->mc_pg[csrc->mc_top], 0);
        key.mv_size = NODEKSZ(s2);
        key.mv_data = NODEKEY(s2);
      }
      csrc->mc_snum = snum--;
      csrc->mc_top = snum;
    } else {
      key.mv_size = NODEKSZ(srcnode);
      key.mv_data = NODEKEY(srcnode);
    }
    data.mv_size = NODEDSZ(srcnode);
    data.mv_data = NODEDATA(srcnode);
  }
  mn.mc_xcursor = NULL;
  if (IS_BRANCH(cdst->mc_pg[cdst->mc_top]) && cdst->mc_ki[cdst->mc_top] == 0) {
    unsigned snum = cdst->mc_snum;
    MDB_node *s2;
    MDB_val bkey;
    /* must find the lowest key below dst */
    mdbx_cursor_copy(cdst, &mn);
    rc = mdbx_page_search_lowest(&mn);
    if (unlikely(rc))
      return rc;
    if (IS_LEAF2(mn.mc_pg[mn.mc_top])) {
      bkey.mv_size = mn.mc_db->md_xsize;
      bkey.mv_data = LEAF2KEY(mn.mc_pg[mn.mc_top], 0, bkey.mv_size);
    } else {
      s2 = NODEPTR(mn.mc_pg[mn.mc_top], 0);
      bkey.mv_size = NODEKSZ(s2);
      bkey.mv_data = NODEKEY(s2);
    }
    mn.mc_snum = snum--;
    mn.mc_top = snum;
    mn.mc_ki[snum] = 0;
    rc = mdbx_update_key(&mn, &bkey);
    if (unlikely(rc))
      return rc;
  }

  mdbx_debug("moving %s node %u [%s] on page %" PRIuPTR
             " to node %u on page %" PRIuPTR "",
             IS_LEAF(csrc->mc_pg[csrc->mc_top]) ? "leaf" : "branch",
             csrc->mc_ki[csrc->mc_top], DKEY(&key),
             csrc->mc_pg[csrc->mc_top]->mp_pgno, cdst->mc_ki[cdst->mc_top],
             cdst->mc_pg[cdst->mc_top]->mp_pgno);

  /* Add the node to the destination page. */
  rc =
      mdbx_node_add(cdst, cdst->mc_ki[cdst->mc_top], &key, &data, srcpg, flags);
  if (unlikely(rc != MDB_SUCCESS))
    return rc;

  /* Delete the node from the source page. */
  mdbx_node_del(csrc, key.mv_size);

  {
    /* Adjust other cursors pointing to mp */
    MDB_cursor *m2, *m3;
    MDB_dbi dbi = csrc->mc_dbi;
    MDB_page *mpd, *mps;

    mps = csrc->mc_pg[csrc->mc_top];
    /* If we're adding on the left, bump others up */
    if (fromleft) {
      mpd = cdst->mc_pg[csrc->mc_top];
      for (m2 = csrc->mc_txn->mt_cursors[dbi]; m2; m2 = m2->mc_next) {
        if (csrc->mc_flags & C_SUB)
          m3 = &m2->mc_xcursor->mx_cursor;
        else
          m3 = m2;
        if (!(m3->mc_flags & C_INITIALIZED) || m3->mc_top < csrc->mc_top)
          continue;
        if (m3 != cdst && m3->mc_pg[csrc->mc_top] == mpd &&
            m3->mc_ki[csrc->mc_top] >= cdst->mc_ki[csrc->mc_top]) {
          m3->mc_ki[csrc->mc_top]++;
        }
        if (m3 != csrc && m3->mc_pg[csrc->mc_top] == mps &&
            m3->mc_ki[csrc->mc_top] == csrc->mc_ki[csrc->mc_top]) {
          m3->mc_pg[csrc->mc_top] = cdst->mc_pg[cdst->mc_top];
          m3->mc_ki[csrc->mc_top] = cdst->mc_ki[cdst->mc_top];
          m3->mc_ki[csrc->mc_top - 1]++;
        }
        if (XCURSOR_INITED(m3) && IS_LEAF(mps))
          XCURSOR_REFRESH(m3, m3->mc_pg[csrc->mc_top], m3->mc_ki[csrc->mc_top]);
      }
    } else
    /* Adding on the right, bump others down */
    {
      for (m2 = csrc->mc_txn->mt_cursors[dbi]; m2; m2 = m2->mc_next) {
        if (csrc->mc_flags & C_SUB)
          m3 = &m2->mc_xcursor->mx_cursor;
        else
          m3 = m2;
        if (m3 == csrc)
          continue;
        if (!(m3->mc_flags & C_INITIALIZED) || m3->mc_top < csrc->mc_top)
          continue;
        if (m3->mc_pg[csrc->mc_top] == mps) {
          if (!m3->mc_ki[csrc->mc_top]) {
            m3->mc_pg[csrc->mc_top] = cdst->mc_pg[cdst->mc_top];
            m3->mc_ki[csrc->mc_top] = cdst->mc_ki[cdst->mc_top];
            m3->mc_ki[csrc->mc_top - 1]--;
          } else {
            m3->mc_ki[csrc->mc_top]--;
          }
          if (XCURSOR_INITED(m3) && IS_LEAF(mps))
            XCURSOR_REFRESH(m3, m3->mc_pg[csrc->mc_top],
                            m3->mc_ki[csrc->mc_top]);
        }
      }
    }
  }

  /* Update the parent separators. */
  if (csrc->mc_ki[csrc->mc_top] == 0) {
    if (csrc->mc_ki[csrc->mc_top - 1] != 0) {
      if (IS_LEAF2(csrc->mc_pg[csrc->mc_top])) {
        key.mv_data = LEAF2KEY(csrc->mc_pg[csrc->mc_top], 0, key.mv_size);
      } else {
        srcnode = NODEPTR(csrc->mc_pg[csrc->mc_top], 0);
        key.mv_size = NODEKSZ(srcnode);
        key.mv_data = NODEKEY(srcnode);
      }
      mdbx_debug("update separator for source page %" PRIuPTR " to [%s]",
                 csrc->mc_pg[csrc->mc_top]->mp_pgno, DKEY(&key));
      mdbx_cursor_copy(csrc, &mn);
      mn.mc_snum--;
      mn.mc_top--;
      /* We want mdbx_rebalance to find mn when doing fixups */
      WITH_CURSOR_TRACKING(mn, rc = mdbx_update_key(&mn, &key));
      if (unlikely(rc != MDB_SUCCESS))
        return rc;
    }
    if (IS_BRANCH(csrc->mc_pg[csrc->mc_top])) {
      MDB_val nullkey;
      indx_t ix = csrc->mc_ki[csrc->mc_top];
      nullkey.mv_size = 0;
      csrc->mc_ki[csrc->mc_top] = 0;
      rc = mdbx_update_key(csrc, &nullkey);
      csrc->mc_ki[csrc->mc_top] = ix;
      mdbx_cassert(csrc, rc == MDB_SUCCESS);
    }
  }

  if (cdst->mc_ki[cdst->mc_top] == 0) {
    if (cdst->mc_ki[cdst->mc_top - 1] != 0) {
      if (IS_LEAF2(csrc->mc_pg[csrc->mc_top])) {
        key.mv_data = LEAF2KEY(cdst->mc_pg[cdst->mc_top], 0, key.mv_size);
      } else {
        srcnode = NODEPTR(cdst->mc_pg[cdst->mc_top], 0);
        key.mv_size = NODEKSZ(srcnode);
        key.mv_data = NODEKEY(srcnode);
      }
      mdbx_debug("update separator for destination page %" PRIuPTR " to [%s]",
                 cdst->mc_pg[cdst->mc_top]->mp_pgno, DKEY(&key));
      mdbx_cursor_copy(cdst, &mn);
      mn.mc_snum--;
      mn.mc_top--;
      /* We want mdbx_rebalance to find mn when doing fixups */
      WITH_CURSOR_TRACKING(mn, rc = mdbx_update_key(&mn, &key));
      if (unlikely(rc != MDB_SUCCESS))
        return rc;
    }
    if (IS_BRANCH(cdst->mc_pg[cdst->mc_top])) {
      MDB_val nullkey;
      indx_t ix = cdst->mc_ki[cdst->mc_top];
      nullkey.mv_size = 0;
      cdst->mc_ki[cdst->mc_top] = 0;
      rc = mdbx_update_key(cdst, &nullkey);
      cdst->mc_ki[cdst->mc_top] = ix;
      mdbx_cassert(cdst, rc == MDB_SUCCESS);
    }
  }

  return MDB_SUCCESS;
}

/** Merge one page into another.
 *  The nodes from the page pointed to by \b csrc will
 *	be copied to the page pointed to by \b cdst and then
 *	the \b csrc page will be freed.
 * @param[in] csrc Cursor pointing to the source page.
 * @param[in] cdst Cursor pointing to the destination page.
 * @return 0 on success, non-zero on failure.
 */
static int mdbx_page_merge(MDB_cursor *csrc, MDB_cursor *cdst) {
  MDB_page *psrc, *pdst;
  MDB_node *srcnode;
  MDB_val key, data;
  unsigned nkeys;
  int rc;
  indx_t i, j;

  psrc = csrc->mc_pg[csrc->mc_top];
  pdst = cdst->mc_pg[cdst->mc_top];

  mdbx_debug("merging page %" PRIuPTR " into %" PRIuPTR "", psrc->mp_pgno,
             pdst->mp_pgno);

  mdbx_cassert(csrc, csrc->mc_snum > 1); /* can't merge root page */
  mdbx_cassert(csrc, cdst->mc_snum > 1);

  /* Mark dst as dirty. */
  if (unlikely(rc = mdbx_page_touch(cdst)))
    return rc;

  /* get dst page again now that we've touched it. */
  pdst = cdst->mc_pg[cdst->mc_top];

  /* Move all nodes from src to dst.
   */
  j = nkeys = NUMKEYS(pdst);
  if (IS_LEAF2(psrc)) {
    key.mv_size = csrc->mc_db->md_xsize;
    key.mv_data = PAGEDATA(psrc);
    for (i = 0; i < NUMKEYS(psrc); i++, j++) {
      rc = mdbx_node_add(cdst, j, &key, NULL, 0, 0);
      if (unlikely(rc != MDB_SUCCESS))
        return rc;
      key.mv_data = (char *)key.mv_data + key.mv_size;
    }
  } else {
    for (i = 0; i < NUMKEYS(psrc); i++, j++) {
      srcnode = NODEPTR(psrc, i);
      if (i == 0 && IS_BRANCH(psrc)) {
        MDB_cursor mn;
        MDB_node *s2;
        mdbx_cursor_copy(csrc, &mn);
        mn.mc_xcursor = NULL;
        /* must find the lowest key below src */
        rc = mdbx_page_search_lowest(&mn);
        if (unlikely(rc))
          return rc;
        if (IS_LEAF2(mn.mc_pg[mn.mc_top])) {
          key.mv_size = mn.mc_db->md_xsize;
          key.mv_data = LEAF2KEY(mn.mc_pg[mn.mc_top], 0, key.mv_size);
        } else {
          s2 = NODEPTR(mn.mc_pg[mn.mc_top], 0);
          key.mv_size = NODEKSZ(s2);
          key.mv_data = NODEKEY(s2);
        }
      } else {
        key.mv_size = srcnode->mn_ksize;
        key.mv_data = NODEKEY(srcnode);
      }

      data.mv_size = NODEDSZ(srcnode);
      data.mv_data = NODEDATA(srcnode);
      rc = mdbx_node_add(cdst, j, &key, &data, NODEPGNO(srcnode),
                         srcnode->mn_flags);
      if (unlikely(rc != MDB_SUCCESS))
        return rc;
    }
  }

  mdbx_debug("dst page %" PRIuPTR " now has %u keys (%.1f%% filled)",
             pdst->mp_pgno, NUMKEYS(pdst),
             (float)PAGEFILL(cdst->mc_txn->mt_env, pdst) / 10);

  /* Unlink the src page from parent and add to free list.
   */
  csrc->mc_top--;
  mdbx_node_del(csrc, 0);
  if (csrc->mc_ki[csrc->mc_top] == 0) {
    key.mv_size = 0;
    rc = mdbx_update_key(csrc, &key);
    if (unlikely(rc)) {
      csrc->mc_top++;
      return rc;
    }
  }
  csrc->mc_top++;

  psrc = csrc->mc_pg[csrc->mc_top];
  /* If not operating on FreeDB, allow this page to be reused
   * in this txn. Otherwise just add to free list.
   */
  rc = mdbx_page_loose(csrc, psrc);
  if (unlikely(rc))
    return rc;
  if (IS_LEAF(psrc))
    csrc->mc_db->md_leaf_pages--;
  else
    csrc->mc_db->md_branch_pages--;
  {
    /* Adjust other cursors pointing to mp */
    MDB_cursor *m2, *m3;
    MDB_dbi dbi = csrc->mc_dbi;
    unsigned top = csrc->mc_top;

    for (m2 = csrc->mc_txn->mt_cursors[dbi]; m2; m2 = m2->mc_next) {
      if (csrc->mc_flags & C_SUB)
        m3 = &m2->mc_xcursor->mx_cursor;
      else
        m3 = m2;
      if (m3 == csrc)
        continue;
      if (m3->mc_snum < csrc->mc_snum)
        continue;
      if (m3->mc_pg[top] == psrc) {
        m3->mc_pg[top] = pdst;
        m3->mc_ki[top] += nkeys;
        m3->mc_ki[top - 1] = cdst->mc_ki[top - 1];
      } else if (m3->mc_pg[top - 1] == csrc->mc_pg[top - 1] &&
                 m3->mc_ki[top - 1] > csrc->mc_ki[top - 1]) {
        m3->mc_ki[top - 1]--;
      }
      if (XCURSOR_INITED(m3) && IS_LEAF(psrc))
        XCURSOR_REFRESH(m3, m3->mc_pg[top], m3->mc_ki[top]);
    }
  }
  {
    unsigned snum = cdst->mc_snum;
    uint16_t depth = cdst->mc_db->md_depth;
    mdbx_cursor_pop(cdst);
    rc = mdbx_rebalance(cdst);
    /* Did the tree height change? */
    if (depth != cdst->mc_db->md_depth)
      snum += cdst->mc_db->md_depth - depth;
    cdst->mc_snum = snum;
    cdst->mc_top = snum - 1;
  }
  return rc;
}

/** Copy the contents of a cursor.
 * @param[in] csrc The cursor to copy from.
 * @param[out] cdst The cursor to copy to.
 */
static void mdbx_cursor_copy(const MDB_cursor *csrc, MDB_cursor *cdst) {
  unsigned i;

  cdst->mc_txn = csrc->mc_txn;
  cdst->mc_dbi = csrc->mc_dbi;
  cdst->mc_db = csrc->mc_db;
  cdst->mc_dbx = csrc->mc_dbx;
  cdst->mc_snum = csrc->mc_snum;
  cdst->mc_top = csrc->mc_top;
  cdst->mc_flags = csrc->mc_flags;

  for (i = 0; i < csrc->mc_snum; i++) {
    cdst->mc_pg[i] = csrc->mc_pg[i];
    cdst->mc_ki[i] = csrc->mc_ki[i];
  }
}

/** Rebalance the tree after a delete operation.
 * @param[in] mc Cursor pointing to the page where rebalancing
 * should begin.
 * @return 0 on success, non-zero on failure.
 */
static int mdbx_rebalance(MDB_cursor *mc) {
  MDB_node *node;
  int rc, fromleft;
  unsigned ptop, minkeys, thresh;
  MDB_cursor mn;
  indx_t oldki;

  if (IS_BRANCH(mc->mc_pg[mc->mc_top])) {
    minkeys = 2;
    thresh = 1;
  } else {
    minkeys = 1;
    thresh = FILL_THRESHOLD;
  }
  mdbx_debug("rebalancing %s page %" PRIuPTR " (has %u keys, %.1f%% full)",
             IS_LEAF(mc->mc_pg[mc->mc_top]) ? "leaf" : "branch",
             mdbx_dbg_pgno(mc->mc_pg[mc->mc_top]),
             NUMKEYS(mc->mc_pg[mc->mc_top]),
             (float)PAGEFILL(mc->mc_txn->mt_env, mc->mc_pg[mc->mc_top]) / 10);

  if (PAGEFILL(mc->mc_txn->mt_env, mc->mc_pg[mc->mc_top]) >= thresh &&
      NUMKEYS(mc->mc_pg[mc->mc_top]) >= minkeys) {
    mdbx_debug("no need to rebalance page %" PRIuPTR ", above fill threshold",
               mdbx_dbg_pgno(mc->mc_pg[mc->mc_top]));
    return MDB_SUCCESS;
  }

  if (mc->mc_snum < 2) {
    MDB_page *mp = mc->mc_pg[0];
    unsigned nkeys = NUMKEYS(mp);
    if (IS_SUBP(mp)) {
      mdbx_debug("Can't rebalance a subpage, ignoring");
      return MDB_SUCCESS;
    }
    if (nkeys == 0) {
      mdbx_debug("tree is completely empty");
      mc->mc_db->md_root = P_INVALID;
      mc->mc_db->md_depth = 0;
      mc->mc_db->md_leaf_pages = 0;
      rc = mdbx_midl_append(&mc->mc_txn->mt_free_pgs, mp->mp_pgno);
      if (unlikely(rc))
        return rc;
      /* Adjust cursors pointing to mp */
      mc->mc_snum = 0;
      mc->mc_top = 0;
      mc->mc_flags &= ~C_INITIALIZED;
      {
        MDB_cursor *m2, *m3;
        MDB_dbi dbi = mc->mc_dbi;

        for (m2 = mc->mc_txn->mt_cursors[dbi]; m2; m2 = m2->mc_next) {
          if (mc->mc_flags & C_SUB)
            m3 = &m2->mc_xcursor->mx_cursor;
          else
            m3 = m2;
          if (!(m3->mc_flags & C_INITIALIZED) || (m3->mc_snum < mc->mc_snum))
            continue;
          if (m3->mc_pg[0] == mp) {
            m3->mc_snum = 0;
            m3->mc_top = 0;
            m3->mc_flags &= ~C_INITIALIZED;
          }
        }
      }
    } else if (IS_BRANCH(mp) && NUMKEYS(mp) == 1) {
      int i;
      mdbx_debug("collapsing root page!");
      rc = mdbx_midl_append(&mc->mc_txn->mt_free_pgs, mp->mp_pgno);
      if (unlikely(rc))
        return rc;
      mc->mc_db->md_root = NODEPGNO(NODEPTR(mp, 0));
      rc = mdbx_page_get(mc, mc->mc_db->md_root, &mc->mc_pg[0], NULL);
      if (unlikely(rc))
        return rc;
      mc->mc_db->md_depth--;
      mc->mc_db->md_branch_pages--;
      mc->mc_ki[0] = mc->mc_ki[1];
      for (i = 1; i < mc->mc_db->md_depth; i++) {
        mc->mc_pg[i] = mc->mc_pg[i + 1];
        mc->mc_ki[i] = mc->mc_ki[i + 1];
      }
      {
        /* Adjust other cursors pointing to mp */
        MDB_cursor *m2, *m3;
        MDB_dbi dbi = mc->mc_dbi;

        for (m2 = mc->mc_txn->mt_cursors[dbi]; m2; m2 = m2->mc_next) {
          if (mc->mc_flags & C_SUB)
            m3 = &m2->mc_xcursor->mx_cursor;
          else
            m3 = m2;
          if (m3 == mc)
            continue;
          if (!(m3->mc_flags & C_INITIALIZED))
            continue;
          if (m3->mc_pg[0] == mp) {
            for (i = 0; i < mc->mc_db->md_depth; i++) {
              m3->mc_pg[i] = m3->mc_pg[i + 1];
              m3->mc_ki[i] = m3->mc_ki[i + 1];
            }
            m3->mc_snum--;
            m3->mc_top--;
          }
        }
      }
    } else
      mdbx_debug("root page doesn't need rebalancing");
    return MDB_SUCCESS;
  }

  /* The parent (branch page) must have at least 2 pointers,
   * otherwise the tree is invalid.
   */
  ptop = mc->mc_top - 1;
  mdbx_cassert(mc, NUMKEYS(mc->mc_pg[ptop]) > 1);

  /* Leaf page fill factor is below the threshold.
   * Try to move keys from left or right neighbor, or
   * merge with a neighbor page.
   */

  /* Find neighbors.
   */
  mdbx_cursor_copy(mc, &mn);
  mn.mc_xcursor = NULL;

  oldki = mc->mc_ki[mc->mc_top];
  if (mc->mc_ki[ptop] == 0) {
    /* We're the leftmost leaf in our parent.
     */
    mdbx_debug("reading right neighbor");
    mn.mc_ki[ptop]++;
    node = NODEPTR(mc->mc_pg[ptop], mn.mc_ki[ptop]);
    rc = mdbx_page_get(mc, NODEPGNO(node), &mn.mc_pg[mn.mc_top], NULL);
    if (unlikely(rc))
      return rc;
    mn.mc_ki[mn.mc_top] = 0;
    mc->mc_ki[mc->mc_top] = NUMKEYS(mc->mc_pg[mc->mc_top]);
    fromleft = 0;
  } else {
    /* There is at least one neighbor to the left.
     */
    mdbx_debug("reading left neighbor");
    mn.mc_ki[ptop]--;
    node = NODEPTR(mc->mc_pg[ptop], mn.mc_ki[ptop]);
    rc = mdbx_page_get(mc, NODEPGNO(node), &mn.mc_pg[mn.mc_top], NULL);
    if (unlikely(rc))
      return rc;
    mn.mc_ki[mn.mc_top] = NUMKEYS(mn.mc_pg[mn.mc_top]) - 1;
    mc->mc_ki[mc->mc_top] = 0;
    fromleft = 1;
  }

  mdbx_debug("found neighbor page %" PRIuPTR " (%u keys, %.1f%% full)",
             mn.mc_pg[mn.mc_top]->mp_pgno, NUMKEYS(mn.mc_pg[mn.mc_top]),
             (float)PAGEFILL(mc->mc_txn->mt_env, mn.mc_pg[mn.mc_top]) / 10);

  /* If the neighbor page is above threshold and has enough keys,
   * move one key from it. Otherwise we should try to merge them.
   * (A branch page must never have less than 2 keys.)
   */
  if (PAGEFILL(mc->mc_txn->mt_env, mn.mc_pg[mn.mc_top]) >= thresh &&
      NUMKEYS(mn.mc_pg[mn.mc_top]) > minkeys) {
    rc = mdbx_node_move(&mn, mc, fromleft);
    if (fromleft) {
      /* if we inserted on left, bump position up */
      oldki++;
    }
  } else {
    if (!fromleft) {
      rc = mdbx_page_merge(&mn, mc);
    } else {
      oldki += NUMKEYS(mn.mc_pg[mn.mc_top]);
      mn.mc_ki[mn.mc_top] += mc->mc_ki[mn.mc_top] + 1;
      /* We want mdbx_rebalance to find mn when doing fixups */
      WITH_CURSOR_TRACKING(mn, rc = mdbx_page_merge(mc, &mn));
      mdbx_cursor_copy(&mn, mc);
    }
    mc->mc_flags &= ~C_EOF;
  }
  mc->mc_ki[mc->mc_top] = oldki;
  return rc;
}

/** Complete a delete operation started by #mdbx_cursor_del(). */
static int mdbx_cursor_del0(MDB_cursor *mc) {
  int rc;
  MDB_page *mp;
  indx_t ki;
  unsigned nkeys;
  MDB_cursor *m2, *m3;
  MDB_dbi dbi = mc->mc_dbi;

  ki = mc->mc_ki[mc->mc_top];
  mp = mc->mc_pg[mc->mc_top];
  mdbx_node_del(mc, mc->mc_db->md_xsize);
  mc->mc_db->md_entries--;
  {
    /* Adjust other cursors pointing to mp */
    for (m2 = mc->mc_txn->mt_cursors[dbi]; m2; m2 = m2->mc_next) {
      m3 = (mc->mc_flags & C_SUB) ? &m2->mc_xcursor->mx_cursor : m2;
      if (!(m2->mc_flags & m3->mc_flags & C_INITIALIZED))
        continue;
      if (m3 == mc || m3->mc_snum < mc->mc_snum)
        continue;
      if (m3->mc_pg[mc->mc_top] == mp) {
        if (m3->mc_ki[mc->mc_top] == ki) {
          m3->mc_flags |= C_DEL;
          if (mc->mc_db->md_flags & MDB_DUPSORT) {
            /* Sub-cursor referred into dataset which is gone */
            m3->mc_xcursor->mx_cursor.mc_flags &= ~(C_INITIALIZED | C_EOF);
          }
          continue;
        } else if (m3->mc_ki[mc->mc_top] > ki) {
          m3->mc_ki[mc->mc_top]--;
        }
        if (XCURSOR_INITED(m3))
          XCURSOR_REFRESH(m3, m3->mc_pg[mc->mc_top], m3->mc_ki[mc->mc_top]);
      }
    }
  }
  rc = mdbx_rebalance(mc);

  if (likely(rc == MDB_SUCCESS)) {
    /* DB is totally empty now, just bail out.
     * Other cursors adjustments were already done
     * by mdbx_rebalance and aren't needed here.
     */
    if (!mc->mc_snum) {
      mc->mc_flags |= C_DEL | C_EOF;
      return rc;
    }

    mp = mc->mc_pg[mc->mc_top];
    nkeys = NUMKEYS(mp);

    /* Adjust other cursors pointing to mp */
    for (m2 = mc->mc_txn->mt_cursors[dbi]; !rc && m2; m2 = m2->mc_next) {
      m3 = (mc->mc_flags & C_SUB) ? &m2->mc_xcursor->mx_cursor : m2;
      if (!(m2->mc_flags & m3->mc_flags & C_INITIALIZED))
        continue;
      if (m3->mc_snum < mc->mc_snum)
        continue;
      if (m3->mc_pg[mc->mc_top] == mp) {
        /* if m3 points past last node in page, find next sibling */
        if (m3->mc_ki[mc->mc_top] >= mc->mc_ki[mc->mc_top]) {
          if (m3->mc_ki[mc->mc_top] >= nkeys) {
            rc = mdbx_cursor_sibling(m3, 1);
            if (rc == MDB_NOTFOUND) {
              m3->mc_flags |= C_EOF;
              rc = MDB_SUCCESS;
              continue;
            }
          }
          if (mc->mc_db->md_flags & MDB_DUPSORT) {
            MDB_node *node =
                NODEPTR(m3->mc_pg[m3->mc_top], m3->mc_ki[m3->mc_top]);
            /* If this node has dupdata, it may need to be reinited
             * because its data has moved.
             * If the xcursor was not initd it must be reinited.
             * Else if node points to a subDB, nothing is needed.
             */
            if (node->mn_flags & F_DUPDATA) {
              if (m3->mc_xcursor->mx_cursor.mc_flags & C_INITIALIZED) {
                if (!(node->mn_flags & F_SUBDATA))
                  m3->mc_xcursor->mx_cursor.mc_pg[0] = NODEDATA(node);
              } else
                mdbx_xcursor_init1(m3, node);
            }
          }
        }
      }
    }
    mc->mc_flags |= C_DEL;
  }

  if (unlikely(rc))
    mc->mc_txn->mt_flags |= MDB_TXN_ERROR;
  return rc;
}

int mdbx_del(MDB_txn *txn, MDB_dbi dbi, MDB_val *key, MDB_val *data) {
  if (unlikely(!key || !txn))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_signature != MDBX_MT_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(!TXN_DBI_EXIST(txn, dbi, DB_USRVALID)))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_flags & (MDB_TXN_RDONLY | MDB_TXN_BLOCKED)))
    return (txn->mt_flags & MDB_TXN_RDONLY) ? MDBX_EACCESS : MDB_BAD_TXN;

  return mdbx_del0(txn, dbi, key, data, 0);
}

static int mdbx_del0(MDB_txn *txn, MDB_dbi dbi, MDB_val *key, MDB_val *data,
                     unsigned flags) {
  MDB_cursor mc;
  MDB_xcursor mx;
  MDB_cursor_op op;
  MDB_val rdata;
  int rc, exact = 0;
  DKBUF;

  mdbx_debug("====> delete db %u key [%s]", dbi, DKEY(key));

  mdbx_cursor_init(&mc, txn, dbi, &mx);

  if (data) {
    op = MDB_GET_BOTH;
    rdata = *data;
    data = &rdata;
  } else {
    op = MDB_SET;
    flags |= MDB_NODUPDATA;
  }
  rc = mdbx_cursor_set(&mc, key, data, op, &exact);
  if (likely(rc == 0)) {
    /* let mdbx_page_split know about this cursor if needed:
     * delete will trigger a rebalance; if it needs to move
     * a node from one page to another, it will have to
     * update the parent's separator key(s). If the new sepkey
     * is larger than the current one, the parent page may
     * run out of space, triggering a split. We need this
     * cursor to be consistent until the end of the rebalance.
     */
    mc.mc_next = txn->mt_cursors[dbi];
    txn->mt_cursors[dbi] = &mc;
    rc = mdbx_cursor_del(&mc, flags);
    txn->mt_cursors[dbi] = mc.mc_next;
  }
  return rc;
}

/** Split a page and insert a new node.
 * Set #MDB_TXN_ERROR on failure.
 * @param[in,out] mc Cursor pointing to the page and desired insertion index.
 * The cursor will be updated to point to the actual page and index where
 * the node got inserted after the split.
 * @param[in] newkey The key for the newly inserted node.
 * @param[in] newdata The data for the newly inserted node.
 * @param[in] newpgno The page number, if the new node is a branch node.
 * @param[in] nflags The #NODE_ADD_FLAGS for the new node.
 * @return 0 on success, non-zero on failure.
 */
static int mdbx_page_split(MDB_cursor *mc, MDB_val *newkey, MDB_val *newdata,
                           pgno_t newpgno, unsigned nflags) {
  unsigned flags;
  int rc = MDB_SUCCESS, new_root = 0, did_split = 0;
  indx_t newindx;
  pgno_t pgno = 0;
  int i, j, split_indx, nkeys, pmax;
  MDB_env *env = mc->mc_txn->mt_env;
  MDB_node *node;
  MDB_val sepkey, rkey, xdata, *rdata = &xdata;
  MDB_page *copy = NULL;
  MDB_page *mp, *rp, *pp;
  int ptop;
  MDB_cursor mn;
  DKBUF;

  mp = mc->mc_pg[mc->mc_top];
  newindx = mc->mc_ki[mc->mc_top];
  nkeys = NUMKEYS(mp);

  mdbx_debug("-----> splitting %s page %" PRIuPTR
             " and adding [%s] at index %i/%i",
             IS_LEAF(mp) ? "leaf" : "branch", mp->mp_pgno, DKEY(newkey),
             mc->mc_ki[mc->mc_top], nkeys);

  /* Create a right sibling. */
  if ((rc = mdbx_page_new(mc, mp->mp_flags, 1, &rp)))
    return rc;
  rp->mp_leaf2_ksize = mp->mp_leaf2_ksize;
  mdbx_debug("new right sibling: page %" PRIuPTR "", rp->mp_pgno);

  /* Usually when splitting the root page, the cursor
   * height is 1. But when called from mdbx_update_key,
   * the cursor height may be greater because it walks
   * up the stack while finding the branch slot to update.
   */
  if (mc->mc_top < 1) {
    if ((rc = mdbx_page_new(mc, P_BRANCH, 1, &pp)))
      goto done;
    /* shift current top to make room for new parent */
    for (i = mc->mc_snum; i > 0; i--) {
      mc->mc_pg[i] = mc->mc_pg[i - 1];
      mc->mc_ki[i] = mc->mc_ki[i - 1];
    }
    mc->mc_pg[0] = pp;
    mc->mc_ki[0] = 0;
    mc->mc_db->md_root = pp->mp_pgno;
    mdbx_debug("root split! new root = %" PRIuPTR "", pp->mp_pgno);
    new_root = mc->mc_db->md_depth++;

    /* Add left (implicit) pointer. */
    if (unlikely((rc = mdbx_node_add(mc, 0, NULL, NULL, mp->mp_pgno, 0)) !=
                 MDB_SUCCESS)) {
      /* undo the pre-push */
      mc->mc_pg[0] = mc->mc_pg[1];
      mc->mc_ki[0] = mc->mc_ki[1];
      mc->mc_db->md_root = mp->mp_pgno;
      mc->mc_db->md_depth--;
      goto done;
    }
    mc->mc_snum++;
    mc->mc_top++;
    ptop = 0;
  } else {
    ptop = mc->mc_top - 1;
    mdbx_debug("parent branch page is %" PRIuPTR "", mc->mc_pg[ptop]->mp_pgno);
  }

  mdbx_cursor_copy(mc, &mn);
  mn.mc_xcursor = NULL;
  mn.mc_pg[mn.mc_top] = rp;
  mn.mc_ki[ptop] = mc->mc_ki[ptop] + 1;

  if (nflags & MDB_APPEND) {
    mn.mc_ki[mn.mc_top] = 0;
    sepkey = *newkey;
    split_indx = newindx;
    nkeys = 0;
  } else {
    split_indx = (nkeys + 1) / 2;

    if (IS_LEAF2(rp)) {
      char *split, *ins;
      int x;
      unsigned lsize, rsize, ksize;
      /* Move half of the keys to the right sibling */
      x = mc->mc_ki[mc->mc_top] - split_indx;
      ksize = mc->mc_db->md_xsize;
      split = LEAF2KEY(mp, split_indx, ksize);
      rsize = (nkeys - split_indx) * ksize;
      lsize = (nkeys - split_indx) * sizeof(indx_t);
      mp->mp_lower -= lsize;
      rp->mp_lower += lsize;
      mp->mp_upper += rsize - lsize;
      rp->mp_upper -= rsize - lsize;
      sepkey.mv_size = ksize;
      if (newindx == split_indx) {
        sepkey.mv_data = newkey->mv_data;
      } else {
        sepkey.mv_data = split;
      }
      if (x < 0) {
        ins = LEAF2KEY(mp, mc->mc_ki[mc->mc_top], ksize);
        memcpy(rp->mp_ptrs, split, rsize);
        sepkey.mv_data = rp->mp_ptrs;
        memmove(ins + ksize, ins, (split_indx - mc->mc_ki[mc->mc_top]) * ksize);
        memcpy(ins, newkey->mv_data, ksize);
        mp->mp_lower += sizeof(indx_t);
        mp->mp_upper -= ksize - sizeof(indx_t);
      } else {
        if (x)
          memcpy(rp->mp_ptrs, split, x * ksize);
        ins = LEAF2KEY(rp, x, ksize);
        memcpy(ins, newkey->mv_data, ksize);
        memcpy(ins + ksize, split + x * ksize, rsize - x * ksize);
        rp->mp_lower += sizeof(indx_t);
        rp->mp_upper -= ksize - sizeof(indx_t);
        mc->mc_ki[mc->mc_top] = x;
      }
    } else {
      int psize, nsize, k;
      /* Maximum free space in an empty page */
      pmax = env->me_psize - PAGEHDRSZ;
      if (IS_LEAF(mp))
        nsize = mdbx_leaf_size(env, newkey, newdata);
      else
        nsize = mdbx_branch_size(env, newkey);
      nsize = EVEN(nsize);

      /* grab a page to hold a temporary copy */
      copy = mdbx_page_malloc(mc->mc_txn, 1);
      if (unlikely(copy == NULL)) {
        rc = MDBX_ENOMEM;
        goto done;
      }
      copy->mp_pgno = mp->mp_pgno;
      copy->mp_flags = mp->mp_flags;
      copy->mp_lower = (PAGEHDRSZ - PAGEBASE);
      copy->mp_upper = env->me_psize - PAGEBASE;

      /* prepare to insert */
      for (i = 0, j = 0; i < nkeys; i++) {
        if (i == newindx) {
          copy->mp_ptrs[j++] = 0;
        }
        copy->mp_ptrs[j++] = mp->mp_ptrs[i];
      }

      /* When items are relatively large the split point needs
       * to be checked, because being off-by-one will make the
       * difference between success or failure in mdbx_node_add.
       *
       * It's also relevant if a page happens to be laid out
       * such that one half of its nodes are all "small" and
       * the other half of its nodes are "large." If the new
       * item is also "large" and falls on the half with
       * "large" nodes, it also may not fit.
       *
       * As a final tweak, if the new item goes on the last
       * spot on the page (and thus, onto the new page), bias
       * the split so the new page is emptier than the old page.
       * This yields better packing during sequential inserts.
       */
      if (nkeys < 20 || nsize > pmax / 16 || newindx >= nkeys) {
        /* Find split point */
        psize = 0;
        if (newindx <= split_indx || newindx >= nkeys) {
          i = 0;
          j = 1;
          k = newindx >= nkeys ? nkeys : split_indx + 1 + IS_LEAF(mp);
        } else {
          i = nkeys;
          j = -1;
          k = split_indx - 1;
        }
        for (; i != k; i += j) {
          if (i == newindx) {
            psize += nsize;
            node = NULL;
          } else {
            node = (MDB_node *)((char *)mp + copy->mp_ptrs[i] + PAGEBASE);
            psize += NODESIZE + NODEKSZ(node) + sizeof(indx_t);
            if (IS_LEAF(mp)) {
              if (F_ISSET(node->mn_flags, F_BIGDATA))
                psize += sizeof(pgno_t);
              else
                psize += NODEDSZ(node);
            }
            psize = EVEN(psize);
          }
          if (psize > pmax || i == k - j) {
            split_indx = i + (j < 0);
            break;
          }
        }
      }
      if (split_indx == newindx) {
        sepkey.mv_size = newkey->mv_size;
        sepkey.mv_data = newkey->mv_data;
      } else {
        node = (MDB_node *)((char *)mp + copy->mp_ptrs[split_indx] + PAGEBASE);
        sepkey.mv_size = node->mn_ksize;
        sepkey.mv_data = NODEKEY(node);
      }
    }
  }

  mdbx_debug("separator is %d [%s]", split_indx, DKEY(&sepkey));

  /* Copy separator key to the parent. */
  if (SIZELEFT(mn.mc_pg[ptop]) < mdbx_branch_size(env, &sepkey)) {
    int snum = mc->mc_snum;
    mn.mc_snum--;
    mn.mc_top--;
    did_split = 1;
    /* We want other splits to find mn when doing fixups */
    WITH_CURSOR_TRACKING(
        mn, rc = mdbx_page_split(&mn, &sepkey, NULL, rp->mp_pgno, 0));
    if (unlikely(rc != MDB_SUCCESS))
      goto done;

    /* root split? */
    if (mc->mc_snum > snum) {
      ptop++;
    }
    /* Right page might now have changed parent.
     * Check if left page also changed parent.
     */
    if (mn.mc_pg[ptop] != mc->mc_pg[ptop] &&
        mc->mc_ki[ptop] >= NUMKEYS(mc->mc_pg[ptop])) {
      for (i = 0; i < ptop; i++) {
        mc->mc_pg[i] = mn.mc_pg[i];
        mc->mc_ki[i] = mn.mc_ki[i];
      }
      mc->mc_pg[ptop] = mn.mc_pg[ptop];
      if (mn.mc_ki[ptop]) {
        mc->mc_ki[ptop] = mn.mc_ki[ptop] - 1;
      } else {
        /* find right page's left sibling */
        mc->mc_ki[ptop] = mn.mc_ki[ptop];
        rc = mdbx_cursor_sibling(mc, 0);
      }
    }
  } else {
    mn.mc_top--;
    rc = mdbx_node_add(&mn, mn.mc_ki[ptop], &sepkey, NULL, rp->mp_pgno, 0);
    mn.mc_top++;
  }
  if (unlikely(rc != MDB_SUCCESS)) {
    if (rc == MDB_NOTFOUND) /* improper mdbx_cursor_sibling() result */
      rc = MDB_PROBLEM;
    goto done;
  }
  if (nflags & MDB_APPEND) {
    mc->mc_pg[mc->mc_top] = rp;
    mc->mc_ki[mc->mc_top] = 0;
    rc = mdbx_node_add(mc, 0, newkey, newdata, newpgno, nflags);
    if (rc)
      goto done;
    for (i = 0; i < mc->mc_top; i++)
      mc->mc_ki[i] = mn.mc_ki[i];
  } else if (!IS_LEAF2(mp)) {
    /* Move nodes */
    mc->mc_pg[mc->mc_top] = rp;
    i = split_indx;
    j = 0;
    do {
      if (i == newindx) {
        rkey.mv_data = newkey->mv_data;
        rkey.mv_size = newkey->mv_size;
        if (IS_LEAF(mp)) {
          rdata = newdata;
        } else
          pgno = newpgno;
        flags = nflags;
        /* Update index for the new key. */
        mc->mc_ki[mc->mc_top] = j;
      } else {
        node = (MDB_node *)((char *)mp + copy->mp_ptrs[i] + PAGEBASE);
        rkey.mv_data = NODEKEY(node);
        rkey.mv_size = node->mn_ksize;
        if (IS_LEAF(mp)) {
          xdata.mv_data = NODEDATA(node);
          xdata.mv_size = NODEDSZ(node);
          rdata = &xdata;
        } else
          pgno = NODEPGNO(node);
        flags = node->mn_flags;
      }

      if (!IS_LEAF(mp) && j == 0) {
        /* First branch index doesn't need key data. */
        rkey.mv_size = 0;
      }

      rc = mdbx_node_add(mc, j, &rkey, rdata, pgno, flags);
      if (rc)
        goto done;
      if (i == nkeys) {
        i = 0;
        j = 0;
        mc->mc_pg[mc->mc_top] = copy;
      } else {
        i++;
        j++;
      }
    } while (i != split_indx);

    nkeys = NUMKEYS(copy);
    for (i = 0; i < nkeys; i++)
      mp->mp_ptrs[i] = copy->mp_ptrs[i];
    mp->mp_lower = copy->mp_lower;
    mp->mp_upper = copy->mp_upper;
    memcpy(NODEPTR(mp, nkeys - 1), NODEPTR(copy, nkeys - 1),
           env->me_psize - copy->mp_upper - PAGEBASE);

    /* reset back to original page */
    if (newindx < split_indx) {
      mc->mc_pg[mc->mc_top] = mp;
    } else {
      mc->mc_pg[mc->mc_top] = rp;
      mc->mc_ki[ptop]++;
      /* Make sure mc_ki is still valid.
       */
      if (mn.mc_pg[ptop] != mc->mc_pg[ptop] &&
          mc->mc_ki[ptop] >= NUMKEYS(mc->mc_pg[ptop])) {
        for (i = 0; i <= ptop; i++) {
          mc->mc_pg[i] = mn.mc_pg[i];
          mc->mc_ki[i] = mn.mc_ki[i];
        }
      }
    }
    if (nflags & MDB_RESERVE) {
      node = NODEPTR(mc->mc_pg[mc->mc_top], mc->mc_ki[mc->mc_top]);
      if (!(node->mn_flags & F_BIGDATA))
        newdata->mv_data = NODEDATA(node);
    }
  } else {
    if (newindx >= split_indx) {
      mc->mc_pg[mc->mc_top] = rp;
      mc->mc_ki[ptop]++;
      /* Make sure mc_ki is still valid.
       */
      if (mn.mc_pg[ptop] != mc->mc_pg[ptop] &&
          mc->mc_ki[ptop] >= NUMKEYS(mc->mc_pg[ptop])) {
        for (i = 0; i <= ptop; i++) {
          mc->mc_pg[i] = mn.mc_pg[i];
          mc->mc_ki[i] = mn.mc_ki[i];
        }
      }
    }
  }

  {
    /* Adjust other cursors pointing to mp */
    MDB_cursor *m2, *m3;
    MDB_dbi dbi = mc->mc_dbi;
    nkeys = NUMKEYS(mp);

    for (m2 = mc->mc_txn->mt_cursors[dbi]; m2; m2 = m2->mc_next) {
      if (mc->mc_flags & C_SUB)
        m3 = &m2->mc_xcursor->mx_cursor;
      else
        m3 = m2;
      if (m3 == mc)
        continue;
      if (!(m2->mc_flags & m3->mc_flags & C_INITIALIZED))
        continue;
      if (new_root) {
        int k;
        /* sub cursors may be on different DB */
        if (m3->mc_pg[0] != mp)
          continue;
        /* root split */
        for (k = new_root; k >= 0; k--) {
          m3->mc_ki[k + 1] = m3->mc_ki[k];
          m3->mc_pg[k + 1] = m3->mc_pg[k];
        }
        if (m3->mc_ki[0] >= nkeys) {
          m3->mc_ki[0] = 1;
        } else {
          m3->mc_ki[0] = 0;
        }
        m3->mc_pg[0] = mc->mc_pg[0];
        m3->mc_snum++;
        m3->mc_top++;
      }
      if (m3->mc_top >= mc->mc_top && m3->mc_pg[mc->mc_top] == mp) {
        if (m3->mc_ki[mc->mc_top] >= newindx && !(nflags & MDB_SPLIT_REPLACE))
          m3->mc_ki[mc->mc_top]++;
        if (m3->mc_ki[mc->mc_top] >= nkeys) {
          m3->mc_pg[mc->mc_top] = rp;
          m3->mc_ki[mc->mc_top] -= nkeys;
          for (i = 0; i < mc->mc_top; i++) {
            m3->mc_ki[i] = mn.mc_ki[i];
            m3->mc_pg[i] = mn.mc_pg[i];
          }
        }
      } else if (!did_split && m3->mc_top >= ptop &&
                 m3->mc_pg[ptop] == mc->mc_pg[ptop] &&
                 m3->mc_ki[ptop] >= mc->mc_ki[ptop]) {
        m3->mc_ki[ptop]++;
      }
      if (XCURSOR_INITED(m3) && IS_LEAF(mp))
        XCURSOR_REFRESH(m3, m3->mc_pg[mc->mc_top], m3->mc_ki[mc->mc_top]);
    }
  }
  mdbx_debug("mp left: %d, rp left: %d", SIZELEFT(mp), SIZELEFT(rp));

done:
  if (copy) /* tmp page */
    mdbx_page_free(env, copy);
  if (unlikely(rc))
    mc->mc_txn->mt_flags |= MDB_TXN_ERROR;
  return rc;
}

int mdbx_put(MDB_txn *txn, MDB_dbi dbi, MDB_val *key, MDB_val *data,
             unsigned flags) {
  MDB_cursor mc;
  MDB_xcursor mx;

  if (unlikely(!key || !data || !txn))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_signature != MDBX_MT_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(!TXN_DBI_EXIST(txn, dbi, DB_USRVALID)))
    return MDBX_EINVAL;

  if (unlikely(flags &
               ~(MDB_NOOVERWRITE | MDB_NODUPDATA | MDB_RESERVE | MDB_APPEND |
                 MDB_APPENDDUP | MDB_CURRENT)))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_flags & (MDB_TXN_RDONLY | MDB_TXN_BLOCKED)))
    return (txn->mt_flags & MDB_TXN_RDONLY) ? MDBX_EACCESS : MDB_BAD_TXN;

  mdbx_cursor_init(&mc, txn, dbi, &mx);
  mc.mc_next = txn->mt_cursors[dbi];
  txn->mt_cursors[dbi] = &mc;

  int rc = MDB_SUCCESS;
  /* LY: support for update (explicit overwrite) */
  if (flags & MDB_CURRENT) {
    rc = mdbx_cursor_get(&mc, key, NULL, MDB_SET);
    if (likely(rc == MDB_SUCCESS) &&
        (txn->mt_dbs[dbi].md_flags & MDB_DUPSORT)) {
      /* LY: allows update (explicit overwrite) only for unique keys */
      MDB_node *leaf = NODEPTR(mc.mc_pg[mc.mc_top], mc.mc_ki[mc.mc_top]);
      if (F_ISSET(leaf->mn_flags, F_DUPDATA)) {
        mdbx_tassert(txn, XCURSOR_INITED(&mc) &&
                              mc.mc_xcursor->mx_db.md_entries > 1);
        rc = MDBX_EMULTIVAL;
      }
    }
  }

  if (likely(rc == MDB_SUCCESS))
    rc = mdbx_cursor_put(&mc, key, data, flags);
  txn->mt_cursors[dbi] = mc.mc_next;

  return rc;
}

#ifndef MDB_WBUF
#define MDB_WBUF (1024 * 1024)
#endif
#define MDB_EOF 0x10 /**< #mdbx_env_copyfd1() is done reading */

/** State needed for a double-buffering compacting copy. */
typedef struct mdbx_copy {
  MDB_env *mc_env;
  MDB_txn *mc_txn;
  mdbx_mutex_t mc_mutex;
  mdbx_cond_t mc_cond; /**< Condition variable for #mc_new */
  char *mc_wbuf[2];
  char *mc_over[2];
  int mc_wlen[2];
  int mc_olen[2];
  pgno_t mc_next_pgno;
  mdbx_filehandle_t mc_fd;
  int mc_toggle; /**< Buffer number in provider */
  int mc_new;    /**< (0-2 buffers to write) | (#MDB_EOF at end) */
  /** Error code.  Never cleared if set.  Both threads can set nonzero
   *	to fail the copy.  Not mutex-protected, LMDB expects atomic int.
   */
  volatile int mc_error;
} mdbx_copy;

/** Dedicated writer thread for compacting copy. */
static THREAD_RESULT __cold THREAD_CALL mdbx_env_copythr(void *arg) {
  mdbx_copy *my = arg;
  char *ptr;
  int toggle = 0, wsize;

  mdbx_mutex_lock(&my->mc_mutex);
  while (!my->mc_error) {
    while (!my->mc_new)
      mdbx_cond_wait(&my->mc_cond, &my->mc_mutex);
    if (my->mc_new == 0 + MDB_EOF) /* 0 buffers, just EOF */
      break;
    wsize = my->mc_wlen[toggle];
    ptr = my->mc_wbuf[toggle];
  again:
    if (wsize > 0 && !my->mc_error) {
      int rc = mdbx_write(my->mc_fd, ptr, wsize);
      if (rc != MDB_SUCCESS)
        my->mc_error = rc;
    }

    /* If there's an overflow page tail, write it too */
    if (my->mc_olen[toggle]) {
      wsize = my->mc_olen[toggle];
      ptr = my->mc_over[toggle];
      my->mc_olen[toggle] = 0;
      goto again;
    }
    my->mc_wlen[toggle] = 0;
    toggle ^= 1;
    /* Return the empty buffer to provider */
    my->mc_new--;
    mdbx_cond_signal(&my->mc_cond);
  }
  mdbx_mutex_unlock(&my->mc_mutex);
  return (THREAD_RESULT)0;
}

/** Give buffer and/or #MDB_EOF to writer thread, await unused buffer.
 *
 * @param[in] my control structure.
 * @param[in] adjust (1 to hand off 1 buffer) | (MDB_EOF when ending).
 */
static int __cold mdbx_env_cthr_toggle(mdbx_copy *my, int adjust) {
  mdbx_mutex_lock(&my->mc_mutex);
  my->mc_new += adjust;
  mdbx_cond_signal(&my->mc_cond);
  while (my->mc_new & 2) /* both buffers in use */
    mdbx_cond_wait(&my->mc_cond, &my->mc_mutex);
  mdbx_mutex_unlock(&my->mc_mutex);

  my->mc_toggle ^= (adjust & 1);
  /* Both threads reset mc_wlen, to be safe from threading errors */
  my->mc_wlen[my->mc_toggle] = 0;
  return my->mc_error;
}

/** Depth-first tree traversal for compacting copy.
 * @param[in] my control structure.
 * @param[in,out] pg database root.
 * @param[in] flags includes #F_DUPDATA if it is a sorted-duplicate sub-DB.
 */
static int __cold mdbx_env_cwalk(mdbx_copy *my, pgno_t *pg, int flags) {
  MDB_cursor mc;
  MDB_node *ni;
  MDB_page *mo, *mp, *leaf;
  char *buf, *ptr;
  int rc, toggle;
  unsigned i;

  /* Empty DB, nothing to do */
  if (*pg == P_INVALID)
    return MDB_SUCCESS;

  memset(&mc, 0, sizeof(mc));
  mc.mc_snum = 1;
  mc.mc_txn = my->mc_txn;

  rc = mdbx_page_get(&mc, *pg, &mc.mc_pg[0], NULL);
  if (rc)
    return rc;
  rc = mdbx_page_search_root(&mc, NULL, MDB_PS_FIRST);
  if (rc)
    return rc;

  /* Make cursor pages writable */
  buf = ptr = malloc(my->mc_env->me_psize * mc.mc_snum);
  if (buf == NULL)
    return MDBX_ENOMEM;

  for (i = 0; i < mc.mc_top; i++) {
    mdbx_page_copy((MDB_page *)ptr, mc.mc_pg[i], my->mc_env->me_psize);
    mc.mc_pg[i] = (MDB_page *)ptr;
    ptr += my->mc_env->me_psize;
  }

  /* This is writable space for a leaf page. Usually not needed. */
  leaf = (MDB_page *)ptr;

  toggle = my->mc_toggle;
  while (mc.mc_snum > 0) {
    unsigned n;
    mp = mc.mc_pg[mc.mc_top];
    n = NUMKEYS(mp);

    if (IS_LEAF(mp)) {
      if (!IS_LEAF2(mp) && !(flags & F_DUPDATA)) {
        for (i = 0; i < n; i++) {
          ni = NODEPTR(mp, i);
          if (ni->mn_flags & F_BIGDATA) {
            MDB_page *omp;

            /* Need writable leaf */
            if (mp != leaf) {
              mc.mc_pg[mc.mc_top] = leaf;
              mdbx_page_copy(leaf, mp, my->mc_env->me_psize);
              mp = leaf;
              ni = NODEPTR(mp, i);
            }

            pgno_t pgno;
            memcpy(&pgno, NODEDATA(ni), sizeof(pgno));
            memcpy(NODEDATA(ni), &my->mc_next_pgno, sizeof(pgno_t));
            rc = mdbx_page_get(&mc, pgno, &omp, NULL);
            if (rc)
              goto done;
            if (my->mc_wlen[toggle] >= MDB_WBUF) {
              rc = mdbx_env_cthr_toggle(my, 1);
              if (rc)
                goto done;
              toggle = my->mc_toggle;
            }
            mo = (MDB_page *)(my->mc_wbuf[toggle] + my->mc_wlen[toggle]);
            memcpy(mo, omp, my->mc_env->me_psize);
            mo->mp_pgno = my->mc_next_pgno;
            my->mc_next_pgno += omp->mp_pages;
            my->mc_wlen[toggle] += my->mc_env->me_psize;
            if (omp->mp_pages > 1) {
              my->mc_olen[toggle] = my->mc_env->me_psize * (omp->mp_pages - 1);
              my->mc_over[toggle] = (char *)omp + my->mc_env->me_psize;
              rc = mdbx_env_cthr_toggle(my, 1);
              if (rc)
                goto done;
              toggle = my->mc_toggle;
            }
          } else if (ni->mn_flags & F_SUBDATA) {
            MDB_db db;

            /* Need writable leaf */
            if (mp != leaf) {
              mc.mc_pg[mc.mc_top] = leaf;
              mdbx_page_copy(leaf, mp, my->mc_env->me_psize);
              mp = leaf;
              ni = NODEPTR(mp, i);
            }

            memcpy(&db, NODEDATA(ni), sizeof(db));
            my->mc_toggle = toggle;
            rc = mdbx_env_cwalk(my, &db.md_root, ni->mn_flags & F_DUPDATA);
            if (rc)
              goto done;
            toggle = my->mc_toggle;
            memcpy(NODEDATA(ni), &db, sizeof(db));
          }
        }
      }
    } else {
      mc.mc_ki[mc.mc_top]++;
      if (mc.mc_ki[mc.mc_top] < n) {
        pgno_t pgno;
      again:
        ni = NODEPTR(mp, mc.mc_ki[mc.mc_top]);
        pgno = NODEPGNO(ni);
        rc = mdbx_page_get(&mc, pgno, &mp, NULL);
        if (rc)
          goto done;
        mc.mc_top++;
        mc.mc_snum++;
        mc.mc_ki[mc.mc_top] = 0;
        if (IS_BRANCH(mp)) {
          /* Whenever we advance to a sibling branch page,
           * we must proceed all the way down to its first leaf.
           */
          mdbx_page_copy(mc.mc_pg[mc.mc_top], mp, my->mc_env->me_psize);
          goto again;
        } else
          mc.mc_pg[mc.mc_top] = mp;
        continue;
      }
    }
    if (my->mc_wlen[toggle] >= MDB_WBUF) {
      rc = mdbx_env_cthr_toggle(my, 1);
      if (rc)
        goto done;
      toggle = my->mc_toggle;
    }
    mo = (MDB_page *)(my->mc_wbuf[toggle] + my->mc_wlen[toggle]);
    mdbx_page_copy(mo, mp, my->mc_env->me_psize);
    mo->mp_pgno = my->mc_next_pgno++;
    my->mc_wlen[toggle] += my->mc_env->me_psize;
    if (mc.mc_top) {
      /* Update parent if there is one */
      ni = NODEPTR(mc.mc_pg[mc.mc_top - 1], mc.mc_ki[mc.mc_top - 1]);
      SETPGNO(ni, mo->mp_pgno);
      mdbx_cursor_pop(&mc);
    } else {
      /* Otherwise we're done */
      *pg = mo->mp_pgno;
      break;
    }
  }
done:
  free(buf);
  return rc;
}

/** Copy environment with compaction. */
static int __cold mdbx_env_copyfd1(MDB_env *env, mdbx_filehandle_t fd) {
  MDB_meta *mm;
  MDB_page *mp;
  mdbx_copy my;
  MDB_txn *txn = NULL;
  mdbx_thread_t thr;
  pgno_t root, new_root;
  int rc;

  memset(&my, 0, sizeof(my));
  if ((rc = mdbx_mutex_init(&my.mc_mutex)) != 0)
    return rc;
  if ((rc = mdbx_cond_init(&my.mc_cond)) != 0)
    goto done2;
  rc = mdbx_memalign_alloc(env->me_os_psize, MDB_WBUF * 2,
                           (void **)&my.mc_wbuf[0]);
  if (rc != MDB_SUCCESS)
    goto done;

  memset(my.mc_wbuf[0], 0, MDB_WBUF * 2);
  my.mc_wbuf[1] = my.mc_wbuf[0] + MDB_WBUF;
  my.mc_next_pgno = NUM_METAS;
  my.mc_env = env;
  my.mc_fd = fd;
  rc = mdbx_thread_create(&thr, mdbx_env_copythr, &my);
  if (rc)
    goto done;

  rc = mdbx_txn_begin(env, NULL, MDB_RDONLY, &txn);
  if (rc)
    goto finish;

  mp = (MDB_page *)my.mc_wbuf[0];
  memset(mp, 0, NUM_METAS * env->me_psize);
  mp->mp_pgno = 0;
  mp->mp_flags = P_META;
  mm = (MDB_meta *)PAGEDATA(mp);
  mdbx_env_init_meta0(env, mm);

  mp = (MDB_page *)(my.mc_wbuf[0] + env->me_psize);
  mp->mp_pgno = 1;
  mp->mp_flags = P_META;
  *(MDB_meta *)PAGEDATA(mp) = *mm;
  mm = (MDB_meta *)PAGEDATA(mp);

  /* Set metapage 1 with current main DB */
  root = new_root = txn->mt_dbs[MAIN_DBI].md_root;
  if (root != P_INVALID) {
    /* Count free pages + freeDB pages.  Subtract from last_pg
     * to find the new last_pg, which also becomes the new root.
     */
    MDB_ID freecount = 0;
    MDB_cursor mc;
    MDB_val key, data;
    mdbx_cursor_init(&mc, txn, FREE_DBI, NULL);
    while ((rc = mdbx_cursor_get(&mc, &key, &data, MDB_NEXT)) == 0)
      freecount += *(MDB_ID *)data.mv_data;
    if (rc != MDB_NOTFOUND)
      goto finish;
    freecount += txn->mt_dbs[FREE_DBI].md_branch_pages +
                 txn->mt_dbs[FREE_DBI].md_leaf_pages +
                 txn->mt_dbs[FREE_DBI].md_overflow_pages;

    new_root = txn->mt_next_pgno - 1 - freecount;
    mm->mm_last_pg = new_root;
    mm->mm_dbs[MAIN_DBI] = txn->mt_dbs[MAIN_DBI];
    mm->mm_dbs[MAIN_DBI].md_root = new_root;
  } else {
    /* When the DB is empty, handle it specially to
     * fix any breakage like page leaks from ITS#8174.
     */
    mm->mm_dbs[MAIN_DBI].md_flags = txn->mt_dbs[MAIN_DBI].md_flags;
  }
  if (root != P_INVALID || mm->mm_dbs[MAIN_DBI].md_flags) {
    mm->mm_txnid = 1; /* use metapage 1 */
  }

  my.mc_wlen[0] = env->me_psize * NUM_METAS;
  my.mc_txn = txn;
  rc = mdbx_env_cwalk(&my, &root, 0);
  if (rc == MDB_SUCCESS && root != new_root) {
    rc = MDB_INCOMPATIBLE; /* page leak or corrupt DB */
  }

finish:
  if (rc)
    my.mc_error = rc;
  mdbx_env_cthr_toggle(&my, 1 | MDB_EOF);
  rc = mdbx_thread_join(thr);
  mdbx_txn_abort(txn);

done:
  mdbx_memalign_free(my.mc_wbuf[0]);
  mdbx_cond_destroy(&my.mc_cond);
done2:
  mdbx_mutex_destroy(&my.mc_mutex);
  return rc ? rc : my.mc_error;
}

/** Copy environment as-is. */
static int __cold mdbx_env_copyfd0(MDB_env *env, mdbx_filehandle_t fd) {
  MDB_txn *txn = NULL;
  int rc;

  /* Do the lock/unlock of the reader mutex before starting the
   * write txn.  Otherwise other read txns could block writers. */
  rc = mdbx_txn_begin(env, NULL, MDB_RDONLY, &txn);
  if (unlikely(rc))
    return rc;

  /* We must start the actual read txn after blocking writers */
  rc = mdbx_txn_end(txn, MDB_END_RESET_TMP);
  if (unlikely(rc))
    goto bailout; /* FIXME: or just return? */

  /* Temporarily block writers until we snapshot the meta pages */
  rc = mdbx_txn_lock(env);
  if (unlikely(rc))
    goto bailout;

  rc = mdbx_txn_renew0(txn, MDB_RDONLY);
  if (rc) {
    mdbx_txn_unlock(env);
    goto bailout;
  }

  rc = mdbx_write(fd, env->me_map, env->me_psize * NUM_METAS);
  mdbx_txn_unlock(env);

  if (rc == MDB_SUCCESS)
    rc = mdbx_ftruncate(fd, txn->mt_next_pgno * env->me_psize);

bailout:
  mdbx_txn_abort(txn);
  return rc;
}

int __cold mdbx_env_copyfd2(MDB_env *env, mdbx_filehandle_t fd,
                            unsigned flags) {
  if (flags & MDB_CP_COMPACT)
    return mdbx_env_copyfd1(env, fd);
  else
    return mdbx_env_copyfd0(env, fd);
}

int __cold mdbx_env_copyfd(MDB_env *env, mdbx_filehandle_t fd) {
  return mdbx_env_copyfd2(env, fd, 0);
}

int __cold mdbx_env_copy2(MDB_env *env, const char *path, unsigned flags) {
  int rc, len;
  char *lck_pathname;
  mdbx_filehandle_t newfd = INVALID_HANDLE_VALUE;

  if (env->me_flags & MDB_NOSUBDIR) {
    lck_pathname = (char *)path;
  } else {
    len = strlen(path);
    len += sizeof(MDBX_DATANAME);
    lck_pathname = malloc(len);
    if (!lck_pathname)
      return MDBX_ENOMEM;
    sprintf(lck_pathname, "%s" MDBX_DATANAME, path);
  }

  /* The destination path must exist, but the destination file must not.
   * We don't want the OS to cache the writes, since the source data is
   * already in the OS cache. */
  rc = mdbx_openfile(lck_pathname, O_WRONLY | O_CREAT | O_EXCL, 0666, &newfd);
  if (rc == MDB_SUCCESS) {
    if (env->me_psize >= env->me_os_psize) {
#ifdef F_NOCACHE /* __APPLE__ */
      (void)fcntl(newfd, F_NOCACHE, 1);
#elif defined(O_DIRECT) && defined(F_GETFL)
      /* Set O_DIRECT if the file system supports it */
      if ((rc = fcntl(newfd, F_GETFL)) != -1)
        (void)fcntl(newfd, F_SETFL, rc | O_DIRECT);
#endif
    }
    rc = mdbx_env_copyfd2(env, newfd, flags);
  }

  if (!(env->me_flags & MDB_NOSUBDIR))
    free(lck_pathname);

  if (newfd != INVALID_HANDLE_VALUE) {
    int err = mdbx_closefile(newfd);
    if (rc == MDB_SUCCESS && err != rc)
      rc = err;
  }

  return rc;
}

int __cold mdbx_env_copy(MDB_env *env, const char *path) {
  return mdbx_env_copy2(env, path, 0);
}

int __cold mdbx_env_set_flags(MDB_env *env, unsigned flags, int onoff) {
  if (unlikely(flags & ~CHANGEABLE))
    return MDBX_EINVAL;

  int rc = mdbx_txn_lock(env);
  if (unlikely(rc))
    return rc;

  if (onoff)
    env->me_flags |= flags;
  else
    env->me_flags &= ~flags;

  mdbx_txn_unlock(env);
  return MDB_SUCCESS;
}

int __cold mdbx_env_get_flags(MDB_env *env, unsigned *arg) {
  if (unlikely(!env || !arg))
    return MDBX_EINVAL;

  *arg = env->me_flags & (CHANGEABLE | CHANGELESS);
  return MDB_SUCCESS;
}

int __cold mdbx_env_set_userctx(MDB_env *env, void *ctx) {
  if (unlikely(!env))
    return MDBX_EINVAL;
  env->me_userctx = ctx;
  return MDB_SUCCESS;
}

void *__cold mdbx_env_get_userctx(MDB_env *env) {
  return env ? env->me_userctx : NULL;
}

int __cold mdbx_env_set_assert(MDB_env *env, MDB_assert_func *func) {
  if (unlikely(!env))
    return MDBX_EINVAL;
#if MDB_DEBUG
  env->me_assert_func = func;
  return MDB_SUCCESS;
#else
  (void)func;
  return MDBX_ENOSYS;
#endif
}

int __cold mdbx_env_get_path(MDB_env *env, const char **arg) {
  if (unlikely(!env || !arg))
    return MDBX_EINVAL;

  *arg = env->me_path;
  return MDB_SUCCESS;
}

int __cold mdbx_env_get_fd(MDB_env *env, mdbx_filehandle_t *arg) {
  if (unlikely(!env || !arg))
    return MDBX_EINVAL;

  *arg = env->me_fd;
  return MDB_SUCCESS;
}

/** Common code for #mdbx_dbi_stat() and #mdbx_env_stat().
 * @param[in] env the environment to operate in.
 * @param[in] db the #MDB_db record containing the stats to return.
 * @param[out] arg the address of an #MDB_stat structure to receive the stats.
 * @return 0, this function always succeeds.
 */
static int __cold mdbx_stat0(MDB_env *env, MDB_db *db, MDBX_stat *arg) {
  arg->ms_psize = env->me_psize;
  arg->ms_depth = db->md_depth;
  arg->ms_branch_pages = db->md_branch_pages;
  arg->ms_leaf_pages = db->md_leaf_pages;
  arg->ms_overflow_pages = db->md_overflow_pages;
  arg->ms_entries = db->md_entries;
  return MDB_SUCCESS;
}

int __cold mdbx_env_stat(MDB_env *env, MDBX_stat *arg, size_t bytes) {
  MDB_meta *meta;

  if (unlikely(env == NULL || arg == NULL))
    return MDBX_EINVAL;
  if (unlikely(bytes != sizeof(MDBX_stat)))
    return MDBX_EINVAL;

  meta = mdbx_meta_head(env);
  return mdbx_stat0(env, &meta->mm_dbs[MAIN_DBI], arg);
}

int __cold mdbx_env_info(MDB_env *env, MDBX_envinfo *arg, size_t bytes) {
  MDB_meta *meta;

  if (unlikely(env == NULL || arg == NULL))
    return MDBX_EINVAL;

  if (bytes != sizeof(MDBX_envinfo))
    return MDBX_EINVAL;

  MDB_meta *m1, *m2;
  MDB_reader *r;
  unsigned i;

  m1 = METAPAGE_1(env);
  m2 = METAPAGE_2(env);

  do {
    meta = mdbx_meta_head(env);
    arg->me_last_txnid = meta->mm_txnid;
    arg->me_last_pgno = meta->mm_last_pg;
    arg->me_meta1_txnid = m1->mm_txnid;
    arg->me_meta1_sign = m1->mm_datasync_sign;
    arg->me_meta2_txnid = m2->mm_txnid;
    arg->me_meta2_sign = m2->mm_datasync_sign;
  } while (unlikely(arg->me_last_txnid != mdbx_meta_head(env)->mm_txnid ||
                    arg->me_meta1_sign != m1->mm_datasync_sign ||
                    arg->me_meta2_sign != m2->mm_datasync_sign));

  arg->me_mapsize = env->me_mapsize;
  arg->me_maxreaders = env->me_maxreaders;
  arg->me_numreaders = env->me_lck->mti_numreaders;
  arg->me_tail_txnid = 0;

  r = env->me_lck->mti_readers;
  arg->me_tail_txnid = arg->me_last_txnid;
  for (i = 0; i < arg->me_numreaders; ++i) {
    if (r[i].mr_pid) {
      txnid_t mr = r[i].mr_txnid;
      if (arg->me_tail_txnid > mr)
        arg->me_tail_txnid = mr;
    }
  }

  return MDB_SUCCESS;
}

static MDB_cmp_func *mdbx_default_keycmp(unsigned flags) {
  return (flags & MDB_REVERSEKEY) ? mdbx_cmp_memnr : (flags & MDB_INTEGERKEY)
                                                         ? mdbx_cmp_int_a2
                                                         : mdbx_cmp_memn;
}

static MDB_cmp_func *mdbx_default_datacmp(unsigned flags) {
  return !(flags & MDB_DUPSORT)
             ? 0
             : ((flags & MDB_INTEGERDUP)
                    ? mdbx_cmp_int_ua
                    : ((flags & MDB_REVERSEDUP) ? mdbx_cmp_memnr
                                                : mdbx_cmp_memn));
}

/** Set the default comparison functions for a database.
 * Called immediately after a database is opened to set the defaults.
 * The user can then override them with #mdbx_set_compare() or
 * #mdbx_set_dupsort().
 * @param[in] txn A transaction handle returned by #mdbx_txn_begin()
 * @param[in] dbi A database handle returned by #mdbx_dbi_open()
 */
static void mdbx_default_cmp(MDB_txn *txn, MDB_dbi dbi) {
  unsigned flags = txn->mt_dbs[dbi].md_flags;
  txn->mt_dbxs[dbi].md_cmp = mdbx_default_keycmp(flags);
  txn->mt_dbxs[dbi].md_dcmp = mdbx_default_datacmp(flags);
}

int mdbx_dbi_open(MDB_txn *txn, const char *name, unsigned flags,
                  MDB_dbi *dbi) {
  MDB_val key, data;
  MDB_dbi i;
  MDB_cursor mc;
  int rc, dbflag, exact;
  unsigned unused = 0, seq;
  char *namedup;
  size_t len;

  if (unlikely(!txn || !dbi))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_signature != MDBX_MT_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(flags & ~VALID_FLAGS))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_flags & MDB_TXN_BLOCKED))
    return MDB_BAD_TXN;

  /* main DB? */
  if (!name) {
    *dbi = MAIN_DBI;
    if (flags & PERSISTENT_FLAGS) {
      uint16_t f2 = flags & PERSISTENT_FLAGS;
      /* make sure flag changes get committed */
      if ((txn->mt_dbs[MAIN_DBI].md_flags | f2) !=
          txn->mt_dbs[MAIN_DBI].md_flags) {
        txn->mt_dbs[MAIN_DBI].md_flags |= f2;
        txn->mt_flags |= MDB_TXN_DIRTY;
      }
    }
    mdbx_default_cmp(txn, MAIN_DBI);
    return MDB_SUCCESS;
  }

  if (txn->mt_dbxs[MAIN_DBI].md_cmp == NULL) {
    mdbx_default_cmp(txn, MAIN_DBI);
  }

  /* Is the DB already open? */
  len = strlen(name);
  for (i = CORE_DBS; i < txn->mt_numdbs; i++) {
    if (!txn->mt_dbxs[i].md_name.mv_size) {
      /* Remember this free slot */
      if (!unused)
        unused = i;
      continue;
    }
    if (len == txn->mt_dbxs[i].md_name.mv_size &&
        !strncmp(name, txn->mt_dbxs[i].md_name.mv_data, len)) {
      *dbi = i;
      return MDB_SUCCESS;
    }
  }

  /* If no free slot and max hit, fail */
  if (!unused && unlikely(txn->mt_numdbs >= txn->mt_env->me_maxdbs))
    return MDB_DBS_FULL;

  /* Cannot mix named databases with some mainDB flags */
  if (unlikely(txn->mt_dbs[MAIN_DBI].md_flags & (MDB_DUPSORT | MDB_INTEGERKEY)))
    return (flags & MDB_CREATE) ? MDB_INCOMPATIBLE : MDB_NOTFOUND;

  /* Find the DB info */
  dbflag = DB_NEW | DB_VALID | DB_USRVALID;
  exact = 0;
  key.mv_size = len;
  key.mv_data = (void *)name;
  mdbx_cursor_init(&mc, txn, MAIN_DBI, NULL);
  rc = mdbx_cursor_set(&mc, &key, &data, MDB_SET, &exact);
  if (likely(rc == MDB_SUCCESS)) {
    /* make sure this is actually a DB */
    MDB_node *node = NODEPTR(mc.mc_pg[mc.mc_top], mc.mc_ki[mc.mc_top]);
    if (unlikely((node->mn_flags & (F_DUPDATA | F_SUBDATA)) != F_SUBDATA))
      return MDB_INCOMPATIBLE;
  } else if (!(rc == MDB_NOTFOUND && (flags & MDB_CREATE))) {
    return rc;
  }

  /* Done here so we cannot fail after creating a new DB */
  if (unlikely((namedup = mdbx_strdup(name)) == NULL))
    return MDBX_ENOMEM;

  if (unlikely(rc)) {
    MDB_db db_dummy;
    /* MDB_NOTFOUND and MDB_CREATE: Create new DB */
    memset(&db_dummy, 0, sizeof(db_dummy));
    db_dummy.md_root = P_INVALID;
    db_dummy.md_flags = flags & PERSISTENT_FLAGS;
    data.mv_size = sizeof(db_dummy);
    data.mv_data = &db_dummy;
    WITH_CURSOR_TRACKING(mc, rc = mdbx_cursor_put(&mc, &key, &data, F_SUBDATA));
    dbflag |= DB_DIRTY;
  }

  if (unlikely(rc)) {
    free(namedup);
  } else {
    /* Got info, register DBI in this txn */
    unsigned slot = unused ? unused : txn->mt_numdbs;
    txn->mt_dbxs[slot].md_name.mv_data = namedup;
    txn->mt_dbxs[slot].md_name.mv_size = len;
    txn->mt_dbflags[slot] = dbflag;
    /* txn-> and env-> are the same in read txns, use
     * tmp variable to avoid undefined assignment
     */
    seq = ++txn->mt_env->me_dbiseqs[slot];
    txn->mt_dbiseqs[slot] = seq;

    memcpy(&txn->mt_dbs[slot], data.mv_data, sizeof(MDB_db));
    *dbi = slot;
    mdbx_default_cmp(txn, slot);
    if (!unused) {
      txn->mt_numdbs++;
    }
  }

  return rc;
}

int __cold mdbx_dbi_stat(MDB_txn *txn, MDB_dbi dbi, MDBX_stat *arg,
                         size_t bytes) {
  if (unlikely(!arg || !txn))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_signature != MDBX_MT_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(!TXN_DBI_EXIST(txn, dbi, DB_VALID)))
    return MDBX_EINVAL;

  if (unlikely(bytes != sizeof(MDBX_stat)))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_flags & MDB_TXN_BLOCKED))
    return MDB_BAD_TXN;

  if (unlikely(txn->mt_dbflags[dbi] & DB_STALE)) {
    MDB_cursor mc;
    MDB_xcursor mx;
    /* Stale, must read the DB's root. cursor_init does it for us. */
    mdbx_cursor_init(&mc, txn, dbi, &mx);
  }
  return mdbx_stat0(txn->mt_env, &txn->mt_dbs[dbi], arg);
}

void mdbx_dbi_close(MDB_env *env, MDB_dbi dbi) {
  char *ptr;
  if (dbi < CORE_DBS || dbi >= env->me_maxdbs)
    return;
  ptr = env->me_dbxs[dbi].md_name.mv_data;
  /* If there was no name, this was already closed */
  if (ptr) {
    env->me_dbxs[dbi].md_name.mv_data = NULL;
    env->me_dbxs[dbi].md_name.mv_size = 0;
    env->me_dbflags[dbi] = 0;
    env->me_dbiseqs[dbi]++;
    free(ptr);
  }
}

int mdbx_dbi_flags(MDB_txn *txn, MDB_dbi dbi, unsigned *flags) {
  if (unlikely(!txn || !flags))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_signature != MDBX_MT_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(!TXN_DBI_EXIST(txn, dbi, DB_VALID)))
    return MDBX_EINVAL;

  *flags = txn->mt_dbs[dbi].md_flags & PERSISTENT_FLAGS;
  return MDB_SUCCESS;
}

/** Add all the DB's pages to the free list.
 * @param[in] mc Cursor on the DB to free.
 * @param[in] subs non-Zero to check for sub-DBs in this DB.
 * @return 0 on success, non-zero on failure.
 */
static int mdbx_drop0(MDB_cursor *mc, int subs) {
  int rc;

  rc = mdbx_page_search(mc, NULL, MDB_PS_FIRST);
  if (likely(rc == MDB_SUCCESS)) {
    MDB_txn *txn = mc->mc_txn;
    MDB_node *ni;
    MDB_cursor mx;
    unsigned i;

    /* DUPSORT sub-DBs have no ovpages/DBs. Omit scanning leaves.
     * This also avoids any P_LEAF2 pages, which have no nodes.
     * Also if the DB doesn't have sub-DBs and has no overflow
     * pages, omit scanning leaves.
     */
    if ((mc->mc_flags & C_SUB) || (!subs && !mc->mc_db->md_overflow_pages))
      mdbx_cursor_pop(mc);

    mdbx_cursor_copy(mc, &mx);
    while (mc->mc_snum > 0) {
      MDB_page *mp = mc->mc_pg[mc->mc_top];
      unsigned n = NUMKEYS(mp);
      if (IS_LEAF(mp)) {
        for (i = 0; i < n; i++) {
          ni = NODEPTR(mp, i);
          if (ni->mn_flags & F_BIGDATA) {
            MDB_page *omp;
            pgno_t pg;
            memcpy(&pg, NODEDATA(ni), sizeof(pg));
            rc = mdbx_page_get(mc, pg, &omp, NULL);
            if (unlikely(rc))
              goto done;
            mdbx_cassert(mc, IS_OVERFLOW(omp));
            rc = mdbx_midl_append_range(&txn->mt_free_pgs, pg, omp->mp_pages);
            if (unlikely(rc))
              goto done;
            mc->mc_db->md_overflow_pages -= omp->mp_pages;
            if (!mc->mc_db->md_overflow_pages && !subs)
              break;
          } else if (subs && (ni->mn_flags & F_SUBDATA)) {
            mdbx_xcursor_init1(mc, ni);
            rc = mdbx_drop0(&mc->mc_xcursor->mx_cursor, 0);
            if (unlikely(rc))
              goto done;
          }
        }
        if (!subs && !mc->mc_db->md_overflow_pages)
          goto pop;
      } else {
        if (unlikely((rc = mdbx_midl_need(&txn->mt_free_pgs, n)) != 0))
          goto done;
        for (i = 0; i < n; i++) {
          pgno_t pg;
          ni = NODEPTR(mp, i);
          pg = NODEPGNO(ni);
          /* free it */
          mdbx_midl_xappend(txn->mt_free_pgs, pg);
        }
      }
      if (!mc->mc_top)
        break;
      mc->mc_ki[mc->mc_top] = i;
      rc = mdbx_cursor_sibling(mc, 1);
      if (rc) {
        if (unlikely(rc != MDB_NOTFOUND))
          goto done;
      /* no more siblings, go back to beginning
       * of previous level.
       */
      pop:
        mdbx_cursor_pop(mc);
        mc->mc_ki[0] = 0;
        for (i = 1; i < mc->mc_snum; i++) {
          mc->mc_ki[i] = 0;
          mc->mc_pg[i] = mx.mc_pg[i];
        }
      }
    }
    /* free it */
    rc = mdbx_midl_append(&txn->mt_free_pgs, mc->mc_db->md_root);
  done:
    if (unlikely(rc))
      txn->mt_flags |= MDB_TXN_ERROR;
  } else if (rc == MDB_NOTFOUND) {
    rc = MDB_SUCCESS;
  }
  mc->mc_flags &= ~C_INITIALIZED;
  return rc;
}

int mdbx_drop(MDB_txn *txn, MDB_dbi dbi, int del) {
  MDB_cursor *mc, *m2;
  int rc;

  if (unlikely(1 < (unsigned)del || !txn))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_signature != MDBX_MT_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(!TXN_DBI_EXIST(txn, dbi, DB_USRVALID)))
    return MDBX_EINVAL;

  if (unlikely(TXN_DBI_CHANGED(txn, dbi)))
    return MDB_BAD_DBI;

  if (unlikely(F_ISSET(txn->mt_flags, MDB_TXN_RDONLY)))
    return MDBX_EACCESS;

  rc = mdbx_cursor_open(txn, dbi, &mc);
  if (unlikely(rc))
    return rc;

  rc = mdbx_drop0(mc, mc->mc_db->md_flags & MDB_DUPSORT);
  /* Invalidate the dropped DB's cursors */
  for (m2 = txn->mt_cursors[dbi]; m2; m2 = m2->mc_next)
    m2->mc_flags &= ~(C_INITIALIZED | C_EOF);
  if (unlikely(rc))
    goto leave;

  /* Can't delete the main DB */
  if (del && dbi >= CORE_DBS) {
    rc = mdbx_del0(txn, MAIN_DBI, &mc->mc_dbx->md_name, NULL, F_SUBDATA);
    if (likely(!rc)) {
      txn->mt_dbflags[dbi] = DB_STALE;
      mdbx_dbi_close(txn->mt_env, dbi);
    } else {
      txn->mt_flags |= MDB_TXN_ERROR;
    }
  } else {
    /* reset the DB record, mark it dirty */
    txn->mt_dbflags[dbi] |= DB_DIRTY;
    txn->mt_dbs[dbi].md_depth = 0;
    txn->mt_dbs[dbi].md_branch_pages = 0;
    txn->mt_dbs[dbi].md_leaf_pages = 0;
    txn->mt_dbs[dbi].md_overflow_pages = 0;
    txn->mt_dbs[dbi].md_entries = 0;
    txn->mt_dbs[dbi].md_root = P_INVALID;
    txn->mt_dbs[dbi].md_seq = 0;

    txn->mt_flags |= MDB_TXN_DIRTY;
  }
leave:
  mdbx_cursor_close(mc);
  return rc;
}

int mdbx_set_compare(MDB_txn *txn, MDB_dbi dbi, MDB_cmp_func *cmp) {
  if (unlikely(!txn))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_signature != MDBX_MT_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(!TXN_DBI_EXIST(txn, dbi, DB_USRVALID)))
    return MDBX_EINVAL;

  txn->mt_dbxs[dbi].md_cmp = cmp;
  return MDB_SUCCESS;
}

int mdbx_set_dupsort(MDB_txn *txn, MDB_dbi dbi, MDB_cmp_func *cmp) {
  if (unlikely(!txn))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_signature != MDBX_MT_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(!TXN_DBI_EXIST(txn, dbi, DB_USRVALID)))
    return MDBX_EINVAL;

  txn->mt_dbxs[dbi].md_dcmp = cmp;
  return MDB_SUCCESS;
}

int __cold mdbx_reader_list(MDB_env *env, MDB_msg_func *func, void *ctx) {
  unsigned i, snap_nreaders;
  MDB_reader *mr;
  char buf[64];
  int rc = 0, first = 1;

  if (unlikely(!env || !func))
    return -MDBX_EINVAL;

  if (unlikely(env->me_signature != MDBX_ME_SIGNATURE))
    return MDBX_EBADSIGN;

  snap_nreaders = env->me_lck->mti_numreaders;
  mr = env->me_lck->mti_readers;
  for (i = 0; i < snap_nreaders; i++) {
    if (mr[i].mr_pid) {
      txnid_t txnid = mr[i].mr_txnid;
      if (txnid == ~(txnid_t)0)
        snprintf(buf, sizeof(buf), "%10d %" PRIxPTR " -\n", (int)mr[i].mr_pid,
                 (size_t)mr[i].mr_tid);
      else
        snprintf(buf, sizeof(buf), "%10d %" PRIxPTR " %" PRIuPTR "\n",
                 (int)mr[i].mr_pid, (size_t)mr[i].mr_tid, txnid);

      if (first) {
        first = 0;
        rc = func("    pid     thread     txnid\n", ctx);
        if (rc < 0)
          break;
      }
      rc = func(buf, ctx);
      if (rc < 0)
        break;
    }
  }
  if (first) {
    rc = func("(no active readers)\n", ctx);
  }
  return rc;
}

/** Insert pid into list if not already present.
 * return -1 if already present.
 */
static int __cold mdbx_pid_insert(mdbx_pid_t *ids, mdbx_pid_t pid) {
  /* binary search of pid in list */
  unsigned base = 0;
  unsigned cursor = 1;
  int val = 0;
  unsigned n = ids[0];

  while (0 < n) {
    unsigned pivot = n >> 1;
    cursor = base + pivot + 1;
    val = pid - ids[cursor];

    if (val < 0) {
      n = pivot;
    } else if (val > 0) {
      base = cursor;
      n -= pivot + 1;
    } else {
      /* found, so it's a duplicate */
      return -1;
    }
  }

  if (val > 0) {
    ++cursor;
  }
  ids[0]++;
  for (n = ids[0]; n > cursor; n--)
    ids[n] = ids[n - 1];
  ids[n] = pid;
  return 0;
}

int __cold mdbx_reader_check(MDB_env *env, int *dead) {
  if (unlikely(!env || env->me_signature != MDBX_ME_SIGNATURE))
    return MDBX_EINVAL;
  if (dead)
    *dead = 0;
  return mdbx_reader_check0(env, 0, dead);
}

int __cold mdbx_reader_check0(MDB_env *env, int rdt_locked, int *dead) {
  assert(rdt_locked >= 0);

  if (unlikely(env->me_pid != mdbx_getpid())) {
    env->me_flags |= MDB_FATAL_ERROR;
    return MDB_PANIC;
  }

  unsigned snap_nreaders = env->me_lck->mti_numreaders;
  mdbx_pid_t *pids = alloca((snap_nreaders + 1) * sizeof(mdbx_pid_t));
  pids[0] = 0;

  int rc = MDBX_RESULT_FALSE, count = 0;
  MDB_reader *mr = env->me_lck->mti_readers;

  for (unsigned i = 0; i < snap_nreaders; i++) {
    const mdbx_pid_t pid = mr[i].mr_pid;
    if (pid == 0)
      continue;
    if (pid != env->me_pid)
      continue;
    if (mdbx_pid_insert(pids, pid) != 0)
      continue;

    rc = mdbx_rpid_check(env, pid);
    if (rc == MDBX_RESULT_TRUE)
      continue; /* reader is live */

    if (rc != MDBX_RESULT_FALSE)
      break; /* mdbx_rpid_check() failed */

    /* stale reader found */
    if (!rdt_locked) {
      rc = mdbx_rdt_lock(env);
      if (MDBX_IS_ERROR(rc))
        break;

      rdt_locked = -1;
      if (rc == MDBX_RESULT_TRUE)
        /* the above checked all readers */
        break;

      /* a other process may have clean and reused slot, recheck */
      if (mr[i].mr_pid != pid)
        continue;

      rc = mdbx_rpid_check(env, pid);
      if (MDBX_IS_ERROR(rc))
        break;

      if (rc != MDBX_RESULT_FALSE) {
        /* the race with other process, slot reused */
        rc = MDBX_RESULT_FALSE;
        continue;
      }
    }

    /* clean it */
    for (unsigned j = i; j < snap_nreaders; j++) {
      if (mr[j].mr_pid == pid) {
        mdbx_debug("clear stale reader pid %u txn %" PRIiPTR "", (unsigned)pid,
                   mr[j].mr_txnid);
        mr[j].mr_pid = 0;
        count++;
      }
    }
  }

  if (rdt_locked < 0)
    mdbx_rdt_unlock(env);

  if (dead)
    *dead = count;
  return rc;
}

static unsigned __hot mdbx_midl_search(MDB_IDL ids, MDB_ID id) {
  /*
   * binary search of id in ids
   * if found, returns position of id
   * if not found, returns first position greater than id
   */
  unsigned base = 0;
  unsigned cursor = 1;
  int val = 0;
  unsigned n = ids[0];

  while (0 < n) {
    unsigned pivot = n >> 1;
    cursor = base + pivot + 1;
    val = mdbx_cmp2int(ids[cursor], id);

    if (val < 0) {
      n = pivot;

    } else if (val > 0) {
      base = cursor;
      n -= pivot + 1;

    } else {
      return cursor;
    }
  }

  if (val > 0) {
    ++cursor;
  }
  return cursor;
}

static MDB_IDL mdbx_midl_alloc(int num) {
  MDB_IDL ids = malloc((num + 2) * sizeof(MDB_ID));
  if (ids) {
    *ids++ = num;
    *ids = 0;
  }
  return ids;
}

static void mdbx_midl_free(MDB_IDL ids) {
  if (ids)
    free(ids - 1);
}

static void mdbx_midl_shrink(MDB_IDL *idp) {
  MDB_IDL ids = *idp;
  if (*(--ids) > MDB_IDL_UM_MAX &&
      (ids = realloc(ids, (MDB_IDL_UM_MAX + 2) * sizeof(MDB_ID)))) {
    *ids++ = MDB_IDL_UM_MAX;
    *idp = ids;
  }
}

static int mdbx_midl_grow(MDB_IDL *idp, int num) {
  MDB_IDL idn = *idp - 1;
  /* grow it */
  idn = realloc(idn, (*idn + num + 2) * sizeof(MDB_ID));
  if (!idn)
    return MDBX_ENOMEM;
  *idn++ += num;
  *idp = idn;
  return 0;
}

static int mdbx_midl_need(MDB_IDL *idp, unsigned num) {
  MDB_IDL ids = *idp;
  num += ids[0];
  if (num > ids[-1]) {
    num = (num + num / 4 + (256 + 2)) & -256;
    if (!(ids = realloc(ids - 1, num * sizeof(MDB_ID))))
      return MDBX_ENOMEM;
    *ids++ = num - 2;
    *idp = ids;
  }
  return 0;
}

static int mdbx_midl_append(MDB_IDL *idp, MDB_ID id) {
  MDB_IDL ids = *idp;
  /* Too big? */
  if (ids[0] >= ids[-1]) {
    if (mdbx_midl_grow(idp, MDB_IDL_UM_MAX))
      return MDBX_ENOMEM;
    ids = *idp;
  }
  ids[0]++;
  ids[ids[0]] = id;
  return 0;
}

static int mdbx_midl_append_list(MDB_IDL *idp, MDB_IDL app) {
  MDB_IDL ids = *idp;
  /* Too big? */
  if (ids[0] + app[0] >= ids[-1]) {
    if (mdbx_midl_grow(idp, app[0]))
      return MDBX_ENOMEM;
    ids = *idp;
  }
  memcpy(&ids[ids[0] + 1], &app[1], app[0] * sizeof(MDB_ID));
  ids[0] += app[0];
  return 0;
}

static int mdbx_midl_append_range(MDB_IDL *idp, MDB_ID id, unsigned n) {
  MDB_ID *ids = *idp, len = ids[0];
  /* Too big? */
  if (len + n > ids[-1]) {
    if (mdbx_midl_grow(idp, n | MDB_IDL_UM_MAX))
      return MDBX_ENOMEM;
    ids = *idp;
  }
  ids[0] = len + n;
  ids += len;
  while (n)
    ids[n--] = id++;
  return 0;
}

static void __hot mdbx_midl_xmerge(MDB_IDL idl, MDB_IDL merge) {
  MDB_ID old_id, merge_id, i = merge[0], j = idl[0], k = i + j, total = k;
  idl[0] = (MDB_ID)-1; /* delimiter for idl scan below */
  old_id = idl[j];
  while (i) {
    merge_id = merge[i--];
    for (; old_id < merge_id; old_id = idl[--j])
      idl[k--] = old_id;
    idl[k--] = merge_id;
  }
  idl[0] = total;
}

/* Quicksort + Insertion sort for small arrays */

#define SMALL 8
#define MIDL_SWAP(a, b)                                                        \
  {                                                                            \
    itmp = (a);                                                                \
    (a) = (b);                                                                 \
    (b) = itmp;                                                                \
  }

static void __hot mdbx_midl_sort(MDB_IDL ids) {
  /* Max possible depth of int-indexed tree * 2 items/level */
  int istack[sizeof(int) * CHAR_BIT * 2];
  int i, j, k, l, ir, jstack;
  MDB_ID a, itmp;

  ir = (int)ids[0];
  l = 1;
  jstack = 0;
  for (;;) {
    if (ir - l < SMALL) { /* Insertion sort */
      for (j = l + 1; j <= ir; j++) {
        a = ids[j];
        for (i = j - 1; i >= 1; i--) {
          if (ids[i] >= a)
            break;
          ids[i + 1] = ids[i];
        }
        ids[i + 1] = a;
      }
      if (jstack == 0)
        break;
      ir = istack[jstack--];
      l = istack[jstack--];
    } else {
      k = (l + ir) >> 1; /* Choose median of left, center, right */
      MIDL_SWAP(ids[k], ids[l + 1]);
      if (ids[l] < ids[ir]) {
        MIDL_SWAP(ids[l], ids[ir]);
      }
      if (ids[l + 1] < ids[ir]) {
        MIDL_SWAP(ids[l + 1], ids[ir]);
      }
      if (ids[l] < ids[l + 1]) {
        MIDL_SWAP(ids[l], ids[l + 1]);
      }
      i = l + 1;
      j = ir;
      a = ids[l + 1];
      for (;;) {
        do
          i++;
        while (ids[i] > a);
        do
          j--;
        while (ids[j] < a);
        if (j < i)
          break;
        MIDL_SWAP(ids[i], ids[j]);
      }
      ids[l + 1] = ids[j];
      ids[j] = a;
      jstack += 2;
      if (ir - i + 1 >= j - l) {
        istack[jstack] = ir;
        istack[jstack - 1] = i;
        ir = j - 1;
      } else {
        istack[jstack] = j - 1;
        istack[jstack - 1] = l;
        l = i;
      }
    }
  }
}

static unsigned __hot mdbx_mid2l_search(MDB_ID2L ids, MDB_ID id) {
  /*
   * binary search of id in ids
   * if found, returns position of id
   * if not found, returns first position greater than id
   */
  unsigned base = 0;
  unsigned cursor = 1;
  int val = 0;
  unsigned n = (unsigned)ids[0].mid;

  while (0 < n) {
    unsigned pivot = n >> 1;
    cursor = base + pivot + 1;
    val = mdbx_cmp2int(id, ids[cursor].mid);

    if (val < 0) {
      n = pivot;

    } else if (val > 0) {
      base = cursor;
      n -= pivot + 1;

    } else {
      return cursor;
    }
  }

  if (val > 0) {
    ++cursor;
  }
  return cursor;
}

static int mdbx_mid2l_insert(MDB_ID2L ids, MDB_ID2 *id) {
  unsigned x, i;

  x = mdbx_mid2l_search(ids, id->mid);

  if (x < 1) {
    /* internal error */
    return -2;
  }

  if (x <= ids[0].mid && ids[x].mid == id->mid) {
    /* duplicate */
    return -1;
  }

  if (ids[0].mid >= MDB_IDL_UM_MAX) {
    /* too big */
    return -2;

  } else {
    /* insert id */
    ids[0].mid++;
    for (i = (unsigned)ids[0].mid; i > x; i--)
      ids[i] = ids[i - 1];
    ids[x] = *id;
  }

  return 0;
}

static int mdbx_mid2l_append(MDB_ID2L ids, MDB_ID2 *id) {
  /* Too big? */
  if (ids[0].mid >= MDB_IDL_UM_MAX) {
    return -2;
  }
  ids[0].mid++;
  ids[ids[0].mid] = *id;
  return 0;
}

int __cold mdbx_setup_debug(int flags, MDBX_debug_func *logger, long edge_txn) {
  unsigned ret = mdbx_runtime_flags;
  if (flags != (int)MDBX_DBG_DNT)
    mdbx_runtime_flags = flags;
  if (logger != (MDBX_debug_func *)MDBX_DBG_DNT)
    mdbx_debug_logger = logger;
  if (edge_txn != (long)MDBX_DBG_DNT) {
#if MDB_DEBUG
    mdbx_debug_edge = edge_txn;
#endif
  }
  return ret;
}

static txnid_t __cold mdbx_oomkick(MDB_env *env, txnid_t oldest) {
  int retry;
  txnid_t snap;
  mdbx_debug("DB size maxed out");

  for (retry = 0;; ++retry) {
    int reader;

    if (mdbx_reader_check(env, NULL))
      break;

    snap = mdbx_find_oldest(env, &reader);
    if (oldest < snap || reader < 0) {
      if (retry && env->me_oom_func) {
        /* LY: notify end of oom-loop */
        env->me_oom_func(env, 0, 0, oldest, snap - oldest, -retry);
      }
      return snap;
    }

    MDB_reader *r;
    mdbx_tid_t tid;
    mdbx_pid_t pid;
    int rc;

    if (!env->me_oom_func)
      break;

    r = &env->me_lck->mti_readers[reader];
    pid = r->mr_pid;
    tid = r->mr_tid;
    if (r->mr_txnid != oldest || pid <= 0)
      continue;

    rc = env->me_oom_func(env, pid, tid, oldest,
                          mdbx_meta_head(env)->mm_txnid - oldest, retry);
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
  return mdbx_find_oldest(env, NULL);
}

int __cold mdbx_env_set_syncbytes(MDB_env *env, size_t bytes) {
  if (unlikely(!env))
    return MDBX_EINVAL;

  if (unlikely(env->me_signature != MDBX_ME_SIGNATURE))
    return MDBX_EBADSIGN;

  env->me_sync_threshold = bytes;
  return env->me_map ? mdbx_env_sync(env, 0) : MDB_SUCCESS;
}

void __cold mdbx_env_set_oomfunc(MDB_env *env, MDBX_oom_func *oomfunc) {
  if (likely(env && env->me_signature == MDBX_ME_SIGNATURE))
    env->me_oom_func = oomfunc;
}

MDBX_oom_func *__cold mdbx_env_get_oomfunc(MDB_env *env) {
  return likely(env && env->me_signature == MDBX_ME_SIGNATURE)
             ? env->me_oom_func
             : NULL;
}

#ifdef __SANITIZE_THREAD__
/* LY: avoid tsan-trap by me_txn, mm_last_pg and mt_next_pgno */
__attribute__((no_sanitize_thread, noinline))
#endif
int mdbx_txn_straggler(MDB_txn *txn, int *percent)
{
  MDB_env *env;
  MDB_meta *meta;
  txnid_t lag;

  if (unlikely(!txn))
    return -MDBX_EINVAL;

  if (unlikely(txn->mt_signature != MDBX_MT_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(!txn->mt_u.reader))
    return -1;

  env = txn->mt_env;
  meta = mdbx_meta_head(env);
  if (percent) {
    size_t maxpg = env->me_maxpg;
    size_t last = meta->mm_last_pg + 1;
    if (env->me_txn)
      last = env->me_txn0->mt_next_pgno;
    *percent = (last * 100ull + maxpg / 2) / maxpg;
  }
  lag = meta->mm_txnid - txn->mt_u.reader->mr_txnid;
  return (lag > INT_MAX) ? INT_MAX : (int)lag;
}

typedef struct mdbx_walk_ctx {
  MDB_txn *mw_txn;
  void *mw_user;
  MDBX_pgvisitor_func *mw_visitor;
} mdbx_walk_ctx_t;

/** Depth-first tree traversal. */
static int __cold mdbx_env_walk(mdbx_walk_ctx_t *ctx, const char *dbi,
                                pgno_t pg, int deep) {
  MDB_page *mp;
  int rc, i, nkeys;
  unsigned header_size, unused_size, payload_size, align_bytes;
  const char *type;

  if (pg == P_INVALID)
    return MDB_SUCCESS; /* empty db */

  MDB_cursor mc;
  memset(&mc, 0, sizeof(mc));
  mc.mc_snum = 1;
  mc.mc_txn = ctx->mw_txn;

  rc = mdbx_page_get(&mc, pg, &mp, NULL);
  if (rc)
    return rc;
  if (pg != mp->mp_p.p_pgno)
    return MDB_CORRUPTED;

  nkeys = NUMKEYS(mp);
  header_size = IS_LEAF2(mp) ? PAGEHDRSZ : PAGEBASE + mp->mp_lower;
  unused_size = SIZELEFT(mp);
  payload_size = 0;

  /* LY: Don't use mask here, e.g bitwise
   * (P_BRANCH|P_LEAF|P_LEAF2|P_META|P_OVERFLOW|P_SUBP).
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
  case P_LEAF | P_SUBP:
    type = "dupsort-subleaf";
    break;
  case P_LEAF | P_LEAF2:
    type = "dupfixed-leaf";
    break;
  case P_LEAF | P_LEAF2 | P_SUBP:
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
      rc = mdbx_env_walk(ctx, dbi, NODEPGNO(node), deep);
      if (rc)
        return rc;
      continue;
    }

    assert(IS_LEAF(mp));
    if (node->mn_flags & F_BIGDATA) {
      MDB_page *omp;
      pgno_t *opg;
      size_t over_header, over_payload, over_unused;

      payload_size += sizeof(pgno_t);
      opg = NODEDATA(node);
      rc = mdbx_page_get(&mc, *opg, &omp, NULL);
      if (rc)
        return rc;
      if (*opg != omp->mp_p.p_pgno)
        return MDB_CORRUPTED;
      /* LY: Don't use mask here, e.g bitwise
       * (P_BRANCH|P_LEAF|P_LEAF2|P_META|P_OVERFLOW|P_SUBP).
       * Pages should not me marked dirty/loose or otherwise. */
      if (P_OVERFLOW != omp->mp_flags)
        return MDB_CORRUPTED;

      over_header = PAGEHDRSZ;
      over_payload = NODEDSZ(node);
      over_unused = omp->mp_pages * ctx->mw_txn->mt_env->me_psize -
                    over_payload - over_header;

      rc = ctx->mw_visitor(*opg, omp->mp_pages, ctx->mw_user, dbi,
                           "overflow-data", 1, over_payload, over_header,
                           over_unused);
      if (rc)
        return rc;
      continue;
    }

    payload_size += NODEDSZ(node);
    if (node->mn_flags & F_SUBDATA) {
      MDB_db *db = NODEDATA(node);
      char *name = NULL;

      if (!(node->mn_flags & F_DUPDATA)) {
        name = NODEKEY(node);
        ptrdiff_t namelen = (char *)db - name;
        name = memcpy(alloca(namelen + 1), name, namelen);
        name[namelen] = 0;
      }
      rc = mdbx_env_walk(ctx, (name && name[0]) ? name : dbi, db->md_root,
                         deep + 1);
      if (rc)
        return rc;
    }
  }

  return ctx->mw_visitor(mp->mp_p.p_pgno, 1, ctx->mw_user, dbi, type, nkeys,
                         payload_size, header_size, unused_size + align_bytes);
}

int __cold mdbx_env_pgwalk(MDB_txn *txn, MDBX_pgvisitor_func *visitor,
                           void *user) {
  mdbx_walk_ctx_t ctx;
  int rc;

  if (unlikely(!txn))
    return MDB_BAD_TXN;
  if (unlikely(txn->mt_signature != MDBX_MT_SIGNATURE))
    return MDBX_EBADSIGN;

  ctx.mw_txn = txn;
  ctx.mw_user = user;
  ctx.mw_visitor = visitor;

  rc = visitor(0, 2, user, "lmdb", "meta", 2, sizeof(MDB_meta) * 2,
               PAGEHDRSZ * 2,
               (txn->mt_env->me_psize - sizeof(MDB_meta) - PAGEHDRSZ) * 2);
  if (!rc)
    rc = mdbx_env_walk(&ctx, "free", txn->mt_dbs[FREE_DBI].md_root, 0);
  if (!rc)
    rc = mdbx_env_walk(&ctx, "main", txn->mt_dbs[MAIN_DBI].md_root, 0);
  if (!rc)
    rc = visitor(P_INVALID, 0, user, NULL, NULL, 0, 0, 0, 0);
  return rc;
}

int mdbx_canary_put(MDB_txn *txn, const mdbx_canary *canary) {
  if (unlikely(!txn))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_signature != MDBX_MT_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(txn->mt_flags & MDB_TXN_BLOCKED))
    return MDB_BAD_TXN;

  if (unlikely(F_ISSET(txn->mt_flags, MDB_TXN_RDONLY)))
    return MDBX_EACCESS;

  if (likely(canary)) {
    if (txn->mt_canary.x == canary->x && txn->mt_canary.y == canary->y &&
        txn->mt_canary.z == canary->z)
      return MDB_SUCCESS;
    txn->mt_canary.x = canary->x;
    txn->mt_canary.y = canary->y;
    txn->mt_canary.z = canary->z;
  }
  txn->mt_canary.v = txn->mt_txnid;

  if ((txn->mt_flags & MDB_TXN_DIRTY) == 0) {
    MDB_env *env = txn->mt_env;
    txn->mt_flags |= MDB_TXN_DIRTY;
    env->me_sync_pending += env->me_psize;
  }

  return MDB_SUCCESS;
}

int mdbx_canary_get(MDB_txn *txn, mdbx_canary *canary) {
  if (unlikely(txn == NULL || canary == NULL))
    return MDBX_EINVAL;
  if (unlikely(txn->mt_signature != MDBX_MT_SIGNATURE))
    return MDBX_EBADSIGN;

  *canary = txn->mt_canary;
  return MDB_SUCCESS;
}

int mdbx_cursor_on_first(MDB_cursor *mc) {
  if (unlikely(mc == NULL))
    return MDBX_EINVAL;

  if (unlikely(mc->mc_signature != MDBX_MC_SIGNATURE))
    return MDBX_EBADSIGN;

  if (!(mc->mc_flags & C_INITIALIZED))
    return MDBX_RESULT_FALSE;

  unsigned i;
  for (i = 0; i < mc->mc_snum; ++i) {
    if (mc->mc_ki[i])
      return MDBX_RESULT_FALSE;
  }

  return MDBX_RESULT_TRUE;
}

int mdbx_cursor_on_last(MDB_cursor *mc) {
  if (unlikely(mc == NULL))
    return MDBX_EINVAL;

  if (unlikely(mc->mc_signature != MDBX_MC_SIGNATURE))
    return MDBX_EBADSIGN;

  if (!(mc->mc_flags & C_INITIALIZED))
    return MDBX_RESULT_FALSE;

  unsigned i;
  for (i = 0; i < mc->mc_snum; ++i) {
    unsigned nkeys = NUMKEYS(mc->mc_pg[i]);
    if (mc->mc_ki[i] < nkeys - 1)
      return MDBX_RESULT_FALSE;
  }

  return MDBX_RESULT_TRUE;
}

int mdbx_cursor_eof(MDB_cursor *mc) {
  if (unlikely(mc == NULL))
    return MDBX_EINVAL;

  if (unlikely(mc->mc_signature != MDBX_MC_SIGNATURE))
    return MDBX_EBADSIGN;

  if ((mc->mc_flags & C_INITIALIZED) == 0)
    return MDBX_RESULT_TRUE;

  if (mc->mc_snum == 0)
    return MDBX_RESULT_TRUE;

  if ((mc->mc_flags & C_EOF) &&
      mc->mc_ki[mc->mc_top] >= NUMKEYS(mc->mc_pg[mc->mc_top]))
    return MDBX_RESULT_TRUE;

  return MDBX_RESULT_FALSE;
}

static int mdbx_is_samedata(const MDB_val *a, const MDB_val *b) {
  return a->iov_len == b->iov_len &&
         memcmp(a->iov_base, b->iov_base, a->iov_len) == 0;
}

/* Позволяет обновить или удалить существующую запись с получением
 * в old_data предыдущего значения данных. При этом если new_data равен
 * нулю, то выполняется удаление, иначе обновление/вставка.
 *
 * Текущее значение может находиться в уже измененной (грязной) странице.
 * В этом случае страница будет перезаписана при обновлении, а само старое
 * значение утрачено. Поэтому исходно в old_data должен быть передан
 * дополнительный буфер для копирования старого значения.
 * Если переданный буфер слишком мал, то функция вернет -1, установив
 * old_data->iov_len в соответствующее значение.
 *
 * Для не-уникальных ключей также возможен второй сценарий использования,
 * когда посредством old_data из записей с одинаковым ключом для
 * удаления/обновления выбирается конкретная. Для выбора этого сценария
 * во flags следует одновременно указать MDB_CURRENT и MDB_NOOVERWRITE.
 * Именно эта комбинация выбрана, так как она лишена смысла, и этим позволяет
 * идентифицировать запрос такого сценария.
 *
 * Функция может быть замещена соответствующими операциями с курсорами
 * после двух доработок (TODO):
 *  - внешняя аллокация курсоров, в том числе на стеке (без malloc).
 *  - получения статуса страницы по адресу (знать о P_DIRTY).
 */
int mdbx_replace(MDB_txn *txn, MDB_dbi dbi, MDB_val *key, MDB_val *new_data,
                 MDB_val *old_data, unsigned flags) {
  MDB_cursor mc;
  MDB_xcursor mx;

  if (unlikely(!key || !old_data || !txn || old_data == new_data))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_signature != MDBX_MT_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(old_data->iov_base == NULL && old_data->iov_len))
    return MDBX_EINVAL;

  if (unlikely(new_data == NULL && !(flags & MDB_CURRENT)))
    return MDBX_EINVAL;

  if (unlikely(!TXN_DBI_EXIST(txn, dbi, DB_USRVALID)))
    return MDBX_EINVAL;

  if (unlikely(flags &
               ~(MDB_NOOVERWRITE | MDB_NODUPDATA | MDB_RESERVE | MDB_APPEND |
                 MDB_APPENDDUP | MDB_CURRENT)))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_flags & (MDB_TXN_RDONLY | MDB_TXN_BLOCKED)))
    return (txn->mt_flags & MDB_TXN_RDONLY) ? MDBX_EACCESS : MDB_BAD_TXN;

  mdbx_cursor_init(&mc, txn, dbi, &mx);
  mc.mc_next = txn->mt_cursors[dbi];
  txn->mt_cursors[dbi] = &mc;

  int rc;
  MDB_val present_key = *key;
  if (F_ISSET(flags, MDB_CURRENT | MDB_NOOVERWRITE)) {
    /* в old_data значение для выбора конкретного дубликата */
    if (unlikely(!(txn->mt_dbs[dbi].md_flags & MDB_DUPSORT))) {
      rc = MDBX_EINVAL;
      goto bailout;
    }

    /* убираем лишний бит, он был признаком запрошенного режима */
    flags -= MDB_NOOVERWRITE;

    rc = mdbx_cursor_get(&mc, &present_key, old_data, MDB_GET_BOTH);
    if (rc != MDB_SUCCESS)
      goto bailout;

    if (new_data) {
      /* обновление конкретного дубликата */
      if (mdbx_is_samedata(old_data, new_data))
        /* если данные совпадают, то ничего делать не надо */
        goto bailout;
#if 0 /* LY: исправлено в mdbx_cursor_put(), здесь в качестве памятки */
			MDB_node *leaf = NODEPTR(mc.mc_pg[mc.mc_top], mc.mc_ki[mc.mc_top]);
			if (F_ISSET(leaf->mn_flags, F_DUPDATA)
					&& mc.mc_xcursor->mx_db.md_entries > 1) {
				/* Если у ключа больше одного значения, то
				 * сначала удаляем найденое "старое" значение.
				 *
				 * Этого можно не делать, так как MDBX уже
				 * обучен корректно обрабатывать такие ситуации.
				 *
				 * Однако, следует помнить, что в LMDB при
				 * совпадении размера данных, значение будет
				 * просто перезаписано с нарушением
				 * упорядоченности, что сломает поиск. */
				rc = mdbx_cursor_del(&mc, 0);
				if (rc != MDB_SUCCESS)
					goto bailout;
				flags -= MDB_CURRENT;
			}
#endif
    }
  } else {
    /* в old_data буфер для сохранения предыдущего значения */
    if (unlikely(new_data && old_data->iov_base == new_data->iov_base))
      return MDBX_EINVAL;
    MDB_val present_data;
    rc = mdbx_cursor_get(&mc, &present_key, &present_data, MDB_SET_KEY);
    if (unlikely(rc != MDB_SUCCESS)) {
      old_data->iov_base = NULL;
      old_data->iov_len = rc;
      if (rc != MDB_NOTFOUND || (flags & MDB_CURRENT))
        goto bailout;
    } else if (flags & MDB_NOOVERWRITE) {
      rc = MDB_KEYEXIST;
      *old_data = present_data;
      goto bailout;
    } else {
      MDB_page *page = mc.mc_pg[mc.mc_top];
      if (txn->mt_dbs[dbi].md_flags & MDB_DUPSORT) {
        if (flags & MDB_CURRENT) {
          /* для не-уникальных ключей позволяем update/delete только если ключ
           * один */
          MDB_node *leaf = NODEPTR(page, mc.mc_ki[mc.mc_top]);
          if (F_ISSET(leaf->mn_flags, F_DUPDATA)) {
            mdbx_tassert(txn, XCURSOR_INITED(&mc) &&
                                  mc.mc_xcursor->mx_db.md_entries > 1);
            if (mc.mc_xcursor->mx_db.md_entries > 1) {
              rc = MDBX_EMULTIVAL;
              goto bailout;
            }
          }
          /* если данные совпадают, то ничего делать не надо */
          if (new_data && mdbx_is_samedata(&present_data, new_data)) {
            *old_data = *new_data;
            goto bailout;
          }
          /* В оригинальной LMDB фладок MDB_CURRENT здесь приведет
           * к замене данных без учета MDB_DUPSORT сортировки,
           * но здесь это в любом случае допустимо, так как мы
           * проверили что для ключа есть только одно значение. */
        } else if ((flags & MDB_NODUPDATA) &&
                   mdbx_is_samedata(&present_data, new_data)) {
          /* если данные совпадают и установлен MDB_NODUPDATA */
          rc = MDB_KEYEXIST;
          goto bailout;
        }
      } else {
        /* если данные совпадают, то ничего делать не надо */
        if (new_data && mdbx_is_samedata(&present_data, new_data)) {
          *old_data = *new_data;
          goto bailout;
        }
        flags |= MDB_CURRENT;
      }

      if (page->mp_flags & P_DIRTY) {
        if (unlikely(old_data->iov_len < present_data.iov_len)) {
          old_data->iov_base = NULL;
          old_data->iov_len = present_data.iov_len;
          rc = MDBX_RESULT_TRUE;
          goto bailout;
        }
        memcpy(old_data->iov_base, present_data.iov_base, present_data.iov_len);
        old_data->iov_len = present_data.iov_len;
      } else {
        *old_data = present_data;
      }
    }
  }

  if (likely(new_data))
    rc = mdbx_cursor_put(&mc, key, new_data, flags);
  else
    rc = mdbx_cursor_del(&mc, 0);

bailout:
  txn->mt_cursors[dbi] = mc.mc_next;
  return rc;
}

int mdbx_get_ex(MDB_txn *txn, MDB_dbi dbi, MDB_val *key, MDB_val *data,
                int *values_count) {
  DKBUF;
  mdbx_debug("===> get db %u key [%s]", dbi, DKEY(key));

  if (unlikely(!key || !data || !txn))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_signature != MDBX_MT_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(!TXN_DBI_EXIST(txn, dbi, DB_USRVALID)))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_flags & MDB_TXN_BLOCKED))
    return MDB_BAD_TXN;

  MDB_cursor mc;
  MDB_xcursor mx;
  mdbx_cursor_init(&mc, txn, dbi, &mx);

  int exact = 0;
  int rc = mdbx_cursor_set(&mc, key, data, MDB_SET_KEY, &exact);
  if (unlikely(rc != MDB_SUCCESS)) {
    if (rc == MDB_NOTFOUND && values_count)
      *values_count = 0;
    return rc;
  }

  if (values_count) {
    *values_count = 1;
    if (mc.mc_xcursor != NULL) {
      MDB_node *leaf = NODEPTR(mc.mc_pg[mc.mc_top], mc.mc_ki[mc.mc_top]);
      if (F_ISSET(leaf->mn_flags, F_DUPDATA)) {
        mdbx_tassert(txn, mc.mc_xcursor == &mx &&
                              (mx.mx_cursor.mc_flags & C_INITIALIZED));
        *values_count = mx.mx_db.md_entries;
      }
    }
  }
  return MDB_SUCCESS;
}

/* Функция сообщает находится ли указанный адрес в "грязной" странице у
 * заданной пишущей транзакции. В конечном счете это позволяет избавиться от
 * лишнего копирования данных из НЕ-грязных страниц.
 *
 * "Грязные" страницы - это те, которые уже были изменены в ходе пишущей
 * транзакции. Соответственно, какие-либо дальнейшие изменения могут привести
 * к перезаписи таких страниц. Поэтому все функции, выполняющие изменения, в
 * качестве аргументов НЕ должны получать указатели на данные в таких
 * страницах. В свою очередь "НЕ грязные" страницы перед модификацией будут
 * скопированы.
 *
 * Другими словами, данные из "грязных" страниц должны быть либо скопированы
 * перед передачей в качестве аргументов для дальнейших модификаций, либо
 * отвергнуты на стадии проверки корректности аргументов.
 *
 * Таким образом, функция позволяет как избавится от лишнего копирования,
 * так и выполнить более полную проверку аргументов.
 *
 * ВАЖНО: Передаваемый указатель должен указывать на начало данных. Только
 * так гарантируется что актуальный заголовок страницы будет физически
 * расположен в той-же странице памяти, в том числе для многостраничных
 * P_OVERFLOW страниц с длинными данными. */
int mdbx_is_dirty(const MDB_txn *txn, const void *ptr) {
  if (unlikely(!txn))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_signature != MDBX_MT_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(txn->mt_flags & MDB_TXN_RDONLY))
    return MDBX_RESULT_FALSE;

  const MDB_env *env = txn->mt_env;
  const uintptr_t mask = ~(uintptr_t)(env->me_psize - 1);
  const MDB_page *page = (const MDB_page *)((uintptr_t)ptr & mask);

  /* LY: Тут не всё хорошо с абсолютной достоверностью результата,
   * так как флажок P_DIRTY в LMDB может означать не совсем то,
   * что было исходно задумано, детали см в логике кода mdbx_page_touch().
   *
   * Более того, в режиме БЕЗ WRITEMAP грязные страницы выделяются через
   * malloc(), т.е. находятся вне mmap-диапазона.
   *
   * Тем не менее, однозначно страница "не грязная" если адрес находится
   * внутри mmap-диапазона и в заголовке страницы нет флажка P_DIRTY. */
  if (env->me_map < (char *)page) {
    const size_t used_size = env->me_psize * txn->mt_next_pgno;
    if ((char *)page < env->me_map + used_size) {
      /* страница внутри диапазона, смотрим на флажки  */
      if ((page->mp_flags & (P_DIRTY | P_LOOSE | P_KEEP)) == 0)
        return MDBX_RESULT_FALSE;
    }
    /* Гипотетически здесь возможна ситуация, когда указатель адресует что-то
     * в пределах mmap, но за границей распределенных страниц. Это тяжелая
     * ошибка, к которой не возможно прийти без каких-то больших нарушений.
     * Поэтому не проверяем этот случай кроме как assert-ом, ибо бестолку. */
    mdbx_tassert(txn, env->me_map + env->me_mapsize > (char *)page);
  }

  /* Страница вне используемого mmap-диапазона, т.е. либо в функцию был
   * передан некорректный адрес, либо адрес в теневой странице, которая была
   * выделена посредством malloc().
   *
   * Поэтому всегда считаем что страница вне mmap-диапазона "грязная",
   * не просматривая при этом списки грязных и spilled страниц у каких-либо
   * транзакций. Такая логика имеет ряд преимуществ:
   *  - не тратим время на просмотр списков;
   *  - результат всегда безопасен (может быть ложно-положительным, но
   *    не ложно-отрицательным);
   *  - результат не зависит от вложенности транзакций и от относительного
   *    положения переданной транзакции в этой рекурсии. */
  return MDBX_RESULT_TRUE;
}

int mdbx_dbi_open_ex(MDB_txn *txn, const char *name, unsigned flags,
                     MDB_dbi *pdbi, MDB_cmp_func *keycmp,
                     MDB_cmp_func *datacmp) {
  int rc = mdbx_dbi_open(txn, name, flags, pdbi);
  if (likely(rc == MDB_SUCCESS)) {
    MDB_dbi dbi = *pdbi;
    unsigned md_flags = txn->mt_dbs[dbi].md_flags;
    txn->mt_dbxs[dbi].md_cmp = keycmp ? keycmp : mdbx_default_keycmp(md_flags);
    txn->mt_dbxs[dbi].md_dcmp =
        datacmp ? datacmp : mdbx_default_datacmp(md_flags);
  }
  return rc;
}

int mdbx_dbi_sequence(MDB_txn *txn, MDB_dbi dbi, uint64_t *result,
                      uint64_t increment) {
  if (unlikely(!txn))
    return MDBX_EINVAL;

  if (unlikely(txn->mt_signature != MDBX_MT_SIGNATURE))
    return MDBX_EBADSIGN;

  if (unlikely(!TXN_DBI_EXIST(txn, dbi, DB_USRVALID)))
    return MDBX_EINVAL;

  if (unlikely(TXN_DBI_CHANGED(txn, dbi)))
    return MDB_BAD_DBI;

  MDB_db *dbs = &txn->mt_dbs[dbi];
  if (likely(result))
    *result = dbs->md_seq;

  if (likely(increment > 0)) {
    if (unlikely(txn->mt_flags & MDB_TXN_BLOCKED))
      return MDB_BAD_TXN;

    if (unlikely(F_ISSET(txn->mt_flags, MDB_TXN_RDONLY)))
      return MDBX_EACCESS;

    uint64_t new = dbs->md_seq + increment;
    if (unlikely(new < increment))
      return MDBX_RESULT_TRUE;

    assert(new > dbs->md_seq);
    dbs->md_seq = new;
    txn->mt_flags |= MDB_TXN_DIRTY;
    txn->mt_dbflags[dbi] |= DB_DIRTY;
  }

  return MDB_SUCCESS;
}
