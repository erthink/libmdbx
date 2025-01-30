/// \copyright SPDX-License-Identifier: Apache-2.0
/// \note Please refer to the COPYRIGHT file for explanations license change,
/// credits and acknowledgments.
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025

#pragma once

/*----------------------------------------------------------------------------*/

#include "essentials.h"

typedef struct dp dp_t;
typedef struct dpl dpl_t;
typedef struct kvx kvx_t;
typedef struct meta_ptr meta_ptr_t;
typedef struct inner_cursor subcur_t;
typedef struct cursor_couple cursor_couple_t;
typedef struct defer_free_item defer_free_item_t;

typedef struct troika {
  uint8_t fsm, recent, prefer_steady, tail_and_flags;
#if MDBX_WORDBITS > 32 /* Workaround for false-positives from Valgrind */
  uint32_t unused_pad;
#endif
#define TROIKA_HAVE_STEADY(troika) ((troika)->fsm & 7u)
#define TROIKA_STRICT_VALID(troika) ((troika)->tail_and_flags & 64u)
#define TROIKA_VALID(troika) ((troika)->tail_and_flags & 128u)
#define TROIKA_TAIL(troika) ((troika)->tail_and_flags & 3u)
  txnid_t txnid[NUM_METAS];
} troika_t;

typedef struct page_get_result {
  page_t *page;
  int err;
} pgr_t;

typedef struct node_search_result {
  node_t *node;
  bool exact;
} nsr_t;

typedef struct bind_reader_slot_result {
  int err;
  reader_slot_t *slot;
} bsr_t;

#include "atomics-ops.h"
#include "proto.h"
#include "rkl.h"
#include "txl.h"
#include "unaligned.h"
#if defined(_WIN32) || defined(_WIN64)
#include "windows-import.h"
#endif /* Windows */

enum signatures {
  env_signature = INT32_C(0x1A899641),
  txn_signature = INT32_C(0x13D53A31),
  cur_signature_live = INT32_C(0x7E05D5B1),
  cur_signature_ready4dispose = INT32_C(0x2817A047),
  cur_signature_wait4eot = INT32_C(0x10E297A7)
};

/*----------------------------------------------------------------------------*/

/* An dirty-page list item is an pgno/pointer pair. */
struct dp {
  page_t *ptr;
  pgno_t pgno, npages;
};

enum dpl_rules {
  dpl_gap_edging = 2,
  dpl_gap_mergesort = 16,
  dpl_reserve_gap = dpl_gap_mergesort + dpl_gap_edging,
  dpl_insertion_threshold = 42
};

/* An DPL (dirty-page list) is a lazy-sorted array of MDBX_DPs. */
struct dpl {
  size_t sorted;
  size_t length;
  /* number of pages, but not an entries. */
  size_t pages_including_loose;
  /* allocated size excluding the dpl_reserve_gap */
  size_t detent;
  /* dynamic size with holes at zero and after the last */
  dp_t items[dpl_reserve_gap];
};

/*----------------------------------------------------------------------------*/
/* Internal structures */

/* Comparing/ordering and length constraints */
typedef struct clc {
  MDBX_cmp_func *cmp; /* comparator */
  size_t lmin, lmax;  /* min/max length constraints */
} clc_t;

/* Вспомогательная информация о table.
 *
 * Совокупность потребностей:
 * 1. Для транзакций и основного курсора нужны все поля.
 * 2. Для вложенного dupsort-курсора нужен компаратор значений, который изнутри
 *    курсора будет выглядеть как компаратор ключей. Плюс заглушка компаратора
 *    значений, которая не должна использоваться в штатных ситуациях, но
 *    требуется хотя-бы для отслеживания таких обращений.
 * 3. Использование компараторов для курсора и вложенного dupsort-курсора
 *    должно выглядеть одинаково.
 * 4. Желательно минимизировать объём данных размещаемых внутри вложенного
 *    dupsort-курсора.
 * 5. Желательно чтобы объем всей структуры был степенью двойки.
 *
 * Решение:
 *  - не храним в dupsort-курсоре ничего лишнего, а только tree;
 *  - в курсоры помещаем только указатель на clc_t, который будет указывать
 *    на соответствующее clc-поле в общей kvx-таблице привязанной к env;
 *  - компаратор размещаем в начале clc_t, в kvx_t сначала размещаем clc
 *    для ключей, потом для значений, а имя БД в конце kvx_t.
 *  - тогда в курсоре clc[0] будет содержать информацию для ключей,
 *    а clc[1] для значений, причем компаратор значений для dupsort-курсора
 *    будет попадать на MDBX_val с именем, что приведет к SIGSEGV при попытке
 *    использования такого компаратора.
 *  - размер kvx_t становится равным 8 словам.
 *
 * Трюки и прочая экономия на спичках:
 *  - не храним dbi внутри курсора, вместо этого вычисляем его как разницу между
 *    dbi_state курсора и началом таблицы dbi_state в транзакции. Смысл тут в
 *    экономии кол-ва полей при инициализации курсора. Затрат это не создает,
 *    так как dbi требуется для последующего доступа к массивам в транзакции,
 *    т.е. при вычислении dbi разыменовывается тот-же указатель на txn
 *    и читается та же кэш-линия с указателями. */
typedef struct clc2 {
  clc_t k; /* для ключей */
  clc_t v; /* для значений */
} clc2_t;

struct kvx {
  clc2_t clc;
  MDBX_val name; /* имя table */
};

/* Non-shared DBI state flags inside transaction */
enum dbi_state {
  DBI_DIRTY = 0x01 /* DB was written in this txn */,
  DBI_STALE = 0x02 /* Named-DB record is older than txnID */,
  DBI_FRESH = 0x04 /* Named-DB handle opened in this txn */,
  DBI_CREAT = 0x08 /* Named-DB handle created in this txn */,
  DBI_VALID = 0x10 /* Handle is valid, see also DB_VALID */,
  DBI_OLDEN = 0x40 /* Handle was closed/reopened outside txn */,
  DBI_LINDO = 0x80 /* Lazy initialization done for DBI-slot */,
};

enum txn_flags {
  txn_ro_begin_flags = MDBX_TXN_RDONLY | MDBX_TXN_RDONLY_PREPARE,
  txn_rw_begin_flags = MDBX_TXN_NOMETASYNC | MDBX_TXN_NOSYNC | MDBX_TXN_TRY,
  txn_shrink_allowed = UINT32_C(0x40000000),
  txn_parked = MDBX_TXN_PARKED,
  txn_gc_drained = 0x80 /* GC was depleted up to oldest reader */,
  txn_may_have_cursors = 0x100,
  txn_state_flags = MDBX_TXN_FINISHED | MDBX_TXN_ERROR | MDBX_TXN_DIRTY | MDBX_TXN_SPILLS | MDBX_TXN_HAS_CHILD |
                    MDBX_TXN_INVALID | txn_gc_drained
};

/* A database transaction.
 * Every operation requires a transaction handle. */
struct MDBX_txn {
  int32_t signature;
  uint32_t flags; /* Transaction Flags */
  size_t n_dbi;
  size_t owner; /* thread ID that owns this transaction */

  MDBX_txn *parent; /* parent of a nested txn */
  MDBX_txn *nested; /* nested txn under this txn,
                       set together with MDBX_TXN_HAS_CHILD */
  geo_t geo;

  /* The ID of this transaction. IDs are integers incrementing from
   * INITIAL_TXNID. Only committed write transactions increment the ID. If a
   * transaction aborts, the ID may be re-used by the next writer. */
  txnid_t txnid, front_txnid;

  MDBX_env *env; /* the DB environment */
  tree_t *dbs;   /* Array of tree_t records for each known DB */

#if MDBX_ENABLE_DBI_SPARSE
  unsigned *__restrict dbi_sparse;
#endif /* MDBX_ENABLE_DBI_SPARSE */

  /* Array of non-shared txn's flags of DBI.
   * Модификатор __restrict тут полезен и безопасен в текущем понимании,
   * так как пересечение возможно только с dbi_state курсоров,
   * и происходит по-чтению до последующего изменения/записи. */
  uint8_t *__restrict dbi_state;

  /* Array of sequence numbers for each DB handle. */
  uint32_t *__restrict dbi_seqs;

  /* Массив с головами односвязных списков отслеживания курсоров. */
  MDBX_cursor **cursors;

  /* "Канареечные" маркеры/счетчики */
  MDBX_canary canary;

  /* User-settable context */
  void *userctx;

  union {
    struct {
      /* For read txns: This thread/txn's slot table slot, or nullptr. */
      reader_slot_t *slot;
    } ro;
    struct {
      troika_t troika;
      pnl_t __restrict repnl; /* Reclaimed GC pages */
      struct {
        /* The list of reclaimed txn-ids from GC */
        txl_t __restrict retxl;
        txnid_t last_reclaimed; /* ID of last used record */
        uint64_t time_acc;
      } gc;
      bool prefault_write_activated;
#if MDBX_ENABLE_REFUND
      pgno_t loose_refund_wl /* FIXME: describe */;
#endif /* MDBX_ENABLE_REFUND */
      /* a sequence to spilling dirty page with LRU policy */
      unsigned dirtylru;
      /* dirtylist room: Dirty array size - dirty pages visible to this txn.
       * Includes ancestor txns' dirty pages not hidden by other txns'
       * dirty/spilled pages. Thus commit(nested txn) has room to merge
       * dirtylist into parent after freeing hidden parent pages. */
      size_t dirtyroom;
      /* For write txns: Modified pages. Sorted when not MDBX_WRITEMAP. */
      dpl_t *__restrict dirtylist;
      /* The list of pages that became unused during this transaction. */
      pnl_t __restrict retired_pages;
      /* The list of loose pages that became unused and may be reused
       * in this transaction, linked through `page_next()`. */
      page_t *__restrict loose_pages;
      /* Number of loose pages (wr.loose_pages) */
      size_t loose_count;
      union {
        struct {
          size_t least_removed;
          /* The sorted list of dirty pages we temporarily wrote to disk
           * because the dirty list was full. page numbers in here are
           * shifted left by 1, deleted slots have the LSB set. */
          pnl_t __restrict list;
        } spilled;
        size_t writemap_dirty_npages;
        size_t writemap_spilled_npages;
      };
      /* In write txns, next is located the array of cursors for each DB */
    } wr;
  };
};

#define CURSOR_STACK_SIZE (16 + MDBX_WORDBITS / 4)

struct MDBX_cursor {
  int32_t signature;
  union {
    /* Тут некоторые трюки/заморочки с тем чтобы во всех основных сценариях
     * проверять состояние курсора одной простой операцией сравнения,
     * и при этом ни на каплю не усложнять код итерации стека курсора.
     *
     * Поэтому решение такое:
     *  - поля flags и top сделаны знаковыми, а их отрицательные значения
     *    используются для обозначения не-установленного/не-инициализированного
     *    состояния курсора;
     *  - для инвалидации/сброса курсора достаточно записать отрицательное
     *    значение в объединенное поле top_and_flags;
     *  - все проверки состояния сводятся к сравнению одного из полей
     *    flags/snum/snum_and_flags, которые в зависимости от сценария,
     *    трактуются либо как знаковые, либо как безнаковые. */
    __anonymous_struct_extension__ struct {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
      int8_t flags;
      /* индекс вершины стека, меньше нуля для не-инициализированного курсора */
      int8_t top;
#else
      int8_t top;
      int8_t flags;
#endif
    };
    int16_t top_and_flags;
  };
  /* флаги проверки, в том числе биты для проверки типа листовых страниц. */
  uint8_t checking;

  /* Указывает на txn->dbi_state[] для DBI этого курсора.
   * Модификатор __restrict тут полезен и безопасен в текущем понимании,
   * так как пересечение возможно только с dbi_state транзакции,
   * и происходит по-чтению до последующего изменения/записи. */
  uint8_t *__restrict dbi_state;
  /* Связь списка отслеживания курсоров в транзакции */
  MDBX_txn *txn;
  /* Указывает на tree->dbs[] для DBI этого курсора. */
  tree_t *tree;
  /* Указывает на env->kvs[] для DBI этого курсора. */
  clc2_t *clc;
  subcur_t *__restrict subcur;
  page_t *pg[CURSOR_STACK_SIZE]; /* stack of pushed pages */
  indx_t ki[CURSOR_STACK_SIZE];  /* stack of page indices */
  MDBX_cursor *next;
  /* Состояние на момент старта вложенной транзакции */
  MDBX_cursor *backup;
};

struct inner_cursor {
  MDBX_cursor cursor;
  tree_t nested_tree;
};

struct cursor_couple {
  MDBX_cursor outer;
  void *userctx; /* User-settable context */
  subcur_t inner;
};

enum env_flags {
  /* Failed to update the meta page. Probably an I/O error. */
  ENV_FATAL_ERROR = INT32_MIN /* 0x80000000 */,
  /* Some fields are initialized. */
  ENV_ACTIVE = UINT32_C(0x20000000),
  /* me_txkey is set */
  ENV_TXKEY = UINT32_C(0x10000000),
  /* Legacy MDBX_MAPASYNC (prior v0.9) */
  DEPRECATED_MAPASYNC = UINT32_C(0x100000),
  /* Legacy MDBX_COALESCE (prior v0.12) */
  DEPRECATED_COALESCE = UINT32_C(0x2000000),
  ENV_INTERNAL_FLAGS = ENV_FATAL_ERROR | ENV_ACTIVE | ENV_TXKEY,
  /* Only a subset of the mdbx_env flags can be changed
   * at runtime. Changing other flags requires closing the
   * environment and re-opening it with the new flags. */
  ENV_CHANGEABLE_FLAGS = MDBX_SAFE_NOSYNC | MDBX_NOMETASYNC | DEPRECATED_MAPASYNC | MDBX_NOMEMINIT |
                         DEPRECATED_COALESCE | MDBX_PAGEPERTURB | MDBX_ACCEDE | MDBX_VALIDATION,
  ENV_CHANGELESS_FLAGS = MDBX_NOSUBDIR | MDBX_RDONLY | MDBX_WRITEMAP | MDBX_NOSTICKYTHREADS | MDBX_NORDAHEAD |
                         MDBX_LIFORECLAIM | MDBX_EXCLUSIVE,
  ENV_USABLE_FLAGS = ENV_CHANGEABLE_FLAGS | ENV_CHANGELESS_FLAGS
};

/* The database environment. */
struct MDBX_env {
  /* ----------------------------------------------------- mostly static part */
  mdbx_atomic_uint32_t signature;
  uint32_t flags;
  unsigned ps;          /* DB page size, initialized from me_os_psize */
  osal_mmap_t dxb_mmap; /* The main data file */
#define lazy_fd dxb_mmap.fd
  mdbx_filehandle_t dsync_fd, fd4meta;
#if defined(_WIN32) || defined(_WIN64)
  HANDLE dxb_lock_event;
#endif                  /* Windows */
  osal_mmap_t lck_mmap; /* The lock file */
  lck_t *lck;

  uint16_t leaf_nodemax;   /* max size of a leaf-node */
  uint16_t branch_nodemax; /* max size of a branch-node */
  uint16_t subpage_limit;
  uint16_t subpage_room_threshold;
  uint16_t subpage_reserve_prereq;
  uint16_t subpage_reserve_limit;
  atomic_pgno_t mlocked_pgno;
  uint8_t ps2ln;                                /* log2 of DB page size */
  int8_t stuck_meta;                            /* recovery-only: target meta page or less that zero */
  uint16_t merge_threshold, merge_threshold_gc; /* pages emptier than this are
                                                   candidates for merging */
  unsigned max_readers;                         /* size of the reader table */
  MDBX_dbi max_dbi;                             /* size of the DB table */
  uint32_t pid;                                 /* process ID of this env */
  osal_thread_key_t me_txkey;                   /* thread-key for readers */
  struct {                                      /* path to the DB files */
    pathchar_t *lck, *dxb, *specified;
    void *buffer;
  } pathname;
  void *page_auxbuf;              /* scratch area for DUPSORT put() */
  MDBX_txn *basal_txn;            /* preallocated write transaction */
  kvx_t *kvs;                     /* array of auxiliary key-value properties */
  uint8_t *__restrict dbs_flags;  /* array of flags from tree_t.flags */
  mdbx_atomic_uint32_t *dbi_seqs; /* array of dbi sequence numbers */
  unsigned maxgc_large1page;      /* Number of pgno_t fit in a single large page */
  unsigned maxgc_per_branch;
  uint32_t registered_reader_pid; /* have liveness lock in reader table */
  void *userctx;                  /* User-settable context */
  MDBX_hsr_func *hsr_callback;    /* Callback for kicking laggard readers */
  size_t madv_threshold;

  struct {
    unsigned dp_reserve_limit;
    unsigned rp_augment_limit;
    unsigned dp_limit;
    unsigned dp_initial;
    uint64_t gc_time_limit;
    uint8_t dp_loose_limit;
    uint8_t spill_max_denominator;
    uint8_t spill_min_denominator;
    uint8_t spill_parent4child_denominator;
    unsigned merge_threshold_16dot16_percent;
#if !(defined(_WIN32) || defined(_WIN64))
    unsigned writethrough_threshold;
#endif /* Windows */
    bool prefault_write;
    bool prefer_waf_insteadof_balance; /* Strive to minimize WAF instead of
                                          balancing pages fullment */
    bool need_dp_limit_adjust;
    struct {
      uint16_t limit;
      uint16_t room_threshold;
      uint16_t reserve_prereq;
      uint16_t reserve_limit;
    } subpage;

    union {
      unsigned all;
      /* tracks options with non-auto values but tuned by user */
      struct {
        unsigned dp_limit : 1;
        unsigned rp_augment_limit : 1;
        unsigned prefault_write : 1;
      } non_auto;
    } flags;
  } options;

  /* struct geo_in_bytes used for accepting db-geo params from user for the new
   * database creation, i.e. when mdbx_env_set_geometry() was called before
   * mdbx_env_open(). */
  struct {
    size_t lower;  /* minimal size of datafile */
    size_t upper;  /* maximal size of datafile */
    size_t now;    /* current size of datafile */
    size_t grow;   /* step to grow datafile */
    size_t shrink; /* threshold to shrink datafile */
  } geo_in_bytes;

#if MDBX_LOCKING == MDBX_LOCKING_SYSV
  union {
    key_t key;
    int semid;
  } me_sysv_ipc;
#endif /* MDBX_LOCKING == MDBX_LOCKING_SYSV */
  bool incore;

#if MDBX_ENABLE_DBI_LOCKFREE
  defer_free_item_t *defer_free;
#endif /* MDBX_ENABLE_DBI_LOCKFREE */

  /* -------------------------------------------------------------- debugging */

#if MDBX_DEBUG
  MDBX_assert_func *assert_func; /*  Callback for assertion failures */
#endif
#ifdef ENABLE_MEMCHECK
  int valgrind_handle;
#endif
#if defined(ENABLE_MEMCHECK) || defined(__SANITIZE_ADDRESS__)
  pgno_t poison_edge;
#endif /* ENABLE_MEMCHECK || __SANITIZE_ADDRESS__ */

#ifndef xMDBX_DEBUG_SPILLING
#define xMDBX_DEBUG_SPILLING 0
#endif
#if xMDBX_DEBUG_SPILLING == 2
  size_t debug_dirtied_est, debug_dirtied_act;
#endif /* xMDBX_DEBUG_SPILLING */

  /* --------------------------------------------------- mostly volatile part */

  MDBX_txn *txn; /* current write transaction */
  osal_fastmutex_t dbi_lock;
  unsigned n_dbi; /* number of DBs opened */

  unsigned shadow_reserve_len;
  page_t *__restrict shadow_reserve; /* list of malloc'ed blocks for re-use */

  osal_ioring_t ioring;

#if defined(_WIN32) || defined(_WIN64)
  osal_srwlock_t remap_guard;
  /* Workaround for LockFileEx and WriteFile multithread bug */
  CRITICAL_SECTION windowsbug_lock;
  char *pathname_char; /* cache of multi-byte representation of pathname
                             to the DB files */
#else
  osal_fastmutex_t remap_guard;
#endif

  /* ------------------------------------------------- stub for lck-less mode */
  mdbx_atomic_uint64_t lckless_placeholder[(sizeof(lck_t) + MDBX_CACHELINE_SIZE - 1) / sizeof(mdbx_atomic_uint64_t)];
};

/*----------------------------------------------------------------------------*/

/* pseudo-error code, not exposed outside libmdbx */
#define MDBX_NO_ROOT (MDBX_LAST_ADDED_ERRCODE + 33)

/* Number of slots in the reader table.
 * This value was chosen somewhat arbitrarily. The 61 is a prime number,
 * and such readers plus a couple mutexes fit into single 4KB page.
 * Applications should set the table size using mdbx_env_set_maxreaders(). */
#define DEFAULT_READERS 61

enum db_flags {
  DB_PERSISTENT_FLAGS =
      MDBX_REVERSEKEY | MDBX_DUPSORT | MDBX_INTEGERKEY | MDBX_DUPFIXED | MDBX_INTEGERDUP | MDBX_REVERSEDUP,

  /* mdbx_dbi_open() flags */
  DB_USABLE_FLAGS = DB_PERSISTENT_FLAGS | MDBX_CREATE | MDBX_DB_ACCEDE,

  DB_VALID = 0x80u /* DB handle is valid, for dbs_flags */,
  DB_POISON = 0x7fu /* update pending */,
  DB_INTERNAL_FLAGS = DB_VALID
};

#if !defined(__cplusplus) || CONSTEXPR_ENUM_FLAGS_OPERATIONS
MDBX_MAYBE_UNUSED static void static_checks(void) {
  STATIC_ASSERT(MDBX_WORDBITS == sizeof(void *) * CHAR_BIT);
  STATIC_ASSERT(UINT64_C(0x80000000) == (uint32_t)ENV_FATAL_ERROR);
  STATIC_ASSERT_MSG(INT16_MAX - CORE_DBS == MDBX_MAX_DBI, "Oops, MDBX_MAX_DBI or CORE_DBS?");
  STATIC_ASSERT_MSG((unsigned)(MDBX_DB_ACCEDE | MDBX_CREATE) ==
                        ((DB_USABLE_FLAGS | DB_INTERNAL_FLAGS) & (ENV_USABLE_FLAGS | ENV_INTERNAL_FLAGS)),
                    "Oops, some flags overlapped or wrong");
  STATIC_ASSERT_MSG((DB_INTERNAL_FLAGS & DB_USABLE_FLAGS) == 0, "Oops, some flags overlapped or wrong");
  STATIC_ASSERT_MSG((DB_PERSISTENT_FLAGS & ~DB_USABLE_FLAGS) == 0, "Oops, some flags overlapped or wrong");
  STATIC_ASSERT(DB_PERSISTENT_FLAGS <= UINT8_MAX);
  STATIC_ASSERT_MSG((ENV_INTERNAL_FLAGS & ENV_USABLE_FLAGS) == 0, "Oops, some flags overlapped or wrong");

  STATIC_ASSERT_MSG((txn_state_flags & (txn_rw_begin_flags | txn_ro_begin_flags)) == 0,
                    "Oops, some txn flags overlapped or wrong");
  STATIC_ASSERT_MSG(((txn_rw_begin_flags | txn_ro_begin_flags | txn_state_flags) & txn_shrink_allowed) == 0,
                    "Oops, some txn flags overlapped or wrong");

  STATIC_ASSERT(sizeof(reader_slot_t) == 32);
#if MDBX_LOCKING > 0
  STATIC_ASSERT(offsetof(lck_t, wrt_lock) % MDBX_CACHELINE_SIZE == 0);
  STATIC_ASSERT(offsetof(lck_t, rdt_lock) % MDBX_CACHELINE_SIZE == 0);
#else
  STATIC_ASSERT(offsetof(lck_t, cached_oldest) % MDBX_CACHELINE_SIZE == 0);
  STATIC_ASSERT(offsetof(lck_t, rdt_length) % MDBX_CACHELINE_SIZE == 0);
#endif /* MDBX_LOCKING */
#if FLEXIBLE_ARRAY_MEMBERS
  STATIC_ASSERT(offsetof(lck_t, rdt) % MDBX_CACHELINE_SIZE == 0);
#endif /* FLEXIBLE_ARRAY_MEMBERS */

#if FLEXIBLE_ARRAY_MEMBERS
  STATIC_ASSERT(NODESIZE == offsetof(node_t, payload));
  STATIC_ASSERT(PAGEHDRSZ == offsetof(page_t, entries));
#endif /* FLEXIBLE_ARRAY_MEMBERS */
  STATIC_ASSERT(sizeof(clc_t) == 3 * sizeof(void *));
  STATIC_ASSERT(sizeof(kvx_t) == 8 * sizeof(void *));

#if MDBX_WORDBITS == 64
#define KVX_SIZE_LN2 6
#else
#define KVX_SIZE_LN2 5
#endif
  STATIC_ASSERT(sizeof(kvx_t) == (1u << KVX_SIZE_LN2));
}
#endif /* Disabled for MSVC 19.0 (VisualStudio 2015) */

/******************************************************************************/

#include "node.h"

#include "dbi.h"

#include "cogs.h"

#include "cursor.h"

#include "dpl.h"

#include "gc.h"

#include "lck.h"

#include "meta.h"

#include "page-iov.h"

#include "spill.h"

#include "page-ops.h"

#include "tls.h"

#include "walk.h"

#include "sort.h"
