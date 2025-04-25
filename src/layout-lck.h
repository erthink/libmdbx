/// \copyright SPDX-License-Identifier: Apache-2.0
/// \note Please refer to the COPYRIGHT file for explanations license change,
/// credits and acknowledgments.
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025

#pragma once

#include "essentials.h"

/* The version number for a database's lockfile format. */
#define MDBX_LOCK_VERSION 6

#if MDBX_LOCKING == MDBX_LOCKING_WIN32FILES

#define MDBX_LCK_SIGN UINT32_C(0xF10C)
typedef void osal_ipclock_t;
#elif MDBX_LOCKING == MDBX_LOCKING_SYSV

#define MDBX_LCK_SIGN UINT32_C(0xF18D)
typedef mdbx_pid_t osal_ipclock_t;

#elif MDBX_LOCKING == MDBX_LOCKING_POSIX2001 || MDBX_LOCKING == MDBX_LOCKING_POSIX2008

#define MDBX_LCK_SIGN UINT32_C(0x8017)
typedef pthread_mutex_t osal_ipclock_t;

#elif MDBX_LOCKING == MDBX_LOCKING_POSIX1988

#define MDBX_LCK_SIGN UINT32_C(0xFC29)
typedef sem_t osal_ipclock_t;

#else
#error "FIXME"
#endif /* MDBX_LOCKING */

/* Статистика профилирования работы GC */
typedef struct gc_prof_stat {
  /* Монотонное время по "настенным часам"
   * затраченное на чтение и поиск внутри GC */
  uint64_t rtime_monotonic;
  /* Процессорное время в режим пользователя
   * на подготовку страниц извлекаемых из GC, включая подкачку с диска. */
  uint64_t xtime_cpu;
  /* Количество итераций чтения-поиска внутри GC при выделении страниц */
  uint32_t rsteps;
  /* Количество запросов на выделение последовательностей страниц,
   * т.е. когда запрашивает выделение больше одной страницы */
  uint32_t xpages;
  /* Счетчик выполнения по медленному пути (slow path execution count) */
  uint32_t spe_counter;
  /* page faults (hard page faults) */
  uint32_t majflt;
  /* Для разборок с pnl_merge() */
  struct {
    uint64_t time;
    uint64_t volume;
    uint32_t calls;
  } pnl_merge;
} gc_prof_stat_t;

/* Statistics of pages operations for all transactions,
 * including incomplete and aborted. */
typedef struct pgops {
  mdbx_atomic_uint64_t newly;   /* Quantity of a new pages added */
  mdbx_atomic_uint64_t cow;     /* Quantity of pages copied for update */
  mdbx_atomic_uint64_t clone;   /* Quantity of parent's dirty pages clones
                                   for nested transactions */
  mdbx_atomic_uint64_t split;   /* Page splits */
  mdbx_atomic_uint64_t merge;   /* Page merges */
  mdbx_atomic_uint64_t spill;   /* Quantity of spilled dirty pages */
  mdbx_atomic_uint64_t unspill; /* Quantity of unspilled/reloaded pages */
  mdbx_atomic_uint64_t wops;    /* Number of explicit write operations (not a pages) to a disk */
  mdbx_atomic_uint64_t msync;   /* Number of explicit msync/flush-to-disk operations */
  mdbx_atomic_uint64_t fsync;   /* Number of explicit fsync/flush-to-disk operations */

  mdbx_atomic_uint64_t prefault; /* Number of prefault write operations */
  mdbx_atomic_uint64_t mincore;  /* Number of mincore() calls */

  mdbx_atomic_uint32_t incoherence; /* number of https://libmdbx.dqdkfa.ru/dead-github/issues/269
                                       caught */
  mdbx_atomic_uint32_t reserved;

  /* Статистика для профилирования GC.
   * Логически эти данные, возможно, стоит вынести в другую структуру,
   * но разница будет сугубо косметическая. */
  struct {
    /* Затраты на поддержку данных пользователя */
    gc_prof_stat_t work;
    /* Затраты на поддержку и обновления самой GC */
    gc_prof_stat_t self;
    /* Итераций обновления GC,
     * больше 1 если были повторы/перезапуски */
    uint32_t wloops;
    /* Итерации слияния записей GC */
    uint32_t coalescences;
    /* Уничтожения steady-точек фиксации в MDBX_UTTERLY_NOSYNC */
    uint32_t wipes;
    /* Сбросы данные на диск вне MDBX_UTTERLY_NOSYNC */
    uint32_t flushes;
    /* Попытки пнуть тормозящих читателей */
    uint32_t kicks;
  } gc_prof;
} pgop_stat_t;

/* Reader Lock Table
 *
 * Readers don't acquire any locks for their data access. Instead, they
 * simply record their transaction ID in the reader table. The reader
 * mutex is needed just to find an empty slot in the reader table. The
 * slot's address is saved in thread-specific data so that subsequent
 * read transactions started by the same thread need no further locking to
 * proceed.
 *
 * If MDBX_NOSTICKYTHREADS is set, the slot address is not saved in
 * thread-specific data. No reader table is used if the database is on a
 * read-only filesystem.
 *
 * Since the database uses multi-version concurrency control, readers don't
 * actually need any locking. This table is used to keep track of which
 * readers are using data from which old transactions, so that we'll know
 * when a particular old transaction is no longer in use. Old transactions
 * that have discarded any data pages can then have those pages reclaimed
 * for use by a later write transaction.
 *
 * The lock table is constructed such that reader slots are aligned with the
 * processor's cache line size. Any slot is only ever used by one thread.
 * This alignment guarantees that there will be no contention or cache
 * thrashing as threads update their own slot info, and also eliminates
 * any need for locking when accessing a slot.
 *
 * A writer thread will scan every slot in the table to determine the oldest
 * outstanding reader transaction. Any freed pages older than this will be
 * reclaimed by the writer. The writer doesn't use any locks when scanning
 * this table. This means that there's no guarantee that the writer will
 * see the most up-to-date reader info, but that's not required for correct
 * operation - all we need is to know the upper bound on the oldest reader,
 * we don't care at all about the newest reader. So the only consequence of
 * reading stale information here is that old pages might hang around a
 * while longer before being reclaimed. That's actually good anyway, because
 * the longer we delay reclaiming old pages, the more likely it is that a
 * string of contiguous pages can be found after coalescing old pages from
 * many old transactions together. */

/* The actual reader record, with cacheline padding. */
typedef struct reader_slot {
  /* Current Transaction ID when this transaction began, or INVALID_TXNID.
   * Multiple readers that start at the same time will probably have the
   * same ID here. Again, it's not important to exclude them from
   * anything; all we need to know is which version of the DB they
   * started from so we can avoid overwriting any data used in that
   * particular version. */
  atomic_txnid_t txnid;

  /* The information we store in a single slot of the reader table.
   * In addition to a transaction ID, we also record the process and
   * thread ID that owns a slot, so that we can detect stale information,
   * e.g. threads or processes that went away without cleaning up.
   *
   * NOTE: We currently don't check for stale records.
   * We simply re-init the table when we know that we're the only process
   * opening the lock file. */

  /* Псевдо thread_id для пометки вытесненных читающих транзакций. */
#define MDBX_TID_TXN_OUSTED (UINT64_MAX - 1)

  /* Псевдо thread_id для пометки припаркованных читающих транзакций. */
#define MDBX_TID_TXN_PARKED UINT64_MAX

  /* The thread ID of the thread owning this txn. */
  mdbx_atomic_uint64_t tid;

  /* The process ID of the process owning this reader txn. */
  mdbx_atomic_uint32_t pid;

  /* The number of pages used in the reader's MVCC snapshot,
   * i.e. the value of meta->geometry.first_unallocated and
   * txn->geo.first_unallocated */
  atomic_pgno_t snapshot_pages_used;
  /* Number of retired pages at the time this reader starts transaction. So,
   * at any time the difference meta.pages_retired -
   * reader.snapshot_pages_retired will give the number of pages which this
   * reader restraining from reuse. */
  mdbx_atomic_uint64_t snapshot_pages_retired;
} reader_slot_t;

/* The header for the reader table (a memory-mapped lock file). */
typedef struct shared_lck {
  /* Stamp identifying this as an MDBX file.
   * It must be set to MDBX_MAGIC with MDBX_LOCK_VERSION. */
  uint64_t magic_and_version;

  /* Format of this lock file. Must be set to MDBX_LOCK_FORMAT. */
  uint32_t os_and_format;

  /* Flags which environment was opened. */
  mdbx_atomic_uint32_t envmode;

  /* Threshold of un-synced-with-disk pages for auto-sync feature,
   * zero means no-threshold, i.e. auto-sync is disabled. */
  atomic_pgno_t autosync_threshold;

  /* Low 32-bit of txnid with which meta-pages was synced,
   * i.e. for sync-polling in the MDBX_NOMETASYNC mode. */
#define MDBX_NOMETASYNC_LAZY_UNK (UINT32_MAX / 3)
#define MDBX_NOMETASYNC_LAZY_FD (MDBX_NOMETASYNC_LAZY_UNK + UINT32_MAX / 8)
#define MDBX_NOMETASYNC_LAZY_WRITEMAP (MDBX_NOMETASYNC_LAZY_UNK - UINT32_MAX / 8)
  mdbx_atomic_uint32_t meta_sync_txnid;

  /* Period for timed auto-sync feature, i.e. at the every steady checkpoint
   * the mti_unsynced_timeout sets to the current_time + autosync_period.
   * The time value is represented in a suitable system-dependent form, for
   * example clock_gettime(CLOCK_BOOTTIME) or clock_gettime(CLOCK_MONOTONIC).
   * Zero means timed auto-sync is disabled. */
  mdbx_atomic_uint64_t autosync_period;

  /* Marker to distinguish uniqueness of DB/CLK. */
  mdbx_atomic_uint64_t bait_uniqueness;

  /* Paired counter of processes that have mlock()ed part of mmapped DB.
   * The (mlcnt[0] - mlcnt[1]) > 0 means at least one process
   * lock at least one page, so therefore madvise() could return EINVAL. */
  mdbx_atomic_uint32_t mlcnt[2];

  MDBX_ALIGNAS(MDBX_CACHELINE_SIZE) /* cacheline ----------------------------*/

  /* Statistics of costly ops of all (running, completed and aborted)
   * transactions */
  pgop_stat_t pgops;

  MDBX_ALIGNAS(MDBX_CACHELINE_SIZE) /* cacheline ----------------------------*/

#if MDBX_LOCKING > 0
  /* Write transaction lock. */
  osal_ipclock_t wrt_lock;
#endif /* MDBX_LOCKING > 0 */

  atomic_txnid_t cached_oldest;

  /* Timestamp of entering an out-of-sync state. Value is represented in a
   * suitable system-dependent form, for example clock_gettime(CLOCK_BOOTTIME)
   * or clock_gettime(CLOCK_MONOTONIC). */
  mdbx_atomic_uint64_t eoos_timestamp;

  /* Number un-synced-with-disk pages for auto-sync feature. */
  mdbx_atomic_uint64_t unsynced_pages;

  /* Timestamp of the last readers check. */
  mdbx_atomic_uint64_t readers_check_timestamp;

  /* Number of page which was discarded last time by madvise(DONTNEED). */
  atomic_pgno_t discarded_tail;

  /* Shared anchor for tracking readahead edge and enabled/disabled status. */
  pgno_t readahead_anchor;

  /* Shared cache for mincore() results */
  struct {
    pgno_t begin[4];
    uint64_t mask[4];
  } mincore_cache;

  MDBX_ALIGNAS(MDBX_CACHELINE_SIZE) /* cacheline ----------------------------*/

#if MDBX_LOCKING > 0
  /* Readeaders table lock. */
  osal_ipclock_t rdt_lock;
#endif /* MDBX_LOCKING > 0 */

  /* The number of slots that have been used in the reader table.
   * This always records the maximum count, it is not decremented
   * when readers release their slots. */
  mdbx_atomic_uint32_t rdt_length;
  mdbx_atomic_uint32_t rdt_refresh_flag;

#if FLEXIBLE_ARRAY_MEMBERS
  MDBX_ALIGNAS(MDBX_CACHELINE_SIZE) /* cacheline ----------------------------*/
  reader_slot_t rdt[] /* dynamic size */;

/* Lockfile format signature: version, features and field layout */
#define MDBX_LOCK_FORMAT                                                                                               \
  (MDBX_LCK_SIGN * 27733 + (unsigned)sizeof(reader_slot_t) * 13 +                                                      \
   (unsigned)offsetof(reader_slot_t, snapshot_pages_used) * 251 + (unsigned)offsetof(lck_t, cached_oldest) * 83 +      \
   (unsigned)offsetof(lck_t, rdt_length) * 37 + (unsigned)offsetof(lck_t, rdt) * 29)
#endif /* FLEXIBLE_ARRAY_MEMBERS */
} lck_t;

#define MDBX_LOCK_MAGIC ((MDBX_MAGIC << 8) + MDBX_LOCK_VERSION)

#define MDBX_READERS_LIMIT 32767
