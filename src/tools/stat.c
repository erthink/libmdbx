/// \copyright SPDX-License-Identifier: Apache-2.0
/// \note Please refer to the COPYRIGHT file for explanations license change,
/// credits and acknowledgments.
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025
///
/// mdbx_stat.c - memory-mapped database status tool
///

#ifdef _MSC_VER
#if _MSC_VER > 1800
#pragma warning(disable : 4464) /* relative include path contains '..' */
#endif
#pragma warning(disable : 4996) /* The POSIX name is deprecated... */
#endif                          /* _MSC_VER (warnings) */

#define xMDBX_TOOLS /* Avoid using internal eASSERT() */
#include "essentials.h"

#if defined(_WIN32) || defined(_WIN64)
#include "wingetopt.h"

static volatile BOOL user_break;
static BOOL WINAPI ConsoleBreakHandlerRoutine(DWORD dwCtrlType) {
  (void)dwCtrlType;
  user_break = true;
  return true;
}

#else /* WINDOWS */

static volatile sig_atomic_t user_break;
static void signal_handler(int sig) {
  (void)sig;
  user_break = 1;
}

#endif /* !WINDOWS */

static void print_stat(MDBX_stat *ms) {
  printf("  Pagesize: %u\n", ms->ms_psize);
  printf("  Tree depth: %u\n", ms->ms_depth);
  printf("  Branch pages: %" PRIu64 "\n", ms->ms_branch_pages);
  printf("  Leaf pages: %" PRIu64 "\n", ms->ms_leaf_pages);
  printf("  Overflow pages: %" PRIu64 "\n", ms->ms_overflow_pages);
  printf("  Entries: %" PRIu64 "\n", ms->ms_entries);
}

static void usage(const char *prog) {
  fprintf(stderr,
          "usage: %s [-V] [-q] [-e] [-f[f[f]]] [-r[r]] [-a|-s table] dbpath\n"
          "  -V\t\tprint version and exit\n"
          "  -q\t\tbe quiet\n"
          "  -p\t\tshow statistics of page operations for current session\n"
          "  -e\t\tshow whole DB info\n"
          "  -f\t\tshow GC info\n"
          "  -r\t\tshow readers\n"
          "  -a\t\tprint stat of main DB and all tables\n"
          "  -s table\tprint stat of only the specified named table\n"
          "  \t\tby default print stat of only the main DB\n",
          prog);
  exit(EXIT_FAILURE);
}

static int reader_list_func(void *ctx, int num, int slot, mdbx_pid_t pid, mdbx_tid_t thread, uint64_t txnid,
                            uint64_t lag, size_t bytes_used, size_t bytes_retained) {
  (void)ctx;
  if (num == 1)
    printf("Reader Table\n"
           "   #\tslot\t%6s %*s %20s %10s %13s %13s\n",
           "pid", (int)sizeof(size_t) * 2, "thread", "txnid", "lag", "used", "retained");

  if (thread < (mdbx_tid_t)((intptr_t)MDBX_TID_TXN_OUSTED))
    printf(" %3d)\t[%d]\t%6" PRIdSIZE " %*" PRIxPTR, num, slot, (size_t)pid, (int)sizeof(size_t) * 2,
           (uintptr_t)thread);
  else
    printf(" %3d)\t[%d]\t%6" PRIdSIZE " %sed", num, slot, (size_t)pid,
           (thread == (mdbx_tid_t)((uintptr_t)MDBX_TID_TXN_PARKED)) ? "park" : "oust");

  if (txnid)
    printf(" %20" PRIu64 " %10" PRIu64 " %12.1fM %12.1fM\n", txnid, lag, bytes_used / 1048576.0,
           bytes_retained / 1048576.0);
  else
    printf(" %20s %10s %13s %13s\n", "-", "0", "0", "0");

  return user_break ? MDBX_RESULT_TRUE : MDBX_RESULT_FALSE;
}

const char *prog;
bool quiet = false;
static void error(const char *func, int rc) {
  if (!quiet)
    fprintf(stderr, "%s: %s() error %d %s\n", prog, func, rc, mdbx_strerror(rc));
}

static void logger(MDBX_log_level_t level, const char *function, int line, const char *fmt, va_list args) {
  static const char *const prefixes[] = {
      "!!!fatal: ", // 0 fatal
      " ! ",        // 1 error
      " ~ ",        // 2 warning
      "   ",        // 3 notice
      "   //",      // 4 verbose
  };
  if (level < MDBX_LOG_DEBUG) {
    if (function && line)
      fprintf(stderr, "%s", prefixes[level]);
    vfprintf(stderr, fmt, args);
  }
}

int main(int argc, char *argv[]) {
  int opt, rc;
  MDBX_env *env;
  MDBX_txn *txn;
  MDBX_dbi dbi;
  MDBX_envinfo mei;
  prog = argv[0];
  char *envname;
  char *table = nullptr;
  bool alldbs = false, envinfo = false, pgop = false;
  int freinfo = 0, rdrinfo = 0;

  if (argc < 2)
    usage(prog);

  while ((opt = getopt(argc, argv,
                       "V"
                       "q"
                       "p"
                       "a"
                       "e"
                       "f"
                       "n"
                       "r"
                       "s:")) != EOF) {
    switch (opt) {
    case 'V':
      printf("mdbx_stat version %d.%d.%d.%d\n"
             " - source: %s %s, commit %s, tree %s\n"
             " - anchor: %s\n"
             " - build: %s for %s by %s\n"
             " - flags: %s\n"
             " - options: %s\n",
             mdbx_version.major, mdbx_version.minor, mdbx_version.patch, mdbx_version.tweak, mdbx_version.git.describe,
             mdbx_version.git.datetime, mdbx_version.git.commit, mdbx_version.git.tree, mdbx_sourcery_anchor,
             mdbx_build.datetime, mdbx_build.target, mdbx_build.compiler, mdbx_build.flags, mdbx_build.options);
      return EXIT_SUCCESS;
    case 'q':
      quiet = true;
      break;
    case 'p':
      pgop = true;
      break;
    case 'a':
      if (table)
        usage(prog);
      alldbs = true;
      break;
    case 'e':
      envinfo = true;
      break;
    case 'f':
      freinfo += 1;
      break;
    case 'n':
      break;
    case 'r':
      rdrinfo += 1;
      break;
    case 's':
      if (alldbs)
        usage(prog);
      table = optarg;
      break;
    default:
      usage(prog);
    }
  }

  if (optind != argc - 1)
    usage(prog);

#if defined(_WIN32) || defined(_WIN64)
  SetConsoleCtrlHandler(ConsoleBreakHandlerRoutine, true);
#else
#ifdef SIGPIPE
  signal(SIGPIPE, signal_handler);
#endif
#ifdef SIGHUP
  signal(SIGHUP, signal_handler);
#endif
  signal(SIGINT, signal_handler);
  signal(SIGTERM, signal_handler);
#endif /* !WINDOWS */

  envname = argv[optind];
  envname = argv[optind];
  if (!quiet) {
    printf("mdbx_stat %s (%s, T-%s)\nRunning for %s...\n", mdbx_version.git.describe, mdbx_version.git.datetime,
           mdbx_version.git.tree, envname);
    fflush(nullptr);
    mdbx_setup_debug(MDBX_LOG_NOTICE, MDBX_DBG_DONTCHANGE, logger);
  }

  rc = mdbx_env_create(&env);
  if (unlikely(rc != MDBX_SUCCESS)) {
    error("mdbx_env_create", rc);
    return EXIT_FAILURE;
  }

  if (alldbs || table) {
    rc = mdbx_env_set_maxdbs(env, 2);
    if (unlikely(rc != MDBX_SUCCESS)) {
      error("mdbx_env_set_maxdbs", rc);
      goto env_close;
    }
  }

  rc = mdbx_env_open(env, envname, MDBX_RDONLY, 0);
  if (unlikely(rc != MDBX_SUCCESS)) {
    error("mdbx_env_open", rc);
    goto env_close;
  }

  rc = mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &txn);
  if (unlikely(rc != MDBX_SUCCESS)) {
    error("mdbx_txn_begin", rc);
    goto txn_abort;
  }

  if (envinfo || freinfo || pgop) {
    rc = mdbx_env_info_ex(env, txn, &mei, sizeof(mei));
    if (unlikely(rc != MDBX_SUCCESS)) {
      error("mdbx_env_info_ex", rc);
      goto txn_abort;
    }
  } else {
    /* LY: zap warnings from gcc */
    memset(&mei, 0, sizeof(mei));
  }

  if (pgop) {
    printf("Page Operations (for current session):\n");
    printf("      New: %8" PRIu64 "\t// quantity of a new pages added\n", mei.mi_pgop_stat.newly);
    printf("      CoW: %8" PRIu64 "\t// quantity of pages copied for altering\n", mei.mi_pgop_stat.cow);
    printf("    Clone: %8" PRIu64 "\t// quantity of parent's dirty pages "
           "clones for nested transactions\n",
           mei.mi_pgop_stat.clone);
    printf("    Split: %8" PRIu64 "\t// page splits during insertions or updates\n", mei.mi_pgop_stat.split);
    printf("    Merge: %8" PRIu64 "\t// page merges during deletions or updates\n", mei.mi_pgop_stat.merge);
    printf("    Spill: %8" PRIu64 "\t// quantity of spilled/ousted `dirty` "
           "pages during large transactions\n",
           mei.mi_pgop_stat.spill);
    printf("  Unspill: %8" PRIu64 "\t// quantity of unspilled/redone `dirty` "
           "pages during large transactions\n",
           mei.mi_pgop_stat.unspill);
    printf("      WOP: %8" PRIu64 "\t// number of explicit write operations (not a pages) to a disk\n",
           mei.mi_pgop_stat.wops);
    printf(" PreFault: %8" PRIu64 "\t// number of prefault write operations (not a pages)\n",
           mei.mi_pgop_stat.prefault);
    printf("  mInCore: %8" PRIu64 "\t// number of mincore() calls\n", mei.mi_pgop_stat.mincore);
    printf("    mSync: %8" PRIu64 "\t// number of explicit msync-to-disk operations (not a pages)\n",
           mei.mi_pgop_stat.msync);
    printf("    fSync: %8" PRIu64 "\t// number of explicit fsync-to-disk operations (not a pages)\n",
           mei.mi_pgop_stat.fsync);
  }

  if (envinfo) {
    printf("Environment Info\n");
    printf("  Pagesize: %u\n", mei.mi_dxb_pagesize);
    if (mei.mi_geo.lower != mei.mi_geo.upper) {
      printf("  Dynamic datafile: %" PRIu64 "..%" PRIu64 " bytes (+%" PRIu64 "/-%" PRIu64 "), %" PRIu64 "..%" PRIu64
             " pages (+%" PRIu64 "/-%" PRIu64 ")\n",
             mei.mi_geo.lower, mei.mi_geo.upper, mei.mi_geo.grow, mei.mi_geo.shrink,
             mei.mi_geo.lower / mei.mi_dxb_pagesize, mei.mi_geo.upper / mei.mi_dxb_pagesize,
             mei.mi_geo.grow / mei.mi_dxb_pagesize, mei.mi_geo.shrink / mei.mi_dxb_pagesize);
      printf("  Current mapsize: %" PRIu64 " bytes, %" PRIu64 " pages \n", mei.mi_mapsize,
             mei.mi_mapsize / mei.mi_dxb_pagesize);
      printf("  Current datafile: %" PRIu64 " bytes, %" PRIu64 " pages\n", mei.mi_geo.current,
             mei.mi_geo.current / mei.mi_dxb_pagesize);
#if defined(_WIN32) || defined(_WIN64)
      if (mei.mi_geo.shrink && mei.mi_geo.current != mei.mi_geo.upper)
        printf("                    WARNING: Due Windows system limitations a "
               "file couldn't\n                    be truncated while database "
               "is opened. So, the size of\n                    database file "
               "may by large than the database itself,\n                    "
               "until it will be closed or reopened in read-write mode.\n");
#endif
    } else {
      printf("  Fixed datafile: %" PRIu64 " bytes, %" PRIu64 " pages\n", mei.mi_geo.current,
             mei.mi_geo.current / mei.mi_dxb_pagesize);
    }
    printf("  Last transaction ID: %" PRIu64 "\n", mei.mi_recent_txnid);
    printf("  Latter reader transaction ID: %" PRIu64 " (%" PRIi64 ")\n", mei.mi_latter_reader_txnid,
           mei.mi_latter_reader_txnid - mei.mi_recent_txnid);
    printf("  Max readers: %u\n", mei.mi_maxreaders);
    printf("  Number of reader slots uses: %u\n", mei.mi_numreaders);
  }

  if (rdrinfo) {
    rc = mdbx_reader_list(env, reader_list_func, nullptr);
    if (MDBX_IS_ERROR(rc)) {
      error("mdbx_reader_list", rc);
      goto txn_abort;
    }
    if (rc == MDBX_RESULT_TRUE)
      printf("Reader Table is absent\n");
    else if (rc == MDBX_SUCCESS && rdrinfo > 1) {
      int dead;
      rc = mdbx_reader_check(env, &dead);
      if (MDBX_IS_ERROR(rc)) {
        error("mdbx_reader_check", rc);
        goto txn_abort;
      }
      if (rc == MDBX_RESULT_TRUE) {
        printf("  %d stale readers cleared.\n", dead);
        rc = mdbx_reader_list(env, reader_list_func, nullptr);
        if (rc == MDBX_RESULT_TRUE)
          printf("  Now Reader Table is empty\n");
      } else
        printf("  No stale readers.\n");
    }
    if (!(table || alldbs || freinfo))
      goto txn_abort;
  }

  if (freinfo) {
    printf("Garbage Collection\n");
    dbi = 0;
    MDBX_cursor *cursor;
    rc = mdbx_cursor_open(txn, dbi, &cursor);
    if (unlikely(rc != MDBX_SUCCESS)) {
      error("mdbx_cursor_open", rc);
      goto txn_abort;
    }

    MDBX_stat mst;
    rc = mdbx_dbi_stat(txn, dbi, &mst, sizeof(mst));
    if (unlikely(rc != MDBX_SUCCESS)) {
      error("mdbx_dbi_stat", rc);
      goto txn_abort;
    }
    print_stat(&mst);

    size_t gc_pages = 0, *iptr;
    size_t gc_reclaimable = 0;
    MDBX_val key, data;
    while (MDBX_SUCCESS == (rc = mdbx_cursor_get(cursor, &key, &data, MDBX_NEXT))) {
      if (user_break) {
        rc = MDBX_EINTR;
        break;
      }
      iptr = data.iov_base;
      const pgno_t number = *iptr++;

      gc_pages += number;
      if (envinfo && mei.mi_latter_reader_txnid > *(txnid_t *)key.iov_base)
        gc_reclaimable += number;

      if (freinfo > 1) {
        char *bad = "";
        pgno_t prev = MDBX_PNL_ASCENDING ? NUM_METAS - 1 : (pgno_t)mei.mi_last_pgno + 1;
        pgno_t span = 1;
        for (unsigned i = 0; i < number; ++i) {
          pgno_t pg = iptr[i];
          if (MDBX_PNL_DISORDERED(prev, pg))
            bad = " [bad sequence]";
          prev = pg;
          while (i + span < number && iptr[i + span] == (MDBX_PNL_ASCENDING ? pgno_add(pg, span) : pgno_sub(pg, span)))
            ++span;
        }
        printf("    Transaction %" PRIaTXN ", %" PRIaPGNO " pages, maxspan %" PRIaPGNO "%s\n", *(txnid_t *)key.iov_base,
               number, span, bad);
        if (freinfo > 2) {
          for (unsigned i = 0; i < number; i += span) {
            const pgno_t pg = iptr[i];
            for (span = 1;
                 i + span < number && iptr[i + span] == (MDBX_PNL_ASCENDING ? pgno_add(pg, span) : pgno_sub(pg, span));
                 ++span)
              ;
            if (span > 1)
              printf("     %9" PRIaPGNO "[%" PRIaPGNO "]\n", pg, span);
            else
              printf("     %9" PRIaPGNO "\n", pg);
          }
        }
      }
    }
    mdbx_cursor_close(cursor);
    cursor = nullptr;

    switch (rc) {
    case MDBX_SUCCESS:
    case MDBX_NOTFOUND:
      break;
    case MDBX_EINTR:
      if (!quiet)
        fprintf(stderr, "Interrupted by signal/user\n");
      goto txn_abort;
    default:
      error("mdbx_cursor_get", rc);
      goto txn_abort;
    }

    if (envinfo) {
      char buffer[64];

      puts("Page Usage");
      const size_t total_pages = mei.mi_mapsize / mei.mi_dxb_pagesize;
      printf("  Total: %" PRIuSIZE " 100%%\n", total_pages);

      const size_t backed_pages = mei.mi_geo.current / mei.mi_dxb_pagesize;
      printf("  Backed: %" PRIuSIZE " %s%%\n", backed_pages,
             mdbx_ratio2percents(backed_pages, total_pages, buffer, sizeof(buffer)));

      const size_t allocated_pages = mei.mi_last_pgno + 1;
      printf("  Allocated: %" PRIuSIZE " %s%%\n", allocated_pages,
             mdbx_ratio2percents(allocated_pages, total_pages, buffer, sizeof(buffer)));

      const size_t remained_pages = total_pages - allocated_pages;
      printf("  Remained: %" PRIuSIZE " %s%%\n", remained_pages,
             mdbx_ratio2percents(remained_pages, total_pages, buffer, sizeof(buffer)));

      const size_t used_pages = allocated_pages - gc_pages;
      printf("  Used: %" PRIuSIZE " %s%%\n", used_pages,
             mdbx_ratio2percents(used_pages, total_pages, buffer, sizeof(buffer)));

      printf("  GC: %" PRIuSIZE " %s%%\n", gc_pages,
             mdbx_ratio2percents(gc_pages, total_pages, buffer, sizeof(buffer)));

      printf("  Reclaimable: %" PRIuSIZE " %s%%\n", gc_reclaimable,
             mdbx_ratio2percents(gc_reclaimable, total_pages, buffer, sizeof(buffer)));

      const size_t gc_retained = gc_pages - gc_reclaimable;
      printf("  Retained: %" PRIuSIZE " %s%%\n", gc_retained,
             mdbx_ratio2percents(gc_retained, total_pages, buffer, sizeof(buffer)));

      const size_t available_pages = gc_reclaimable + remained_pages;
      printf("  Available: %" PRIuSIZE " %s%%\n", available_pages,
             mdbx_ratio2percents(available_pages, total_pages, buffer, sizeof(buffer)));
    } else
      printf("  GC: %" PRIuSIZE " pages\n", gc_pages);
  }

  rc = mdbx_dbi_open(txn, table, MDBX_DB_ACCEDE, &dbi);
  if (unlikely(rc != MDBX_SUCCESS)) {
    error("mdbx_dbi_open", rc);
    goto txn_abort;
  }

  MDBX_stat mst;
  rc = mdbx_dbi_stat(txn, dbi, &mst, sizeof(mst));
  if (unlikely(rc != MDBX_SUCCESS)) {
    error("mdbx_dbi_stat", rc);
    goto txn_abort;
  }
  printf("Status of %s\n", table ? table : "Main DB");
  print_stat(&mst);

  if (alldbs) {
    MDBX_cursor *cursor;
    rc = mdbx_cursor_open(txn, dbi, &cursor);
    if (unlikely(rc != MDBX_SUCCESS)) {
      error("mdbx_cursor_open", rc);
      goto txn_abort;
    }

    MDBX_val key;
    while (MDBX_SUCCESS == (rc = mdbx_cursor_get(cursor, &key, nullptr, MDBX_NEXT_NODUP))) {
      MDBX_dbi xdbi;
      if (memchr(key.iov_base, '\0', key.iov_len))
        continue;
      table = osal_malloc(key.iov_len + 1);
      memcpy(table, key.iov_base, key.iov_len);
      table[key.iov_len] = '\0';
      rc = mdbx_dbi_open(txn, table, MDBX_DB_ACCEDE, &xdbi);
      if (rc == MDBX_SUCCESS)
        printf("Status of %s\n", table);
      osal_free(table);
      if (unlikely(rc != MDBX_SUCCESS)) {
        if (rc == MDBX_INCOMPATIBLE)
          continue;
        error("mdbx_dbi_open", rc);
        goto txn_abort;
      }

      rc = mdbx_dbi_stat(txn, xdbi, &mst, sizeof(mst));
      if (unlikely(rc != MDBX_SUCCESS)) {
        error("mdbx_dbi_stat", rc);
        goto txn_abort;
      }
      print_stat(&mst);

      rc = mdbx_dbi_close(env, xdbi);
      if (unlikely(rc != MDBX_SUCCESS)) {
        error("mdbx_dbi_close", rc);
        goto txn_abort;
      }
    }
    mdbx_cursor_close(cursor);
    cursor = nullptr;
  }

  switch (rc) {
  case MDBX_SUCCESS:
  case MDBX_NOTFOUND:
    break;
  case MDBX_EINTR:
    if (!quiet)
      fprintf(stderr, "Interrupted by signal/user\n");
    break;
  default:
    if (unlikely(rc != MDBX_SUCCESS))
      error("mdbx_cursor_get", rc);
  }

  mdbx_dbi_close(env, dbi);
txn_abort:
  mdbx_txn_abort(txn);
env_close:
  mdbx_env_close(env);

  return rc ? EXIT_FAILURE : EXIT_SUCCESS;
}
