/* mdbx_stat.c - memory-mapped database status tool */

/*
 * Copyright 2015-2018 Leonid Yuriev <leo@yuriev.ru>
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

#ifdef _MSC_VER
#if _MSC_VER > 1800
#pragma warning(disable : 4464) /* relative include path contains '..' */
#endif
#pragma warning(disable : 4996) /* The POSIX name is deprecated... */
#endif                          /* _MSC_VER (warnings) */

#include "../bits.h"

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

static void prstat(MDBX_stat *ms) {
  printf("  Pagesize: %u\n", ms->ms_psize);
  printf("  Tree depth: %u\n", ms->ms_depth);
  printf("  Branch pages: %" PRIu64 "\n", ms->ms_branch_pages);
  printf("  Leaf pages: %" PRIu64 "\n", ms->ms_leaf_pages);
  printf("  Overflow pages: %" PRIu64 "\n", ms->ms_overflow_pages);
  printf("  Entries: %" PRIu64 "\n", ms->ms_entries);
}

static void usage(char *prog) {
  fprintf(stderr,
          "usage: %s [-V] [-n] [-e] [-r[r]] [-f[f[f]]] [-a|-s subdb] dbpath\n",
          prog);
  exit(EXIT_FAILURE);
}

int main(int argc, char *argv[]) {
  int o, rc;
  MDBX_env *env;
  MDBX_txn *txn;
  MDBX_dbi dbi;
  MDBX_stat mst;
  MDBX_envinfo mei;
  char *prog = argv[0];
  char *envname;
  char *subname = NULL;
  int alldbs = 0, envinfo = 0, envflags = 0, freinfo = 0, rdrinfo = 0;

  if (argc < 2) {
    usage(prog);
  }

  /* -a: print stat of main DB and all subDBs
   * -s: print stat of only the named subDB
   * -e: print env info
   * -f: print freelist info
   * -r: print reader info
   * -n: use NOSUBDIR flag on env_open
   * -V: print version and exit
   * (default) print stat of only the main DB
   */
  while ((o = getopt(argc, argv, "Vaefnrs:")) != EOF) {
    switch (o) {
    case 'V':
      printf("%s (%s, build %s)\n", mdbx_version.git.describe,
             mdbx_version.git.datetime, mdbx_build.datetime);
      exit(EXIT_SUCCESS);
      break;
    case 'a':
      if (subname)
        usage(prog);
      alldbs++;
      break;
    case 'e':
      envinfo++;
      break;
    case 'f':
      freinfo++;
      break;
    case 'n':
      envflags |= MDBX_NOSUBDIR;
      break;
    case 'r':
      rdrinfo++;
      break;
    case 's':
      if (alldbs)
        usage(prog);
      subname = optarg;
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
  rc = mdbx_env_create(&env);
  if (rc) {
    fprintf(stderr, "mdbx_env_create failed, error %d %s\n", rc,
            mdbx_strerror(rc));
    return EXIT_FAILURE;
  }

  if (alldbs || subname) {
    mdbx_env_set_maxdbs(env, 4);
  }

  rc = mdbx_env_open(env, envname, envflags | MDBX_RDONLY, 0664);
  if (rc) {
    fprintf(stderr, "mdbx_env_open failed, error %d %s\n", rc,
            mdbx_strerror(rc));
    goto env_close;
  }

  if (envinfo) {
    (void)mdbx_env_stat(env, &mst, sizeof(mst));
    (void)mdbx_env_info(env, &mei, sizeof(mei));
    printf("Environment Info\n");
    printf("  Pagesize: %u\n", mst.ms_psize);
    if (mei.mi_geo.lower != mei.mi_geo.upper) {
      printf("  Dynamic datafile: %" PRIu64 "..%" PRIu64 " bytes (+%" PRIu64
             "/-%" PRIu64 "), %" PRIu64 "..%" PRIu64 " pages (+%" PRIu64
             "/-%" PRIu64 ")\n",
             mei.mi_geo.lower, mei.mi_geo.upper, mei.mi_geo.grow,
             mei.mi_geo.shrink, mei.mi_geo.lower / mst.ms_psize,
             mei.mi_geo.upper / mst.ms_psize, mei.mi_geo.grow / mst.ms_psize,
             mei.mi_geo.shrink / mst.ms_psize);
      printf("  Current datafile: %" PRIu64 " bytes, %" PRIu64 " pages\n",
             mei.mi_geo.current, mei.mi_geo.current / mst.ms_psize);
    } else {
      printf("  Fixed datafile: %" PRIu64 " bytes, %" PRIu64 " pages\n",
             mei.mi_geo.current, mei.mi_geo.current / mst.ms_psize);
    }
    printf("  Current mapsize: %" PRIu64 " bytes, %" PRIu64 " pages \n",
           mei.mi_mapsize, mei.mi_mapsize / mst.ms_psize);
    printf("  Number of pages used: %" PRIu64 "\n", mei.mi_last_pgno + 1);
    printf("  Last transaction ID: %" PRIu64 "\n", mei.mi_recent_txnid);
    printf("  Tail transaction ID: %" PRIu64 " (%" PRIi64 ")\n",
           mei.mi_latter_reader_txnid,
           mei.mi_latter_reader_txnid - mei.mi_recent_txnid);
    printf("  Max readers: %u\n", mei.mi_maxreaders);
    printf("  Number of readers used: %u\n", mei.mi_numreaders);
  } else {
    /* LY: zap warnings from gcc */
    memset(&mst, 0, sizeof(mst));
    memset(&mei, 0, sizeof(mei));
  }

  if (rdrinfo) {
    printf("Reader Table Status\n");
    rc = mdbx_reader_list(env, (MDBX_msg_func *)fputs, stdout);
    if (rdrinfo > 1) {
      int dead;
      mdbx_reader_check(env, &dead);
      printf("  %d stale readers cleared.\n", dead);
      rc = mdbx_reader_list(env, (MDBX_msg_func *)fputs, stdout);
    }
    if (!(subname || alldbs || freinfo))
      goto env_close;
  }

  rc = mdbx_txn_begin(env, NULL, MDBX_RDONLY, &txn);
  if (rc) {
    fprintf(stderr, "mdbx_txn_begin failed, error %d %s\n", rc,
            mdbx_strerror(rc));
    goto env_close;
  }

  if (freinfo) {
    MDBX_cursor *cursor;
    MDBX_val key, data;
    pgno_t pages = 0, *iptr;
    pgno_t reclaimable = 0;

    printf("Freelist Status\n");
    dbi = 0;
    rc = mdbx_cursor_open(txn, dbi, &cursor);
    if (rc) {
      fprintf(stderr, "mdbx_cursor_open failed, error %d %s\n", rc,
              mdbx_strerror(rc));
      goto txn_abort;
    }
    rc = mdbx_dbi_stat(txn, dbi, &mst, sizeof(mst));
    if (rc) {
      fprintf(stderr, "mdbx_dbi_stat failed, error %d %s\n", rc,
              mdbx_strerror(rc));
      goto txn_abort;
    }
    prstat(&mst);
    while ((rc = mdbx_cursor_get(cursor, &key, &data, MDBX_NEXT)) ==
           MDBX_SUCCESS) {
      if (user_break) {
        rc = MDBX_EINTR;
        break;
      }
      iptr = data.iov_base;
      const pgno_t number = *iptr++;

      pages += number;
      if (envinfo && mei.mi_latter_reader_txnid > *(size_t *)key.iov_base)
        reclaimable += number;

      if (freinfo > 1) {
        char *bad = "";
        pgno_t prev =
            MDBX_PNL_ASCENDING ? NUM_METAS - 1 : (pgno_t)mei.mi_last_pgno + 1;
        pgno_t span = 1;
        for (unsigned i = 0; i < number; ++i) {
          pgno_t pg = iptr[i];
          if (MDBX_PNL_DISORDERED(prev, pg))
            bad = " [bad sequence]";
          prev = pg;
          while (i + span < number &&
                 iptr[i + span] == (MDBX_PNL_ASCENDING ? pgno_add(pg, span)
                                                       : pgno_sub(pg, span)))
            ++span;
        }
        printf("    Transaction %" PRIaTXN ", %" PRIaPGNO
               " pages, maxspan %" PRIaPGNO "%s\n",
               *(txnid_t *)key.iov_base, number, span, bad);
        if (freinfo > 2) {
          for (unsigned i = 0; i < number; i += span) {
            const pgno_t pg = iptr[i];
            for (span = 1;
                 i + span < number &&
                 iptr[i + span] == (MDBX_PNL_ASCENDING ? pgno_add(pg, span)
                                                       : pgno_sub(pg, span));
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

    switch (rc) {
    case MDBX_SUCCESS:
    case MDBX_NOTFOUND:
      break;
    case MDBX_EINTR:
      fprintf(stderr, "Interrupted by signal/user\n");
      goto txn_abort;
    default:
      fprintf(stderr, "mdbx_cursor_get failed, error %d %s\n", rc,
              mdbx_strerror(rc));
      goto txn_abort;
    }

    if (envinfo) {
      uint64_t value = mei.mi_mapsize / mst.ms_psize;
      double percent = value / 100.0;
      printf("Page Allocation Info\n");
      printf("  Max pages: %" PRIu64 " 100%%\n", value);

      value = mei.mi_last_pgno + 1;
      printf("  Pages used: %" PRIu64 " %.1f%%\n", value, value / percent);

      value = mei.mi_mapsize / mst.ms_psize - (mei.mi_last_pgno + 1);
      printf("  Remained: %" PRIu64 " %.1f%%\n", value, value / percent);

      value = mei.mi_last_pgno + 1 - pages;
      printf("  Used now: %" PRIu64 " %.1f%%\n", value, value / percent);

      value = pages;
      printf("  Unallocated: %" PRIu64 " %.1f%%\n", value, value / percent);

      value = pages - reclaimable;
      printf("  Detained: %" PRIu64 " %.1f%%\n", value, value / percent);

      value = reclaimable;
      printf("  Reclaimable: %" PRIu64 " %.1f%%\n", value, value / percent);

      value =
          mei.mi_mapsize / mst.ms_psize - (mei.mi_last_pgno + 1) + reclaimable;
      printf("  Available: %" PRIu64 " %.1f%%\n", value, value / percent);
    } else
      printf("  Free pages: %" PRIaPGNO "\n", pages);
  }

  rc = mdbx_dbi_open(txn, subname, 0, &dbi);
  if (rc) {
    fprintf(stderr, "mdbx_open failed, error %d %s\n", rc, mdbx_strerror(rc));
    goto txn_abort;
  }

  rc = mdbx_dbi_stat(txn, dbi, &mst, sizeof(mst));
  if (rc) {
    fprintf(stderr, "mdbx_dbi_stat failed, error %d %s\n", rc,
            mdbx_strerror(rc));
    goto txn_abort;
  }
  printf("Status of %s\n", subname ? subname : "Main DB");
  prstat(&mst);

  if (alldbs) {
    MDBX_cursor *cursor;
    MDBX_val key;

    rc = mdbx_cursor_open(txn, dbi, &cursor);
    if (rc) {
      fprintf(stderr, "mdbx_cursor_open failed, error %d %s\n", rc,
              mdbx_strerror(rc));
      goto txn_abort;
    }
    while ((rc = mdbx_cursor_get(cursor, &key, NULL, MDBX_NEXT_NODUP)) == 0) {
      char *str;
      MDBX_dbi db2;
      if (memchr(key.iov_base, '\0', key.iov_len))
        continue;
      str = malloc(key.iov_len + 1);
      memcpy(str, key.iov_base, key.iov_len);
      str[key.iov_len] = '\0';
      rc = mdbx_dbi_open(txn, str, 0, &db2);
      if (rc == MDBX_SUCCESS)
        printf("Status of %s\n", str);
      free(str);
      if (rc)
        continue;
      rc = mdbx_dbi_stat(txn, db2, &mst, sizeof(mst));
      if (rc) {
        fprintf(stderr, "mdbx_dbi_stat failed, error %d %s\n", rc,
                mdbx_strerror(rc));
        goto txn_abort;
      }
      prstat(&mst);
      mdbx_dbi_close(env, db2);
    }
    mdbx_cursor_close(cursor);
  }

  if (rc == MDBX_NOTFOUND)
    rc = MDBX_SUCCESS;

  mdbx_dbi_close(env, dbi);
txn_abort:
  mdbx_txn_abort(txn);
env_close:
  mdbx_env_close(env);

  return rc ? EXIT_FAILURE : EXIT_SUCCESS;
}
