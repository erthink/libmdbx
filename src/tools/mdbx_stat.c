/* mdbx_stat.c - memory-mapped database status tool */

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

#include "../../mdbx.h"
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static void prstat(MDBX_stat *ms) {
  printf("  Page size: %u\n", ms->ms_psize);
  printf("  Tree depth: %u\n", ms->ms_depth);
  printf("  Branch pages: %" PRIuPTR "\n", ms->ms_branch_pages);
  printf("  Leaf pages: %" PRIuPTR "\n", ms->ms_leaf_pages);
  printf("  Overflow pages: %" PRIuPTR "\n", ms->ms_overflow_pages);
  printf("  Entries: %" PRIuPTR "\n", ms->ms_entries);
}

static void usage(char *prog) {
  fprintf(stderr,
          "usage: %s [-V] [-n] [-e] [-r[r]] [-f[f[f]]] [-a|-s subdb] dbpath\n",
          prog);
  exit(EXIT_FAILURE);
}

int main(int argc, char *argv[]) {
  int i, rc;
  MDB_env *env;
  MDB_txn *txn;
  MDB_dbi dbi;
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
  while ((i = getopt(argc, argv, "Vaefnrs:")) != EOF) {
    switch (i) {
    case 'V':
      printf("%s\n", MDBX_VERSION_STRING);
      exit(0);
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
      envflags |= MDB_NOSUBDIR;
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

  rc = mdbx_env_open(env, envname, envflags | MDB_RDONLY, 0664);
  if (rc) {
    fprintf(stderr, "mdbx_env_open failed, error %d %s\n", rc,
            mdbx_strerror(rc));
    goto env_close;
  }

  if (envinfo) {
    (void)mdbx_env_stat(env, &mst, sizeof(mst));
    (void)mdbx_env_info(env, &mei, sizeof(mei));
    printf("Environment Info\n");
    printf("  Map address: %p\n", mei.me_mapaddr);
    printf("  Map size: %" PRIuPTR "\n", mei.me_mapsize);
    printf("  Page size: %u\n", mst.ms_psize);
    printf("  Max pages: %" PRIuPTR "\n", mei.me_mapsize / mst.ms_psize);
    printf("  Number of pages used: %" PRIuPTR "\n", mei.me_last_pgno + 1);
    printf("  Last transaction ID: %" PRIuPTR "\n", mei.me_last_txnid);
    printf("  Tail transaction ID: %" PRIuPTR " (%" PRIiPTR ")\n",
           mei.me_tail_txnid, mei.me_tail_txnid - mei.me_last_txnid);
    printf("  Max readers: %u\n", mei.me_maxreaders);
    printf("  Number of readers used: %u\n", mei.me_numreaders);
  } else {
    /* LY: zap warnings from gcc */
    memset(&mst, 0, sizeof(mst));
    memset(&mei, 0, sizeof(mei));
  }

  if (rdrinfo) {
    printf("Reader Table Status\n");
    rc = mdbx_reader_list(env, (MDB_msg_func *)fputs, stdout);
    if (rdrinfo > 1) {
      int dead;
      mdbx_reader_check(env, &dead);
      printf("  %d stale readers cleared.\n", dead);
      rc = mdbx_reader_list(env, (MDB_msg_func *)fputs, stdout);
    }
    if (!(subname || alldbs || freinfo))
      goto env_close;
  }

  rc = mdbx_txn_begin(env, NULL, MDB_RDONLY, &txn);
  if (rc) {
    fprintf(stderr, "mdbx_txn_begin failed, error %d %s\n", rc,
            mdbx_strerror(rc));
    goto env_close;
  }

  if (freinfo) {
    MDB_cursor *cursor;
    MDB_val key, data;
    size_t pages = 0, *iptr;
    size_t reclaimable = 0;

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
    while ((rc = mdbx_cursor_get(cursor, &key, &data, MDB_NEXT)) == 0) {
      iptr = data.mv_data;
      pages += *iptr;
      if (envinfo && mei.me_tail_txnid > *(size_t *)key.mv_data)
        reclaimable += *iptr;
      if (freinfo > 1) {
        char *bad = "";
        size_t pg, prev;
        ssize_t i, j, span = 0;
        j = *iptr++;
        for (i = j, prev = 1; --i >= 0;) {
          pg = iptr[i];
          if (pg <= prev)
            bad = " [bad sequence]";
          prev = pg;
          pg += span;
          for (; i >= span && iptr[i - span] == pg; span++, pg++)
            ;
        }
        printf("    Transaction %" PRIuPTR ", %" PRIiPTR
               " pages, maxspan %" PRIiPTR "%s\n",
               *(size_t *)key.mv_data, j, span, bad);
        if (freinfo > 2) {
          for (--j; j >= 0;) {
            pg = iptr[j];
            for (span = 1; --j >= 0 && iptr[j] == pg + span; span++)
              ;
            if (span > 1)
              printf("     %9zu[%" PRIiPTR "]\n", pg, span);
            else
              printf("     %9zu\n", pg);
          }
        }
      }
    }
    mdbx_cursor_close(cursor);
    if (envinfo) {
      size_t value = mei.me_mapsize / mst.ms_psize;
      double percent = value / 100.0;
      printf("Page Allocation Info\n");
      printf("  Max pages: %9zu 100%%\n", value);

      value = mei.me_last_pgno + 1;
      printf("  Number of pages used: %" PRIuPTR " %.1f%%\n", value,
             value / percent);

      value = mei.me_mapsize / mst.ms_psize - (mei.me_last_pgno + 1);
      printf("  Remained: %" PRIuPTR " %.1f%%\n", value, value / percent);

      value = mei.me_last_pgno + 1 - pages;
      printf("  Used now: %" PRIuPTR " %.1f%%\n", value, value / percent);

      value = pages;
      printf("  Unallocated: %" PRIuPTR " %.1f%%\n", value, value / percent);

      value = pages - reclaimable;
      printf("  Detained: %" PRIuPTR " %.1f%%\n", value, value / percent);

      value = reclaimable;
      printf("  Reclaimable: %" PRIuPTR " %.1f%%\n", value, value / percent);

      value =
          mei.me_mapsize / mst.ms_psize - (mei.me_last_pgno + 1) + reclaimable;
      printf("  Available: %" PRIuPTR " %.1f%%\n", value, value / percent);
    } else
      printf("  Free pages: %" PRIuPTR "\n", pages);
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
    MDB_cursor *cursor;
    MDB_val key;

    rc = mdbx_cursor_open(txn, dbi, &cursor);
    if (rc) {
      fprintf(stderr, "mdbx_cursor_open failed, error %d %s\n", rc,
              mdbx_strerror(rc));
      goto txn_abort;
    }
    while ((rc = mdbx_cursor_get(cursor, &key, NULL, MDB_NEXT_NODUP)) == 0) {
      char *str;
      MDB_dbi db2;
      if (memchr(key.mv_data, '\0', key.mv_size))
        continue;
      str = malloc(key.mv_size + 1);
      memcpy(str, key.mv_data, key.mv_size);
      str[key.mv_size] = '\0';
      rc = mdbx_dbi_open(txn, str, 0, &db2);
      if (rc == MDB_SUCCESS)
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

  if (rc == MDB_NOTFOUND)
    rc = MDB_SUCCESS;

  mdbx_dbi_close(env, dbi);
txn_abort:
  mdbx_txn_abort(txn);
env_close:
  mdbx_env_close(env);

  return rc ? EXIT_FAILURE : EXIT_SUCCESS;
}
