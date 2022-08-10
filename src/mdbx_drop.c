/* mdbx_drop.c - memory-mapped database delete tool */

/*
 * Copyright 2021 Leonid Yuriev <leo@yuriev.ru>
 * and other libmdbx authors: please see AUTHORS file.
 *
 * Copyright 2016-2022 Howard Chu, Symas Corp.
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

#define xMDBX_TOOLS /* Avoid using internal eASSERT() */
#include "internals.h"

#include <ctype.h>

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

static char *prog;
bool quiet = false;
static void usage(void) {
  fprintf(stderr,
          "usage: %s [-V] [-q] [-d] [-s name] dbpath\n"
          "  -V\t\tprint version and exit\n"
          "  -q\t\tbe quiet\n"
          "  -d\t\tdelete the specified database, don't just empty it\n"
          "  -s name\tdrop the specified named subDB\n"
          "  \t\tby default empty the main DB\n",
          prog);
  exit(EXIT_FAILURE);
}

static void error(const char *func, int rc) {
  if (!quiet)
    fprintf(stderr, "%s: %s() error %d %s\n", prog, func, rc,
            mdbx_strerror(rc));
}

int main(int argc, char *argv[]) {
  int i, rc;
  MDBX_env *env;
  MDBX_txn *txn;
  MDBX_dbi dbi;
  char *envname = nullptr;
  char *subname = nullptr;
  int envflags = MDBX_ACCEDE;
  bool delete = false;

  prog = argv[0];
  if (argc < 2)
    usage();

  while ((i = getopt(argc, argv,
                     "d"
                     "s:"
                     "n"
                     "q"
                     "V")) != EOF) {
    switch (i) {
    case 'V':
      printf("mdbx_drop version %d.%d.%d.%d\n"
             " - source: %s %s, commit %s, tree %s\n"
             " - anchor: %s\n"
             " - build: %s for %s by %s\n"
             " - flags: %s\n"
             " - options: %s\n",
             mdbx_version.major, mdbx_version.minor, mdbx_version.release,
             mdbx_version.revision, mdbx_version.git.describe,
             mdbx_version.git.datetime, mdbx_version.git.commit,
             mdbx_version.git.tree, mdbx_sourcery_anchor, mdbx_build.datetime,
             mdbx_build.target, mdbx_build.compiler, mdbx_build.flags,
             mdbx_build.options);
      return EXIT_SUCCESS;
    case 'q':
      quiet = true;
      break;
    case 'd':
      delete = true;
      break;
    case 'n':
      break;
    case 's':
      subname = optarg;
      break;
    default:
      usage();
    }
  }

  if (optind != argc - 1)
    usage();

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
  if (!quiet) {
    printf("mdbx_drop %s (%s, T-%s)\nRunning for %s/%s...\n",
           mdbx_version.git.describe, mdbx_version.git.datetime,
           mdbx_version.git.tree, envname, subname ? subname : "@MAIN");
    fflush(nullptr);
  }

  rc = mdbx_env_create(&env);
  if (unlikely(rc != MDBX_SUCCESS)) {
    error("mdbx_env_create", rc);
    return EXIT_FAILURE;
  }

  if (subname) {
    rc = mdbx_env_set_maxdbs(env, 2);
    if (unlikely(rc != MDBX_SUCCESS)) {
      error("mdbx_env_set_maxdbs", rc);
      goto env_close;
    }
  }

  rc = mdbx_env_open(env, envname, envflags, 0);
  if (unlikely(rc != MDBX_SUCCESS)) {
    error("mdbx_env_open", rc);
    goto env_close;
  }

  rc = mdbx_txn_begin(env, NULL, 0, &txn);
  if (unlikely(rc != MDBX_SUCCESS)) {
    error("mdbx_txn_begin", rc);
    goto env_close;
  }

  rc = mdbx_dbi_open(txn, subname, MDBX_DB_ACCEDE, &dbi);
  if (unlikely(rc != MDBX_SUCCESS)) {
    error("mdbx_dbi_open", rc);
    goto txn_abort;
  }

  rc = mdbx_drop(txn, dbi, delete);
  if (unlikely(rc != MDBX_SUCCESS)) {
    error("mdbx_drop", rc);
    goto txn_abort;
  }

  rc = mdbx_txn_commit(txn);
  if (unlikely(rc != MDBX_SUCCESS)) {
    error("mdbx_txn_commit", rc);
    goto txn_abort;
  }
  txn = nullptr;

txn_abort:
  if (txn)
    mdbx_txn_abort(txn);
env_close:
  mdbx_env_close(env);

  return rc ? EXIT_FAILURE : EXIT_SUCCESS;
}
