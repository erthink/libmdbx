/* mdbx_copy.c - memory-mapped database backup tool */

/*
 * Copyright 2015-2022 Leonid Yuriev <leo@yuriev.ru>
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

#define xMDBX_TOOLS /* Avoid using internal eASSERT() */
#include "internals.h"

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

static void usage(const char *prog) {
  fprintf(
      stderr,
      "usage: %s [-V] [-q] [-c] [-u|U] src_path [dest_path]\n"
      "  -V\t\tprint version and exit\n"
      "  -q\t\tbe quiet\n"
      "  -c\t\tenable compactification (skip unused pages)\n"
      "  -u\t\twarmup database before copying\n"
      "  -U\t\twarmup and try lock database pages in memory before copying\n"
      "  src_path\tsource database\n"
      "  dest_path\tdestination (stdout if not specified)\n",
      prog);
  exit(EXIT_FAILURE);
}

int main(int argc, char *argv[]) {
  int rc;
  MDBX_env *env = NULL;
  const char *progname = argv[0], *act;
  unsigned flags = MDBX_RDONLY;
  unsigned cpflags = 0;
  bool quiet = false;
  bool warmup = false;
  MDBX_warmup_flags_t warmup_flags = MDBX_warmup_default;

  for (; argc > 1 && argv[1][0] == '-'; argc--, argv++) {
    if (argv[1][1] == 'n' && argv[1][2] == '\0')
      flags |= MDBX_NOSUBDIR;
    else if (argv[1][1] == 'c' && argv[1][2] == '\0')
      cpflags |= MDBX_CP_COMPACT;
    else if (argv[1][1] == 'q' && argv[1][2] == '\0')
      quiet = true;
    else if (argv[1][1] == 'u' && argv[1][2] == '\0')
      warmup = true;
    else if (argv[1][1] == 'U' && argv[1][2] == '\0') {
      warmup = true;
      warmup_flags =
          MDBX_warmup_force | MDBX_warmup_touchlimit | MDBX_warmup_lock;
    } else if ((argv[1][1] == 'h' && argv[1][2] == '\0') ||
               strcmp(argv[1], "--help") == 0)
      usage(progname);
    else if (argv[1][1] == 'V' && argv[1][2] == '\0') {
      printf("mdbx_copy version %d.%d.%d.%d\n"
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
    } else
      argc = 0;
  }

  if (argc < 2 || argc > 3)
    usage(progname);

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

  if (!quiet) {
    fprintf((argc == 2) ? stderr : stdout,
            "mdbx_copy %s (%s, T-%s)\nRunning for copy %s to %s...\n",
            mdbx_version.git.describe, mdbx_version.git.datetime,
            mdbx_version.git.tree, argv[1], (argc == 2) ? "stdout" : argv[2]);
    fflush(NULL);
  }

  act = "opening environment";
  rc = mdbx_env_create(&env);
  if (rc == MDBX_SUCCESS)
    rc = mdbx_env_open(env, argv[1], flags, 0);

  if (rc == MDBX_SUCCESS && warmup) {
    act = "warming up";
    rc = mdbx_env_warmup(env, nullptr, warmup_flags, 3600 * 65536);
  }

  if (!MDBX_IS_ERROR(rc)) {
    act = "copying";
    if (argc == 2) {
      mdbx_filehandle_t fd;
#if defined(_WIN32) || defined(_WIN64)
      fd = GetStdHandle(STD_OUTPUT_HANDLE);
#else
      fd = fileno(stdout);
#endif
      rc = mdbx_env_copy2fd(env, fd, cpflags);
    } else
      rc = mdbx_env_copy(env, argv[2], cpflags);
  }
  if (rc)
    fprintf(stderr, "%s: %s failed, error %d (%s)\n", progname, act, rc,
            mdbx_strerror(rc));
  mdbx_env_close(env);

  return rc ? EXIT_FAILURE : EXIT_SUCCESS;
}
