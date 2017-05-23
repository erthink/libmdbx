/* mdbx_copy.c - memory-mapped database backup tool */

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
 * <http://www.OpenLDAP.org/license.html>. */

#include "../../mdbx.h"
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

static void sighandle(int sig) { (void)sig; }

int main(int argc, char *argv[]) {
  int rc;
  MDBX_env *env = NULL;
  const char *progname = argv[0], *act;
  unsigned flags = MDBX_RDONLY;
  unsigned cpflags = 0;

  for (; argc > 1 && argv[1][0] == '-'; argc--, argv++) {
    if (argv[1][1] == 'n' && argv[1][2] == '\0')
      flags |= MDBX_NOSUBDIR;
    else if (argv[1][1] == 'c' && argv[1][2] == '\0')
      cpflags |= MDBX_CP_COMPACT;
    else if (argv[1][1] == 'V' && argv[1][2] == '\0') {
      printf("%s (%s, build %s)\n", mdbx_version.git.describe,
             mdbx_version.git.datetime, mdbx_build.datetime);
      exit(EXIT_SUCCESS);
    } else
      argc = 0;
  }

  if (argc < 2 || argc > 3) {
    fprintf(stderr, "usage: %s [-V] [-c] [-n] srcpath [dstpath]\n", progname);
    exit(EXIT_FAILURE);
  }

#ifdef SIGPIPE
  signal(SIGPIPE, sighandle);
#endif
#ifdef SIGHUP
  signal(SIGHUP, sighandle);
#endif
  signal(SIGINT, sighandle);
  signal(SIGTERM, sighandle);

  act = "opening environment";
  rc = mdbx_env_create(&env);
  if (rc == MDBX_SUCCESS) {
    rc = mdbx_env_open(env, argv[1], flags, 0640);
  }
  if (rc == MDBX_SUCCESS) {
    act = "copying";
    if (argc == 2)
      rc = mdbx_env_copy2fd(env, STDOUT_FILENO, cpflags);
    else
      rc = mdbx_env_copy(env, argv[2], cpflags);
  }
  if (rc)
    fprintf(stderr, "%s: %s failed, error %d (%s)\n", progname, act, rc,
            mdbx_strerror(rc));
  mdbx_env_close(env);

  return rc ? EXIT_FAILURE : EXIT_SUCCESS;
}
