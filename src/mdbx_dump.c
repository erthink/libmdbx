/* mdbx_dump.c - memory-mapped database dump tool */

/*
 * Copyright 2015-2020 Leonid Yuriev <leo@yuriev.ru>
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

#define MDBX_TOOLS /* Avoid using internal mdbx_assert() */
#include "internals.h"

#include <ctype.h>

#define PRINT 1
static int mode;

typedef struct flagbit {
  int bit;
  char *name;
} flagbit;

flagbit dbflags[] = {{MDBX_REVERSEKEY, "reversekey"},
                     {MDBX_DUPSORT, "dupsort"},
                     {MDBX_INTEGERKEY, "integerkey"},
                     {MDBX_DUPFIXED, "dupfixed"},
                     {MDBX_INTEGERDUP, "integerdup"},
                     {MDBX_REVERSEDUP, "reversedup"},
                     {0, NULL}};

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

static const char hexc[] = "0123456789abcdef";

static void dumpbyte(unsigned char c) {
  putchar(hexc[c >> 4]);
  putchar(hexc[c & 0xf]);
}

static void text(MDBX_val *v) {
  unsigned char *c, *end;

  putchar(' ');
  c = v->iov_base;
  end = c + v->iov_len;
  while (c < end) {
    if (isprint(*c) && *c != '\\') {
      putchar(*c);
    } else {
      putchar('\\');
      dumpbyte(*c);
    }
    c++;
  }
  putchar('\n');
}

static void dumpval(MDBX_val *v) {
  unsigned char *c, *end;

  putchar(' ');
  c = v->iov_base;
  end = c + v->iov_len;
  while (c < end) {
    dumpbyte(*c++);
  }
  putchar('\n');
}

/* Dump in BDB-compatible format */
static int dumpit(MDBX_txn *txn, MDBX_dbi dbi, char *name) {
  MDBX_cursor *mc;
  MDBX_stat ms;
  MDBX_val key, data;
  MDBX_envinfo info;
  unsigned int flags;
  int rc, i;

  rc = mdbx_dbi_flags(txn, dbi, &flags);
  if (rc)
    return rc;

  rc = mdbx_dbi_stat(txn, dbi, &ms, sizeof(ms));
  if (rc)
    return rc;

  rc = mdbx_env_info_ex(mdbx_txn_env(txn), txn, &info, sizeof(info));
  if (rc)
    return rc;

  printf("VERSION=3\n");
  printf("format=%s\n", mode & PRINT ? "print" : "bytevalue");
  if (name)
    printf("database=%s\n", name);
  printf("type=btree\n");
  printf("mapsize=%" PRIu64 "\n", info.mi_geo.upper);
  printf("maxreaders=%u\n", info.mi_maxreaders);

  for (i = 0; dbflags[i].bit; i++)
    if (flags & dbflags[i].bit)
      printf("%s=1\n", dbflags[i].name);

  printf("db_pagesize=%d\n", ms.ms_psize);
  printf("HEADER=END\n");

  rc = mdbx_cursor_open(txn, dbi, &mc);
  if (rc)
    return rc;

  while ((rc = mdbx_cursor_get(mc, &key, &data, MDBX_NEXT)) == MDBX_SUCCESS) {
    if (user_break) {
      rc = MDBX_EINTR;
      break;
    }
    if (mode & PRINT) {
      text(&key);
      text(&data);
    } else {
      dumpval(&key);
      dumpval(&data);
    }
  }
  printf("DATA=END\n");
  if (rc == MDBX_NOTFOUND)
    rc = MDBX_SUCCESS;

  return rc;
}

static void usage(char *prog) {
  fprintf(stderr,
          "usage: %s [-V] [-q] [-f file] [-l] [-p] [-a|-s subdb] [-r] [-n] "
          "dbpath\n"
          "  -V\t\tprint version and exit\n"
          "  -q\t\tbe quiet\n"
          "  -f\t\twrite to file instead of stdout\n"
          "  -l\t\tlist subDBs and exit\n"
          "  -p\t\tuse printable characters\n"
          "  -a\t\tdump main DB and all subDBs,\n"
          "    \t\tby default dump only the main DB\n"
          "  -s\t\tdump only the named subDB\n"
          "  -r\t\trescure mode (ignore errors to dump corrupted DB)\n"
          "  -n\t\tNOSUBDIR mode for open\n",
          prog);
  exit(EXIT_FAILURE);
}

int main(int argc, char *argv[]) {
  int i, rc;
  MDBX_env *env;
  MDBX_txn *txn;
  MDBX_dbi dbi;
  char *prog = argv[0];
  char *envname;
  char *subname = NULL;
  int alldbs = 0, envflags = 0, list = 0, quiet = 0, rescue = 0;

  if (argc < 2)
    usage(prog);

  while ((i = getopt(argc, argv, "af:lnps:Vrq")) != EOF) {
    switch (i) {
    case 'V':
      printf("mdbx_dump version %d.%d.%d.%d\n"
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
    case 'l':
      list = 1;
      /*FALLTHROUGH*/;
      __fallthrough;
    case 'a':
      if (subname)
        usage(prog);
      alldbs++;
      break;
    case 'f':
      if (freopen(optarg, "w", stdout) == NULL) {
        fprintf(stderr, "%s: %s: reopen: %s\n", prog, optarg,
                mdbx_strerror(errno));
        exit(EXIT_FAILURE);
      }
      break;
    case 'n':
      envflags |= MDBX_NOSUBDIR;
      break;
    case 'p':
      mode |= PRINT;
      break;
    case 's':
      if (alldbs)
        usage(prog);
      subname = optarg;
      break;
    case 'q':
      quiet = 1;
      break;
    case 'r':
      rescue = 1;
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
  if (!quiet) {
    fprintf(stderr, "mdbx_dump %s (%s, T-%s)\nRunning for %s...\n",
            mdbx_version.git.describe, mdbx_version.git.datetime,
            mdbx_version.git.tree, envname);
    fflush(NULL);
  }

  rc = mdbx_env_create(&env);
  if (rc) {
    fprintf(stderr, "mdbx_env_create failed, error %d %s\n", rc,
            mdbx_strerror(rc));
    return EXIT_FAILURE;
  }

  if (alldbs || subname) {
    mdbx_env_set_maxdbs(env, 2);
  }

  rc = mdbx_env_open(
      env, envname,
      envflags | (rescue ? MDBX_RDONLY | MDBX_EXCLUSIVE : MDBX_RDONLY), 0);
  if (rc) {
    fprintf(stderr, "mdbx_env_open failed, error %d %s\n", rc,
            mdbx_strerror(rc));
    goto env_close;
  }

  rc = mdbx_txn_begin(env, NULL, MDBX_RDONLY, &txn);
  if (rc) {
    fprintf(stderr, "mdbx_txn_begin failed, error %d %s\n", rc,
            mdbx_strerror(rc));
    goto env_close;
  }

  rc = mdbx_dbi_open(txn, subname, 0, &dbi);
  if (rc) {
    fprintf(stderr, "mdbx_open failed, error %d %s\n", rc, mdbx_strerror(rc));
    goto txn_abort;
  }

  if (alldbs) {
    MDBX_cursor *cursor;
    MDBX_val key;
    int count = 0;

    rc = mdbx_cursor_open(txn, dbi, &cursor);
    if (rc) {
      fprintf(stderr, "mdbx_cursor_open failed, error %d %s\n", rc,
              mdbx_strerror(rc));
      goto txn_abort;
    }
    while ((rc = mdbx_cursor_get(cursor, &key, NULL, MDBX_NEXT_NODUP)) == 0) {
      if (user_break) {
        rc = MDBX_EINTR;
        break;
      }
      char *str;
      MDBX_dbi db2;
      if (memchr(key.iov_base, '\0', key.iov_len))
        continue;
      count++;
      str = mdbx_malloc(key.iov_len + 1);
      memcpy(str, key.iov_base, key.iov_len);
      str[key.iov_len] = '\0';
      rc = mdbx_dbi_open(txn, str, 0, &db2);
      if (rc == MDBX_SUCCESS) {
        if (list) {
          printf("%s\n", str);
          list++;
        } else {
          rc = dumpit(txn, db2, str);
          if (rc) {
            if (!rescue)
              break;
            fprintf(stderr, "%s: %s: ignore %s for `%s` and continue\n", prog,
                    envname, mdbx_strerror(rc), str);
            /* Here is a hack for rescue mode, don't do that:
             *  - we should restart transaction in case error due
             *    database corruption;
             *  - but we won't close cursor, reopen and re-positioning it
             *    for new a transaction;
             *  - this is possible since DB is opened in read-only exclusive
             *    mode and transaction is the same, i.e. has the same address
             *    and so on. */
            rc = mdbx_txn_reset(txn);
            if (rc) {
              fprintf(stderr, "mdbx_txn_reset failed, error %d %s\n", rc,
                      mdbx_strerror(rc));
              goto env_close;
            }
            rc = mdbx_txn_renew(txn);
            if (rc) {
              fprintf(stderr, "mdbx_txn_renew failed, error %d %s\n", rc,
                      mdbx_strerror(rc));
              goto env_close;
            }
          }
        }
        mdbx_dbi_close(env, db2);
      }
      mdbx_free(str);
      if (rc)
        continue;
    }
    mdbx_cursor_close(cursor);
    if (!count) {
      fprintf(stderr, "%s: %s does not contain multiple databases\n", prog,
              envname);
      rc = MDBX_NOTFOUND;
    } else if (rc == MDBX_INCOMPATIBLE) {
      /* LY: the record it not a named sub-db. */
      rc = MDBX_SUCCESS;
    }
  } else {
    rc = dumpit(txn, dbi, subname);
  }
  if (rc && rc != MDBX_NOTFOUND)
    fprintf(stderr, "%s: %s: %s\n", prog, envname, mdbx_strerror(rc));

  mdbx_dbi_close(env, dbi);
txn_abort:
  mdbx_txn_abort(txn);
env_close:
  mdbx_env_close(env);

  return rc ? EXIT_FAILURE : EXIT_SUCCESS;
}
