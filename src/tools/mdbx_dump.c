/* mdbx_dump.c - memory-mapped database dump tool */

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

#ifdef _MSC_VER
#if _MSC_VER > 1800
#pragma warning(disable : 4464) /* relative include path contains '..' */
#endif
#pragma warning(disable : 4996) /* The POSIX name is deprecated... */
#endif                          /* _MSC_VER (warnings) */

#include "../bits.h"
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

  rc = mdbx_env_info(mdbx_txn_env(txn), &info, sizeof(info));
  if (rc)
    return rc;

  printf("VERSION=3\n");
  printf("format=%s\n", mode & PRINT ? "print" : "bytevalue");
  if (name)
    printf("database=%s\n", name);
  printf("type=btree\n");
  printf("mapsize=%" PRIu64 "\n", info.mi_mapsize);
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
          "usage: %s [-V] [-f output] [-l] [-n] [-p] [-a|-s subdb] dbpath\n",
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
  int alldbs = 0, envflags = 0, list = 0;

  if (argc < 2) {
    usage(prog);
  }

  /* -a: dump main DB and all subDBs
   * -s: dump only the named subDB
   * -n: use NOSUBDIR flag on env_open
   * -p: use printable characters
   * -f: write to file instead of stdout
   * -V: print version and exit
   * (default) dump only the main DB
   */
  while ((i = getopt(argc, argv, "af:lnps:V")) != EOF) {
    switch (i) {
    case 'V':
      printf("%s (%s, build %s)\n", mdbx_version.git.describe,
             mdbx_version.git.datetime, mdbx_build.datetime);
      exit(EXIT_SUCCESS);
      break;
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
        fprintf(stderr, "%s: %s: reopen: %s\n", prog, optarg, strerror(errno));
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
    mdbx_env_set_maxdbs(env, 2);
  }

  rc = mdbx_env_open(env, envname, envflags | MDBX_RDONLY, 0664);
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
      str = malloc(key.iov_len + 1);
      memcpy(str, key.iov_base, key.iov_len);
      str[key.iov_len] = '\0';
      rc = mdbx_dbi_open(txn, str, 0, &db2);
      if (rc == MDBX_SUCCESS) {
        if (list) {
          printf("%s\n", str);
          list++;
        } else {
          rc = dumpit(txn, db2, str);
          if (rc)
            break;
        }
        mdbx_dbi_close(env, db2);
      }
      free(str);
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
