/* mdbx_load.c - memory-mapped database load tool */

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

#define PRINT 1
#define NOHDR 2
static int mode;

static char *subname = NULL;
static size_t lineno;
static int version;

static int dbi_flags;
static char *prog;
static int Eof;

static MDBX_envinfo envinfo;
static MDBX_val kbuf, dbuf;

#define STRLENOF(s) (sizeof(s) - 1)

typedef struct flagbit {
  int bit;
  char *name;
  int len;
} flagbit;

#define S(s) s, STRLENOF(s)

flagbit dbflags[] = {{MDBX_REVERSEKEY, S("reversekey")},
                     {MDBX_DUPSORT, S("dupsort")},
                     {MDBX_INTEGERKEY, S("integerkey")},
                     {MDBX_DUPFIXED, S("dupfixed")},
                     {MDBX_INTEGERDUP, S("integerdup")},
                     {MDBX_REVERSEDUP, S("reversedup")},
                     {0, NULL, 0}};

static void readhdr(void) {
  char *ptr;

  dbi_flags = 0;
  while (fgets(dbuf.iov_base, (int)dbuf.iov_len, stdin) != NULL) {
    lineno++;
    if (!strncmp(dbuf.iov_base, "db_pagesize=", STRLENOF("db_pagesize=")) ||
        !strncmp(dbuf.iov_base, "duplicates=", STRLENOF("duplicates="))) {
      /* LY: silently ignore information fields. */
      continue;
    } else if (!strncmp(dbuf.iov_base, "VERSION=", STRLENOF("VERSION="))) {
      version = atoi((char *)dbuf.iov_base + STRLENOF("VERSION="));
      if (version > 3) {
        fprintf(stderr, "%s: line %" PRIiSIZE ": unsupported VERSION %d\n",
                prog, lineno, version);
        exit(EXIT_FAILURE);
      }
    } else if (!strncmp(dbuf.iov_base, "HEADER=END", STRLENOF("HEADER=END"))) {
      break;
    } else if (!strncmp(dbuf.iov_base, "format=", STRLENOF("format="))) {
      if (!strncmp((char *)dbuf.iov_base + STRLENOF("FORMAT="), "print",
                   STRLENOF("print")))
        mode |= PRINT;
      else if (strncmp((char *)dbuf.iov_base + STRLENOF("FORMAT="), "bytevalue",
                       STRLENOF("bytevalue"))) {
        fprintf(stderr, "%s: line %" PRIiSIZE ": unsupported FORMAT %s\n", prog,
                lineno, (char *)dbuf.iov_base + STRLENOF("FORMAT="));
        exit(EXIT_FAILURE);
      }
    } else if (!strncmp(dbuf.iov_base, "database=", STRLENOF("database="))) {
      ptr = memchr(dbuf.iov_base, '\n', dbuf.iov_len);
      if (ptr)
        *ptr = '\0';
      if (subname)
        free(subname);
      subname = strdup((char *)dbuf.iov_base + STRLENOF("database="));
    } else if (!strncmp(dbuf.iov_base, "type=", STRLENOF("type="))) {
      if (strncmp((char *)dbuf.iov_base + STRLENOF("type="), "btree",
                  STRLENOF("btree"))) {
        fprintf(stderr, "%s: line %" PRIiSIZE ": unsupported type %s\n", prog,
                lineno, (char *)dbuf.iov_base + STRLENOF("type="));
        exit(EXIT_FAILURE);
      }
    } else if (!strncmp(dbuf.iov_base, "mapaddr=", STRLENOF("mapaddr="))) {
      int i;
      ptr = memchr(dbuf.iov_base, '\n', dbuf.iov_len);
      if (ptr)
        *ptr = '\0';
      void *unused;
      i = sscanf((char *)dbuf.iov_base + STRLENOF("mapaddr="), "%p", &unused);
      if (i != 1) {
        fprintf(stderr, "%s: line %" PRIiSIZE ": invalid mapaddr %s\n", prog,
                lineno, (char *)dbuf.iov_base + STRLENOF("mapaddr="));
        exit(EXIT_FAILURE);
      }
    } else if (!strncmp(dbuf.iov_base, "mapsize=", STRLENOF("mapsize="))) {
      int i;
      ptr = memchr(dbuf.iov_base, '\n', dbuf.iov_len);
      if (ptr)
        *ptr = '\0';
      i = sscanf((char *)dbuf.iov_base + STRLENOF("mapsize="), "%" PRIu64 "",
                 &envinfo.mi_mapsize);
      if (i != 1) {
        fprintf(stderr, "%s: line %" PRIiSIZE ": invalid mapsize %s\n", prog,
                lineno, (char *)dbuf.iov_base + STRLENOF("mapsize="));
        exit(EXIT_FAILURE);
      }
    } else if (!strncmp(dbuf.iov_base, "maxreaders=",
                        STRLENOF("maxreaders="))) {
      int i;
      ptr = memchr(dbuf.iov_base, '\n', dbuf.iov_len);
      if (ptr)
        *ptr = '\0';
      i = sscanf((char *)dbuf.iov_base + STRLENOF("maxreaders="), "%u",
                 &envinfo.mi_maxreaders);
      if (i != 1) {
        fprintf(stderr, "%s: line %" PRIiSIZE ": invalid maxreaders %s\n", prog,
                lineno, (char *)dbuf.iov_base + STRLENOF("maxreaders="));
        exit(EXIT_FAILURE);
      }
    } else {
      int i;
      for (i = 0; dbflags[i].bit; i++) {
        if (!strncmp(dbuf.iov_base, dbflags[i].name, dbflags[i].len) &&
            ((char *)dbuf.iov_base)[dbflags[i].len] == '=') {
          if (((char *)dbuf.iov_base)[dbflags[i].len + 1] == '1')
            dbi_flags |= dbflags[i].bit;
          break;
        }
      }
      if (!dbflags[i].bit) {
        ptr = memchr(dbuf.iov_base, '=', dbuf.iov_len);
        if (!ptr) {
          fprintf(stderr, "%s: line %" PRIiSIZE ": unexpected format\n", prog,
                  lineno);
          exit(EXIT_FAILURE);
        } else {
          *ptr = '\0';
          fprintf(stderr,
                  "%s: line %" PRIiSIZE ": unrecognized keyword ignored: %s\n",
                  prog, lineno, (char *)dbuf.iov_base);
        }
      }
    }
  }
}

static void badend(void) {
  fprintf(stderr, "%s: line %" PRIiSIZE ": unexpected end of input\n", prog,
          lineno);
}

static int unhex(unsigned char *c2) {
  int x, c;
  x = *c2++ & 0x4f;
  if (x & 0x40)
    x -= 55;
  c = x << 4;
  x = *c2 & 0x4f;
  if (x & 0x40)
    x -= 55;
  c |= x;
  return c;
}

static int readline(MDBX_val *out, MDBX_val *buf) {
  unsigned char *c1, *c2, *end;
  size_t len, l2;
  int c;

  if (!(mode & NOHDR)) {
    c = fgetc(stdin);
    if (c == EOF) {
      Eof = 1;
      return EOF;
    }
    if (c != ' ') {
      lineno++;
      if (fgets(buf->iov_base, (int)buf->iov_len, stdin) == NULL) {
      badend:
        Eof = 1;
        badend();
        return EOF;
      }
      if (c == 'D' && !strncmp(buf->iov_base, "ATA=END", STRLENOF("ATA=END")))
        return EOF;
      goto badend;
    }
  }
  if (fgets(buf->iov_base, (int)buf->iov_len, stdin) == NULL) {
    Eof = 1;
    return EOF;
  }
  lineno++;

  c1 = buf->iov_base;
  len = strlen((char *)c1);
  l2 = len;

  /* Is buffer too short? */
  while (c1[len - 1] != '\n') {
    buf->iov_base = realloc(buf->iov_base, buf->iov_len * 2);
    if (!buf->iov_base) {
      Eof = 1;
      fprintf(stderr, "%s: line %" PRIiSIZE ": out of memory, line too long\n",
              prog, lineno);
      return EOF;
    }
    c1 = buf->iov_base;
    c1 += l2;
    if (fgets((char *)c1, (int)buf->iov_len + 1, stdin) == NULL) {
      Eof = 1;
      badend();
      return EOF;
    }
    buf->iov_len *= 2;
    len = strlen((char *)c1);
    l2 += len;
  }
  c1 = c2 = buf->iov_base;
  len = l2;
  c1[--len] = '\0';
  end = c1 + len;

  if (mode & PRINT) {
    while (c2 < end) {
      if (*c2 == '\\') {
        if (c2[1] == '\\') {
          c1++;
          c2 += 2;
        } else {
          if (c2 + 3 > end || !isxdigit(c2[1]) || !isxdigit(c2[2])) {
            Eof = 1;
            badend();
            return EOF;
          }
          *c1++ = (char)unhex(++c2);
          c2 += 2;
        }
      } else {
        /* copies are redundant when no escapes were used */
        *c1++ = *c2++;
      }
    }
  } else {
    /* odd length not allowed */
    if (len & 1) {
      Eof = 1;
      badend();
      return EOF;
    }
    while (c2 < end) {
      if (!isxdigit(*c2) || !isxdigit(c2[1])) {
        Eof = 1;
        badend();
        return EOF;
      }
      *c1++ = (char)unhex(c2);
      c2 += 2;
    }
  }
  c2 = out->iov_base = buf->iov_base;
  out->iov_len = c1 - c2;

  return 0;
}

static void usage(void) {
  fprintf(stderr, "usage: %s [-V] [-f input] [-n] [-s name] [-N] [-T] dbpath\n",
          prog);
  exit(EXIT_FAILURE);
}

int main(int argc, char *argv[]) {
  int i, rc;
  MDBX_env *env = NULL;
  MDBX_txn *txn = NULL;
  MDBX_cursor *mc = NULL;
  MDBX_dbi dbi;
  char *envname = NULL;
  int envflags = 0, putflags = 0;

  prog = argv[0];

  if (argc < 2) {
    usage();
  }

  /* -f: load file instead of stdin
   * -n: use NOSUBDIR flag on env_open
   * -s: load into named subDB
   * -N: use NOOVERWRITE on puts
   * -T: read plaintext
   * -V: print version and exit
   */
  while ((i = getopt(argc, argv, "f:ns:NTV")) != EOF) {
    switch (i) {
    case 'V':
      printf("%s (%s, build %s)\n", mdbx_version.git.describe,
             mdbx_version.git.datetime, mdbx_build.datetime);
      exit(EXIT_SUCCESS);
      break;
    case 'f':
      if (freopen(optarg, "r", stdin) == NULL) {
        fprintf(stderr, "%s: %s: reopen: %s\n", prog, optarg, strerror(errno));
        exit(EXIT_FAILURE);
      }
      break;
    case 'n':
      envflags |= MDBX_NOSUBDIR;
      break;
    case 's':
      subname = strdup(optarg);
      break;
    case 'N':
      putflags = MDBX_NOOVERWRITE | MDBX_NODUPDATA;
      break;
    case 'T':
      mode |= NOHDR | PRINT;
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

  dbuf.iov_len = 4096;
  dbuf.iov_base = malloc(dbuf.iov_len);

  if (!(mode & NOHDR))
    readhdr();

  envname = argv[optind];
  rc = mdbx_env_create(&env);
  if (rc) {
    fprintf(stderr, "mdbx_env_create failed, error %d %s\n", rc,
            mdbx_strerror(rc));
    return EXIT_FAILURE;
  }

  mdbx_env_set_maxdbs(env, 2);

  if (envinfo.mi_maxreaders)
    mdbx_env_set_maxreaders(env, envinfo.mi_maxreaders);

  if (envinfo.mi_mapsize) {
    if (envinfo.mi_mapsize > SIZE_MAX) {
      fprintf(stderr, "mdbx_env_set_mapsize failed, error %d %s\n", rc,
              mdbx_strerror(MDBX_TOO_LARGE));
      return EXIT_FAILURE;
    }
    mdbx_env_set_mapsize(env, (size_t)envinfo.mi_mapsize);
  }

#ifdef MDBX_FIXEDMAP
  if (info.mi_mapaddr)
    envflags |= MDBX_FIXEDMAP;
#endif

  rc = mdbx_env_open(env, envname, envflags, 0664);
  if (rc) {
    fprintf(stderr, "mdbx_env_open failed, error %d %s\n", rc,
            mdbx_strerror(rc));
    goto env_close;
  }

  kbuf.iov_len = mdbx_env_get_maxkeysize(env) * 2 + 2;
  kbuf.iov_base = malloc(kbuf.iov_len);

  while (!Eof) {
    if (user_break) {
      rc = MDBX_EINTR;
      break;
    }

    MDBX_val key, data;
    int batch = 0;

    rc = mdbx_txn_begin(env, NULL, 0, &txn);
    if (rc) {
      fprintf(stderr, "mdbx_txn_begin failed, error %d %s\n", rc,
              mdbx_strerror(rc));
      goto env_close;
    }

    rc = mdbx_dbi_open(txn, subname, dbi_flags | MDBX_CREATE, &dbi);
    if (rc) {
      fprintf(stderr, "mdbx_open failed, error %d %s\n", rc, mdbx_strerror(rc));
      goto txn_abort;
    }

    rc = mdbx_cursor_open(txn, dbi, &mc);
    if (rc) {
      fprintf(stderr, "mdbx_cursor_open failed, error %d %s\n", rc,
              mdbx_strerror(rc));
      goto txn_abort;
    }

    while (1) {
      rc = readline(&key, &kbuf);
      if (rc) /* rc == EOF */
        break;

      rc = readline(&data, &dbuf);
      if (rc) {
        fprintf(stderr, "%s: line %" PRIiSIZE ": failed to read key value\n",
                prog, lineno);
        goto txn_abort;
      }

      rc = mdbx_cursor_put(mc, &key, &data, putflags);
      if (rc == MDBX_KEYEXIST && putflags)
        continue;
      if (rc) {
        fprintf(stderr, "mdbx_cursor_put failed, error %d %s\n", rc,
                mdbx_strerror(rc));
        goto txn_abort;
      }
      batch++;
      if (batch == 100) {
        rc = mdbx_txn_commit(txn);
        if (rc) {
          fprintf(stderr, "%s: line %" PRIiSIZE ": txn_commit: %s\n", prog,
                  lineno, mdbx_strerror(rc));
          goto env_close;
        }
        rc = mdbx_txn_begin(env, NULL, 0, &txn);
        if (rc) {
          fprintf(stderr, "mdbx_txn_begin failed, error %d %s\n", rc,
                  mdbx_strerror(rc));
          goto env_close;
        }
        rc = mdbx_cursor_open(txn, dbi, &mc);
        if (rc) {
          fprintf(stderr, "mdbx_cursor_open failed, error %d %s\n", rc,
                  mdbx_strerror(rc));
          goto txn_abort;
        }
        batch = 0;
      }
    }
    rc = mdbx_txn_commit(txn);
    txn = NULL;
    if (rc) {
      fprintf(stderr, "%s: line %" PRIiSIZE ": txn_commit: %s\n", prog, lineno,
              mdbx_strerror(rc));
      goto env_close;
    }
    mdbx_dbi_close(env, dbi);
    if (!(mode & NOHDR))
      readhdr();
  }

txn_abort:
  mdbx_txn_abort(txn);
env_close:
  mdbx_env_close(env);

  return rc ? EXIT_FAILURE : EXIT_SUCCESS;
}
