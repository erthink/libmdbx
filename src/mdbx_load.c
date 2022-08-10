/* mdbx_load.c - memory-mapped database load tool */

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
static bool quiet = false;
static size_t lineno;
static void error(const char *func, int rc) {
  if (!quiet) {
    if (lineno)
      fprintf(stderr, "%s: at input line %" PRIiSIZE ": %s() error %d, %s\n",
              prog, lineno, func, rc, mdbx_strerror(rc));
    else
      fprintf(stderr, "%s: %s() error %d %s\n", prog, func, rc,
              mdbx_strerror(rc));
  }
}

static char *valstr(char *line, const char *item) {
  const size_t len = strlen(item);
  if (strncmp(line, item, len) != 0)
    return nullptr;
  if (line[len] != '=') {
    if (line[len] > ' ')
      return nullptr;
    if (!quiet)
      fprintf(stderr,
              "%s: line %" PRIiSIZE ": unexpected line format for '%s'\n", prog,
              lineno, item);
    exit(EXIT_FAILURE);
  }
  char *ptr = strchr(line, '\n');
  if (ptr)
    *ptr = '\0';
  return line + len + 1;
}

static bool valnum(char *line, const char *item, uint64_t *value) {
  char *str = valstr(line, item);
  if (!str)
    return false;

  char *end = nullptr;
  *value = strtoull(str, &end, 0);
  if (end && *end) {
    if (!quiet)
      fprintf(stderr,
              "%s: line %" PRIiSIZE ": unexpected number format for '%s'\n",
              prog, lineno, item);
    exit(EXIT_FAILURE);
  }
  return true;
}

static bool valbool(char *line, const char *item, bool *value) {
  uint64_t u64;
  if (!valnum(line, item, &u64))
    return false;

  if (u64 > 1) {
    if (!quiet)
      fprintf(stderr, "%s: line %" PRIiSIZE ": unexpected value for '%s'\n",
              prog, lineno, item);
    exit(EXIT_FAILURE);
  }
  *value = u64 != 0;
  return true;
}

/*----------------------------------------------------------------------------*/

static char *subname = nullptr;
static int dbi_flags;
static txnid_t txnid;
static uint64_t sequence;
static MDBX_canary canary;
static MDBX_envinfo envinfo;

#define PRINT 1
#define NOHDR 2
#define GLOBAL 4
static int mode = GLOBAL;

static MDBX_val kbuf, dbuf;

#define STRLENOF(s) (sizeof(s) - 1)

typedef struct flagbit {
  unsigned bit;
  unsigned len;
  char *name;
} flagbit;

#define S(s) STRLENOF(s), s

flagbit dbflags[] = {
    {MDBX_REVERSEKEY, S("reversekey")}, {MDBX_DUPSORT, S("duplicates")},
    {MDBX_DUPSORT, S("dupsort")},       {MDBX_INTEGERKEY, S("integerkey")},
    {MDBX_DUPFIXED, S("dupfixed")},     {MDBX_INTEGERDUP, S("integerdup")},
    {MDBX_REVERSEDUP, S("reversedup")}, {0, 0, nullptr}};

static int readhdr(void) {
  /* reset parameters */
  if (subname) {
    free(subname);
    subname = nullptr;
  }
  dbi_flags = 0;
  txnid = 0;
  sequence = 0;

  while (true) {
    errno = 0;
    if (fgets(dbuf.iov_base, (int)dbuf.iov_len, stdin) == nullptr)
      return errno ? errno : EOF;
    if (user_break)
      return MDBX_EINTR;

    lineno++;
    uint64_t u64;

    if (valnum(dbuf.iov_base, "VERSION", &u64)) {
      if (u64 != 3) {
        if (!quiet)
          fprintf(stderr,
                  "%s: line %" PRIiSIZE ": unsupported value %" PRIu64
                  " for %s\n",
                  prog, lineno, u64, "VERSION");
        exit(EXIT_FAILURE);
      }
      continue;
    }

    if (valnum(dbuf.iov_base, "db_pagesize", &u64)) {
      if (!(mode & GLOBAL) && envinfo.mi_dxb_pagesize != u64) {
        if (!quiet)
          fprintf(stderr,
                  "%s: line %" PRIiSIZE ": ignore value %" PRIu64
                  " for '%s' in non-global context\n",
                  prog, lineno, u64, "db_pagesize");
      } else if (u64 < MDBX_MIN_PAGESIZE || u64 > MDBX_MAX_PAGESIZE) {
        if (!quiet)
          fprintf(stderr,
                  "%s: line %" PRIiSIZE ": ignore unsupported value %" PRIu64
                  " for %s\n",
                  prog, lineno, u64, "db_pagesize");
      } else
        envinfo.mi_dxb_pagesize = (uint32_t)u64;
      continue;
    }

    char *str = valstr(dbuf.iov_base, "format");
    if (str) {
      if (strcmp(str, "print") == 0) {
        mode |= PRINT;
        continue;
      }
      if (strcmp(str, "bytevalue") == 0) {
        mode &= ~PRINT;
        continue;
      }
      if (!quiet)
        fprintf(stderr,
                "%s: line %" PRIiSIZE ": unsupported value '%s' for %s\n", prog,
                lineno, str, "format");
      exit(EXIT_FAILURE);
    }

    str = valstr(dbuf.iov_base, "database");
    if (str) {
      if (*str) {
        free(subname);
        subname = osal_strdup(str);
        if (!subname) {
          if (!quiet)
            perror("strdup()");
          exit(EXIT_FAILURE);
        }
      }
      continue;
    }

    str = valstr(dbuf.iov_base, "type");
    if (str) {
      if (strcmp(str, "btree") != 0) {
        if (!quiet)
          fprintf(stderr,
                  "%s: line %" PRIiSIZE ": unsupported value '%s' for %s\n",
                  prog, lineno, str, "type");
        free(subname);
        exit(EXIT_FAILURE);
      }
      continue;
    }

    if (valnum(dbuf.iov_base, "mapaddr", &u64)) {
      if (u64) {
        if (!quiet)
          fprintf(stderr,
                  "%s: line %" PRIiSIZE ": ignore unsupported value 0x%" PRIx64
                  " for %s\n",
                  prog, lineno, u64, "mapaddr");
      }
      continue;
    }

    if (valnum(dbuf.iov_base, "mapsize", &u64)) {
      if (!(mode & GLOBAL)) {
        if (!quiet)
          fprintf(stderr,
                  "%s: line %" PRIiSIZE ": ignore value %" PRIu64
                  " for '%s' in non-global context\n",
                  prog, lineno, u64, "mapsize");
      } else if (u64 < MIN_MAPSIZE || u64 > MAX_MAPSIZE64) {
        if (!quiet)
          fprintf(stderr,
                  "%s: line %" PRIiSIZE ": ignore unsupported value 0x%" PRIx64
                  " for %s\n",
                  prog, lineno, u64, "mapsize");
      } else
        envinfo.mi_mapsize = (size_t)u64;
      continue;
    }

    if (valnum(dbuf.iov_base, "maxreaders", &u64)) {
      if (!(mode & GLOBAL)) {
        if (!quiet)
          fprintf(stderr,
                  "%s: line %" PRIiSIZE ": ignore value %" PRIu64
                  " for '%s' in non-global context\n",
                  prog, lineno, u64, "maxreaders");
      } else if (u64 < 1 || u64 > MDBX_READERS_LIMIT) {
        if (!quiet)
          fprintf(stderr,
                  "%s: line %" PRIiSIZE ": ignore unsupported value 0x%" PRIx64
                  " for %s\n",
                  prog, lineno, u64, "maxreaders");
      } else
        envinfo.mi_maxreaders = (int)u64;
      continue;
    }

    if (valnum(dbuf.iov_base, "txnid", &u64)) {
      if (u64 < MIN_TXNID || u64 > MAX_TXNID) {
        if (!quiet)
          fprintf(stderr,
                  "%s: line %" PRIiSIZE ": ignore unsupported value 0x%" PRIx64
                  " for %s\n",
                  prog, lineno, u64, "txnid");
      } else
        txnid = u64;
      continue;
    }

    if (valnum(dbuf.iov_base, "sequence", &u64)) {
      sequence = u64;
      continue;
    }

    str = valstr(dbuf.iov_base, "geometry");
    if (str) {
      if (!(mode & GLOBAL)) {
        if (!quiet)
          fprintf(stderr,
                  "%s: line %" PRIiSIZE ": ignore values %s"
                  " for '%s' in non-global context\n",
                  prog, lineno, str, "geometry");
      } else if (sscanf(str,
                        "l%" PRIu64 ",c%" PRIu64 ",u%" PRIu64 ",s%" PRIu64
                        ",g%" PRIu64,
                        &envinfo.mi_geo.lower, &envinfo.mi_geo.current,
                        &envinfo.mi_geo.upper, &envinfo.mi_geo.shrink,
                        &envinfo.mi_geo.grow) != 5) {
        if (!quiet)
          fprintf(stderr,
                  "%s: line %" PRIiSIZE ": unexpected line format for '%s'\n",
                  prog, lineno, "geometry");
        exit(EXIT_FAILURE);
      }
      continue;
    }

    str = valstr(dbuf.iov_base, "canary");
    if (str) {
      if (!(mode & GLOBAL)) {
        if (!quiet)
          fprintf(stderr,
                  "%s: line %" PRIiSIZE ": ignore values %s"
                  " for '%s' in non-global context\n",
                  prog, lineno, str, "canary");
      } else if (sscanf(str, "v%" PRIu64 ",x%" PRIu64 ",y%" PRIu64 ",z%" PRIu64,
                        &canary.v, &canary.x, &canary.y, &canary.z) != 4) {
        if (!quiet)
          fprintf(stderr,
                  "%s: line %" PRIiSIZE ": unexpected line format for '%s'\n",
                  prog, lineno, "canary");
        exit(EXIT_FAILURE);
      }
      continue;
    }

    for (int i = 0; dbflags[i].bit; i++) {
      bool value = false;
      if (valbool(dbuf.iov_base, dbflags[i].name, &value)) {
        if (value)
          dbi_flags |= dbflags[i].bit;
        else
          dbi_flags &= ~dbflags[i].bit;
        goto next;
      }
    }

    str = valstr(dbuf.iov_base, "HEADER");
    if (str) {
      if (strcmp(str, "END") == 0)
        return MDBX_SUCCESS;
    }

    if (!quiet)
      fprintf(stderr,
              "%s: line %" PRIiSIZE ": unrecognized keyword ignored: %s\n",
              prog, lineno, (char *)dbuf.iov_base);
  next:;
  }
  return EOF;
}

static int badend(void) {
  if (!quiet)
    fprintf(stderr, "%s: line %" PRIiSIZE ": unexpected end of input\n", prog,
            lineno);
  return errno ? errno : MDBX_ENODATA;
}

static __inline int unhex(unsigned char *c2) {
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

__hot static int readline(MDBX_val *out, MDBX_val *buf) {
  unsigned char *c1, *c2, *end;
  size_t len, l2;
  int c;

  if (user_break)
    return MDBX_EINTR;

  errno = 0;
  if (!(mode & NOHDR)) {
    c = fgetc(stdin);
    if (c == EOF)
      return errno ? errno : EOF;
    if (c != ' ') {
      lineno++;
      errno = 0;
      if (fgets(buf->iov_base, (int)buf->iov_len, stdin)) {
        if (c == 'D' && !strncmp(buf->iov_base, "ATA=END", STRLENOF("ATA=END")))
          return EOF;
      }
      return badend();
    }
  }
  if (fgets(buf->iov_base, (int)buf->iov_len, stdin) == nullptr)
    return errno ? errno : EOF;
  lineno++;

  c1 = buf->iov_base;
  len = strlen((char *)c1);
  l2 = len;

  /* Is buffer too short? */
  while (c1[len - 1] != '\n') {
    buf->iov_base = osal_realloc(buf->iov_base, buf->iov_len * 2);
    if (!buf->iov_base) {
      if (!quiet)
        fprintf(stderr,
                "%s: line %" PRIiSIZE ": out of memory, line too long\n", prog,
                lineno);
      return MDBX_ENOMEM;
    }
    c1 = buf->iov_base;
    c1 += l2;
    errno = 0;
    if (fgets((char *)c1, (int)buf->iov_len + 1, stdin) == nullptr)
      return errno ? errno : EOF;
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
      if (unlikely(*c2 == '\\')) {
        if (c2[1] == '\\') {
          *c1++ = '\\';
        } else {
          if (c2 + 3 > end || !isxdigit(c2[1]) || !isxdigit(c2[2]))
            return badend();
          *c1++ = (char)unhex(++c2);
        }
        c2 += 2;
      } else {
        /* copies are redundant when no escapes were used */
        *c1++ = *c2++;
      }
    }
  } else {
    /* odd length not allowed */
    if (len & 1)
      return badend();
    while (c2 < end) {
      if (!isxdigit(*c2) || !isxdigit(c2[1]))
        return badend();
      *c1++ = (char)unhex(c2);
      c2 += 2;
    }
  }
  c2 = out->iov_base = buf->iov_base;
  out->iov_len = c1 - c2;

  return MDBX_SUCCESS;
}

static void usage(void) {
  fprintf(stderr,
          "usage: %s "
          "[-V] [-q] [-a] [-f file] [-s name] [-N] [-p] [-T] [-r] [-n] dbpath\n"
          "  -V\t\tprint version and exit\n"
          "  -q\t\tbe quiet\n"
          "  -a\t\tappend records in input order (required for custom "
          "comparators)\n"
          "  -f file\tread from file instead of stdin\n"
          "  -s name\tload into specified named subDB\n"
          "  -N\t\tdon't overwrite existing records when loading, just skip "
          "ones\n"
          "  -p\t\tpurge subDB before loading\n"
          "  -T\t\tread plaintext\n"
          "  -r\t\trescue mode (ignore errors to load corrupted DB dump)\n"
          "  -n\t\tdon't use subdirectory for newly created database "
          "(MDBX_NOSUBDIR)\n",
          prog);
  exit(EXIT_FAILURE);
}

static int equal_or_greater(const MDBX_val *a, const MDBX_val *b) {
  return (a->iov_len == b->iov_len &&
          memcmp(a->iov_base, b->iov_base, a->iov_len) == 0)
             ? 0
             : 1;
}

int main(int argc, char *argv[]) {
  int i, rc;
  MDBX_env *env = nullptr;
  MDBX_txn *txn = nullptr;
  MDBX_cursor *mc = nullptr;
  MDBX_dbi dbi;
  char *envname = nullptr;
  int envflags = MDBX_SAFE_NOSYNC | MDBX_ACCEDE, putflags = MDBX_UPSERT;
  bool rescue = false;
  bool purge = false;

  prog = argv[0];
  if (argc < 2)
    usage();

  while ((i = getopt(argc, argv,
                     "a"
                     "f:"
                     "n"
                     "s:"
                     "N"
                     "p"
                     "T"
                     "V"
                     "r"
                     "q")) != EOF) {
    switch (i) {
    case 'V':
      printf("mdbx_load version %d.%d.%d.%d\n"
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
    case 'a':
      putflags |= MDBX_APPEND;
      break;
    case 'f':
      if (freopen(optarg, "r", stdin) == nullptr) {
        if (!quiet)
          fprintf(stderr, "%s: %s: open: %s\n", prog, optarg,
                  mdbx_strerror(errno));
        exit(EXIT_FAILURE);
      }
      break;
    case 'n':
      envflags |= MDBX_NOSUBDIR;
      break;
    case 's':
      subname = osal_strdup(optarg);
      break;
    case 'N':
      putflags |= MDBX_NOOVERWRITE | MDBX_NODUPDATA;
      break;
    case 'p':
      purge = true;
      break;
    case 'T':
      mode |= NOHDR | PRINT;
      break;
    case 'q':
      quiet = true;
      break;
    case 'r':
      rescue = true;
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
  if (!quiet)
    printf("mdbx_load %s (%s, T-%s)\nRunning for %s...\n",
           mdbx_version.git.describe, mdbx_version.git.datetime,
           mdbx_version.git.tree, envname);
  fflush(nullptr);

  dbuf.iov_len = 4096;
  dbuf.iov_base = osal_malloc(dbuf.iov_len);
  if (!dbuf.iov_base) {
    rc = MDBX_ENOMEM;
    error("value-buffer", rc);
    goto env_close;
  }

  /* read first header for mapsize= */
  if (!(mode & NOHDR)) {
    rc = readhdr();
    if (unlikely(rc != MDBX_SUCCESS)) {
      if (rc == EOF)
        rc = MDBX_ENODATA;
      error("readheader", rc);
      goto env_close;
    }
  }

  rc = mdbx_env_create(&env);
  if (unlikely(rc != MDBX_SUCCESS)) {
    error("mdbx_env_create", rc);
    return EXIT_FAILURE;
  }

  mdbx_env_set_maxdbs(env, 2);
  if (envinfo.mi_maxreaders) {
    rc = mdbx_env_set_maxreaders(env, envinfo.mi_maxreaders);
    if (unlikely(rc != MDBX_SUCCESS)) {
      error("mdbx_env_set_maxreaders", rc);
      goto env_close;
    }
  }

  if (envinfo.mi_geo.current | envinfo.mi_mapsize) {
    if (envinfo.mi_geo.current) {
      rc = mdbx_env_set_geometry(
          env, (intptr_t)envinfo.mi_geo.lower, (intptr_t)envinfo.mi_geo.current,
          (intptr_t)envinfo.mi_geo.upper, (intptr_t)envinfo.mi_geo.shrink,
          (intptr_t)envinfo.mi_geo.grow,
          envinfo.mi_dxb_pagesize ? (intptr_t)envinfo.mi_dxb_pagesize : -1);
    } else {
      if (envinfo.mi_mapsize > MAX_MAPSIZE) {
        if (!quiet)
          fprintf(
              stderr,
              "Database size is too large for current system (mapsize=%" PRIu64
              " is great than system-limit %zu)\n",
              envinfo.mi_mapsize, (size_t)MAX_MAPSIZE);
        goto env_close;
      }
      rc = mdbx_env_set_geometry(
          env, (intptr_t)envinfo.mi_mapsize, (intptr_t)envinfo.mi_mapsize,
          (intptr_t)envinfo.mi_mapsize, 0, 0,
          envinfo.mi_dxb_pagesize ? (intptr_t)envinfo.mi_dxb_pagesize : -1);
    }
    if (unlikely(rc != MDBX_SUCCESS)) {
      error("mdbx_env_set_geometry", rc);
      goto env_close;
    }
  }

  rc = mdbx_env_open(env, envname, envflags, 0664);
  if (unlikely(rc != MDBX_SUCCESS)) {
    error("mdbx_env_open", rc);
    goto env_close;
  }

  kbuf.iov_len = mdbx_env_get_maxvalsize_ex(env, 0) + 1;
  if (kbuf.iov_len >= INTPTR_MAX / 2) {
    if (!quiet)
      fprintf(stderr, "mdbx_env_get_maxkeysize() failed, returns %zu\n",
              kbuf.iov_len);
    goto env_close;
  }

  kbuf.iov_base = malloc(kbuf.iov_len);
  if (!kbuf.iov_base) {
    rc = MDBX_ENOMEM;
    error("key-buffer", rc);
    goto env_close;
  }

  while (rc == MDBX_SUCCESS) {
    if (user_break) {
      rc = MDBX_EINTR;
      break;
    }

    rc = mdbx_txn_begin(env, nullptr, 0, &txn);
    if (unlikely(rc != MDBX_SUCCESS)) {
      error("mdbx_txn_begin", rc);
      goto env_close;
    }

    if (mode & GLOBAL) {
      mode -= GLOBAL;
      if (canary.v | canary.x | canary.y | canary.z) {
        rc = mdbx_canary_put(txn, &canary);
        if (unlikely(rc != MDBX_SUCCESS)) {
          error("mdbx_canary_put", rc);
          goto txn_abort;
        }
      }
    }

    const char *const dbi_name = subname ? subname : "@MAIN";
    rc =
        mdbx_dbi_open_ex(txn, subname, dbi_flags | MDBX_CREATE, &dbi,
                         (putflags & MDBX_APPEND) ? equal_or_greater : nullptr,
                         (putflags & MDBX_APPEND) ? equal_or_greater : nullptr);
    if (unlikely(rc != MDBX_SUCCESS)) {
      error("mdbx_dbi_open_ex", rc);
      goto txn_abort;
    }

    uint64_t present_sequence;
    rc = mdbx_dbi_sequence(txn, dbi, &present_sequence, 0);
    if (unlikely(rc != MDBX_SUCCESS)) {
      error("mdbx_dbi_sequence", rc);
      goto txn_abort;
    }
    if (present_sequence > sequence) {
      if (!quiet)
        fprintf(stderr,
                "present sequence for '%s' value (%" PRIu64
                ") is greater than loaded (%" PRIu64 ")\n",
                dbi_name, present_sequence, sequence);
      rc = MDBX_RESULT_TRUE;
      goto txn_abort;
    }
    if (present_sequence < sequence) {
      rc = mdbx_dbi_sequence(txn, dbi, nullptr, sequence - present_sequence);
      if (unlikely(rc != MDBX_SUCCESS)) {
        error("mdbx_dbi_sequence", rc);
        goto txn_abort;
      }
    }

    if (purge) {
      rc = mdbx_drop(txn, dbi, false);
      if (unlikely(rc != MDBX_SUCCESS)) {
        error("mdbx_drop", rc);
        goto txn_abort;
      }
    }

    if (putflags & MDBX_APPEND)
      putflags = (dbi_flags & MDBX_DUPSORT) ? putflags | MDBX_APPENDDUP
                                            : putflags & ~MDBX_APPENDDUP;

    rc = mdbx_cursor_open(txn, dbi, &mc);
    if (unlikely(rc != MDBX_SUCCESS)) {
      error("mdbx_cursor_open", rc);
      goto txn_abort;
    }

    int batch = 0;
    while (rc == MDBX_SUCCESS) {
      MDBX_val key, data;
      rc = readline(&key, &kbuf);
      if (rc == EOF)
        break;

      if (rc == MDBX_SUCCESS)
        rc = readline(&data, &dbuf);
      if (rc) {
        if (!quiet)
          fprintf(stderr, "%s: line %" PRIiSIZE ": failed to read key value\n",
                  prog, lineno);
        goto txn_abort;
      }

      rc = mdbx_cursor_put(mc, &key, &data, putflags);
      if (rc == MDBX_KEYEXIST && putflags)
        continue;
      if (rc == MDBX_BAD_VALSIZE && rescue) {
        if (!quiet)
          fprintf(stderr, "%s: skip line %" PRIiSIZE ": due %s\n", prog, lineno,
                  mdbx_strerror(rc));
        continue;
      }
      if (unlikely(rc != MDBX_SUCCESS)) {
        error("mdbx_cursor_put", rc);
        goto txn_abort;
      }
      batch++;

      MDBX_txn_info txn_info;
      rc = mdbx_txn_info(txn, &txn_info, false);
      if (unlikely(rc != MDBX_SUCCESS)) {
        error("mdbx_txn_info", rc);
        goto txn_abort;
      }

      if (batch == 10000 || txn_info.txn_space_dirty > MEGABYTE * 256) {
        rc = mdbx_txn_commit(txn);
        if (unlikely(rc != MDBX_SUCCESS)) {
          error("mdbx_txn_commit", rc);
          goto env_close;
        }
        batch = 0;

        rc = mdbx_txn_begin(env, nullptr, 0, &txn);
        if (unlikely(rc != MDBX_SUCCESS)) {
          error("mdbx_txn_begin", rc);
          goto env_close;
        }
        rc = mdbx_cursor_bind(txn, mc, dbi);
        if (unlikely(rc != MDBX_SUCCESS)) {
          error("mdbx_cursor_bind", rc);
          goto txn_abort;
        }
      }
    }

    mdbx_cursor_close(mc);
    mc = nullptr;
    rc = mdbx_txn_commit(txn);
    txn = nullptr;
    if (unlikely(rc != MDBX_SUCCESS)) {
      error("mdbx_txn_commit", rc);
      goto env_close;
    }
    if (subname) {
      assert(dbi != MAIN_DBI);
      rc = mdbx_dbi_close(env, dbi);
      if (unlikely(rc != MDBX_SUCCESS)) {
        error("mdbx_dbi_close", rc);
        goto env_close;
      }
    } else {
      assert(dbi == MAIN_DBI);
    }

    /* try read next header */
    if (!(mode & NOHDR))
      rc = readhdr();
    else if (ferror(stdin) || feof(stdin))
      break;
  }

  switch (rc) {
  case EOF:
    rc = MDBX_SUCCESS;
  case MDBX_SUCCESS:
    break;
  case MDBX_EINTR:
    if (!quiet)
      fprintf(stderr, "Interrupted by signal/user\n");
    break;
  default:
    if (unlikely(rc != MDBX_SUCCESS))
      error("readline", rc);
  }

txn_abort:
  mdbx_cursor_close(mc);
  mdbx_txn_abort(txn);
env_close:
  mdbx_env_close(env);
  free(kbuf.iov_base);
  free(dbuf.iov_base);

  return rc ? EXIT_FAILURE : EXIT_SUCCESS;
}
