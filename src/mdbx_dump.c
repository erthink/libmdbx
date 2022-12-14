/* mdbx_dump.c - memory-mapped database dump tool */

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

#define PRINT 1
#define GLOBAL 2
static int mode = GLOBAL;

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
                     {0, nullptr}};

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
  putchar(hexc[c & 15]);
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
  while (c < end)
    dumpbyte(*c++);
  putchar('\n');
}

bool quiet = false, rescue = false;
const char *prog;
static void error(const char *func, int rc) {
  if (!quiet)
    fprintf(stderr, "%s: %s() error %d %s\n", prog, func, rc,
            mdbx_strerror(rc));
}

/* Dump in BDB-compatible format */
static int dump_sdb(MDBX_txn *txn, MDBX_dbi dbi, char *name) {
  unsigned int flags;
  int rc = mdbx_dbi_flags(txn, dbi, &flags);
  if (unlikely(rc != MDBX_SUCCESS)) {
    error("mdbx_dbi_flags", rc);
    return rc;
  }

  MDBX_stat ms;
  rc = mdbx_dbi_stat(txn, dbi, &ms, sizeof(ms));
  if (unlikely(rc != MDBX_SUCCESS)) {
    error("mdbx_dbi_stat", rc);
    return rc;
  }

  MDBX_envinfo info;
  rc = mdbx_env_info_ex(mdbx_txn_env(txn), txn, &info, sizeof(info));
  if (unlikely(rc != MDBX_SUCCESS)) {
    error("mdbx_env_info_ex", rc);
    return rc;
  }

  printf("VERSION=3\n");
  if (mode & GLOBAL) {
    mode -= GLOBAL;
    if (info.mi_geo.upper != info.mi_geo.lower)
      printf("geometry=l%" PRIu64 ",c%" PRIu64 ",u%" PRIu64 ",s%" PRIu64
             ",g%" PRIu64 "\n",
             info.mi_geo.lower, info.mi_geo.current, info.mi_geo.upper,
             info.mi_geo.shrink, info.mi_geo.grow);
    printf("mapsize=%" PRIu64 "\n", info.mi_geo.upper);
    printf("maxreaders=%u\n", info.mi_maxreaders);

    MDBX_canary canary;
    rc = mdbx_canary_get(txn, &canary);
    if (unlikely(rc != MDBX_SUCCESS)) {
      error("mdbx_canary_get", rc);
      return rc;
    }
    if (canary.v)
      printf("canary=v%" PRIu64 ",x%" PRIu64 ",y%" PRIu64 ",z%" PRIu64 "\n",
             canary.v, canary.x, canary.y, canary.z);
  }
  printf("format=%s\n", mode & PRINT ? "print" : "bytevalue");
  if (name)
    printf("database=%s\n", name);
  printf("type=btree\n");
  printf("db_pagesize=%u\n", ms.ms_psize);
  /* if (ms.ms_mod_txnid)
    printf("txnid=%" PRIaTXN "\n", ms.ms_mod_txnid);
  else if (!name)
    printf("txnid=%" PRIaTXN "\n", mdbx_txn_id(txn)); */

  printf("duplicates=%d\n", (flags & (MDBX_DUPSORT | MDBX_DUPFIXED |
                                      MDBX_INTEGERDUP | MDBX_REVERSEDUP))
                                ? 1
                                : 0);
  for (int i = 0; dbflags[i].bit; i++)
    if (flags & dbflags[i].bit)
      printf("%s=1\n", dbflags[i].name);

  uint64_t sequence;
  rc = mdbx_dbi_sequence(txn, dbi, &sequence, 0);
  if (unlikely(rc != MDBX_SUCCESS)) {
    error("mdbx_dbi_sequence", rc);
    return rc;
  }
  if (sequence)
    printf("sequence=%" PRIu64 "\n", sequence);

  printf("HEADER=END\n"); /*-------------------------------------------------*/

  MDBX_cursor *cursor;
  MDBX_val key, data;
  rc = mdbx_cursor_open(txn, dbi, &cursor);
  if (unlikely(rc != MDBX_SUCCESS)) {
    error("mdbx_cursor_open", rc);
    return rc;
  }
  if (rescue) {
    cursor->mc_checking |= CC_SKIPORD;
    if (cursor->mc_xcursor)
      cursor->mc_xcursor->mx_cursor.mc_checking |= CC_SKIPORD;
  }

  while ((rc = mdbx_cursor_get(cursor, &key, &data, MDBX_NEXT)) ==
         MDBX_SUCCESS) {
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
  if (unlikely(rc != MDBX_SUCCESS))
    error("mdbx_cursor_get", rc);

  mdbx_cursor_close(cursor);
  return rc;
}

static void usage(void) {
  fprintf(
      stderr,
      "usage: %s "
      "[-V] [-q] [-f file] [-l] [-p] [-r] [-a|-s subdb] [-u|U] "
      "dbpath\n"
      "  -V\t\tprint version and exit\n"
      "  -q\t\tbe quiet\n"
      "  -f\t\twrite to file instead of stdout\n"
      "  -l\t\tlist subDBs and exit\n"
      "  -p\t\tuse printable characters\n"
      "  -r\t\trescue mode (ignore errors to dump corrupted DB)\n"
      "  -a\t\tdump main DB and all subDBs\n"
      "  -s name\tdump only the specified named subDB\n"
      "  -u\t\twarmup database before dumping\n"
      "  -U\t\twarmup and try lock database pages in memory before dumping\n"
      "  \t\tby default dump only the main DB\n",
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
  MDBX_env *env;
  MDBX_txn *txn;
  MDBX_dbi dbi;
  prog = argv[0];
  char *envname;
  char *subname = nullptr, *buf4free = nullptr;
  unsigned envflags = 0;
  bool alldbs = false, list = false;
  bool warmup = false;
  MDBX_warmup_flags_t warmup_flags = MDBX_warmup_default;

  if (argc < 2)
    usage();

  while ((i = getopt(argc, argv,
                     "uU"
                     "a"
                     "f:"
                     "l"
                     "n"
                     "p"
                     "s:"
                     "V"
                     "r"
                     "q")) != EOF) {
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
      list = true;
      /*FALLTHROUGH*/;
      __fallthrough;
    case 'a':
      if (subname)
        usage();
      alldbs = true;
      break;
    case 'f':
      if (freopen(optarg, "w", stdout) == nullptr) {
        fprintf(stderr, "%s: %s: reopen: %s\n", prog, optarg,
                mdbx_strerror(errno));
        exit(EXIT_FAILURE);
      }
      break;
    case 'n':
      break;
    case 'p':
      mode |= PRINT;
      break;
    case 's':
      if (alldbs)
        usage();
      subname = optarg;
      break;
    case 'q':
      quiet = true;
      break;
    case 'r':
      rescue = true;
      break;
    case 'u':
      warmup = true;
      break;
    case 'U':
      warmup = true;
      warmup_flags =
          MDBX_warmup_force | MDBX_warmup_touchlimit | MDBX_warmup_lock;
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
    fprintf(stderr, "mdbx_dump %s (%s, T-%s)\nRunning for %s...\n",
            mdbx_version.git.describe, mdbx_version.git.datetime,
            mdbx_version.git.tree, envname);
    fflush(nullptr);
  }

  rc = mdbx_env_create(&env);
  if (unlikely(rc != MDBX_SUCCESS)) {
    error("mdbx_env_create", rc);
    return EXIT_FAILURE;
  }

  if (alldbs || subname) {
    rc = mdbx_env_set_maxdbs(env, 2);
    if (unlikely(rc != MDBX_SUCCESS)) {
      error("mdbx_env_set_maxdbs", rc);
      goto env_close;
    }
  }

  rc = mdbx_env_open(
      env, envname,
      envflags | (rescue ? MDBX_RDONLY | MDBX_EXCLUSIVE | MDBX_VALIDATION
                         : MDBX_RDONLY),
      0);
  if (unlikely(rc != MDBX_SUCCESS)) {
    error("mdbx_env_open", rc);
    goto env_close;
  }

  if (warmup) {
    rc = mdbx_env_warmup(env, nullptr, warmup_flags, 3600 * 65536);
    if (MDBX_IS_ERROR(rc)) {
      error("mdbx_env_warmup", rc);
      goto env_close;
    }
  }

  rc = mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &txn);
  if (unlikely(rc != MDBX_SUCCESS)) {
    error("mdbx_txn_begin", rc);
    goto env_close;
  }

  rc = mdbx_dbi_open(txn, subname, MDBX_DB_ACCEDE, &dbi);
  if (unlikely(rc != MDBX_SUCCESS)) {
    error("mdbx_dbi_open", rc);
    goto txn_abort;
  }

  if (alldbs) {
    assert(dbi == MAIN_DBI);

    MDBX_cursor *cursor;
    rc = mdbx_cursor_open(txn, MAIN_DBI, &cursor);
    if (unlikely(rc != MDBX_SUCCESS)) {
      error("mdbx_cursor_open", rc);
      goto txn_abort;
    }
    if (rescue) {
      cursor->mc_checking |= CC_SKIPORD;
      if (cursor->mc_xcursor)
        cursor->mc_xcursor->mx_cursor.mc_checking |= CC_SKIPORD;
    }

    bool have_raw = false;
    int count = 0;
    MDBX_val key;
    while (MDBX_SUCCESS ==
           (rc = mdbx_cursor_get(cursor, &key, nullptr, MDBX_NEXT_NODUP))) {
      if (user_break) {
        rc = MDBX_EINTR;
        break;
      }

      if (memchr(key.iov_base, '\0', key.iov_len))
        continue;
      subname = osal_realloc(buf4free, key.iov_len + 1);
      if (!subname) {
        rc = MDBX_ENOMEM;
        break;
      }

      buf4free = subname;
      memcpy(subname, key.iov_base, key.iov_len);
      subname[key.iov_len] = '\0';

      MDBX_dbi sub_dbi;
      rc = mdbx_dbi_open_ex(txn, subname, MDBX_DB_ACCEDE, &sub_dbi,
                            rescue ? equal_or_greater : nullptr,
                            rescue ? equal_or_greater : nullptr);
      if (unlikely(rc != MDBX_SUCCESS)) {
        if (rc == MDBX_INCOMPATIBLE) {
          have_raw = true;
          continue;
        }
        error("mdbx_dbi_open", rc);
        if (!rescue)
          break;
      } else {
        count++;
        if (list) {
          printf("%s\n", subname);
        } else {
          rc = dump_sdb(txn, sub_dbi, subname);
          if (unlikely(rc != MDBX_SUCCESS)) {
            if (!rescue)
              break;
            if (!quiet)
              fprintf(stderr, "%s: %s: ignore %s for `%s` and continue\n", prog,
                      envname, mdbx_strerror(rc), subname);
            /* Here is a hack for rescue mode, don't do that:
             *  - we should restart transaction in case error due
             *    database corruption;
             *  - but we won't close cursor, reopen and re-positioning it
             *    for new a transaction;
             *  - this is possible since DB is opened in read-only exclusive
             *    mode and transaction is the same, i.e. has the same address
             *    and so on. */
            rc = mdbx_txn_reset(txn);
            if (unlikely(rc != MDBX_SUCCESS)) {
              error("mdbx_txn_reset", rc);
              goto env_close;
            }
            rc = mdbx_txn_renew(txn);
            if (unlikely(rc != MDBX_SUCCESS)) {
              error("mdbx_txn_renew", rc);
              goto env_close;
            }
          }
        }
        rc = mdbx_dbi_close(env, sub_dbi);
        if (unlikely(rc != MDBX_SUCCESS)) {
          error("mdbx_dbi_close", rc);
          break;
        }
      }
    }
    mdbx_cursor_close(cursor);
    cursor = nullptr;

    if (have_raw && (!count /* || rescue */))
      rc = dump_sdb(txn, MAIN_DBI, nullptr);
    else if (!count) {
      if (!quiet)
        fprintf(stderr, "%s: %s does not contain multiple databases\n", prog,
                envname);
      rc = MDBX_NOTFOUND;
    }
  } else {
    rc = dump_sdb(txn, dbi, subname);
  }

  switch (rc) {
  case MDBX_NOTFOUND:
    rc = MDBX_SUCCESS;
  case MDBX_SUCCESS:
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
  free(buf4free);

  return rc ? EXIT_FAILURE : EXIT_SUCCESS;
}
