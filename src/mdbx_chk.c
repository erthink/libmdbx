/* mdbx_chk.c - memory-mapped database check tool */

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

typedef struct flagbit {
  int bit;
  const char *name;
} flagbit;

const flagbit dbflags[] = {{MDBX_DUPSORT, "dupsort"},
                           {MDBX_INTEGERKEY, "integerkey"},
                           {MDBX_REVERSEKEY, "reversekey"},
                           {MDBX_DUPFIXED, "dupfixed"},
                           {MDBX_REVERSEDUP, "reversedup"},
                           {MDBX_INTEGERDUP, "integerdup"},
                           {0, nullptr}};

#if defined(_WIN32) || defined(_WIN64)
#include "wingetopt.h"

static volatile BOOL user_break;
static BOOL WINAPI ConsoleBreakHandlerRoutine(DWORD dwCtrlType) {
  (void)dwCtrlType;
  user_break = 1;
  return true;
}

static uint64_t GetMilliseconds(void) {
  LARGE_INTEGER Counter, Frequency;
  return (QueryPerformanceFrequency(&Frequency) &&
          QueryPerformanceCounter(&Counter))
             ? Counter.QuadPart * 1000ul / Frequency.QuadPart
             : 0;
}

#else /* WINDOWS */

static volatile sig_atomic_t user_break;
static void signal_handler(int sig) {
  (void)sig;
  user_break = 1;
}

#endif /* !WINDOWS */

#define EXIT_INTERRUPTED (EXIT_FAILURE + 4)
#define EXIT_FAILURE_SYS (EXIT_FAILURE + 3)
#define EXIT_FAILURE_MDBX (EXIT_FAILURE + 2)
#define EXIT_FAILURE_CHECK_MAJOR (EXIT_FAILURE + 1)
#define EXIT_FAILURE_CHECK_MINOR EXIT_FAILURE

typedef struct {
  MDBX_val name;
  struct {
    uint64_t branch, large_count, large_volume, leaf;
    uint64_t subleaf_dupsort, leaf_dupfixed, subleaf_dupfixed;
    uint64_t total, empty, other;
  } pages;
  uint64_t payload_bytes;
  uint64_t lost_bytes;
} walk_dbi_t;

struct {
  short *pagemap;
  uint64_t total_payload_bytes;
  uint64_t pgcount;
  walk_dbi_t
      dbi[MDBX_MAX_DBI + CORE_DBS + /* account pseudo-entry for meta */ 1];
} walk;

#define dbi_free walk.dbi[FREE_DBI]
#define dbi_main walk.dbi[MAIN_DBI]
#define dbi_meta walk.dbi[CORE_DBS]

int envflags = MDBX_RDONLY | MDBX_EXCLUSIVE | MDBX_VALIDATION;
MDBX_env *env;
MDBX_txn *txn;
MDBX_envinfo envinfo;
size_t userdb_count, skipped_subdb;
uint64_t total_unused_bytes, reclaimable_pages, gc_pages, alloc_pages,
    unused_pages, backed_pages;
unsigned verbose;
bool ignore_wrong_order, quiet, dont_traversal;
MDBX_val only_subdb;
int stuck_meta = -1;

struct problem {
  struct problem *pr_next;
  size_t count;
  const char *caption;
};

struct problem *problems_list;
unsigned total_problems, data_tree_problems, gc_tree_problems;

static void MDBX_PRINTF_ARGS(1, 2) print(const char *msg, ...) {
  if (!quiet) {
    va_list args;

    fflush(stderr);
    va_start(args, msg);
    vfprintf(stdout, msg, args);
    va_end(args);
  }
}

static MDBX_val printable_buf;
static void free_printable_buf(void) { osal_free(printable_buf.iov_base); }

static const char *sdb_name(const MDBX_val *val) {
  if (val == MDBX_PGWALK_MAIN)
    return "@MAIN";
  if (val == MDBX_PGWALK_GC)
    return "@GC";
  if (val == MDBX_PGWALK_META)
    return "@META";

  const unsigned char *const data = val->iov_base;
  const size_t len = val->iov_len;
  if (data == MDBX_PGWALK_MAIN)
    return "@MAIN";
  if (data == MDBX_PGWALK_GC)
    return "@GC";
  if (data == MDBX_PGWALK_META)
    return "@META";

  if (!len)
    return "<zero-length>";
  if (!data)
    return "<nullptr>";
  if (len > 65536) {
    static char buf[64];
    snprintf(buf, sizeof(buf), "<too-long-%zu>", len);
    return buf;
  }

  bool printable = true;
  bool quoting = false;
  size_t xchars = 0;
  for (size_t i = 0; i < val->iov_len && printable; ++i) {
    quoting |= data[i] != '_' && isalnum(data[i]) == 0;
    printable = isprint(data[i]) != 0 ||
                (data[i] < ' ' && ++xchars < 4 && len > xchars * 4);
  }

  size_t need = len + 1;
  if (quoting || !printable)
    need += len + /* quotes */ 2 + 2 * /* max xchars */ 4;
  if (need > printable_buf.iov_len) {
    void *ptr = osal_realloc(printable_buf.iov_base, need);
    if (!ptr)
      return "<out-of-memory>";
    if (!printable_buf.iov_base)
      atexit(free_printable_buf);
    printable_buf.iov_base = ptr;
    printable_buf.iov_len = need;
  }

  char *out = printable_buf.iov_base;
  if (!quoting) {
    memcpy(out, data, len);
    out += len;
  } else if (printable) {
    *out++ = '\'';
    for (size_t i = 0; i < len; ++i) {
      if (data[i] < ' ') {
        assert((char *)printable_buf.iov_base + printable_buf.iov_len >
               out + 4);
        static const char hex[] = "0123456789abcdef";
        out[0] = '\\';
        out[1] = 'x';
        out[2] = hex[data[i] >> 4];
        out[3] = hex[data[i] & 15];
        out += 4;
      } else if (strchr("\"'`\\", data[i])) {
        assert((char *)printable_buf.iov_base + printable_buf.iov_len >
               out + 2);
        out[0] = '\\';
        out[1] = data[i];
        out += 2;
      } else {
        assert((char *)printable_buf.iov_base + printable_buf.iov_len >
               out + 1);
        *out++ = data[i];
      }
    }
    *out++ = '\'';
  }
  assert((char *)printable_buf.iov_base + printable_buf.iov_len > out);
  *out = 0;
  return printable_buf.iov_base;
}

static void va_log(MDBX_log_level_t level, const char *function, int line,
                   const char *msg, va_list args) {
  static const char *const prefixes[] = {
      "!!!fatal: ",       " ! " /* error */,      " ~ " /* warning */,
      "   " /* notice */, "   // " /* verbose */, "   //// " /* debug */,
      "   ////// " /* trace */
  };

  FILE *out = stdout;
  if (level <= MDBX_LOG_ERROR) {
    total_problems++;
    out = stderr;
  }

  if (!quiet && verbose + 1 >= (unsigned)level &&
      (unsigned)level < ARRAY_LENGTH(prefixes)) {
    fflush(nullptr);
    fputs(prefixes[level], out);
    vfprintf(out, msg, args);

    const bool have_lf = msg[strlen(msg) - 1] == '\n';
    if (level == MDBX_LOG_FATAL && function && line)
      fprintf(out, have_lf ? "          %s(), %u\n" : " (%s:%u)\n",
              function + (strncmp(function, "mdbx_", 5) ? 5 : 0), line);
    else if (!have_lf)
      fputc('\n', out);
    fflush(nullptr);
  }

  if (level == MDBX_LOG_FATAL) {
#if !MDBX_DEBUG && !MDBX_FORCE_ASSERTIONS
    exit(EXIT_FAILURE_MDBX);
#endif
    abort();
  }
}

static void MDBX_PRINTF_ARGS(1, 2) error(const char *msg, ...) {
  va_list args;
  va_start(args, msg);
  va_log(MDBX_LOG_ERROR, nullptr, 0, msg, args);
  va_end(args);
}

static void logger(MDBX_log_level_t level, const char *function, int line,
                   const char *msg, va_list args) {
  (void)line;
  (void)function;
  if (level < MDBX_LOG_EXTRA)
    va_log(level, function, line, msg, args);
}

static int check_user_break(void) {
  switch (user_break) {
  case 0:
    return MDBX_SUCCESS;
  case 1:
    print(" - interrupted by signal\n");
    fflush(nullptr);
    user_break = 2;
  }
  return MDBX_EINTR;
}

static void pagemap_cleanup(void) {
  osal_free(walk.pagemap);
  walk.pagemap = nullptr;
}

static bool eq(const MDBX_val a, const MDBX_val b) {
  return a.iov_len == b.iov_len &&
         (a.iov_base == b.iov_base || a.iov_len == 0 ||
          !memcmp(a.iov_base, b.iov_base, a.iov_len));
}

static walk_dbi_t *pagemap_lookup_dbi(const MDBX_val *dbi_name, bool silent) {
  static walk_dbi_t *last;

  if (dbi_name == MDBX_PGWALK_MAIN)
    return &dbi_main;
  if (dbi_name == MDBX_PGWALK_GC)
    return &dbi_free;
  if (dbi_name == MDBX_PGWALK_META)
    return &dbi_meta;

  if (last && eq(last->name, *dbi_name))
    return last;

  walk_dbi_t *dbi = walk.dbi + CORE_DBS + /* account pseudo-entry for meta */ 1;
  for (; dbi < ARRAY_END(walk.dbi) && dbi->name.iov_base; ++dbi) {
    if (eq(dbi->name, *dbi_name))
      return last = dbi;
  }

  if (verbose > 0 && !silent) {
    print(" - found %s area\n", sdb_name(dbi_name));
    fflush(nullptr);
  }

  if (dbi == ARRAY_END(walk.dbi))
    return nullptr;

  dbi->name = *dbi_name;
  return last = dbi;
}

static void MDBX_PRINTF_ARGS(4, 5)
    problem_add(const char *object, uint64_t entry_number, const char *msg,
                const char *extra, ...) {
  total_problems++;

  if (!quiet) {
    int need_fflush = 0;
    struct problem *p;

    for (p = problems_list; p; p = p->pr_next)
      if (p->caption == msg)
        break;

    if (!p) {
      p = osal_calloc(1, sizeof(*p));
      if (unlikely(!p))
        return;
      p->caption = msg;
      p->pr_next = problems_list;
      problems_list = p;
      need_fflush = 1;
    }

    p->count++;
    if (verbose > 1) {
      print("     %s #%" PRIu64 ": %s", object, entry_number, msg);
      if (extra) {
        va_list args;
        printf(" (");
        va_start(args, extra);
        vfprintf(stdout, extra, args);
        va_end(args);
        printf(")");
      }
      printf("\n");
      if (need_fflush)
        fflush(nullptr);
    }
  }
}

static struct problem *problems_push(void) {
  struct problem *p = problems_list;
  problems_list = nullptr;
  return p;
}

static size_t problems_pop(struct problem *list) {
  size_t count = 0;

  if (problems_list) {
    int i;

    print(" - problems: ");
    for (i = 0; problems_list; ++i) {
      struct problem *p = problems_list->pr_next;
      count += problems_list->count;
      print("%s%s (%" PRIuPTR ")", i ? ", " : "", problems_list->caption,
            problems_list->count);
      osal_free(problems_list);
      problems_list = p;
    }
    print("\n");
    fflush(nullptr);
  }

  problems_list = list;
  return count;
}

static int pgvisitor(const uint64_t pgno, const unsigned pgnumber,
                     void *const ctx, const int deep, const MDBX_val *dbi_name,
                     const size_t page_size, const MDBX_page_type_t pagetype,
                     const MDBX_error_t err, const size_t nentries,
                     const size_t payload_bytes, const size_t header_bytes,
                     const size_t unused_bytes) {
  (void)ctx;
  const bool is_gc_tree = dbi_name == MDBX_PGWALK_GC;
  if (deep > 42) {
    problem_add("deep", deep, "too large", nullptr);
    data_tree_problems += !is_gc_tree;
    gc_tree_problems += is_gc_tree;
    return MDBX_CORRUPTED /* avoid infinite loop/recursion */;
  }

  walk_dbi_t *dbi = pagemap_lookup_dbi(dbi_name, false);
  if (!dbi) {
    data_tree_problems += !is_gc_tree;
    gc_tree_problems += is_gc_tree;
    return MDBX_ENOMEM;
  }

  const size_t page_bytes = payload_bytes + header_bytes + unused_bytes;
  walk.pgcount += pgnumber;

  const char *pagetype_caption;
  bool branch = false;
  switch (pagetype) {
  default:
    problem_add("page", pgno, "unknown page-type", "type %u, deep %i",
                (unsigned)pagetype, deep);
    pagetype_caption = "unknown";
    dbi->pages.other += pgnumber;
    data_tree_problems += !is_gc_tree;
    gc_tree_problems += is_gc_tree;
    break;
  case MDBX_page_broken:
    pagetype_caption = "broken";
    dbi->pages.other += pgnumber;
    data_tree_problems += !is_gc_tree;
    gc_tree_problems += is_gc_tree;
    break;
  case MDBX_subpage_broken:
    pagetype_caption = "broken-subpage";
    data_tree_problems += !is_gc_tree;
    gc_tree_problems += is_gc_tree;
    break;
  case MDBX_page_meta:
    pagetype_caption = "meta";
    dbi->pages.other += pgnumber;
    break;
  case MDBX_page_large:
    pagetype_caption = "large";
    dbi->pages.large_volume += pgnumber;
    dbi->pages.large_count += 1;
    break;
  case MDBX_page_branch:
    pagetype_caption = "branch";
    dbi->pages.branch += pgnumber;
    branch = true;
    break;
  case MDBX_page_leaf:
    pagetype_caption = "leaf";
    dbi->pages.leaf += pgnumber;
    break;
  case MDBX_page_dupfixed_leaf:
    pagetype_caption = "leaf-dupfixed";
    dbi->pages.leaf_dupfixed += pgnumber;
    break;
  case MDBX_subpage_leaf:
    pagetype_caption = "subleaf-dupsort";
    dbi->pages.subleaf_dupsort += 1;
    break;
  case MDBX_subpage_dupfixed_leaf:
    pagetype_caption = "subleaf-dupfixed";
    dbi->pages.subleaf_dupfixed += 1;
    break;
  }

  if (pgnumber) {
    if (verbose > 3 && (!only_subdb.iov_base || eq(only_subdb, dbi->name))) {
      if (pgnumber == 1)
        print("     %s-page %" PRIu64, pagetype_caption, pgno);
      else
        print("     %s-span %" PRIu64 "[%u]", pagetype_caption, pgno, pgnumber);
      print(" of %s: header %" PRIiPTR ", %s %" PRIiPTR ", payload %" PRIiPTR
            ", unused %" PRIiPTR ", deep %i\n",
            sdb_name(&dbi->name), header_bytes,
            (pagetype == MDBX_page_branch) ? "keys" : "entries", nentries,
            payload_bytes, unused_bytes, deep);
    }

    bool already_used = false;
    for (unsigned n = 0; n < pgnumber; ++n) {
      uint64_t spanpgno = pgno + n;
      if (spanpgno >= alloc_pages) {
        problem_add("page", spanpgno, "wrong page-no",
                    "%s-page: %" PRIu64 " > %" PRIu64 ", deep %i",
                    pagetype_caption, spanpgno, alloc_pages, deep);
        data_tree_problems += !is_gc_tree;
        gc_tree_problems += is_gc_tree;
      } else if (walk.pagemap[spanpgno]) {
        walk_dbi_t *coll_dbi = &walk.dbi[walk.pagemap[spanpgno] - 1];
        problem_add("page", spanpgno,
                    (branch && coll_dbi == dbi) ? "loop" : "already used",
                    "%s-page: by %s, deep %i", pagetype_caption,
                    sdb_name(&coll_dbi->name), deep);
        already_used = true;
        data_tree_problems += !is_gc_tree;
        gc_tree_problems += is_gc_tree;
      } else {
        walk.pagemap[spanpgno] = (short)(dbi - walk.dbi + 1);
        dbi->pages.total += 1;
      }
    }

    if (already_used)
      return branch ? MDBX_RESULT_TRUE /* avoid infinite loop/recursion */
                    : MDBX_SUCCESS;
  }

  if (MDBX_IS_ERROR(err)) {
    problem_add("page", pgno, "invalid/corrupted", "%s-page", pagetype_caption);
    data_tree_problems += !is_gc_tree;
    gc_tree_problems += is_gc_tree;
  } else {
    if (unused_bytes > page_size) {
      problem_add("page", pgno, "illegal unused-bytes",
                  "%s-page: %u < %" PRIuPTR " < %u", pagetype_caption, 0,
                  unused_bytes, envinfo.mi_dxb_pagesize);
      data_tree_problems += !is_gc_tree;
      gc_tree_problems += is_gc_tree;
    }

    if (header_bytes < (int)sizeof(long) ||
        (size_t)header_bytes >= envinfo.mi_dxb_pagesize - sizeof(long)) {
      problem_add("page", pgno, "illegal header-length",
                  "%s-page: %" PRIuPTR " < %" PRIuPTR " < %" PRIuPTR,
                  pagetype_caption, sizeof(long), header_bytes,
                  envinfo.mi_dxb_pagesize - sizeof(long));
      data_tree_problems += !is_gc_tree;
      gc_tree_problems += is_gc_tree;
    }
    if (payload_bytes < 1) {
      if (nentries > 1) {
        problem_add("page", pgno, "zero size-of-entry",
                    "%s-page: payload %" PRIuPTR " bytes, %" PRIuPTR " entries",
                    pagetype_caption, payload_bytes, nentries);
        /* if ((size_t)header_bytes + unused_bytes < page_size) {
          // LY: hush a misuse error
          page_bytes = page_size;
        } */
        data_tree_problems += !is_gc_tree;
        gc_tree_problems += is_gc_tree;
      } else {
        problem_add("page", pgno, "empty",
                    "%s-page: payload %" PRIuPTR " bytes, %" PRIuPTR
                    " entries, deep %i",
                    pagetype_caption, payload_bytes, nentries, deep);
        dbi->pages.empty += 1;
        data_tree_problems += !is_gc_tree;
        gc_tree_problems += is_gc_tree;
      }
    }

    if (pgnumber) {
      if (page_bytes != page_size) {
        problem_add("page", pgno, "misused",
                    "%s-page: %" PRIuPTR " != %" PRIuPTR " (%" PRIuPTR
                    "h + %" PRIuPTR "p + %" PRIuPTR "u), deep %i",
                    pagetype_caption, page_size, page_bytes, header_bytes,
                    payload_bytes, unused_bytes, deep);
        if (page_size > page_bytes)
          dbi->lost_bytes += page_size - page_bytes;
        data_tree_problems += !is_gc_tree;
        gc_tree_problems += is_gc_tree;
      } else {
        dbi->payload_bytes += payload_bytes + header_bytes;
        walk.total_payload_bytes += payload_bytes + header_bytes;
      }
    }
  }

  return check_user_break();
}

typedef int(visitor)(const uint64_t record_number, const MDBX_val *key,
                     const MDBX_val *data);
static int process_db(MDBX_dbi dbi_handle, const MDBX_val *dbi_name,
                      visitor *handler);

static int handle_userdb(const uint64_t record_number, const MDBX_val *key,
                         const MDBX_val *data) {
  (void)record_number;
  (void)key;
  (void)data;
  return check_user_break();
}

static int handle_freedb(const uint64_t record_number, const MDBX_val *key,
                         const MDBX_val *data) {
  char *bad = "";
  pgno_t *iptr = data->iov_base;

  if (key->iov_len != sizeof(txnid_t))
    problem_add("entry", record_number, "wrong txn-id size",
                "key-size %" PRIiPTR, key->iov_len);
  else {
    txnid_t txnid;
    memcpy(&txnid, key->iov_base, sizeof(txnid));
    if (txnid < 1 || txnid > envinfo.mi_recent_txnid)
      problem_add("entry", record_number, "wrong txn-id", "%" PRIaTXN, txnid);
    else {
      if (data->iov_len < sizeof(pgno_t) || data->iov_len % sizeof(pgno_t))
        problem_add("entry", txnid, "wrong idl size", "%" PRIuPTR,
                    data->iov_len);
      size_t number = (data->iov_len >= sizeof(pgno_t)) ? *iptr++ : 0;
      if (number < 1 || number > MDBX_PGL_LIMIT)
        problem_add("entry", txnid, "wrong idl length", "%" PRIuPTR, number);
      else if ((number + 1) * sizeof(pgno_t) > data->iov_len) {
        problem_add("entry", txnid, "trimmed idl",
                    "%" PRIuSIZE " > %" PRIuSIZE " (corruption)",
                    (number + 1) * sizeof(pgno_t), data->iov_len);
        number = data->iov_len / sizeof(pgno_t) - 1;
      } else if (data->iov_len - (number + 1) * sizeof(pgno_t) >=
                 /* LY: allow gap up to one page. it is ok
                  * and better than shink-and-retry inside update_gc() */
                 envinfo.mi_dxb_pagesize)
        problem_add("entry", txnid, "extra idl space",
                    "%" PRIuSIZE " < %" PRIuSIZE " (minor, not a trouble)",
                    (number + 1) * sizeof(pgno_t), data->iov_len);

      gc_pages += number;
      if (envinfo.mi_latter_reader_txnid > txnid)
        reclaimable_pages += number;

      pgno_t prev = MDBX_PNL_ASCENDING ? NUM_METAS - 1 : txn->mt_next_pgno;
      pgno_t span = 1;
      for (unsigned i = 0; i < number; ++i) {
        if (check_user_break())
          return MDBX_EINTR;
        const pgno_t pgno = iptr[i];
        if (pgno < NUM_METAS)
          problem_add("entry", txnid, "wrong idl entry",
                      "pgno %" PRIaPGNO " < meta-pages %u", pgno, NUM_METAS);
        else if (pgno >= backed_pages)
          problem_add("entry", txnid, "wrong idl entry",
                      "pgno %" PRIaPGNO " > backed-pages %" PRIu64, pgno,
                      backed_pages);
        else if (pgno >= alloc_pages)
          problem_add("entry", txnid, "wrong idl entry",
                      "pgno %" PRIaPGNO " > alloc-pages %" PRIu64, pgno,
                      alloc_pages - 1);
        else {
          if (MDBX_PNL_DISORDERED(prev, pgno)) {
            bad = " [bad sequence]";
            problem_add("entry", txnid, "bad sequence",
                        "%" PRIaPGNO " %c [%u].%" PRIaPGNO, prev,
                        (prev == pgno) ? '=' : (MDBX_PNL_ASCENDING ? '>' : '<'),
                        i, pgno);
          }
          if (walk.pagemap) {
            int idx = walk.pagemap[pgno];
            if (idx == 0)
              walk.pagemap[pgno] = -1;
            else if (idx > 0)
              problem_add("page", pgno, "already used", "by %s",
                          sdb_name(&walk.dbi[idx - 1].name));
            else
              problem_add("page", pgno, "already listed in GC", nullptr);
          }
        }
        prev = pgno;
        while (i + span < number &&
               iptr[i + span] == (MDBX_PNL_ASCENDING ? pgno_add(pgno, span)
                                                     : pgno_sub(pgno, span)))
          ++span;
      }
      if (verbose > 3 && !only_subdb.iov_base) {
        print("     transaction %" PRIaTXN ", %" PRIuPTR
              " pages, maxspan %" PRIaPGNO "%s\n",
              txnid, number, span, bad);
        if (verbose > 4) {
          for (unsigned i = 0; i < number; i += span) {
            const pgno_t pgno = iptr[i];
            for (span = 1;
                 i + span < number &&
                 iptr[i + span] == (MDBX_PNL_ASCENDING ? pgno_add(pgno, span)
                                                       : pgno_sub(pgno, span));
                 ++span)
              ;
            if (span > 1) {
              print("    %9" PRIaPGNO "[%" PRIaPGNO "]\n", pgno, span);
            } else
              print("    %9" PRIaPGNO "\n", pgno);
          }
        }
      }
    }
  }

  return check_user_break();
}

static int equal_or_greater(const MDBX_val *a, const MDBX_val *b) {
  return eq(*a, *b) ? 0 : 1;
}

static int handle_maindb(const uint64_t record_number, const MDBX_val *key,
                         const MDBX_val *data) {
  if (data->iov_len == sizeof(MDBX_db)) {
    int rc = process_db(~0u, key, handle_userdb);
    if (rc != MDBX_INCOMPATIBLE) {
      userdb_count++;
      return rc;
    }
  }
  return handle_userdb(record_number, key, data);
}

static const char *db_flags2keymode(unsigned flags) {
  flags &= (MDBX_REVERSEKEY | MDBX_INTEGERKEY);
  switch (flags) {
  case 0:
    return "usual";
  case MDBX_REVERSEKEY:
    return "reserve";
  case MDBX_INTEGERKEY:
    return "ordinal";
  case MDBX_REVERSEKEY | MDBX_INTEGERKEY:
    return "msgpack";
  default:
    assert(false);
    __unreachable();
  }
}

static const char *db_flags2valuemode(unsigned flags) {
  flags &= (MDBX_DUPSORT | MDBX_REVERSEDUP | MDBX_DUPFIXED | MDBX_INTEGERDUP);
  switch (flags) {
  case 0:
    return "single";
  case MDBX_DUPSORT:
    return "multi";
  case MDBX_REVERSEDUP:
  case MDBX_DUPSORT | MDBX_REVERSEDUP:
    return "multi-reverse";
  case MDBX_DUPFIXED:
  case MDBX_DUPSORT | MDBX_DUPFIXED:
    return "multi-samelength";
  case MDBX_DUPFIXED | MDBX_REVERSEDUP:
  case MDBX_DUPSORT | MDBX_DUPFIXED | MDBX_REVERSEDUP:
    return "multi-reverse-samelength";
  case MDBX_INTEGERDUP:
  case MDBX_DUPSORT | MDBX_INTEGERDUP:
  case MDBX_DUPSORT | MDBX_DUPFIXED | MDBX_INTEGERDUP:
  case MDBX_DUPFIXED | MDBX_INTEGERDUP:
    return "multi-ordinal";
  case MDBX_INTEGERDUP | MDBX_REVERSEDUP:
  case MDBX_DUPSORT | MDBX_INTEGERDUP | MDBX_REVERSEDUP:
    return "multi-msgpack";
  case MDBX_DUPFIXED | MDBX_INTEGERDUP | MDBX_REVERSEDUP:
  case MDBX_DUPSORT | MDBX_DUPFIXED | MDBX_INTEGERDUP | MDBX_REVERSEDUP:
    return "reserved";
  default:
    assert(false);
    __unreachable();
  }
}

static int process_db(MDBX_dbi dbi_handle, const MDBX_val *dbi_name,
                      visitor *handler) {
  MDBX_cursor *mc;
  MDBX_stat ms;
  MDBX_val key, data;
  MDBX_val prev_key, prev_data;
  unsigned flags;
  int rc, i;
  struct problem *saved_list;
  uint64_t problems_count;
  const bool second_pass = dbi_handle == MAIN_DBI;

  uint64_t record_count = 0, dups = 0;
  uint64_t key_bytes = 0, data_bytes = 0;

  if ((MDBX_TXN_FINISHED | MDBX_TXN_ERROR) & mdbx_txn_flags(txn)) {
    print(" ! abort processing %s due to a previous error\n",
          sdb_name(dbi_name));
    return MDBX_BAD_TXN;
  }

  if (dbi_handle == ~0u) {
    rc = mdbx_dbi_open_ex2(
        txn, dbi_name, MDBX_DB_ACCEDE, &dbi_handle,
        (dbi_name && ignore_wrong_order) ? equal_or_greater : nullptr,
        (dbi_name && ignore_wrong_order) ? equal_or_greater : nullptr);
    if (rc) {
      if (!dbi_name ||
          rc !=
              MDBX_INCOMPATIBLE) /* LY: mainDB's record is not a user's DB. */ {
        error("mdbx_dbi_open(%s) failed, error %d %s\n", sdb_name(dbi_name), rc,
              mdbx_strerror(rc));
      }
      return rc;
    }
  }

  if (dbi_handle >= CORE_DBS && dbi_name && only_subdb.iov_base &&
      !eq(only_subdb, *dbi_name)) {
    if (verbose) {
      print("Skip processing %s...\n", sdb_name(dbi_name));
      fflush(nullptr);
    }
    skipped_subdb++;
    return MDBX_SUCCESS;
  }

  if (!second_pass && verbose)
    print("Processing %s...\n", sdb_name(dbi_name));
  fflush(nullptr);

  rc = mdbx_dbi_flags(txn, dbi_handle, &flags);
  if (rc) {
    error("mdbx_dbi_flags() failed, error %d %s\n", rc, mdbx_strerror(rc));
    return rc;
  }

  rc = mdbx_dbi_stat(txn, dbi_handle, &ms, sizeof(ms));
  if (rc) {
    error("mdbx_dbi_stat() failed, error %d %s\n", rc, mdbx_strerror(rc));
    return rc;
  }

  if (!second_pass && verbose) {
    print(" - key-value kind: %s-key => %s-value", db_flags2keymode(flags),
          db_flags2valuemode(flags));
    if (verbose > 1) {
      print(", flags:");
      if (!flags)
        print(" none");
      else {
        for (i = 0; dbflags[i].bit; i++)
          if (flags & dbflags[i].bit)
            print(" %s", dbflags[i].name);
      }
      if (verbose > 2)
        print(" (0x%02X), dbi-id %d", flags, dbi_handle);
    }
    print("\n");
    if (ms.ms_mod_txnid)
      print(" - last modification txn#%" PRIu64 "\n", ms.ms_mod_txnid);
    if (verbose > 1) {
      print(" - page size %u, entries %" PRIu64 "\n", ms.ms_psize,
            ms.ms_entries);
      print(" - b-tree depth %u, pages: branch %" PRIu64 ", leaf %" PRIu64
            ", overflow %" PRIu64 "\n",
            ms.ms_depth, ms.ms_branch_pages, ms.ms_leaf_pages,
            ms.ms_overflow_pages);
    }
  }

  walk_dbi_t *dbi = (dbi_handle < CORE_DBS)
                        ? &walk.dbi[dbi_handle]
                        : pagemap_lookup_dbi(dbi_name, true);
  if (!dbi) {
    error("too many DBIs or out of memory\n");
    return MDBX_ENOMEM;
  }
  if (!dont_traversal) {
    const uint64_t subtotal_pages =
        ms.ms_branch_pages + ms.ms_leaf_pages + ms.ms_overflow_pages;
    if (subtotal_pages != dbi->pages.total)
      error("%s pages mismatch (%" PRIu64 " != walked %" PRIu64 ")\n",
            "subtotal", subtotal_pages, dbi->pages.total);
    if (ms.ms_branch_pages != dbi->pages.branch)
      error("%s pages mismatch (%" PRIu64 " != walked %" PRIu64 ")\n", "branch",
            ms.ms_branch_pages, dbi->pages.branch);
    const uint64_t allleaf_pages = dbi->pages.leaf + dbi->pages.leaf_dupfixed;
    if (ms.ms_leaf_pages != allleaf_pages)
      error("%s pages mismatch (%" PRIu64 " != walked %" PRIu64 ")\n",
            "all-leaf", ms.ms_leaf_pages, allleaf_pages);
    if (ms.ms_overflow_pages != dbi->pages.large_volume)
      error("%s pages mismatch (%" PRIu64 " != walked %" PRIu64 ")\n",
            "large/overlow", ms.ms_overflow_pages, dbi->pages.large_volume);
  }
  rc = mdbx_cursor_open(txn, dbi_handle, &mc);
  if (rc) {
    error("mdbx_cursor_open() failed, error %d %s\n", rc, mdbx_strerror(rc));
    return rc;
  }

  if (ignore_wrong_order) { /* for debugging with enabled assertions */
    mc->mc_checking |= CC_SKIPORD;
    if (mc->mc_xcursor)
      mc->mc_xcursor->mx_cursor.mc_checking |= CC_SKIPORD;
  }

  const size_t maxkeysize = mdbx_env_get_maxkeysize_ex(env, flags);
  saved_list = problems_push();
  prev_key.iov_base = nullptr;
  prev_key.iov_len = 0;
  prev_data.iov_base = nullptr;
  prev_data.iov_len = 0;
  rc = mdbx_cursor_get(mc, &key, &data, MDBX_FIRST);
  while (rc == MDBX_SUCCESS) {
    rc = check_user_break();
    if (rc)
      goto bailout;

    if (!second_pass) {
      bool bad_key = false;
      if (key.iov_len > maxkeysize) {
        problem_add("entry", record_count, "key length exceeds max-key-size",
                    "%" PRIuPTR " > %" PRIuPTR, key.iov_len, maxkeysize);
        bad_key = true;
      } else if ((flags & MDBX_INTEGERKEY) && key.iov_len != sizeof(uint64_t) &&
                 key.iov_len != sizeof(uint32_t)) {
        problem_add("entry", record_count, "wrong key length",
                    "%" PRIuPTR " != 4or8", key.iov_len);
        bad_key = true;
      }

      bool bad_data = false;
      if ((flags & MDBX_INTEGERDUP) && data.iov_len != sizeof(uint64_t) &&
          data.iov_len != sizeof(uint32_t)) {
        problem_add("entry", record_count, "wrong data length",
                    "%" PRIuPTR " != 4or8", data.iov_len);
        bad_data = true;
      }

      if (prev_key.iov_base) {
        if (prev_data.iov_base && !bad_data && (flags & MDBX_DUPFIXED) &&
            prev_data.iov_len != data.iov_len) {
          problem_add("entry", record_count, "different data length",
                      "%" PRIuPTR " != %" PRIuPTR, prev_data.iov_len,
                      data.iov_len);
          bad_data = true;
        }

        if (!bad_key) {
          int cmp = mdbx_cmp(txn, dbi_handle, &key, &prev_key);
          if (cmp == 0) {
            ++dups;
            if ((flags & MDBX_DUPSORT) == 0) {
              problem_add("entry", record_count, "duplicated entries", nullptr);
              if (prev_data.iov_base && data.iov_len == prev_data.iov_len &&
                  memcmp(data.iov_base, prev_data.iov_base, data.iov_len) ==
                      0) {
                problem_add("entry", record_count, "complete duplicate",
                            nullptr);
              }
            } else if (!bad_data && prev_data.iov_base) {
              cmp = mdbx_dcmp(txn, dbi_handle, &data, &prev_data);
              if (cmp == 0) {
                problem_add("entry", record_count, "complete duplicate",
                            nullptr);
              } else if (cmp < 0 && !ignore_wrong_order) {
                problem_add("entry", record_count,
                            "wrong order of multi-values", nullptr);
              }
            }
          } else if (cmp < 0 && !ignore_wrong_order) {
            problem_add("entry", record_count, "wrong order of entries",
                        nullptr);
          }
        }
      }

      if (!bad_key) {
        if (verbose && (flags & MDBX_INTEGERKEY) && !prev_key.iov_base)
          print(" - fixed key-size %" PRIuPTR "\n", key.iov_len);
        prev_key = key;
      }
      if (!bad_data) {
        if (verbose && (flags & (MDBX_INTEGERDUP | MDBX_DUPFIXED)) &&
            !prev_data.iov_base)
          print(" - fixed data-size %" PRIuPTR "\n", data.iov_len);
        prev_data = data;
      }
    }

    if (handler) {
      rc = handler(record_count, &key, &data);
      if (MDBX_IS_ERROR(rc))
        goto bailout;
    }

    record_count++;
    key_bytes += key.iov_len;
    data_bytes += data.iov_len;

    rc = mdbx_cursor_get(mc, &key, &data, MDBX_NEXT);
  }
  if (rc != MDBX_NOTFOUND)
    error("mdbx_cursor_get() failed, error %d %s\n", rc, mdbx_strerror(rc));
  else
    rc = 0;

  if (record_count != ms.ms_entries)
    problem_add("entry", record_count, "different number of entries",
                "%" PRIu64 " != %" PRIu64, record_count, ms.ms_entries);
bailout:
  problems_count = problems_pop(saved_list);
  if (!second_pass && verbose) {
    print(" - summary: %" PRIu64 " records, %" PRIu64 " dups, %" PRIu64
          " key's bytes, %" PRIu64 " data's "
          "bytes, %" PRIu64 " problems\n",
          record_count, dups, key_bytes, data_bytes, problems_count);
    fflush(nullptr);
  }

  mdbx_cursor_close(mc);
  return (rc || problems_count) ? MDBX_RESULT_TRUE : MDBX_SUCCESS;
}

static void usage(char *prog) {
  fprintf(
      stderr,
      "usage: %s "
      "[-V] [-v] [-q] [-c] [-0|1|2] [-w] [-d] [-i] [-s subdb] [-u|U] dbpath\n"
      "  -V\t\tprint version and exit\n"
      "  -v\t\tmore verbose, could be used multiple times\n"
      "  -q\t\tbe quiet\n"
      "  -c\t\tforce cooperative mode (don't try exclusive)\n"
      "  -w\t\twrite-mode checking\n"
      "  -d\t\tdisable page-by-page traversal of B-tree\n"
      "  -i\t\tignore wrong order errors (for custom comparators case)\n"
      "  -s subdb\tprocess a specific subdatabase only\n"
      "  -u\t\twarmup database before checking\n"
      "  -U\t\twarmup and try lock database pages in memory before checking\n"
      "  -0|1|2\tforce using specific meta-page 0, or 2 for checking\n"
      "  -t\t\tturn to a specified meta-page on successful check\n"
      "  -T\t\tturn to a specified meta-page EVEN ON UNSUCCESSFUL CHECK!\n",
      prog);
  exit(EXIT_INTERRUPTED);
}

static bool meta_ot(txnid_t txn_a, uint64_t sign_a, txnid_t txn_b,
                    uint64_t sign_b, const bool wanna_steady) {
  if (txn_a == txn_b)
    return SIGN_IS_STEADY(sign_b);

  if (wanna_steady && SIGN_IS_STEADY(sign_a) != SIGN_IS_STEADY(sign_b))
    return SIGN_IS_STEADY(sign_b);

  return txn_a < txn_b;
}

static bool meta_eq(txnid_t txn_a, uint64_t sign_a, txnid_t txn_b,
                    uint64_t sign_b) {
  if (!txn_a || txn_a != txn_b)
    return false;

  if (SIGN_IS_STEADY(sign_a) != SIGN_IS_STEADY(sign_b))
    return false;

  return true;
}

static int meta_recent(const bool wanna_steady) {
  if (meta_ot(envinfo.mi_meta0_txnid, envinfo.mi_meta0_sign,
              envinfo.mi_meta1_txnid, envinfo.mi_meta1_sign, wanna_steady))
    return meta_ot(envinfo.mi_meta2_txnid, envinfo.mi_meta2_sign,
                   envinfo.mi_meta1_txnid, envinfo.mi_meta1_sign, wanna_steady)
               ? 1
               : 2;
  else
    return meta_ot(envinfo.mi_meta0_txnid, envinfo.mi_meta0_sign,
                   envinfo.mi_meta2_txnid, envinfo.mi_meta2_sign, wanna_steady)
               ? 2
               : 0;
}

static int meta_tail(int head) {
  switch (head) {
  case 0:
    return meta_ot(envinfo.mi_meta1_txnid, envinfo.mi_meta1_sign,
                   envinfo.mi_meta2_txnid, envinfo.mi_meta2_sign, true)
               ? 1
               : 2;
  case 1:
    return meta_ot(envinfo.mi_meta0_txnid, envinfo.mi_meta0_sign,
                   envinfo.mi_meta2_txnid, envinfo.mi_meta2_sign, true)
               ? 0
               : 2;
  case 2:
    return meta_ot(envinfo.mi_meta0_txnid, envinfo.mi_meta0_sign,
                   envinfo.mi_meta1_txnid, envinfo.mi_meta1_sign, true)
               ? 0
               : 1;
  default:
    assert(false);
    return -1;
  }
}

static int meta_head(void) { return meta_recent(false); }

void verbose_meta(int num, txnid_t txnid, uint64_t sign, uint64_t bootid_x,
                  uint64_t bootid_y) {
  const bool have_bootid = (bootid_x | bootid_y) != 0;
  const bool bootid_match = bootid_x == envinfo.mi_bootid.current.x &&
                            bootid_y == envinfo.mi_bootid.current.y;

  print(" - meta-%d: ", num);
  switch (sign) {
  case MDBX_DATASIGN_NONE:
    print("no-sync/legacy");
    break;
  case MDBX_DATASIGN_WEAK:
    print("weak-%s", bootid_match ? (have_bootid ? "intact (same boot-id)"
                                                 : "unknown (no boot-id")
                                  : "dead");
    break;
  default:
    print("steady");
    break;
  }
  print(" txn#%" PRIu64, txnid);

  const int head = meta_head();
  if (num == head)
    print(", head");
  else if (num == meta_tail(head))
    print(", tail");
  else
    print(", stay");

  if (stuck_meta >= 0) {
    if (num == stuck_meta)
      print(", forced for checking");
  } else if (txnid > envinfo.mi_recent_txnid &&
             (envflags & (MDBX_EXCLUSIVE | MDBX_RDONLY)) == MDBX_EXCLUSIVE)
    print(", rolled-back %" PRIu64 " (%" PRIu64 " >>> %" PRIu64 ")",
          txnid - envinfo.mi_recent_txnid, txnid, envinfo.mi_recent_txnid);
  print("\n");
}

static uint64_t get_meta_txnid(const unsigned meta_id) {
  switch (meta_id) {
  default:
    assert(false);
    error("unexpected meta_id %u\n", meta_id);
    return 0;
  case 0:
    return envinfo.mi_meta0_txnid;
  case 1:
    return envinfo.mi_meta1_txnid;
  case 2:
    return envinfo.mi_meta2_txnid;
  }
}

static void print_size(const char *prefix, const uint64_t value,
                       const char *suffix) {
  const char sf[] =
      "KMGTPEZY"; /* LY: Kilo, Mega, Giga, Tera, Peta, Exa, Zetta, Yotta! */
  double k = 1024.0;
  size_t i;
  for (i = 0; sf[i + 1] && value / k > 1000.0; ++i)
    k *= 1024;
  print("%s%" PRIu64 " (%.2f %cb)%s", prefix, value, value / k, sf[i], suffix);
}

int main(int argc, char *argv[]) {
  int rc;
  char *prog = argv[0];
  char *envname;
  unsigned problems_maindb = 0, problems_freedb = 0, problems_meta = 0;
  bool write_locked = false;
  bool turn_meta = false;
  bool force_turn_meta = false;
  bool warmup = false;
  MDBX_warmup_flags_t warmup_flags = MDBX_warmup_default;

  double elapsed;
#if defined(_WIN32) || defined(_WIN64)
  uint64_t timestamp_start, timestamp_finish;
  timestamp_start = GetMilliseconds();
#else
  struct timespec timestamp_start, timestamp_finish;
  if (clock_gettime(CLOCK_MONOTONIC, &timestamp_start)) {
    rc = errno;
    error("clock_gettime() failed, error %d %s\n", rc, mdbx_strerror(rc));
    return EXIT_FAILURE_SYS;
  }
#endif

  dbi_meta.name.iov_base = MDBX_PGWALK_META;
  dbi_free.name.iov_base = MDBX_PGWALK_GC;
  dbi_main.name.iov_base = MDBX_PGWALK_MAIN;
  atexit(pagemap_cleanup);

  if (argc < 2)
    usage(prog);

  for (int i; (i = getopt(argc, argv,
                          "uU"
                          "0"
                          "1"
                          "2"
                          "T"
                          "V"
                          "v"
                          "q"
                          "n"
                          "w"
                          "c"
                          "t"
                          "d"
                          "i"
                          "s:")) != EOF;) {
    switch (i) {
    case 'V':
      printf("mdbx_chk version %d.%d.%d.%d\n"
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
    case 'v':
      verbose++;
      break;
    case '0':
      stuck_meta = 0;
      break;
    case '1':
      stuck_meta = 1;
      break;
    case '2':
      stuck_meta = 2;
      break;
    case 't':
      turn_meta = true;
      break;
    case 'T':
      turn_meta = force_turn_meta = true;
      quiet = false;
      if (verbose < 2)
        verbose = 2;
      break;
    case 'q':
      quiet = true;
      break;
    case 'n':
      break;
    case 'w':
      envflags &= ~MDBX_RDONLY;
#if MDBX_MMAP_INCOHERENT_FILE_WRITE
      /* Temporary `workaround` for OpenBSD kernel's flaw.
       * See https://libmdbx.dqdkfa.ru/dead-github/issues/67 */
      envflags |= MDBX_WRITEMAP;
#endif /* MDBX_MMAP_INCOHERENT_FILE_WRITE */
      break;
    case 'c':
      envflags = (envflags & ~MDBX_EXCLUSIVE) | MDBX_ACCEDE;
      break;
    case 'd':
      dont_traversal = true;
      break;
    case 's':
      if (only_subdb.iov_base && strcmp(only_subdb.iov_base, optarg))
        usage(prog);
      only_subdb.iov_base = optarg;
      only_subdb.iov_len = strlen(optarg);
      break;
    case 'i':
      ignore_wrong_order = true;
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
      usage(prog);
    }
  }

  if (optind != argc - 1)
    usage(prog);

  rc = MDBX_SUCCESS;
  if (stuck_meta >= 0 && (envflags & MDBX_EXCLUSIVE) == 0) {
    error("exclusive mode is required to using specific meta-page(%d) for "
          "checking.\n",
          stuck_meta);
    rc = EXIT_INTERRUPTED;
  }
  if (turn_meta) {
    if (stuck_meta < 0) {
      error("meta-page must be specified (by -0, -1 or -2 options) to turn to "
            "it.\n");
      rc = EXIT_INTERRUPTED;
    }
    if (envflags & MDBX_RDONLY) {
      error("write-mode must be enabled to turn to the specified meta-page.\n");
      rc = EXIT_INTERRUPTED;
    }
    if (only_subdb.iov_base || dont_traversal) {
      error(
          "whole database checking with b-tree traversal are required to turn "
          "to the specified meta-page.\n");
      rc = EXIT_INTERRUPTED;
    }
  }
  if (rc)
    exit(rc);

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
  print("mdbx_chk %s (%s, T-%s)\nRunning for %s in 'read-%s' mode...\n",
        mdbx_version.git.describe, mdbx_version.git.datetime,
        mdbx_version.git.tree, envname,
        (envflags & MDBX_RDONLY) ? "only" : "write");
  fflush(nullptr);
  mdbx_setup_debug((verbose < MDBX_LOG_TRACE - 1)
                       ? (MDBX_log_level_t)(verbose + 1)
                       : MDBX_LOG_TRACE,
                   MDBX_DBG_DUMP | MDBX_DBG_ASSERT | MDBX_DBG_AUDIT |
                       MDBX_DBG_LEGACY_OVERLAP | MDBX_DBG_DONT_UPGRADE,
                   logger);

  rc = mdbx_env_create(&env);
  if (rc) {
    error("mdbx_env_create() failed, error %d %s\n", rc, mdbx_strerror(rc));
    return rc < 0 ? EXIT_FAILURE_MDBX : EXIT_FAILURE_SYS;
  }

  rc = mdbx_env_set_maxdbs(env, MDBX_MAX_DBI);
  if (rc) {
    error("mdbx_env_set_maxdbs() failed, error %d %s\n", rc, mdbx_strerror(rc));
    goto bailout;
  }

  if (stuck_meta >= 0) {
    rc = mdbx_env_open_for_recovery(env, envname, stuck_meta,
                                    (envflags & MDBX_RDONLY) ? false : true);
  } else {
    rc = mdbx_env_open(env, envname, envflags, 0);
    if ((envflags & MDBX_EXCLUSIVE) &&
        (rc == MDBX_BUSY ||
#if defined(_WIN32) || defined(_WIN64)
         rc == ERROR_LOCK_VIOLATION || rc == ERROR_SHARING_VIOLATION
#else
         rc == EBUSY || rc == EAGAIN
#endif
         )) {
      envflags &= ~MDBX_EXCLUSIVE;
      rc = mdbx_env_open(env, envname, envflags | MDBX_ACCEDE, 0);
    }
  }

  if (rc) {
    error("mdbx_env_open() failed, error %d %s\n", rc, mdbx_strerror(rc));
    if (rc == MDBX_WANNA_RECOVERY && (envflags & MDBX_RDONLY))
      print("Please run %s in the read-write mode (with '-w' option).\n", prog);
    goto bailout;
  }
  if (verbose)
    print(" - %s mode\n",
          (envflags & MDBX_EXCLUSIVE) ? "monopolistic" : "cooperative");

  if ((envflags & (MDBX_RDONLY | MDBX_EXCLUSIVE)) == 0) {
    if (verbose) {
      print(" - taking write lock...");
      fflush(nullptr);
    }
    rc = mdbx_txn_lock(env, false);
    if (rc != MDBX_SUCCESS) {
      error("mdbx_txn_lock() failed, error %d %s\n", rc, mdbx_strerror(rc));
      goto bailout;
    }
    if (verbose)
      print(" done\n");
    write_locked = true;
  }

  if (warmup) {
    if (verbose) {
      print(" - warming up...");
      fflush(nullptr);
    }
    rc = mdbx_env_warmup(env, nullptr, warmup_flags, 3600 * 65536);
    if (MDBX_IS_ERROR(rc)) {
      error("mdbx_env_warmup(flags %u) failed, error %d %s\n", warmup_flags, rc,
            mdbx_strerror(rc));
      goto bailout;
    }
    if (verbose)
      print(" %s\n", rc ? "timeout" : "done");
  }

  rc = mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &txn);
  if (rc) {
    error("mdbx_txn_begin() failed, error %d %s\n", rc, mdbx_strerror(rc));
    goto bailout;
  }

  rc = mdbx_env_info_ex(env, txn, &envinfo, sizeof(envinfo));
  if (rc) {
    error("mdbx_env_info_ex() failed, error %d %s\n", rc, mdbx_strerror(rc));
    goto bailout;
  }
  if (verbose) {
    print(" - current boot-id ");
    if (envinfo.mi_bootid.current.x | envinfo.mi_bootid.current.y)
      print("%016" PRIx64 "-%016" PRIx64 "\n", envinfo.mi_bootid.current.x,
            envinfo.mi_bootid.current.y);
    else
      print("unavailable\n");
  }

  mdbx_filehandle_t dxb_fd;
  rc = mdbx_env_get_fd(env, &dxb_fd);
  if (rc) {
    error("mdbx_env_get_fd() failed, error %d %s\n", rc, mdbx_strerror(rc));
    goto bailout;
  }

  uint64_t dxb_filesize = 0;
#if defined(_WIN32) || defined(_WIN64)
  {
    BY_HANDLE_FILE_INFORMATION info;
    if (!GetFileInformationByHandle(dxb_fd, &info))
      rc = GetLastError();
    else
      dxb_filesize = info.nFileSizeLow | (uint64_t)info.nFileSizeHigh << 32;
  }
#else
  {
    struct stat st;
    STATIC_ASSERT_MSG(sizeof(off_t) <= sizeof(uint64_t),
                      "libmdbx requires 64-bit file I/O on 64-bit systems");
    if (fstat(dxb_fd, &st))
      rc = errno;
    else
      dxb_filesize = st.st_size;
  }
#endif
  if (rc) {
    error("osal_filesize() failed, error %d %s\n", rc, mdbx_strerror(rc));
    goto bailout;
  }

  errno = 0;
  const uint64_t dxbfile_pages = dxb_filesize / envinfo.mi_dxb_pagesize;
  alloc_pages = txn->mt_next_pgno;
  backed_pages = envinfo.mi_geo.current / envinfo.mi_dxb_pagesize;
  if (backed_pages > dxbfile_pages) {
    print(" ! backed-pages %" PRIu64 " > file-pages %" PRIu64 "\n",
          backed_pages, dxbfile_pages);
    ++problems_meta;
  }
  if (dxbfile_pages < NUM_METAS)
    print(" ! file-pages %" PRIu64 " < %u\n", dxbfile_pages, NUM_METAS);
  if (backed_pages < NUM_METAS)
    print(" ! backed-pages %" PRIu64 " < %u\n", backed_pages, NUM_METAS);
  if (backed_pages < NUM_METAS || dxbfile_pages < NUM_METAS)
    goto bailout;
  if (backed_pages > MAX_PAGENO + 1) {
    print(" ! backed-pages %" PRIu64 " > max-pages %" PRIaPGNO "\n",
          backed_pages, MAX_PAGENO + 1);
    ++problems_meta;
    backed_pages = MAX_PAGENO + 1;
  }

  if ((envflags & (MDBX_EXCLUSIVE | MDBX_RDONLY)) != MDBX_RDONLY) {
    if (backed_pages > dxbfile_pages) {
      print(" ! backed-pages %" PRIu64 " > file-pages %" PRIu64 "\n",
            backed_pages, dxbfile_pages);
      ++problems_meta;
      backed_pages = dxbfile_pages;
    }
    if (alloc_pages > backed_pages) {
      print(" ! alloc-pages %" PRIu64 " > backed-pages %" PRIu64 "\n",
            alloc_pages, backed_pages);
      ++problems_meta;
      alloc_pages = backed_pages;
    }
  } else {
    /* LY: DB may be shrinked by writer down to the allocated pages. */
    if (alloc_pages > backed_pages) {
      print(" ! alloc-pages %" PRIu64 " > backed-pages %" PRIu64 "\n",
            alloc_pages, backed_pages);
      ++problems_meta;
      alloc_pages = backed_pages;
    }
    if (alloc_pages > dxbfile_pages) {
      print(" ! alloc-pages %" PRIu64 " > file-pages %" PRIu64 "\n",
            alloc_pages, dxbfile_pages);
      ++problems_meta;
      alloc_pages = dxbfile_pages;
    }
    if (backed_pages > dxbfile_pages)
      backed_pages = dxbfile_pages;
  }

  if (verbose) {
    print(" - pagesize %u (%u system), max keysize %d..%d"
          ", max readers %u\n",
          envinfo.mi_dxb_pagesize, envinfo.mi_sys_pagesize,
          mdbx_env_get_maxkeysize_ex(env, MDBX_DUPSORT),
          mdbx_env_get_maxkeysize_ex(env, 0), envinfo.mi_maxreaders);
    print_size(" - mapsize ", envinfo.mi_mapsize, "\n");
    if (envinfo.mi_geo.lower == envinfo.mi_geo.upper)
      print_size(" - fixed datafile: ", envinfo.mi_geo.current, "");
    else {
      print_size(" - dynamic datafile: ", envinfo.mi_geo.lower, "");
      print_size(" .. ", envinfo.mi_geo.upper, ", ");
      print_size("+", envinfo.mi_geo.grow, ", ");
      print_size("-", envinfo.mi_geo.shrink, "\n");
      print_size(" - current datafile: ", envinfo.mi_geo.current, "");
    }
    printf(", %" PRIu64 " pages\n",
           envinfo.mi_geo.current / envinfo.mi_dxb_pagesize);
#if defined(_WIN32) || defined(_WIN64)
    if (envinfo.mi_geo.shrink && envinfo.mi_geo.current != envinfo.mi_geo.upper)
      print(
          "                     WARNING: Due Windows system limitations a "
          "file couldn't\n                     be truncated while the database "
          "is opened. So, the size\n                     database file "
          "of may by large than the database itself,\n                     "
          "until it will be closed or reopened in read-write mode.\n");
#endif
    verbose_meta(0, envinfo.mi_meta0_txnid, envinfo.mi_meta0_sign,
                 envinfo.mi_bootid.meta0.x, envinfo.mi_bootid.meta0.y);
    verbose_meta(1, envinfo.mi_meta1_txnid, envinfo.mi_meta1_sign,
                 envinfo.mi_bootid.meta1.x, envinfo.mi_bootid.meta1.y);
    verbose_meta(2, envinfo.mi_meta2_txnid, envinfo.mi_meta2_sign,
                 envinfo.mi_bootid.meta2.x, envinfo.mi_bootid.meta2.y);
  }

  if (stuck_meta >= 0) {
    if (verbose) {
      print(" - skip checking meta-pages since the %u"
            " is selected for verification\n",
            stuck_meta);
      print(" - transactions: recent %" PRIu64
            ", selected for verification %" PRIu64 ", lag %" PRIi64 "\n",
            envinfo.mi_recent_txnid, get_meta_txnid(stuck_meta),
            envinfo.mi_recent_txnid - get_meta_txnid(stuck_meta));
    }
  } else {
    if (verbose > 1)
      print(" - performs check for meta-pages clashes\n");
    if (meta_eq(envinfo.mi_meta0_txnid, envinfo.mi_meta0_sign,
                envinfo.mi_meta1_txnid, envinfo.mi_meta1_sign)) {
      print(" ! meta-%d and meta-%d are clashed\n", 0, 1);
      ++problems_meta;
    }
    if (meta_eq(envinfo.mi_meta1_txnid, envinfo.mi_meta1_sign,
                envinfo.mi_meta2_txnid, envinfo.mi_meta2_sign)) {
      print(" ! meta-%d and meta-%d are clashed\n", 1, 2);
      ++problems_meta;
    }
    if (meta_eq(envinfo.mi_meta2_txnid, envinfo.mi_meta2_sign,
                envinfo.mi_meta0_txnid, envinfo.mi_meta0_sign)) {
      print(" ! meta-%d and meta-%d are clashed\n", 2, 0);
      ++problems_meta;
    }

    const unsigned steady_meta_id = meta_recent(true);
    const uint64_t steady_meta_txnid = get_meta_txnid(steady_meta_id);
    const unsigned weak_meta_id = meta_recent(false);
    const uint64_t weak_meta_txnid = get_meta_txnid(weak_meta_id);
    if (envflags & MDBX_EXCLUSIVE) {
      if (verbose > 1)
        print(" - performs full check recent-txn-id with meta-pages\n");
      if (steady_meta_txnid != envinfo.mi_recent_txnid) {
        print(" ! steady meta-%d txn-id mismatch recent-txn-id (%" PRIi64
              " != %" PRIi64 ")\n",
              steady_meta_id, steady_meta_txnid, envinfo.mi_recent_txnid);
        ++problems_meta;
      }
    } else if (write_locked) {
      if (verbose > 1)
        print(" - performs lite check recent-txn-id with meta-pages (not a "
              "monopolistic mode)\n");
      if (weak_meta_txnid != envinfo.mi_recent_txnid) {
        print(" ! weak meta-%d txn-id mismatch recent-txn-id (%" PRIi64
              " != %" PRIi64 ")\n",
              weak_meta_id, weak_meta_txnid, envinfo.mi_recent_txnid);
        ++problems_meta;
      }
    } else if (verbose) {
      print(" - skip check recent-txn-id with meta-pages (monopolistic or "
            "read-write mode only)\n");
    }
    total_problems += problems_meta;

    if (verbose)
      print(" - transactions: recent %" PRIu64 ", latter reader %" PRIu64
            ", lag %" PRIi64 "\n",
            envinfo.mi_recent_txnid, envinfo.mi_latter_reader_txnid,
            envinfo.mi_recent_txnid - envinfo.mi_latter_reader_txnid);
  }

  if (!dont_traversal) {
    struct problem *saved_list;
    size_t traversal_problems;
    uint64_t empty_pages, lost_bytes;

    print("Traversal b-tree by txn#%" PRIaTXN "...\n", txn->mt_txnid);
    fflush(nullptr);
    walk.pagemap = osal_calloc((size_t)backed_pages, sizeof(*walk.pagemap));
    if (!walk.pagemap) {
      rc = errno ? errno : MDBX_ENOMEM;
      error("calloc() failed, error %d %s\n", rc, mdbx_strerror(rc));
      goto bailout;
    }

    saved_list = problems_push();
    rc = mdbx_env_pgwalk(txn, pgvisitor, nullptr,
                         true /* always skip key ordering checking to avoid
                               MDBX_CORRUPTED when using custom comparators */);
    traversal_problems = problems_pop(saved_list);

    if (rc) {
      if (rc != MDBX_EINTR || !check_user_break())
        error("mdbx_env_pgwalk() failed, error %d %s\n", rc, mdbx_strerror(rc));
      goto bailout;
    }

    for (uint64_t n = 0; n < alloc_pages; ++n)
      if (!walk.pagemap[n])
        unused_pages += 1;

    empty_pages = lost_bytes = 0;
    for (walk_dbi_t *dbi = &dbi_main;
         dbi < ARRAY_END(walk.dbi) && dbi->name.iov_base; ++dbi) {
      empty_pages += dbi->pages.empty;
      lost_bytes += dbi->lost_bytes;
    }

    if (verbose) {
      uint64_t total_page_bytes = walk.pgcount * envinfo.mi_dxb_pagesize;
      print(" - pages: walked %" PRIu64 ", left/unused %" PRIu64 "\n",
            walk.pgcount, unused_pages);
      if (verbose > 1) {
        for (walk_dbi_t *dbi = walk.dbi;
             dbi < ARRAY_END(walk.dbi) && dbi->name.iov_base; ++dbi) {
          print("     %s: subtotal %" PRIu64, sdb_name(&dbi->name),
                dbi->pages.total);
          if (dbi->pages.other && dbi->pages.other != dbi->pages.total)
            print(", other %" PRIu64, dbi->pages.other);
          if (dbi->pages.branch)
            print(", branch %" PRIu64, dbi->pages.branch);
          if (dbi->pages.large_count)
            print(", large %" PRIu64, dbi->pages.large_count);
          uint64_t all_leaf = dbi->pages.leaf + dbi->pages.leaf_dupfixed;
          if (all_leaf) {
            print(", leaf %" PRIu64, all_leaf);
            if (verbose > 2 &&
                (dbi->pages.subleaf_dupsort | dbi->pages.leaf_dupfixed |
                 dbi->pages.subleaf_dupfixed))
              print(" (usual %" PRIu64 ", sub-dupsort %" PRIu64
                    ", dupfixed %" PRIu64 ", sub-dupfixed %" PRIu64 ")",
                    dbi->pages.leaf, dbi->pages.subleaf_dupsort,
                    dbi->pages.leaf_dupfixed, dbi->pages.subleaf_dupfixed);
          }
          print("\n");
        }
      }

      if (verbose > 1)
        print(" - usage: total %" PRIu64 " bytes, payload %" PRIu64
              " (%.1f%%), unused "
              "%" PRIu64 " (%.1f%%)\n",
              total_page_bytes, walk.total_payload_bytes,
              walk.total_payload_bytes * 100.0 / total_page_bytes,
              total_page_bytes - walk.total_payload_bytes,
              (total_page_bytes - walk.total_payload_bytes) * 100.0 /
                  total_page_bytes);
      if (verbose > 2) {
        for (walk_dbi_t *dbi = walk.dbi;
             dbi < ARRAY_END(walk.dbi) && dbi->name.iov_base; ++dbi)
          if (dbi->pages.total) {
            uint64_t dbi_bytes = dbi->pages.total * envinfo.mi_dxb_pagesize;
            print("     %s: subtotal %" PRIu64 " bytes (%.1f%%),"
                  " payload %" PRIu64 " (%.1f%%), unused %" PRIu64 " (%.1f%%)",
                  sdb_name(&dbi->name), dbi_bytes,
                  dbi_bytes * 100.0 / total_page_bytes, dbi->payload_bytes,
                  dbi->payload_bytes * 100.0 / dbi_bytes,
                  dbi_bytes - dbi->payload_bytes,
                  (dbi_bytes - dbi->payload_bytes) * 100.0 / dbi_bytes);
            if (dbi->pages.empty)
              print(", %" PRIu64 " empty pages", dbi->pages.empty);
            if (dbi->lost_bytes)
              print(", %" PRIu64 " bytes lost", dbi->lost_bytes);
            print("\n");
          } else
            print("     %s: empty\n", sdb_name(&dbi->name));
      }
      print(" - summary: average fill %.1f%%",
            walk.total_payload_bytes * 100.0 / total_page_bytes);
      if (empty_pages)
        print(", %" PRIu64 " empty pages", empty_pages);
      if (lost_bytes)
        print(", %" PRIu64 " bytes lost", lost_bytes);
      print(", %" PRIuPTR " problems\n", traversal_problems);
    }
  } else if (verbose) {
    print("Skipping b-tree walk...\n");
    fflush(nullptr);
  }

  if (gc_tree_problems) {
    print("Skip processing %s since %s is corrupted (%u problems)\n", "@GC",
          "b-tree", gc_tree_problems);
    problems_freedb = gc_tree_problems;
  } else
    problems_freedb = process_db(FREE_DBI, MDBX_PGWALK_GC, handle_freedb);

  if (verbose) {
    uint64_t value = envinfo.mi_mapsize / envinfo.mi_dxb_pagesize;
    double percent = value / 100.0;
    print(" - space: %" PRIu64 " total pages", value);
    print(", backed %" PRIu64 " (%.1f%%)", backed_pages,
          backed_pages / percent);
    print(", allocated %" PRIu64 " (%.1f%%)", alloc_pages,
          alloc_pages / percent);

    if (verbose > 1) {
      value = envinfo.mi_mapsize / envinfo.mi_dxb_pagesize - alloc_pages;
      print(", remained %" PRIu64 " (%.1f%%)", value, value / percent);

      value = dont_traversal ? alloc_pages - gc_pages : walk.pgcount;
      print(", used %" PRIu64 " (%.1f%%)", value, value / percent);

      print(", gc %" PRIu64 " (%.1f%%)", gc_pages, gc_pages / percent);

      value = gc_pages - reclaimable_pages;
      print(", detained %" PRIu64 " (%.1f%%)", value, value / percent);

      print(", reclaimable %" PRIu64 " (%.1f%%)", reclaimable_pages,
            reclaimable_pages / percent);
    }

    value = envinfo.mi_mapsize / envinfo.mi_dxb_pagesize - alloc_pages +
            reclaimable_pages;
    print(", available %" PRIu64 " (%.1f%%)\n", value, value / percent);
  }

  if ((problems_maindb = data_tree_problems) == 0 && problems_freedb == 0) {
    if (!dont_traversal &&
        (envflags & (MDBX_EXCLUSIVE | MDBX_RDONLY)) != MDBX_RDONLY) {
      if (walk.pgcount != alloc_pages - gc_pages) {
        error("used pages mismatch (%" PRIu64 "(walked) != %" PRIu64
              "(allocated - GC))\n",
              walk.pgcount, alloc_pages - gc_pages);
      }
      if (unused_pages != gc_pages) {
        error("GC pages mismatch (%" PRIu64 "(expected) != %" PRIu64 "(GC))\n",
              unused_pages, gc_pages);
      }
    } else if (verbose) {
      print(" - skip check used and GC pages (btree-traversal with "
            "monopolistic or read-write mode only)\n");
    }

    problems_maindb = process_db(~0u, /* MAIN_DBI */ nullptr, nullptr);
    if (problems_maindb == 0) {
      print("Scanning %s for %s...\n", "@MAIN", "sub-database(s)");
      if (!process_db(MAIN_DBI, nullptr, handle_maindb)) {
        if (!userdb_count && verbose)
          print(" - does not contain multiple databases\n");
      }
    } else {
      print("Skip processing %s since %s is corrupted (%u problems)\n",
            "sub-database(s)", "@MAIN", problems_maindb);
    }
  } else {
    print("Skip processing %s since %s is corrupted (%u problems)\n", "@MAIN",
          "b-tree", data_tree_problems);
  }

  if (rc == 0 && total_problems == 1 && problems_meta == 1 && !dont_traversal &&
      (envflags & MDBX_RDONLY) == 0 && !only_subdb.iov_base && stuck_meta < 0 &&
      get_meta_txnid(meta_recent(true)) < envinfo.mi_recent_txnid) {
    print("Perform sync-to-disk for make steady checkpoint at txn-id #%" PRIi64
          "\n",
          envinfo.mi_recent_txnid);
    fflush(nullptr);
    if (write_locked) {
      mdbx_txn_unlock(env);
      write_locked = false;
    }
    rc = mdbx_env_sync_ex(env, true, false);
    if (rc != MDBX_SUCCESS)
      error("mdbx_env_pgwalk() failed, error %d %s\n", rc, mdbx_strerror(rc));
    else {
      total_problems -= 1;
      problems_meta -= 1;
    }
  }

  if (turn_meta && stuck_meta >= 0 && !dont_traversal && !only_subdb.iov_base &&
      (envflags & (MDBX_RDONLY | MDBX_EXCLUSIVE)) == MDBX_EXCLUSIVE) {
    const bool successful_check = (rc | total_problems | problems_meta) == 0;
    if (successful_check || force_turn_meta) {
      fflush(nullptr);
      print(" = Performing turn to the specified meta-page (%d) due to %s!\n",
            stuck_meta,
            successful_check ? "successful check" : "the -T option was given");
      fflush(nullptr);
      rc = mdbx_env_turn_for_recovery(env, stuck_meta);
      if (rc != MDBX_SUCCESS)
        error("mdbx_env_turn_for_recovery() failed, error %d %s\n", rc,
              mdbx_strerror(rc));
    } else {
      print(" = Skipping turn to the specified meta-page (%d) due to "
            "unsuccessful check!\n",
            stuck_meta);
    }
  }

bailout:
  if (txn)
    mdbx_txn_abort(txn);
  if (write_locked) {
    mdbx_txn_unlock(env);
    write_locked = false;
  }
  if (env) {
    const bool dont_sync = rc != 0 || total_problems;
    mdbx_env_close_ex(env, dont_sync);
  }
  fflush(nullptr);
  if (rc) {
    if (rc < 0)
      return user_break ? EXIT_INTERRUPTED : EXIT_FAILURE_SYS;
    return EXIT_FAILURE_MDBX;
  }

#if defined(_WIN32) || defined(_WIN64)
  timestamp_finish = GetMilliseconds();
  elapsed = (timestamp_finish - timestamp_start) * 1e-3;
#else
  if (clock_gettime(CLOCK_MONOTONIC, &timestamp_finish)) {
    rc = errno;
    error("clock_gettime() failed, error %d %s\n", rc, mdbx_strerror(rc));
    return EXIT_FAILURE_SYS;
  }
  elapsed = timestamp_finish.tv_sec - timestamp_start.tv_sec +
            (timestamp_finish.tv_nsec - timestamp_start.tv_nsec) * 1e-9;
#endif /* !WINDOWS */

  if (total_problems) {
    print("Total %u error%s detected, elapsed %.3f seconds.\n", total_problems,
          (total_problems > 1) ? "s are" : " is", elapsed);
    if (problems_meta || problems_maindb || problems_freedb)
      return EXIT_FAILURE_CHECK_MAJOR;
    return EXIT_FAILURE_CHECK_MINOR;
  }
  print("No error is detected, elapsed %.3f seconds\n", elapsed);
  return EXIT_SUCCESS;
}
