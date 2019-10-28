/* mdbx_chk.c - memory-mapped database check tool */

/*
 * Copyright 2015-2019 Leonid Yuriev <leo@yuriev.ru>
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
#include "../elements/internals.h"

typedef struct flagbit {
  int bit;
  char *name;
} flagbit;

flagbit dbflags[] = {{MDBX_DUPSORT, "dupsort"},
                     {MDBX_INTEGERKEY, "integerkey"},
                     {MDBX_REVERSEKEY, "reversekey"},
                     {MDBX_DUPFIXED, "dupfixed"},
                     {MDBX_REVERSEDUP, "reversedup"},
                     {MDBX_INTEGERDUP, "integerdup"},
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

#define EXIT_INTERRUPTED (EXIT_FAILURE + 4)
#define EXIT_FAILURE_SYS (EXIT_FAILURE + 3)
#define EXIT_FAILURE_MDB (EXIT_FAILURE + 2)
#define EXIT_FAILURE_CHECK_MAJOR (EXIT_FAILURE + 1)
#define EXIT_FAILURE_CHECK_MINOR EXIT_FAILURE

typedef struct {
  const char *name;
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
  walk_dbi_t dbi[MAX_DBI + CORE_DBS + /* account pseudo-entry for meta */ 1];
} walk;

#define dbi_free walk.dbi[FREE_DBI]
#define dbi_main walk.dbi[MAIN_DBI]
#define dbi_meta walk.dbi[CORE_DBS]

uint64_t total_unused_bytes;
int envflags = MDBX_RDONLY | MDBX_EXCLUSIVE;

MDBX_env *env;
MDBX_txn *txn;
MDBX_envinfo envinfo;
MDBX_stat envstat;
size_t maxkeysize, userdb_count, skipped_subdb;
uint64_t reclaimable_pages, gc_pages, alloc_pages, unused_pages, backed_pages;
unsigned verbose;
bool ignore_wrong_order, quiet;
const char *only_subdb;

struct problem {
  struct problem *pr_next;
  size_t count;
  const char *caption;
};

struct problem *problems_list;
uint64_t total_problems;

static void __printf_args(1, 2) print(const char *msg, ...) {
  if (!quiet) {
    va_list args;

    fflush(stderr);
    va_start(args, msg);
    vfprintf(stdout, msg, args);
    va_end(args);
  }
}

static void __printf_args(1, 2) error(const char *msg, ...) {
  total_problems++;

  if (!quiet) {
    va_list args;

    fflush(NULL);
    va_start(args, msg);
    fputs(" ! ", stderr);
    vfprintf(stderr, msg, args);
    va_end(args);
    fflush(NULL);
  }
}

static void pagemap_cleanup(void) {
  for (size_t i = CORE_DBS + /* account pseudo-entry for meta */ 1;
       i < ARRAY_LENGTH(walk.dbi); ++i) {
    if (walk.dbi[i].name) {
      mdbx_free((void *)walk.dbi[i].name);
      walk.dbi[i].name = NULL;
    }
  }

  mdbx_free(walk.pagemap);
  walk.pagemap = NULL;
}

static walk_dbi_t *pagemap_lookup_dbi(const char *dbi_name, bool silent) {
  static walk_dbi_t *last;

  if (dbi_name == MDBX_PGWALK_MAIN)
    return &dbi_main;
  if (dbi_name == MDBX_PGWALK_GC)
    return &dbi_free;
  if (dbi_name == MDBX_PGWALK_META)
    return &dbi_meta;

  if (last && strcmp(last->name, dbi_name) == 0)
    return last;

  walk_dbi_t *dbi = walk.dbi + CORE_DBS + /* account pseudo-entry for meta */ 1;
  for (; dbi < ARRAY_END(walk.dbi) && dbi->name; ++dbi) {
    if (strcmp(dbi->name, dbi_name) == 0)
      return last = dbi;
  }

  if (verbose > 0 && !silent) {
    print(" - found '%s' area\n", dbi_name);
    fflush(NULL);
  }

  if (dbi == ARRAY_END(walk.dbi))
    return NULL;

  dbi->name = mdbx_strdup(dbi_name);
  return last = dbi;
}

static void __printf_args(4, 5)

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
      p = mdbx_calloc(1, sizeof(*p));
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
        fflush(NULL);
    }
  }
}

static struct problem *problems_push(void) {
  struct problem *p = problems_list;
  problems_list = NULL;
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
      mdbx_free(problems_list);
      problems_list = p;
    }
    print("\n");
    fflush(NULL);
  }

  problems_list = list;
  return count;
}

static int pgvisitor(const uint64_t pgno, const unsigned pgnumber,
                     void *const ctx, const int deep,
                     const char *const dbi_name_or_tag, const size_t page_size,
                     const MDBX_page_type_t pagetype, const size_t nentries,
                     const size_t payload_bytes, const size_t header_bytes,
                     const size_t unused_bytes) {
  (void)ctx;
  if (deep > 42) {
    problem_add("deep", deep, "too large", nullptr);
    return MDBX_CORRUPTED /* avoid infinite loop/recursion */;
  }

  if (pagetype == MDBX_page_void)
    return MDBX_SUCCESS;

  walk_dbi_t *dbi = pagemap_lookup_dbi(dbi_name_or_tag, false);
  if (!dbi)
    return MDBX_ENOMEM;

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
    if (verbose > 3 && (!only_subdb || strcmp(only_subdb, dbi->name) == 0)) {
      if (pgnumber == 1)
        print("     %s-page %" PRIu64, pagetype_caption, pgno);
      else
        print("     %s-span %" PRIu64 "[%u]", pagetype_caption, pgno, pgnumber);
      print(" of %s: header %" PRIiPTR ", payload %" PRIiPTR
            ", unused %" PRIiPTR ", deep %i\n",
            dbi->name, header_bytes, payload_bytes, unused_bytes, deep);
    }

    bool already_used = false;
    for (unsigned n = 0; n < pgnumber; ++n) {
      uint64_t spanpgno = pgno + n;
      if (spanpgno >= alloc_pages)
        problem_add("page", spanpgno, "wrong page-no",
                    "%s-page: %" PRIu64 " > %" PRIu64 ", deep %i",
                    pagetype_caption, spanpgno, alloc_pages, deep);
      else if (walk.pagemap[spanpgno]) {
        walk_dbi_t *coll_dbi = &walk.dbi[walk.pagemap[spanpgno] - 1];
        problem_add("page", spanpgno,
                    (branch && coll_dbi == dbi) ? "loop" : "already used",
                    "%s-page: by %s, deep %i", pagetype_caption, coll_dbi->name,
                    deep);
        already_used = true;
      } else {
        walk.pagemap[spanpgno] = (short)(dbi - walk.dbi + 1);
        dbi->pages.total += 1;
      }
    }

    if (already_used)
      return branch ? MDBX_RESULT_TRUE /* avoid infinite loop/recursion */
                    : MDBX_SUCCESS;
  }

  if (unused_bytes > page_size)
    problem_add("page", pgno, "illegal unused-bytes",
                "%s-page: %u < %" PRIuPTR " < %u", pagetype_caption, 0,
                unused_bytes, envstat.ms_psize);

  if (header_bytes < (int)sizeof(long) ||
      (size_t)header_bytes >= envstat.ms_psize - sizeof(long))
    problem_add("page", pgno, "illegal header-length",
                "%s-page: %" PRIuPTR " < %" PRIuPTR " < %" PRIuPTR,
                pagetype_caption, sizeof(long), header_bytes,
                envstat.ms_psize - sizeof(long));
  if (payload_bytes < 1) {
    if (nentries > 1) {
      problem_add("page", pgno, "zero size-of-entry",
                  "%s-page: payload %" PRIuPTR " bytes, %" PRIuPTR " entries",
                  pagetype_caption, payload_bytes, nentries);
      /* if ((size_t)header_bytes + unused_bytes < page_size) {
        // LY: hush a misuse error
        page_bytes = page_size;
      } */
    } else {
      problem_add("page", pgno, "empty",
                  "%s-page: payload %" PRIuPTR " bytes, %" PRIuPTR
                  " entries, deep %i",
                  pagetype_caption, payload_bytes, nentries, deep);
      dbi->pages.empty += 1;
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
    } else {
      dbi->payload_bytes += payload_bytes + header_bytes;
      walk.total_payload_bytes += payload_bytes + header_bytes;
    }
  }

  return user_break ? MDBX_EINTR : MDBX_SUCCESS;
}

typedef int(visitor)(const uint64_t record_number, const MDBX_val *key,
                     const MDBX_val *data);
static int process_db(MDBX_dbi dbi_handle, char *dbi_name, visitor *handler,
                      bool silent);

static int handle_userdb(const uint64_t record_number, const MDBX_val *key,
                         const MDBX_val *data) {
  (void)record_number;
  (void)key;
  (void)data;
  return MDBX_SUCCESS;
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
      if (number < 1 || number > MDBX_PNL_MAX)
        problem_add("entry", txnid, "wrong idl length", "%" PRIuPTR, number);
      else if ((number + 1) * sizeof(pgno_t) > data->iov_len) {
        problem_add("entry", txnid, "trimmed idl",
                    "%" PRIuSIZE " > %" PRIuSIZE " (corruption)",
                    (number + 1) * sizeof(pgno_t), data->iov_len);
        number = data->iov_len / sizeof(pgno_t) - 1;
      } else if (data->iov_len - (number + 1) * sizeof(pgno_t) >=
                 /* LY: allow gap upto one page. it is ok
                  * and better than shink-and-retry inside mdbx_update_gc() */
                 envstat.ms_psize)
        problem_add("entry", txnid, "extra idl space",
                    "%" PRIuSIZE " < %" PRIuSIZE " (minor, not a trouble)",
                    (number + 1) * sizeof(pgno_t), data->iov_len);

      gc_pages += number;
      if (envinfo.mi_latter_reader_txnid > txnid)
        reclaimable_pages += number;

      pgno_t prev = MDBX_PNL_ASCENDING ? NUM_METAS - 1 : txn->mt_next_pgno;
      pgno_t span = 1;
      for (unsigned i = 0; i < number; ++i) {
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
                          walk.dbi[idx - 1].name);
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
      if (verbose > 3 && !only_subdb) {
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

  return MDBX_SUCCESS;
}

static int handle_maindb(const uint64_t record_number, const MDBX_val *key,
                         const MDBX_val *data) {
  char *name;
  int rc;
  size_t i;

  name = key->iov_base;
  for (i = 0; i < key->iov_len; ++i) {
    if (name[i] < ' ')
      return handle_userdb(record_number, key, data);
  }

  name = mdbx_malloc(key->iov_len + 1);
  memcpy(name, key->iov_base, key->iov_len);
  name[key->iov_len] = '\0';
  userdb_count++;

  rc = process_db(~0u, name, handle_userdb, false);
  mdbx_free(name);
  if (rc != MDBX_INCOMPATIBLE)
    return rc;

  return handle_userdb(record_number, key, data);
}

static int process_db(MDBX_dbi dbi_handle, char *dbi_name, visitor *handler,
                      bool silent) {
  MDBX_cursor *mc;
  MDBX_stat ms;
  MDBX_val key, data;
  MDBX_val prev_key, prev_data;
  unsigned flags;
  int rc, i;
  struct problem *saved_list;
  uint64_t problems_count;

  uint64_t record_count = 0, dups = 0;
  uint64_t key_bytes = 0, data_bytes = 0;

  if (dbi_handle == ~0u) {
    rc = mdbx_dbi_open(txn, dbi_name, 0, &dbi_handle);
    if (rc) {
      if (!dbi_name ||
          rc !=
              MDBX_INCOMPATIBLE) /* LY: mainDB's record is not a user's DB. */ {
        error("mdbx_open '%s' failed, error %d %s\n",
              dbi_name ? dbi_name : "main", rc, mdbx_strerror(rc));
      }
      return rc;
    }
  }

  if (dbi_handle >= CORE_DBS && dbi_name && only_subdb &&
      strcmp(only_subdb, dbi_name) != 0) {
    if (verbose) {
      print("Skip processing '%s'...\n", dbi_name);
      fflush(NULL);
    }
    skipped_subdb++;
    return MDBX_SUCCESS;
  }

  if (!silent && verbose) {
    print("Processing '%s'...\n", dbi_name ? dbi_name : "@MAIN");
    fflush(NULL);
  }

  rc = mdbx_dbi_flags(txn, dbi_handle, &flags);
  if (rc) {
    error("mdbx_dbi_flags failed, error %d %s\n", rc, mdbx_strerror(rc));
    return rc;
  }

  rc = mdbx_dbi_stat(txn, dbi_handle, &ms, sizeof(ms));
  if (rc) {
    error("mdbx_dbi_stat failed, error %d %s\n", rc, mdbx_strerror(rc));
    return rc;
  }

  if (!silent && verbose) {
    print(" - dbi-id %d, flags:", dbi_handle);
    if (!flags)
      print(" none");
    else {
      for (i = 0; dbflags[i].bit; i++)
        if (flags & dbflags[i].bit)
          print(" %s", dbflags[i].name);
    }
    print(" (0x%02X)\n", flags);
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
  const uint64_t subtotal_pages =
      ms.ms_branch_pages + ms.ms_leaf_pages + ms.ms_overflow_pages;
  if (subtotal_pages != dbi->pages.total)
    error("%s pages mismatch (%" PRIu64 " != walked %" PRIu64 ")\n", "subtotal",
          subtotal_pages, dbi->pages.total);
  if (ms.ms_branch_pages != dbi->pages.branch)
    error("%s pages mismatch (%" PRIu64 " != walked %" PRIu64 ")\n", "branch",
          ms.ms_branch_pages, dbi->pages.branch);
  const uint64_t allleaf_pages = dbi->pages.leaf + dbi->pages.leaf_dupfixed;
  if (ms.ms_leaf_pages != allleaf_pages)
    error("%s pages mismatch (%" PRIu64 " != walked %" PRIu64 ")\n", "all-leaf",
          ms.ms_leaf_pages, allleaf_pages);
  if (ms.ms_overflow_pages != dbi->pages.large_volume)
    error("%s pages mismatch (%" PRIu64 " != walked %" PRIu64 ")\n",
          "large/overlow", ms.ms_overflow_pages, dbi->pages.large_volume);

  rc = mdbx_cursor_open(txn, dbi_handle, &mc);
  if (rc) {
    error("mdbx_cursor_open failed, error %d %s\n", rc, mdbx_strerror(rc));
    return rc;
  }

  saved_list = problems_push();
  prev_key.iov_base = NULL;
  prev_key.iov_len = 0;
  prev_data.iov_base = NULL;
  prev_data.iov_len = 0;
  rc = mdbx_cursor_get(mc, &key, &data, MDBX_FIRST);
  while (rc == MDBX_SUCCESS) {
    if (user_break) {
      print(" - interrupted by signal\n");
      fflush(NULL);
      rc = MDBX_EINTR;
      goto bailout;
    }

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

    if (prev_key.iov_base && !bad_data) {
      if ((flags & MDBX_DUPFIXED) && prev_data.iov_len != data.iov_len) {
        problem_add("entry", record_count, "different data length",
                    "%" PRIuPTR " != %" PRIuPTR, prev_data.iov_len,
                    data.iov_len);
        bad_data = true;
      }

      if (!bad_key) {
        int cmp = mdbx_cmp(txn, dbi_handle, &prev_key, &key);
        if (cmp == 0) {
          ++dups;
          if ((flags & MDBX_DUPSORT) == 0) {
            problem_add("entry", record_count, "duplicated entries", NULL);
            if (data.iov_len == prev_data.iov_len &&
                memcmp(data.iov_base, prev_data.iov_base, data.iov_len) == 0) {
              problem_add("entry", record_count, "complete duplicate", NULL);
            }
          } else if (!bad_data) {
            cmp = mdbx_dcmp(txn, dbi_handle, &prev_data, &data);
            if (cmp == 0) {
              problem_add("entry", record_count, "complete duplicate", NULL);
            } else if (cmp > 0 && !ignore_wrong_order) {
              problem_add("entry", record_count, "wrong order of multi-values",
                          NULL);
            }
          }
        } else if (cmp > 0 && !ignore_wrong_order) {
          problem_add("entry", record_count, "wrong order of entries", NULL);
        }
      }
    } else if (verbose) {
      if (flags & MDBX_INTEGERKEY)
        print(" - fixed key-size %" PRIuPTR "\n", key.iov_len);
      if (flags & (MDBX_INTEGERDUP | MDBX_DUPFIXED))
        print(" - fixed data-size %" PRIuPTR "\n", data.iov_len);
    }

    if (handler) {
      rc = handler(record_count, &key, &data);
      if (MDBX_IS_ERROR(rc))
        goto bailout;
    }

    record_count++;
    key_bytes += key.iov_len;
    data_bytes += data.iov_len;

    if (!bad_key)
      prev_key = key;
    if (!bad_data)
      prev_data = data;
    rc = mdbx_cursor_get(mc, &key, &data, MDBX_NEXT);
  }
  if (rc != MDBX_NOTFOUND)
    error("mdbx_cursor_get failed, error %d %s\n", rc, mdbx_strerror(rc));
  else
    rc = 0;

  if (record_count != ms.ms_entries)
    problem_add("entry", record_count, "differentent number of entries",
                "%" PRIu64 " != %" PRIu64, record_count, ms.ms_entries);
bailout:
  problems_count = problems_pop(saved_list);
  if (!silent && verbose) {
    print(" - summary: %" PRIu64 " records, %" PRIu64 " dups, %" PRIu64
          " key's bytes, %" PRIu64 " data's "
          "bytes, %" PRIu64 " problems\n",
          record_count, dups, key_bytes, data_bytes, problems_count);
    fflush(NULL);
  }

  mdbx_cursor_close(mc);
  return (rc || problems_count) ? MDBX_RESULT_TRUE : MDBX_SUCCESS;
}

static void usage(char *prog) {
  fprintf(stderr,
          "usage: %s dbpath [-V] [-v] [-n] [-q] [-w] [-c] [-d] [-s subdb]\n"
          "  -V\t\tshow version\n"
          "  -v\t\tmore verbose, could be used multiple times\n"
          "  -n\t\tNOSUBDIR mode for open\n"
          "  -q\t\tbe quiet\n"
          "  -w\t\tlock DB for writing while checking\n"
          "  -d\t\tdisable page-by-page traversal of B-tree\n"
          "  -s subdb\tprocess a specific subdatabase only\n"
          "  -c\t\tforce cooperative mode (don't try exclusive)\n"
          "  -i\t\tignore wrong order errors (for custom comparators case)\n",
          prog);
  exit(EXIT_INTERRUPTED);
}

const char *meta_synctype(uint64_t sign) {
  switch (sign) {
  case MDBX_DATASIGN_NONE:
    return "no-sync/legacy";
  case MDBX_DATASIGN_WEAK:
    return "weak";
  default:
    return "steady";
  }
}

static __inline bool meta_ot(txnid_t txn_a, uint64_t sign_a, txnid_t txn_b,
                             uint64_t sign_b, const bool roolback2steady) {
  if (txn_a == txn_b)
    return SIGN_IS_STEADY(sign_b);

  if (roolback2steady && SIGN_IS_STEADY(sign_a) != SIGN_IS_STEADY(sign_b))
    return SIGN_IS_STEADY(sign_b);

  return txn_a < txn_b;
}

static __inline bool meta_eq(txnid_t txn_a, uint64_t sign_a, txnid_t txn_b,
                             uint64_t sign_b) {
  if (txn_a != txn_b)
    return false;

  if (SIGN_IS_STEADY(sign_a) != SIGN_IS_STEADY(sign_b))
    return false;

  return true;
}

static __inline int meta_recent(const bool roolback2steady) {

  if (meta_ot(envinfo.mi_meta0_txnid, envinfo.mi_meta0_sign,
              envinfo.mi_meta1_txnid, envinfo.mi_meta1_sign, roolback2steady))
    return meta_ot(envinfo.mi_meta2_txnid, envinfo.mi_meta2_sign,
                   envinfo.mi_meta1_txnid, envinfo.mi_meta1_sign,
                   roolback2steady)
               ? 1
               : 2;

  return meta_ot(envinfo.mi_meta0_txnid, envinfo.mi_meta0_sign,
                 envinfo.mi_meta2_txnid, envinfo.mi_meta2_sign, roolback2steady)
             ? 2
             : 0;
}

static __inline int meta_tail(int head) {

  if (head == 0)
    return meta_ot(envinfo.mi_meta1_txnid, envinfo.mi_meta1_sign,
                   envinfo.mi_meta2_txnid, envinfo.mi_meta2_sign, true)
               ? 1
               : 2;
  if (head == 1)
    return meta_ot(envinfo.mi_meta0_txnid, envinfo.mi_meta0_sign,
                   envinfo.mi_meta2_txnid, envinfo.mi_meta2_sign, true)
               ? 0
               : 2;
  if (head == 2)
    return meta_ot(envinfo.mi_meta0_txnid, envinfo.mi_meta0_sign,
                   envinfo.mi_meta1_txnid, envinfo.mi_meta1_sign, true)
               ? 0
               : 1;
  assert(false);
  return -1;
}

static int meta_steady(void) { return meta_recent(true); }

static int meta_head(void) { return meta_recent(false); }

void verbose_meta(int num, txnid_t txnid, uint64_t sign) {
  print(" - meta-%d: %s %" PRIu64, num, meta_synctype(sign), txnid);
  bool stay = true;

  const int steady = meta_steady();
  const int head = meta_head();
  if (num == steady && num == head) {
    print(", head");
    stay = false;
  } else if (num == steady) {
    print(", head-steady");
    stay = false;
  } else if (num == head) {
    print(", head-weak");
    stay = false;
  }
  if (num == meta_tail(head)) {
    print(", tail");
    stay = false;
  }
  if (stay)
    print(", stay");

  if (txnid > envinfo.mi_recent_txnid &&
      (envflags & (MDBX_EXCLUSIVE | MDBX_RDONLY)) == MDBX_EXCLUSIVE)
    print(", rolled-back %" PRIu64 " (%" PRIu64 " >>> %" PRIu64 ")",
          txnid - envinfo.mi_recent_txnid, txnid, envinfo.mi_recent_txnid);
  print("\n");
}

static int check_meta_head(bool steady) {
  switch (meta_recent(steady)) {
  default:
    assert(false);
    error("unexpected internal error (%s)\n",
          steady ? "meta_steady_head" : "meta_weak_head");
    __fallthrough;
  case 0:
    if (envinfo.mi_meta0_txnid != envinfo.mi_recent_txnid) {
      print(" - meta-%d txn-id mismatch recent-txn-id (%" PRIi64 " != %" PRIi64
            ")\n",
            0, envinfo.mi_meta0_txnid, envinfo.mi_recent_txnid);
      return 1;
    }
    break;
  case 1:
    if (envinfo.mi_meta1_txnid != envinfo.mi_recent_txnid) {
      print(" - meta-%d txn-id mismatch recent-txn-id (%" PRIi64 " != %" PRIi64
            ")\n",
            1, envinfo.mi_meta1_txnid, envinfo.mi_recent_txnid);
      return 1;
    }
    break;
  case 2:
    if (envinfo.mi_meta2_txnid != envinfo.mi_recent_txnid) {
      print(" - meta-%d txn-id mismatch recent-txn-id (%" PRIi64 " != %" PRIi64
            ")\n",
            2, envinfo.mi_meta2_txnid, envinfo.mi_recent_txnid);
      return 1;
    }
  }
  return 0;
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
  int problems_maindb = 0, problems_freedb = 0, problems_meta = 0;
  bool dont_traversal = false;
  bool locked = false;

  double elapsed;
#if defined(_WIN32) || defined(_WIN64)
  uint64_t timestamp_start, timestamp_finish;
  timestamp_start = GetTickCount64();
#else
  struct timespec timestamp_start, timestamp_finish;
  if (clock_gettime(CLOCK_MONOTONIC, &timestamp_start)) {
    rc = errno;
    error("clock_gettime failed, error %d %s\n", rc, mdbx_strerror(rc));
    return EXIT_FAILURE_SYS;
  }
#endif

  dbi_meta.name = "@META";
  dbi_free.name = "@GC";
  dbi_main.name = "@MAIN";
  atexit(pagemap_cleanup);

  if (argc < 2)
    usage(prog);

  for (int i; (i = getopt(argc, argv, "Vvqnwcdsi:")) != EOF;) {
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
    case 'q':
      quiet = true;
      break;
    case 'n':
      envflags |= MDBX_NOSUBDIR;
      break;
    case 'w':
      envflags &= ~MDBX_RDONLY;
      break;
    case 'c':
      envflags &= ~MDBX_EXCLUSIVE;
      break;
    case 'd':
      dont_traversal = true;
      break;
    case 's':
      if (only_subdb && strcmp(only_subdb, optarg))
        usage(prog);
      only_subdb = optarg;
      break;
    case 'i':
      ignore_wrong_order = true;
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
  print("mdbx_chk %s (%s, T-%s)\nRunning for %s in 'read-%s' mode...\n",
        mdbx_version.git.describe, mdbx_version.git.datetime,
        mdbx_version.git.tree, envname,
        (envflags & MDBX_RDONLY) ? "only" : "write");
  fflush(NULL);

  rc = mdbx_env_create(&env);
  if (rc) {
    error("mdbx_env_create failed, error %d %s\n", rc, mdbx_strerror(rc));
    return rc < 0 ? EXIT_FAILURE_MDB : EXIT_FAILURE_SYS;
  }

  rc = mdbx_env_set_maxdbs(env, MAX_DBI);
  if (rc) {
    error("mdbx_env_set_maxdbs failed, error %d %s\n", rc, mdbx_strerror(rc));
    goto bailout;
  }

  rc = mdbx_env_open(env, envname, envflags, 0664);
  if ((envflags & MDBX_EXCLUSIVE) &&
      (rc == MDBX_BUSY ||
#if defined(_WIN32) || defined(_WIN64)
       rc == ERROR_LOCK_VIOLATION || rc == ERROR_SHARING_VIOLATION
#else
       rc == EBUSY || rc == EAGAIN
#endif
       )) {
    envflags &= ~MDBX_EXCLUSIVE;
    rc = mdbx_env_open(env, envname, envflags, 0664);
  }

  if (rc) {
    error("mdbx_env_open failed, error %d %s\n", rc, mdbx_strerror(rc));
    if (rc == MDBX_WANNA_RECOVERY && (envflags & MDBX_RDONLY))
      print("Please run %s in the read-write mode (with '-w' option).\n", prog);
    goto bailout;
  }
  if (verbose)
    print(" - %s mode\n",
          (envflags & MDBX_EXCLUSIVE) ? "monopolistic" : "cooperative");

  if ((envflags & MDBX_RDONLY) == 0) {
    rc = mdbx_txn_lock(env, false);
    if (rc != MDBX_SUCCESS) {
      error("mdbx_txn_lock failed, error %d %s\n", rc, mdbx_strerror(rc));
      goto bailout;
    }
    locked = true;
  }

  rc = mdbx_txn_begin(env, NULL, MDBX_RDONLY, &txn);
  if (rc) {
    error("mdbx_txn_begin() failed, error %d %s\n", rc, mdbx_strerror(rc));
    goto bailout;
  }

  rc = mdbx_env_get_maxkeysize(env);
  if (rc < 0) {
    error("mdbx_env_get_maxkeysize failed, error %d %s\n", rc,
          mdbx_strerror(rc));
    goto bailout;
  }
  maxkeysize = rc;

  rc = mdbx_env_info_ex(env, txn, &envinfo, sizeof(envinfo));
  if (rc) {
    error("mdbx_env_info failed, error %d %s\n", rc, mdbx_strerror(rc));
    goto bailout;
  }

  rc = mdbx_env_stat_ex(env, txn, &envstat, sizeof(envstat));
  if (rc) {
    error("mdbx_env_stat failed, error %d %s\n", rc, mdbx_strerror(rc));
    goto bailout;
  }

  mdbx_filehandle_t dxb_fd;
  rc = mdbx_env_get_fd(env, &dxb_fd);
  if (rc) {
    error("mdbx_env_get_fd failed, error %d %s\n", rc, mdbx_strerror(rc));
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
    error("mdbx_filesize failed, error %d %s\n", rc, mdbx_strerror(rc));
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
  if (backed_pages > MAX_PAGENO) {
    print(" ! backed-pages %" PRIu64 " > max-pages %" PRIaPGNO "\n",
          backed_pages, MAX_PAGENO);
    ++problems_meta;
    backed_pages = MAX_PAGENO;
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
    /* LY: DB may be shrinked by writer downto the allocated pages. */
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
    print(" - pagesize %u (%u system), max keysize %" PRIuPTR
          ", max readers %u\n",
          envinfo.mi_dxb_pagesize, envinfo.mi_sys_pagesize, maxkeysize,
          envinfo.mi_maxreaders);
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
    print(" - transactions: recent %" PRIu64 ", latter reader %" PRIu64
          ", lag %" PRIi64 "\n",
          envinfo.mi_recent_txnid, envinfo.mi_latter_reader_txnid,
          envinfo.mi_recent_txnid - envinfo.mi_latter_reader_txnid);

    verbose_meta(0, envinfo.mi_meta0_txnid, envinfo.mi_meta0_sign);
    verbose_meta(1, envinfo.mi_meta1_txnid, envinfo.mi_meta1_sign);
    verbose_meta(2, envinfo.mi_meta2_txnid, envinfo.mi_meta2_sign);
  }

  if (verbose)
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

  if (envflags & MDBX_EXCLUSIVE) {
    if (verbose)
      print(" - performs full check recent-txn-id with meta-pages\n");
    problems_meta += check_meta_head(true);
  } else if (locked) {
    if (verbose)
      print(" - performs lite check recent-txn-id with meta-pages (not a "
            "monopolistic mode)\n");
    problems_meta += check_meta_head(false);
  } else if (verbose) {
    print(" - skip check recent-txn-id with meta-pages (monopolistic or "
          "read-write mode only)\n");
  }

  if (!dont_traversal) {
    struct problem *saved_list;
    size_t traversal_problems;
    uint64_t empty_pages, lost_bytes;

    print("Traversal b-tree by txn#%" PRIaTXN "...\n", txn->mt_txnid);
    fflush(NULL);
    walk.pagemap = mdbx_calloc((size_t)backed_pages, sizeof(*walk.pagemap));
    if (!walk.pagemap) {
      rc = errno ? errno : MDBX_ENOMEM;
      error("calloc failed, error %d %s\n", rc, mdbx_strerror(rc));
      goto bailout;
    }

    saved_list = problems_push();
    rc = mdbx_env_pgwalk(txn, pgvisitor, NULL);
    traversal_problems = problems_pop(saved_list);

    if (rc) {
      if (rc == MDBX_EINTR && user_break) {
        print(" - interrupted by signal\n");
        fflush(NULL);
      } else {
        error("mdbx_env_pgwalk failed, error %d %s\n", rc, mdbx_strerror(rc));
      }
      goto bailout;
    }

    for (uint64_t n = 0; n < alloc_pages; ++n)
      if (!walk.pagemap[n])
        unused_pages += 1;

    empty_pages = lost_bytes = 0;
    for (walk_dbi_t *dbi = &dbi_main; dbi < ARRAY_END(walk.dbi) && dbi->name;
         ++dbi) {
      empty_pages += dbi->pages.empty;
      lost_bytes += dbi->lost_bytes;
    }

    if (verbose) {
      uint64_t total_page_bytes = walk.pgcount * envstat.ms_psize;
      print(" - pages: total %" PRIu64 ", unused %" PRIu64 "\n", walk.pgcount,
            unused_pages);
      if (verbose > 1) {
        for (walk_dbi_t *dbi = walk.dbi; dbi < ARRAY_END(walk.dbi) && dbi->name;
             ++dbi) {
          print("     %s: subtotal %" PRIu64, dbi->name, dbi->pages.total);
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
                (dbi->pages.leaf_dupfixed | dbi->pages.subleaf_dupsort |
                 dbi->pages.subleaf_dupsort))
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
        for (walk_dbi_t *dbi = walk.dbi; dbi < ARRAY_END(walk.dbi) && dbi->name;
             ++dbi)
          if (dbi->pages.total) {
            uint64_t dbi_bytes = dbi->pages.total * envstat.ms_psize;
            print("     %s: subtotal %" PRIu64 " bytes (%.1f%%),"
                  " payload %" PRIu64 " (%.1f%%), unused %" PRIu64 " (%.1f%%)",
                  dbi->name, dbi_bytes, dbi_bytes * 100.0 / total_page_bytes,
                  dbi->payload_bytes, dbi->payload_bytes * 100.0 / dbi_bytes,
                  dbi_bytes - dbi->payload_bytes,
                  (dbi_bytes - dbi->payload_bytes) * 100.0 / dbi_bytes);
            if (dbi->pages.empty)
              print(", %" PRIu64 " empty pages", dbi->pages.empty);
            if (dbi->lost_bytes)
              print(", %" PRIu64 " bytes lost", dbi->lost_bytes);
            print("\n");
          } else
            print("     %s: empty\n", dbi->name);
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
    fflush(NULL);
  }

  if (!verbose)
    print("Iterating DBIs...\n");
  problems_maindb = process_db(~0u, /* MAIN_DBI */ NULL, NULL, false);
  problems_freedb = process_db(FREE_DBI, "@GC", handle_freedb, false);

  if (verbose) {
    uint64_t value = envinfo.mi_mapsize / envstat.ms_psize;
    double percent = value / 100.0;
    print(" - space: %" PRIu64 " total pages", value);
    print(", backed %" PRIu64 " (%.1f%%)", backed_pages,
          backed_pages / percent);
    print(", allocated %" PRIu64 " (%.1f%%)", alloc_pages,
          alloc_pages / percent);

    if (verbose > 1) {
      value = envinfo.mi_mapsize / envstat.ms_psize - alloc_pages;
      print(", remained %" PRIu64 " (%.1f%%)", value, value / percent);

      value = alloc_pages - gc_pages;
      print(", used %" PRIu64 " (%.1f%%)", value, value / percent);

      print(", gc %" PRIu64 " (%.1f%%)", gc_pages, gc_pages / percent);

      value = gc_pages - reclaimable_pages;
      print(", detained %" PRIu64 " (%.1f%%)", value, value / percent);

      print(", reclaimable %" PRIu64 " (%.1f%%)", reclaimable_pages,
            reclaimable_pages / percent);
    }

    value =
        envinfo.mi_mapsize / envstat.ms_psize - alloc_pages + reclaimable_pages;
    print(", available %" PRIu64 " (%.1f%%)\n", value, value / percent);
  }

  if (problems_maindb == 0 && problems_freedb == 0) {
    if (!dont_traversal &&
        (envflags & (MDBX_EXCLUSIVE | MDBX_RDONLY)) != MDBX_RDONLY) {
      if (walk.pgcount != alloc_pages - gc_pages) {
        error("used pages mismatch (%" PRIu64 "(walked) != %" PRIu64
              "(allocated - GC))\n",
              walk.pgcount, alloc_pages - gc_pages);
      }
      if (unused_pages != gc_pages) {
        error("gc pages mismatch (%" PRIu64 "(walked) != %" PRIu64 "(GC))\n",
              unused_pages, gc_pages);
      }
    } else if (verbose) {
      print(" - skip check used and gc pages (btree-traversal with "
            "monopolistic or read-write mode only)\n");
    }

    if (!process_db(MAIN_DBI, NULL, handle_maindb, true)) {
      if (!userdb_count && verbose)
        print(" - does not contain multiple databases\n");
    }
  }

bailout:
  if (txn)
    mdbx_txn_abort(txn);
  if (locked)
    mdbx_txn_unlock(env);
  if (env)
    mdbx_env_close(env);
  fflush(NULL);
  if (rc) {
    if (rc < 0)
      return (user_break) ? EXIT_INTERRUPTED : EXIT_FAILURE_SYS;
    return EXIT_FAILURE_MDB;
  }

#if defined(_WIN32) || defined(_WIN64)
  timestamp_finish = GetTickCount64();
  elapsed = (timestamp_finish - timestamp_start) * 1e-3;
#else
  if (clock_gettime(CLOCK_MONOTONIC, &timestamp_finish)) {
    rc = errno;
    error("clock_gettime failed, error %d %s\n", rc, mdbx_strerror(rc));
    return EXIT_FAILURE_SYS;
  }
  elapsed = timestamp_finish.tv_sec - timestamp_start.tv_sec +
            (timestamp_finish.tv_nsec - timestamp_start.tv_nsec) * 1e-9;
#endif /* !WINDOWS */

  total_problems += problems_meta;
  if (total_problems || problems_maindb || problems_freedb) {
    print("Total %" PRIu64 " error%s detected, elapsed %.3f seconds.\n",
          total_problems, (total_problems > 1) ? "s are" : " is", elapsed);
    if (problems_meta || problems_maindb || problems_freedb)
      return EXIT_FAILURE_CHECK_MAJOR;
    return EXIT_FAILURE_CHECK_MINOR;
  }
  print("No error is detected, elapsed %.3f seconds\n", elapsed);
  return EXIT_SUCCESS;
}
