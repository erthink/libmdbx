/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2024
///

///
/// mdbx_chk.c - memory-mapped database check tool
///

#ifdef _MSC_VER
#if _MSC_VER > 1800
#pragma warning(disable : 4464) /* relative include path contains '..' */
#endif
#pragma warning(disable : 4996) /* The POSIX name is deprecated... */
#endif                          /* _MSC_VER (warnings) */

#define xMDBX_TOOLS /* Avoid using internal eASSERT() */
#include "essentials.h"

#include <ctype.h>

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
  return (QueryPerformanceFrequency(&Frequency) && QueryPerformanceCounter(&Counter))
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

MDBX_env_flags_t env_flags = MDBX_RDONLY | MDBX_EXCLUSIVE | MDBX_VALIDATION;
MDBX_env *env;
MDBX_txn *txn;
unsigned verbose = 0;
bool quiet;
MDBX_val only_table;
int stuck_meta = -1;
MDBX_chk_context_t chk;
bool turn_meta = false;
bool force_turn_meta = false;
MDBX_chk_flags_t chk_flags = MDBX_CHK_DEFAULTS;
MDBX_chk_stage_t chk_stage = MDBX_chk_none;

static MDBX_chk_line_t line_struct;
static size_t anchor_lineno;
static size_t line_count;
static FILE *line_output;

#define LINE_SEVERITY_NONE 255
static bool lf(void) {
  if (!line_struct.empty) {
    line_count += 1;
    line_struct.empty = true;
    line_struct.severity = LINE_SEVERITY_NONE;
    line_struct.scope_depth = 0;
    if (line_output) {
      fputc('\n', line_output);
      return true;
    }
  }
  return false;
}

static void flush(void) { fflush(nullptr); }

static void lf_flush(void) {
  if (lf())
    flush();
}

static bool silently(enum MDBX_chk_severity severity) {
  int cutoff = chk.scope ? chk.scope->verbosity >> MDBX_chk_severity_prio_shift
                         : verbose + (MDBX_chk_result >> MDBX_chk_severity_prio_shift);
  int prio = (severity >> MDBX_chk_severity_prio_shift);
  if (chk.scope && chk.scope->stage == MDBX_chk_tables && verbose < 2)
    prio += 1;
  return quiet || cutoff < ((prio > 0) ? prio : 0);
}

static FILE *prefix(enum MDBX_chk_severity severity) {
  if (silently(severity))
    return nullptr;

  static const char *const prefixes[16] = {
      "!!!fatal: ", // 0 fatal
      " ! ",        // 1 error
      " ~ ",        // 2 warning
      "   ",        // 3 notice
      "",           // 4 result
      " = ",        // 5 resolution
      " - ",        // 6 processing
      "   ",        // 7 info
      "   ",        // 8 verbose
      "   ",        // 9 details
      "   // ",     // A lib-verbose
      "   //// ",   // B lib-debug
      "   ////// ", // C lib-trace
      "   ////// ", // D lib-extra
      "   ////// ", // E +1
      "   ////// "  // F +2
  };

  const bool nl = line_struct.scope_depth != chk.scope_nesting ||
                  (line_struct.severity != severity && (line_struct.severity != MDBX_chk_processing ||
                                                        severity < MDBX_chk_result || severity > MDBX_chk_resolution));
  if (nl)
    lf();
  if (severity < MDBX_chk_warning)
    flush();
  FILE *out = (severity > MDBX_chk_error) ? stdout : stderr;
  if (nl || line_struct.empty) {
    line_struct.severity = severity;
    line_struct.scope_depth = chk.scope_nesting;
    unsigned kind = line_struct.severity & MDBX_chk_severity_kind_mask;
    if (line_struct.scope_depth || *prefixes[kind]) {
      line_struct.empty = false;
      for (size_t i = 0; i < line_struct.scope_depth; ++i)
        fputs("   ", out);
      fputs(prefixes[kind], out);
    }
  }
  return line_output = out;
}

static void suffix(size_t cookie, const char *str) {
  if (cookie == line_count && !line_struct.empty) {
    fprintf(line_output, " %s", str);
    line_struct.empty = false;
    lf();
  }
}

static size_t MDBX_PRINTF_ARGS(2, 3) print(enum MDBX_chk_severity severity, const char *msg, ...) {
  FILE *out = prefix(severity);
  if (out) {
    va_list args;
    va_start(args, msg);
    vfprintf(out, msg, args);
    va_end(args);
    line_struct.empty = false;
    return line_count;
  }
  return 0;
}

static FILE *MDBX_PRINTF_ARGS(2, 3) print_ln(enum MDBX_chk_severity severity, const char *msg, ...) {
  FILE *out = prefix(severity);
  if (out) {
    va_list args;
    va_start(args, msg);
    vfprintf(out, msg, args);
    va_end(args);
    line_struct.empty = false;
    lf();
  }
  return out;
}

static void logger(MDBX_log_level_t level, const char *function, int line, const char *fmt, va_list args) {
  if (level <= MDBX_LOG_ERROR)
    mdbx_env_chk_encount_problem(&chk);

  const unsigned kind =
      (level > MDBX_LOG_NOTICE) ? level - MDBX_LOG_NOTICE + (MDBX_chk_extra & MDBX_chk_severity_kind_mask) : level;
  const unsigned prio = kind << MDBX_chk_severity_prio_shift;
  enum MDBX_chk_severity severity = prio + kind;
  FILE *out = prefix(severity);
  if (out) {
    vfprintf(out, fmt, args);
    const bool have_lf = fmt[strlen(fmt) - 1] == '\n';
    if (level == MDBX_LOG_FATAL && function && line) {
      if (have_lf)
        for (size_t i = 0; i < line_struct.scope_depth; ++i)
          fputs("   ", out);
      fprintf(out, have_lf ? "          %s(), %u" : " (%s:%u)", function + (strncmp(function, "mdbx_", 5) ? 0 : 5),
              line);
      lf();
    } else if (have_lf) {
      line_struct.empty = true;
      line_struct.severity = LINE_SEVERITY_NONE;
      line_count += 1;
    } else
      lf();
  }
  if (level < MDBX_LOG_VERBOSE)
    flush();
  if (level == MDBX_LOG_FATAL) {
#if !MDBX_DEBUG && !MDBX_FORCE_ASSERTIONS
    exit(EXIT_FAILURE_MDBX);
#endif
    abort();
  }
}

static void MDBX_PRINTF_ARGS(1, 2) error_fmt(const char *msg, ...) {
  va_list args;
  va_start(args, msg);
  logger(MDBX_LOG_ERROR, nullptr, 0, msg, args);
  va_end(args);
}

static int error_fn(const char *fn, int err) {
  if (err)
    error_fmt("%s() failed, error %d, %s", fn, err, mdbx_strerror(err));
  return err;
}

static bool check_break(MDBX_chk_context_t *ctx) {
  (void)ctx;
  if (!user_break)
    return false;
  if (user_break == 1) {
    print(MDBX_chk_resolution, "interrupted by signal");
    lf_flush();
    user_break = 2;
  }
  return true;
}

static int scope_push(MDBX_chk_context_t *ctx, MDBX_chk_scope_t *scope, MDBX_chk_scope_t *inner, const char *fmt,
                      va_list args) {
  (void)scope;
  if (fmt && *fmt) {
    FILE *out = prefix(MDBX_chk_processing);
    if (out) {
      vfprintf(out, fmt, args);
      inner->usr_o.number = line_count;
      line_struct.ctx = ctx;
      flush();
    }
  }
  return MDBX_SUCCESS;
}

static void scope_pop(MDBX_chk_context_t *ctx, MDBX_chk_scope_t *scope, MDBX_chk_scope_t *inner) {
  (void)ctx;
  (void)scope;
  suffix(inner->usr_o.number, inner->subtotal_issues ? "error(s)" : "done");
  flush();
}

static MDBX_chk_user_table_cookie_t *table_filter(MDBX_chk_context_t *ctx, const MDBX_val *name,
                                                  MDBX_db_flags_t flags) {
  (void)ctx;
  (void)flags;
  return (!only_table.iov_base ||
          (only_table.iov_len == name->iov_len && memcmp(only_table.iov_base, name->iov_base, name->iov_len) == 0))
             ? (void *)(intptr_t)-1
             : nullptr;
}

static int stage_begin(MDBX_chk_context_t *ctx, enum MDBX_chk_stage stage) {
  (void)ctx;
  chk_stage = stage;
  anchor_lineno = line_count;
  flush();
  return MDBX_SUCCESS;
}

static int conclude(MDBX_chk_context_t *ctx);
static int stage_end(MDBX_chk_context_t *ctx, enum MDBX_chk_stage stage, int err) {
  if (stage == MDBX_chk_conclude && !err)
    err = conclude(ctx);
  suffix(anchor_lineno, err ? "error(s)" : "done");
  flush();
  chk_stage = MDBX_chk_none;
  return err;
}

static MDBX_chk_line_t *print_begin(MDBX_chk_context_t *ctx, enum MDBX_chk_severity severity) {
  (void)ctx;
  if (silently(severity))
    return nullptr;
  if (line_struct.ctx) {
    if (line_struct.severity == MDBX_chk_processing && severity >= MDBX_chk_result && severity <= MDBX_chk_resolution &&
        line_output)
      fputc(' ', line_output);
    else
      lf();
    line_struct.ctx = nullptr;
  }
  line_struct.severity = severity;
  return &line_struct;
}

static void print_flush(MDBX_chk_line_t *line) {
  (void)line;
  flush();
}

static void print_done(MDBX_chk_line_t *line) {
  lf();
  line->ctx = nullptr;
}

static void print_chars(MDBX_chk_line_t *line, const char *str, size_t len) {
  if (line->empty)
    prefix(line->severity);
  fwrite(str, 1, len, line_output);
}

static void print_format(MDBX_chk_line_t *line, const char *fmt, va_list args) {
  if (line->empty)
    prefix(line->severity);
  vfprintf(line_output, fmt, args);
}

static const MDBX_chk_callbacks_t cb = {.check_break = check_break,
                                        .scope_push = scope_push,
                                        .scope_pop = scope_pop,
                                        .table_filter = table_filter,
                                        .stage_begin = stage_begin,
                                        .stage_end = stage_end,
                                        .print_begin = print_begin,
                                        .print_flush = print_flush,
                                        .print_done = print_done,
                                        .print_chars = print_chars,
                                        .print_format = print_format};

static void usage(char *prog) {
  fprintf(stderr,
          "usage: %s "
          "[-V] [-v] [-q] [-c] [-0|1|2] [-w] [-d] [-i] [-s table] [-u|U] dbpath\n"
          "  -V\t\tprint version and exit\n"
          "  -v\t\tmore verbose, could be repeated upto 9 times for extra details\n"
          "  -q\t\tbe quiet\n"
          "  -c\t\tforce cooperative mode (don't try exclusive)\n"
          "  -w\t\twrite-mode checking\n"
          "  -d\t\tdisable page-by-page traversal of B-tree\n"
          "  -i\t\tignore wrong order errors (for custom comparators case)\n"
          "  -s table\tprocess a specific subdatabase only\n"
          "  -u\t\twarmup database before checking\n"
          "  -U\t\twarmup and try lock database pages in memory before checking\n"
          "  -0|1|2\tforce using specific meta-page 0, or 2 for checking\n"
          "  -t\t\tturn to a specified meta-page on successful check\n"
          "  -T\t\tturn to a specified meta-page EVEN ON UNSUCCESSFUL CHECK!\n",
          prog);
  exit(EXIT_INTERRUPTED);
}

static int conclude(MDBX_chk_context_t *ctx) {
  int err = MDBX_SUCCESS;
  if (ctx->result.total_problems == 1 && ctx->result.problems_meta == 1 &&
      (chk_flags & (MDBX_CHK_SKIP_BTREE_TRAVERSAL | MDBX_CHK_SKIP_KV_TRAVERSAL)) == 0 &&
      (env_flags & MDBX_RDONLY) == 0 && !only_table.iov_base && stuck_meta < 0 &&
      ctx->result.steady_txnid < ctx->result.recent_txnid) {
    const size_t step_lineno = print(MDBX_chk_resolution,
                                     "Perform sync-to-disk for make steady checkpoint"
                                     " at txn-id #%" PRIi64 "...",
                                     ctx->result.recent_txnid);
    flush();
    err = error_fn("walk_pages", mdbx_env_sync_ex(ctx->env, true, false));
    if (err == MDBX_SUCCESS) {
      ctx->result.problems_meta -= 1;
      ctx->result.total_problems -= 1;
      suffix(step_lineno, "done");
    }
  }

  if (turn_meta && stuck_meta >= 0 && (chk_flags & (MDBX_CHK_SKIP_BTREE_TRAVERSAL | MDBX_CHK_SKIP_KV_TRAVERSAL)) == 0 &&
      !only_table.iov_base && (env_flags & (MDBX_RDONLY | MDBX_EXCLUSIVE)) == MDBX_EXCLUSIVE) {
    const bool successful_check = (err | ctx->result.total_problems | ctx->result.problems_meta) == 0;
    if (successful_check || force_turn_meta) {
      const size_t step_lineno =
          print(MDBX_chk_resolution, "Performing turn to the specified meta-page (%d) due to %s!", stuck_meta,
                successful_check ? "successful check" : "the -T option was given");
      flush();
      err = mdbx_env_turn_for_recovery(ctx->env, stuck_meta);
      if (err != MDBX_SUCCESS)
        error_fn("mdbx_env_turn_for_recovery", err);
      else
        suffix(step_lineno, "done");
    } else {
      print(MDBX_chk_resolution,
            "Skipping turn to the specified meta-page (%d) due to "
            "unsuccessful check!",
            stuck_meta);
      lf_flush();
    }
  }

  return err;
}

int main(int argc, char *argv[]) {
  int rc;
  char *prog = argv[0];
  char *envname;
  bool warmup = false;
  MDBX_warmup_flags_t warmup_flags = MDBX_warmup_default;

  if (argc < 2)
    usage(prog);

  double elapsed;
#if defined(_WIN32) || defined(_WIN64)
  uint64_t timestamp_start, timestamp_finish;
  timestamp_start = GetMilliseconds();
#else
  struct timespec timestamp_start, timestamp_finish;
  if (clock_gettime(CLOCK_MONOTONIC, &timestamp_start)) {
    error_fn("clock_gettime", errno);
    return EXIT_FAILURE_SYS;
  }
#endif

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
             mdbx_version.major, mdbx_version.minor, mdbx_version.patch, mdbx_version.tweak, mdbx_version.git.describe,
             mdbx_version.git.datetime, mdbx_version.git.commit, mdbx_version.git.tree, mdbx_sourcery_anchor,
             mdbx_build.datetime, mdbx_build.target, mdbx_build.compiler, mdbx_build.flags, mdbx_build.options);
      return EXIT_SUCCESS;
    case 'v':
      if (verbose >= 9 && 0)
        usage(prog);
      else {
        verbose += 1;
        if (verbose == 0 && !MDBX_DEBUG)
          printf("Verbosity level %u exposures only to"
                 " a debug/extra-logging-enabled builds (with NDEBUG undefined"
                 " or MDBX_DEBUG > 0)\n",
                 verbose);
      }
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
      break;
    case 'q':
      quiet = true;
      break;
    case 'n':
      break;
    case 'w':
      env_flags &= ~MDBX_RDONLY;
      chk_flags |= MDBX_CHK_READWRITE;
#if MDBX_MMAP_INCOHERENT_FILE_WRITE
      /* Temporary `workaround` for OpenBSD kernel's flaw.
       * See https://libmdbx.dqdkfa.ru/dead-github/issues/67 */
      env_flags |= MDBX_WRITEMAP;
#endif /* MDBX_MMAP_INCOHERENT_FILE_WRITE */
      break;
    case 'c':
      env_flags = (env_flags & ~MDBX_EXCLUSIVE) | MDBX_ACCEDE;
      break;
    case 'd':
      chk_flags |= MDBX_CHK_SKIP_BTREE_TRAVERSAL;
      break;
    case 's':
      if (only_table.iov_base && strcmp(only_table.iov_base, optarg))
        usage(prog);
      else {
        only_table.iov_base = optarg;
        only_table.iov_len = strlen(optarg);
      }
      break;
    case 'i':
      chk_flags |= MDBX_CHK_IGNORE_ORDER;
      break;
    case 'u':
      warmup = true;
      break;
    case 'U':
      warmup = true;
      warmup_flags = MDBX_warmup_force | MDBX_warmup_touchlimit | MDBX_warmup_lock;
      break;
    default:
      usage(prog);
    }
  }

  if (optind != argc - 1)
    usage(prog);

  rc = MDBX_SUCCESS;
  if (stuck_meta >= 0 && (env_flags & MDBX_EXCLUSIVE) == 0) {
    error_fmt("exclusive mode is required to using specific meta-page(%d) for "
              "checking.",
              stuck_meta);
    rc = EXIT_INTERRUPTED;
  }
  if (turn_meta) {
    if (stuck_meta < 0) {
      error_fmt("meta-page must be specified (by -0, -1 or -2 options) to turn to "
                "it.");
      rc = EXIT_INTERRUPTED;
    }
    if (env_flags & MDBX_RDONLY) {
      error_fmt("write-mode must be enabled to turn to the specified meta-page.");
      rc = EXIT_INTERRUPTED;
    }
    if (only_table.iov_base || (chk_flags & (MDBX_CHK_SKIP_BTREE_TRAVERSAL | MDBX_CHK_SKIP_KV_TRAVERSAL))) {
      error_fmt("whole database checking with b-tree traversal are required to turn "
                "to the specified meta-page.");
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
  print(MDBX_chk_result,
        "mdbx_chk %s (%s, T-%s)\nRunning for %s in 'read-%s' mode with "
        "verbosity level %u (%s)...",
        mdbx_version.git.describe, mdbx_version.git.datetime, mdbx_version.git.tree, envname,
        (env_flags & MDBX_RDONLY) ? "only" : "write", verbose,
        (verbose > 8)
            ? (MDBX_DEBUG ? "extra details for debugging" : "same as 8 for non-debug builds with MDBX_DEBUG=0")
            : "of 0..9");
  lf_flush();
  mdbx_setup_debug(
      (verbose + MDBX_LOG_WARN < MDBX_LOG_TRACE) ? (MDBX_log_level_t)(verbose + MDBX_LOG_WARN) : MDBX_LOG_TRACE,
      MDBX_DBG_DUMP | MDBX_DBG_ASSERT | MDBX_DBG_AUDIT | MDBX_DBG_LEGACY_OVERLAP | MDBX_DBG_DONT_UPGRADE, logger);

  rc = mdbx_env_create(&env);
  if (rc) {
    error_fn("mdbx_env_create", rc);
    return rc < 0 ? EXIT_FAILURE_MDBX : EXIT_FAILURE_SYS;
  }

  rc = mdbx_env_set_maxdbs(env, CORE_DBS);
  if (rc) {
    error_fn("mdbx_env_set_maxdbs", rc);
    goto bailout;
  }

  if (stuck_meta >= 0) {
    rc = mdbx_env_open_for_recovery(env, envname, stuck_meta, (env_flags & MDBX_RDONLY) ? false : true);
  } else {
    rc = mdbx_env_open(env, envname, env_flags, 0);
    if ((env_flags & MDBX_EXCLUSIVE) && (rc == MDBX_BUSY ||
#if defined(_WIN32) || defined(_WIN64)
                                         rc == ERROR_LOCK_VIOLATION || rc == ERROR_SHARING_VIOLATION
#else
                                         rc == EBUSY || rc == EAGAIN
#endif
                                         )) {
      env_flags &= ~MDBX_EXCLUSIVE;
      rc = mdbx_env_open(env, envname, env_flags | MDBX_ACCEDE, 0);
    }
  }

  if (rc) {
    error_fn("mdbx_env_open", rc);
    if (rc == MDBX_WANNA_RECOVERY && (env_flags & MDBX_RDONLY))
      print_ln(MDBX_chk_result, "Please run %s in the read-write mode (with '-w' option).", prog);
    goto bailout;
  }
  print_ln(MDBX_chk_verbose, "%s mode", (env_flags & MDBX_EXCLUSIVE) ? "monopolistic" : "cooperative");

  if (warmup) {
    anchor_lineno = print(MDBX_chk_verbose, "warming up...");
    flush();
    rc = mdbx_env_warmup(env, nullptr, warmup_flags, 3600 * 65536);
    if (MDBX_IS_ERROR(rc)) {
      error_fn("mdbx_env_warmup", rc);
      goto bailout;
    }
    suffix(anchor_lineno, rc ? "timeout" : "done");
  }

  rc = mdbx_env_chk(env, &cb, &chk, chk_flags, MDBX_chk_result + (verbose << MDBX_chk_severity_prio_shift), 0);
  if (rc) {
    if (chk.result.total_problems == 0)
      error_fn("mdbx_env_chk", rc);
    else if (rc != MDBX_EINTR && rc != MDBX_RESULT_TRUE && !user_break)
      rc = 0;
  }

bailout:
  if (env) {
    const bool dont_sync = rc != 0 || chk.result.total_problems || (chk_flags & MDBX_CHK_READWRITE) == 0;
    mdbx_env_close_ex(env, dont_sync);
  }
  flush();
  if (rc) {
    if (rc > 0)
      return user_break ? EXIT_INTERRUPTED : EXIT_FAILURE_SYS;
    return EXIT_FAILURE_MDBX;
  }

#if defined(_WIN32) || defined(_WIN64)
  timestamp_finish = GetMilliseconds();
  elapsed = (timestamp_finish - timestamp_start) * 1e-3;
#else
  if (clock_gettime(CLOCK_MONOTONIC, &timestamp_finish)) {
    error_fn("clock_gettime", errno);
    return EXIT_FAILURE_SYS;
  }
  elapsed =
      timestamp_finish.tv_sec - timestamp_start.tv_sec + (timestamp_finish.tv_nsec - timestamp_start.tv_nsec) * 1e-9;
#endif /* !WINDOWS */

  if (chk.result.total_problems) {
    print_ln(MDBX_chk_result, "Total %" PRIuSIZE " error%s detected, elapsed %.3f seconds.", chk.result.total_problems,
             (chk.result.total_problems > 1) ? "s are" : " is", elapsed);
    if (chk.result.problems_meta || chk.result.problems_kv || chk.result.problems_gc)
      return EXIT_FAILURE_CHECK_MAJOR;
    return EXIT_FAILURE_CHECK_MINOR;
  }
  print_ln(MDBX_chk_result, "No error is detected, elapsed %.3f seconds.", elapsed);
  return EXIT_SUCCESS;
}
