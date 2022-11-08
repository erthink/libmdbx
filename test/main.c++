/*
 * Copyright 2017-2022 Leonid Yuriev <leo@yuriev.ru>
 * and other libmdbx authors: please see AUTHORS file.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in the file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 */

#include "test.h++"

#if !(defined(_WIN32) || defined(_WIN64))
#include <sys/resource.h>
#include <sys/time.h>
#endif /* !Windows */

MDBX_NORETURN void usage(void) {
  puts(
      "usage:\n"
      "  --help or -h              Show this text\n"
      "Common parameters:\n"
      "  --loglevel=[0-7]|[fatal..extra]s"
      "  --pathname=...            Path and/or name of database files\n"
      "  --repeat=N                Set repeat counter\n"
      "  --threads=N               Number of thread (unsupported for now)\n"
      "  --timeout=N[s|m|h|d]      Set timeout in seconds/minutes/hours/days\n"
      "  --failfast[=YES/no]       Lill all actors on first failure/error\n"
      "  --max-readers=N           See mdbx_env_set_maxreaders() description\n"
      "  --max-tables=N            Se mdbx_env_set_maxdbs() description\n"
      "  --dump-config[=YES/no]    Dump entire test config before run\n"
      "  --progress[=YES/no]       Enable/disable progress `canary`\n"
      "  --console[=yes/no]        Enable/disable console-like output\n"
      "  --cleanup-before[=YES/no] Cleanup/remove and re-create database\n"
      "  --cleanup-after[=YES/no]  Cleanup/remove database after completion\n"
      "Database size control:\n"
      "  --pagesize=...            Database page size: min, max, 256..65536\n"
      "  --size-lower=N[K|M|G|T]   Lower-bound of size in Kb/Mb/Gb/Tb\n"
      "  --size-upper              Upper-bound of size in Kb/Mb/Gb/Tb\n"
      "  --size                    Initial size in Kb/Mb/Gb/Tb\n"
      "  --shrink-threshold        Shrink threshold in Kb/Mb/Gb/Tb\n"
      "  --growth-step             Grow step in Kb/Mb/Gb/Tb\n"
      "Predefined complex scenarios/cases:\n"
      "  --case=...   Only `basic` scenario implemented for now\n"
      "    basic == Simultaneous multi-process execution\n"
      "             of test-actors: nested,hill,ttl,copy,append,jitter,try\n"
      "Test actors:\n"
      "  --hill                        Fill-up and empty-down\n"
      "                                by CRUD-operation quads\n"
      "  --ttl                         Stochastic time-to-live simulation\n"
      "  --nested                      Nested transactionы\n"
      "                                with stochastic-size bellows\n"
      "  --jitter                      Jitter/delays simulation\n"
      "  --try                         Try write-transaction, no more\n"
      "  --copy                        Online copy/backup\n"
      "  --append                      Append-mode insertions\n"
      "  --dead.reader                 Dead-reader simulator\n"
      "  --dead.writer                 Dead-writer simulator\n"
      "Actor options:\n"
      "  --batch.read=N                Read-operations batch size\n"
      "  --batch.write=N               Write-operations batch size\n"
      "  --delay=N | --no-delay        (no)Delay test-actor before start\n"
      "  --wait4ops=N | --no-wait4ops  (no)Wait for previous test-actor\n"
      "                                completes # ops before start\n"
      "  --duration=N[s|m|h|d]         Define running duration\n"
      "  --nops=N[K|M|G|T]             Define number of operations/steps\n"
      "  --inject-writefault[=yes|NO]  TBD (see the source code)\n"
      "  --drop[=yes|NO]               Drop key-value space/table on "
      "completion\n"
      "  --ignore-dbfull[=yes|NO]      Ignore MDBX_MAP_FULL error\n"
      "  --speculum[=yes|NO]           Use internal `speculum` to check "
      "dataset\n"
      "  --geometry-jitter[=YES|no]    Use jitter for geometry upper-limit\n"
      "Keys and Value:\n"
      "  --keylen.min=N                Minimal keys length\n"
      "  --keylen.max=N                Miximal keys length\n"
      "  --keylen=N                    Set both min/max for keys length\n"
      "  --datalen.min=N               Minimal data length\n"
      "  --datalen.max=N               Miximal data length\n"
      "  --datalen=N                   Set both min/max for data length\n"
      "  --keygen.width=N              TBD (see the source code)\n"
      "  --keygen.mesh=N               TBD (see the source code)\n"
      "  --keygen.seed=N               TBD (see the source code)\n"
      "  --keygen.zerofill=yes|NO      TBD (see the source code)\n"
      "  --keygen.split=N              TBD (see the source code)\n"
      "  --keygen.rotate=N             TBD (see the source code)\n"
      "  --keygen.offset=N             TBD (see the source code)\n"
      "  --keygen.case=random          Generator case (only `random` for now)\n"
      "Database operation mode:\n"
      "  --mode={[+-]FLAG}[,[+-]FLAG]...\n"
      "    nosubdir       == MDBX_NOSUBDIR\n"
      "    rdonly         == MDBX_RDONLY\n"
      "    exclusive      == MDBX_EXCLUSIVE\n"
      "    accede         == MDBX_ACCEDE\n"
      "    nometasync     == MDBX_NOMETASYNC\n"
      "    lifo           == MDBX_LIFORECLAIM\n"
      "    nosync-safe    == MDBX_SAFE_NOSYNC\n"
      "    writemap       == MDBX_WRITEMAP\n"
      "    nosync-utterly == MDBX_UTTERLY_NOSYNC\n"
      "    perturb        == MDBX_PAGEPERTURB\n"
      "    notls          == MDBX_NOTLS\n"
      "    nordahead      == MDBX_NORDAHEAD\n"
      "    nomeminit      == MDBX_NOMEMINIT\n"
      "  --random-writemap[=YES|no]    Toggle MDBX_WRITEMAP randomly\n"
      "Key-value space/table options:\n"
      "  --table={[+-]FLAG}[,[+-]FLAG]...\n"
      "    key.reverse  == MDBX_REVERSEKEY\n"
      "    key.integer  == MDBX_INTEGERKEY\n"
      "    data.dups    == MDBX_DUPSORT\n"
      "    data.integer == MDBX_INTEGERDUP | MDBX_DUPFIXED | MDBX_DUPSORT\n"
      "    data.fixed   == MDBX_DUPFIXED | MDBX_DUPSORT\n"
      "    data.reverse == MDBX_REVERSEDUP | MDBX_DUPSORT\n");
  exit(EXIT_FAILURE);
}

//-----------------------------------------------------------------------------

void actor_params::set_defaults(const std::string &tmpdir) {
  pathname_log = "";
  loglevel =
#if defined(NDEBUG) || defined(_WIN32) || defined(_WIN64)
      logging::verbose;
#else
      logging::trace;
#endif

  pathname_db = tmpdir + "mdbx-test.db";
  mode_flags = MDBX_NOSUBDIR | MDBX_WRITEMAP | MDBX_SAFE_NOSYNC |
               MDBX_NOMEMINIT | MDBX_COALESCE | MDBX_LIFORECLAIM | MDBX_ACCEDE;
  table_flags = MDBX_DUPSORT;

  size_lower = -1;
  size_now = intptr_t((table_flags & MDBX_DUPSORT) ? 256 : 1024) << 20;
  size_now = std::min(size_now, mdbx_limits_dbsize_max(-1));
  size_upper = -1;
  shrink_threshold = -1;
  growth_step = -1;
  pagesize = -1;

  keygen.seed = 1;
  keygen.zero_fill = false;
  keygen.keycase = kc_random;
  keygen.width = (table_flags & MDBX_DUPSORT) ? 32 : 64;
  keygen.mesh = keygen.width;
  keygen.split = keygen.width / 2;
  keygen.rotate = 3;
  keygen.offset = 41;

  test_duration = 0;
  test_nops = 1000;
  nrepeat = 1;
  nthreads = 1;

  keylen_min = mdbx_keylen_min();
  keylen_max = mdbx_keylen_max();
  datalen_min = mdbx_datalen_min();
  datalen_max = std::min(mdbx_datalen_max(), 256u * 1024 + 42);

  batch_read = 42;
  batch_write = 42;

  delaystart = 0;
  waitfor_nops = 0;
  inject_writefaultn = 0;

  drop_table = false;
  ignore_dbfull = false;
  speculum = false;
  random_writemap = true;

  max_readers = 42;
  max_tables = 42;

  global::config::timeout_duration_seconds = 0 /* infinite */;
  global::config::dump_config = true;
  global::config::cleanup_before = true;
  global::config::cleanup_after = true;
  global::config::failfast = true;
  global::config::progress_indicator = true;
  global::config::console_mode = osal_istty(STDERR_FILENO);
  global::config::geometry_jitter = true;
}

namespace global {

std::vector<actor_config> actors;
std::unordered_map<unsigned, actor_config *> events;
std::unordered_map<mdbx_pid_t, actor_config *> pid2actor;
std::set<std::string> databases;
unsigned nactors;
chrono::time start_monotonic;
chrono::time deadline_monotonic;
bool singlemode;

namespace config {
unsigned timeout_duration_seconds;
bool dump_config;
bool cleanup_before;
bool cleanup_after;
bool failfast;
bool progress_indicator;
bool console_mode;
bool geometry_jitter;
} /* namespace config */

} /* namespace global */

//-----------------------------------------------------------------------------

const char global::thunk_param_prefix[] = "--execute=";

std::string thunk_param(const actor_config &config) {
  return config.serialize(global::thunk_param_prefix);
}

void cleanup() {
  log_trace(">> cleanup");
  for (const auto &db_path : global::databases) {
    int err = mdbx_env_delete(db_path.c_str(), MDBX_ENV_JUST_DELETE);
    if (err != MDBX_SUCCESS && err != MDBX_RESULT_TRUE)
      failure_perror(db_path.c_str(), err);
  }
  log_trace("<< cleanup");
}

static void fixup4qemu(actor_params &params) {
#ifdef MDBX_SAFE4QEMU
#if MDBX_WORDBITS == 32
  intptr_t safe4qemu_limit = size_t(512) << 20 /* 512 megabytes */;
#if defined(__SANITIZE_ADDRESS__)
  safe4qemu_limit >>= 1;
#else
  if (RUNNING_ON_VALGRIND)
    safe4qemu_limit >>= 1;
#endif /* __SANITIZE_ADDRESS__ */

  if (params.size_lower > safe4qemu_limit ||
      params.size_now > safe4qemu_limit ||
      params.size_upper > safe4qemu_limit) {
    params.size_upper = std::min(params.size_upper, safe4qemu_limit);
    params.size_now = std::min(params.size_now, params.size_upper);
    params.size_lower = std::min(params.size_lower, params.size_now);
    log_notice("workaround: for conformance 32-bit build with "
               "QEMU/ASAN/Valgrind database size reduced to %zu megabytes",
               safe4qemu_limit >> 20);
  }
#endif /* MDBX_WORDBITS == 32 */

#if defined(__alpha__) || defined(__alpha) || defined(__sparc__) ||            \
    defined(__sparc) || defined(__sparc64__) || defined(__sparc64)
  if (params.size_lower != params.size_upper) {
    log_notice(
        "workaround: for conformance Alpha/Sparc build with QEMU/ASAN/Valgrind "
        "enforce fixed database size %zu megabytes",
        params.size_upper >> 20);
    params.size_lower = params.size_now = params.size_upper;
  }
#endif /* Alpha || Sparc */
#endif /* MDBX_SAFE4QEMU */
  (void)params;
}

int main(int argc, char *const argv[]) {

#ifdef _DEBUG
  log_trace("#argc = %d", argc);
  for (int i = 0; i < argc; ++i)
    log_trace("#argv[%d] = %s", i, argv[i]);
#endif /* _DEBUG */

  if (argc < 2)
    failure("No parameters given. Try --help\n");

  if (argc == 2 && strncmp(argv[1], global::thunk_param_prefix,
                           strlen(global::thunk_param_prefix)) == 0)
    return test_execute(
               actor_config(argv[1] + strlen(global::thunk_param_prefix)))
               ? EXIT_SUCCESS
               : EXIT_FAILURE;

  if (argc == 2 &&
      (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-h") == 0))
    usage();

  actor_params params;
  params.set_defaults(osal_tempdir());
  global::config::dump_config = true;
  logging::setup((logging::loglevel)params.loglevel, "main");
  unsigned last_space_id = 0;

  for (int narg = 1; narg < argc; ++narg) {
    if (config::parse_option(argc, argv, narg, "dump-config",
                             global::config::dump_config))
      continue;
    if (config::parse_option(argc, argv, narg, "cleanup-before",
                             global::config::cleanup_before))
      continue;
    if (config::parse_option(argc, argv, narg, "cleanup-after",
                             global::config::cleanup_after))
      continue;
    if (config::parse_option(argc, argv, narg, "failfast",
                             global::config::failfast))
      continue;
    if (config::parse_option(argc, argv, narg, "progress",
                             global::config::progress_indicator))
      continue;
    if (config::parse_option(argc, argv, narg, "console",
                             global::config::console_mode))
      continue;
    if (config::parse_option(argc, argv, narg, "geometry-jitter",
                             global::config::geometry_jitter))
      continue;
    if (config::parse_option(argc, argv, narg, "timeout",
                             global::config::timeout_duration_seconds,
                             config::duration, 1))
      continue;

    logging::loglevel loglevel;
    if (config::parse_option(argc, argv, narg, "loglevel", loglevel)) {
      logging::setup(loglevel, "main");
      params.loglevel = static_cast<uint8_t>(loglevel);
      continue;
    }

    const char *value = nullptr;
    if (config::parse_option(argc, argv, narg, "case", &value)) {
      fixup4qemu(params);
      testcase_setup(value, params, last_space_id);
      continue;
    }
    if (config::parse_option(argc, argv, narg, "pathname", params.pathname_db))
      continue;
    if (config::parse_option(argc, argv, narg, "mode", params.mode_flags,
                             config::mode_bits))
      continue;
    if (config::parse_option(argc, argv, narg, "random-writemap",
                             params.random_writemap))
      continue;
    if (config::parse_option(argc, argv, narg, "table", params.table_flags,
                             config::table_bits)) {
      if ((params.table_flags & MDBX_DUPFIXED) == 0)
        params.table_flags &= ~MDBX_INTEGERDUP;
      if ((params.table_flags & MDBX_DUPSORT) == 0)
        params.table_flags &=
            ~(MDBX_DUPFIXED | MDBX_REVERSEDUP | MDBX_INTEGERDUP);
      const unsigned keylen_max = params.mdbx_keylen_max();
      if (params.keylen_min > keylen_max)
        params.keylen_min = keylen_max;
      if (params.keylen_max > keylen_max)
        params.keylen_max = keylen_max;
      const unsigned keylen_min = params.mdbx_keylen_min();
      if (params.keylen_min < keylen_min)
        params.keylen_min = keylen_min;
      if (params.keylen_max < keylen_min)
        params.keylen_max = keylen_min;

      const unsigned datalen_max = params.mdbx_datalen_max();
      if (params.datalen_min > datalen_max)
        params.datalen_min = datalen_max;
      if (params.datalen_max > datalen_max)
        params.datalen_max = datalen_max;
      const unsigned datalen_min = params.mdbx_datalen_min();
      if (params.datalen_min < datalen_min)
        params.datalen_min = datalen_min;
      if (params.datalen_max < datalen_min)
        params.datalen_max = datalen_min;
      continue;
    }

    if (config::parse_option(argc, argv, narg, "pagesize", params.pagesize,
                             int(mdbx_limits_pgsize_min()),
                             int(mdbx_limits_pgsize_max()))) {
      const unsigned keylen_max = params.mdbx_keylen_max();
      if (params.keylen_min > keylen_max)
        params.keylen_min = keylen_max;
      if (params.keylen_max > keylen_max)
        params.keylen_max = keylen_max;
      const unsigned datalen_max = params.mdbx_datalen_max();
      if (params.datalen_min > datalen_max)
        params.datalen_min = datalen_max;
      if (params.datalen_max > datalen_max)
        params.datalen_max = datalen_max;
      continue;
    }
    if (config::parse_option(argc, argv, narg, "repeat", params.nrepeat,
                             config::no_scale))
      continue;
    if (config::parse_option(argc, argv, narg, "threads", params.nthreads,
                             config::no_scale, 1, 64))
      continue;

    if (config::parse_option_intptr(argc, argv, narg, "size-lower",
                                    params.size_lower,
                                    mdbx_limits_dbsize_min(params.pagesize),
                                    mdbx_limits_dbsize_max(params.pagesize)))
      continue;
    int64_t i64 = params.size_upper;
    if (config::parse_option(argc, argv, narg, "size-upper-upto", i64,
                             int64_t(mdbx_limits_dbsize_min(params.pagesize)),
                             INT64_MAX, -1)) {
      if (i64 > mdbx_limits_dbsize_max(params.pagesize))
        i64 = mdbx_limits_dbsize_max(params.pagesize);
      params.size_upper = intptr_t(i64);
      continue;
    }
    if (config::parse_option_intptr(argc, argv, narg, "size-upper",
                                    params.size_upper,
                                    mdbx_limits_dbsize_min(params.pagesize),
                                    mdbx_limits_dbsize_max(params.pagesize)))
      continue;
    if (config::parse_option_intptr(argc, argv, narg, "size", params.size_now,
                                    mdbx_limits_dbsize_min(params.pagesize),
                                    mdbx_limits_dbsize_max(params.pagesize)))
      continue;
    if (config::parse_option(
            argc, argv, narg, "shrink-threshold", params.shrink_threshold, 0,
            (int)std::min((intptr_t)INT_MAX,
                          mdbx_limits_dbsize_max(params.pagesize) -
                              mdbx_limits_dbsize_min(params.pagesize))))
      continue;
    if (config::parse_option(
            argc, argv, narg, "growth-step", params.growth_step, 0,
            (int)std::min((intptr_t)INT_MAX,
                          mdbx_limits_dbsize_max(params.pagesize) -
                              mdbx_limits_dbsize_min(params.pagesize))))
      continue;

    if (config::parse_option(argc, argv, narg, "keygen.width",
                             params.keygen.width, 8, 64))
      continue;
    if (config::parse_option(argc, argv, narg, "keygen.mesh",
                             params.keygen.mesh, 0, 64))
      continue;
    if (config::parse_option(argc, argv, narg, "keygen.seed",
                             params.keygen.seed, config::no_scale))
      continue;
    if (config::parse_option(argc, argv, narg, "keygen.zerofill",
                             params.keygen.zero_fill))
      continue;
    if (config::parse_option(argc, argv, narg, "keygen.split",
                             params.keygen.split, 0, 63))
      continue;
    if (config::parse_option(argc, argv, narg, "keygen.rotate",
                             params.keygen.rotate, 0, 63))
      continue;
    if (config::parse_option(argc, argv, narg, "keygen.offset",
                             params.keygen.offset, config::binary))
      continue;
    if (config::parse_option(argc, argv, narg, "keygen.case", &value)) {
      keycase_setup(value, params);
      continue;
    }
    if (config::parse_option(argc, argv, narg, "keylen.min", params.keylen_min,
                             config::no_scale, params.mdbx_keylen_min(),
                             params.mdbx_keylen_max())) {
      if ((params.table_flags & MDBX_INTEGERKEY) ||
          params.keylen_max < params.keylen_min)
        params.keylen_max = params.keylen_min;
      continue;
    }
    if (config::parse_option(argc, argv, narg, "keylen.max", params.keylen_max,
                             config::no_scale, params.mdbx_keylen_min(),
                             params.mdbx_keylen_max())) {
      if ((params.table_flags & MDBX_INTEGERKEY) ||
          params.keylen_min > params.keylen_max)
        params.keylen_min = params.keylen_max;
      continue;
    }
    if (config::parse_option(argc, argv, narg, "keylen", params.keylen_min,
                             config::no_scale, params.mdbx_keylen_min(),
                             params.mdbx_keylen_max())) {
      params.keylen_max = params.keylen_min;
      continue;
    }
    if (config::parse_option(argc, argv, narg, "datalen.min",
                             params.datalen_min, config::no_scale,
                             params.mdbx_datalen_min(),
                             params.mdbx_datalen_max())) {
      if ((params.table_flags & (MDBX_INTEGERDUP | MDBX_DUPFIXED)) ||
          params.datalen_max < params.datalen_min)
        params.datalen_max = params.datalen_min;
      continue;
    }
    if (config::parse_option(argc, argv, narg, "datalen.max",
                             params.datalen_max, config::no_scale,
                             params.mdbx_datalen_min(),
                             params.mdbx_datalen_max())) {
      if ((params.table_flags & (MDBX_INTEGERDUP | MDBX_DUPFIXED)) ||
          params.datalen_min > params.datalen_max)
        params.datalen_min = params.datalen_max;
      continue;
    }
    if (config::parse_option(argc, argv, narg, "datalen", params.datalen_min,
                             config::no_scale, params.mdbx_datalen_min(),
                             params.mdbx_datalen_max())) {
      params.datalen_max = params.datalen_min;
      continue;
    }
    if (config::parse_option(argc, argv, narg, "batch.read", params.batch_read,
                             config::no_scale, 1))
      continue;
    if (config::parse_option(argc, argv, narg, "batch.write",
                             params.batch_write, config::no_scale, 1))
      continue;
    if (config::parse_option(argc, argv, narg, "delay", params.delaystart,
                             config::duration))
      continue;
    if (config::parse_option(argc, argv, narg, "wait4ops", params.waitfor_nops,
                             config::decimal))
      continue;
    if (config::parse_option(argc, argv, narg, "inject-writefault",
                             params.inject_writefaultn, config::decimal))
      continue;
    if (config::parse_option(argc, argv, narg, "drop", params.drop_table))
      continue;
    if (config::parse_option(argc, argv, narg, "ignore-dbfull",
                             params.ignore_dbfull))
      continue;
    if (config::parse_option(argc, argv, narg, "speculum", params.speculum))
      continue;
    if (config::parse_option(argc, argv, narg, "max-readers",
                             params.max_readers, config::no_scale, 1, 255))
      continue;
    if (config::parse_option(argc, argv, narg, "max-tables", params.max_tables,
                             config::no_scale, 1, INT16_MAX))
      continue;

    if (config::parse_option(argc, argv, narg, "no-delay", nullptr)) {
      params.delaystart = 0;
      continue;
    }
    if (config::parse_option(argc, argv, narg, "no-wait4ops", nullptr)) {
      params.waitfor_nops = 0;
      continue;
    }
    if (config::parse_option(argc, argv, narg, "duration", params.test_duration,
                             config::duration, 1)) {
      params.test_nops = 0;
      continue;
    }
    if (config::parse_option(argc, argv, narg, "nops", params.test_nops,
                             config::decimal, 1)) {
      params.test_duration = 0;
      continue;
    }
    if (config::parse_option(argc, argv, narg, "hill", &value, "auto")) {
      fixup4qemu(params);
      configure_actor(last_space_id, ac_hill, value, params);
      continue;
    }
    if (config::parse_option(argc, argv, narg, "jitter", nullptr)) {
      fixup4qemu(params);
      configure_actor(last_space_id, ac_jitter, value, params);
      continue;
    }
    if (config::parse_option(argc, argv, narg, "dead.reader", nullptr)) {
      fixup4qemu(params);
      configure_actor(last_space_id, ac_deadread, value, params);
      continue;
    }
    if (config::parse_option(argc, argv, narg, "dead.writer", nullptr)) {
      fixup4qemu(params);
      configure_actor(last_space_id, ac_deadwrite, value, params);
      continue;
    }
    if (config::parse_option(argc, argv, narg, "try", nullptr)) {
      fixup4qemu(params);
      configure_actor(last_space_id, ac_try, value, params);
      continue;
    }
    if (config::parse_option(argc, argv, narg, "copy", nullptr)) {
      fixup4qemu(params);
      configure_actor(last_space_id, ac_copy, value, params);
      continue;
    }
    if (config::parse_option(argc, argv, narg, "append", nullptr)) {
      fixup4qemu(params);
      configure_actor(last_space_id, ac_append, value, params);
      continue;
    }
    if (config::parse_option(argc, argv, narg, "ttl", nullptr)) {
      fixup4qemu(params);
      configure_actor(last_space_id, ac_ttl, value, params);
      continue;
    }
    if (config::parse_option(argc, argv, narg, "nested", nullptr)) {
      fixup4qemu(params);
      configure_actor(last_space_id, ac_nested, value, params);
      continue;
    }

    if (*argv[narg] != '-') {
      fixup4qemu(params);
      testcase_setup(argv[narg], params, last_space_id);
    } else
      failure("Unknown option '%s'. Try --help\n", argv[narg]);
  }

  if (global::config::dump_config)
    config::dump();

  //--------------------------------------------------------------------------

  if (global::actors.empty()) {
    log_notice("no testcase(s) configured, exiting");
    return EXIT_SUCCESS;
  }

  bool failed = false;
  global::start_monotonic = chrono::now_monotonic();
  global::deadline_monotonic.fixedpoint =
      (global::config::timeout_duration_seconds == 0)
          ? chrono::infinite().fixedpoint
          : global::start_monotonic.fixedpoint +
                chrono::from_seconds(global::config::timeout_duration_seconds)
                    .fixedpoint;

  if (global::config::cleanup_before)
    cleanup();

  if (global::actors.size() == 1) {
    logging::setup("main");
    global::singlemode = true;
    if (!test_execute(global::actors.front()))
      failed = true;
  } else {
    logging::setup("overlord");

    log_trace("=== preparing...");
    log_trace(">> osal_setup");
    osal_setup(global::actors);
    log_trace("<< osal_setup");

    for (auto &a : global::actors) {
      mdbx_pid_t pid;
      log_trace(">> actor_start");
      int rc = osal_actor_start(a, pid);
      log_trace("<< actor_start");
      if (rc) {
        log_trace(">> killall_actors: (%s)", "start failed");
        osal_killall_actors();
        log_trace("<< killall_actors");
        failure("Failed to start actor #%u (%s)\n", a.actor_id,
                test_strerror(rc));
      }
      global::pid2actor[pid] = &a;
    }

    log_trace("=== ready to start...");
    atexit(osal_killall_actors);
    log_trace(">> wait4barrier");
    osal_wait4barrier();
    log_trace("<< wait4barrier");

    size_t left = global::actors.size();
    log_trace("=== polling...");
    while (left > 0) {
      unsigned timeout_seconds_left = INT_MAX;
      chrono::time now_monotonic = chrono::now_monotonic();
      if (now_monotonic.fixedpoint >= global::deadline_monotonic.fixedpoint)
        timeout_seconds_left = 0;
      else {
        chrono::time left_monotonic;
        left_monotonic.fixedpoint =
            global::deadline_monotonic.fixedpoint - now_monotonic.fixedpoint;
        timeout_seconds_left = left_monotonic.seconds();
      }

      mdbx_pid_t pid;
      int rc = osal_actor_poll(pid, timeout_seconds_left);
      if (rc)
        failure("Poll error: %s (%d)\n", test_strerror(rc), rc);

      if (pid) {
        actor_status status = osal_actor_info(pid);
        actor_config *actor = global::pid2actor.at(pid);
        if (!actor)
          continue;

        if (status > as_running) {
          log_notice("actor #%u, id %d, pid %ld: %s\n", actor->actor_id,
                     actor->space_id, (long)pid, status2str(status));
          left -= 1;
          if (status != as_successful) {
            if (global::config::failfast && !failed) {
              log_trace(">> killall_actors: (%s)", "failfast");
              osal_killall_actors();
              log_trace("<< killall_actors");
            }
            failed = true;
          }
        } else {
          log_verbose("actor #%u, id %d, pid %ld: %s\n", actor->actor_id,
                      actor->space_id, (long)pid, status2str(status));
        }
      } else {
        if (timeout_seconds_left == 0)
          failure("Timeout\n");
      }
    }
    log_trace("=== done...");
  }

  log_notice("RESULT: %s\n", failed ? "Failed" : "Successful");
  if (global::config::cleanup_after) {
    if (failed)
      log_verbose("skip cleanup");
    else
      cleanup();
  }

#if !(defined(_WIN32) || defined(_WIN64))
  struct rusage spent;
  if (!getrusage(global::singlemode ? RUSAGE_SELF : RUSAGE_CHILDREN, &spent)) {
    log_notice("%6s: user %f, system %f", "CPU",
               spent.ru_utime.tv_sec + spent.ru_utime.tv_usec * 1e-6,
               spent.ru_stime.tv_sec + spent.ru_stime.tv_usec * 1e-6);
#if defined(__linux__) || defined(__gnu_linux__) || defined(__FreeBSD__) ||    \
    defined(__NetBSD__) || defined(__OpenBSD__) || defined(__BSD__) ||         \
    defined(__bsdi__) || defined(__DragonFly__) || defined(__APPLE__) ||       \
    defined(__MACH__) || defined(__sun)
    log_notice("%6s: read %ld, write %ld", "IOPs", spent.ru_inblock,
               spent.ru_oublock);
    if (spent.ru_maxrss > 0)
      log_notice("%6s: %ld Kb", "RAM",
                 spent.ru_maxrss
#if defined(__sun)
                     * getpagesize() / 1024u
#elif defined(__APPLE__)
                     / 1024u
#endif
      );
    log_notice("%6s: reclaims %ld, faults %ld, swaps %ld", "Paging",
               spent.ru_minflt, spent.ru_majflt, spent.ru_nswap);
#endif /* Linux */
  }
#endif /* !Windows */

  return failed ? EXIT_FAILURE : EXIT_SUCCESS;
}
