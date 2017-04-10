/*
 * Copyright 2017 Leonid Yuriev <leo@yuriev.ru>
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

#include "test.h"

void __noreturn usage(void) {
  printf("usage:\n"
         "\tFIXME\n");
  exit(EXIT_FAILURE);
}

//-----------------------------------------------------------------------------

void actor_params::set_defaults(void) {
  pathname_log = "";
  loglevel =
#ifdef NDEBUG
      loggging::notice;
#else
      loggging::trace;
#endif

  pathname_db =
#ifdef __linux__
      "/dev/shm/test_tmpdb.mdbx";
#else
      "test_tmpdb.mdbx";
#endif
  mode_flags = MDB_NOSUBDIR | MDB_WRITEMAP | MDB_MAPASYNC | MDB_NORDAHEAD |
               MDB_NOMEMINIT | MDBX_COALESCE | MDBX_LIFORECLAIM;
  table_flags = MDB_DUPSORT;
  size = 1024 * 1024;
  seed = 1;

  test_duration = 0;
  test_nrecords = 1000;
  nrepeat = 1;
  nthreads = 1;

  keylen_min = 0;
  keylen_max = 42;
  datalen_min = 0;
  datalen_max = 256;

  batch_read = 4;
  batch_write = 4;

  delaystart = 0;
  waitfor_nops = 0;

  drop_table = false;

  max_readers = 42;
  max_tables = 42;
}

namespace global {

std::vector<actor_config> actors;
std::unordered_map<unsigned, actor_config *> events;
std::unordered_map<mdbx_pid_t, actor_config *> pid2actor;
std::set<std::string> databases;
unsigned nactors;

namespace config {
unsigned timeout;
bool dump_config;
bool dont_cleanup_before;
bool dont_cleanup_after;
} /* namespace config */

} /* namespace global */

//-----------------------------------------------------------------------------

const char global::thunk_param_prefix[] = "--execute=";

std::string thunk_param(const actor_config &config) {
  return config.serialize(global::thunk_param_prefix);
}

void cleanup() {
  log_trace(">> cleanup");
  /* TODO: remove each database */
  log_trace("<< cleanup");
}

int main(int argc, char *const argv[]) {

#ifdef _DEBUG
  log_trace("#argc = %d", argc);
  for (int i = 0; i < argc; ++i)
    log_trace("#argv[%d] = %s", i, argv[i]);
#endif /* _DEBUG */

  if (argc < 2)
    failure("No parameters given\n");

  if (argc == 2 &&
      strncmp(argv[1], global::thunk_param_prefix,
              strlen(global::thunk_param_prefix)) == 0)
    return test_execute(
               actor_config(argv[1] + strlen(global::thunk_param_prefix)))
               ? EXIT_SUCCESS
               : EXIT_FAILURE;

  actor_params params;
  params.set_defaults();
  global::config::dump_config = true;
  loggging::setup((loggging::loglevel)params.loglevel, "main");
  unsigned lastid = 0;

  if (argc == 2 && strncmp(argv[1], "--case=", 7) == 0) {
    const char *casename = argv[1] + 7;
    if (!testcase_setup(casename, params, lastid))
      failure("unknown testcase `%s`", casename);
  } else {
    for (int i = 1; i < argc;) {
      const char *value = nullptr;
      if (config::parse_option(argc, argv, i, "basic", nullptr)) {
        bool ok = testcase_setup("basic", params, lastid);
        assert(ok);
        (void)ok;
      } else if (config::parse_option(argc, argv, i, "race", nullptr)) {
        bool ok = testcase_setup("race", params, lastid);
        assert(ok);
        (void)ok;
      } else if (config::parse_option(argc, argv, i, "bench", nullptr)) {
        bool ok = testcase_setup("bench", params, lastid);
        assert(ok);
        (void)ok;
      } else if (config::parse_option(argc, argv, i, "pathname",
                                      params.pathname_db) ||
                 config::parse_option(argc, argv, i, "mode", params.mode_flags,
                                      config::mode_bits) ||
                 config::parse_option(argc, argv, i, "table",
                                      params.table_flags, config::table_bits) ||
                 config::parse_option(argc, argv, i, "size", params.size,
                                      config::binary, 4096 * 4) ||
                 config::parse_option(argc, argv, i, "seed", params.seed,
                                      config::no_scale) ||
                 config::parse_option(argc, argv, i, "repeat", params.nrepeat,
                                      config::no_scale) ||
                 config::parse_option(argc, argv, i, "threads", params.nthreads,
                                      config::no_scale, 1, 64) ||
                 config::parse_option(argc, argv, i, "timeout",
                                      global::config::timeout, config::duration,
                                      1) ||
                 config::parse_option(argc, argv, i, "keylen.min",
                                      params.keylen_min, config::no_scale, 0,
                                      params.keylen_max) ||
                 config::parse_option(argc, argv, i, "keylen.max",
                                      params.keylen_max, config::no_scale,
                                      params.keylen_min,
                                      mdbx_get_maxkeysize(0)) ||
                 config::parse_option(argc, argv, i, "datalen.min",
                                      params.datalen_min, config::no_scale, 0,
                                      params.datalen_max) ||
                 config::parse_option(argc, argv, i, "datalen.max",
                                      params.datalen_max, config::no_scale,
                                      params.datalen_min, MDBX_MAXDATASIZE) ||
                 config::parse_option(argc, argv, i, "batch.read",
                                      params.batch_read, config::no_scale, 1) ||
                 config::parse_option(argc, argv, i, "batch.write",
                                      params.batch_write, config::no_scale,
                                      1) ||
                 config::parse_option(argc, argv, i, "delay", params.delaystart,
                                      config::duration) ||
                 config::parse_option(argc, argv, i, "wait4ops",
                                      params.waitfor_nops, config::decimal) ||
                 config::parse_option(argc, argv, i, "drop",
                                      params.drop_table) ||
                 config::parse_option(argc, argv, i, "dump-config",
                                      global::config::dump_config) ||
                 config::parse_option(argc, argv, i, "dont-cleanup-before",
                                      global::config::dont_cleanup_before) ||
                 config::parse_option(argc, argv, i, "dont-cleanup-after",
                                      global::config::dont_cleanup_after) ||
                 config::parse_option(argc, argv, i, "max-readers",
                                      params.max_readers, config::no_scale, 1,
                                      255) ||
                 config::parse_option(argc, argv, i, "max-tables",
                                      params.max_tables, config::no_scale, 1,
                                      INT16_MAX) ||
                 false) {
        continue;
      } else if (config::parse_option(argc, argv, i, "no-delay", nullptr)) {
        params.delaystart = 0;
      } else if (config::parse_option(argc, argv, i, "no-wait", nullptr)) {
        params.waitfor_nops = 0;
      } else if (config::parse_option(argc, argv, i, "duration",
                                      params.test_duration, config::duration,
                                      1)) {
        params.test_nrecords = 0;
        continue;
      } else if (config::parse_option(argc, argv, i, "records",
                                      params.test_nrecords, config::decimal,
                                      1)) {
        params.test_duration = 0;
        continue;
      } else if (config::parse_option(argc, argv, i, "hill", &value)) {
        configure_actor(lastid, ac_hill, value, params);
        continue;
      } else if (config::parse_option(argc, argv, i, "jitter", nullptr)) {
        configure_actor(lastid, ac_jitter, value, params);
        continue;
      } else if (config::parse_option(argc, argv, i, "dead.reader", nullptr)) {
        configure_actor(lastid, ac_deadread, value, params);
        continue;
      } else if (config::parse_option(argc, argv, i, "dead.writer", nullptr)) {
        configure_actor(lastid, ac_deadwrite, value, params);
        continue;
      } else {
        failure("Unknown option '%s'\n", argv[i]);
      }
    }
  }

  if (global::config::dump_config)
    config::dump(stdout);

  bool failed = false;
  if (global::actors.size()) {
    loggging::setup("overlord");

    if (!global::config::dont_cleanup_before)
      cleanup();

    log_trace(">> osal_setup");
    osal_setup(global::actors);
    log_trace("<< osal_setup");

    for (auto &a : global::actors) {
      mdbx_pid_t pid;
      log_trace(">> actor_start");
      int rc = osal_actor_start(a, pid);
      log_trace("<< actor_start");
      if (rc) {
        log_trace(">> killall_actors");
        osal_killall_actors();
        log_trace("<< killall_actors");
        failure("Failed to start actor #%u (%s)\n", a.order, test_strerror(rc));
      }
      global::pid2actor[pid] = &a;
    }

    atexit(osal_killall_actors);
    log_trace(">> wait4barrier");
    osal_wait4barrier();
    log_trace("<< wait4barrier");
  }

  time_t timestamp_start = time(nullptr);
  size_t left = global::actors.size();

  while (left > 0) {
    unsigned timeout = INT_MAX;
    if (global::config::timeout) {
      time_t timestamp_now = time(nullptr);
      if (timestamp_now - timestamp_start > global::config::timeout)
        timeout = 0;
      else
        timeout = global::config::timeout -
                  (unsigned)(timestamp_now - timestamp_start);
    }

    mdbx_pid_t pid;
    int rc = osal_actor_poll(pid, timeout);
    if (rc)
      failure("Poll error: %s (%d)\n", test_strerror(rc), rc);

    if (pid) {
      actor_status status = osal_actor_info(pid);
      actor_config *actor = global::pid2actor.at(pid);
      if (!actor)
        continue;

      log_info("actor #%u, id %d, pid %u: %s\n", actor->order, actor->id, pid,
               status2str(status));
      if (status > as_running) {
        left -= 1;
        if (status != as_successful)
          failed = true;
      }
    } else {
      if (global::config::timeout &&
          time(nullptr) - timestamp_start > global::config::timeout)
        failure("Timeout\n");
    }
  }

  log_notice("OVERALL: %s\n", failed ? "Failed" : "Successful");
  if (!global::config::dont_cleanup_before) {
    if (failed)
      log_info("skip cleanup");
    else
      cleanup();
  }
  return failed ? EXIT_FAILURE : EXIT_SUCCESS;
}
