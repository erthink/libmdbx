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

const char *testcase2str(const actor_testcase testcase) {
  switch (testcase) {
  default:
    assert(false);
    return "?!";
  case ac_none:
    return "none";
  case ac_hill:
    return "hill";
  case ac_deadread:
    return "deadread";
  case ac_deadwrite:
    return "deadwrite";
  case ac_jitter:
    return "jitter";
  case ac_try:
    return "try";
  }
}

const char *status2str(actor_status status) {
  switch (status) {
  default:
    assert(false);
    return "?!";
  case as_debuging:
    return "debuging";
  case as_running:
    return "running";
  case as_successful:
    return "successful";
  case as_killed:
    return "killed";
  case as_failed:
    return "failed";
  }
}

const char *keygencase2str(const keygen_case keycase) {
  switch (keycase) {
  default:
    assert(false);
    return "?!";
  case kc_random:
    return "random";
  case kc_dashes:
    return "dashes";
  case kc_custom:
    return "custom";
  }
}

//-----------------------------------------------------------------------------

static void mdbx_logger(int type, const char *function, int line,
                        const char *msg, va_list args) {
  logging::loglevel level = logging::info;
  if (type & MDBX_DBG_EXTRA)
    level = logging::extra;
  if (type & MDBX_DBG_TRACE)
    level = logging::trace;
  if (type & MDBX_DBG_PRINT)
    level = logging::verbose;

  if (!function)
    function = "unknown";
  if (type & MDBX_DBG_ASSERT) {
    log_error("mdbx: assertion failure: %s, %d", function, line);
    level = logging::failure;
  }

  if (logging::output(level, strncmp(function, "mdbx_", 5) == 0 ? "%s: "
                                                                : "mdbx: %s: ",
                      function))
    logging::feed(msg, args);
  if (type & MDBX_DBG_ASSERT)
    abort();
}

int testcase::oom_callback(MDBX_env *env, int pid, mdbx_tid_t tid, uint64_t txn,
                           unsigned gap, int retry) {

  testcase *self = (testcase *)mdbx_env_get_userctx(env);

  if (retry == 0)
    log_notice("oom_callback: waitfor pid %u, thread %" PRIuPTR
               ", txn #%" PRIu64 ", gap %d",
               pid, (size_t)tid, txn, gap);

  if (self->should_continue(true)) {
    osal_yield();
    if (retry > 0)
      osal_udelay(retry * 100);
    return 0 /* always retry */;
  }

  return -1;
}

void testcase::db_prepare() {
  log_trace(">> db_prepare");
  assert(!db_guard);

  int mdbx_dbg_opts = MDBX_DBG_ASSERT | MDBX_DBG_JITTER | MDBX_DBG_DUMP;
  if (config.params.loglevel <= logging::trace)
    mdbx_dbg_opts |= MDBX_DBG_TRACE;
  if (config.params.loglevel <= logging::verbose)
    mdbx_dbg_opts |= MDBX_DBG_PRINT;
  int rc = mdbx_setup_debug(mdbx_dbg_opts, mdbx_logger);
  log_info("set mdbx debug-opts: 0x%02x", rc);

  MDBX_env *env = nullptr;
  rc = mdbx_env_create(&env);
  if (unlikely(rc != MDBX_SUCCESS))
    failure_perror("mdbx_env_create()", rc);

  assert(env != nullptr);
  db_guard.reset(env);

  rc = mdbx_env_set_userctx(env, this);
  if (unlikely(rc != MDBX_SUCCESS))
    failure_perror("mdbx_env_set_userctx()", rc);

  rc = mdbx_env_set_maxreaders(env, config.params.max_readers);
  if (unlikely(rc != MDBX_SUCCESS))
    failure_perror("mdbx_env_set_maxreaders()", rc);

  rc = mdbx_env_set_maxdbs(env, config.params.max_tables);
  if (unlikely(rc != MDBX_SUCCESS))
    failure_perror("mdbx_env_set_maxdbs()", rc);

  rc = mdbx_env_set_oomfunc(env, testcase::oom_callback);
  if (unlikely(rc != MDBX_SUCCESS))
    failure_perror("mdbx_env_set_oomfunc()", rc);

  rc = mdbx_env_set_mapsize(env, (size_t)config.params.size);
  if (unlikely(rc != MDBX_SUCCESS))
    failure_perror("mdbx_env_set_mapsize()", rc);

  log_trace("<< db_prepare");
}

void testcase::db_open() {
  log_trace(">> db_open");

  if (!db_guard)
    db_prepare();
  int rc = mdbx_env_open(db_guard.get(), config.params.pathname_db.c_str(),
                         (unsigned)config.params.mode_flags, 0640);
  if (unlikely(rc != MDBX_SUCCESS))
    failure_perror("mdbx_env_open()", rc);

  log_trace("<< db_open");
}

void testcase::db_close() {
  log_trace(">> db_close");
  cursor_guard.reset();
  txn_guard.reset();
  db_guard.reset();
  log_trace("<< db_close");
}

void testcase::txn_begin(bool readonly, unsigned flags) {
  assert((flags & MDBX_RDONLY) == 0);
  log_trace(">> txn_begin(%s, 0x%04X)", readonly ? "read-only" : "read-write",
            flags);
  assert(!txn_guard);

  MDBX_txn *txn = nullptr;
  int rc = mdbx_txn_begin(db_guard.get(), nullptr,
                          readonly ? flags | MDBX_RDONLY : flags, &txn);
  if (unlikely(rc != MDBX_SUCCESS))
    failure_perror("mdbx_txn_begin()", rc);
  txn_guard.reset(txn);

  log_trace("<< txn_begin(%s, 0x%04X)", readonly ? "read-only" : "read-write",
            flags);
}

void testcase::txn_end(bool abort) {
  log_trace(">> txn_end(%s)", abort ? "abort" : "commit");
  assert(txn_guard);

  MDBX_txn *txn = txn_guard.release();
  if (abort) {
    int rc = mdbx_txn_abort(txn);
    if (unlikely(rc != MDBX_SUCCESS))
      failure_perror("mdbx_txn_abort()", rc);
  } else {
    int rc = mdbx_txn_commit(txn);
    if (unlikely(rc != MDBX_SUCCESS))
      failure_perror("mdbx_txn_commit()", rc);
  }

  log_trace("<< txn_end(%s)", abort ? "abort" : "commit");
}

void testcase::txn_restart(bool abort, bool readonly, unsigned flags) {
  if (txn_guard)
    txn_end(abort);
  txn_begin(readonly, flags);
}

bool testcase::wait4start() {
  if (config.wait4id) {
    log_trace(">> wait4start(%u)", config.wait4id);
    assert(!global::singlemode);
    int rc = osal_waitfor(config.wait4id);
    if (rc) {
      log_trace("<< wait4start(%u), failed %s", config.wait4id,
                test_strerror(rc));
      return false;
    }
  } else {
    log_trace("== skip wait4start: not needed");
  }

  if (config.params.delaystart) {
    int rc = osal_delay(config.params.delaystart);
    if (rc) {
      log_trace("<< delay(%u), failed %s", config.params.delaystart,
                test_strerror(rc));
      return false;
    }
  } else {
    log_trace("== skip delay: not needed");
  }

  return true;
}

void testcase::kick_progress(bool active) const {
  chrono::time now = chrono::now_motonic();
  if (active) {
    static int last_point = -1;
    int point = (now.fixedpoint >> 29) & 3;
    if (point != last_point) {
      last.progress_timestamp = now;
      fprintf(stderr, "%c\b", "-\\|/"[last_point = point]);
      fflush(stderr);
    }
  } else if (now.fixedpoint - last.progress_timestamp.fixedpoint >
             chrono::from_seconds(2).fixedpoint) {
    last.progress_timestamp = now;
    fprintf(stderr, "%c\b", "@*"[now.utc & 1]);
    fflush(stderr);
  }
}

void testcase::report(size_t nops_done) {
  assert(nops_done > 0);
  if (!nops_done)
    return;

  nops_completed += nops_done;
  log_verbose("== complete +%" PRIuPTR " iteration, total %" PRIuPTR " done",
              nops_done, nops_completed);

  if (global::config::progress_indicator)
    kick_progress(true);

  if (config.signal_nops && !signalled &&
      config.signal_nops <= nops_completed) {
    log_trace(">> signal(n-ops %" PRIuPTR ")", nops_completed);
    if (!global::singlemode)
      osal_broadcast(config.actor_id);
    signalled = true;
    log_trace("<< signal(n-ops %" PRIuPTR ")", nops_completed);
  }
}

void testcase::signal() {
  if (!signalled) {
    log_trace(">> signal(forced)");
    if (!global::singlemode)
      osal_broadcast(config.actor_id);
    signalled = true;
    log_trace("<< signal(forced)");
  }
}

bool testcase::setup() {
  db_prepare();
  if (!wait4start())
    return false;

  start_timestamp = chrono::now_motonic();
  return true;
}

bool testcase::teardown() {
  log_trace(">> testcase::teardown");
  signal();
  db_close();
  log_trace("<< testcase::teardown");
  return true;
}

bool testcase::should_continue(bool check_timeout_only) const {
  bool result = true;

  if (config.params.test_duration) {
    chrono::time since;
    since.fixedpoint =
        chrono::now_motonic().fixedpoint - start_timestamp.fixedpoint;
    if (since.seconds() >= config.params.test_duration)
      result = false;
  }

  if (!check_timeout_only && config.params.test_nops &&
      nops_completed >= config.params.test_nops)
    result = false;

  if (result && global::config::progress_indicator)
    kick_progress(false);

  return result;
}

void testcase::fetch_canary() {
  mdbx_canary canary_now;
  log_trace(">> fetch_canary");

  int rc = mdbx_canary_get(txn_guard.get(), &canary_now);
  if (unlikely(rc != MDBX_SUCCESS))
    failure_perror("mdbx_canary_get()", rc);

  if (canary_now.v < last.canary.v)
    failure("fetch_canary: %" PRIu64 "(canary-now.v) < %" PRIu64
            "(canary-last.v)",
            canary_now.v, last.canary.v);
  if (canary_now.y < last.canary.y)
    failure("fetch_canary: %" PRIu64 "(canary-now.y) < %" PRIu64
            "(canary-last.y)",
            canary_now.y, last.canary.y);

  last.canary = canary_now;
  log_trace("<< fetch_canary: db-sequence %" PRIu64
            ", db-sequence.txnid %" PRIu64,
            last.canary.y, last.canary.v);
}

void testcase::update_canary(uint64_t increment) {
  mdbx_canary canary_now = last.canary;

  log_trace(">> update_canary: sequence %" PRIu64 " += %" PRIu64, canary_now.y,
            increment);
  canary_now.y += increment;

  int rc = mdbx_canary_put(txn_guard.get(), &canary_now);
  if (unlikely(rc != MDBX_SUCCESS))
    failure_perror("mdbx_canary_put()", rc);

  log_trace("<< update_canary: sequence = %" PRIu64, canary_now.y);
}

MDBX_dbi testcase::db_table_open(bool create) {
  log_trace(">> testcase::db_table_create");

  char tablename_buf[16];
  const char *tablename = nullptr;
  if (config.space_id) {
    int rc = snprintf(tablename_buf, sizeof(tablename_buf), "TBL%04u",
                      config.space_id);
    if (rc < 4 || rc >= (int)sizeof(tablename_buf) - 1)
      failure("snprintf(tablename): %d", rc);
    tablename = tablename_buf;
  }
  log_verbose("use %s table", tablename ? tablename : "MAINDB");

  MDBX_dbi handle = 0;
  int rc = mdbx_dbi_open(txn_guard.get(), tablename,
                         (create ? MDBX_CREATE : 0) | config.params.table_flags,
                         &handle);
  if (unlikely(rc != MDBX_SUCCESS))
    failure_perror("mdbx_dbi_open()", rc);

  log_trace("<< testcase::db_table_create, handle %u", handle);
  return handle;
}

void testcase::db_table_drop(MDBX_dbi handle) {
  log_trace(">> testcase::db_table_drop, handle %u", handle);

  if (config.params.drop_table) {
    int rc = mdbx_drop(txn_guard.get(), handle, true);
    if (unlikely(rc != MDBX_SUCCESS))
      failure_perror("mdbx_drop()", rc);
    log_trace("<< testcase::db_table_drop");
  } else {
    log_trace("<< testcase::db_table_drop: not needed");
  }
}

void testcase::db_table_close(MDBX_dbi handle) {
  log_trace(">> testcase::db_table_close, handle %u", handle);
  assert(!txn_guard);
  int rc = mdbx_dbi_close(db_guard.get(), handle);
  if (unlikely(rc != MDBX_SUCCESS))
    failure_perror("mdbx_dbi_close()", rc);
  log_trace("<< testcase::db_table_close");
}

//-----------------------------------------------------------------------------

bool test_execute(const actor_config &config) {
  const mdbx_pid_t pid = osal_getpid();

  if (global::singlemode) {
    logging::setup(format("single_%s", testcase2str(config.testcase)));
  } else {
    logging::setup((logging::loglevel)config.params.loglevel,
                   format("child_%u.%u", config.actor_id, config.space_id));
    log_trace(">> wait4barrier");
    osal_wait4barrier();
    log_trace("<< wait4barrier");
  }

  try {
    std::unique_ptr<testcase> test;
    switch (config.testcase) {
    case ac_hill:
      test.reset(new testcase_hill(config, pid));
      break;
    case ac_deadread:
      test.reset(new testcase_deadread(config, pid));
      break;
    case ac_deadwrite:
      test.reset(new testcase_deadwrite(config, pid));
      break;
    case ac_jitter:
      test.reset(new testcase_jitter(config, pid));
      break;
    case ac_try:
      test.reset(new testcase_try(config, pid));
      break;
    default:
      test.reset(new testcase(config, pid));
      break;
    }

    if (!test->setup())
      log_notice("test setup failed");
    else if (!test->run())
      log_notice("test failed");
    else if (!test->teardown())
      log_notice("test teardown failed");
    else {
      log_info("test successed");
      return true;
    }
  } catch (const std::exception &pipets) {
    failure("***** Exception: %s *****", pipets.what());
  }
  return false;
}
