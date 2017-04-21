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

//-----------------------------------------------------------------------------

static void mdbx_debug_logger(int type, const char *function, int line,
                              const char *msg, va_list args) {
  logging::loglevel level = logging::info;
  if (type & MDBX_DBG_EXTRA)
    level = logging::extra;
  if (type & MDBX_DBG_TRACE)
    level = logging::trace;
  if (type & MDBX_DBG_PRINT)
    level = logging::verbose;

  if (type & MDBX_DBG_ASSERT) {
    log_error("mdbx: assertion failure: %s, %d",
              function ? function : "unknown", line);
    level = logging::failure;
  }

  if (logging::output(level, "mdbx: "))
    logging::feed(msg, args);
  if (type & MDBX_DBG_ASSERT)
    abort();
}

void testcase::db_prepare() {
  log_trace(">> db_prepare");
  assert(!db_guard);

  int mdbx_dbg_opts = MDBX_DBG_ASSERT;
  if (config.params.loglevel <= logging::trace)
    mdbx_dbg_opts |= MDBX_DBG_TRACE;
  if (config.params.loglevel <= logging::verbose)
    mdbx_dbg_opts |= MDBX_DBG_PRINT;
  int rc = mdbx_setup_debug(mdbx_dbg_opts, mdbx_debug_logger, MDBX_DBG_DNT);
  log_info("set mdbx debug-opts: 0x%02x", rc);

  MDB_env *env = nullptr;
  rc = mdbx_env_create(&env);
  if (rc != MDB_SUCCESS)
    failure_perror("mdbx_env_create()", rc);

  assert(env != nullptr);
  db_guard.reset(env);

  rc = mdbx_env_set_userctx(env, this);
  if (rc != MDB_SUCCESS)
    failure_perror("mdbx_env_set_userctx()", rc);

  rc = mdbx_env_set_maxreaders(env, config.params.max_readers);
  if (rc != MDB_SUCCESS)
    failure_perror("mdbx_env_set_maxreaders()", rc);

  rc = mdbx_env_set_maxdbs(env, config.params.max_tables);
  if (rc != MDB_SUCCESS)
    failure_perror("mdbx_env_set_maxdbs()", rc);

  rc = mdbx_env_set_mapsize(env, (size_t)config.params.size);
  if (rc != MDB_SUCCESS)
    failure_perror("mdbx_env_set_mapsize()", rc);

  log_trace("<< db_prepare");
}

void testcase::db_open() {
  log_trace(">> db_open");

  if (!db_guard)
    db_prepare();
  int rc = mdbx_env_open(db_guard.get(), config.params.pathname_db.c_str(),
                         (unsigned)config.params.mode_flags, 0640);
  if (rc != MDB_SUCCESS)
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

void testcase::txn_begin(bool readonly) {
  log_trace(">> txn_begin(%s)", readonly ? "read-only" : "read-write");
  assert(!txn_guard);

  MDB_txn *txn = nullptr;
  int rc =
      mdbx_txn_begin(db_guard.get(), nullptr, readonly ? MDB_RDONLY : 0, &txn);
  if (rc != MDB_SUCCESS)
    failure_perror("mdbx_txn_begin()", rc);
  txn_guard.reset(txn);

  log_trace("<< txn_begin(%s)", readonly ? "read-only" : "read-write");
}

void testcase::txn_end(bool abort) {
  log_trace(">> txn_end(%s)", abort ? "abort" : "commit");
  assert(txn_guard);

  MDB_txn *txn = txn_guard.release();
  if (abort) {
    int rc = mdbx_txn_abort(txn);
    if (rc != MDB_SUCCESS)
      failure_perror("mdbx_txn_abort()", rc);
  } else {
    int rc = mdbx_txn_commit(txn);
    if (rc != MDB_SUCCESS)
      failure_perror("mdbx_txn_commit()", rc);
  }

  log_trace("<< txn_end(%s)", abort ? "abort" : "commit");
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

void testcase::report(size_t nops_done) {
  nops_completed += nops_done;
  log_verbose("== complete +%zu iteration, total %zu done", nops_done,
              nops_completed);

  if (config.signal_nops && !signalled &&
      config.signal_nops <= nops_completed) {
    log_trace(">> signal(n-ops %zu)", nops_completed);
    if (!global::singlemode)
      osal_broadcast(config.actor_id);
    signalled = true;
    log_trace("<< signal(n-ops %zu)", nops_completed);
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

bool testcase::should_continue() const {
  bool result = true;

  if (config.params.test_duration) {
    chrono::time since;
    since.fixedpoint =
        chrono::now_motonic().fixedpoint - start_timestamp.fixedpoint;
    if (since.seconds() >= config.params.test_duration)
      result = false;
  }

  if (config.params.test_nops && nops_completed >= config.params.test_nops)
    result = false;

  return result;
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
