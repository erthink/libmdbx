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
  logging::loglevel level = logging::trace;
  if (type & MDBX_DBG_PRINT)
    level = logging::info;
  if (type & MDBX_DBG_ASSERT) {
    log_error("libmdbx assertion failure: %s, %d",
              function ? function : "unknown", line);
    level = logging::failure;
  }

  output(level, msg, args);
  if (type & MDBX_DBG_ASSERT)
    abort();
}

void testcase::mdbx_prepare() {
  log_trace(">> mdbx_prepare");

  int rc = mdbx_setup_debug(MDBX_DBG_DNT, mdbx_debug_logger, MDBX_DBG_DNT);
  log_info("libmdbx debug-flags: 0x%02x", rc);

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

  log_trace("<< mdbx_prepare");
}

void testcase::mdbx_open() {
  log_trace(">> mdbx_open");
  int rc = mdbx_env_open(db_guard.get(), config.params.pathname_db.c_str(),
                         (unsigned)config.params.mode_flags, 0640);
  if (rc != MDB_SUCCESS)
    failure_perror("mdbx_env_open()", rc);
  log_trace("<< mdbx_open");
}

void testcase::mdbx_close() {
  log_trace(">> mdbx_close");
  cursor_guard.reset();
  txn_guard.reset();
  db_guard.reset();
  log_trace("<< mdbx_close");
}

bool testcase::wait4start() {
  if (config.wait4id) {
    log_trace(">> wait4start(%u)", config.wait4id);
    int rc = osal_waitfor(config.wait4id);
    if (rc) {
      log_trace("<< wait4start(%u), failed %s", config.wait4id,
                test_strerror(rc));
      return false;
    }
    return true;
  } else {
    log_trace("== wait4start(not needed)");
    return true;
  }
}

void testcase::report(size_t nops_done) {
  if (config.signal_nops && !signalled && config.signal_nops <= nops_done) {
    log_trace(">> signal(n-ops %zu)", nops_done);
    osal_broadcast(config.id);
    signalled = true;
    log_trace("<< signal(n-ops %zu)", nops_done);
  }
}

void testcase::signal() {
  if (!signalled) {
    log_trace(">> signal(forced)");
    osal_broadcast(config.id);
    signalled = true;
    log_trace("<< signal(forced)");
  }
}

bool testcase::setup() {
  mdbx_prepare();
  return wait4start();
}

bool testcase::teardown() {
  log_trace(">> testcase::teardown");
  signal();
  mdbx_close();
  log_trace("<< testcase::teardown");
  return true;
}

//-----------------------------------------------------------------------------

bool test_execute(const actor_config &config) {
  const mdbx_pid_t pid = osal_getpid();
  logging::setup((logging::loglevel)config.params.loglevel,
                 format("child_%u.%u", config.order, config.id));

  log_trace(">> wait4barrier");
  osal_wait4barrier();
  log_trace("<< wait4barrier");

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
    failure("Exception: %s", pipets.what());
  }
  return false;
}
