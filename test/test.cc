/*
 * Copyright 2017-2019 Leonid Yuriev <leo@yuriev.ru>
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
  case ac_copy:
    return "copy";
  case ac_append:
    return "append";
  case ac_ttl:
    return "ttl";
  case ac_nested:
    return "nested";
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
  case as_coredump:
    return "coredump";
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

int testcase::oom_callback(MDBX_env *env, mdbx_pid_t pid, mdbx_tid_t tid,
                           uint64_t txn, unsigned gap, size_t space,
                           int retry) {

  testcase *self = (testcase *)mdbx_env_get_userctx(env);

  if (retry == 0)
    log_notice("oom_callback: waitfor pid %u, thread %" PRIuPTR
               ", txn #%" PRIu64 ", gap %d, scape %zu",
               pid, (size_t)tid, txn, gap, space);

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

  MDBX_env *env = nullptr;
  int rc = mdbx_env_create(&env);
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

  rc = mdbx_env_set_geometry(
      env, config.params.size_lower, config.params.size_now,
      config.params.size_upper, config.params.growth_step,
      config.params.shrink_threshold, config.params.pagesize);
  if (unlikely(rc != MDBX_SUCCESS))
    failure_perror("mdbx_env_set_mapsize()", rc);

  log_trace("<< db_prepare");
}

void testcase::db_open() {
  log_trace(">> db_open");

  if (!db_guard)
    db_prepare();

  jitter_delay(true);
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

int testcase::breakable_commit() {
  int rc = MDBX_SUCCESS;
  log_trace(">> txn_commit");
  assert(txn_guard);

  MDBX_txn *txn = txn_guard.release();
  txn_inject_writefault(txn);
  int err = mdbx_txn_commit(txn);
  if (unlikely(err != MDBX_SUCCESS)) {
    if (err == MDBX_MAP_FULL && config.params.ignore_dbfull) {
      rc = err;
      err = mdbx_txn_abort(txn);
      if (unlikely(err != MDBX_SUCCESS && err != MDBX_THREAD_MISMATCH &&
                   err != MDBX_BAD_TXN))
        failure_perror("mdbx_txn_abort()", err);
    } else
      failure_perror("mdbx_txn_commit()", err);
  }

  log_trace("<< txn_commit: %s", rc ? "failed" : "Ok");
  return rc;
}

unsigned testcase::txn_underutilization_x256(MDBX_txn *txn) const {
  if (txn) {
    MDBX_txn_info info;
    int err = mdbx_txn_info(txn, &info, false);
    if (unlikely(err != MDBX_SUCCESS))
      failure_perror("mdbx_txn_info()", err);
    const size_t left = size_t(info.txn_space_leftover);
    const size_t total =
        size_t(info.txn_space_leftover) + size_t(info.txn_space_dirty);
    return (unsigned)(left / (total >> 8));
  }
  return 0;
}

void testcase::txn_end(bool abort) {
  log_trace(">> txn_end(%s)", abort ? "abort" : "commit");
  assert(txn_guard);

  MDBX_txn *txn = txn_guard.release();
  if (abort) {
    int err = mdbx_txn_abort(txn);
    if (unlikely(err != MDBX_SUCCESS && err != MDBX_THREAD_MISMATCH &&
                 err != MDBX_BAD_TXN))
      failure_perror("mdbx_txn_abort()", err);
  } else {
    txn_inject_writefault(txn);
    int err = mdbx_txn_commit(txn);
    if (unlikely(err != MDBX_SUCCESS))
      failure_perror("mdbx_txn_commit()", err);
  }

  log_trace("<< txn_end(%s)", abort ? "abort" : "commit");
}

void testcase::cursor_open(MDBX_dbi handle) {
  log_trace(">> cursor_open(%u)", handle);
  assert(!cursor_guard);
  assert(txn_guard);

  MDBX_cursor *cursor = nullptr;
  int rc = mdbx_cursor_open(txn_guard.get(), handle, &cursor);
  if (unlikely(rc != MDBX_SUCCESS))
    failure_perror("mdbx_cursor_open()", rc);
  cursor_guard.reset(cursor);

  log_trace("<< cursor_open(%u)", handle);
}

void testcase::cursor_close() {
  log_trace(">> cursor_close()");
  assert(cursor_guard);
  MDBX_cursor *cursor = cursor_guard.release();
  mdbx_cursor_close(cursor);
  log_trace("<< cursor_close()");
}

int testcase::breakable_restart() {
  int rc = MDBX_SUCCESS;
  if (txn_guard)
    rc = breakable_commit();
  if (cursor_guard)
    cursor_close();
  txn_begin(false, 0);
  return rc;
}

void testcase::txn_restart(bool abort, bool readonly, unsigned flags) {
  if (txn_guard)
    txn_end(abort);
  if (cursor_guard)
    cursor_close();
  txn_begin(readonly, flags);
}

void testcase::txn_inject_writefault(void) {
  if (txn_guard)
    txn_inject_writefault(txn_guard.get());
}

void testcase::txn_inject_writefault(MDBX_txn *txn) {
  if (config.params.inject_writefaultn && txn) {
    if (config.params.inject_writefaultn <= nops_completed &&
        (mdbx_txn_flags(txn) & MDBX_RDONLY) == 0) {
      log_verbose(
          "== txn_inject_writefault(): got %u nops or more, inject FAULT",
          config.params.inject_writefaultn);
      log_flush();
#if defined(_WIN32) || defined(_WIN64) || defined(_WINDOWS)
      TerminateProcess(GetCurrentProcess(), 42);
#else
      raise(SIGKILL);
#endif
    }
  }
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
  if (!global::config::progress_indicator)
    return;
  logging::progress_canary(active);
}

void testcase::report(size_t nops_done) {
  assert(nops_done > 0);
  if (!nops_done)
    return;

  nops_completed += nops_done;
  log_debug("== complete +%" PRIuPTR " iteration, total %" PRIuPTR " done",
            nops_done, nops_completed);

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
  nops_completed = 0;
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

  if (result)
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

int testcase::db_open__begin__table_create_open_clean(MDBX_dbi &handle) {
  db_open();

  int err, retry_left = 42;
  for (;;) {
    txn_begin(false);
    handle = db_table_open(true);
    db_table_clear(handle);
    err = breakable_commit();
    if (likely(err == MDBX_SUCCESS)) {
      txn_begin(false);
      return MDBX_SUCCESS;
    }
    if (--retry_left == 0)
      break;
    jitter_delay(true);
  }
  log_notice("db_begin_table_create_open_clean: bailout due '%s'",
             mdbx_strerror(err));
  return err;
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
  log_debug("use %s table", tablename ? tablename : "MAINDB");

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
      failure_perror("mdbx_drop(delete=true)", rc);
    log_trace("<< testcase::db_table_drop");
  } else {
    log_trace("<< testcase::db_table_drop: not needed");
  }
}

void testcase::db_table_clear(MDBX_dbi handle, MDBX_txn *txn) {
  log_trace(">> testcase::db_table_clear, handle %u", handle);
  int rc = mdbx_drop(txn ? txn : txn_guard.get(), handle, false);
  if (unlikely(rc != MDBX_SUCCESS))
    failure_perror("mdbx_drop(delete=false)", rc);
  log_trace("<< testcase::db_table_clear");
}

void testcase::db_table_close(MDBX_dbi handle) {
  log_trace(">> testcase::db_table_close, handle %u", handle);
  assert(!txn_guard);
  int rc = mdbx_dbi_close(db_guard.get(), handle);
  if (unlikely(rc != MDBX_SUCCESS))
    failure_perror("mdbx_dbi_close()", rc);
  log_trace("<< testcase::db_table_close");
}

void testcase::checkdata(const char *step, MDBX_dbi handle, MDBX_val key2check,
                         MDBX_val expected_valued) {
  MDBX_val actual_value = expected_valued;
  int rc = mdbx_get_nearest(txn_guard.get(), handle, &key2check, &actual_value);
  if (unlikely(rc != MDBX_SUCCESS))
    failure_perror(step, rc);
  if (!is_samedata(&actual_value, &expected_valued))
    failure("%s data mismatch", step);
}

//-----------------------------------------------------------------------------

bool test_execute(const actor_config &config_const) {
  const mdbx_pid_t pid = osal_getpid();
  actor_config config = config_const;

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
    case ac_copy:
      test.reset(new testcase_copy(config, pid));
      break;
    case ac_append:
      test.reset(new testcase_append(config, pid));
      break;
    case ac_ttl:
      test.reset(new testcase_ttl(config, pid));
      break;
    case ac_nested:
      test.reset(new testcase_nested(config, pid));
      break;
    default:
      test.reset(new testcase(config, pid));
      break;
    }

    size_t iter = 0;
    do {
      iter++;
      if (!test->setup()) {
        log_notice("test setup failed");
        return false;
      }
      if (!test->run()) {
        log_notice("test failed");
        return false;
      }
      if (!test->teardown()) {
        log_notice("test teardown failed");
        return false;
      }

      if (config.params.nrepeat == 1)
        log_verbose("test successed");
      else {
        if (config.params.nrepeat)
          log_verbose("test successed (iteration %zi of %zi)", iter,
                      size_t(config.params.nrepeat));
        else
          log_verbose("test successed (iteration %zi)", iter);
        config.params.keygen.seed += INT32_C(0xA4F4D37B);
      }

    } while (config.params.nrepeat == 0 || iter < config.params.nrepeat);
    return true;
  } catch (const std::exception &pipets) {
    failure("***** Exception: %s *****", pipets.what());
    return false;
  }
}

//-----------------------------------------------------------------------------

int testcase::insert(const keygen::buffer &akey, const keygen::buffer &adata,
                     unsigned flags) {
  int err = mdbx_put(txn_guard.get(), dbi, &akey->value, &adata->value, flags);
  if (err == MDBX_SUCCESS && config.params.speculum) {
    const auto S_key = S(akey);
    const auto S_data = S(adata);
    const bool inserted = speculum.emplace(S_key, S_data).second;
    assert(inserted);
    (void)inserted;
  }
  return err;
}

int testcase::replace(const keygen::buffer &akey,
                      const keygen::buffer &new_data,
                      const keygen::buffer &old_data, unsigned flags) {
  if (config.params.speculum) {
    const auto S_key = S(akey);
    const auto S_old = S(old_data);
    const auto S_new = S(new_data);
    const auto removed = speculum.erase(SET::key_type(S_key, S_old));
    assert(removed == 1);
    (void)removed;
    const bool inserted = speculum.emplace(S_key, S_new).second;
    assert(inserted);
    (void)inserted;
  }
  return mdbx_replace(txn_guard.get(), dbi, &akey->value, &new_data->value,
                      &old_data->value, flags);
}

int testcase::remove(const keygen::buffer &akey, const keygen::buffer &adata) {
  if (config.params.speculum) {
    const auto S_key = S(akey);
    const auto S_data = S(adata);
    const auto removed = speculum.erase(SET::key_type(S_key, S_data));
    assert(removed == 1);
    (void)removed;
  }
  return mdbx_del(txn_guard.get(), dbi, &akey->value, &adata->value);
}

bool testcase::speculum_verify() const {
  if (!config.params.speculum)
    return true;

  char dump_key[128], dump_value[128];
  char dump_mkey[128], dump_mvalue[128];

  MDBX_cursor *cursor;
  int err = mdbx_cursor_open(txn_guard.get(), dbi, &cursor);
  if (err != MDBX_SUCCESS)
    failure_perror("mdbx_cursor_open()", err);

  bool rc = true;
  MDBX_val akey, avalue;
  MDBX_val mkey, mvalue;
  err = mdbx_cursor_get(cursor, &akey, &avalue, MDBX_FIRST);

  assert(std::is_sorted(speculum.cbegin(), speculum.cend(), ItemCompare(this)));
  auto it = speculum.cbegin();
  while (true) {
    if (err != MDBX_SUCCESS) {
      akey.iov_len = avalue.iov_len = 0;
      akey.iov_base = avalue.iov_base = nullptr;
    }
    const auto S_key = S(akey);
    const auto S_data = S(avalue);
    if (it != speculum.cend()) {
      mkey.iov_base = (void *)it->first.c_str();
      mkey.iov_len = it->first.size();
      mvalue.iov_base = (void *)it->second.c_str();
      mvalue.iov_len = it->second.size();
    }
    if (err == MDBX_SUCCESS && it != speculum.cend() && S_key == it->first &&
        S_data == it->second) {
      ++it;
      err = mdbx_cursor_get(cursor, &akey, &avalue, MDBX_NEXT);
    } else if (err == MDBX_SUCCESS &&
               (it == speculum.cend() || S_key < it->first ||
                (S_key == it->first && S_data < it->second))) {
      if (it != speculum.cend()) {
        log_error("extra pair: db{%s, %s} < mi{%s, %s}",
                  mdbx_dump_val(&akey, dump_key, sizeof(dump_key)),
                  mdbx_dump_val(&avalue, dump_value, sizeof(dump_value)),
                  mdbx_dump_val(&mkey, dump_mkey, sizeof(dump_mkey)),
                  mdbx_dump_val(&mvalue, dump_mvalue, sizeof(dump_mvalue)));
      } else {
        log_error("extra pair: db{%s, %s} < mi.END",
                  mdbx_dump_val(&akey, dump_key, sizeof(dump_key)),
                  mdbx_dump_val(&avalue, dump_value, sizeof(dump_value)));
      }
      err = mdbx_cursor_get(cursor, &akey, &avalue, MDBX_NEXT);
      rc = false;
    } else if (it != speculum.cend() &&
               (err == MDBX_NOTFOUND || S_key > it->first ||
                (S_key == it->first && S_data > it->second))) {
      if (err == MDBX_NOTFOUND) {
        log_error("lost pair: db.END > mi{%s, %s}",
                  mdbx_dump_val(&mkey, dump_mkey, sizeof(dump_mkey)),
                  mdbx_dump_val(&mvalue, dump_mvalue, sizeof(dump_mvalue)));
      } else {
        log_error("lost pair: db{%s, %s} > mi{%s, %s}",
                  mdbx_dump_val(&akey, dump_key, sizeof(dump_key)),
                  mdbx_dump_val(&avalue, dump_value, sizeof(dump_value)),
                  mdbx_dump_val(&mkey, dump_mkey, sizeof(dump_mkey)),
                  mdbx_dump_val(&mvalue, dump_mvalue, sizeof(dump_mvalue)));
      }
      ++it;
      rc = false;
    } else if (err == MDBX_NOTFOUND && it == speculum.cend()) {
      break;
    } else if (err != MDBX_SUCCESS) {
      failure_perror("mdbx_cursor_get()", err);
    } else {
      assert(!"WTF?");
    }
  }

  mdbx_cursor_close(cursor);
  return rc;
}
