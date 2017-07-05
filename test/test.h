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

#pragma once

#include "base.h"
#include "chrono.h"
#include "config.h"
#include "keygen.h"
#include "log.h"
#include "osal.h"
#include "utils.h"

bool test_execute(const actor_config &config);
std::string thunk_param(const actor_config &config);
void testcase_setup(const char *casename, actor_params &params,
                    unsigned &last_space_id);
void configure_actor(unsigned &last_space_id, const actor_testcase testcase,
                     const char *space_id_cstr, const actor_params &params);
void keycase_setup(const char *casename, actor_params &params);

namespace global {

extern const char thunk_param_prefix[];
extern std::vector<actor_config> actors;
extern std::unordered_map<unsigned, actor_config *> events;
extern std::unordered_map<mdbx_pid_t, actor_config *> pid2actor;
extern std::set<std::string> databases;
extern unsigned nactors;
extern chrono::time start_motonic;
extern chrono::time deadline_motonic;
extern bool singlemode;

namespace config {
extern unsigned timeout_duration_seconds;
extern bool dump_config;
extern bool cleanup_before;
extern bool cleanup_after;
extern bool failfast;
extern bool progress_indicator;
} /* namespace config */

} /* namespace global */

//-----------------------------------------------------------------------------

struct db_deleter : public std::unary_function<void, MDBX_env *> {
  void operator()(MDBX_env *env) const { mdbx_env_close(env); }
};

struct txn_deleter : public std::unary_function<void, MDBX_txn *> {
  void operator()(MDBX_txn *txn) const {
    int rc = mdbx_txn_abort(txn);
    if (rc)
      log_trouble(mdbx_func_, "mdbx_txn_abort()", rc);
  }
};

struct cursor_deleter : public std::unary_function<void, MDBX_cursor *> {
  void operator()(MDBX_cursor *cursor) const { mdbx_cursor_close(cursor); }
};

typedef std::unique_ptr<MDBX_env, db_deleter> scoped_db_guard;
typedef std::unique_ptr<MDBX_txn, txn_deleter> scoped_txn_guard;
typedef std::unique_ptr<MDBX_cursor, cursor_deleter> scoped_cursor_guard;

//-----------------------------------------------------------------------------

class testcase {
protected:
  const actor_config &config;
  const mdbx_pid_t pid;

  scoped_db_guard db_guard;
  scoped_txn_guard txn_guard;
  scoped_cursor_guard cursor_guard;
  bool signalled;

  size_t nops_completed;
  chrono::time start_timestamp;
  keygen::buffer key;
  keygen::buffer data;
  keygen::maker keyvalue_maker;

  struct {
    mdbx_canary canary;
    mutable chrono::time progress_timestamp;
  } last;

  static int oom_callback(MDBX_env *env, int pid, mdbx_tid_t tid, uint64_t txn,
                          unsigned gap, int retry);

  void db_prepare();
  void db_open();
  void db_close();
  void txn_begin(bool readonly);
  void txn_end(bool abort);
  void txn_restart(bool abort, bool readonly);
  void fetch_canary();
  void update_canary(uint64_t increment);
  void kick_progress(bool active) const;

  MDBX_dbi db_table_open(bool create);
  void db_table_drop(MDBX_dbi handle);
  void db_table_close(MDBX_dbi handle);

  bool wait4start();
  void report(size_t nops_done);
  void signal();
  bool should_continue(bool check_timeout_only = false) const;

  void generate_pair(const keygen::serial_t serial, keygen::buffer &out_key,
                     keygen::buffer &out_value, keygen::serial_t data_age = 0) {
    keyvalue_maker.pair(serial, out_key, out_value, data_age);
  }

  void generate_pair(const keygen::serial_t serial,
                     keygen::serial_t data_age = 0) {
    generate_pair(serial, key, data, data_age);
  }

  bool mode_readonly() const {
    return (config.params.mode_flags & MDBX_RDONLY) ? true : false;
  }

public:
  testcase(const actor_config &config, const mdbx_pid_t pid)
      : config(config), pid(pid), signalled(false), nops_completed(0) {
    start_timestamp.reset();
    memset(&last, 0, sizeof(last));
  }

  virtual bool setup();
  virtual bool run() { return true; }
  virtual bool teardown();
  virtual ~testcase() {}
};

class testcase_hill : public testcase {
  typedef testcase inherited;

public:
  testcase_hill(const actor_config &config, const mdbx_pid_t pid)
      : testcase(config, pid) {}
  bool setup();
  bool run();
  bool teardown();
};

class testcase_deadread : public testcase {
  typedef testcase inherited;

public:
  testcase_deadread(const actor_config &config, const mdbx_pid_t pid)
      : testcase(config, pid) {}
  bool setup();
  bool run();
  bool teardown();
};

class testcase_deadwrite : public testcase {
  typedef testcase inherited;

public:
  testcase_deadwrite(const actor_config &config, const mdbx_pid_t pid)
      : testcase(config, pid) {}
  bool setup();
  bool run();
  bool teardown();
};

class testcase_jitter : public testcase {
  typedef testcase inherited;

public:
  testcase_jitter(const actor_config &config, const mdbx_pid_t pid)
      : testcase(config, pid) {}
  bool setup();
  bool run();
  bool teardown();
};
