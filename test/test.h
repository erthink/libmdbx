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
#include "config.h"
#include "keygen.h"
#include "log.h"
#include "osal.h"
#include "utils.h"

bool test_execute(const actor_config &config);
std::string thunk_param(const actor_config &config);
bool testcase_setup(const char *casename, const actor_params &params,
                    unsigned &lastid);
void configure_actor(unsigned &lastid, const actor_testcase testcase,
                     const char *id_cstr, const actor_params &params);

namespace global {

extern const char thunk_param_prefix[];
extern std::vector<actor_config> actors;
extern std::unordered_map<unsigned, actor_config *> events;
extern std::unordered_map<mdbx_pid_t, actor_config *> pid2actor;
extern std::set<std::string> databases;

namespace config {
extern unsigned timeout;
extern bool dump_config;
extern bool dont_cleanup_before;
extern bool dont_cleanup_after;
} /* namespace config */

} /* namespace global */

//-----------------------------------------------------------------------------

struct db_deleter : public std::unary_function<void, MDB_env *> {
  void operator()(MDB_env *env) const { mdbx_env_close(env); }
};

struct txn_deleter : public std::unary_function<void, MDB_txn *> {
  void operator()(MDB_txn *txn) const {
    int rc = mdbx_txn_abort(txn);
    if (rc)
      log_touble(__func__, "mdbx_txn_abort()", rc);
  }
};

struct cursor_deleter : public std::unary_function<void, MDB_cursor *> {
  void operator()(MDB_cursor *cursor) const { mdbx_cursor_close(cursor); }
};

typedef std::unique_ptr<MDB_env, db_deleter> scoped_db_guard;
typedef std::unique_ptr<MDB_txn, txn_deleter> scoped_txn_guard;
typedef std::unique_ptr<MDB_cursor, cursor_deleter> scoped_cursor_guard;

//-----------------------------------------------------------------------------

class testcase {
protected:
  const actor_config &config;
  const mdbx_pid_t pid;

  scoped_db_guard db_guard;
  scoped_txn_guard txn_guard;
  scoped_cursor_guard cursor_guard;
  bool signalled;

  void mdbx_prepare();
  void mdbx_open();
  void mdbx_close();

  bool wait4start();
  void report(size_t nops_done);
  void signal();

public:
  testcase(const actor_config &config, const mdbx_pid_t pid)
      : config(config), pid(pid) {
    loggging::setup(format("%s_%u.%u", testcase2str(config.testcase),
                           config.order, config.id));
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
