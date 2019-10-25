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

#pragma once

#include "base.h"
#include "chrono.h"
#include "config.h"
#include "keygen.h"
#include "log.h"
#include "osal.h"
#include "utils.h"

#include <deque>
#include <set>
#include <stack>
#include <tuple>

#ifndef HAVE_cxx17_std_string_view
#if __cplusplus >= 201703L && __has_include(<string_view>)
#include <string_view>
#define HAVE_cxx17_std_string_view 1
#else
#define HAVE_cxx17_std_string_view 0
#endif
#endif /* HAVE_cxx17_std_string_view */

#if HAVE_cxx17_std_string_view
#include <string_view>
#endif

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
extern bool console_mode;
} /* namespace config */

} /* namespace global */

//-----------------------------------------------------------------------------

struct db_deleter /* : public std::unary_function<void, MDBX_env *> */ {
  void operator()(MDBX_env *env) const { mdbx_env_close(env); }
};

struct txn_deleter /* : public std::unary_function<void, MDBX_txn *> */ {
  void operator()(MDBX_txn *txn) const {
    int rc = mdbx_txn_abort(txn);
    if (rc)
      log_trouble(__func__, "mdbx_txn_abort()", rc);
  }
};

struct cursor_deleter /* : public std::unary_function<void, MDBX_cursor *> */ {
  void operator()(MDBX_cursor *cursor) const { mdbx_cursor_close(cursor); }
};

typedef std::unique_ptr<MDBX_env, db_deleter> scoped_db_guard;
typedef std::unique_ptr<MDBX_txn, txn_deleter> scoped_txn_guard;
typedef std::unique_ptr<MDBX_cursor, cursor_deleter> scoped_cursor_guard;

//-----------------------------------------------------------------------------

class testcase {
protected:
#if HAVE_cxx17_std_string_view
  using data_view = std::string_view;
#else
  using data_view = std::string;
#endif
  static inline data_view S(const MDBX_val &v) {
    return data_view(static_cast<const char *>(v.iov_base), v.iov_len);
  }
  static inline data_view S(const keygen::buffer &b) { return S(b->value); }

  using Item = std::pair<std::string, std::string>;
  struct ItemCompare {
    const testcase *context;
    ItemCompare(const testcase *owner) : context(owner) {}

    bool operator()(const Item &a, const Item &b) const {
      MDBX_val va, vb;
      va.iov_base = (void *)a.first.data();
      va.iov_len = a.first.size();
      vb.iov_base = (void *)b.first.data();
      vb.iov_len = b.first.size();
      int cmp = mdbx_cmp(context->txn_guard.get(), context->dbi, &va, &vb);
      if (cmp == 0 &&
          (context->config.params.table_flags & MDBX_DUPSORT) != 0) {
        va.iov_base = (void *)a.second.data();
        va.iov_len = a.second.size();
        vb.iov_base = (void *)b.second.data();
        vb.iov_len = b.second.size();
        cmp = mdbx_dcmp(context->txn_guard.get(), context->dbi, &va, &vb);
      }
      return cmp < 0;
    }
  };
  using SET = std::set<Item, ItemCompare>;

  const actor_config &config;
  const mdbx_pid_t pid;

  MDBX_dbi dbi;
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
  } last;

  SET speculum;
  bool speculum_verify() const;
  int insert(const keygen::buffer &akey, const keygen::buffer &adata,
             unsigned flags);
  int replace(const keygen::buffer &akey, const keygen::buffer &new_value,
              const keygen::buffer &old_value, unsigned flags);
  int remove(const keygen::buffer &akey, const keygen::buffer &adata);

  static int oom_callback(MDBX_env *env, mdbx_pid_t pid, mdbx_tid_t tid,
                          uint64_t txn, unsigned gap, size_t space, int retry);

  bool is_nested_txn_available() const {
    return (config.params.mode_flags & MDBX_WRITEMAP) == 0;
  }
  void kick_progress(bool active) const;
  void db_prepare();
  void db_open();
  void db_close();
  void txn_begin(bool readonly, unsigned flags = 0);
  int breakable_commit();
  void txn_end(bool abort);
  int breakable_restart();
  void txn_restart(bool abort, bool readonly, unsigned flags = 0);
  void cursor_open(MDBX_dbi handle);
  void cursor_close();
  void txn_inject_writefault(void);
  void txn_inject_writefault(MDBX_txn *txn);
  void fetch_canary();
  void update_canary(uint64_t increment);
  void checkdata(const char *step, MDBX_dbi handle, MDBX_val key2check,
                 MDBX_val expected_valued);
  unsigned txn_underutilization_x256(MDBX_txn *txn) const;

  MDBX_dbi db_table_open(bool create);
  void db_table_drop(MDBX_dbi handle);
  void db_table_clear(MDBX_dbi handle, MDBX_txn *txn = nullptr);
  void db_table_close(MDBX_dbi handle);
  int db_open__begin__table_create_open_clean(MDBX_dbi &handle);

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
      : config(config), pid(pid), signalled(false), nops_completed(0),
        speculum(ItemCompare(this)) {
    start_timestamp.reset();
    memset(&last, 0, sizeof(last));
  }

  virtual bool setup();
  virtual bool run() { return true; }
  virtual bool teardown();
  virtual ~testcase() {}
};

class testcase_ttl : public testcase {
public:
  testcase_ttl(const actor_config &config, const mdbx_pid_t pid)
      : testcase(config, pid) {}
  bool run() override;
};

class testcase_hill : public testcase {
  using inherited = testcase;
  SET speculum_commited;

public:
  testcase_hill(const actor_config &config, const mdbx_pid_t pid)
      : testcase(config, pid), speculum_commited(ItemCompare(this)) {}
  bool run() override;
};

class testcase_append : public testcase {
public:
  testcase_append(const actor_config &config, const mdbx_pid_t pid)
      : testcase(config, pid) {}
  bool run() override;
};

class testcase_deadread : public testcase {
public:
  testcase_deadread(const actor_config &config, const mdbx_pid_t pid)
      : testcase(config, pid) {}
  bool run() override;
};

class testcase_deadwrite : public testcase {
public:
  testcase_deadwrite(const actor_config &config, const mdbx_pid_t pid)
      : testcase(config, pid) {}
  bool run() override;
};

class testcase_jitter : public testcase {
public:
  testcase_jitter(const actor_config &config, const mdbx_pid_t pid)
      : testcase(config, pid) {}
  bool run() override;
};

class testcase_try : public testcase {
public:
  testcase_try(const actor_config &config, const mdbx_pid_t pid)
      : testcase(config, pid) {}
  bool run() override;
};

class testcase_copy : public testcase {
  const std::string copy_pathname;
  void copy_db(const bool with_compaction);

public:
  testcase_copy(const actor_config &config, const mdbx_pid_t pid)
      : testcase(config, pid),
        copy_pathname(config.params.pathname_db + "-copy") {}
  bool run() override;
};

class testcase_nested : public testcase {
  using inherited = testcase;
  using FIFO = std::deque<std::pair<uint64_t, unsigned>>;

  uint64_t serial;
  FIFO fifo;
  std::stack<std::tuple<scoped_txn_guard, uint64_t, FIFO, SET>> stack;

  bool trim_tail(unsigned window_width);
  bool grow_head(unsigned head_count);
  bool pop_txn(bool abort);
  bool pop_txn() {
    return pop_txn(inherited::is_nested_txn_available() ? flipcoin_x3()
                                                        : flipcoin_x2());
  }
  void push_txn();
  bool stochastic_breakable_restart_with_nested(bool force_restart = false);

public:
  testcase_nested(const actor_config &config, const mdbx_pid_t pid)
      : testcase(config, pid) {}
  bool setup() override;
  bool run() override;
  bool teardown() override;
};
