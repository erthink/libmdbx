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

#pragma once

#include "base.h++"
#include "chrono.h++"
#include "config.h++"
#include "keygen.h++"
#include "log.h++"
#include "osal.h++"
#include "utils.h++"

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
void testcase_setup(const char *casename, const actor_params &params,
                    unsigned &last_space_id);
void configure_actor(unsigned &last_space_id, const actor_testcase testcase,
                     const char *space_id_cstr, actor_params params);
void keycase_setup(const char *casename, actor_params &params);

namespace global {

extern const char thunk_param_prefix[];
extern std::vector<actor_config> actors;
extern std::unordered_map<unsigned, actor_config *> events;
extern std::unordered_map<mdbx_pid_t, actor_config *> pid2actor;
extern std::set<std::string> databases;
extern unsigned nactors;
extern chrono::time start_monotonic;
extern chrono::time deadline_monotonic;
extern bool singlemode;

namespace config {
extern unsigned timeout_duration_seconds;
extern bool dump_config;
extern bool cleanup_before;
extern bool cleanup_after;
extern bool failfast;
extern bool progress_indicator;
extern bool console_mode;
extern bool geometry_jitter;
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

using scoped_db_guard = std::unique_ptr<MDBX_env, db_deleter>;
using scoped_txn_guard = std::unique_ptr<MDBX_txn, txn_deleter>;
using scoped_cursor_guard = std::unique_ptr<MDBX_cursor, cursor_deleter>;

//-----------------------------------------------------------------------------

class testcase;

class registry {
  struct record {
    actor_testcase id;
    std::string name;
    bool (*review_params)(actor_params &);
    testcase *(*constructor)(const actor_config &, const mdbx_pid_t);
  };
  std::unordered_map<std::string, const record *> name2id;
  std::unordered_map<int, const record *> id2record;
  static bool add(const record *item);
  static registry *instance();

public:
  template <class TESTCASE> struct factory : public record {
    factory(const actor_testcase id, const char *name) {
      this->id = id;
      this->name = name;
      review_params = TESTCASE::review_params;
      constructor = [](const actor_config &config,
                       const mdbx_pid_t pid) -> testcase * {
        return new TESTCASE(config, pid);
      };
      add(this);
    }
  };
  static bool review_actor_params(const actor_testcase id,
                                  actor_params &params);
  static testcase *create_actor(const actor_config &config,
                                const mdbx_pid_t pid);
};

#define REGISTER_TESTCASE(NAME)                                                \
  static registry::factory<testcase_##NAME> gRegister_##NAME(                  \
      ac_##NAME, MDBX_STRINGIFY(NAME))

class testcase {
protected:
  using data_view = mdbx::slice;
  static inline data_view iov2dataview(const MDBX_val &v) {
    return (v.iov_base && v.iov_len)
               ? data_view(static_cast<const char *>(v.iov_base), v.iov_len)
               : data_view();
  }
  static inline data_view iov2dataview(const keygen::buffer &b) {
    return iov2dataview(b->value);
  }

  using Item = std::pair<::mdbx::buffer<>, ::mdbx::buffer<>>;

  static MDBX_val dataview2iov(const data_view &v) {
    MDBX_val r;
    r.iov_base = (void *)v.data();
    r.iov_len = v.size();
    return r;
  }
  struct ItemCompare {
    const testcase *context;
    ItemCompare(const testcase *owner) : context(owner) {
      /* The context->txn_guard may be empty/null here */
    }

    bool operator()(const Item &a, const Item &b) const {
      MDBX_val va = dataview2iov(a.first), vb = dataview2iov(b.first);
      assert(context->txn_guard.get() != nullptr);
      int cmp = mdbx_cmp(context->txn_guard.get(), context->dbi, &va, &vb);
      if (cmp == 0 &&
          (context->config.params.table_flags & MDBX_DUPSORT) != 0) {
        va = dataview2iov(a.second);
        vb = dataview2iov(b.second);
        cmp = mdbx_dcmp(context->txn_guard.get(), context->dbi, &va, &vb);
      }
      return cmp < 0;
    }
  };

  // for simplify the set<pair<key,value>>
  // is used instead of multimap<key,value>
  using SET = std::set<Item, ItemCompare>;

  const actor_config &config;
  const mdbx_pid_t pid;

  MDBX_dbi dbi{0};
  scoped_db_guard db_guard;
  scoped_txn_guard txn_guard;
  scoped_cursor_guard cursor_guard;
  bool signalled{false};
  bool need_speculum_assign{false};

  uint64_t nops_completed{0};
  chrono::time start_timestamp;
  keygen::buffer key;
  keygen::buffer data;
  keygen::maker keyvalue_maker;

  struct {
    MDBX_canary canary;
  } last;

  SET speculum{ItemCompare(this)}, speculum_committed{ItemCompare(this)};
#ifndef SPECULUM_CURSORS
#define SPECULUM_CURSORS 1
#endif /* SPECULUM_CURSORS */
#if SPECULUM_CURSORS
  scoped_cursor_guard speculum_cursors[5 + 1];
  void speculum_prepare_cursors(const Item &item);
  void speculum_check_cursor(const char *where, const char *stage,
                             const testcase::SET::const_iterator &it,
                             int cursor_err, const MDBX_val &cursor_key,
                             const MDBX_val &cursor_data) const;
  void speculum_check_cursor(const char *where, const char *stage,
                             const testcase::SET::const_iterator &it,
                             MDBX_cursor *cursor,
                             const MDBX_cursor_op op) const;
#endif /* SPECULUM_CURSORS */
  void speculum_check_iterator(const char *where, const char *stage,
                               const testcase::SET::const_iterator &it,
                               const MDBX_val &k, const MDBX_val &v) const;

  void verbose(const char *where, const char *stage,
               const testcase::SET::const_iterator &it) const;
  void verbose(const char *where, const char *stage, const MDBX_val &k,
               const MDBX_val &v, int err = MDBX_SUCCESS) const;

  bool is_same(const Item &a, const Item &b) const;
  bool is_same(const SET::const_iterator &it, const MDBX_val &k,
               const MDBX_val &v) const;

  bool speculum_verify();
  bool check_batch_get();
  int insert(const keygen::buffer &akey, const keygen::buffer &adata,
             MDBX_put_flags_t flags);
  int replace(const keygen::buffer &akey, const keygen::buffer &new_value,
              const keygen::buffer &old_value, MDBX_put_flags_t flags);
  int remove(const keygen::buffer &akey, const keygen::buffer &adata);

  static int hsr_callback(const MDBX_env *env, const MDBX_txn *txn,
                          mdbx_pid_t pid, mdbx_tid_t tid, uint64_t laggard,
                          unsigned gap, size_t space,
                          int retry) MDBX_CXX17_NOEXCEPT;

  MDBX_env_flags_t actual_env_mode{MDBX_ENV_DEFAULTS};
  bool is_nested_txn_available() const {
    return (actual_env_mode & MDBX_WRITEMAP) == 0;
  }
  void kick_progress(bool active) const;
  void db_prepare();
  void db_open();
  void db_close();
  void txn_begin(bool readonly, MDBX_txn_flags_t flags = MDBX_TXN_READWRITE);
  int breakable_commit();
  void txn_end(bool abort);
  int breakable_restart();
  void txn_restart(bool abort, bool readonly,
                   MDBX_txn_flags_t flags = MDBX_TXN_READWRITE);
  void cursor_open(MDBX_dbi handle);
  void cursor_close();
  void cursor_renew();
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
  bool is_handle_created_in_current_txn(const MDBX_dbi handle, MDBX_txn *txn);

  bool wait4start();
  void report(size_t nops_done);
  void signal();
  bool should_continue(bool check_timeout_only = false) const;

  void generate_pair(const keygen::serial_t serial, keygen::buffer &out_key,
                     keygen::buffer &out_value, keygen::serial_t data_age) {
    keyvalue_maker.pair(serial, out_key, out_value, data_age, false);
  }

  void generate_pair(const keygen::serial_t serial) {
    keyvalue_maker.pair(serial, key, data, 0, true);
  }

  bool mode_readonly() const {
    return (config.params.mode_flags & MDBX_RDONLY) ? true : false;
  }

public:
  testcase(const actor_config &config, const mdbx_pid_t pid)
      : config(config), pid(pid) {
    start_timestamp.reset();
    memset(&last, 0, sizeof(last));
  }

  static bool review_params(actor_params &params) {
    // silently fix key/data length for fixed-length modes
    if ((params.table_flags & MDBX_INTEGERKEY) &&
        params.keylen_min != params.keylen_max)
      params.keylen_min = params.keylen_max;
    if ((params.table_flags & (MDBX_INTEGERDUP | MDBX_DUPFIXED)) &&
        params.datalen_min != params.datalen_max)
      params.datalen_min = params.datalen_max;
    return true;
  }

  virtual bool setup();
  virtual bool run() { return true; }
  virtual bool teardown();
  virtual ~testcase() {}
};

//-----------------------------------------------------------------------------

class testcase_ttl : public testcase {
  using inherited = testcase;

protected:
  struct {
    unsigned max_window_size{0};
    unsigned max_step_size{0};
  } sliding;
  unsigned edge2window(uint64_t edge);
  unsigned edge2count(uint64_t edge);

public:
  testcase_ttl(const actor_config &config, const mdbx_pid_t pid)
      : inherited(config, pid) {}
  bool setup() override;
  bool run() override;
};
