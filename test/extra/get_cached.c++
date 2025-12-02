#include "mdbx.h++"

#include <chrono>
#include <iostream>
#include <vector>
#if defined(__cpp_lib_latch) && __cpp_lib_latch >= 201907L
#include <latch>
#include <thread>
#endif
#include <array>
#include <functional>
#include <iterator>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <unordered_map>

#if defined(__cpp_lib_latch) && __cpp_lib_latch >= 201907L
/* TODO */
#endif

static void logger_nofmt(MDBX_log_level_t loglevel, const char *function, int line, const char *msg,
                         unsigned length) noexcept {
  (void)length;
  (void)loglevel;
  fflush(nullptr);
  std::cout << function << ":" << line << " " << msg;
  std::cout.flush();
}

static char log_buffer[1024];

MDBX_MAYBE_UNUSED static std::string format_va(const char *fmt, va_list ap) {
  va_list ones;
  va_copy(ones, ap);
#ifdef _MSC_VER
  int needed = _vscprintf(fmt, ap);
#else
  int needed = vsnprintf(nullptr, 0, fmt, ap);
#endif
  assert(needed >= 0);
  std::string result;
  result.reserve(size_t(needed + 1));
  result.resize(size_t(needed), '\0');
  assert(int(result.capacity()) > needed);
  int actual = vsnprintf(const_cast<char *>(result.data()), result.capacity(), fmt, ones);
  assert(actual == needed);
  (void)actual;
  va_end(ones);
  return result;
}

static void debug(int line, const char *msg, ...) {
  va_list ap;
  va_start(ap, msg);
  // std::string result = format_va(msg, ap);
  va_end(ap);
  // std::cout << "line " << line << ": " << result << std::endl;
  // std::cout.flush();
  (void)line;
}

//--------------------------------------------------------------------------------------------

typedef MDBX_cache_result_t (*get_cached_t)(const MDBX_txn *txn, MDBX_dbi dbi, const MDBX_val *key, MDBX_val *data,
                                            MDBX_cache_entry_t *entry);

static bool check_state(const MDBX_cache_result_t &r, const MDBX_error_t wanna_errcode,
                        const MDBX_cache_status_t wanna_status, unsigned line) {
  if (r.errcode == wanna_errcode && r.status == wanna_status)
    return true;
  std::cerr << "unecpected (at " << line
            << "): "
               "err "
            << r.errcode << " (wanna " << wanna_errcode
            << "), "
               "status "
            << r.status << " (wanna " << wanna_status << ")" << std::endl;
  return false;
}

static bool check_state_and_value(const MDBX_cache_result_t &r, const mdbx::slice &value,
                                  const MDBX_error_t wanna_errcode, const MDBX_cache_status_t wanna_status,
                                  const mdbx::slice &wanna_value, unsigned line) {

  bool ok = check_state(r, wanna_errcode, wanna_status, line);
  if (value != wanna_value) {
    std::cerr << "mismatch value (at " << line << "): " << value << " (wanna " << wanna_value << ")" << std::endl;
    ok = false;
  }
  return ok;
}

bool case0_trivia(mdbx::env env) {
  get_cached_t get_cached = mdbx_cache_get_SingleThreaded;
  auto txn = env.start_write();
  auto table = txn.create_map("case0", mdbx::key_mode::usual, mdbx::value_mode::single);

  MDBX_cache_entry_t entry;
  mdbx_cache_init(&entry);
  MDBX_val data;
  MDBX_cache_result_t r;

  bool ok = true;
  r = get_cached(txn, table, mdbx::slice("key"), &data, &entry);
  ok = check_state(r, MDBX_NOTFOUND, MDBX_CACHE_DIRTY, __LINE__) && ok;

  txn.commit_embark_read();
  r = get_cached(txn, table, mdbx::slice("key"), &data, &entry);
  ok = check_state(r, MDBX_NOTFOUND, MDBX_CACHE_REFRESHED, __LINE__) && ok;

  // drops the table as if it were done by another process
  {
    auto params = mdbx::env::operate_parameters(42);
    params.options.no_sticky_threads = true;
    mdbx::env_managed env2(env.get_path(), params);
    auto txn2 = env2.start_write();
    txn2.drop_map("case0");
    txn2.commit();
  }
  txn.renew_reading();
  r = get_cached(txn, table, mdbx::slice("key"), &data, &entry);
  ok = check_state(r, MDBX_NOTFOUND, MDBX_CACHE_CONFIRMED, __LINE__) && ok;

  txn.abort();
  txn = env.start_write();
  table = txn.create_map("case0", mdbx::key_mode::usual, mdbx::value_mode::single);
  r = get_cached(txn, table, mdbx::slice("key"), &data, &entry);
  ok = check_state(r, MDBX_NOTFOUND, MDBX_CACHE_DIRTY, __LINE__) && ok;

  txn.commit_embark_read();
  r = get_cached(txn, table, mdbx::slice("key"), &data, &entry);
  ok = check_state(r, MDBX_NOTFOUND, MDBX_CACHE_CONFIRMED, __LINE__) && ok;

  txn.abort();
  txn = env.start_write();
  txn.insert(table, "key", "value");
  r = get_cached(txn, table, mdbx::slice("key"), &data, &entry);
  ok = check_state_and_value(r, data, MDBX_SUCCESS, MDBX_CACHE_DIRTY, "value", __LINE__) && ok;

  txn.commit_embark_read();
  r = get_cached(txn, table, mdbx::slice("key"), &data, &entry);
  ok = check_state_and_value(r, data, MDBX_SUCCESS, MDBX_CACHE_REFRESHED, "value", __LINE__) && ok;

  txn.abort();
  txn = env.start_write();
  r = get_cached(txn, table, mdbx::slice("key"), &data, &entry);
  ok = check_state_and_value(r, data, MDBX_SUCCESS, MDBX_CACHE_HIT, "value", __LINE__) && ok;
  txn.update(table, "key", "42");
  r = get_cached(txn, table, mdbx::slice("key"), &data, &entry);
  ok = check_state_and_value(r, data, MDBX_SUCCESS, MDBX_CACHE_DIRTY, "42", __LINE__) && ok;

  txn.commit_embark_read();
  r = get_cached(txn, table, mdbx::slice("key"), &data, &entry);
  ok = check_state_and_value(r, data, MDBX_SUCCESS, MDBX_CACHE_REFRESHED, "42", __LINE__) && ok;

  MDBX_cache_entry_t entry2;
  mdbx_cache_init(&entry2);
  txn.abort();
  txn = env.start_write();
  txn.insert(table, "key2", "value2");
  r = get_cached(txn, table, mdbx::slice("key"), &data, &entry);
  ok = check_state_and_value(r, data, MDBX_SUCCESS, MDBX_CACHE_DIRTY, "42", __LINE__) && ok;
  r = get_cached(txn, table, mdbx::slice("key2"), &data, &entry2);
  ok = check_state_and_value(r, data, MDBX_SUCCESS, MDBX_CACHE_DIRTY, "value2", __LINE__) && ok;

  txn.commit_embark_read();
  r = get_cached(txn, table, mdbx::slice("key"), &data, &entry);
  ok = check_state_and_value(r, data, MDBX_SUCCESS, MDBX_CACHE_REFRESHED, "42", __LINE__) && ok;
  r = get_cached(txn, table, mdbx::slice("key2"), &data, &entry2);
  ok = check_state_and_value(r, data, MDBX_SUCCESS, MDBX_CACHE_REFRESHED, "value2", __LINE__) && ok;

  return ok;
}

//--------------------------------------------------------------------------------------------

using buffer = mdbx::default_buffer;
using checking_map = std::map<buffer, buffer>;
using prng = std::mt19937_64;

struct generator {
  enum keys_order { shocastic, begin = shocastic, increasing, decreasing, zigzag, zagzig, end };

  keys_order order;
  int serial;
  uint64_t salt;

  generator(keys_order order, prng &rnd) : order(order), serial(0), salt(rnd()) {}

  void seed(prng &rnd) { salt ^= rnd(); }

  void rolldice(bool black_either_red = false) {
    /* just the linear congruential PRNG */
    salt = salt * UINT64_C(6364136223846793005) + (black_either_red ? 1 : UINT64_C(1442695040888963407));
  }

  void turn() {
    rolldice(true);
    serial += 1;
  }

  static buffer make_value(uint32_t salt) { return buffer::hex(salt) /*.append('\0')*/; }
  buffer make_value() { return make_value(uint32_t(salt)); }

  static buffer make_key(uint32_t salt) { return buffer::base58(salt) /*.append('\0')*/; }

  bool coin() const { return intptr_t(salt) < 0; }

  buffer make_key() {
    uint32_t base = 0;
    switch (order) {
    default:
      base = uint32_t((salt >> 32) + (salt ^ serial * INT64_C(36207372675342559)) % UINT64_C(14635046041047337));
      debug(__LINE__, "%s(%i) key-base %0x08u", "rnd", serial, base);
      break;
    case increasing:
      base = uint32_t(serial);
      debug(__LINE__, "%s(%i) key-base %0x08u", "inc", serial, base);
      break;
    case decreasing:
      base = uint32_t(INT_MAX - serial);
      debug(__LINE__, "%s(%i) key-base %0x08u", "dec", serial, base);
      break;
    case zigzag:
      base = uint32_t(serial);
      base = base << 5 | base >> 5;
      debug(__LINE__, "%s(%i) key-base %0x08u", "zig", serial, base);
      break;
    case zagzig:
      base = uint32_t(-serial);
      base = (base & 63) | ((base ^ (base >> 1)) & ~63);
      debug(__LINE__, "%s(%i) key-base %0x08u", "zag", serial, base);
      break;
    }
    turn();
    return make_key(base);
  }
};

MDBX_NORETURN static void unexpected(unsigned line) {
  std::cout.flush();
  std::cerr.flush();
  throw std::runtime_error(std::string("unexpected at line ") + std::to_string(line));
}

static bool failed(unsigned line) {
  std::cout.flush();
  std::cerr << "failed ad line " << line << std::endl;
  std::cerr.flush();
  return false;
}

struct track_point {
  mdbx::txnid changed_or_dirtied;
  buffer value;
  track_point(mdbx::txnid changed_or_dirtied, const buffer &value)
      : changed_or_dirtied(changed_or_dirtied), value(value) {}
  track_point(mdbx::txnid changed_or_dirtied, buffer &&value)
      : changed_or_dirtied(changed_or_dirtied), value(std::move(value)) {}
  track_point(const track_point &) = default;
  track_point(track_point &&) = default;
  track_point &operator=(const track_point &) = default;
  track_point &operator=(track_point &&) = default;

  bool is_erased() const noexcept { return value.is_null(); }
  static buffer erased_value() { return buffer::null(); }
};

struct history {
  std::map<buffer, std::vector<track_point>> dataset;

  void erase_all(mdbx::txnid mvcc) {
    for (auto &pair : dataset) {
      auto &track = pair.second;
      if (track.empty())
        unexpected(__LINE__);
      if (!track.back().is_erased())
        track.emplace_back(mvcc, track_point::erased_value());
    }
  }

  void erase(mdbx::txnid mvcc, const mdbx::slice &key) {
    auto &track = dataset.at(buffer(key, true));
    if (track.empty())
      unexpected(__LINE__);
    if (track.back().changed_or_dirtied > mvcc)
      unexpected(__LINE__);
    track.emplace_back(mvcc, track_point::erased_value());
  }

  void dirtied_by_neighbour(mdbx::txnid mvcc, const mdbx::slice &key) {
    auto &track = dataset.at(buffer(key, true));
    if (track.empty())
      unexpected(__LINE__);
    auto &last = track.back();
    if (last.changed_or_dirtied > mvcc)
      unexpected(__LINE__);
    assert(!last.is_erased());
    // assert(last.value.is_inplace());
    track.emplace_back(mvcc, last.value.make_inplace_or_reference());
  }

  void insert(mdbx::txnid mvcc, buffer &&key, buffer &&value) {
    auto &track = dataset[key];
    track.emplace_back(mvcc, std::move(value));
  }

  void update(mdbx::txnid mvcc, buffer &&key, buffer &&value) {
    auto &track = dataset.at(key);
    if (track.empty())
      unexpected(__LINE__);
    if (track.back().changed_or_dirtied > mvcc)
      unexpected(__LINE__);
    track.emplace_back(mvcc, std::move(value));
  }
};

struct track_context {
  std::map<mdbx::txnid, mdbx::txn_managed> rx;
  std::map<mdbx::map_handle, history> tables;
  using table_ref = decltype(tables)::value_type;
  std::vector<table_ref *> tables_vector;
  get_cached_t get_cached = mdbx_cache_get_SingleThreaded;

  struct entry {
    table_ref *ref;
    buffer key;
    mdbx::slice value;
    mdbx::cache_entry cache;

    entry(table_ref *ref, const buffer &key, const mdbx::slice &value, const mdbx::cache_entry &cache)
        : ref(ref), key(key), value(value), cache(cache) {}
    entry(table_ref *ref, buffer &&key, const mdbx::slice &value, const mdbx::cache_entry &cache)
        : ref(ref), key(std::move(key)), value(value), cache(cache) {}
    entry(const entry &) = default;
    entry(entry &&) = default;
    entry &operator=(const entry &) = default;
    entry &operator=(entry &&) = default;
  };

  using pool = std::vector<entry>;

  void create_tables(const std::string &prefix, mdbx::txn &txn, size_t n_tables) {
    for (size_t i = 0; i < n_tables; ++i)
      tables.emplace(std::make_pair(
          txn.create_map(prefix + std::to_string(i), mdbx::key_mode::usual, mdbx::value_mode::single), history()));

    tables_vector.reserve(tables.size());
    for (auto &pair : tables)
      tables_vector.push_back(&pair);
  }

  void turn(mdbx::txn &txn, generator &gen, table_ref &table, size_t wanna_deep) {
    const auto mvcc = txn.id();
    auto cursor = txn.open_cursor(table.first);
    buffer value, key;
    if (wanna_deep == 0) {
      debug(__LINE__, "mvcc %zu, dbi %u, zero-deep (erase-all)", (size_t)mvcc, table.first.dbi);
      txn.clear_map(table.first);
      table.second.erase_all(mvcc);
    } else {
      mdbx::txn::map_stat stat;
      size_t changes = 0;
      do {
        stat = txn.get_map_stat(table.first);
        key = gen.make_key();
        if (stat.ms_depth < wanna_deep || (wanna_deep == 1 && stat.ms_entries < 5)) {
          // growth/insert
          while (!txn.try_insert(table.first, key, value = gen.make_value()).done)
            key = gen.make_key();
          debug(__LINE__, "mvcc %zu, dbi %u, insert %.*s-%.*s", (size_t)mvcc, table.first.dbi, key.length(), key.data(),
                value.length(), value.data());
          table.second.insert(mvcc, std::move(key), std::move(value));
        } else {
          cursor.to_key_lesser_or_equal(key, false);
          if (stat.ms_depth > wanna_deep || gen.coin()) {
            // shrink/delete
            const auto kv = cursor.current();
            debug(__LINE__, "mvcc %zu, dbi %u, delete %.*s-%.*s", (size_t)mvcc, table.first.dbi, kv.key.length(),
                  kv.key.data(), kv.value.length(), kv.value.data());
            table.second.erase(mvcc, kv.key);
            cursor.erase();
          } else {
            // update/change
            key = cursor.current().key;
            cursor.update(key, value = gen.make_value());
            debug(__LINE__, "mvcc %zu, dbi %u, update %.*s-%.*s", (size_t)mvcc, table.first.dbi, key.length(),
                  key.data(), value.length(), value.data());
            table.second.update(mvcc, std::move(key), std::move(value));
          }
        }
        stat = txn.get_map_stat(table.first);
      } while (changes++ < (stat.ms_entries + 3) / 4 || wanna_deep != stat.ms_depth);

      for (auto &entry : table.second.dataset)
        if (entry.second.empty() || entry.second.back().changed_or_dirtied < mvcc) {
          auto const found = cursor.find(entry.first, false);
          if (found && txn.is_dirty(found.key)) {
            debug(__LINE__, "mvcc %zu, dbi %u, dirtied %.*s-%.*s", (size_t)mvcc, table.first.dbi, found.key.length(),
                  found.key.data(), found.value.length(), found.value.data());
            table.second.dirtied_by_neighbour(mvcc, found.key);
          }
        }
    }
  }

  pool fetch(mdbx::txn txn, const mdbx::txnid mvcc, prng &rnd) {
    pool result;
    for (auto &pair : tables) {
      auto cursor = txn.open_cursor(pair.first);
      auto item = cursor.to_first(false);
      while (item.done) {
        debug(__LINE__, "dbi %zu, mvcc %zu, create-pool-entry: %.*s->%.*s", pair.first, size_t(mvcc), item.key.length(),
              item.key.data(), item.value.length(), item.value.data());
        result.emplace_back(&pair, item.key, mdbx::slice::invalid(), mdbx::cache_entry());
        item = cursor.to_next(false);
      }
    }
    std::shuffle(result.begin(), result.end(), rnd);

    for (auto &entry : result) {
      const auto check_value = txn.get(entry.ref->first, entry.key, mdbx::slice::invalid());
      const auto cache_result = get_cached(txn, entry.ref->first, &entry.key.slice(), entry.value, &entry.cache);
      if (check_value.is_valid()) {
        if (cache_result.errcode != MDBX_SUCCESS)
          unexpected(__LINE__);
        if (check_value != entry.value)
          unexpected(__LINE__);
        debug(__LINE__, "dbi %zu, mvcc %zu, fetch-cache-entry: %.*s->%.*s", entry.ref->first, size_t(mvcc),
              entry.key.length(), entry.key.data(), entry.value.length(), entry.value.data());
      } else {
        if (cache_result.errcode != MDBX_NOTFOUND)
          unexpected(__LINE__);
        debug(__LINE__, "dbi %zu, mvcc %zu, fetch-cache-entry: %.*s->notfound", entry.ref->first, size_t(mvcc),
              entry.key.length(), entry.key.data());
      }
      if (cache_result.status != MDBX_CACHE_REFRESHED &&
          !(cache_result.status == MDBX_CACHE_DIRTY && txn.is_readwrite()))
        unexpected(__LINE__);
    }
    std::shuffle(result.begin(), result.end(), rnd);

    return result;
  }

  bool verify(pool &pool, const mdbx::txnid from_mvcc, mdbx::txn to, const mdbx::txnid to_mvcc) {
    const bool is_write = to.is_readwrite();
    assert(from_mvcc < to_mvcc && to_mvcc <= to.id());
    for (auto &entry : pool) {
      if (from_mvcc > entry.cache.last_confirmed_txnid)
        return failed(__LINE__);
      if (entry.cache.trunk_txnid > entry.cache.last_confirmed_txnid)
        return failed(__LINE__);
      const track_point *from_point = nullptr, *to_point = nullptr;
      const auto &track = entry.ref->second.dataset[entry.key];
      for (const auto &point : track) {
        if (point.changed_or_dirtied <= from_mvcc)
          from_point = &point;
        if (point.changed_or_dirtied <= to_mvcc)
          to_point = &point;
      }

      if (!from_point || !to_point)
        unexpected(__LINE__);
      if (from_point->changed_or_dirtied > to_point->changed_or_dirtied)
        unexpected(__LINE__);

      if (from_point) {
        if (!entry.value.is_valid())
          return failed(__LINE__);
        if (from_point->value != entry.value)
          return failed(__LINE__);
      } else {
        if (entry.value.is_valid())
          return failed(__LINE__);
      }

      const auto check_value = to.get(entry.ref->first, entry.key, mdbx::slice::invalid());
      auto copy = entry.cache;
      const auto cache_result = get_cached(to, entry.ref->first, &entry.key.slice(), &entry.value, &entry.cache);
      if (check_value.is_valid()) {
        if (cache_result.errcode != MDBX_SUCCESS) {
          // debug
          get_cached(to, entry.ref->first, &entry.key.slice(), &entry.value, &copy);
          to.get(entry.ref->first, entry.key, mdbx::slice::invalid());
          return failed(__LINE__);
        }
        if (check_value != entry.value)
          return failed(__LINE__);
        if (to_point) {
          if (to_point->is_erased())
            return failed(__LINE__);
          if (to_point->value != entry.value)
            return failed(__LINE__);
        } else if (from_point) {
          if (from_point->is_erased())
            return failed(__LINE__);
          if (from_point->value != entry.value)
            return failed(__LINE__);
          to_point = from_point;
        } else
          unexpected(__LINE__);
      } else {
        if (cache_result.errcode != MDBX_NOTFOUND) {
          // debug
          get_cached(to, entry.ref->first, &entry.key.slice(), &entry.value, &copy);
          to.get(entry.ref->first, entry.key, mdbx::slice::invalid());
          return failed(__LINE__);
        }
        if (to_point && !to_point->is_erased())
          return failed(__LINE__);
      }

      switch (cache_result.status) {
      default:
        unexpected(__LINE__);
      case MDBX_CACHE_ERROR:
        unexpected(__LINE__);
      case MDBX_CACHE_BEHIND:
        unexpected(__LINE__);
      case MDBX_CACHE_RACE:
        unexpected(__LINE__);
      case MDBX_CACHE_DIRTY:
        if (!is_write)
          return failed(__LINE__);
        if (!to.is_dirty(check_value))
          return failed(__LINE__);
        if (!to_point || to_point->changed_or_dirtied != to_mvcc)
          return failed(__LINE__);
        if (entry.cache.trunk_txnid > entry.cache.last_confirmed_txnid)
          return failed(__LINE__);
        break;
      case MDBX_CACHE_HIT:
        if (from_point != to_point)
          return failed(__LINE__);
        if (to_point && to_point->changed_or_dirtied != entry.cache.trunk_txnid)
          return failed(__LINE__);
        if (to_mvcc > entry.cache.last_confirmed_txnid)
          return failed(__LINE__);
        if (to_mvcc < entry.cache.trunk_txnid)
          return failed(__LINE__);
        if (entry.cache.trunk_txnid > entry.cache.last_confirmed_txnid)
          return failed(__LINE__);
        break;
      case MDBX_CACHE_CONFIRMED:
        if (from_point != to_point)
          return failed(__LINE__);
        if (from_mvcc != entry.cache.trunk_txnid && from_point->changed_or_dirtied != entry.cache.trunk_txnid) {
          // debug
          get_cached(to, entry.ref->first, &entry.key.slice(), &entry.value, &copy);
          return failed(__LINE__);
        }
        if (to_mvcc != entry.cache.last_confirmed_txnid && to.id() != entry.cache.last_confirmed_txnid)
          return failed(__LINE__);
        if (to_mvcc < entry.cache.trunk_txnid)
          return failed(__LINE__);
        if (entry.cache.trunk_txnid > entry.cache.last_confirmed_txnid)
          return failed(__LINE__);
        break;
      case MDBX_CACHE_REFRESHED:
        if (from_point == to_point)
          return failed(__LINE__);
        if (to_point && to_point->changed_or_dirtied < entry.cache.trunk_txnid && !to_point->is_erased())
          return failed(__LINE__);
        if (from_point && from_point->changed_or_dirtied >= entry.cache.trunk_txnid)
          return failed(__LINE__);
        if (to_mvcc != entry.cache.last_confirmed_txnid && to.id() != entry.cache.last_confirmed_txnid)
          return failed(__LINE__);
        if (entry.cache.trunk_txnid > entry.cache.last_confirmed_txnid)
          return failed(__LINE__);
        break;
      }
    }
    return true;
  }
};

static uint_fast32_t deep_transition_mask() {
  uint_fast32_t mask = 0;
  for (size_t from_deep = 0; from_deep < 5; ++from_deep)
    for (size_t to_deep = 0; to_deep < 5; ++to_deep)
      if (from_deep != to_deep)
        mask |= uint_fast32_t(1) << (from_deep * 5 + to_deep);
  return mask;
}

bool case1_stairway_pass(track_context &ctx, mdbx::env env, prng &rnd, generator::keys_order order) {
  bool ok = true;
  ctx.rx.clear();

  /* путь обхода матрицы 5x5, соответствующей проверочным шагам/переходам
   * с измененем глубины b-tree 0 до 4 включительно */
  //                                 0 1 2 3 4
  // a:0->1 b:1->2 c:2->3 d:3->4   0 ! A I M O
  // e:4->3 f:3->2 g:2->1 h:1->0   1 H ! B Q S
  // i:0->2 j:2->4 k:4->2 l:2->0   2 L G ! C J
  // m:0->3 n:3->0 o:0->4 p:4->1   3 N R F ! D
  // q:1->3 r:3->1 s:1->4 t:4->0   4 T P K E !
  //                                0 1 2 3 4
  const std::array<uint8_t, 20> deep_transition_path = {/* A */ 0x01, /* B */ 0x12, /* C */ 0x23, /* D */ 0x34,
                                                        /* E */ 0x43, /* F */ 0x32, /* G */ 0x21, /* H */ 0x10,
                                                        /* I */ 0x02, /* J */ 0x24, /* K */ 0x42, /* L */ 0x20,
                                                        /* M */ 0x03, /* N */ 0x30, /* O */ 0x04, /* P */ 0x41,
                                                        /* Q */ 0x13, /* R */ 0x31, /* S */ 0x14, /* T */ 0x40};

  /* начальные точки при обходе матрицы */
  std::vector<size_t> starts;
  starts.reserve(ctx.tables.size());

  /* начиная со случайной позиции */
  size_t start = rnd() * 532384033u % deep_transition_path.size();
  for (size_t n = 0; n < ctx.tables.size(); ++n) {
    /* ищем точку с нулевой исходной глубиной b-tree */
    do
      start = (start + 1) % deep_transition_path.size();
    while (deep_transition_path[start] & 0xF0);
    starts.push_back(start);
  }

  /* контрольная битовая маска для матрицы 5x5 */
  uint_fast32_t check_mask = 0;

  /* загружаем начальные данные (пока должно быть пусто) */
  auto txn = env.start_read();
  auto mvcc = txn.id();
  ctx.rx.insert(std::make_pair(mvcc, std::move(txn)));
  auto pool = ctx.fetch(ctx.rx.rbegin()->second, mvcc, rnd);

  size_t max_pool = 0, number_checks = 0, number_passes = 0;
  generator gen(order, rnd);
  /* проходим путь обхода матрицы глубины b-tree по всем таблицам параллельно,
   * но начиная с разой стартовой позиции */
  for (size_t i = 0; i < deep_transition_path.size(); ++i) {
    std::shuffle(ctx.tables_vector.begin(), ctx.tables_vector.end(), rnd);
    txn = env.start_write();
    gen.seed(rnd);
    mvcc = txn.id();
    for (size_t n = 0; n < ctx.tables_vector.size(); ++n) {
      const size_t pos = starts[n]++ % deep_transition_path.size();
      const size_t from_deep = deep_transition_path[pos] >> 4;
      const size_t to_deep = deep_transition_path[pos] & 0x0F;
      if (n == 0) {
        const auto step = uint_fast32_t(1) << (from_deep * 5 + to_deep);
        if (check_mask & step)
          unexpected(__LINE__);
        check_mask += step;
      }
      /* обеспечиваем целевую высоту b-tree для выбранной таблице */
      ctx.turn(txn, gen, *ctx.tables_vector[n], to_deep);
    }

    ok = ctx.verify(pool, ctx.rx.rbegin()->first, txn, mvcc) && ok;
    txn.commit_embark_read();
    ctx.rx.insert(std::make_pair(mvcc, std::move(txn)));
    max_pool = std::max(max_pool, pool.size());
    number_checks += pool.size();
    number_passes += 1;
  }
  if (check_mask != deep_transition_mask() || check_mask != 076767676u)
    unexpected(__LINE__);

  /* --------------------------------------------------------------------------------
   * Теперь есть набор MVCC-снимком и читающих их транзакций, а также история изменений в контексте.
   *
   * Проверяем работы кэша стохастически выбирая транзакции в истории. */
  struct coverage_step {
    mdbx::txnid from_mvcc;
    mdbx::txnid to_mvcc;
    mdbx::txn from_txn;
    mdbx::txn to_txn;
    coverage_step(mdbx::txnid from_mvcc, mdbx::txnid to_mvcc, mdbx::txn from_txn, mdbx::txn to_txn)
        : from_mvcc(from_mvcc), to_mvcc(to_mvcc), from_txn(from_txn), to_txn(to_txn) {}
    coverage_step(const coverage_step &) = default;
    coverage_step(coverage_step &&) = default;
    coverage_step &operator=(const coverage_step &) = default;
    coverage_step &operator=(coverage_step &&) = default;
  };

  std::vector<coverage_step> coverage;
  for (const auto &from : ctx.rx)
    for (const auto &to : ctx.rx)
      if (to.first > from.first)
        coverage.emplace_back(from.first, to.first, from.second, to.second);
  std::shuffle(coverage.begin(), coverage.end(), rnd);
  for (const auto &step : coverage) {
    pool = ctx.fetch(step.from_txn, step.from_mvcc, rnd);
    ok = ctx.verify(pool, step.from_mvcc, step.to_txn, step.to_mvcc) && ok;
    max_pool = std::max(max_pool, pool.size());
    number_checks += pool.size();
    number_passes += 1;
  }

  txn = env.start_write();
  for (auto &pair : ctx.tables)
    txn.clear_map(pair.first);
  txn.commit();

  std::cout << "order " << order << ", passes " << number_passes << ", checks " << number_checks << ", max-pool "
            << max_pool << std::endl;
  return ok;
}

bool case1_stairway(mdbx::env env) {
  // get_cached_t get_cached = mdbx_cache_get_SingleThreaded;
  bool ok = true;
#if 1
  std::random_device random;
  std::seed_seq seed({random(), random(), random(), random(), random()});
#else
  std::seed_seq seed({42});
#endif

  std::cout << "seed ";
  seed.param(std::ostream_iterator<size_t>(std::cout, ", "));
  std::cout << std::endl;
  prng rnd(seed);

  auto txn = env.start_write();
  track_context ctx;
  ctx.create_tables("case1_", txn, 4);
  txn.commit();

  for (auto order = generator::keys_order::begin; order < generator::keys_order::end;
       order = generator::keys_order(order + 1))
    ok = case1_stairway_pass(ctx, env, rnd, order) && ok;
  return ok;
}

//--------------------------------------------------------------------------------------------

int doit() {
  mdbx::path db_filename = "test-get-cached";
  mdbx::env::remove(db_filename);

  mdbx::env::operate_options options;
  options.no_sticky_threads = true;
  mdbx::env_managed::create_parameters create_parameters;
  create_parameters.geometry.pagesize = mdbx::env::geometry::minimal_value;
  mdbx::env_managed env(db_filename, create_parameters,
                        mdbx::env::operate_parameters(42, 0, mdbx::env::nested_transactions,
                                                      mdbx::env::durability::robust_synchronous,
                                                      mdbx::env::reclaiming_options(), options));
  if (env.get_info().mi_dxb_pagesize != 256)
    unexpected(__LINE__);

  bool ok = case0_trivia(env);
  ok = case1_stairway(env) && ok;
  // ok = case2(env) && ok;

  if (ok) {
    std::cout << "OK\n";
    return EXIT_SUCCESS;
  } else {
    std::cout << "FAIL!\n";
    return EXIT_FAILURE;
  }
}

int main(int argc, char *argv[]) {
  (void)argc;
  (void)argv;
  mdbx_setup_debug_nofmt(MDBX_LOG_VERBOSE, MDBX_DBG_ASSERT | MDBX_DBG_LEGACY_MULTIOPEN | MDBX_DBG_LEGACY_OVERLAP,
                         logger_nofmt, log_buffer, sizeof(log_buffer));
  try {
    return doit();
  } catch (const std::exception &ex) {
    std::cerr << "Exception: " << ex.what() << "\n";
    return EXIT_FAILURE;
  }
}
