#include "mdbx.h++"

#include <array>
#include <iostream>
#include <random>
#include <vector>

#if MDBX_DEBUG || !defined(NDEBUG) || defined(__APPLE__) || defined(_WIN32)
#define NN 1024
#elif UINTPTR_MAX > 0xffffFFFFul || ULONG_MAX > 0xffffFFFFul
#define NN 4096
#else
#define NN 2048
#endif

std::string format_va(const char *fmt, va_list ap) {
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

std::string format(const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  std::string result = format_va(fmt, ap);
  va_end(ap);
  return result;
}

struct acase {
  unsigned klen_min, klen_max;
  unsigned vlen_min, vlen_max;
  unsigned dupmax_log2;

  acase(unsigned klen_min, unsigned klen_max, unsigned vlen_min, unsigned vlen_max, unsigned dupmax_log2)
      : klen_min(klen_min), klen_max(klen_max), vlen_min(vlen_min), vlen_max(vlen_max), dupmax_log2(dupmax_log2) {}
};

// std::random_device rd;
std::mt19937_64 rnd;

static unsigned prng_fast(uint32_t &seed) {
  seed = seed * 1103515245 + 12345;
  return seed >> 17;
}

static mdbx::slice mk(mdbx::default_buffer &buf, unsigned min, unsigned max) {
  uint32_t seed = rnd() % (NN + NN);
  unsigned len = (min < max) ? min + prng_fast(seed) % (max - min) : min;
  buf.clear_and_reserve(len);
  for (unsigned i = 0; i < len; ++i)
    buf.append_byte(mdbx::byte(prng_fast(seed)));
  return buf.slice();
}

static mdbx::slice mk_key(mdbx::default_buffer &buf, const acase &thecase) {
  return mk(buf, thecase.klen_min, thecase.klen_max);
}

static mdbx::slice mk_val(mdbx::default_buffer &buf, const acase &thecase) {
  return mk(buf, thecase.vlen_min, thecase.vlen_max);
}

static std::string name(unsigned n) { return format("Commitment_%05u", n); }

static mdbx::map_handle create_and_fill(mdbx::txn txn, const acase &thecase, const unsigned n) {
  auto map = txn.create_map(name(n),
                            (thecase.klen_min == thecase.klen_max && (thecase.klen_min == 4 || thecase.klen_max == 8))
                                ? mdbx::key_mode::ordinal
                                : mdbx::key_mode::usual,
                            (thecase.vlen_min == thecase.vlen_max) ? mdbx::value_mode::multi_samelength
                                                                   : mdbx::value_mode::multi);

  if (txn.get_map_stat(map).ms_entries < NN) {
    mdbx::default_buffer k, v;
    for (auto i = 0u; i < NN; i++) {
      mk_key(k, thecase);
      for (auto ii = thecase.dupmax_log2 ? 1u + (rnd() & ((2u << thecase.dupmax_log2) - 1u)) : 1u; ii > 0; --ii)
        txn.upsert(map, k, mk_val(v, thecase));
    }
  }
  return map;
}

static void chunched_delete(mdbx::txn txn, const acase &thecase, const unsigned n) {
  // printf(">> %s, case #%i\n", __FUNCTION__, n);
  mdbx::default_buffer k, v;
  auto map = txn.open_map_accede(name(n));

  {
    auto cursor = txn.open_cursor(map);
    while (true) {
      const auto all = cursor.txn().get_map_stat(cursor.map()).ms_entries;
      // printf("== seek random of %u\n", all);

      const char *last_op;
      bool last_r;

      if (true == ((last_op = "MDBX_GET_BOTH"),
                   (last_r = cursor.find_multivalue(mk_key(k, thecase), mk_val(v, thecase), false))) ||
          rnd() % 3 == 0 ||
          true == ((last_op = "MDBX_SET_RANGE"), (last_r = cursor.lower_bound(mk_key(k, thecase), false)))) {
        int i = int(rnd() % 7) - 3;
        // if (i)
        //   printf(" %s -> %s\n", last_op, last_r ? "true" : "false");
        // printf("== shift multi %i\n", i);
        try {
          while (i < 0 && true == ((last_op = "MDBX_PREV_DUP"), (last_r = cursor.to_current_prev_multi(false))))
            ++i;
          while (i > 0 && true == ((last_op = "MDBX_NEXT_DUP"), (last_r = cursor.to_current_next_multi(false))))
            --i;
        } catch (const mdbx::no_data &) {
          printf("cursor_del() -> exception, last %s %s\n", last_op, last_r ? "true" : "false");
          continue;
        }
      }
      // printf(" %s -> %s\n", last_op, last_r ? "true" : "false");

      if (all < 42) {
        // printf("== erase-tail\n");
        break;
      }
      auto i = all % 17 + 1;
      try {
        last_r = cursor.erase();
        do {
          // printf("== erase-chunk: %u\n", i);
          // printf(" cursor_del() -> %s\n", last_r ? "true" : "false");
        } while (cursor.to_next(false) && --i > 0);
      } catch (const mdbx::no_data &) {
        printf("cursor_del() -> exception, last %s %s\n", last_op, last_r ? "true" : "false");
      }

      // (void) last_op;
      // (void) last_r;
    }

    if (cursor.to_first(false))
      do
        cursor.erase();
      while (cursor.to_next(false));
  }

  // printf("<< %s, case #%i\n", __FUNCTION__, n);
}

static char log_buffer[1024];

static void logger_nofmt(MDBX_log_level_t loglevel, const char *function, int line, const char *msg,
                         unsigned length) noexcept {
  (void)length;
  (void)loglevel;
  fprintf(stdout, "%s:%u %s", function, line, msg);
}

bool outofrange_prev(mdbx::env env) {
  mdbx::cursor_managed cursor;
  const std::array<mdbx::pair, 4> items = {{{"k1", "v1"}, {"k1", "v2"}, {"k2", "v1"}, {"k2", "v2"}}};

  auto txn = env.start_write();
  auto multi = txn.create_map("multi", mdbx::key_mode::usual, mdbx::value_mode::multi);
  auto simple = txn.create_map("simple");
  txn.clear_map(multi);
  txn.clear_map(simple);

  txn.insert(simple, items[0]);
  txn.insert(simple, items[3]);
  cursor.bind(txn, simple);
  const auto simple_oor = cursor.lower_bound("k3");
  if (simple_oor) {
    std::cerr << "simple-outofrange " << simple_oor << "\n";
    return false;
  }
  const auto simple_oor_prevdup = cursor.to_current_prev_multi(false);
  if (simple_oor_prevdup) {
    std::cerr << "simple-outofrange-prevdup " << simple_oor_prevdup << "\n";
    return false;
  }
  const auto simple_oor_prev = cursor.to_previous(false);
  if (!simple_oor_prev || simple_oor_prev != items[3]) {
    std::cerr << "simple-outofrange-prev " << simple_oor_prev << "\n";
    return false;
  }

  txn.append(multi, items[0]);
  txn.append(multi, items[1]);
  txn.append(multi, items[2]);
  txn.append(multi, items[3]);
  cursor.bind(txn, multi);
  const auto multi_oor = cursor.lower_bound("k3");
  if (multi_oor) {
    std::cerr << "multi-outofrange " << multi_oor << "\n";
    return false;
  }
  const auto multi_oor_prevdup = cursor.to_current_prev_multi(false);
  if (multi_oor_prevdup) {
    std::cerr << "multi-outofrange-prevdup " << multi_oor_prevdup << "\n";
    return false;
  }
  const auto multi_oor_prev = cursor.to_previous(false);
  if (!multi_oor_prev || multi_oor_prev != items[3]) {
    std::cerr << "multi-outofrange-prev " << multi_oor_prev << "\n";
    return false;
  }

  txn.commit();
  return true;
}

bool next_prev_current(mdbx::env env) {
  const std::array<mdbx::pair, 4> items = {{{"k1", "v1"}, {"k1", "v2"}, {"k2", "v1"}, {"k2", "v2"}}};

  auto txn = env.start_write();
  auto map = txn.create_map("multi", mdbx::key_mode::usual, mdbx::value_mode::multi);
  txn.clear_map(map);
  for (const auto &i : items)
    txn.upsert(map, i);

  auto cursor = txn.open_cursor(map);
  const auto first = cursor.to_first(false);
  if (!first || first != items[0]) {
    std::cerr << "bad-first " << first << "\n";
    return false;
  }
  const auto next1 = cursor.to_next(false);
  if (!next1 || next1 != items[1]) {
    std::cerr << "bad-next-1 " << next1 << "\n";
    return false;
  }
  const auto next2 = cursor.to_next(false);
  if (!next2 || next2 != items[2]) {
    std::cerr << "bad-next-2 " << next2 << "\n";
    return false;
  }
  const auto prev1 = cursor.to_previous(false);
  if (!prev1 || prev1 != items[1]) {
    std::cerr << "bad-prev-1 " << prev1 << "\n";
    return false;
  }
  const auto prev2 = cursor.to_previous(false);
  if (!prev2 || prev2 != items[0]) {
    std::cerr << "bad-prev-2 " << prev2 << "\n";
    return false;
  }

  if (!cursor.erase(false)) {
    std::cerr << "bad-erase\n";
    return false;
  }

  const auto after_del = cursor.current(false);
  if (!after_del || after_del != items[1]) {
    std::cerr << "bad-after-del, current " << after_del << "\n";
    return false;
  }
  const auto next_after_del1 = cursor.to_next(false);
  if (!next_after_del1 || next_after_del1 != items[2]) {
    std::cerr << "bad-next_after_del1 " << next_after_del1;
    return false;
  }
  const auto next_after_del2 = cursor.to_next(false);
  if (!next_after_del2 || next_after_del2 != items[3]) {
    std::cerr << "bad-next_after_del2 " << next_after_del2;
    return false;
  }
  const auto next_after_del3 = cursor.to_next(false);
  if (next_after_del3) {
    std::cerr << "bad-next_after_del3 " << next_after_del3;
    return false;
  }
  txn.commit();
  return true;
}

bool simple(mdbx::env env) {
  const std::array<mdbx::pair, 3> items = {{{"k0", "v0"}, {"k1", "v1"}, {"k2", "v2"}}};

  auto txn = env.start_write();
  auto map = txn.create_map("simple");
  txn.clear_map(map);
  for (const auto &i : items)
    txn.insert(map, i);

  auto cursor = txn.open_cursor(map);
  cursor.seek(items[1].key);

  const auto seek = cursor.current(false);
  if (seek != items[1]) {
    std::cerr << "bad-seek, current " << seek << "\n";
    return false;
  }
  if (!cursor.erase()) {
    std::cerr << "bad-erase\n";
    return false;
  }

  const auto next = cursor.to_next(false);
  if (!next || next != items[2]) {
    std::cerr << "bad-next " << next;
    return false;
  }

  const auto after_del = cursor.current(false);
  if (!after_del || after_del != items[2]) {
    std::cerr << "bad-after-del, current " << after_del << "\n";
    return false;
  }
  txn.commit();

  txn = env.start_read();
  cursor.bind(txn, map);

#define BAD_CODE 1
#if BAD_CODE
  const auto first = cursor.to_next(false);
#else
  const auto first = cursor.to_first(false);
#endif
  const auto second = cursor.to_next(false);
  const auto eof = cursor.to_next(false);

  if (!first || first != items[0]) {
    std::cerr << "bad-first " << first << "\n";
    return false;
  }
  if (!second || second != items[2]) {
    std::cerr << "bad-second " << second << "\n";
    return false;
  }
  if (eof) {
    std::cerr << "bad-eof " << eof << "\n";
    return false;
  }

  return true;
}

int doit() {
  mdbx::path db_filename = "test-crunched-del";
  mdbx::env::remove(db_filename);

  mdbx::env_managed env(db_filename, mdbx::env_managed::create_parameters(), mdbx::env::operate_parameters(42));
  if (!simple(env) || !next_prev_current(env) || !outofrange_prev(env))
    return EXIT_FAILURE;

  std::vector<acase> testset;
  // Там ключи разной длины - от 1 до 64 байт.
  // Значения разной длины от 100 до 1000 байт.
  testset.emplace_back(/* keylen_min */ 1, /* keylen_max */ 64,
                       /* datalen_min */ 100, /* datalen_max */
                       mdbx_env_get_valsize4page_max(env, MDBX_db_flags_t(mdbx::value_mode::multi)),
                       /* dups_log2 */ 6);
  // В одной таблице DupSort: path -> version_u64+data
  // path - это префикс в дереве. Самые частые длины: 1-5 байт и 32-36 байт.
  testset.emplace_back(1, 5, 100, 1000, 8);
  testset.emplace_back(32, 36, 100, 1000, 7);
  // В другой DupSort: timestamp_u64 -> path
  testset.emplace_back(8, 8, 1, 5, 10);
  testset.emplace_back(8, 8, 32, 36, 9);

  auto txn = env.start_write();
  for (unsigned i = 0; i < testset.size(); ++i)
    create_and_fill(txn, testset[i], i);
  txn.commit();

  // mdbx_setup_debug_nofmt(MDBX_LOG_TRACE, MDBX_DBG_AUDIT | MDBX_DBG_ASSERT,
  //                       logger_nofmt, log_buffer, sizeof(log_buffer));
  txn = env.start_write();
  for (unsigned i = 0; i < testset.size(); ++i)
    chunched_delete(txn, testset[i], i);
  txn.commit();

  std::cout << "OK\n";
  return EXIT_SUCCESS;
}

int main(int argc, char *argv[]) {
  (void)argc;
  (void)argv;
  mdbx_setup_debug_nofmt(MDBX_LOG_NOTICE, MDBX_DBG_ASSERT, logger_nofmt, log_buffer, sizeof(log_buffer));
  try {
    return doit();
  } catch (const std::exception &ex) {
    std::cerr << "Exception: " << ex.what() << "\n";
    return EXIT_FAILURE;
  }
}
