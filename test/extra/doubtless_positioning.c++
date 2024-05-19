/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2024
/// \copyright SPDX-License-Identifier: Apache-2.0

#include "mdbx.h++"
#include <array>
#include <functional>
#include <iostream>
#include <random>
#include <unistd.h>

static ::std::ostream &operator<<(::std::ostream &out,
                                  const mdbx::cursor::move_operation op) {
  static const char *const str[] = {"FIRST",
                                    "FIRST_DUP",
                                    "GET_BOTH",
                                    "GET_BOTH_RANGE",
                                    "GET_CURRENT",
                                    "GET_MULTIPLE",
                                    "LAST",
                                    "LAST_DUP",
                                    "NEXT",
                                    "NEXT_DUP",
                                    "NEXT_MULTIPLE",
                                    "NEXT_NODUP",
                                    "PREV",
                                    "PREV_DUP",
                                    "PREV_NODUP",
                                    "SET",
                                    "SET_KEY",
                                    "SET_RANGE",
                                    "PREV_MULTIPLE",
                                    "SET_LOWERBOUND",
                                    "SET_UPPERBOUND",
                                    "TO_KEY_LESSER_THAN",
                                    "TO_KEY_LESSER_OR_EQUAL",
                                    "TO_KEY_EQUAL",
                                    "TO_KEY_GREATER_OR_EQUAL",
                                    "TO_KEY_GREATER_THAN",
                                    "TO_EXACT_KEY_VALUE_LESSER_THAN",
                                    "TO_EXACT_KEY_VALUE_LESSER_OR_EQUAL",
                                    "TO_EXACT_KEY_VALUE_EQUAL",
                                    "TO_EXACT_KEY_VALUE_GREATER_OR_EQUAL",
                                    "TO_EXACT_KEY_VALUE_GREATER_THAN",
                                    "TO_PAIR_LESSER_THAN",
                                    "TO_PAIR_LESSER_OR_EQUAL",
                                    "TO_PAIR_EQUAL",
                                    "TO_PAIR_GREATER_OR_EQUAL",
                                    "TO_PAIR_GREATER_THAN"};
  return out << str[op];
}

using buffer = mdbx::default_buffer;
using buffer_pair = mdbx::buffer_pair<buffer>;

std::default_random_engine prng(42);

static buffer random(const unsigned &value) {
  switch (prng() % 3) {
  default:
    return buffer::hex(value);
  case 1:
    return buffer::base64(value);
  case 2:
    return buffer::base58(value);
  }
}

static buffer random_key() { return random(prng() % 10007); }

static buffer random_value() { return random(prng() % 47); }

using predicate = std::function<bool(const mdbx::pair &, const mdbx::pair &)>;

static bool probe(mdbx::txn txn, mdbx::map_handle dbi,
                  mdbx::cursor::move_operation op, predicate cmp,
                  const buffer_pair &pair) {
  auto seeker = txn.open_cursor(dbi);
  auto scanner = seeker.clone();

  const bool scan_backward =
      op == mdbx::cursor::key_lesser_than ||
      op == mdbx::cursor::key_lesser_or_equal ||
      op == mdbx::cursor::multi_exactkey_value_lesser_than ||
      op == mdbx::cursor::multi_exactkey_value_lesser_or_equal ||
      op == mdbx::cursor::pair_lesser_than ||
      op == mdbx::cursor::pair_lesser_or_equal;

  const bool is_multi = mdbx::is_multi(txn.get_handle_info(dbi).value_mode());

  auto seek_result = seeker.move(op, pair.key, pair.value, false);
  auto scan_result = scanner.fullscan(
      [cmp, &pair](const mdbx::pair &scan) -> bool { return cmp(scan, pair); },
      scan_backward);
  if (seek_result.done == scan_result &&
      (!scan_result ||
       seeker.is_same_position(
           scanner,
           op < mdbx::cursor::multi_exactkey_value_lesser_than && is_multi)))
    return true;

  std::cerr << std::endl;
  std::cerr << "bug:";
  std::cerr << std::endl;
  std::cerr << std::string(is_multi ? "multi" : "single") << "-map, op " << op
            << ", key " << pair.key << ", value " << pair.value;
  std::cerr << std::endl;
  std::cerr << "\tscanner: ";
  if (scan_result)
    std::cerr << "     done, key " << scanner.current(false).key << ", value "
              << scanner.current(false).value;
  else
    std::cerr << "not-found";
  std::cerr << std::endl;
  std::cerr << "\t seeker: " << (seek_result.done ? "     done" : "not-found")
            << ", key " << seek_result.key << ", value " << seek_result.value;
  std::cerr << std::endl;
  return false;
}

static bool probe(mdbx::txn txn, mdbx::map_handle dbi,
                  mdbx::cursor::move_operation op, predicate cmp) {
  const auto pair = buffer_pair(random_key(), random_value());
  const bool ok = probe(txn, dbi, op, cmp, pair);
#if MDBX_DEBUG
  if (!ok)
    // повтор для отладки и поиска причин
    probe(txn, dbi, op, cmp, pair);
#endif /* MDBX_DEBUG */
  return ok;
}

static bool test(mdbx::txn txn, mdbx::map_handle dbi) {
  bool ok = true;

  ok = probe(txn, dbi, mdbx::cursor::key_lesser_than,
             [txn, dbi](const mdbx::pair &l, const mdbx::pair &r) -> bool {
               return mdbx_cmp(txn, dbi, l.key, r.key) < 0;
             }) &&
       ok;
  ok = probe(txn, dbi, mdbx::cursor::key_lesser_or_equal,
             [txn, dbi](const mdbx::pair &l, const mdbx::pair &r) -> bool {
               return mdbx_cmp(txn, dbi, l.key, r.key) <= 0;
             }) &&
       ok;
  ok = probe(txn, dbi, mdbx::cursor::key_equal,
             [txn, dbi](const mdbx::pair &l, const mdbx::pair &r) -> bool {
               return mdbx_cmp(txn, dbi, l.key, r.key) == 0;
             }) &&
       ok;
  ok = probe(txn, dbi, mdbx::cursor::key_greater_or_equal,
             [txn, dbi](const mdbx::pair &l, const mdbx::pair &r) -> bool {
               return mdbx_cmp(txn, dbi, l.key, r.key) >= 0;
             }) &&
       ok;
  ok = probe(txn, dbi, mdbx::cursor::key_greater_than,
             [txn, dbi](const mdbx::pair &l, const mdbx::pair &r) -> bool {
               return mdbx_cmp(txn, dbi, l.key, r.key) > 0;
             }) &&
       ok;

  ok = probe(txn, dbi, mdbx::cursor::multi_exactkey_value_lesser_than,
             [txn, dbi](const mdbx::pair &l, const mdbx::pair &r) -> bool {
               return mdbx_cmp(txn, dbi, l.key, r.key) == 0 &&
                      mdbx_dcmp(txn, dbi, l.value, r.value) < 0;
             }) &&
       ok;
  ok = probe(txn, dbi, mdbx::cursor::multi_exactkey_value_lesser_or_equal,
             [txn, dbi](const mdbx::pair &l, const mdbx::pair &r) -> bool {
               return mdbx_cmp(txn, dbi, l.key, r.key) == 0 &&
                      mdbx_dcmp(txn, dbi, l.value, r.value) <= 0;
             }) &&
       ok;
  ok = probe(txn, dbi, mdbx::cursor::multi_exactkey_value_equal,
             [txn, dbi](const mdbx::pair &l, const mdbx::pair &r) -> bool {
               return mdbx_cmp(txn, dbi, l.key, r.key) == 0 &&
                      mdbx_dcmp(txn, dbi, l.value, r.value) == 0;
             }) &&
       ok;
  ok = probe(txn, dbi, mdbx::cursor::multi_exactkey_value_greater_or_equal,
             [txn, dbi](const mdbx::pair &l, const mdbx::pair &r) -> bool {
               return mdbx_cmp(txn, dbi, l.key, r.key) == 0 &&
                      mdbx_dcmp(txn, dbi, l.value, r.value) >= 0;
             }) &&
       ok;
  ok = probe(txn, dbi, mdbx::cursor::multi_exactkey_value_greater,
             [txn, dbi](const mdbx::pair &l, const mdbx::pair &r) -> bool {
               return mdbx_cmp(txn, dbi, l.key, r.key) == 0 &&
                      mdbx_dcmp(txn, dbi, l.value, r.value) > 0;
             }) &&
       ok;

  ok = probe(txn, dbi, mdbx::cursor::pair_lesser_than,
             [txn, dbi](const mdbx::pair &l, const mdbx::pair &r) -> bool {
               auto cmp = mdbx_cmp(txn, dbi, l.key, r.key);
               if (cmp == 0)
                 cmp = mdbx_dcmp(txn, dbi, l.value, r.value);
               return cmp < 0;
             }) &&
       ok;
  ok = probe(txn, dbi, mdbx::cursor::pair_lesser_or_equal,
             [txn, dbi](const mdbx::pair &l, const mdbx::pair &r) -> bool {
               auto cmp = mdbx_cmp(txn, dbi, l.key, r.key);
               if (cmp == 0)
                 cmp = mdbx_dcmp(txn, dbi, l.value, r.value);
               return cmp <= 0;
             }) &&
       ok;
  ok = probe(txn, dbi, mdbx::cursor::pair_equal,
             [](const mdbx::pair &l, const mdbx::pair &r) -> bool {
               return l == r;
             }) &&
       ok;
  ok = probe(txn, dbi, mdbx::cursor::pair_greater_or_equal,
             [txn, dbi](const mdbx::pair &l, const mdbx::pair &r) -> bool {
               auto cmp = mdbx_cmp(txn, dbi, l.key, r.key);
               if (cmp == 0)
                 cmp = mdbx_dcmp(txn, dbi, l.value, r.value);
               return cmp >= 0;
             }) &&
       ok;
  ok = probe(txn, dbi, mdbx::cursor::pair_greater_than,
             [txn, dbi](const mdbx::pair &l, const mdbx::pair &r) -> bool {
               auto cmp = mdbx_cmp(txn, dbi, l.key, r.key);
               if (cmp == 0)
                 cmp = mdbx_dcmp(txn, dbi, l.value, r.value);
               return cmp > 0;
             }) &&
       ok;
  return ok;
}

int main(int argc, const char *argv[]) {
  (void)argc;
  (void)argv;

  unlink("." MDBX_DATANAME);
  unlink("." MDBX_LOCKNAME);
  mdbx::env_managed env(".", mdbx::env_managed::create_parameters(),
                        mdbx::env::operate_parameters(3));

  auto txn = env.start_write();
  auto single =
      txn.create_map("single", mdbx::key_mode::usual, mdbx::value_mode::single);
  auto multi =
      txn.create_map("multi", mdbx::key_mode::usual, mdbx::value_mode::multi);
  for (size_t i = 0; i < 1000; ++i) {
    auto key = random_key();
    txn.upsert(single, key, random_value());
    for (auto n = prng() % 5 + 1; n > 0; --n)
      txn.upsert(multi, key, random_value());
  }
  txn.commit_embark_read();

  bool ok = true;
  for (size_t i = 0; ok && i < 3333; ++i) {
    ok = test(txn, single) && ok;
    ok = test(txn, multi) && ok;
  }

  if (!ok) {
    std::cerr << "Fail\n";
    return EXIT_FAILURE;
  }
  std::cout << "OK\n";
  return EXIT_SUCCESS;
}
