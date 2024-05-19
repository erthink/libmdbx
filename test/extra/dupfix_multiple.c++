/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2024
/// \copyright SPDX-License-Identifier: Apache-2.0

#include "mdbx.h++"
#include <array>
#include <iostream>
#include <unistd.h>

int main(int argc, const char *argv[]) {
  (void)argc;
  (void)argv;

  unlink("." MDBX_DATANAME);
  unlink("." MDBX_LOCKNAME);

  mdbx::env_managed env(".", mdbx::env_managed::create_parameters(),
                        mdbx::env::operate_parameters());

  using buffer =
      mdbx::buffer<mdbx::default_allocator, mdbx::default_capacity_policy>;
  auto txn = env.start_write();
  auto map = txn.create_map(nullptr, mdbx::key_mode::ordinal,
                            mdbx::value_mode::multi_ordinal);

  txn.insert(map, buffer::key_from_u64(21), buffer::key_from_u64(18));
  txn.insert(map, buffer::key_from_u64(7), buffer::key_from_u64(19));
  txn.insert(map, buffer::key_from_u64(22), buffer::key_from_u64(17));
  txn.insert(map, buffer::key_from_u64(26), buffer::key_from_u64(13));
  txn.insert(map, buffer::key_from_u64(24), buffer::key_from_u64(15));
  txn.insert(map, buffer::key_from_u64(23), buffer::key_from_u64(16));
  txn.insert(map, buffer::key_from_u64(25), buffer::key_from_u64(14));
  txn.insert(map, buffer::key_from_u64(27), buffer::key_from_u64(12));
  txn.commit();

  txn = env.start_read();
  auto cursor = txn.open_cursor(map);
  if (cursor.to_first().value.as_uint64() != 19 ||
      cursor.to_next().value.as_uint64() != 18 ||
      cursor.to_next().value.as_uint64() != 17 ||
      cursor.to_next().value.as_uint64() != 16 ||
      cursor.to_next().value.as_uint64() != 15 ||
      cursor.to_next().value.as_uint64() != 14 ||
      cursor.to_next().value.as_uint64() != 13 ||
      cursor.to_next().value.as_uint64() != 12 || cursor.to_next(false).done ||
      !cursor.eof()) {
    std::cerr << "Fail\n";
    return EXIT_FAILURE;
  }
  txn.abort();

  const uint64_t array[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 42, 17, 99, 0, 33, 333};
  txn = env.start_write();
  txn.put_multiple(map, buffer::key_from_u64(13), array + 3, 4, mdbx::upsert);
  txn.put_multiple(map, buffer::key_from_u64(10), array + 0, 1, mdbx::upsert);
  txn.put_multiple(map, buffer::key_from_u64(12), array + 2, 3, mdbx::upsert);
  txn.put_multiple(map, buffer::key_from_u64(15), array + 5, 6, mdbx::upsert);
  txn.put_multiple(map, buffer::key_from_u64(14), array + 4, 5, mdbx::upsert);
  txn.put_multiple(map, buffer::key_from_u64(11), array + 1, 2, mdbx::upsert);
  txn.put_multiple(map, buffer::key_from_u64(16), array + 6, 7, mdbx::upsert);
  txn.commit();

  txn = env.start_read();
  cursor = txn.open_cursor(map);
  if (/* key = 7 */ cursor.to_first().value.as_uint64() != 19 ||

      /* key = 10: 1 элемент */
      cursor.to_next().value.as_uint64() != 1 ||

      /* key = 11: 2 элемента, пропуск 1 */
      cursor.to_next().value.as_uint64() != 2 ||
      cursor.to_next().value.as_uint64() != 3 ||

      /* key = 12: 3 элемента, пропуск 2 */
      cursor.to_next().value.as_uint64() != 3 ||
      cursor.to_next().value.as_uint64() != 4 ||
      cursor.to_next().value.as_uint64() != 5 ||

      /* key = 13: 4 элемента, пропуск 3 */
      cursor.to_next().value.as_uint64() != 4 ||
      cursor.to_next().value.as_uint64() != 5 ||
      cursor.to_next().value.as_uint64() != 6 ||
      cursor.to_next().value.as_uint64() != 7 ||

      /* key = 14: 5 элементов, пропуск 4 */
      cursor.to_next().value.as_uint64() != 5 ||
      cursor.to_next().value.as_uint64() != 6 ||
      cursor.to_next().value.as_uint64() != 7 ||
      cursor.to_next().value.as_uint64() != 8 ||
      cursor.to_next().value.as_uint64() != 9 ||

      /* key = 15: 6 элементов, пропуск 5 */
      cursor.to_next().value.as_uint64() != 6 ||
      cursor.to_next().value.as_uint64() != 7 ||
      cursor.to_next().value.as_uint64() != 8 ||
      cursor.to_next().value.as_uint64() != 9 ||
      cursor.to_next().value.as_uint64() != 17 ||
      cursor.to_next().value.as_uint64() != 42 ||

      /* key = 16: 7 элементов, пропуск 6 */
      cursor.to_next().value.as_uint64() != 0 ||
      cursor.to_next().value.as_uint64() != 7 ||
      cursor.to_next().value.as_uint64() != 8 ||
      cursor.to_next().value.as_uint64() != 9 ||
      cursor.to_next().value.as_uint64() != 17 ||
      cursor.to_next().value.as_uint64() != 42 ||
      cursor.to_next().value.as_uint64() != 99 ||

      /* key = 21 */ cursor.to_next().value.as_uint64() != 18 ||
      /* key = 22 */ cursor.to_next().value.as_uint64() != 17 ||
      /* key = 23 */ cursor.to_next().value.as_uint64() != 16 ||
      /* key = 24 */ cursor.to_next().value.as_uint64() != 15 ||
      /* key = 25 */ cursor.to_next().value.as_uint64() != 14 ||
      /* key = 26 */ cursor.to_next().value.as_uint64() != 13 ||
      /* key = 27 */ cursor.to_next().value.as_uint64() != 12 ||
      cursor.to_next(false).done || !cursor.eof()) {
    std::cerr << "Fail\n";
    return EXIT_FAILURE;
  }
  txn.abort();

  txn = env.start_write();
  txn.put_multiple(map, buffer::key_from_u64(7), array + 3, 4, mdbx::update);
  txn.upsert(map, buffer::key_from_u64(10), buffer::key_from_u64(14));
  txn.put_multiple(map, buffer::key_from_u64(11), array + 4, 5, mdbx::upsert);
  txn.put_multiple(map, buffer::key_from_u64(12), array + 0, 1, mdbx::update);
  txn.update(map, buffer::key_from_u64(13), buffer::key_from_u64(18));
  txn.put_multiple(map, buffer::key_from_u64(14), array + 2, 3, mdbx::update);
  txn.update(map, buffer::key_from_u64(15), buffer::key_from_u64(13));
  txn.put_multiple(map, buffer::key_from_u64(16), array + 6, 9, mdbx::update);
  txn.update(map, buffer::key_from_u64(21), buffer::key_from_u64(17));
  txn.update(map, buffer::key_from_u64(22), buffer::key_from_u64(15));
  txn.put_multiple(map, buffer::key_from_u64(23), array + 1, 2, mdbx::update);
  txn.update(map, buffer::key_from_u64(24), buffer::key_from_u64(16));
  txn.put_multiple(map, buffer::key_from_u64(25), array + 5, 6, mdbx::update);
  txn.upsert(map, buffer::key_from_u64(26), buffer::key_from_u64(12));
  txn.put_multiple(map, buffer::key_from_u64(27), array + 12, 3, mdbx::update);
  txn.commit();

  txn = env.start_read();
  cursor = txn.open_cursor(map);
  if (/* key = 7 */
      cursor.to_first().value.as_uint64() != 4 ||
      cursor.to_next().value.as_uint64() != 5 ||
      cursor.to_next().value.as_uint64() != 6 ||
      cursor.to_next().value.as_uint64() != 7 ||

      /* key = 10: 1 элемент */
      cursor.to_next().value.as_uint64() != 1 ||
      /* +1 upsert */
      cursor.to_next().value.as_uint64() != 14 ||

      /* key = 11: 2 элемента, пропуск 1 */
      cursor.to_next().value.as_uint64() != 2 ||
      cursor.to_next().value.as_uint64() != 3 ||
      /* +5 элементов, пропуск 4 */
      cursor.to_next().value.as_uint64() != 5 ||
      cursor.to_next().value.as_uint64() != 6 ||
      cursor.to_next().value.as_uint64() != 7 ||
      cursor.to_next().value.as_uint64() != 8 ||
      cursor.to_next().value.as_uint64() != 9 ||

      /* key = 12: 1 элемент */
      cursor.to_next().value.as_uint64() != 1 ||
      /* key = 13 */ cursor.to_next().value.as_uint64() != 18 ||

      /* key = 14: 3 элемента, пропуск 2 */
      cursor.to_next().value.as_uint64() != 3 ||
      cursor.to_next().value.as_uint64() != 4 ||
      cursor.to_next().value.as_uint64() != 5 ||

      /* key = 15 */ cursor.to_next().value.as_uint64() != 13 ||

      /* key = 16: 9 элементов, пропуск 6 */
      cursor.to_next().value.as_uint64() != 0 ||
      cursor.to_next().value.as_uint64() != 7 ||
      cursor.to_next().value.as_uint64() != 8 ||
      cursor.to_next().value.as_uint64() != 9 ||
      cursor.to_next().value.as_uint64() != 17 ||
      cursor.to_next().value.as_uint64() != 33 ||
      cursor.to_next().value.as_uint64() != 42 ||
      cursor.to_next().value.as_uint64() != 99 ||
      cursor.to_next().value.as_uint64() != 333 ||

      /* key = 21 */ cursor.to_next().value.as_uint64() != 17 ||
      /* key = 22 */ cursor.to_next().value.as_uint64() != 15 ||
      /* key = 23: 2 элемента, пропуск 1 */
      cursor.to_next().value.as_uint64() != 2 ||
      cursor.to_next().value.as_uint64() != 3 ||
      /* key = 24 */ cursor.to_next().value.as_uint64() != 16 ||
      /* key = 25: 6 элемента, пропуск 5 */
      cursor.to_next().value.as_uint64() != 6 ||
      cursor.to_next().value.as_uint64() != 7 ||
      cursor.to_next().value.as_uint64() != 8 ||
      cursor.to_next().value.as_uint64() != 9 ||
      cursor.to_next().value.as_uint64() != 17 ||
      cursor.to_next().value.as_uint64() != 42 ||

      /* key = 26, 1+1 upsert */
      cursor.to_next().value.as_uint64() != 12 ||
      cursor.to_next().value.as_uint64() != 13 ||

      /* key = 27: 3 элемента, пропуск 12 */
      cursor.to_next().value.as_uint64() != 0 ||
      cursor.to_next().value.as_uint64() != 33 ||
      cursor.to_next().value.as_uint64() != 333 ||

      cursor.to_next(false).done || !cursor.eof()) {
    std::cerr << "Fail\n";
    return EXIT_FAILURE;
  }
  std::cout << "OK\n";
  return EXIT_SUCCESS;
}
