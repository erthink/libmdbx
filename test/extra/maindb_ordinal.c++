/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2024
/// \copyright SPDX-License-Identifier: Apache-2.0

#include "mdbx.h++"
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
                            mdbx::value_mode::single);
#if 0 /* workaround */
  txn.commit();
  env.close();
  env = mdbx::env_managed(".", mdbx::env_managed::create_parameters(),
                        mdbx::env::operate_parameters());
  txn = env.start_write();
#endif

  txn.insert(map, buffer::key_from_u64(UINT64_C(8) << 8 * 0), buffer("a"));
  txn.insert(map, buffer::key_from_u64(UINT64_C(7) << 8 * 1), buffer("b"));
  txn.insert(map, buffer::key_from_u64(UINT64_C(6) << 8 * 2), buffer("c"));
  txn.insert(map, buffer::key_from_u64(UINT64_C(5) << 8 * 3), buffer("d"));
  txn.insert(map, buffer::key_from_u64(UINT64_C(4) << 8 * 4), buffer("e"));
  txn.insert(map, buffer::key_from_u64(UINT64_C(3) << 8 * 5), buffer("f"));
  txn.insert(map, buffer::key_from_u64(UINT64_C(2) << 8 * 6), buffer("g"));
  txn.insert(map, buffer::key_from_u64(UINT64_C(1) << 8 * 7), buffer("h"));
  txn.commit();

  txn = env.start_read();
  auto cursor = txn.open_cursor(map);
#if defined(__cpp_lib_string_view) && __cpp_lib_string_view >= 201606L
  if (cursor.to_first().value.string_view() == "a" &&
      cursor.to_next().value.string_view() == "b" &&
      cursor.to_next().value.string_view() == "c" &&
      cursor.to_next().value.string_view() == "d" &&
      cursor.to_next().value.string_view() == "e" &&
      cursor.to_next().value.string_view() == "f" &&
      cursor.to_next().value.string_view() == "g" &&
      cursor.to_next().value.string_view() == "h" &&
      !cursor.to_next(false).done && cursor.eof()) {
    std::cout << "OK\n";
    return EXIT_SUCCESS;
  }
  std::cerr << "Fail\n";
  return EXIT_FAILURE;
#else
  std::cerr << "Skipped since no std::string_view\n";
  return EXIT_SUCCESS;
#endif /* __cpp_lib_string_view >= 201606L */
}
