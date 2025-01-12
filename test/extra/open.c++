#include "mdbx.h++"

#include <iostream>

#if !defined(__cpp_lib_latch) && __cpp_lib_latch < 201907L

int main(int argc, const char *argv[]) {
  (void)argc;
  (void)argv;
  std::cout << "FAKE-OK (since no C++20 std::thread and/or std::latch)\n";
  return EXIT_SUCCESS;
}

#else

#include <latch>
#include <thread>

static char log_buffer[1024];

static void logger_nofmt(MDBX_log_level_t loglevel, const char *function, int line, const char *msg,
                         unsigned length) noexcept {
  (void)length;
  (void)loglevel;
  fprintf(stdout, "%s:%u %s", function, line, msg);
}

int main(int argc, const char *argv[]) {
  (void)argc;
  (void)argv;

  mdbx_setup_debug_nofmt(MDBX_LOG_VERBOSE, MDBX_DBG_ASSERT, logger_nofmt, log_buffer, sizeof(log_buffer));

  mdbx::path path = "test-open";
  mdbx::env::remove(path);

  {
    mdbx::env::operate_parameters operateParameters2(100, 10);
    mdbx::env_managed::create_parameters createParameters2;
    createParameters2.geometry.make_fixed(42 * mdbx::env::geometry::MiB);
    mdbx::env_managed env2(path, createParameters2, operateParameters2);
    mdbx::txn_managed txn2 = env2.start_write(false);
    /* mdbx::map_handle testHandle2 = */ txn2.create_map("fap1", mdbx::key_mode::reverse, mdbx::value_mode::single);
    txn2.commit();
  }

  mdbx::env::operate_parameters operateParameters(100, 10);
  mdbx::env_managed::create_parameters createParameters;
  createParameters.geometry.make_dynamic(21 * mdbx::env::geometry::MiB, 84 * mdbx::env::geometry::MiB);
  mdbx::env_managed env(path, createParameters, operateParameters);
  mdbx::txn_managed txn = env.start_write(false);
  /* mdbx::map_handle testHandle = */ txn.create_map("fap1", mdbx::key_mode::usual, mdbx::value_mode::single);
  txn.commit();

  std::latch starter(1);

  std::thread t1([&]() {
    starter.wait();
    // mdbx::env_managed env(path, createParameters, operateParameters);
    mdbx::txn_managed txn = env.start_write(false);
    /* mdbx::map_handle testHandle = */ txn.create_map("fap1", mdbx::key_mode::usual, mdbx::value_mode::single);
    txn.commit();
  });

  std::thread t2([&]() {
    starter.wait();
    // mdbx::env_managed env(path, createParameters, operateParameters);
    mdbx::txn_managed txn = env.start_write(false);
    /* mdbx::map_handle testHandle = */ txn.create_map("fap1", mdbx::key_mode::usual, mdbx::value_mode::single);
    txn.commit();
  });

  starter.count_down();

  t1.join();
  t2.join();

  std::cout << "OK\n";
  return EXIT_SUCCESS;
}

#endif /* __cpp_lib_latch */
