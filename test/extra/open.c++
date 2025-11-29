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

mdbx::path db_pathname = "test-open";

static char log_buffer[1024];

static void logger_nofmt(MDBX_log_level_t loglevel, const char *function, int line, const char *msg,
                         unsigned length) noexcept {
  (void)length;
  (void)loglevel;
  std::cout.flush();
  fprintf(stdout, "%s:%u %s", function, line, msg);
  fflush(stdout);
}

bool case1() {
  {
    mdbx::env::operate_parameters operateParameters2(100, 10);
    mdbx::env_managed::create_parameters createParameters2;
    createParameters2.geometry.make_fixed(42 * mdbx::env::geometry::MiB);
    mdbx::env_managed env2(db_pathname, createParameters2, operateParameters2);
    mdbx::txn_managed txn2 = env2.start_write(false);
    /* mdbx::map_handle testHandle2 = */ txn2.create_map("fap1", mdbx::key_mode::reverse, mdbx::value_mode::single);
    txn2.commit();
  }

  {
    mdbx::env::operate_parameters operateParameters(100, 10);
    mdbx::env_managed::create_parameters createParameters;
    createParameters.geometry.make_dynamic(21 * mdbx::env::geometry::MiB, mdbx::env::geometry::GiB / 2);
    mdbx::env_managed env(db_pathname, createParameters, operateParameters);
  }

  mdbx::env::operate_parameters operateParameters(100, 10);
  mdbx::env_managed::create_parameters createParameters;
  createParameters.geometry.make_dynamic(21 * mdbx::env::geometry::MiB, 84 * mdbx::env::geometry::MiB);
  mdbx::env_managed env(db_pathname, createParameters, operateParameters);
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

  return true;
}

bool case2() {
  mdbx_setup_debug_nofmt(MDBX_LOG_VERBOSE, MDBX_DBG_ASSERT | MDBX_DBG_LEGACY_MULTIOPEN | MDBX_DBG_LEGACY_OVERLAP,
                         logger_nofmt, log_buffer, sizeof(log_buffer));
  mdbx::env::operate_options options;
  options.no_sticky_threads = true;
  mdbx::env_managed::create_parameters create_parameters;
  create_parameters.geometry.pagesize = mdbx::env::geometry::minimal_value;
  mdbx::env_managed env(db_pathname, create_parameters,
                        mdbx::env::operate_parameters(42, 0, mdbx::env::/* write_mapped_io */ nested_transactions,
                                                      mdbx::env::durability::robust_synchronous,
                                                      mdbx::env::reclaiming_options(), options));
  auto txn = env.start_write();
  txn.create_map("case2", mdbx::key_mode::usual, mdbx::value_mode::single);
  txn.commit();
  {
    auto params = mdbx::env::operate_parameters(4);
    params.options.no_sticky_threads = true;
    mdbx::env_managed env2(env.get_path(), params);
    auto txn2 = env2.start_write();
    txn2.drop_map("case2");
    txn2.commit();
  }

  txn = env.start_write();
  txn.create_map("case2", mdbx::key_mode::usual, mdbx::value_mode::single);
  txn.commit_embark_read();
  {
    auto params = mdbx::env::operate_parameters(1);
    params.options.no_sticky_threads = true;
    mdbx::env_managed env2(env.get_path(), params);
    auto txn2 = env2.start_write();
    txn2.drop_map("case2");
    txn2.commit();
  }

  std::latch starter_1(1);
  std::thread t1([&]() {
    starter_1.wait();
    auto params = mdbx::env::operate_parameters(1);
    params.options.no_sticky_threads = true;
    mdbx::env_managed env2(env.get_path(), params);
    auto txn2 = env2.start_write();
    txn2.drop_map("case2");
    txn2.commit();
  });
  txn = env.start_write();
  txn.create_map("case2", mdbx::key_mode::usual, mdbx::value_mode::single);
  txn.commit_embark_read();
  starter_1.count_down();

  std::latch starter_2(1);
  txn = env.start_write();
  txn.create_map("case2", mdbx::key_mode::usual, mdbx::value_mode::single);
  std::thread t2([&]() {
    starter_2.wait();
    // mdbx::env_managed env(path, createParameters, operateParameters);
    mdbx::txn_managed txn = env.start_write(false);
    /* mdbx::map_handle testHandle = */ txn.create_map("fap1", mdbx::key_mode::usual, mdbx::value_mode::single);
    txn.commit();
  });
  starter_2.count_down();
  txn.commit_embark_read();

  t1.join();
  t2.join();

  return true;
}

int doit() {
  mdbx::env::remove(db_pathname);
  bool ok = true;
  ok = case1() && ok;
  ok = case2() && ok;
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
  try {
    mdbx_setup_debug_nofmt(MDBX_LOG_VERBOSE, MDBX_DBG_ASSERT | MDBX_DBG_LEGACY_MULTIOPEN, logger_nofmt, log_buffer,
                           sizeof(log_buffer));
    return doit();
  } catch (const std::exception &ex) {
    std::cerr << "Exception: " << ex.what() << "\n";
    return EXIT_FAILURE;
  }
}

#endif /* __cpp_lib_latch */
