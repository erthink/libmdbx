#include "mdbx.h++"
#include MDBX_CONFIG_H

#include <iostream>

#if defined(ENABLE_MEMCHECK) || defined(MDBX_CI)
#if MDBX_DEBUG || !defined(NDEBUG)
#define RELIEF_FACTOR 16
#else
#define RELIEF_FACTOR 8
#endif
#elif MDBX_DEBUG || !defined(NDEBUG) || defined(__APPLE__) || defined(_WIN32)
#define RELIEF_FACTOR 4
#elif UINTPTR_MAX > 0xffffFFFFul || ULONG_MAX > 0xffffFFFFul
#define RELIEF_FACTOR 2
#else
#define RELIEF_FACTOR 1
#endif

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

bool case0(const mdbx::path &path) {
  mdbx::env_managed::create_parameters createParameters;
  createParameters.geometry.make_dynamic(21 * mdbx::env::geometry::MiB, 84 * mdbx::env::geometry::MiB);

  mdbx::env::operate_parameters operateParameters(100, 10);
  operateParameters.options.no_sticky_threads = false;
  mdbx::env_managed env(path, createParameters, operateParameters);
  auto txn = env.start_write(false);
  /* mdbx::map_handle testHandle = */ txn.create_map("xyz", mdbx::key_mode::usual, mdbx::value_mode::single);
  txn.commit();

  //-------------------------------------
  txn = env.start_write();
  MDBX_txn *c_txn = txn;
  int err = mdbx_txn_reset(txn);
  assert(err == MDBX_EINVAL);
  bool ok = err == MDBX_EINVAL;

  err = mdbx_txn_break(txn);
  assert(err == MDBX_SUCCESS);
  ok = ok && err == MDBX_SUCCESS;

  err = mdbx_txn_commit(txn);
  assert(err == MDBX_RESULT_TRUE);
  ok = ok && err == MDBX_RESULT_TRUE;

  //-------------------------------------
  err = mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &c_txn);
  assert(err == MDBX_SUCCESS);
  ok = ok && err == MDBX_SUCCESS;
  assert(c_txn == (const MDBX_txn *)txn);

  err = mdbx_txn_break(txn);
  assert(err == MDBX_SUCCESS);
  ok = ok && err == MDBX_SUCCESS;

  err = mdbx_txn_reset(txn);
  assert(err == MDBX_EINVAL);
  ok = ok && err == MDBX_EINVAL;

  err = mdbx_txn_commit(txn);
  assert(err == MDBX_RESULT_TRUE);
  ok = ok && err == MDBX_RESULT_TRUE;

  err = mdbx_txn_abort(c_txn);
  assert(err == MDBX_BAD_TXN);
  ok = ok && err == MDBX_BAD_TXN;
  //-------------------------------------
  err = mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &c_txn);
  assert(err == MDBX_SUCCESS);
  ok = ok && err == MDBX_SUCCESS;
  assert(c_txn == (const MDBX_txn *)txn);
  txn.commit();

  err = mdbx_txn_reset(c_txn);
  assert(err == MDBX_BAD_TXN);
  ok = ok && err == MDBX_BAD_TXN;
  err = mdbx_txn_break(c_txn);
  assert(err == MDBX_BAD_TXN);
  ok = ok && err == MDBX_BAD_TXN;
  err = mdbx_txn_abort(c_txn);
  assert(err == MDBX_BAD_TXN);
  ok = ok && err == MDBX_BAD_TXN;

  //=====================================

  txn = env.start_read();
  err = mdbx_txn_begin(env, txn, MDBX_TXN_READWRITE, &c_txn);
  assert(err == MDBX_BAD_TXN);
  ok = ok && err == MDBX_BAD_TXN;
  txn.make_broken();
  err = mdbx_txn_begin(env, txn, MDBX_TXN_READWRITE, &c_txn);
  assert(err == MDBX_BAD_TXN);
  ok = ok && err == MDBX_BAD_TXN;
  txn.reset_reading();
  err = mdbx_txn_begin(env, txn, MDBX_TXN_READWRITE, &c_txn);
  assert(err == MDBX_BAD_TXN);
  ok = ok && err == MDBX_BAD_TXN;
  txn.abort();

  //-------------------------------------

  txn = env.start_read();
  txn.reset_reading();
  txn.make_broken();
  txn.abort();

  //=====================================

  std::latch s(1);
  txn = env.start_read();
  c_txn = txn;

  std::thread t([&]() {
    s.wait();
#if MDBX_TXN_CHECKOWNER
    err = mdbx_txn_reset(c_txn);
    assert(err == MDBX_THREAD_MISMATCH);
    ok = ok && err == MDBX_THREAD_MISMATCH;
    err = mdbx_txn_break(c_txn);
    assert(err == MDBX_THREAD_MISMATCH);
    ok = ok && err == MDBX_THREAD_MISMATCH;
    err = mdbx_txn_commit(c_txn);
    assert(err == MDBX_THREAD_MISMATCH);
    ok = ok && err == MDBX_THREAD_MISMATCH;
    err = mdbx_txn_abort(c_txn);
    assert(err == MDBX_THREAD_MISMATCH);
    ok = ok && err == MDBX_THREAD_MISMATCH;
#endif /* MDBX_TXN_CHECKOWNER */

    err = mdbx_txn_begin(env, txn, MDBX_TXN_READWRITE, &c_txn);
#if MDBX_TXN_CHECKOWNER
    assert(err == MDBX_THREAD_MISMATCH);
    ok = ok && err == MDBX_THREAD_MISMATCH;
#else
    assert(err == MDBX_BAD_TXN);
    ok = ok && err == MDBX_BAD_TXN;
#endif /* MDBX_TXN_CHECKOWNER */
  });

  s.count_down();
  t.join();

  return ok;
}

bool case1(const mdbx::path &path) {
  mdbx::env::operate_parameters operateParameters(100, 10);
  operateParameters.options.no_sticky_threads = true;
  operateParameters.options.nested_write_transactions = true;
  mdbx::env_managed env(path, operateParameters);

  //-------------------------------------
  auto txn = env.start_write();
  MDBX_txn *c_txn = txn;
  int err = mdbx_txn_reset(txn);
  assert(err == MDBX_EINVAL);
  bool ok = err == MDBX_EINVAL;

  err = mdbx_txn_break(txn);
  assert(err == MDBX_SUCCESS);
  ok = ok && err == MDBX_SUCCESS;

  err = mdbx_txn_commit(txn);
  assert(err == MDBX_RESULT_TRUE);
  ok = ok && err == MDBX_RESULT_TRUE;

  //-------------------------------------
  err = mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &c_txn);
  assert(err == MDBX_SUCCESS);
  ok = ok && err == MDBX_SUCCESS;
  assert(c_txn == (const MDBX_txn *)txn);

  err = mdbx_txn_break(txn);
  assert(err == MDBX_SUCCESS);
  ok = ok && err == MDBX_SUCCESS;

  err = mdbx_txn_reset(txn);
  assert(err == MDBX_EINVAL);
  ok = ok && err == MDBX_EINVAL;

  err = mdbx_txn_commit(txn);
  assert(err == MDBX_RESULT_TRUE);
  ok = ok && err == MDBX_RESULT_TRUE;

  err = mdbx_txn_abort(c_txn);
  assert(err == MDBX_BAD_TXN);
  ok = ok && err == MDBX_BAD_TXN;
  //-------------------------------------
  err = mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &c_txn);
  assert(err == MDBX_SUCCESS);
  ok = ok && err == MDBX_SUCCESS;
  assert(c_txn == (const MDBX_txn *)txn);
  txn.commit();

  err = mdbx_txn_reset(c_txn);
  assert(err == MDBX_BAD_TXN);
  ok = ok && err == MDBX_BAD_TXN;
  err = mdbx_txn_break(c_txn);
  assert(err == MDBX_BAD_TXN);
  ok = ok && err == MDBX_BAD_TXN;
  err = mdbx_txn_abort(c_txn);
  assert(err == MDBX_BAD_TXN);
  ok = ok && err == MDBX_BAD_TXN;

  //=====================================

  txn = env.start_read();
  err = mdbx_txn_begin(env, txn, MDBX_TXN_READWRITE, &c_txn);
  assert(err == MDBX_BAD_TXN);
  ok = ok && err == MDBX_BAD_TXN;
  txn.make_broken();
  err = mdbx_txn_begin(env, txn, MDBX_TXN_READWRITE, &c_txn);
  assert(err == MDBX_BAD_TXN);
  ok = ok && err == MDBX_BAD_TXN;
  txn.reset_reading();
  err = mdbx_txn_begin(env, txn, MDBX_TXN_READWRITE, &c_txn);
  assert(err == MDBX_BAD_TXN);
  ok = ok && err == MDBX_BAD_TXN;
  txn.abort();

  //-------------------------------------

  txn = env.start_read();
  txn.reset_reading();
  txn.make_broken();
  txn.abort();

  //=====================================

  std::latch s1(1), s2(1), s3(1);
  txn = env.start_read();
  c_txn = txn;

  std::thread t([&]() {
    s1.wait();
    err = mdbx_txn_break(c_txn);
    assert(err == MDBX_SUCCESS);
    ok = ok && err == MDBX_SUCCESS;
    err = mdbx_txn_reset(c_txn);
    assert(err == MDBX_SUCCESS);
    ok = ok && err == MDBX_SUCCESS;
    txn.renew_reading();
    s2.count_down();

    s3.wait();
    err = mdbx_txn_begin(env, txn, MDBX_TXN_READWRITE, &c_txn);
    assert(err == MDBX_SUCCESS);
    ok = ok && err == MDBX_SUCCESS;
    err = mdbx_txn_commit(c_txn);
    assert(err == MDBX_SUCCESS);
    ok = ok && err == MDBX_SUCCESS;
    c_txn = txn;
    err = mdbx_txn_commit(c_txn);
    assert(err == MDBX_THREAD_MISMATCH);
    ok = ok && err == MDBX_THREAD_MISMATCH;
    err = mdbx_txn_abort(c_txn);
    assert(err == MDBX_THREAD_MISMATCH);
    ok = ok && err == MDBX_THREAD_MISMATCH;
    err = mdbx_txn_break(c_txn);
    assert(err == MDBX_SUCCESS);
    ok = ok && err == MDBX_SUCCESS;
    err = mdbx_txn_reset(c_txn);
    assert(err == MDBX_EINVAL);
    ok = ok && err == MDBX_EINVAL;
  });

  s1.count_down();
  s2.wait();
  txn.commit();
  txn = env.start_write();
  s3.count_down();

  t.join();
  txn.abort();

  return ok;
}

bool case2(const mdbx::path &path, bool no_sticky_threads) {
  mdbx::env::operate_parameters operateParameters(100, 10);
  operateParameters.options.no_sticky_threads = no_sticky_threads;
  mdbx::env_managed env(path, operateParameters);

  std::latch s(1);
  std::vector<std::thread> l;
  for (size_t n = 0; n < 8; ++n)
    l.push_back(std::thread([&]() {
      s.wait();
      for (size_t i = 0; i < 1000000 / RELIEF_FACTOR; ++i) {
        auto txn = env.start_read();
        txn.abort();
      }
    }));

  s.count_down();
  for (auto &t : l)
    t.join();

  return true;
}

bool case3(const mdbx::path &path, bool no_sticky_threads) {
  mdbx::env::remove(path);
  mdbx::env_managed::create_parameters createParameters;
  createParameters.geometry.make_dynamic(21 * mdbx::env::geometry::MiB, 84 * mdbx::env::geometry::MiB);
  mdbx::env::operate_parameters operateParameters(100, 10);
  operateParameters.options.no_sticky_threads = no_sticky_threads;
  mdbx::env_managed env(path, createParameters, operateParameters);

  mdbx::pair pair = {"key", "val"};
  const auto N = std::thread::hardware_concurrency() * 2;
  std::latch s0(N + 1), s1(N + 1), s2(N + 1);
  std::vector<std::thread> l;

  volatile bool ok = true;
  for (size_t n = 0; n < N; ++n)
    l.push_back(std::thread([&]() {
      try {
        s0.arrive_and_wait();
        {
          auto txn = env.start_read();
          mdbx::slice value;
          int err = mdbx_get(txn, 1, pair.key, &value);
          if (err != MDBX_NOTFOUND) {
            ok = false;
            std::cerr << "Unexpected error " << err << "\n";
          }
        }

        s1.arrive_and_wait();
        s2.arrive_and_wait();
        {
          auto txn = env.start_read();
          if (txn.get(1, pair.key) != pair.value)
            ok = false;
        }
      } catch (const std::exception &ex) {
        std::cerr << "Exception: " << ex.what() << "\n";
        ok = false;
      }
    }));

  s0.arrive_and_wait();
  auto txn = env.start_write();
  s1.arrive_and_wait();
  txn.insert(1, pair);
  txn.commit();
  s2.arrive_and_wait();

  for (auto &t : l)
    t.join();

  return ok;
}

int doit() {
  mdbx::path path = "test-txn";
  mdbx::env::remove(path);

  bool ok = true;
  ok = case0(path) && ok;
  ok = case1(path) && ok;
  ok = case2(path, false) && ok;
  ok = case2(path, true) && ok;
  ok = case3(path, false) && ok;
  ok = case3(path, true) && ok;

  std::cout << (ok ? "OK\n" : "FAIL\n");
  return ok ? EXIT_SUCCESS : EXIT_FAILURE;
}

static char log_buffer[1024];

static void logger_nofmt(MDBX_log_level_t loglevel, const char *function, int line, const char *msg,
                         unsigned length) noexcept {
  (void)length;
  (void)loglevel;
  fprintf(stdout, "%s:%u %s", function, line, msg);
}

int main(int argc, char *argv[]) {
  (void)argc;
  (void)argv;
  mdbx_setup_debug_nofmt(MDBX_LOG_VERBOSE, MDBX_DBG_ASSERT, logger_nofmt, log_buffer, sizeof(log_buffer));
  try {
    return doit();
  } catch (const std::exception &ex) {
    std::cerr << "Exception: " << ex.what() << "\n";
    return EXIT_FAILURE;
  }
}

#endif /* __cpp_lib_latch */
