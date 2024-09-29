#include "mdbx.h++"

#include <iostream>

static void logger(MDBX_log_level_t loglevel, const char *function, int line,
                   const char *fmt, va_list args) noexcept {
  (void)loglevel;
  fprintf(stdout, "%s:%u ", function, line);
  vfprintf(stdout, fmt, args);
}

int main(int argc, const char *argv[]) {
  (void)argc;
  (void)argv;

  mdbx_setup_debug(MDBX_LOG_NOTICE, MDBX_DBG_ASSERT, logger);

  mdbx::path path = "test-dbi";
  mdbx::env::remove(path);

  mdbx::env::operate_parameters operateParameters(100, 10);
  mdbx::env_managed::create_parameters createParameters;
  {
    mdbx::env_managed env2(path, createParameters, operateParameters);
    mdbx::txn_managed txn2 = env2.start_write(false);
    /* mdbx::map_handle testHandle2 = */ txn2.create_map(
        "fap1", mdbx::key_mode::reverse, mdbx::value_mode::single);
    txn2.commit();
  }
  mdbx::env_managed env(path, createParameters, operateParameters);
  mdbx::txn_managed txn = env.start_write(false);
  /* mdbx::map_handle testHandle = */ txn.create_map(
      "fap1", mdbx::key_mode::usual, mdbx::value_mode::single);
  txn.commit();

  std::cout << "OK\n";
  return EXIT_SUCCESS;
}
