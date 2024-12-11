#include "mdbx.h++"

#include <iostream>

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

  mdbx_setup_debug_nofmt(MDBX_LOG_NOTICE, MDBX_DBG_ASSERT, logger_nofmt, log_buffer, sizeof(log_buffer));

  mdbx::path db_filename = "test-dbi";
  mdbx::env::remove(db_filename);

  mdbx::env::operate_parameters operateParameters(100, 10);
  mdbx::env_managed::create_parameters createParameters;
  {
    mdbx::env_managed env2(db_filename, createParameters, operateParameters);
    mdbx::txn_managed txn2 = env2.start_write(false);
    /* mdbx::map_handle testHandle2 = */ txn2.create_map("fap1", mdbx::key_mode::reverse, mdbx::value_mode::single);
    txn2.commit();
  }
  mdbx::env_managed env(db_filename, createParameters, operateParameters);
  mdbx::txn_managed txn = env.start_write(false);
  /* mdbx::map_handle testHandle = */ txn.create_map("fap1", mdbx::key_mode::usual, mdbx::value_mode::single);
  txn.commit();

  std::cout << "OK\n";
  return EXIT_SUCCESS;
}
