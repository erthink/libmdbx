#include "mdbx.h++"

#include <iostream>

static void logger_nofmt(MDBX_log_level_t loglevel, const char *function, int line, const char *msg,
                         unsigned length) noexcept {
  (void)length;
  (void)loglevel;
  std::cout << function << ":" << line << " " << msg;
}

static char log_buffer[1024];

int main(int argc, const char *argv[]) {
  (void)argc;
  (void)argv;

  mdbx_setup_debug_nofmt(MDBX_LOG_NOTICE, MDBX_DBG_ASSERT, logger_nofmt, log_buffer, sizeof(log_buffer));

  mdbx::path db_filename = "test-cursor-closing";
  mdbx::env::remove(db_filename);

  mdbx::env_managed env(db_filename, mdbx::env_managed::create_parameters(),
                        mdbx::env::operate_parameters(42, 0, mdbx::env::nested_transactions));

  {
    auto txn = env.start_write();
    auto table = txn.create_map("dummy", mdbx::key_mode::usual, mdbx::value_mode::single);
    auto cursor_1 = txn.open_cursor(table);
    auto cursor_2 = cursor_1.clone();

    auto nested = env.start_write(txn);
    auto nested_cursor_1 = nested.open_cursor(table);
    auto nested_cursor_2 = nested_cursor_1.clone();
    auto nested_cursor_3 = cursor_1.clone();

    auto deep = env.start_write(nested);
    auto deep_cursor_1 = deep.open_cursor(table);
    auto deep_cursor_2 = nested_cursor_1.clone();
    auto deep_cursor_3 = cursor_1.clone();
    deep_cursor_1.close();
    deep.commit();
    deep_cursor_2.close();

    nested_cursor_1.close();
    nested.abort();
    nested_cursor_2.close();

    cursor_1.close();
    txn.commit();
    cursor_2.close();
  }

  std::cout << "OK\n";
  return EXIT_SUCCESS;
}
