#include "mdbx.h++"

#include <iostream>

static char log_buffer[1024];

static void logger_nofmt(MDBX_log_level_t loglevel, const char *function, int line, const char *msg,
                         unsigned length) noexcept {
  (void)length;
  (void)loglevel;
  fprintf(stdout, "%s:%u %s", function, line, msg);
}

int doit() {
  mdbx::path db_filename = "test-dbi";
  mdbx::env::remove(db_filename);

  mdbx::env::operate_parameters operateParameters(100, 10, mdbx::env::nested_transactions);
  mdbx::env_managed::create_parameters createParameters;
  {
    mdbx::env_managed env(db_filename, createParameters, operateParameters);
    mdbx::txn_managed txn = env.start_write();
    /* mdbx::map_handle dbi = */ txn.create_map("fap1", mdbx::key_mode::reverse, mdbx::value_mode::single);
    txn.commit();
  }

  mdbx::env_managed env(db_filename, createParameters, operateParameters);
  {
    // проверяем доступность в родительской транзакции хендла открытого в дочерней транзакции после коммита
    mdbx::txn_managed txn = env.start_write();
    mdbx::txn_managed nested = txn.start_nested();
    mdbx::map_handle dbi = nested.open_map_accede("fap1");
    nested.commit();
    MDBX_MAYBE_UNUSED auto stat = txn.get_map_stat(dbi);
    txn.commit();
    env.close_map(dbi);
  }

  {
    // проверяем НЕ доступность в родительской транзакции хендла открытого в дочерней транзакции после прерывания
    mdbx::txn_managed txn = env.start_write();
    mdbx::txn_managed nested = txn.start_nested();
    mdbx::map_handle dbi = nested.open_map_accede("fap1");
    nested.abort();
    MDBX_stat stat;
    int err = mdbx_dbi_stat(txn, dbi, &stat, sizeof(stat));
    if (err != MDBX_BAD_DBI) {
      std::cerr << "unexpected result err-code " << err;
      return EXIT_FAILURE;
    }
    txn.commit();
  }

  {
    // снова проверяем что таблица открывается и хендл доступень в родительской транзакции после коммита открывшей его
    // дочерней
    mdbx::txn_managed txn = env.start_write();
    mdbx::txn_managed nested = txn.start_nested();
    mdbx::map_handle dbi = nested.open_map_accede("fap1");
    nested.commit();
    MDBX_MAYBE_UNUSED auto stat = txn.get_map_stat(dbi);
    txn.commit();
    env.close_map(dbi);
  }

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
