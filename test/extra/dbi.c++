#include "mdbx.h++"

#include <iostream>

mdbx::path db_filename = "test-dbi";

bool case1() {
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
      std::cerr << "Unexpected err " << err << " (wanna MDBX_BAD_DBI/-30780)\n";
      return false;
    }
    txn.commit();
  }

  {
    // снова проверяем что таблица открывается и хендл доступень в родительской транзакции,
    // после коммита открывшей его дочерней
    mdbx::txn_managed txn = env.start_write();
    mdbx::txn_managed nested = txn.start_nested();
    mdbx::map_handle dbi = nested.open_map_accede("fap1");
    nested.commit();
    MDBX_MAYBE_UNUSED auto stat = txn.get_map_stat(dbi);
    txn.commit();
    env.close_map(dbi);
  }

  return true;
}

bool case2() {
  bool ok = true;
  mdbx::env_managed::create_parameters createParameters;
  mdbx::env::remove(db_filename);
  {
    mdbx::env::operate_parameters operateParameters(0, 10, mdbx::env::nested_transactions);
    mdbx::env_managed env(db_filename, createParameters, operateParameters);
    {
      mdbx::txn_managed txn = env.start_write();
      MDBX_dbi dbi = 0;
      int err = mdbx_dbi_open(txn, "test", MDBX_CREATE, &dbi);
      if (err != MDBX_DBS_FULL) {
        std::cerr << "Unexpected err " << err << " (wanna MDBX_DBS_FULL/-30791)\n";
        ok = false;
      }
    }
    {
      mdbx::txn_managed txn = env.start_write();
      MDBX_dbi dbi = 0;
      int err = mdbx_dbi_open(txn, "test", MDBX_CREATE | MDBX_DUPSORT | MDBX_DUPFIXED, &dbi);
      if (err != MDBX_DBS_FULL) {
        std::cerr << "Unexpected err " << err << " (wanna MDBX_DBS_FULL/-30791)\n";
        ok = false;
      }
    }
  }

  {
    mdbx::env::operate_parameters operateParameters(1, 10, mdbx::env::nested_transactions);
    mdbx::env_managed env(db_filename, createParameters, operateParameters);
    {
      mdbx::txn_managed txn = env.start_write();
      mdbx::map_handle dbi = txn.create_map("dup", mdbx::key_mode::ordinal, mdbx::value_mode::multi_ordinal);
      txn.commit();
      env.close_map(dbi);
    }
    {
      mdbx::txn_managed txn = env.start_write();
      mdbx::map_handle dbi = txn.create_map("uni", mdbx::key_mode::reverse, mdbx::value_mode::single);
      txn.commit();
      env.close_map(dbi);
    }
  }

  {
    mdbx::env::operate_parameters operateParameters(0, 10, mdbx::env::nested_transactions);
    mdbx::env_managed env(db_filename, createParameters, operateParameters);
    {
      mdbx::txn_managed txn = env.start_read();
      MDBX_dbi dbi = 0;
      int err = mdbx_dbi_open(txn, "uni", MDBX_DB_ACCEDE, &dbi);
      if (err != MDBX_DBS_FULL) {
        std::cerr << "Unexpected err " << err << " (wanna MDBX_DBS_FULL/-30791)\n";
        ok = false;
      }
      if (dbi)
        env.close_map(dbi);
    }

    {
      mdbx::txn_managed txn = env.start_read();
      MDBX_dbi dbi = 0;
      int err = mdbx_dbi_open(txn, "dup", MDBX_DB_ACCEDE, &dbi);
      if (err != MDBX_DBS_FULL) {
        std::cerr << "Unexpected err " << err << " (wanna MDBX_DBS_FULL/-30791)\n";
        ok = false;
      }
      if (dbi)
        env.close_map(dbi);
    }
  }

  {
    {
      mdbx::env::operate_parameters operateParameters(1, 10, mdbx::env::nested_transactions);
      mdbx::env_managed env(db_filename, createParameters, operateParameters);
      {
        mdbx::txn_managed txn = env.start_read();
        MDBX_dbi dbi = 0;
        int err = mdbx_dbi_open(txn, "uni", MDBX_DB_ACCEDE, &dbi);
        if (err != MDBX_SUCCESS) {
          std::cerr << "Unexpected err " << err << "\n";
          ok = false;
        }
        if (dbi)
          env.close_map(dbi);
      }
      {
        mdbx::txn_managed txn = env.start_read();
        MDBX_dbi dbi = 0;
        int err = mdbx_dbi_open(txn, "dup", MDBX_DB_ACCEDE, &dbi);
        if (err != MDBX_SUCCESS) {
          std::cerr << "Unexpected err " << err << "\n";
          ok = false;
        }
        if (dbi)
          env.close_map(dbi);
      }
    }
  }

  return ok;
}

bool case3() {
  bool ok = true;
  mdbx::env_managed::create_parameters createParameters;
  mdbx::env::remove(db_filename);
  {
    mdbx::env::operate_parameters operateParameters(1, 10, mdbx::env::nested_transactions);
    mdbx::env_managed env(db_filename, createParameters, operateParameters);
    {
      mdbx::txn_managed txn = env.start_write();
      MDBX_dbi notexists_dbi = 0;
      int err = mdbx_dbi_open(txn, "test", MDBX_DB_DEFAULTS, &notexists_dbi);
      if (err != MDBX_NOTFOUND) {
        std::cerr << "Unexpected err " << err << " (wanna MDBX_NOTFOUND/-30798)\n";
        ok = false;
      }
      mdbx::map_handle dbi = txn.create_map("test", mdbx::key_mode::ordinal, mdbx::value_mode::single);
      dbi = txn.open_map("test", mdbx::key_mode::ordinal, mdbx::value_mode::single);
      err = mdbx_dbi_close(env, dbi);
      if (err != MDBX_DANGLING_DBI) {
        std::cerr << "Unexpected err " << err << " (wanna MDBX_DANGLING_DBI/-30412)\n";
        ok = false;
      }
      txn.commit();
      env.close_map(dbi);
    }
  }
  return ok;
}

int doit() {

  bool ok = true;
  ok = case1() && ok;
  ok = case2() && ok;
  ok = case3() && ok;

  if (ok) {
    std::cout << "OK\n";
    return EXIT_SUCCESS;
  } else {
    std::cerr << "FAIL\n";
    return EXIT_FAILURE;
  }
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
  mdbx_setup_debug_nofmt(MDBX_LOG_NOTICE, MDBX_DBG_ASSERT, logger_nofmt, log_buffer, sizeof(log_buffer));
  try {
    return doit();
  } catch (const std::exception &ex) {
    std::cerr << "Exception: " << ex.what() << "\n";
    return EXIT_FAILURE;
  }
}
