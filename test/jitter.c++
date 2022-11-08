/*
 * Copyright 2017-2022 Leonid Yuriev <leo@yuriev.ru>
 * and other libmdbx authors: please see AUTHORS file.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in the file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 */

#include "test.h++"

class testcase_jitter : public testcase {
protected:
  void check_dbi_error(int expect, const char *stage);

public:
  testcase_jitter(const actor_config &config, const mdbx_pid_t pid)
      : testcase(config, pid) {}
  bool run() override;
};
REGISTER_TESTCASE(jitter);

void testcase_jitter::check_dbi_error(int expect, const char *stage) {
  MDBX_stat stat;
  int err = mdbx_dbi_stat(txn_guard.get(), dbi, &stat, sizeof(stat));
  if (err != expect)
    failure("unexpected result for %s dbi-handle: expect %d, got %d", stage,
            expect, err);
}

bool testcase_jitter::run() {
  int err;
  size_t upper_limit = config.params.size_upper;
  if (upper_limit < 1)
    upper_limit = config.params.size_now * 2;

  while (should_continue()) {
    jitter_delay();
    db_open();

    if (!dbi && !mode_readonly()) {
      // create table
      txn_begin(false);
      dbi = db_table_open(true);
      check_dbi_error(MDBX_SUCCESS, "created-uncommitted");
      // note: here and below the 4-byte length keys and value are used
      //       to be compatible with any Db-flags given from command line.
      MDBX_val k = {(void *)"k000", 4}, v = {(void *)"v001", 4};
      err = mdbx_put(txn_guard.get(), dbi, &k, &v, MDBX_UPSERT);
      if (err != MDBX_SUCCESS)
        failure_perror("jitter.put-1", err);
      txn_end(false);

      // drop & re-create table, but abort txn
      txn_begin(false);
      check_dbi_error(MDBX_SUCCESS, "created-committed");
      err = mdbx_drop(txn_guard.get(), dbi, true);
      if (unlikely(err != MDBX_SUCCESS))
        failure_perror("mdbx_drop(delete=true)", err);
      check_dbi_error(MDBX_BAD_DBI, "dropped-uncommitted");
      dbi = db_table_open(true);
      check_dbi_error(MDBX_SUCCESS, "recreated-uncommitted");
      txn_end(true);

      // check after aborted txn
      txn_begin(false);
      v = {(void *)"v002", 4};
      err = mdbx_put(txn_guard.get(), dbi, &k, &v, MDBX_UPSERT);
      if (err != MDBX_BAD_DBI)
        failure_perror("jitter.put-2", err);
      check_dbi_error(MDBX_BAD_DBI, "dropped-recreated-aborted");
      // restore DBI
      dbi = db_table_open(false);
      check_dbi_error(MDBX_SUCCESS, "dropped-recreated-aborted+reopened");
      v = {(void *)"v003", 4};
      err = mdbx_put(txn_guard.get(), dbi, &k, &v, MDBX_UPSERT);
      if (err != MDBX_SUCCESS)
        failure_perror("jitter.put-3", err);
      txn_end(false);
    }

    if (upper_limit < 1) {
      MDBX_envinfo info;
      err = mdbx_env_info_ex(db_guard.get(), txn_guard.get(), &info,
                             sizeof(info));
      if (err)
        failure_perror("mdbx_env_info_ex()", err);
      upper_limit = (info.mi_geo.upper < INTPTR_MAX)
                        ? (intptr_t)info.mi_geo.upper
                        : INTPTR_MAX;
    }

    if (flipcoin()) {
      jitter_delay();
      txn_begin(true);
      fetch_canary();
      jitter_delay();
      txn_end(flipcoin());
    }

    const bool coin4size = flipcoin();
    jitter_delay();
    txn_begin(mode_readonly());
    jitter_delay();
    if (!mode_readonly()) {
      fetch_canary();
      update_canary(1);
      if (global::config::geometry_jitter) {
        err = mdbx_env_set_geometry(
            db_guard.get(), -1, -1,
            coin4size ? upper_limit * 2 / 3 : upper_limit * 3 / 2, -1, -1, -1);
        if (err != MDBX_SUCCESS && err != MDBX_UNABLE_EXTEND_MAPSIZE &&
            err != MDBX_MAP_FULL && err != MDBX_TOO_LARGE && err != MDBX_EPERM)
          failure_perror("mdbx_env_set_geometry-1", err);
      }
    }
    txn_end(flipcoin());

    if (global::config::geometry_jitter) {
      err = mdbx_env_set_geometry(
          db_guard.get(), -1, -1,
          !coin4size ? upper_limit * 2 / 3 : upper_limit * 3 / 2, -1, -1, -1);
      if (err != MDBX_SUCCESS && err != MDBX_UNABLE_EXTEND_MAPSIZE &&
          err != MDBX_MAP_FULL && err != MDBX_TOO_LARGE && err != MDBX_EPERM)
        failure_perror("mdbx_env_set_geometry-2", err);
    }

    if (flipcoin()) {
      jitter_delay();
      txn_begin(true);
      jitter_delay();
      txn_end(flipcoin());
    }

    if (global::config::geometry_jitter) {
      jitter_delay();
      err = mdbx_env_set_geometry(db_guard.get(), -1, -1, upper_limit, -1, -1,
                                  -1);
      if (err != MDBX_SUCCESS && err != MDBX_UNABLE_EXTEND_MAPSIZE &&
          err != MDBX_MAP_FULL && err != MDBX_TOO_LARGE && err != MDBX_EPERM)
        failure_perror("mdbx_env_set_geometry-3", err);
    }

    db_close();

    /* just 'align' nops with other tests with batching */
    const auto batching =
        std::max(config.params.batch_read, config.params.batch_write);
    report(std::max(1u, batching / 2));
  }
  return true;
}
