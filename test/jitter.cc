/*
 * Copyright 2017-2020 Leonid Yuriev <leo@yuriev.ru>
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

#include "test.h"

bool testcase_jitter::run() {
  int err;
  size_t upper_limit = config.params.size_upper;
  if (upper_limit < 1)
    upper_limit = config.params.size_now * 2;

  while (should_continue()) {
    jitter_delay();
    db_open();

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
      err = mdbx_env_set_geometry(
          db_guard.get(), -1, -1,
          coin4size ? upper_limit * 2 / 3 : upper_limit * 3 / 2, -1, -1, -1);
      if (err != MDBX_SUCCESS && err != MDBX_UNABLE_EXTEND_MAPSIZE &&
          err != MDBX_MAP_FULL && err != MDBX_TOO_LARGE)
        failure_perror("mdbx_env_set_geometry-1", err);
    }
    txn_end(flipcoin());

    err = mdbx_env_set_geometry(
        db_guard.get(), -1, -1,
        !coin4size ? upper_limit * 2 / 3 : upper_limit * 3 / 2, -1, -1, -1);
    if (err != MDBX_SUCCESS && err != MDBX_UNABLE_EXTEND_MAPSIZE &&
        err != MDBX_MAP_FULL && err != MDBX_TOO_LARGE)
      failure_perror("mdbx_env_set_geometry-2", err);

    if (flipcoin()) {
      jitter_delay();
      txn_begin(true);
      jitter_delay();
      txn_end(flipcoin());
    }

    jitter_delay();
    err =
        mdbx_env_set_geometry(db_guard.get(), -1, -1, upper_limit, -1, -1, -1);
    if (err != MDBX_SUCCESS && err != MDBX_UNABLE_EXTEND_MAPSIZE &&
        err != MDBX_MAP_FULL && err != MDBX_TOO_LARGE)
      failure_perror("mdbx_env_set_geometry-3", err);

    db_close();

    /* just 'align' nops with other tests with batching */
    const auto batching =
        std::max(config.params.batch_read, config.params.batch_write);
    report(std::max(1u, batching / 2));
  }
  return true;
}
