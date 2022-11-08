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

class testcase_deadread : public testcase {
public:
  testcase_deadread(const actor_config &config, const mdbx_pid_t pid)
      : testcase(config, pid) {}
  bool run() override;
};
REGISTER_TESTCASE(deadread);

bool testcase_deadread::run() {
  db_open();
  txn_begin(true);
  cursor_guard.reset();
  txn_guard.reset();
  db_guard.reset();
  return true;
}

//-----------------------------------------------------------------------------

class testcase_deadwrite : public testcase {
public:
  testcase_deadwrite(const actor_config &config, const mdbx_pid_t pid)
      : testcase(config, pid) {}
  bool run() override;
};

REGISTER_TESTCASE(deadwrite);

bool testcase_deadwrite::run() {
  db_open();
  txn_begin(false);
  cursor_guard.reset();
  txn_guard.reset();
  db_guard.reset();
  return true;
}
