/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2024
/// \copyright SPDX-License-Identifier: Apache-2.0

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
