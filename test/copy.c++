/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025
/// \copyright SPDX-License-Identifier: Apache-2.0

#include "test.h++"

class testcase_copy : public testcase {
  const std::string copy_pathname;
  void copy_db(const bool with_compaction);

public:
  testcase_copy(const actor_config &config, const mdbx_pid_t pid)
      : testcase(config, pid), copy_pathname(config.params.pathname_db + "-copy") {}
  bool run() override;
};
REGISTER_TESTCASE(copy);

void testcase_copy::copy_db(const bool with_compaction) {
  int err = mdbx_env_delete(copy_pathname.c_str(), MDBX_ENV_JUST_DELETE);
  if (err != MDBX_SUCCESS && err != MDBX_RESULT_TRUE)
    failure_perror("osal_removefile()", err);

  if (flipcoin()) {
    err = mdbx_env_copy(db_guard.get(), copy_pathname.c_str(), with_compaction ? MDBX_CP_COMPACT : MDBX_CP_DEFAULTS);
    log_verbose("mdbx_env_copy(%s), err %d", with_compaction ? "true" : "false", err);
    if (unlikely(err != MDBX_SUCCESS))
      failure_perror(with_compaction ? "mdbx_env_copy(MDBX_CP_COMPACT)" : "mdbx_env_copy(MDBX_CP_ASIS)", err);
  } else {
    do {
      const bool ro = mode_readonly() || flipcoin();
      const bool throttle = ro && flipcoin();
      const bool dynsize = flipcoin();
      const bool flush = flipcoin();
      const bool enable_renew = flipcoin();
      const MDBX_copy_flags_t flags = (with_compaction ? MDBX_CP_COMPACT : MDBX_CP_DEFAULTS) |
                                      (dynsize ? MDBX_CP_FORCE_DYNAMIC_SIZE : MDBX_CP_DEFAULTS) |
                                      (throttle ? MDBX_CP_THROTTLE_MVCC : MDBX_CP_DEFAULTS) |
                                      (flush ? MDBX_CP_DEFAULTS : MDBX_CP_DONT_FLUSH) |
                                      (enable_renew ? MDBX_CP_RENEW_TXN : MDBX_CP_DEFAULTS);
      txn_begin(ro);
      err = mdbx_txn_copy2pathname(txn_guard.get(), copy_pathname.c_str(), flags);
      log_verbose("mdbx_txn_copy2pathname(flags=0x%X), err %d", flags, err);
      txn_end(err != MDBX_SUCCESS || flipcoin());
      if (unlikely(err != MDBX_SUCCESS && !(throttle && err == MDBX_OUSTED) &&
                   !(!enable_renew && err == MDBX_MVCC_RETARDED) &&
                   !(err == MDBX_EINVAL && !ro && (flags & (MDBX_CP_THROTTLE_MVCC | MDBX_CP_RENEW_TXN)) != 0)))
        failure_perror(
            with_compaction ? "mdbx_txn_copy2pathname(MDBX_CP_COMPACT)" : "mdbx_txn_copy2pathname(MDBX_CP_ASIS)", err);
    } while (err != MDBX_SUCCESS);
  }
}

bool testcase_copy::run() {
  jitter_delay();
  db_open();
  assert(!txn_guard);
  const bool order = flipcoin();
  jitter_delay();
  copy_db(order);
  jitter_delay();
  copy_db(!order);
  return true;
}
