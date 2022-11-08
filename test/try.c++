#include "test.h++"

class testcase_try : public testcase {
public:
  testcase_try(const actor_config &config, const mdbx_pid_t pid)
      : testcase(config, pid) {}
  bool run() override;
};
REGISTER_TESTCASE(try);

bool testcase_try::run() {
  db_open();
  assert(!txn_guard);

  MDBX_txn *txn = nullptr;
  MDBX_txn *txn2 = nullptr;
  int rc = mdbx_txn_begin(db_guard.get(), nullptr, MDBX_TXN_READWRITE, &txn);
  if (unlikely(rc != MDBX_SUCCESS))
    failure_perror("mdbx_txn_begin(MDBX_TXN_TRY)", rc);
  else {
    rc = mdbx_txn_begin(db_guard.get(), nullptr, MDBX_TXN_TRY, &txn2);
    if (unlikely(rc != MDBX_BUSY))
      failure_perror("mdbx_txn_begin(MDBX_TXN_TRY)", rc);
  }

  txn_guard.reset(txn);
  return true;
}
