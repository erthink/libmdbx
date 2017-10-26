#include "test.h"

bool testcase_try::setup() {
  log_trace(">> setup");
  if (!inherited::setup())
    return false;

  log_trace("<< setup");
  return true;
}

bool testcase_try::run() {
  db_open();
  assert(!txn_guard);

  MDBX_txn *txn = nullptr;
  MDBX_txn *txn2 = nullptr;
  int rc = mdbx_txn_begin(db_guard.get(), nullptr, 0, &txn);
  if (unlikely(rc != MDBX_SUCCESS))
    failure_perror("mdbx_txn_begin(MDBX_TRYTXN)", rc);
  else {
    rc = mdbx_txn_begin(db_guard.get(), nullptr, MDBX_TRYTXN, &txn2);
    if (unlikely(rc != MDBX_BUSY))
      failure_perror("mdbx_txn_begin(MDBX_TRYTXN)", rc);
  }

  txn_guard.reset(txn);
  return true;
}

bool testcase_try::teardown() {
  log_trace(">> teardown");
  cursor_guard.release();
  txn_guard.release();
  db_guard.release();
  return inherited::teardown();
}
