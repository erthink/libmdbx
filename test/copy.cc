#include "test.h"

void testcase_copy::copy_db(const bool with_compaction) {
  int err = osal_removefile(copy_pathname);
  if (err != MDBX_SUCCESS && err != MDBX_ENOFILE)
    failure_perror("mdbx_removefile()", err);

  err = mdbx_env_copy(db_guard.get(), copy_pathname.c_str(),
                      with_compaction ? MDBX_CP_COMPACT : 0);
  if (unlikely(err != MDBX_SUCCESS))
    failure_perror(with_compaction ? "mdbx_env_copy(MDBX_CP_COMPACT)"
                                   : "mdbx_env_copy(MDBX_CP_ASIS)",
                   err);
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
