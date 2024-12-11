#include "mdbx.h++"
#include <cstring>

static const char *const testkey = "testkey";
static uint64_t testval = 11;

int main(int argc, char *argv[]) {
  (void)argc;
  (void)argv;

  mdbx::path db_filename = "test-early_close_dbi";
  mdbx::env_managed::remove(db_filename);

  MDBX_env *environment;
  MDBX_MAYBE_UNUSED int err = mdbx_env_create(&environment);
  assert(err == MDBX_SUCCESS);

  err = mdbx_env_set_option(environment, MDBX_opt_max_db, 2);
  assert(err == MDBX_SUCCESS);
  err = mdbx_env_set_option(environment, MDBX_opt_max_readers, 2);
  assert(err == MDBX_SUCCESS);
  // status = mdbx_env_set_option(environment, MDBX_opt_prefault_write_enable,
  // 1); assert(err == MDBX_SUCCESS);

  intptr_t lowerbound(0), size(0), upperbound(mdbx::env::geometry::GiB / 2);
  intptr_t step(128 * mdbx::env::geometry::MiB), shrink(256 * mdbx::env::geometry::MiB), pagesize(-1);
  err = mdbx_env_set_geometry(environment, lowerbound, size, upperbound, step, shrink, pagesize);
  assert(err == MDBX_SUCCESS);

  MDBX_env_flags_t flags(MDBX_NOSUBDIR | MDBX_WRITEMAP | MDBX_LIFORECLAIM | MDBX_NORDAHEAD);
  err = mdbx_env_openT(environment, db_filename.c_str(), flags, 0644);
  assert(err == MDBX_SUCCESS);

  // ---

  MDBX_txn *transaction;
  err = mdbx_txn_begin(environment, nullptr, MDBX_TXN_READWRITE, &transaction);
  assert(err == MDBX_SUCCESS);

  MDBX_dbi textindex;
  err = mdbx_dbi_open(transaction, "testdb", MDBX_DB_DEFAULTS, &textindex);
  assert(err == MDBX_NOTFOUND);
  err = mdbx_dbi_open(transaction, "testdb", MDBX_CREATE, &textindex);
  assert(err == MDBX_SUCCESS);

  MDBX_val mdbxkey{(void *)testkey, std::strlen(testkey)}, mdbxval{};
  err = mdbx_get(transaction, textindex, &mdbxkey, &mdbxval);
  assert(err == MDBX_NOTFOUND);

  unsigned dbi_flags, dbi_state;
  err = mdbx_dbi_flags_ex(transaction, textindex, &dbi_flags, &dbi_state);
  assert(err == MDBX_SUCCESS);
  assert((dbi_state & (MDBX_DBI_CREAT | MDBX_DBI_DIRTY)) != 0);
  err = mdbx_dbi_close(environment, textindex);
  assert(err == MDBX_DANGLING_DBI);

  err = mdbx_txn_commit(transaction);
  assert(err == MDBX_SUCCESS);

  // ---

  err = mdbx_txn_begin(environment, nullptr, MDBX_TXN_READWRITE, &transaction);
  assert(err == MDBX_SUCCESS);

  MDBX_val mdbxput{&testval, sizeof(uint64_t)};
  err = mdbx_put(transaction, textindex, &mdbxkey, &mdbxput, MDBX_NOOVERWRITE);
  assert(err == MDBX_SUCCESS);
  err = mdbx_get(transaction, textindex, &mdbxkey, &mdbxval);
  assert(err == MDBX_SUCCESS);
  assert(testval == *reinterpret_cast<uint64_t *>(mdbxval.iov_base));

  err = mdbx_put(transaction, textindex, &mdbxkey, &mdbxput, MDBX_NOOVERWRITE);
  assert(err == MDBX_KEYEXIST);
  err = mdbx_get(transaction, textindex, &mdbxkey, &mdbxval);
  assert(err == MDBX_SUCCESS);
  assert(testval == *reinterpret_cast<uint64_t *>(mdbxval.iov_base));

  err = mdbx_dbi_flags_ex(transaction, textindex, &dbi_flags, &dbi_state);
  assert(err == MDBX_SUCCESS);
  assert((dbi_state & MDBX_DBI_DIRTY) != 0);
  err = mdbx_dbi_close(environment, textindex);
  assert(err == MDBX_DANGLING_DBI);
  err = mdbx_txn_commit(transaction);
  assert(err == MDBX_SUCCESS);

  // ---

  err = mdbx_txn_begin(environment, nullptr, MDBX_TXN_RDONLY, &transaction);
  assert(err == MDBX_SUCCESS);
  err = mdbx_get(transaction, textindex, &mdbxkey, &mdbxval);
  assert(err == MDBX_SUCCESS);
  assert(testval == *reinterpret_cast<uint64_t *>(mdbxval.iov_base));

  err = mdbx_dbi_close(environment, textindex);
  assert(err == MDBX_SUCCESS);
  err = mdbx_txn_commit(transaction);
  assert(err == MDBX_SUCCESS);
  err = mdbx_env_close_ex(environment, true);
  assert(err == MDBX_SUCCESS);

  return 0;
}
