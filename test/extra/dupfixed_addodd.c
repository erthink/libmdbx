/*
 * @Dvirsw (https://t.me/Dvirsw)
 * I think there is a bug with DUPFIXED. The following code fails.
 *
 * https://t.me/libmdbx/5368
 */

#include <sys/stat.h>
#include <sys/time.h>

#include "mdbx.h"
#include <assert.h>
#include <inttypes.h>
#include <limits.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main() {
  int rc;
  MDBX_env *env = NULL;
  MDBX_dbi dbi = 0;
  MDBX_val key, data;
  MDBX_txn *txn = NULL;

  rc = mdbx_env_create(&env);
  if (rc != MDBX_SUCCESS) {
    fprintf(stderr, "mdbx_env_create: (%d) %s\n", rc, mdbx_strerror(rc));
    exit(EXIT_FAILURE);
  }

  rc = mdbx_env_set_maxdbs(env, 1);
  if (rc != MDBX_SUCCESS) {
    fprintf(stderr, "mdbx_env_create: (%d) %s\n", rc, mdbx_strerror(rc));
    exit(EXIT_FAILURE);
  }

  rc = mdbx_env_open(env, "./example-db", MDBX_NOSUBDIR | MDBX_LIFORECLAIM,
                     0664);
  if (rc != MDBX_SUCCESS) {
    fprintf(stderr, "mdbx_env_open: (%d) %s\n", rc, mdbx_strerror(rc));
    exit(EXIT_FAILURE);
  }

  rc = mdbx_txn_begin(env, NULL, 0, &txn);
  if (rc != MDBX_SUCCESS) {
    fprintf(stderr, "mdbx_txn_begin: (%d) %s\n", rc, mdbx_strerror(rc));
    exit(EXIT_FAILURE);
  }

  rc = mdbx_dbi_open(txn, "test", MDBX_DUPSORT | MDBX_DUPFIXED | MDBX_CREATE,
                     &dbi);
  if (rc != MDBX_SUCCESS) {
    fprintf(stderr, "mdbx_dbi_open: (%d) %s\n", rc, mdbx_strerror(rc));
    exit(EXIT_FAILURE);
  }

  char key_bytes[32] = {0};
  key.iov_len = 32;
  key.iov_base = key_bytes;

  // Another put after this will fail.
  unsigned char idx;
  for (idx = 0; idx < 129; idx++) {
    char data_bytes[15] = {idx};
    data.iov_len = 15;
    data.iov_base = data_bytes;
    rc = mdbx_put(txn, dbi, &key, &data, 0);
    if (rc != MDBX_SUCCESS) {
      fprintf(stderr, "mdbx_put: (%d) %s\n", rc, mdbx_strerror(rc));
      exit(EXIT_FAILURE);
    }
  }

  // This will fail and exit.
  char data_bytes[15] = {idx};
  data.iov_len = 15;
  data.iov_base = data_bytes;
  rc = mdbx_put(txn, dbi, &key, &data, 0);
  if (rc != MDBX_SUCCESS) {
    fprintf(stderr, "mdbx_put: (%d) %s\n", rc, mdbx_strerror(rc));
    fprintf(stderr, "expected failure\n");
    exit(EXIT_FAILURE);
  }

  rc = mdbx_txn_commit(txn);
  if (rc) {
    fprintf(stderr, "mdbx_txn_commit: (%d) %s\n", rc, mdbx_strerror(rc));
    exit(EXIT_FAILURE);
  }
}
