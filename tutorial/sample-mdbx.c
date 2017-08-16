/* sample-mdb.txt - MDB toy/sample
 *
 * Do a line-by-line comparison of this and sample-bdb.txt
 */

/*
 * Copyright 2017 Ilya Shipitsin <chipitsine@gmail.com>.
 * Copyright 2015-2017 Leonid Yuriev <leo@yuriev.ru>.
 * Copyright 2012-2015 Howard Chu, Symas Corp.
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

#include "mdbx.h"
#include <stdio.h>

int main(int argc, char *argv[]) {
  (void)argc;
  (void)argv;

  int rc;
  MDBX_env *env;
  MDBX_dbi dbi;
  MDBX_val key, data;
  MDBX_txn *txn;
  MDBX_cursor *cursor;
  char sval[32];

  rc = mdbx_env_create(&env);
  if (rc != MDBX_SUCCESS) {
    fprintf(stderr, "mdbx_env_create: (%d) %s\n", rc, mdbx_strerror(rc));
    return 0;
  }
  rc = mdbx_env_open(env, "./example-db",
                     MDBX_NOSUBDIR | MDBX_COALESCE | MDBX_LIFORECLAIM, 0664);
  if (rc != MDBX_SUCCESS) {
    mdbx_env_close(env);
    fprintf(stderr, "mdbx_env_open: (%d) %s\n", rc, mdbx_strerror(rc));
    return 0;
  }

  rc = mdbx_txn_begin(env, NULL, 0, &txn);
  if (rc != MDBX_SUCCESS) {
    fprintf(stderr, "mdbx_txn_begin: (%d) %s\n", rc, mdbx_strerror(rc));
    goto leave;
  }
  rc = mdbx_dbi_open(txn, NULL, 0, &dbi);
  if (rc != MDBX_SUCCESS) {
    fprintf(stderr, "mdbx_dbi_open: (%d) %s\n", rc, mdbx_strerror(rc));
    goto leave;
  }

  key.iov_len = sizeof(int);
  key.iov_base = sval;
  data.iov_len = sizeof(sval);
  data.iov_base = sval;

  sprintf(sval, "%03x %d foo bar", 32, 3141592);
  rc = mdbx_put(txn, dbi, &key, &data, 0);
  if (rc != MDBX_SUCCESS) {
    fprintf(stderr, "mdbx_put: (%d) %s\n", rc, mdbx_strerror(rc));
    goto leave;
  }
  rc = mdbx_txn_commit(txn);
  if (rc) {
    fprintf(stderr, "mdbx_txn_commit: (%d) %s\n", rc, mdbx_strerror(rc));
    goto leave;
  }
  rc = mdbx_txn_begin(env, NULL, MDBX_RDONLY, &txn);
  rc = mdbx_cursor_open(txn, dbi, &cursor);
  while ((rc = mdbx_cursor_get(cursor, &key, &data, MDBX_NEXT)) == 0) {
    printf("key: %p %.*s, data: %p %.*s\n", key.iov_base, (int)key.iov_len,
           (char *)key.iov_base, data.iov_base, (int)data.iov_len,
           (char *)data.iov_base);
  }
  mdbx_cursor_close(cursor);
  mdbx_txn_abort(txn);
leave:
  mdbx_dbi_close(env, dbi);
  mdbx_env_close(env);
  return 0;
}
