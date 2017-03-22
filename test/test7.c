/*
 * Copyright 2017 Klaus Malorny <klaus.malorny@knipp.de>
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

#include <endian.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "../mdbx.h"

static const char *fileName = "/dev/shm/test.mdbx";
static const char *dbName = "test";
static long size = 1500000000;
static int recordCount = 33000000;
static int majorIdCount = 6000;
static int minorIdCount = 1000000;
static unsigned int seed = 1;
static long *majorIds;

typedef struct {
  long majorId;
  long minorId;
} KeyType;

typedef struct { long refId; } DataType;

typedef struct {
  KeyType key;
  DataType data;
} KeyDataType;

void check(const char *op, int error) {
  if (error != 0) {
    fprintf(stderr, "%s: unexpected error %d: %s\n", op, error,
            mdbx_strerror(error));
    exit(1);
  }
}

void shuffle(void *data, int recordSize, int recordCount) {
  char *ptr = (char *)data;
  char *swapBuf = malloc(recordSize);

  for (int i = recordCount - 2; i >= 0; i--) {
    int j = (int)(random() % (recordCount - i));

    if (j > 0) {
      char *ptr1 = ptr + i * recordSize;
      char *ptr2 = ptr + (i + j) * recordSize;

      memcpy(swapBuf, ptr1, recordSize);
      memcpy(ptr1, ptr2, recordSize);
      memcpy(ptr2, swapBuf, recordSize);
    }
  }

  free(swapBuf);
}

void fill(MDB_env *env, MDB_dbi dbi) {
  KeyType key;
  DataType data;

  MDB_val keyRef;
  MDB_val dataRef;
  MDB_txn *txn;

  printf("generating data\n");

  srandom(seed);

  majorIds = (long *)malloc(majorIdCount * sizeof(long));

  if (!majorIds) {
    fprintf(stderr, "out of memory\n");
    exit(1);
  }

  for (int i = 0; i < majorIdCount; i++)
    majorIds[i] = i;

  // now shuffle (for later deletion test)
  shuffle((void *)majorIds, sizeof(long), majorIdCount);

  KeyDataType *records = malloc(sizeof(KeyDataType) * recordCount);
  KeyDataType *ptr = records;
  int remaining = recordCount;
  long refId = 0;

  for (int i = 0; i < minorIdCount; i++) {
    long majorId = random() % majorIdCount;
    long minorId = i;

    int max = remaining / (minorIdCount - i + 1);
    int use;

    if (i == minorIdCount - 1 || max < 2) {
      use = max;

    } else {
      long rand1 = random() % max;
      long rand2 = random() % max;
      use = (int)((rand1 * rand2 / (max - 1))) + 1; // non-linear distribution
    }

    //    printf ("%d %d %d\n", i, max, use);

    while (use-- > 0) {
      ptr->key.majorId = majorId;
      ptr->key.minorId = minorId;
      ptr->data.refId = ++refId;
      ptr++;
      remaining--;
    }
  }

  shuffle((void *)records, sizeof(KeyDataType), recordCount);

  printf("writing data\n");

  check("txn_begin", mdbx_txn_begin(env, NULL, 0, &txn));

  ptr = records;

  for (int i = recordCount; i > 0; i--) {

    key.majorId = htobe64(ptr->key.majorId);
    key.minorId = htobe64(ptr->key.minorId);
    data.refId = htobe64(ptr->data.refId);

    keyRef.mv_size = sizeof(key);
    keyRef.mv_data = (void *)&key;
    dataRef.mv_size = sizeof(data);
    dataRef.mv_data = (void *)&data;

    check("mdbx_put", mdbx_put(txn, dbi, &keyRef, &dataRef, 0));

    ptr++;
  }

  check("txn_commit", mdbx_txn_commit(txn));

  printf("%d records written\n", recordCount);
}

void deleteRange(MDB_env *env, MDB_dbi dbi, MDB_txn *txn, KeyType *startKey,
                 KeyType *endKey, int endIsInclusive) {
  MDB_cursor *cursor;
  MDB_val curKeyRef;
  MDB_val endKeyRef;
  MDB_val curDataRef;
  (void)env;

  check("cursor_open", mdbx_cursor_open(txn, dbi, &cursor));

  curKeyRef.mv_size = sizeof(KeyType);
  curKeyRef.mv_data = (void *)startKey;
  endKeyRef.mv_size = sizeof(KeyType);
  endKeyRef.mv_data = (void *)endKey;
  curDataRef.mv_size = 0;
  curDataRef.mv_data = NULL;

  int error = mdbx_cursor_get(cursor, &curKeyRef, &curDataRef, MDB_SET_RANGE);

  while (error != MDB_NOTFOUND) {
    check("mdbx_cursor_get", error);

    int compResult = mdbx_cmp(txn, dbi, &curKeyRef, &endKeyRef);

    if (compResult > 0 || (!compResult && !endIsInclusive))
      break;

    check("mdbx_cursor_del", mdbx_cursor_del(cursor, MDB_NODUPDATA));

    error = mdbx_cursor_get(cursor, &curKeyRef, &curDataRef, MDB_NEXT);
  }

  mdbx_cursor_close(cursor);
}

void testDelete(MDB_env *env, MDB_dbi dbi) {
  MDB_txn *txn;
  KeyType startKey;
  KeyType endKey;

  printf("testing\n");

  check("txn_begin", mdbx_txn_begin(env, NULL, 0, &txn));

  long majorId;

  for (int i = 0; i < majorIdCount; i++) {
    majorId = majorIds[i];
    startKey.majorId = htobe64(majorId);
    startKey.minorId = htobe64(1);
    endKey.majorId = htobe64(majorId);
    endKey.minorId = htobe64((long)(~0UL >> 1));

    deleteRange(env, dbi, txn, &startKey, &endKey, 1);
  }

  check("txn_commit", mdbx_txn_commit(txn));
}

int main(int argc, char *argv[]) {
  MDB_env *env;
  MDB_dbi dbi;
  MDB_txn *txn;
  (void)argc;
  (void)argv;

  printf("LMDB version: %s\n", MDBX_VERSION_STRING);

  unlink(fileName);
  check("env_create", mdbx_env_create(&env));
  check("env_set_mapsize", mdbx_env_set_mapsize(env, size));
  check("env_set_maxdbs", mdbx_env_set_maxdbs(env, 2));

  check("env_open",
        mdbx_env_open(env, fileName, MDB_NOSUBDIR | MDB_WRITEMAP, 0666));

  check("txn_begin", mdbx_txn_begin(env, NULL, 0, &txn));

  check("dbi_open", mdbx_dbi_open(txn, dbName, MDB_CREATE | MDB_DUPSORT, &dbi));

  check("txn_commit", mdbx_txn_commit(txn));

  fill(env, dbi);
  testDelete(env, dbi);

  mdbx_env_close(env);

  printf("done.\n");
}
