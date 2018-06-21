/* mtest7.c - memory-mapped database tester/toy */
/*
 * Copyright 2015 Ilya Usvyatsky, Nexenta Corp.
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

/* Tests for DB attributes */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <sys/sysmacros.h>
#include "mdbx.h"

#define E(expr) CHECK((rc = (expr)) == MDB_SUCCESS, #expr)
#define RES(err, expr) ((rc = expr) == (err) || (CHECK(!rc, #expr), 0))
#define CHECK(test, msg) ((test) ? (void)0 : ((void)fprintf(stderr, \
	"%s:%d: %s: %s\n", __FILE__, __LINE__, msg, mdb_strerror(rc)), abort()))

char dkbuf[1024];

#ifndef DBPATH
#	define DBPATH "./testdb/data.mdb"
#endif

int main(int argc,char * argv[])
{
	int i = 0, j = 0, rc;
	MDB_env *env;
	MDB_dbi dbi;
	MDB_val key, data;
	MDB_txn *txn;
	MDB_stat mst;
	int count;
	int *values;
	char sval[32];
	uint64_t *timestamps, timestamp;
	struct timeval tv;
	int env_opt = MDB_NOMEMINIT | MDB_NOSYNC | MDB_NOSUBDIR | MDB_NORDAHEAD;

	srand(time(NULL));

	memset(sval, 0, sizeof(sval));
	count = (rand()%384) + 64;
	if (argc > 1)
		count = atoi(argv[1]);
	values = (int *)malloc(count*sizeof(int));
	timestamps = (uint64_t *)calloc(count,sizeof(uint64_t));

	unlink(DBPATH);
	E(mdb_env_create(&env));
	E(mdb_env_set_mapsize(env, 104857600));
	E(mdb_env_set_maxdbs(env, 8));
	E(mdb_env_open(env, DBPATH, env_opt, 0664));

	E(mdb_txn_begin(env, NULL, 0, &txn));
	E(mdb_dbi_open(txn, "id7", MDB_CREATE|MDB_INTEGERKEY, &dbi));

	key.mv_size = sizeof(int);
	data.mv_size = sizeof(sval);
	data.mv_data = sval;

	printf("Adding %d values\n", count);
	for (i=0;i<count;i++) {
		(void)gettimeofday(&tv, NULL);
		timestamps[i] = tv.tv_usec + 1000000UL * tv.tv_sec;
		values[i] = rand()%16383 ^ (timestamps[i] & 0xffff);
		key.mv_data = values + i;
		sprintf(sval, "%03x %d foo bar", values[i], values[i]);
		E(mdbx_put_attr(txn, dbi, &key, &data, timestamps[i], MDB_NODUPDATA));
	}
	if (j) printf("%d duplicates skipped\n", j);
	E(mdb_txn_commit(txn));
	E(mdb_env_stat(env, &mst));
	mdb_env_close(env);

	E(mdb_env_create(&env));
	E(mdb_env_set_mapsize(env, 10485760));
	E(mdb_env_set_maxdbs(env, 8));
	E(mdb_env_open(env, DBPATH, env_opt, 0664));

	E(mdb_txn_begin(env, NULL, 0, &txn));
	E(mdb_dbi_open(txn, "id7", MDB_CREATE|MDB_INTEGERKEY, &dbi));
	for (i=0;!rc&&i<count;i++) {
		if (!timestamps[i])
			continue;
		key.mv_data = values + i;
		sprintf(sval, "%03x %d foo bar", values[i], values[i]);
		E(mdbx_get_attr(txn, dbi, &key, &data, &timestamp));
		if (timestamps[i] != timestamp) {
			for (j = 0; j < count; ++j) {
				if (j != i && values[i] == values[j] &&
					timestamp == timestamps[j]) {
					printf("Duplicate keys "
						"%d %d %d %d %lu %lu\n",
						i, j, values[i], values[j],
						timestamps[i], timestamps[j]);
					break;
				}
			}
			if (j >= count) {
				printf("Timestamp mismatch "
					"%d %03x %d %lu != %lu\n",
					i, values[i], values[i], timestamps[i],
					timestamp);
				break;
			}
		}
	}

	E(mdb_txn_commit(txn));
	E(mdb_env_stat(env, &mst));
	mdb_env_close(env);

	return 0;
}
