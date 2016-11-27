/*
 * Copyright (c) 2015,2016 Leonid Yuriev <leo@yuriev.ru>.
 * Copyright (c) 2015,2016 Peter-Service R&D LLC.
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

/* Based on mtest2.c - memory-mapped database tester/toy */

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <errno.h>
#include <sys/stat.h>
#include "mdbx.h"

#define E(expr) CHECK((rc = (expr)) == MDB_SUCCESS, #expr)
#define RES(err, expr) ((rc = expr) == (err) || (CHECK(!rc, #expr), 0))
#define CHECK(test, msg) ((test) ? (void)0 : ((void)fprintf(stderr, \
	"%s:%d: %s: %s\n", __FILE__, __LINE__, msg, mdb_strerror(rc)), abort()))

#ifndef DBPATH
#	define DBPATH "./testdb"
#endif

int main(int argc,char * argv[])
{
	int i = 0, j = 0, rc;
	MDB_env *env;
	MDB_dbi dbi;
	MDB_val key, data;
	MDB_txn *txn;
	MDB_stat mst;
	MDB_cursor *cursor;
	int count;
	int *values;
	char sval[32] = "";
	int env_oflags;
	struct stat db_stat, exe_stat;

	(void) argc;
	(void) argv;
	srand(time(NULL));

	count = (rand()%384) + 64;
	values = (int *)malloc(count*sizeof(int));

	for(i = 0;i<count;i++) {
		values[i] = rand()%1024;
	}

	E(mdb_env_create(&env));
	E(mdb_env_set_maxreaders(env, 1));
	E(mdb_env_set_mapsize(env, 10485760));
	E(mdb_env_set_maxdbs(env, 4));

	E(stat("/proc/self/exe", &exe_stat)?errno:0);
	E(stat(DBPATH "/.", &db_stat)?errno:0);
	env_oflags = MDB_FIXEDMAP | MDB_NOSYNC;
	if (major(db_stat.st_dev) != major(exe_stat.st_dev)) {
		/* LY: Assume running inside a CI-environment:
		 *  1) don't use FIXEDMAP to avoid EBUSY in case collision,
		 *     which could be inspired by address space randomisation feature.
		 *  2) drop MDB_NOSYNC expecting that DBPATH is at a tmpfs or some dedicated storage.
		 */
		env_oflags = 0;
	}
	/* LY: especially here we always needs MDB_NOSYNC
	 * for testing mdbx_env_close_ex() and "redo-to-steady" on open. */
	env_oflags |= MDB_NOSYNC;
	E(mdb_env_open(env, DBPATH, env_oflags, 0664));

	E(mdb_txn_begin(env, NULL, 0, &txn));
	if (mdb_dbi_open(txn, "id1", MDB_CREATE, &dbi) == MDB_SUCCESS)
		E(mdb_drop(txn, dbi, 1));
	E(mdb_dbi_open(txn, "id1", MDB_CREATE, &dbi));

	key.mv_size = sizeof(int);
	key.mv_data = sval;
	data.mv_size = sizeof(sval);
	data.mv_data = sval;

	printf("Adding %d values\n", count);
	for (i=0;i<count;i++) {
		sprintf(sval, "%03x %d foo bar", values[i], values[i]);
		if (RES(MDB_KEYEXIST, mdb_put(txn, dbi, &key, &data, MDB_NOOVERWRITE)))
			j++;
	}
	if (j) printf("%d duplicates skipped\n", j);
	E(mdb_txn_commit(txn));
	E(mdb_env_stat(env, &mst));

	printf("check-preset-a\n");
	E(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
	E(mdb_cursor_open(txn, dbi, &cursor));
	int present_a = 0;
	while ((rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT)) == 0) {
		printf("key: %p %.*s, data: %p %.*s\n",
			key.mv_data,  (int) key.mv_size,  (char *) key.mv_data,
			data.mv_data, (int) data.mv_size, (char *) data.mv_data);
		++present_a;
	}
	CHECK(rc == MDB_NOTFOUND, "mdb_cursor_get");
	CHECK(present_a == count - j, "mismatch");
	mdb_cursor_close(cursor);
	mdb_txn_abort(txn);
	mdb_env_sync(env, 1);

	int deleted = 0;
	key.mv_data = sval;
	for (i = count - 1; i > -1; i -= (rand()%5)) {
		txn=NULL;
		E(mdb_txn_begin(env, NULL, 0, &txn));
		sprintf(sval, "%03x ", values[i]);
		if (RES(MDB_NOTFOUND, mdb_del(txn, dbi, &key, NULL))) {
			mdb_txn_abort(txn);
		} else {
			E(mdb_txn_commit(txn));
			deleted++;
		}
	}
	free(values);
	printf("Deleted %d values\n", deleted);

	printf("check-preset-b.cursor-next\n");
	E(mdb_env_stat(env, &mst));
	E(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
	E(mdb_cursor_open(txn, dbi, &cursor));
	int present_b = 0;
	while ((rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT)) == 0) {
		printf("key: %.*s, data: %.*s\n",
			(int) key.mv_size,  (char *) key.mv_data,
			(int) data.mv_size, (char *) data.mv_data);
		++present_b;
	}
	CHECK(rc == MDB_NOTFOUND, "mdb_cursor_get");
	CHECK(present_b == present_a - deleted, "mismatch");

	printf("check-preset-b.cursor-prev\n");
	j = 1;
	while ((rc = mdb_cursor_get(cursor, &key, &data, MDB_PREV)) == 0) {
		printf("key: %.*s, data: %.*s\n",
			(int) key.mv_size,  (char *) key.mv_data,
			(int) data.mv_size, (char *) data.mv_data);
		++j;
	}
	CHECK(rc == MDB_NOTFOUND, "mdb_cursor_get");
	CHECK(present_b == j, "mismatch");
	mdb_cursor_close(cursor);
	mdb_txn_abort(txn);

	mdb_dbi_close(env, dbi);
	/********************* LY: kept DB dirty ****************/
	mdbx_env_close_ex(env, 1);
	E(mdb_env_create(&env));
	E(mdb_env_set_maxdbs(env, 4));
	E(mdb_env_open(env, DBPATH, env_oflags, 0664));

	printf("check-preset-c.cursor-next\n");
	E(mdb_env_stat(env, &mst));
	E(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
	E(mdb_dbi_open(txn, "id1", 0, &dbi));
	E(mdb_cursor_open(txn, dbi, &cursor));
	int present_c = 0;
	while ((rc = mdb_cursor_get(cursor, &key, &data, MDB_NEXT)) == 0) {
		printf("key: %.*s, data: %.*s\n",
			(int) key.mv_size,  (char *) key.mv_data,
			(int) data.mv_size, (char *) data.mv_data);
		++present_c;
	}
	CHECK(rc == MDB_NOTFOUND, "mdb_cursor_get");
	printf("Rolled back %d deletion(s)\n", present_c - (present_a - deleted));
	CHECK(present_c > present_a - deleted, "mismatch");

	printf("check-preset-d.cursor-prev\n");
	j = 1;
	while ((rc = mdb_cursor_get(cursor, &key, &data, MDB_PREV)) == 0) {
		printf("key: %.*s, data: %.*s\n",
			(int) key.mv_size,  (char *) key.mv_data,
			(int) data.mv_size, (char *) data.mv_data);
		++j;
	}
	CHECK(rc == MDB_NOTFOUND, "mdb_cursor_get");
	CHECK(present_c == j, "mismatch");
	mdb_cursor_close(cursor);
	mdb_txn_abort(txn);

	mdb_dbi_close(env, dbi);
	mdbx_env_close_ex(env, 0);

	return 0;
}
