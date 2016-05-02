/*
	Copyright (c) 2015,2016 Leonid Yuriev <leo@yuriev.ru>.
	Copyright (c) 2015,2016 Peter-Service R&D LLC.

	This file is part of ReOpenLDAP.

	ReOpenLDAP is free software; you can redistribute it and/or modify it under
	the terms of the GNU Affero General Public License as published by
	the Free Software Foundation; either version 3 of the License, or
	(at your option) any later version.

	ReOpenLDAP is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
	GNU Affero General Public License for more details.

	You should have received a copy of the GNU Affero General Public License
	along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <errno.h>

#include "mdbx.h"

#define E(expr) CHECK((rc = (expr)) == MDB_SUCCESS, #expr)
#define RES(err, expr) ((rc = expr) == (err) || (CHECK(!rc, #expr), 0))
#define CHECK(test, msg) ((test) ? (void)0 : ((void)fprintf(stderr, \
	"%s:%d: %s: %s\n", __FILE__, __LINE__, msg, mdb_strerror(rc)), abort()))

#ifndef DBPATH
#	define DBPATH "./testdb"
#endif

struct t0 {
	struct rusage ru;
	struct timespec ts;
};

void t0(struct t0 *t0)
{
	int rc;
	E(getrusage(RUSAGE_SELF, &t0->ru));
	E(clock_gettime(CLOCK_MONOTONIC_RAW, &t0->ts));
}

struct info {
	double wall_s, cpu_sys_s, cpu_user_s;
	long iops_r, iops_w, iops_pf;
};

double delta_s(const struct timeval *begin, const struct timeval *end)
{
	return end->tv_sec - begin->tv_sec
		+ (end->tv_usec - begin->tv_usec) / 1000000.0;
}

double delta2_s(const struct timespec *begin, const struct timespec *end)
{
	return end->tv_sec - begin->tv_sec
		+ (end->tv_nsec - begin->tv_nsec) / 1000000000.0;
}

void measure(const struct t0 *t0, struct info *i)
{
	struct t0 t1;
	int rc;

	E(clock_gettime(CLOCK_MONOTONIC_RAW, &t1.ts));
	E(getrusage(RUSAGE_SELF, &t1.ru));

	i->wall_s = delta2_s(&t0->ts, &t1.ts);
	i->cpu_user_s = delta_s(&t0->ru.ru_utime, &t1.ru.ru_utime);
	i->cpu_sys_s = delta_s(&t0->ru.ru_stime, &t1.ru.ru_stime);
	i->iops_r = t1.ru.ru_inblock - t0->ru.ru_inblock;
	i->iops_w = t1.ru.ru_oublock - t0->ru.ru_oublock;
	i->iops_pf = t1.ru.ru_majflt - t0->ru.ru_majflt
			 + t1.ru.ru_minflt - t0->ru.ru_minflt;
}

void print(struct info *i)
{
	printf("wall-clock %.3f, iops: %lu reads, %lu writes, %lu page-faults, "
		   "cpu: %.3f user, %.3f sys\n",
		   i->wall_s, i->iops_r, i->iops_w, i->iops_pf, i->cpu_user_s, i->cpu_sys_s);

}

static void wbench(int flags, int mb, int count, int salt)
{
	MDB_env *env;
	MDB_dbi dbi;
	MDB_txn *txn;
	MDB_val key, data;
	unsigned key_value = salt;
	char data_value[777];
	int i, rc;
	struct t0 start;
	struct info ra, rd, rs, rt;

	mkdir(DBPATH, 0755);
	unlink(DBPATH "/data.mdb");
	unlink(DBPATH "/lock.mdb");

	printf("\nProbing %d Mb, %d items, flags:", mb, count);
	if (flags & MDB_NOSYNC)
		printf(" NOSYNC");
	if (flags & MDB_NOMETASYNC)
		printf(" NOMETASYNC");
	if (flags & MDB_WRITEMAP)
		printf(" WRITEMAP");
	if (flags & MDB_MAPASYNC)
		printf(" MAPASYNC");
#if defined(MDBX_COALESCE) && defined(MDBX_LIFORECLAIM)
	if (flags & MDBX_COALESCE)
		printf(" COALESCE");
	if (flags & MDBX_LIFORECLAIM)
		printf(" LIFO");
#endif
	printf(" 0x%X\n", flags);

	E(mdb_env_create(&env));
	E(mdb_env_set_mapsize(env, (1ull << 20) * mb));
	E(mdb_env_open(env, DBPATH, flags, 0664));

	key.mv_size = sizeof(key_value);
	key.mv_data = &key_value;
	data.mv_size = sizeof(data_value);
	data.mv_data = &data_value;

	printf("\tAdding %d values...", count);
	fflush(stdout);
	key_value = salt;
	t0(&start);
	for(i = 0; i < count; ++i) {
		E(mdb_txn_begin(env, NULL, 0, &txn));
		E(mdb_dbi_open(txn, NULL, 0, &dbi));

		snprintf(data_value, sizeof(data_value), "value=%u", key_value);
		E(mdb_put(txn, dbi, &key, &data, MDB_NOOVERWRITE));
		E(mdb_txn_commit(txn));

		key_value = key_value * 1664525 + 1013904223;
	}
	measure(&start, &ra);
	print(&ra);

	printf("\tDeleting %d values...", count);
	fflush(stdout);
	key_value = salt;
	t0(&start);
	for(i = 0; i < count; ++i) {
		E(mdb_txn_begin(env, NULL, 0, &txn));
		E(mdb_dbi_open(txn, NULL, 0, &dbi));

		E(mdb_del(txn, dbi, &key, NULL));
		E(mdb_txn_commit(txn));

		key_value = key_value * 1664525 + 1013904223;
	}
	measure(&start, &rd);
	print(&rd);

	printf("\tCheckpoint...");
	fflush(stdout);
	t0(&start);
	mdb_env_sync(env, 1);
	measure(&start, &rs);
	print(&rs);

	mdb_env_close(env);
	rt.wall_s = ra.wall_s + rd.wall_s + rs.wall_s;
	rt.cpu_sys_s = ra.cpu_sys_s + rd.cpu_sys_s + rs.cpu_sys_s;
	rt.cpu_user_s = ra.cpu_user_s + rd.cpu_user_s + rs.cpu_user_s;
	rt.iops_r = ra.iops_r + rd.iops_r + rs.iops_r;
	rt.iops_w = ra.iops_w + rd.iops_w + rs.iops_w;
	rt.iops_pf = ra.iops_pf + rd.iops_pf + rs.iops_pf;
	printf("Total ");
	print(&rt);

	fprintf(stderr, "flags: ");
	if (flags & MDB_NOSYNC)
		fprintf(stderr, " NOSYNC");
	if (flags & MDB_NOMETASYNC)
		fprintf(stderr, " NOMETASYNC");
	if (flags & MDB_WRITEMAP)
		fprintf(stderr, " WRITEMAP");
	if (flags & MDB_MAPASYNC)
		fprintf(stderr, " MAPASYNC");
#if defined(MDBX_COALESCE) && defined(MDBX_LIFORECLAIM)
	if (flags & MDBX_COALESCE)
		fprintf(stderr, " COALESCE");
	if (flags & MDBX_LIFORECLAIM)
		fprintf(stderr, " LIFO");
#endif
	fprintf(stderr, "\t%.3f\t%.3f\t%.3f\t%.3f\n", rt.iops_w / 1000.0, rt.cpu_user_s, rt.cpu_sys_s, rt.wall_s);

}

int main(int argc,char * argv[])
{
	(void) argc;
	(void) argv;

#define SALT	1
#define COUNT	10000
#define SIZE	12

	printf("\nDefault 'sync' mode...");
	wbench(0, SIZE, COUNT, SALT);
#if defined(MDBX_COALESCE) && defined(MDBX_LIFORECLAIM)
//	wbench(MDBX_COALESCE, SIZE, COUNT, SALT);
	wbench(MDBX_COALESCE | MDBX_LIFORECLAIM, SIZE, COUNT, SALT);
//	wbench(MDBX_LIFORECLAIM, SIZE, COUNT, SALT);
#endif

	printf("\nno-meta-sync hack...");
	wbench(MDB_NOMETASYNC, SIZE, COUNT, SALT);
#if defined(MDBX_COALESCE) && defined(MDBX_LIFORECLAIM)
//	wbench(MDB_NOMETASYNC | MDBX_COALESCE, SIZE, COUNT, SALT);
	wbench(MDB_NOMETASYNC | MDBX_COALESCE | MDBX_LIFORECLAIM, SIZE, COUNT, SALT);
//	wbench(MDB_NOMETASYNC | MDBX_LIFORECLAIM, SIZE, COUNT, SALT);
#endif

	printf("\nno-sync...");
	wbench(MDB_NOSYNC, SIZE, COUNT, SALT);
#if defined(MDBX_COALESCE) && defined(MDBX_LIFORECLAIM)
//	wbench(MDB_NOSYNC | MDBX_COALESCE, SIZE, COUNT, SALT);
//	wbench(MDB_NOSYNC | MDBX_COALESCE | MDBX_LIFORECLAIM, SIZE, COUNT, SALT);
//	wbench(MDB_NOSYNC | MDBX_LIFORECLAIM, SIZE, COUNT, SALT);
#endif

	printf("\nr/w-map...");
	wbench(MDB_WRITEMAP, SIZE, COUNT, SALT);
#if defined(MDBX_COALESCE) && defined(MDBX_LIFORECLAIM)
//	wbench(MDB_WRITEMAP | MDBX_COALESCE, SIZE, COUNT, SALT);
	wbench(MDB_WRITEMAP | MDBX_COALESCE | MDBX_LIFORECLAIM, SIZE, COUNT, SALT);
//	wbench(MDB_WRITEMAP | MDBX_LIFORECLAIM, SIZE, COUNT, SALT);
#endif

	printf("\nasync...");
	wbench(MDB_WRITEMAP | MDB_MAPASYNC, SIZE, COUNT, SALT);
#if defined(MDBX_COALESCE) && defined(MDBX_LIFORECLAIM)
//	wbench(MDB_WRITEMAP | MDB_MAPASYNC | MDBX_COALESCE, SIZE, COUNT, SALT);
	wbench(MDB_WRITEMAP | MDB_MAPASYNC | MDBX_COALESCE | MDBX_LIFORECLAIM, SIZE, COUNT, SALT);
//	wbench(MDB_WRITEMAP | MDB_MAPASYNC | MDBX_LIFORECLAIM, SIZE, COUNT, SALT);
#endif

	printf("\nr/w-map + no-sync...");
	wbench(MDB_NOSYNC | MDB_WRITEMAP, SIZE, COUNT, SALT);
#if defined(MDBX_COALESCE) && defined(MDBX_LIFORECLAIM)
//	wbench(MDB_NOSYNC | MDB_WRITEMAP | MDBX_COALESCE, SIZE, COUNT, SALT);
	wbench(MDB_NOSYNC | MDB_WRITEMAP | MDBX_COALESCE | MDBX_LIFORECLAIM, SIZE, COUNT, SALT);
//	wbench(MDB_NOSYNC | MDB_WRITEMAP | MDBX_LIFORECLAIM, SIZE, COUNT, SALT);
#endif

	return 0;
}
