/*
	Copyright (c) 2015 Leonid Yuriev <leo@yuriev.ru>.
	Copyright (c) 2015 Peter-Service R&D LLC.

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
/* mdb_chk.c - memory-mapped database check tool */
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <unistd.h>
#include <signal.h>
#include <stdarg.h>
#include <malloc.h>

#include "lmdb.h"
#include "midl.h"

typedef struct flagbit {
	int bit;
	char *name;
} flagbit;

flagbit dbflags[] = {
	{ MDB_DUPSORT, "dupsort" },
	{ MDB_INTEGERKEY, "integerkey" },
	{ MDB_REVERSEKEY, "reversekey" },
	{ MDB_DUPFIXED, "dupfixed" },
	{ MDB_REVERSEDUP, "reversedup" },
	{ MDB_INTEGERDUP, "integerdup" },
	{ 0, NULL }
};

static volatile sig_atomic_t gotsignal;

static void signal_hanlder( int sig )
{
	gotsignal = 1;
}

#define MAX_DBI 32768

const char* dbi_names[MAX_DBI] = { "@gc" };
size_t dbi_pages[MAX_DBI];
short *pagemap;

MDB_env *env;
MDB_txn *txn;
MDB_envinfo info;
MDB_stat stat;
size_t maxkeysize, reclaimable_pages, freedb_pages, lastpgno;
unsigned userdb_count;
unsigned verbose = 1, quiet;
size_t pgcount;

static void print(const char* msg, ...) {
	if (! quiet) {
		va_list args;

		fflush(stderr);
		va_start(args, msg);
		vfprintf(stdout, msg, args);
		va_end(args);
	}
}

static void error(const char* msg, ...) {
	if (! quiet) {
		va_list args;

		fflush(stdout);
		va_start(args, msg);
		vfprintf(stderr, msg, args);
		va_end(args);
	}
}

struct problem {
	struct problem* pr_next;
	size_t count;
	const char* caption;
};

struct problem* problems_list;
size_t total_problems;

static int pagemap_lookup_dbi(const char* dbi) {
	static int last;

	if (last > 0 && strcmp(dbi_names[last], dbi) == 0)
		return last;

	for(last = 1; dbi_names[last] && last < MAX_DBI; ++last)
		if (strcmp(dbi_names[last], dbi) == 0)
			return last;

	if (last == MAX_DBI)
		return last = -1;

	dbi_names[last] = strdup(dbi);
	return last;
}

static void problem_add(size_t entry_number, const char* msg, const char *extra, ...) {
	total_problems++;

	if (! quiet) {
		struct problem* p;

		for (p = problems_list; p; p = p->pr_next)
			if (p->caption == msg)
				break;

		if (! p) {
			p = calloc(1, sizeof(*p));
			p->caption = msg;
			p->pr_next = problems_list;
			problems_list = p;
		}

		p->count++;
		if (verbose > 1) {
			print(" - entry #%zu: %s", entry_number, msg);
			if (extra) {
				va_list args;
				va_start(args, extra);
				vfprintf(stdout, extra, args);
				va_end(args);
			}
			printf("\n");
		}
	}
}

static struct problem* problems_push() {
	struct problem* p = problems_list;
	problems_list = NULL;
	return p;
}

static size_t problems_pop(struct problem* list) {
	size_t total = 0;

	if (problems_list) {
		int i;

		print(" - problems: ");
		for (i = 0; problems_list; ++i) {
			struct problem* p = problems_list->pr_next;
			total += problems_list->count;
			print("%s%s (%zu)", i ? ", " : "", problems_list->caption, problems_list->count);
			free(problems_list);
			problems_list = p;
		}
		print("\n");
	}

	problems_list = list;
	return total;
}

static int pgvisitor(size_t pgno, unsigned pgnumber, void* ctx, const char* dbi, char type)
{
	if (pgnumber) {
		pgcount += pgnumber;

		int index = pagemap_lookup_dbi(dbi);
		if (index < 0)
			return ENOMEM;

		do {
			if (pgno >= lastpgno)
				problem_add(pgno, "wrong page-no", "(> %zi)", lastpgno);
			else if (pagemap[pgno])
				problem_add(pgno, "page already used", "(in %s)", dbi_names[pagemap[pgno]]);
			else {
				pagemap[pgno] = index;
				dbi_pages[index] += 1;
			}
			++pgno;
		} while(--pgnumber);
	}

	return MDB_SUCCESS;
}

typedef long (visitor)(size_t record_number, MDB_val *key, MDB_val* data);
static long process_db(MDB_dbi dbi, char *name, visitor *handler, int silent);

static long handle_userdb(size_t record_number, MDB_val *key, MDB_val* data) {
	return MDB_SUCCESS;
}

static long handle_freedb(size_t record_number, MDB_val *key, MDB_val* data) {
	char *bad = "";
	size_t pg, prev;
	ssize_t i, number, span = 0;
	size_t *iptr = data->mv_data, txnid = *(size_t*)key->mv_data;
	long problem_count = 0;

	if (key->mv_size != sizeof(txnid))
		problem_add(record_number, "wrong txn-id size", "(key-size %zi)", key->mv_size);
	else if (txnid < 1 || txnid > info.me_last_txnid)
		problem_add(record_number, "wrong txn-id", "(%zu)", txnid);

	if (data->mv_size < sizeof(size_t) || data->mv_size % sizeof(size_t))
		problem_add(record_number, "wrong idl size", "(%zu)", data->mv_size);
	else {
		number = *iptr++;
		if (number <= 0 || number >= MDB_IDL_UM_MAX)
			problem_add(record_number, "wrong idl length", "(%zi)", number);
		else if ((number + 1) * sizeof(size_t) != data->mv_size)
			problem_add(record_number, "mismatch idl length", "(%zi != %zu)",
						number * sizeof(size_t), data->mv_size);
		else {
			freedb_pages  += number;
			if (info.me_tail_txnid > txnid)
				reclaimable_pages += number;
			for (i = number, prev = 1; --i >= 0; ) {
				pg = iptr[i];
				if (pg < 2 /* META_PAGE */ || pg > info.me_last_pgno)
					problem_add(record_number, "wrong idl entry", "(2 < %zi < %zi)",
								pg, info.me_last_pgno);
				if (pg <= prev) {
					bad = " [bad sequence]";
					++problem_count;
				}
				prev = pg;
				pg += span;
				for (; i >= span && iptr[i - span] == pg; span++, pg++) ;
			}
			if (verbose > 2)
				print(" - transaction %zu, %zd pages, maxspan %zd%s\n",
					*(size_t *)key->mv_data, number, span, bad);
			if (verbose > 3) {
				int j = number - 1;
				while (j >= 0) {
					pg = iptr[j];
					for (span = 1; --j >= 0 && iptr[j] == pg + span; span++) ;
					print((span > 1) ? "    %9zu[%zd]\n" : "    %9zu\n", pg, span);
				}
			}
		}
	}

	return problem_count;
}

static long handle_maindb(size_t record_number, MDB_val *key, MDB_val* data) {
	char *name;
	int i;
	long rc;

	name = key->mv_data;
	for(i = 0; i < key->mv_size; ++i) {
		if (name[i] < ' ')
			return handle_userdb(record_number, key, data);
	}

	name = malloc(key->mv_size + 1);
	memcpy(name, key->mv_data, key->mv_size);
	name[key->mv_size] = '\0';
	userdb_count++;

	rc = process_db(-1, name, handle_userdb, 0);
	free(name);
	if (rc != MDB_BAD_DBI)
		return rc;

	return handle_userdb(record_number, key, data);
}

static long process_db(MDB_dbi dbi, char *name, visitor *handler, int silent)
{
	MDB_cursor *mc;
	MDB_stat ms;
	MDB_val key, data;
	MDB_val prev_key, prev_data;
	unsigned flags;
	int rc, i;
	struct problem* saved_list;
	size_t problems_count;

	unsigned record_count = 0, dups = 0;
	size_t key_bytes = 0, data_bytes = 0;

	if (0 > (int) dbi) {
		rc = mdb_dbi_open(txn, name, 0, &dbi);
		if (rc) {
			if (!name || rc != MDB_BAD_DBI) /* LY: mainDB's record is not a user's DB. */ {
				error(" - mdb_open '%s' failed, error %d %s\n",
					  name ? name : "main", rc, mdb_strerror(rc));
			}
			return rc;
		}
	}

	if (! silent && verbose)
		print("Processing %s'db...\n", name ? name : "main");

	rc = mdb_dbi_flags(txn, dbi, &flags);
	if (rc) {
		error(" - mdb_dbi_flags failed, error %d %s\n", rc, mdb_strerror(rc));
		mdb_dbi_close(env, dbi);
		return rc;
	}

	rc = mdb_stat(txn, dbi, &ms);
	if (rc) {
		error(" - mdb_stat failed, error %d %s\n", rc, mdb_strerror(rc));
		mdb_dbi_close(env, dbi);
		return rc;
	}

	if (! silent && verbose) {
		print(" - flags:");
		if (! flags)
			print(" none");
		else {
			if (flags & MDB_DUPSORT)
				print(" duplicates");
			for (i=0; dbflags[i].bit; i++)
				if (flags & dbflags[i].bit)
					print(" %s", dbflags[i].name);
		}
		print(" (0x%x)\n", flags);
		print(" - entries %zu\n", ms.ms_psize, ms.ms_entries);
		print(" - b-tree depth %u, pages: branch %zu, leaf %zu, overflow %zu\n",
			  ms.ms_depth, ms.ms_branch_pages, ms.ms_leaf_pages, ms.ms_overflow_pages);
	}

	rc = mdb_cursor_open(txn, dbi, &mc);
	if (rc) {
		error(" - mdb_cursor_open failed, error %d %s\n", rc, mdb_strerror(rc));
		mdb_dbi_close(env, dbi);
		return rc;
	}

	saved_list = problems_push();
	prev_key.mv_data = NULL;
	prev_data.mv_size = 0;
	rc = mdb_cursor_get(mc, &key, &data, MDB_FIRST);
	while (rc == MDB_SUCCESS) {
		if (key.mv_size == 0) {
			problem_add(record_count, "key with zero length", NULL);
		} else if (key.mv_size > maxkeysize) {
			problem_add(record_count, "key length exceeds max-key-size",
						" (%zu > %zu)", key.mv_size, maxkeysize);
		} else if ((flags & MDB_INTEGERKEY)
			&& key.mv_size != sizeof(size_t) && key.mv_size != sizeof(int)) {
			problem_add(record_count, "wrong key length",
						" (%zu != %zu)", key.mv_size, sizeof(size_t));
		}

		if ((flags & MDB_INTEGERDUP)
			&& data.mv_size != sizeof(size_t) && data.mv_size != sizeof(int)) {
			problem_add(record_count, "wrong data length",
						" (%zu != %zu)", data.mv_size, sizeof(size_t));
		}

		if (prev_key.mv_data) {
			if ((flags & MDB_DUPFIXED) && prev_data.mv_size != data.mv_size) {
				problem_add(record_count, "different data length",
						" (%zu != %zu)", prev_data.mv_size, data.mv_size);
			}

			int cmp = mdb_cmp(txn, dbi, &prev_key, &key);
			if (cmp > 0) {
				problem_add(record_count, "broken ordering of entries", NULL);
			} else if (cmp == 0) {
				++dups;
				if (! (flags & MDB_DUPSORT))
					problem_add(record_count, "duplicated entries", NULL);
			}
		} else {
			if (flags & MDB_INTEGERKEY)
				print(" - fixed key-size %zu\n", key.mv_size );
			if (flags & (MDB_INTEGERDUP | MDB_DUPFIXED))
				print(" - fixed data-size %zu\n", data.mv_size );
		}

		rc = gotsignal ? EINTR : (handler ? handler(record_count, &key, &data) : 0);
		if (rc)
			goto bailout;

		record_count++;
		key_bytes += key.mv_size;
		data_bytes += data.mv_size;

		prev_key = key;
		prev_data = data;
		rc = mdb_cursor_get(mc, &key, &data, MDB_NEXT);
	}
	if (rc == MDB_NOTFOUND)
		rc = MDB_SUCCESS;

	if (record_count != ms.ms_entries )
		problem_add(record_count, "differentent number of entries",
				" (%zu != %zu)", record_count, ms.ms_entries);
bailout:
	problems_count = problems_pop(saved_list);
	if (! silent && verbose) {
		print(" - summary: %u entries, %u dups, %zu key's bytes, %zu data's bytes, %zu problems\n",
			  record_count, dups, key_bytes, data_bytes, problems_count);
	}

	mdb_cursor_close(mc);
	mdb_dbi_close(env, dbi);
	return rc;
}

static void usage(char *prog)
{
	fprintf(stderr, "usage: %s dbpath [-V] [-v] [-n] [-q]\n", prog);
	exit(EXIT_FAILURE);
}

const char* meta_synctype(size_t sign) {
	switch(sign) {
	case 0:
		return "legacy/unknown";
	case 1:
		return "weak";
	default:
		return "steady";
	}
}

int meta_lt(size_t txn1, size_t sign1, size_t txn2, size_t sign2) {
	return ((sign1 > 1) == (sign2 > 1)) ? txn1 < txn2 : sign2 > 1;
}

int main(int argc, char *argv[])
{
	int i, rc;
	char *prog = argv[0];
	char *envname;
	int envflags = 0;
	long problems_maindb = 0, problems_freedb = 0, problems_deep = 0;
	int problems_meta = 0;
	size_t n;

	if (argc < 2) {
		usage(prog);
	}

	/* -n: use NOSUBDIR flag on env_open
	 * -V: print version and exit
	 * (default) dump only the main DB
	 */
	while ((i = getopt(argc, argv, "Vvqn")) != EOF) {
		switch(i) {
		case 'V':
			printf("%s\n", MDB_VERSION_STRING);
			exit(0);
			break;
		case 'v':
			verbose++;
			break;
		case 'q':
			quiet = 1;
			break;
		case 'n':
			envflags |= MDB_NOSUBDIR;
			break;
		default:
			usage(prog);
		}
	}

	if (optind != argc - 1)
		usage(prog);

#ifdef SIGPIPE
	signal(SIGPIPE, signal_hanlder);
#endif
#ifdef SIGHUP
	signal(SIGHUP, signal_hanlder);
#endif
	signal(SIGINT, signal_hanlder);
	signal(SIGTERM, signal_hanlder);

	envname = argv[optind];
	print("Running mdb_chk for '%s'...\n", envname);

	rc = mdb_env_create(&env);
	if (rc) {
		error("mdb_env_create failed, error %d %s\n", rc, mdb_strerror(rc));
		return EXIT_FAILURE;
	}

	rc = mdb_env_get_maxkeysize(env);
	if (rc < 0) {
		error("mdb_env_get_maxkeysize failed, error %d %s\n", rc, mdb_strerror(rc));
		goto bailout;
	}
	maxkeysize = rc;

	mdb_env_set_maxdbs(env, 3);

	rc = mdb_env_open(env, envname, envflags | MDB_RDONLY, 0664);
	if (rc) {
		error("mdb_env_open failed, error %d %s\n", rc, mdb_strerror(rc));
		goto bailout;
	}

	rc = mdb_env_info(env, &info);
	if (rc) {
		error("mdb_env_info failed, error %d %s\n", rc, mdb_strerror(rc));
		goto bailout;
	}

	rc = mdb_env_stat(env, &stat);
	if (rc) {
		error("mdb_env_stat failed, error %d %s\n", rc, mdb_strerror(rc));
		goto bailout;
	}

	lastpgno = info.me_last_pgno + 1;
	errno = 0;
	pagemap = calloc(lastpgno, sizeof(*pagemap));
	if (! pagemap) {
		rc = errno ? errno : ENOMEM;
		error("calloc failed, error %d %s\n", rc, mdb_strerror(rc));
		goto bailout;
	}

	if (verbose) {
		double k = 1024.0;
		const char sf[] = "KMGTPEZY"; /* LY: Kilo, Mega, Giga, Tera, Peta, Exa, Zetta, Yotta! */
		for(i = 0; sf[i+1] && info.me_mapsize / k > 1000.0; ++i)
			k *= 1024;
		print(" - map size %zu (%.1f%cb)\n", info.me_mapsize,
			  info.me_mapsize / k, sf[i]);
		if (info.me_mapaddr)
			print(" - mapaddr %p\n", info.me_mapaddr);
		print(" - pagesize %u, max keysize %zu, max readers %u\n",
			  stat.ms_psize, maxkeysize, info.me_maxreaders);
		print(" - transactions: last %zu, bottom %zu, lag reading %zi\n", info.me_last_txnid,
			  info.me_tail_txnid, info.me_last_txnid - info.me_tail_txnid);

		print(" - meta-1: %s %zu, %s",
			   meta_synctype(info.me_meta1_sign), info.me_meta1_txnid,
			   meta_lt(info.me_meta1_txnid, info.me_meta1_sign,
					   info.me_meta2_txnid, info.me_meta2_sign) ? "tail" : "head");
		print(info.me_meta1_txnid > info.me_last_txnid
			  ? ", rolled-back %zu (%zu >>> %zu)\n" : "\n",
			  info.me_meta1_txnid - info.me_last_txnid,
			  info.me_meta1_txnid, info.me_last_txnid);

		print(" - meta-2: %s %zu, %s",
			   meta_synctype(info.me_meta2_sign), info.me_meta2_txnid,
			   meta_lt(info.me_meta2_txnid, info.me_meta2_sign,
					   info.me_meta1_txnid, info.me_meta1_sign) ? "tail" : "head");
		print(info.me_meta2_txnid > info.me_last_txnid
			  ? ", rolled-back %zu (%zu >>> %zu)\n" : "\n",
			  info.me_meta2_txnid - info.me_last_txnid,
			  info.me_meta2_txnid, info.me_last_txnid);
	}

	if (! meta_lt(info.me_meta1_txnid, info.me_meta1_sign,
			info.me_meta2_txnid, info.me_meta2_sign)
		&& info.me_meta1_txnid != info.me_last_txnid) {
		print(" - meta1 txn-id mismatch\n");
		++problems_meta;
	}

	if (! meta_lt(info.me_meta2_txnid, info.me_meta2_sign,
			info.me_meta1_txnid, info.me_meta1_sign)
		&& info.me_meta2_txnid != info.me_last_txnid) {
		print(" - meta2 txn-id mismatch\n");
		++problems_meta;
	}

	rc = mdb_txn_begin(env, NULL, MDB_RDONLY, &txn);
	if (rc) {
		error("mdb_txn_begin failed, error %d %s\n", rc, mdb_strerror(rc));
		goto bailout;
	}

	print("Walking b-tree...\n");
	rc = mdb_env_pgwalk(txn, pgvisitor, NULL);
	if (rc) {
		error("mdb_env_pgwalk failed, error %d %s\n", rc, mdb_strerror(rc));
		goto bailout;
	}
	for( n = 0; n < lastpgno; ++n)
		if (! pagemap[n])
			dbi_pages[0] += 1;
	if (verbose) {
		print(" - dbi pages: %zu total", pgcount);
		if (verbose > 1)
			for (i = 1; i < MAX_DBI && dbi_names[i]; ++i)
				print(", %s %zu", dbi_names[i], dbi_pages[i]);
		print(", %s %zu\n", dbi_names[0], dbi_pages[0]);
	}

	problems_maindb = process_db(-1, /* MAIN_DBI */ NULL, NULL, 0);
	problems_freedb = process_db(0 /* FREE_DBI */, "free", handle_freedb, 0);

	if (verbose) {
		size_t value = info.me_mapsize / stat.ms_psize;
		double percent = value / 100.0;
		print(" - pages info: %zu total", value);
		print(", allocated %zu (%.1f%%)", lastpgno, lastpgno / percent);

		if (verbose > 1) {
			value = info.me_mapsize / stat.ms_psize - lastpgno;
			print(", remained %zu (%.1f%%)", value, value / percent);

			value = lastpgno - freedb_pages;
			print(", used %zu (%.1f%%)", value, value / percent);

			print(", gc %zu (%.1f%%)", freedb_pages, freedb_pages / percent);

			value = freedb_pages - reclaimable_pages;
			print(", reading %zu (%.1f%%)", value, value / percent);

			print(", reclaimable %zu (%.1f%%)", reclaimable_pages, reclaimable_pages / percent);
		}

		value = info.me_mapsize / stat.ms_psize - lastpgno + reclaimable_pages;
		print(", available %zu (%.1f%%)\n", value, value / percent);
	}

	if (pgcount != lastpgno - freedb_pages) {
		error("used pages mismatch (%zu != %zu)\n", pgcount, lastpgno - freedb_pages);
		goto bailout;
	}
	if (dbi_pages[0] != freedb_pages) {
		error("gc pages mismatch (%zu != %zu)\n", dbi_pages[0], freedb_pages);
		goto bailout;
	}

	if (problems_maindb == 0 && problems_freedb == 0)
		problems_deep = process_db(-1, NULL, handle_maindb, 1);

	mdb_txn_abort(txn);

	if (! userdb_count && verbose)
		print("%s: %s does not contain multiple databases\n", prog, envname);

	if (rc)
		error("%s: %s: %s\n", prog, envname, mdb_strerror(rc));

bailout:
	mdb_env_close(env);
	free(pagemap);
	if (rc)
		return EXIT_FAILURE + 2;
	if (problems_meta || problems_maindb || problems_freedb)
		return EXIT_FAILURE + 1;
	if (problems_deep)
		return EXIT_FAILURE;
	print("No error is detected.\n");
	return EXIT_SUCCESS;
}
