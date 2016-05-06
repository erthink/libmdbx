#include <sys/time.h>
#include <sys/stat.h>

#include <limits.h>
#include <stddef.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <assert.h>
#include "mdbx.h"

#define IP_PRINTF_ARG_HOST(addr) (int)((addr) >> 24), (int)((addr) >> 16 & 0xff), (int)((addr) >> 8 & 0xff), (int)((addr) & 0xff)

char opt_db_path[PATH_MAX] = "/dev/shm/lmdb_bench2";
static MDB_env *env;
#define REC_COUNT 1024000
int64_t ids[REC_COUNT * 10];
int32_t ids_count = 0;

int64_t lmdb_add = 0;
int64_t lmdb_del = 0;
int64_t obj_id = 0;
int64_t lmdb_data_size = 0;
int64_t lmdb_key_size = 0;

static void add_id_to_pool(int64_t id) {
	ids[ids_count] = id;
	ids_count++;
}

static inline int64_t getTimeMicroseconds(void) {
	struct timeval val;
	gettimeofday(&val, NULL);
	return val.tv_sec * ((int64_t) 1000000) + val.tv_usec;
}

static int64_t get_id_from_pool() {
	if (ids_count == 0) {
		return -1;
	}
	int32_t index = rand() % ids_count;
	int64_t id = ids[index];
	ids[index] = ids[ids_count - 1];
	ids_count--;
	return id;
}

#define LMDB_CHECK(x) \
	do {\
		const int rc = (x);\
		if ( rc != MDB_SUCCESS ) {\
			printf("Error [%d] %s in %s at %s:%d\n", rc, mdb_strerror(rc), #x, __FILE__, __LINE__); \
			exit(EXIT_FAILURE); \
		}\
	} while(0)

static void db_connect() {
	MDB_dbi dbi_session;
	MDB_dbi dbi_session_id;
	MDB_dbi dbi_event;
	MDB_dbi dbi_ip;

	LMDB_CHECK(mdb_env_create(&env));
	LMDB_CHECK(mdb_env_set_mapsize(env, 300000L * 4096L));
	LMDB_CHECK(mdb_env_set_maxdbs(env, 30));
#if defined(MDBX_LIFORECLAIM)
	LMDB_CHECK(mdb_env_open(env, opt_db_path, MDB_CREATE | MDB_NOSYNC | MDB_WRITEMAP | MDBX_LIFORECLAIM, 0664));
#else
	LMDB_CHECK(mdb_env_open(env, opt_db_path, MDB_CREATE | MDB_NOSYNC | MDB_WRITEMAP, 0664));
#endif
	MDB_txn *txn;

	// transaction init
	LMDB_CHECK(mdb_txn_begin(env, NULL, 0, &txn));
	// open database in read-write mode
	LMDB_CHECK(mdb_dbi_open(txn, "session", MDB_CREATE, &dbi_session));
	LMDB_CHECK(mdb_dbi_open(txn, "session_id", MDB_CREATE, &dbi_session_id));
	LMDB_CHECK(mdb_dbi_open(txn, "event", MDB_CREATE, &dbi_event));
	LMDB_CHECK(mdb_dbi_open(txn, "ip", MDB_CREATE, &dbi_ip));
	// transaction commit
	LMDB_CHECK(mdb_txn_commit(txn));
	printf("Connection open\n");
}

typedef struct {
	char session_id1[100];
	char session_id2[100];
	char ip[20];
	uint8_t fill[100];
} session_data_t;

typedef struct {
	int64_t obj_id;
	int8_t event_type;
} __attribute__((__packed__)) event_data_t;

static void create_record(int64_t record_id) {
	MDB_dbi dbi_session;
	MDB_dbi dbi_session_id;
	MDB_dbi dbi_event;
	MDB_dbi dbi_ip;
	event_data_t event;
	MDB_txn *txn;
	session_data_t data;
	// transaction init
	snprintf(data.session_id1, sizeof (data.session_id1), "mskugw%02ld_%02ld.gx.yota.ru;3800464060;4152;%ld", record_id % 3 + 1, record_id % 9 + 1, record_id);
	snprintf(data.session_id2, sizeof (data.session_id2), "gx_service;%ld;%ld;node@spb-jsm1", record_id, record_id % 1000000000 + 99999);
	snprintf(data.ip, sizeof (data.ip), "%d.%d.%d.%d", IP_PRINTF_ARG_HOST(record_id & 0xFFFFFFFF));
	event.obj_id = record_id;
	event.event_type = 1;

	MDB_val _session_id1_rec = {data.session_id1, strlen(data.session_id1)};
	MDB_val _session_id2_rec = {data.session_id2, strlen(data.session_id2)};
	MDB_val _ip_rec = {data.ip, strlen(data.ip)};
	MDB_val _obj_id_rec = {&record_id, sizeof(record_id)};
	MDB_val _data_rec = {&data, offsetof(session_data_t, fill) + (rand() % sizeof (data.fill))};
	MDB_val _event_rec = {&event, sizeof(event)};

	LMDB_CHECK(mdb_txn_begin(env, NULL, 0, &txn));
	LMDB_CHECK(mdb_dbi_open(txn, "session", MDB_CREATE, &dbi_session));
	LMDB_CHECK(mdb_dbi_open(txn, "session_id", MDB_CREATE, &dbi_session_id));
	LMDB_CHECK(mdb_dbi_open(txn, "event", MDB_CREATE, &dbi_event));
	LMDB_CHECK(mdb_dbi_open(txn, "ip", MDB_CREATE, &dbi_ip));
	LMDB_CHECK(mdb_put(txn, dbi_session, &_obj_id_rec, &_data_rec, MDB_NOOVERWRITE | MDB_NODUPDATA));
	LMDB_CHECK(mdb_put(txn, dbi_session_id, &_session_id1_rec, &_obj_id_rec, MDB_NOOVERWRITE | MDB_NODUPDATA));
	LMDB_CHECK(mdb_put(txn, dbi_session_id, &_session_id2_rec, &_obj_id_rec, MDB_NOOVERWRITE | MDB_NODUPDATA));
	LMDB_CHECK(mdb_put(txn, dbi_ip, &_ip_rec, &_obj_id_rec, 0));
	LMDB_CHECK(mdb_put(txn, dbi_event, &_event_rec, &_obj_id_rec, 0));
	lmdb_data_size += (_data_rec.mv_size + _obj_id_rec.mv_size * 4);
	lmdb_key_size += (_obj_id_rec.mv_size + _session_id1_rec.mv_size + _session_id2_rec.mv_size + _ip_rec.mv_size + _event_rec.mv_size);

	// transaction commit
	LMDB_CHECK(mdb_txn_commit(txn));
	lmdb_add++;
}

static void delete_record(int64_t record_id) {
	MDB_dbi dbi_session;
	MDB_dbi dbi_session_id;
	MDB_dbi dbi_event;
	MDB_dbi dbi_ip;
	event_data_t event;
	MDB_txn *txn;

	// transaction init
	LMDB_CHECK(mdb_txn_begin(env, NULL, 0, &txn));
	// open database in read-write mode
	LMDB_CHECK(mdb_dbi_open(txn, "session", MDB_CREATE, &dbi_session));
	LMDB_CHECK(mdb_dbi_open(txn, "session_id", MDB_CREATE, &dbi_session_id));
	LMDB_CHECK(mdb_dbi_open(txn, "event", MDB_CREATE, &dbi_event));
	LMDB_CHECK(mdb_dbi_open(txn, "ip", MDB_CREATE, &dbi_ip));
	// put data
	MDB_val _obj_id_rec = {&record_id, sizeof(record_id)};
	MDB_val _data_rec;
	// get data
	LMDB_CHECK(mdb_get(txn, dbi_session, &_obj_id_rec, &_data_rec));
	session_data_t* data = (session_data_t*) _data_rec.mv_data;

	MDB_val _session_id1_rec = {data->session_id1, strlen(data->session_id1)};
	MDB_val _session_id2_rec = {data->session_id2, strlen(data->session_id2)};
	MDB_val _ip_rec = {data->ip, strlen(data->ip)};
	LMDB_CHECK(mdb_del(txn, dbi_session_id, &_session_id1_rec, NULL));
	LMDB_CHECK(mdb_del(txn, dbi_session_id, &_session_id2_rec, NULL));
	LMDB_CHECK(mdb_del(txn, dbi_ip, &_ip_rec, NULL));
	event.obj_id = record_id;
	event.event_type = 1;
	MDB_val _event_rec = {&event, sizeof(event)};
	LMDB_CHECK(mdb_del(txn, dbi_event, &_event_rec, NULL));
	LMDB_CHECK(mdb_del(txn, dbi_session, &_obj_id_rec, NULL));

	lmdb_data_size -= (_data_rec.mv_size + _obj_id_rec.mv_size * 4);
	lmdb_key_size -= (_obj_id_rec.mv_size + _session_id1_rec.mv_size + _session_id2_rec.mv_size + _ip_rec.mv_size + _event_rec.mv_size);

	// transaction commit
	LMDB_CHECK(mdb_txn_commit(txn));
	lmdb_del++;
}

static void db_disconnect() {
	mdb_env_close(env);
	printf("Connection closed\n");
}

static void get_db_stat(const char* db, int64_t* ms_branch_pages, int64_t* ms_leaf_pages) {
	MDB_txn *txn;
	MDB_stat stat;
	MDB_dbi dbi;

	LMDB_CHECK(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
	LMDB_CHECK(mdb_dbi_open(txn, db, MDB_CREATE, &dbi));
	LMDB_CHECK(mdb_stat(txn, dbi, &stat));
	mdb_txn_abort(txn);
	printf("%15s | %15ld | %5u | %10ld | %10ld | %11ld |\n",
			db,
			stat.ms_branch_pages,
			stat.ms_depth,
			stat.ms_entries,
			stat.ms_leaf_pages,
			stat.ms_overflow_pages);
	(*ms_branch_pages) += stat.ms_branch_pages;
	(*ms_leaf_pages) += stat.ms_leaf_pages;
}

static void periodic_stat(void) {
	int64_t ms_branch_pages = 0;
	int64_t ms_leaf_pages = 0;
	printf("           Name | ms_branch_pages | depth |    entries | leaf_pages | overf_pages |\n");
	get_db_stat("session", &ms_branch_pages, &ms_leaf_pages);
	get_db_stat("session_id", &ms_branch_pages, &ms_leaf_pages);
	get_db_stat("event", &ms_branch_pages, &ms_leaf_pages);
	get_db_stat("ip", &ms_branch_pages, &ms_leaf_pages);
	printf("%15s | %15ld | %5s | %10s | %10ld | %11s |\n", "", ms_branch_pages, "", "", ms_leaf_pages, "");
	static int64_t prev_add;
	static int64_t prev_del;
	static int64_t t = -1;
	if (t > 0) {
		int64_t delta = getTimeMicroseconds() - t;
		printf("CPS: add %ld, delete %ld, items processed - %ldK data=%ldK key=%ldK\n", (lmdb_add - prev_add)*1000000 / delta, (lmdb_del - prev_del)*1000000 / delta, obj_id / 1024, lmdb_data_size / 1024, lmdb_key_size / 1024);
		printf("usage data=%ld%%\n", ((lmdb_data_size + lmdb_key_size) * 100) / ((ms_leaf_pages + ms_branch_pages)*4096));
	}
	t = getTimeMicroseconds();
	prev_add = lmdb_add;
	prev_del = lmdb_del;
}

//static void periodic_add_rec() {
//	for (int i = 0; i < 10240; i++) {
//		if (ids_count <= REC_COUNT) {
//			int64_t id = obj_id++;
//			create_record(id);
//			add_id_to_pool(id);
//		}
//		if (ids_count > REC_COUNT) {
//			int64_t id = get_id_from_pool();
//			delete_record(id);
//		}
//	}
//	periodic_stat();
//}

int main(int argc, char** argv) {
	(void) argc;
	(void) argv;

	char filename[PATH_MAX];
	int i;
	int64_t t;

	mkdir(opt_db_path, 0775);

	strcpy(filename, opt_db_path);
	strcat(filename, "/data.mdb");
	remove(filename);

	strcpy(filename, opt_db_path);
	strcat(filename, "/lock.mdb");
	remove(filename);

	db_connect();
	periodic_stat();
	for (i = 0; i < 1024000; i++) {
		int64_t id = obj_id++;
		create_record(id);
		add_id_to_pool(id);
	}
	periodic_stat();
	t = getTimeMicroseconds();
	while (1) {
		int i;
		int64_t now;
		for (i = 0; i < 100; i++) {
			int64_t id = obj_id++;
			create_record(id);
			add_id_to_pool(id);
			id = get_id_from_pool();
			delete_record(id);
		}
		//int64_t id = obj_id++;
		//create_record(id);
		//add_id_to_pool(id);
		now = getTimeMicroseconds();
		if ((now - t) > 100000) {
			periodic_stat();
			t = now;
		}
	}
	db_disconnect();
	return 0;
}
