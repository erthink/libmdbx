//
//  libmdbx/test/extra/upsert_alldups.c
//
//  Created by Masatoshi Fukunaga <https://gitflic.ru/user/mah0x211>
//  on 2023-01-31.
//

#include "mdbx.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int dump(MDBX_cursor *cur) {
  MDBX_val key = {NULL, 0};
  MDBX_val data = {NULL, 0};
  int rc = mdbx_cursor_get(cur, &key, &data, MDBX_FIRST);

  while (rc == 0) {
    printf("(%.*s) = (%.*s)\n", (int)key.iov_len, (const char *)key.iov_base,
           (int)data.iov_len, (const char *)data.iov_base);
    rc = mdbx_cursor_get(cur, &key, &data, MDBX_NEXT);
  }
  return rc;
}

static int clear(MDBX_cursor *cur) {
  MDBX_val key = {NULL, 0};
  MDBX_val data = {NULL, 0};
  int rc = mdbx_cursor_get(cur, &key, &data, MDBX_FIRST);

  while (rc == 0) {
    rc = mdbx_cursor_del(cur, MDBX_ALLDUPS);
    if (rc)
      return rc;
    rc = mdbx_cursor_get(cur, &key, &data, MDBX_NEXT);
  }
  return (rc == MDBX_NOTFOUND) ? 0 : rc;
}

static int put(MDBX_txn *txn, MDBX_dbi dbi, const char *k, const char *v,
               MDBX_put_flags_t flags) {
  MDBX_val key = {.iov_base = (void *)k, .iov_len = strlen(k)};
  MDBX_val data = {.iov_base = (void *)v, .iov_len = strlen(v)};
  return mdbx_put(txn, dbi, &key, &data, flags);
}

int main(int argc, const char *argv[]) {
  (void)argc;
  (void)argv;
  char *errmsg = NULL;
  MDBX_env *env = NULL;
  MDBX_txn *txn = NULL;
  MDBX_cursor *cur = NULL;
  MDBX_dbi dbi = 0;

  unlink("." MDBX_DATANAME);
  unlink("." MDBX_LOCKNAME);

  int rc = 0;
  if ((rc = mdbx_env_create(&env))) {
    errmsg = "failed to mdbx_env_create: %s\n";
    goto Fail;
  }
  if ((rc = mdbx_env_open(
           env, ".", MDBX_NOSUBDIR | MDBX_COALESCE | MDBX_LIFORECLAIM, 0644))) {
    errmsg = "failed to mdbx_env_open: %s\n";
    goto Fail;
  }
  if ((rc = mdbx_txn_begin(env, NULL, 0, &txn))) {
    errmsg = "failed to mdbx_txn_begin: %s\n";
    goto Fail;
  }
  if ((rc = mdbx_dbi_open(txn, NULL, MDBX_DUPSORT | MDBX_CREATE, &dbi))) {
    errmsg = "failed to mdbx_dbi_open: %s\n";
    goto Fail;
  }
  if ((rc = mdbx_cursor_open(txn, dbi, &cur))) {
    errmsg = "failed to mdbx_cursor_open: %s\n";
    goto Fail;
  }

#define DUMP()                                                                 \
  do {                                                                         \
    if ((rc = dump(cur)) && rc != MDBX_NOTFOUND) {                             \
      errmsg = "failed to mdbx_cursor_get(FIRST): %s\n";                       \
      goto Fail;                                                               \
    }                                                                          \
    puts("");                                                                  \
  } while (0)

#define PUTVAL(k, v, flags)                                                    \
  do {                                                                         \
    if ((rc = put(txn, dbi, k, v, flags))) {                                   \
      errmsg = "failed to mdbx_put: %s\n";                                     \
      goto Fail;                                                               \
    }                                                                          \
  } while (0)

  puts("TEST WITH MULTIPLE KEYS ====================");
  // UPSERTING
  // MDBX_UPSERT:
  //   Key is absent → Insertion (Insertion)
  //   Key exist → Wanna to add new values (Overwrite by single new value)
  puts("insert multiple keys and values {");
  puts("  foo = bar, baz, qux");
  puts("  hello = world");
  puts("}");
  PUTVAL("foo", "bar", MDBX_UPSERT);
  PUTVAL("foo", "baz", MDBX_UPSERT);
  PUTVAL("foo", "qux", MDBX_UPSERT);
  PUTVAL("hello", "world", MDBX_UPSERT);
  DUMP();
  //
  // above code will output the fllowing;
  //
  //   insert multiple values {
  //     foo = bar, baz, qux
  //     hello = world
  //   }
  //   (foo) = (bar)
  //   (foo) = (baz)
  //   (foo) = (qux)
  //   (hello) = (world)
  //

  // UPSERTING
  // MDBX_UPSERT + MDBX_ALLDUPS:
  //   Key exist → Replace all values with a new one (Overwrite by single new
  //   value)
  puts("overwrite by single new value: MDBX_UPSERT + MDBX_ALLDUPS {");
  puts("  foo = baa");
  puts("}");
  PUTVAL("foo", "baa", MDBX_UPSERT | MDBX_ALLDUPS);
  DUMP();
  // above code will output the fllowing;
  //   overwrite by single new value {
  //     foo = baa
  //   }
  //   (foo) = (baa)
  //   (hello) = (world)
  if ((rc = clear(cur))) {
    errmsg = "failed to clear: %s\n";
    goto Fail;
  }
  DUMP();

  puts("TEST WITH A SINGLE KEY ====================");
  // UPSERTING
  // MDBX_UPSERT:
  //   Key is absent → Insertion (Insertion)
  //   Key exist → Wanna to add new values (Overwrite by single new value)
  puts("insert single key and multiple values {");
  puts("  foo = bar, baz, qux");
  puts("}");
  PUTVAL("foo", "bar", MDBX_UPSERT);
  PUTVAL("foo", "baz", MDBX_UPSERT);
  PUTVAL("foo", "qux", MDBX_UPSERT);
  DUMP();
  //
  // above code will output the fllowing;
  //
  //   insert: foo = bar, baz, qux
  //   foo = bar
  //   foo = baz
  //   foo = qux

  // UPSERTING
  // MDBX_UPSERT + MDBX_ALLDUPS:
  //   Key exist → Replace all values with a new one (Overwrite by single new
  //   value)
  puts("overwrite by single new value: MDBX_UPSERT + MDBX_ALLDUPS {");
  puts("  foo = baa");
  puts("}");
  PUTVAL("foo", "baa", MDBX_UPSERT | MDBX_ALLDUPS);
  DUMP();
  // above code outputs nothing.
  // all data associated with key has been deleted.
  // Is it a bug? Or, am I misunderstanding how to use it?

  if ((rc = mdbx_txn_commit(txn))) {
    errmsg = "failed to mdbx_txn_commit: %s\n";
    goto Fail;
  }
  mdbx_cursor_close(cur);
  if ((rc = mdbx_env_close(env))) {
    errmsg = "failed to mdbx_env_close: %s\n";
    goto Fail;
  }
  return 0;

Fail:
  printf(errmsg, mdbx_strerror(rc));
  return EXIT_FAILURE;
}
