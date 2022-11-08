/*
 * Copyright 2017-2022 Leonid Yuriev <leo@yuriev.ru>
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

#include "test.h++"

class testcase_append : public testcase {
public:
  testcase_append(const actor_config &config, const mdbx_pid_t pid)
      : testcase(config, pid) {}
  bool run() override;

  static bool review_params(actor_params &params) {
    return testcase::review_params(params) && params.make_keygen_linear();
  }
};
REGISTER_TESTCASE(append);

bool testcase_append::run() {
  const bool reverse = flipcoin();
  const char *const caption = reverse ? "ahead" : "append";
  log_notice("the '%s' scenario is selected", caption);

  int err = db_open__begin__table_create_open_clean(dbi);
  if (unlikely(err != MDBX_SUCCESS)) {
    log_notice("%s: bailout-prepare due '%s'", caption, mdbx_strerror(err));
    return true;
  }

  cursor_open(dbi);
  keyvalue_maker.setup(config.params, config.actor_id, 0 /* thread_number */);
  /* LY: тест наполнения таблиц в append-режиме,
   * при котором записи добавляются строго в конец (в порядке сортировки) */
  const MDBX_put_flags_t flags =
      reverse
          ? ((config.params.table_flags & MDBX_DUPSORT) ? MDBX_UPSERT
                                                        : MDBX_NOOVERWRITE)
          : ((config.params.table_flags & MDBX_DUPSORT)
                 ? (flipcoin() ? MDBX_APPEND | MDBX_APPENDDUP : MDBX_APPENDDUP)
                 : MDBX_APPEND);

  key = keygen::alloc(config.params.keylen_max);
  data = keygen::alloc(config.params.datalen_max);

  simple_checksum inserted_checksum;
  uint64_t inserted_number = 0;
  uint64_t serial_count = 0;
  if (reverse)
    keyvalue_maker.seek2end(serial_count);

  unsigned txn_nops = 0;
  uint64_t committed_inserted_number = inserted_number;
  simple_checksum committed_inserted_checksum = inserted_checksum;
  while (should_continue()) {
    const keygen::serial_t serial = serial_count;
    const bool turn_key = (config.params.table_flags & MDBX_DUPSORT) == 0 ||
                          flipcoin_n(config.params.keygen.split);
    if (turn_key
            ? !keyvalue_maker.increment_key_part(serial_count, reverse ? -1 : 1)
            : !keyvalue_maker.increment(serial_count, reverse ? -1 : 1)) {
      // дошли до границы пространства ключей
      break;
    }

    log_trace("%s: insert-a %" PRIu64, caption, serial);
    generate_pair(serial);
    // keygen::log_pair(logging::verbose, "append.", key, data);

    bool expect_key_mismatch = false;
    if (flags & (MDBX_APPEND | MDBX_APPENDDUP)) {
      MDBX_val ge_key = key->value;
      MDBX_val ge_data = data->value;
      err = mdbx_get_equal_or_great(txn_guard.get(), dbi, &ge_key, &ge_data);

      if (err == MDBX_SUCCESS /* exact match */) {
        expect_key_mismatch = true;
        assert(inserted_number > 0);
        assert(mdbx_cmp(txn_guard.get(), dbi, &key->value, &ge_key) == 0);
        assert((config.params.table_flags & MDBX_DUPSORT) == 0 ||
               mdbx_dcmp(txn_guard.get(), dbi, &data->value, &ge_data) == 0);
        assert(inserted_number > 0);
      } else if (err == MDBX_RESULT_TRUE /* have key-value pair great than */) {
        assert(mdbx_cmp(txn_guard.get(), dbi, &key->value, &ge_key) < 0 ||
               ((config.params.table_flags & MDBX_DUPSORT) &&
                mdbx_cmp(txn_guard.get(), dbi, &key->value, &ge_key) == 0 &&
                mdbx_dcmp(txn_guard.get(), dbi, &data->value, &ge_data) < 0));
        switch (int(flags)) {
        default:
          abort();
#if CONSTEXPR_ENUM_FLAGS_OPERATIONS
        case MDBX_APPEND | MDBX_APPENDDUP:
#else
        case int(MDBX_APPEND) | int(MDBX_APPENDDUP):
#endif
          assert((config.params.table_flags & MDBX_DUPSORT) != 0);
          __fallthrough;
          // fall through
        case MDBX_APPEND:
          expect_key_mismatch = true;
          break;
        case MDBX_APPENDDUP:
          assert((config.params.table_flags & MDBX_DUPSORT) != 0);
          expect_key_mismatch =
              mdbx_cmp(txn_guard.get(), dbi, &key->value, &ge_key) == 0;
          break;
        }
      } else if (err == MDBX_NOTFOUND /* all pair are less than */) {
        switch (int(flags)) {
        default:
          abort();
        case MDBX_APPENDDUP:
#if CONSTEXPR_ENUM_FLAGS_OPERATIONS
        case MDBX_APPEND | MDBX_APPENDDUP:
#else
        case int(MDBX_APPEND) | int(MDBX_APPENDDUP):
#endif
          assert((config.params.table_flags & MDBX_DUPSORT) != 0);
          __fallthrough;
          // fall through
        case MDBX_APPEND:
          expect_key_mismatch = false;
          break;
        }
      } else
        failure_perror("mdbx_get_equal_or_great()", err);

      assert(!expect_key_mismatch);
    }

    err = mdbx_cursor_put(cursor_guard.get(), &key->value, &data->value, flags);
    if (err == MDBX_MAP_FULL && config.params.ignore_dbfull) {
      log_notice("%s: bailout-insert due '%s'", caption, mdbx_strerror(err));
      txn_end(true);
      inserted_number = committed_inserted_number;
      inserted_checksum = committed_inserted_checksum;
      break;
    }

    if (!expect_key_mismatch) {
      if (unlikely(err != MDBX_SUCCESS))
        failure_perror("mdbx_cursor_put(insert-a)", err);
      ++inserted_number;
      inserted_checksum.push((uint32_t)inserted_number, key->value);
      inserted_checksum.push(10639, data->value);
    } else if (unlikely(err != MDBX_EKEYMISMATCH))
      failure_perror("mdbx_cursor_put(insert-a) != MDBX_EKEYMISMATCH", err);

    if (++txn_nops >= config.params.batch_write) {
      err = breakable_restart();
      if (unlikely(err != MDBX_SUCCESS)) {
        log_notice("%s: bailout-commit due '%s'", caption, mdbx_strerror(err));
        inserted_number = committed_inserted_number;
        inserted_checksum = committed_inserted_checksum;
        break;
      }
      committed_inserted_number = inserted_number;
      committed_inserted_checksum = inserted_checksum;
      txn_nops = 0;
    }

    report(1);
  }

  if (txn_guard) {
    err = breakable_commit();
    if (unlikely(err != MDBX_SUCCESS)) {
      log_notice("%s: bailout-commit due '%s'", caption, mdbx_strerror(err));
      inserted_number = committed_inserted_number;
      inserted_checksum = committed_inserted_checksum;
    }
  }
  //----------------------------------------------------------------------------
  txn_begin(true);
  cursor_renew();

  MDBX_val check_key, check_data;
  err = mdbx_cursor_get(cursor_guard.get(), &check_key, &check_data,
                        reverse ? MDBX_LAST : MDBX_FIRST);
  if (likely(inserted_number)) {
    if (unlikely(err != MDBX_SUCCESS))
      failure_perror("mdbx_cursor_get(MDBX_FIRST)", err);
  }

  simple_checksum read_checksum;
  uint64_t read_count = 0;
  while (err == MDBX_SUCCESS) {
    ++read_count;
    read_checksum.push((uint32_t)read_count, check_key);
    read_checksum.push(10639, check_data);

    err = mdbx_cursor_get(cursor_guard.get(), &check_key, &check_data,
                          reverse ? MDBX_PREV : MDBX_NEXT);
  }

  if (unlikely(err != MDBX_NOTFOUND))
    failure_perror("mdbx_cursor_get(MDBX_NEXT) != EOF", err);

  if (unlikely(read_count != inserted_number))
    failure("read_count(%" PRIu64 ") != inserted_number(%" PRIu64 ")",
            read_count, inserted_number);

  if (unlikely(read_checksum.value != inserted_checksum.value))
    failure("read_checksum(0x%016" PRIu64 ") "
            "!= inserted_checksum(0x%016" PRIu64 ")",
            read_checksum.value, inserted_checksum.value);

  cursor_close();
  txn_end(true);
  //----------------------------------------------------------------------------

  if (dbi) {
    if (config.params.drop_table && !mode_readonly()) {
      txn_begin(false);
      db_table_drop(dbi);
      err = breakable_commit();
      if (unlikely(err != MDBX_SUCCESS)) {
        log_notice("%s: bailout-clean due '%s'", caption, mdbx_strerror(err));
        return true;
      }
    } else
      db_table_close(dbi);
  }
  return true;
}
