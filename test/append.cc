/*
 * Copyright 2017-2019 Leonid Yuriev <leo@yuriev.ru>
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

#include "test.h"

bool testcase_append::run() {
  int err = db_open__begin__table_create_open_clean(dbi);
  if (unlikely(err != MDBX_SUCCESS)) {
    log_notice("append: bailout-prepare due '%s'", mdbx_strerror(err));
    return true;
  }

  keyvalue_maker.setup(config.params, config.actor_id, 0 /* thread_number */);
  /* LY: тест наполнения таблиц в append-режиме,
   * при котором записи добавляются строго в конец (в порядке сортировки) */
  const unsigned flags = (config.params.table_flags & MDBX_DUPSORT)
                             ? MDBX_APPEND | MDBX_APPENDDUP
                             : MDBX_APPEND;
  keyvalue_maker.make_ordered();

  key = keygen::alloc(config.params.keylen_max);
  data = keygen::alloc(config.params.datalen_max);
  keygen::buffer last_key = keygen::alloc(config.params.keylen_max);
  keygen::buffer last_data = keygen::alloc(config.params.datalen_max);
  last_key->value.iov_base = last_key->bytes;
  last_key->value.iov_len = 0;
  last_data->value.iov_base = last_data->bytes;
  last_data->value.iov_len = 0;

  simple_checksum inserted_checksum;
  uint64_t inserted_number = 0;
  uint64_t serial_count = 0;

  unsigned txn_nops = 0;
  uint64_t commited_inserted_number = inserted_number;
  simple_checksum commited_inserted_checksum = inserted_checksum;
  while (should_continue()) {
    const keygen::serial_t serial = serial_count;
    if (!keyvalue_maker.increment(serial_count, 1)) {
      // дошли до границы пространства ключей
      break;
    }

    log_trace("append: append-a %" PRIu64, serial);
    generate_pair(serial, key, data);
    int cmp = inserted_number ? mdbx_cmp(txn_guard.get(), dbi, &key->value,
                                         &last_key->value)
                              : 1;
    if (cmp == 0 && (config.params.table_flags & MDBX_DUPSORT))
      cmp = mdbx_dcmp(txn_guard.get(), dbi, &data->value, &last_data->value);

    err = mdbx_put(txn_guard.get(), dbi, &key->value, &data->value, flags);
    if (err == MDBX_MAP_FULL && config.params.ignore_dbfull) {
      log_notice("append: bailout-insert due '%s'", mdbx_strerror(err));
      txn_end(true);
      inserted_number = commited_inserted_number;
      inserted_checksum = commited_inserted_checksum;
      break;
    }

    if (cmp > 0) {
      if (unlikely(err != MDBX_SUCCESS))
        failure_perror("mdbx_put(appenda-a)", err);

      memcpy(last_key->value.iov_base, key->value.iov_base,
             last_key->value.iov_len = key->value.iov_len);
      memcpy(last_data->value.iov_base, data->value.iov_base,
             last_data->value.iov_len = data->value.iov_len);
      ++inserted_number;
      inserted_checksum.push((uint32_t)inserted_number, key->value);
      inserted_checksum.push(10639, data->value);
    } else {
      if (unlikely(err != MDBX_EKEYMISMATCH))
        failure_perror("mdbx_put(appenda-a) != MDBX_EKEYMISMATCH", err);
    }

    if (++txn_nops >= config.params.batch_write) {
      err = breakable_restart();
      if (unlikely(err != MDBX_SUCCESS)) {
        log_notice("append: bailout-commit due '%s'", mdbx_strerror(err));
        inserted_number = commited_inserted_number;
        inserted_checksum = commited_inserted_checksum;
        break;
      }
      commited_inserted_number = inserted_number;
      commited_inserted_checksum = inserted_checksum;
      txn_nops = 0;
    }

    report(1);
  }

  if (txn_guard) {
    err = breakable_commit();
    if (unlikely(err != MDBX_SUCCESS)) {
      log_notice("append: bailout-commit due '%s'", mdbx_strerror(err));
      inserted_number = commited_inserted_number;
      inserted_checksum = commited_inserted_checksum;
    }
  }
  //----------------------------------------------------------------------------
  txn_begin(true);
  cursor_open(dbi);

  MDBX_val check_key, check_data;
  err =
      mdbx_cursor_get(cursor_guard.get(), &check_key, &check_data, MDBX_FIRST);
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

    err =
        mdbx_cursor_get(cursor_guard.get(), &check_key, &check_data, MDBX_NEXT);
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
        log_notice("append: bailout-clean due '%s'", mdbx_strerror(err));
        return true;
      }
    } else
      db_table_close(dbi);
  }
  return true;
}
