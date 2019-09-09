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

bool testcase_hill::run() {
  MDBX_dbi dbi;
  int err = db_open__begin__table_create_open_clean(dbi);
  if (unlikely(err != MDBX_SUCCESS)) {
    log_notice("hill: bailout-prepare due '%s'", mdbx_strerror(err));
    return true;
  }

  /* LY: тест "холмиком":
   *  - сначала наполняем таблицу циклическими CRUD-манипуляциями,
   *    которые в каждом цикле делают несколько операций, включая удаление,
   *    но в результате добавляют записи.
   *  - затем очищаем таблицу также CRUD-манипуляциями, но уже с другой
   *    пропорцией удалений.
   *
   * При этом очень многое зависит от порядка перебора ключей:
   *   - (псевдо)случайное распределение требуется лишь для полноты картины,
   *     но в целом не покрывает важных кейсов.
   *   - кроме (псевдо)случайного перебора требуется последовательное
   *     итерирование ключей интервалами различной ширины, с тем чтобы
   *     проверить различные варианты как разделения, так и слияния страниц
   *     внутри движка.
   *   - при не-уникальных ключах (MDBX_DUPSORT с подвариантами), для каждого
   *     повтора внутри движка формируется вложенное btree-дерево,
   *     соответственно требуется соблюдение аналогичных принципов
   *     итерирования для значений.
   */

  /* TODO: работа в несколько потоков */
  keyvalue_maker.setup(config.params, config.actor_id, 0 /* thread_number */);

  keygen::buffer a_key = keygen::alloc(config.params.keylen_max);
  keygen::buffer a_data_0 = keygen::alloc(config.params.datalen_max);
  keygen::buffer a_data_1 = keygen::alloc(config.params.datalen_max);
  keygen::buffer b_key = keygen::alloc(config.params.keylen_max);
  keygen::buffer b_data = keygen::alloc(config.params.datalen_max);

  const unsigned insert_flags = (config.params.table_flags & MDBX_DUPSORT)
                                    ? MDBX_NODUPDATA
                                    : MDBX_NODUPDATA | MDBX_NOOVERWRITE;
  const unsigned update_flags =
      (config.params.table_flags & MDBX_DUPSORT)
          ? MDBX_CURRENT | MDBX_NODUPDATA | MDBX_NOOVERWRITE
          : MDBX_NODUPDATA;

  uint64_t serial_count = 0;
  uint64_t commited_serial = serial_count;
  unsigned txn_nops = 0;

  while (should_continue()) {
    const keygen::serial_t a_serial = serial_count;
    if (unlikely(!keyvalue_maker.increment(serial_count, 1))) {
      log_notice("uphill: unexpected key-space overflow");
      break;
    }

    const keygen::serial_t b_serial = serial_count;
    assert(b_serial > a_serial);

    // создаем первую запись из пары
    const keygen::serial_t age_shift = UINT64_C(1) << (a_serial % 31);
    log_trace("uphill: insert-a (age %" PRIu64 ") %" PRIu64, age_shift,
              a_serial);
    generate_pair(a_serial, a_key, a_data_1, age_shift);
    err = mdbx_put(txn_guard.get(), dbi, &a_key->value, &a_data_1->value,
                   insert_flags);
    if (unlikely(err != MDBX_SUCCESS)) {
      if (err == MDBX_MAP_FULL && config.params.ignore_dbfull) {
        log_notice("uphill: bailout at insert-a due '%s'", mdbx_strerror(err));
        txn_restart(true, false);
        serial_count = commited_serial;
        break;
      }
      failure_perror("mdbx_put(insert-a.1)", err);
    }

    if (++txn_nops >= config.params.batch_write) {
      err = breakable_restart();
      if (unlikely(err != MDBX_SUCCESS)) {
        log_notice("uphill: bailout at commit due '%s'", mdbx_strerror(err));
        serial_count = commited_serial;
        break;
      }
      commited_serial = a_serial;
      txn_nops = 0;
    }

    // создаем вторую запись из пары
    log_trace("uphill: insert-b %" PRIu64, b_serial);
    generate_pair(b_serial, b_key, b_data, 0);
    err = mdbx_put(txn_guard.get(), dbi, &b_key->value, &b_data->value,
                   insert_flags);
    if (unlikely(err != MDBX_SUCCESS)) {
      if (err == MDBX_MAP_FULL && config.params.ignore_dbfull) {
        log_notice("uphill: bailout at insert-b due '%s'", mdbx_strerror(err));
        txn_restart(true, false);
        serial_count = commited_serial;
        break;
      }
      failure_perror("mdbx_put(insert-b)", err);
    }

    if (++txn_nops >= config.params.batch_write) {
      err = breakable_restart();
      if (unlikely(err != MDBX_SUCCESS)) {
        log_notice("uphill: bailout at commit due '%s'", mdbx_strerror(err));
        serial_count = commited_serial;
        break;
      }
      commited_serial = a_serial;
      txn_nops = 0;
    }

    // обновляем данные в первой записи
    log_trace("uphill: update-a (age %" PRIu64 "->0) %" PRIu64, age_shift,
              a_serial);
    generate_pair(a_serial, a_key, a_data_0, 0);
    checkdata("uphill: update-a", dbi, a_key->value, a_data_1->value);
    err = mdbx_replace(txn_guard.get(), dbi, &a_key->value, &a_data_0->value,
                       &a_data_1->value, update_flags);
    if (unlikely(err != MDBX_SUCCESS)) {
      if (err == MDBX_MAP_FULL && config.params.ignore_dbfull) {
        log_notice("uphill: bailout at update-a due '%s'", mdbx_strerror(err));
        txn_restart(true, false);
        serial_count = commited_serial;
        break;
      }
      failure_perror("mdbx_replace(update-a: 1->0)", err);
    }

    if (++txn_nops >= config.params.batch_write) {
      err = breakable_restart();
      if (unlikely(err != MDBX_SUCCESS)) {
        log_notice("uphill: bailout at commit due '%s'", mdbx_strerror(err));
        serial_count = commited_serial;
        break;
      }
      commited_serial = a_serial;
      txn_nops = 0;
    }

    // удаляем вторую запись
    log_trace("uphill: delete-b %" PRIu64, b_serial);
    checkdata("uphill: delete-b", dbi, b_key->value, b_data->value);
    err = mdbx_del(txn_guard.get(), dbi, &b_key->value, &b_data->value);
    if (unlikely(err != MDBX_SUCCESS)) {
      if (err == MDBX_MAP_FULL && config.params.ignore_dbfull) {
        log_notice("uphill: bailout at delete-b due '%s'", mdbx_strerror(err));
        txn_restart(true, false);
        serial_count = commited_serial;
        break;
      }
      failure_perror("mdbx_del(b)", err);
    }

    if (++txn_nops >= config.params.batch_write) {
      err = breakable_restart();
      if (unlikely(err != MDBX_SUCCESS)) {
        log_notice("uphill: bailout at commit due '%s'", mdbx_strerror(err));
        serial_count = commited_serial;
        break;
      }
      commited_serial = a_serial;
      txn_nops = 0;
    }

    report(1);
    if (!keyvalue_maker.increment(serial_count, 1)) {
      // дошли до границы пространства ключей
      serial_count = a_serial;
      goto overflow;
    }
  }

  while (serial_count > 1) {
    if (unlikely(!keyvalue_maker.increment(serial_count, -2)))
      failure("downhill: unexpected key-space underflow");

  overflow:
    const keygen::serial_t a_serial = serial_count;
    const keygen::serial_t b_serial = a_serial + 1;
    assert(b_serial > a_serial);

    // обновляем первую запись из пары
    const keygen::serial_t age_shift = UINT64_C(1) << (a_serial % 31);
    log_trace("downhill: update-a (age 0->%" PRIu64 ") %" PRIu64, age_shift,
              a_serial);
    generate_pair(a_serial, a_key, a_data_0, 0);
    generate_pair(a_serial, a_key, a_data_1, age_shift);
    checkdata("downhill: update-a", dbi, a_key->value, a_data_0->value);
    err = mdbx_replace(txn_guard.get(), dbi, &a_key->value, &a_data_1->value,
                       &a_data_0->value, update_flags);
    if (unlikely(err != MDBX_SUCCESS)) {
      if (err == MDBX_MAP_FULL && config.params.ignore_dbfull) {
        log_notice("downhill: bailout at update-a due '%s'",
                   mdbx_strerror(err));
        txn_end(true);
        break;
      }
      failure_perror("mdbx_put(update-a: 0->1)", err);
    }

    if (++txn_nops >= config.params.batch_write) {
      err = breakable_restart();
      if (unlikely(err != MDBX_SUCCESS)) {
        log_notice("downhill: bailout at commit due '%s'", mdbx_strerror(err));
        break;
      }
      txn_nops = 0;
    }

    // создаем вторую запись из пары
    log_trace("downhill: insert-b %" PRIu64, b_serial);
    generate_pair(b_serial, b_key, b_data, 0);
    err = mdbx_put(txn_guard.get(), dbi, &b_key->value, &b_data->value,
                   insert_flags);
    if (unlikely(err != MDBX_SUCCESS)) {
      if (err == MDBX_MAP_FULL && config.params.ignore_dbfull) {
        log_notice("downhill: bailout at insert-a due '%s'",
                   mdbx_strerror(err));
        txn_end(true);
        break;
      }
      failure_perror("mdbx_put(insert-b)", err);
    }

    if (++txn_nops >= config.params.batch_write) {
      err = breakable_restart();
      if (unlikely(err != MDBX_SUCCESS)) {
        log_notice("downhill: bailout at commit due '%s'", mdbx_strerror(err));
        break;
      }
      txn_nops = 0;
    }

    // удаляем первую запись
    log_trace("downhill: delete-a (age %" PRIu64 ") %" PRIu64, age_shift,
              a_serial);
    checkdata("downhill: delete-a", dbi, a_key->value, a_data_1->value);
    err = mdbx_del(txn_guard.get(), dbi, &a_key->value, &a_data_1->value);
    if (unlikely(err != MDBX_SUCCESS)) {
      if (err == MDBX_MAP_FULL && config.params.ignore_dbfull) {
        log_notice("downhill: bailout at delete-a due '%s'",
                   mdbx_strerror(err));
        txn_end(true);
        break;
      }
      failure_perror("mdbx_del(a)", err);
    }

    if (++txn_nops >= config.params.batch_write) {
      err = breakable_restart();
      if (unlikely(err != MDBX_SUCCESS)) {
        log_notice("downhill: bailout at commit due '%s'", mdbx_strerror(err));
        break;
      }
      txn_nops = 0;
    }

    // удаляем вторую запись
    log_trace("downhill: delete-b %" PRIu64, b_serial);
    checkdata("downhill: delete-b", dbi, b_key->value, b_data->value);
    err = mdbx_del(txn_guard.get(), dbi, &b_key->value, &b_data->value);
    if (unlikely(err != MDBX_SUCCESS)) {
      if (err == MDBX_MAP_FULL && config.params.ignore_dbfull) {
        log_notice("downhill: bailout at delete-b due '%s'",
                   mdbx_strerror(err));
        txn_end(true);
        break;
      }
      failure_perror("mdbx_del(b)", err);
    }

    if (++txn_nops >= config.params.batch_write) {
      err = breakable_restart();
      if (unlikely(err != MDBX_SUCCESS)) {
        log_notice("downhill: bailout at commit due '%s'", mdbx_strerror(err));
        break;
      }
      txn_nops = 0;
    }

    report(1);
  }

  if (txn_guard) {
    err = breakable_commit();
    if (unlikely(err != MDBX_SUCCESS))
      log_notice("downhill: bailout at commit due '%s'", mdbx_strerror(err));
  }

  if (dbi) {
    if (config.params.drop_table && !mode_readonly()) {
      txn_begin(false);
      db_table_drop(dbi);
      err = breakable_commit();
      if (unlikely(err != MDBX_SUCCESS)) {
        log_notice("hill: bailout-clean due '%s'", mdbx_strerror(err));
        return true;
      }
    } else
      db_table_close(dbi);
  }
  return true;
}
