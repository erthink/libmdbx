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

int testcase_hill::insert(const keygen::buffer &akey,
                          const keygen::buffer &adata, unsigned flags) {
  int err = mdbx_put(txn_guard.get(), dbi, &akey->value, &adata->value, flags);
  if (err == MDBX_SUCCESS) {
    const auto S_key = S(akey);
    const auto S_data = S(adata);
    const bool inserted = mirror.emplace(S_key, S_data).second;
    assert(inserted);
    (void)inserted;
  }
  return err;
}

int testcase_hill::replace(const keygen::buffer &akey,
                           const keygen::buffer &new_data,
                           const keygen::buffer &old_data, unsigned flags) {
  const auto S_key = S(akey);
  const auto S_old = S(old_data);
  const auto S_new = S(new_data);
  const auto removed = mirror.erase(set::key_type(S_key, S_old));
  assert(removed == 1);
  (void)removed;
  const bool inserted = mirror.emplace(S_key, S_new).second;
  assert(inserted);
  (void)inserted;
  return mdbx_replace(txn_guard.get(), dbi, &akey->value, &new_data->value,
                      &old_data->value, flags);
}

int testcase_hill::remove(const keygen::buffer &akey,
                          const keygen::buffer &adata) {
  const auto S_key = S(akey);
  const auto S_data = S(adata);
  const auto removed = mirror.erase(set::key_type(S_key, S_data));
  assert(removed == 1);
  (void)removed;
  return mdbx_del(txn_guard.get(), dbi, &akey->value, &adata->value);
}

bool testcase_hill::verify() const {
  char dump_key[128], dump_value[128];
  char dump_mkey[128], dump_mvalue[128];

  MDBX_cursor *cursor;
  int err = mdbx_cursor_open(txn_guard.get(), dbi, &cursor);
  if (err != MDBX_SUCCESS)
    failure_perror("mdbx_cursor_open()", err);

  bool rc = true;
  MDBX_val akey, avalue;
  MDBX_val mkey, mvalue;
  err = mdbx_cursor_get(cursor, &akey, &avalue, MDBX_FIRST);

  assert(std::is_sorted(mirror.cbegin(), mirror.cend(), ItemCompare(this)));
  auto it = mirror.cbegin();
  while (true) {
    if (err != MDBX_SUCCESS) {
      akey.iov_len = avalue.iov_len = 0;
      akey.iov_base = avalue.iov_base = nullptr;
    }
    const auto S_key = S(akey);
    const auto S_data = S(avalue);
    if (it != mirror.cend()) {
      mkey.iov_base = (void *)it->first.c_str();
      mkey.iov_len = it->first.size();
      mvalue.iov_base = (void *)it->second.c_str();
      mvalue.iov_len = it->second.size();
    }
    if (err == MDBX_SUCCESS && it != mirror.cend() && S_key == it->first &&
        S_data == it->second) {
      ++it;
      err = mdbx_cursor_get(cursor, &akey, &avalue, MDBX_NEXT);
    } else if (err == MDBX_SUCCESS &&
               (it == mirror.cend() || S_key < it->first ||
                (S_key == it->first && S_data < it->second))) {
      if (it != mirror.cend()) {
        log_error("extra pair: db{%s, %s} < mi{%s, %s}",
                  mdbx_dump_val(&akey, dump_key, sizeof(dump_key)),
                  mdbx_dump_val(&avalue, dump_value, sizeof(dump_value)),
                  mdbx_dump_val(&mkey, dump_mkey, sizeof(dump_mkey)),
                  mdbx_dump_val(&mvalue, dump_mvalue, sizeof(dump_mvalue)));
      } else {
        log_error("extra pair: db{%s, %s} < mi.END",
                  mdbx_dump_val(&akey, dump_key, sizeof(dump_key)),
                  mdbx_dump_val(&avalue, dump_value, sizeof(dump_value)));
      }
      err = mdbx_cursor_get(cursor, &akey, &avalue, MDBX_NEXT);
      rc = false;
    } else if (it != mirror.cend() &&
               (err == MDBX_NOTFOUND || S_key > it->first ||
                (S_key == it->first && S_data > it->second))) {
      if (err == MDBX_NOTFOUND) {
        log_error("lost pair: db.END > mi{%s, %s}",
                  mdbx_dump_val(&mkey, dump_mkey, sizeof(dump_mkey)),
                  mdbx_dump_val(&mvalue, dump_mvalue, sizeof(dump_mvalue)));
      } else {
        log_error("lost pair: db{%s, %s} > mi{%s, %s}",
                  mdbx_dump_val(&akey, dump_key, sizeof(dump_key)),
                  mdbx_dump_val(&avalue, dump_value, sizeof(dump_value)),
                  mdbx_dump_val(&mkey, dump_mkey, sizeof(dump_mkey)),
                  mdbx_dump_val(&mvalue, dump_mvalue, sizeof(dump_mvalue)));
      }
      ++it;
      rc = false;
    } else if (err == MDBX_NOTFOUND && it == mirror.cend()) {
      break;
    } else if (err != MDBX_SUCCESS) {
      failure_perror("mdbx_cursor_get()", err);
    } else {
      assert(!"WTF?");
    }
  }

  mdbx_cursor_close(cursor);
  return rc;
}

bool testcase_hill::run() {
  int err = db_open__begin__table_create_open_clean(dbi);
  if (unlikely(err != MDBX_SUCCESS)) {
    log_notice("hill: bailout-prepare due '%s'", mdbx_strerror(err));
    return true;
  }
  mirror.clear();
  mirror_commited.clear();

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

  bool rc = false;
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

    err = insert(a_key, a_data_1, insert_flags);
    if (unlikely(err != MDBX_SUCCESS)) {
      if (err == MDBX_MAP_FULL && config.params.ignore_dbfull) {
        log_notice("uphill: bailout at insert-a due '%s'", mdbx_strerror(err));
        txn_restart(true, false);
        serial_count = commited_serial;
        mirror = mirror_commited;
        break;
      }
      failure_perror("mdbx_put(insert-a.1)", err);
    }
    if (!verify()) {
      log_notice("uphill: bailout after insert-a, before commit");
      goto bailout;
    }

    if (++txn_nops >= config.params.batch_write) {
      err = breakable_restart();
      if (unlikely(err != MDBX_SUCCESS)) {
        log_notice("uphill: bailout at commit due '%s'", mdbx_strerror(err));
        serial_count = commited_serial;
        mirror = mirror_commited;
        break;
      }
      mirror_commited = mirror;
      commited_serial = a_serial;
      txn_nops = 0;
      if (!verify()) {
        log_notice("uphill: bailout after insert-a, after commit");
        goto bailout;
      }
    }

    // создаем вторую запись из пары
    log_trace("uphill: insert-b %" PRIu64, b_serial);
    generate_pair(b_serial, b_key, b_data, 0);
    err = insert(b_key, b_data, insert_flags);
    if (unlikely(err != MDBX_SUCCESS)) {
      if (err == MDBX_MAP_FULL && config.params.ignore_dbfull) {
        log_notice("uphill: bailout at insert-b due '%s'", mdbx_strerror(err));
        txn_restart(true, false);
        serial_count = commited_serial;
        mirror = mirror_commited;
        break;
      }
      failure_perror("mdbx_put(insert-b)", err);
    }
    if (!verify()) {
      log_notice("uphill: bailout after insert-b, before commit");
      goto bailout;
    }

    if (++txn_nops >= config.params.batch_write) {
      err = breakable_restart();
      if (unlikely(err != MDBX_SUCCESS)) {
        log_notice("uphill: bailout at commit due '%s'", mdbx_strerror(err));
        serial_count = commited_serial;
        mirror = mirror_commited;
        break;
      }
      mirror_commited = mirror;
      commited_serial = a_serial;
      txn_nops = 0;
      if (!verify()) {
        log_notice("uphill: bailout after insert-b, after commit");
        goto bailout;
      }
    }

    // обновляем данные в первой записи
    log_trace("uphill: update-a (age %" PRIu64 "->0) %" PRIu64, age_shift,
              a_serial);
    generate_pair(a_serial, a_key, a_data_0, 0);
    checkdata("uphill: update-a", dbi, a_key->value, a_data_1->value);
    err = replace(a_key, a_data_0, a_data_1, update_flags);
    if (unlikely(err != MDBX_SUCCESS)) {
      if (err == MDBX_MAP_FULL && config.params.ignore_dbfull) {
        log_notice("uphill: bailout at update-a due '%s'", mdbx_strerror(err));
        txn_restart(true, false);
        serial_count = commited_serial;
        mirror = mirror_commited;
        break;
      }
      failure_perror("mdbx_replace(update-a: 1->0)", err);
    }
    if (!verify()) {
      log_notice("uphill: bailout after update-a, before commit");
      goto bailout;
    }

    if (++txn_nops >= config.params.batch_write) {
      err = breakable_restart();
      if (unlikely(err != MDBX_SUCCESS)) {
        log_notice("uphill: bailout at commit due '%s'", mdbx_strerror(err));
        serial_count = commited_serial;
        mirror = mirror_commited;
        break;
      }
      mirror_commited = mirror;
      commited_serial = a_serial;
      txn_nops = 0;
      if (!verify()) {
        log_notice("uphill: bailout after update-a, after commit");
        goto bailout;
      }
    }

    // удаляем вторую запись
    log_trace("uphill: delete-b %" PRIu64, b_serial);
    checkdata("uphill: delete-b", dbi, b_key->value, b_data->value);
    err = remove(b_key, b_data);
    if (unlikely(err != MDBX_SUCCESS)) {
      if (err == MDBX_MAP_FULL && config.params.ignore_dbfull) {
        log_notice("uphill: bailout at delete-b due '%s'", mdbx_strerror(err));
        txn_restart(true, false);
        serial_count = commited_serial;
        mirror = mirror_commited;
        break;
      }
      failure_perror("mdbx_del(b)", err);
    }
    if (!verify()) {
      log_notice("uphill: bailout after delete-b, before commit");
      goto bailout;
    }

    if (++txn_nops >= config.params.batch_write) {
      err = breakable_restart();
      if (unlikely(err != MDBX_SUCCESS)) {
        log_notice("uphill: bailout at commit due '%s'", mdbx_strerror(err));
        serial_count = commited_serial;
        mirror = mirror_commited;
        break;
      }
      mirror_commited = mirror;
      commited_serial = a_serial;
      txn_nops = 0;
      if (!verify()) {
        log_notice("uphill: bailout after delete-b, after commit");
        goto bailout;
      }
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
    err = replace(a_key, a_data_1, a_data_0, update_flags);
    if (unlikely(err != MDBX_SUCCESS)) {
      if (err == MDBX_MAP_FULL && config.params.ignore_dbfull) {
        log_notice("downhill: bailout at update-a due '%s'",
                   mdbx_strerror(err));
        txn_end(true);
        mirror = mirror_commited;
        break;
      }
      failure_perror("mdbx_put(update-a: 0->1)", err);
    }
    if (!verify()) {
      log_notice("downhill: bailout after update-a, before commit");
      break;
    }

    if (++txn_nops >= config.params.batch_write) {
      err = breakable_restart();
      if (unlikely(err != MDBX_SUCCESS)) {
        log_notice("downhill: bailout at commit due '%s'", mdbx_strerror(err));
        mirror = mirror_commited;
        break;
      }
      mirror_commited = mirror;
      txn_nops = 0;
      if (!verify()) {
        log_notice("downhill: bailout after update-a, after commit");
        break;
      }
    }

    // создаем вторую запись из пары
    log_trace("downhill: insert-b %" PRIu64, b_serial);
    generate_pair(b_serial, b_key, b_data, 0);
    err = insert(b_key, b_data, insert_flags);
    if (unlikely(err != MDBX_SUCCESS)) {
      if (err == MDBX_MAP_FULL && config.params.ignore_dbfull) {
        log_notice("downhill: bailout at insert-a due '%s'",
                   mdbx_strerror(err));
        txn_end(true);
        mirror = mirror_commited;
        break;
      }
      failure_perror("mdbx_put(insert-b)", err);
    }
    if (!verify()) {
      log_notice("downhill: bailout after insert-b, before commit");
      break;
    }

    if (++txn_nops >= config.params.batch_write) {
      err = breakable_restart();
      if (unlikely(err != MDBX_SUCCESS)) {
        log_notice("downhill: bailout at commit due '%s'", mdbx_strerror(err));
        mirror = mirror_commited;
        break;
      }
      mirror_commited = mirror;
      txn_nops = 0;
      if (!verify()) {
        log_notice("downhill: bailout after insert-b, after commit");
        break;
      }
    }

    // удаляем первую запись
    log_trace("downhill: delete-a (age %" PRIu64 ") %" PRIu64, age_shift,
              a_serial);
    checkdata("downhill: delete-a", dbi, a_key->value, a_data_1->value);
    err = remove(a_key, a_data_1);
    if (unlikely(err != MDBX_SUCCESS)) {
      if (err == MDBX_MAP_FULL && config.params.ignore_dbfull) {
        log_notice("downhill: bailout at delete-a due '%s'",
                   mdbx_strerror(err));
        txn_end(true);
        mirror = mirror_commited;
        break;
      }
      failure_perror("mdbx_del(a)", err);
    }
    if (!verify()) {
      log_notice("downhill: bailout after delete-a, before commit");
      break;
    }

    if (++txn_nops >= config.params.batch_write) {
      err = breakable_restart();
      if (unlikely(err != MDBX_SUCCESS)) {
        log_notice("downhill: bailout at commit due '%s'", mdbx_strerror(err));
        mirror = mirror_commited;
        break;
      }
      mirror_commited = mirror;
      txn_nops = 0;
      if (!verify()) {
        log_notice("downhill: bailout after delete-a, after commit");
        break;
      }
    }

    // удаляем вторую запись
    log_trace("downhill: delete-b %" PRIu64, b_serial);
    checkdata("downhill: delete-b", dbi, b_key->value, b_data->value);
    err = remove(b_key, b_data);
    if (unlikely(err != MDBX_SUCCESS)) {
      if (err == MDBX_MAP_FULL && config.params.ignore_dbfull) {
        log_notice("downhill: bailout at delete-b due '%s'",
                   mdbx_strerror(err));
        txn_end(true);
        mirror = mirror_commited;
        break;
      }
      failure_perror("mdbx_del(b)", err);
    }
    if (!verify()) {
      log_notice("downhill: bailout after delete-b, before commit");
      break;
    }

    if (++txn_nops >= config.params.batch_write) {
      err = breakable_restart();
      if (unlikely(err != MDBX_SUCCESS)) {
        log_notice("downhill: bailout at commit due '%s'", mdbx_strerror(err));
        mirror = mirror_commited;
        break;
      }
      mirror_commited = mirror;
      txn_nops = 0;
      if (!verify()) {
        log_notice("downhill: bailout after delete-b, after commit");
        goto bailout;
      }
    }

    report(1);
  }

  rc = verify();
bailout:
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
        return rc;
      }
    } else
      db_table_close(dbi);
  }
  return rc;
}
