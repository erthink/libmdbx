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
#include <cmath>
#include <deque>

/* LY: тест "эмуляцией time-to-live":
 *  - организуется "скользящее окно", которое двигается вперед вдоль
 *    числовой оси каждую транзакцию.
 *  - по переднему краю "скользящего окна" записи добавляются в таблицу,
 *    а по заднему удаляются.
 *  - количество добавляемых/удаляемых записей псевдослучайно зависит
 *    от номера транзакции, но с экспоненциальным распределением.
 *  - размер "скользящего окна" также псевдослучайно зависит от номера
 *    транзакции с "отрицательным" экспоненциальным распределением
 *    MAX_WIDTH - exp(rnd(N)), при уменьшении окна сдвигается задний
 *    край и удаляются записи позади него.
 *
 *  Таким образом имитируется поведение таблицы с TTL: записи стохастически
 *  добавляются и удаляются, но изредка происходит массивное удаление.
 */
REGISTER_TESTCASE(ttl);

unsigned testcase_ttl::edge2count(uint64_t edge) {
  const double rnd = u64_to_double1(prng64_map1_white(edge));
  const unsigned count =
      unsigned(std::lrint(std::pow(sliding.max_step_size, rnd)));
  // average value: (X - 1) / ln(X), where X = sliding.max_step_size
  return count;
}

unsigned testcase_ttl::edge2window(uint64_t edge) {
  const double rnd = u64_to_double1(bleach64(edge));
  const unsigned size =
      sliding.max_window_size -
      unsigned(std::lrint(std::pow(sliding.max_window_size, rnd)));
  // average value: Y - (Y - 1) / ln(Y), where Y = sliding.max_window_size
  return size;
}

static inline double estimate(const double x, const double y) {
  /* среднее кол-во операций N = X' * Y', где X' и Y' средние значения
   * размера окна и кол-ва добавляемых за один шаг записей:
   *  X' = (X - 1) / ln(X), где X = sliding.max_step_size
   *  Y' = Y - (Y - 1) / ln(Y), где Y = sliding.max_window_size */
  return (x - 1) / std::log(x) * (y - (y - 1) / std::log(y));
}

bool testcase_ttl::setup() {
  const unsigned window_top_lower =
      7 /* нижний предел для верхней границы диапазона, в котором будет
           стохастически колебаться размер окна */
      ;
  const unsigned count_top_lower =
      7 /* нижний предел для верхней границы диапазона, в котором будет
           стохастически колебаться кол-во записей добавляемых на одном шаге */
      ;

  /* для параметризации используем подходящие параметры,
   * которые не имеют здесь смысла в первоначальном значении. */
  const double ratio =
      double(config.params.batch_read ? config.params.batch_read : 1) /
      double(config.params.batch_write ? config.params.batch_write : 1);

  /* проще найти двоичным поиском (вариация метода Ньютона) */
  double hi = config.params.test_nops, lo = 1;
  double x = std::sqrt(hi + lo) / ratio;
  while (hi > lo) {
    const double n = estimate(x, x * ratio);
    if (n > config.params.test_nops)
      hi = x - 1;
    else
      lo = x + 1;
    x = (hi + lo) / 2;
  }

  sliding.max_step_size = unsigned(std::lrint(x));
  if (sliding.max_step_size < count_top_lower)
    sliding.max_step_size = count_top_lower;
  sliding.max_window_size = unsigned(std::lrint(x * ratio));
  if (sliding.max_window_size < window_top_lower)
    sliding.max_window_size = window_top_lower;

  while (estimate(sliding.max_step_size, sliding.max_window_size) >
         config.params.test_nops * 2.0) {
    if (ratio * sliding.max_step_size > sliding.max_window_size) {
      if (sliding.max_step_size < count_top_lower)
        break;
      sliding.max_step_size = sliding.max_step_size * 7 / 8;
    } else {
      if (sliding.max_window_size < window_top_lower)
        break;
      sliding.max_window_size = sliding.max_window_size * 7 / 8;
    }
  }

  log_verbose("come up window_max %u from `batch_read`",
              sliding.max_window_size);
  log_verbose("come up step_max %u from `batch_write`", sliding.max_step_size);
  return inherited::setup();
}

bool testcase_ttl::run() {
  int err = db_open__begin__table_create_open_clean(dbi);
  if (unlikely(err != MDBX_SUCCESS)) {
    log_notice("ttl: bailout-prepare due '%s'", mdbx_strerror(err));
    return false;
  }

  uint64_t seed =
      prng64_map2_white(config.params.keygen.seed) + config.actor_id;
  keyvalue_maker.setup(config.params, config.actor_id, 0 /* thread_number */);
  key = keygen::alloc(config.params.keylen_max);
  data = keygen::alloc(config.params.datalen_max);
  const MDBX_put_flags_t insert_flags =
      (config.params.table_flags & MDBX_DUPSORT)
          ? MDBX_NODUPDATA
          : MDBX_NODUPDATA | MDBX_NOOVERWRITE;

  std::deque<std::pair<uint64_t, unsigned>> fifo;
  uint64_t serial = 0;
  bool rc = false;
  unsigned clear_wholetable_passed = 0;
  unsigned clear_stepbystep_passed = 0;
  unsigned dbfull_passed = 0;
  unsigned loops = 0;
  bool keyspace_overflow = false;
  while (true) {
    const uint64_t salt = prng64_white(seed) /* mdbx_txn_id(txn_guard.get()) */;

    const unsigned window_width =
        (!should_continue() || flipcoin_x4()) ? 0 : edge2window(salt);
    unsigned head_count = edge2count(salt);
    log_debug("ttl: step #%" PRIu64 " (serial %" PRIu64
              ", window %u, count %u) salt %" PRIu64,
              nops_completed, serial, window_width, head_count, salt);

    if (window_width || flipcoin()) {
      clear_stepbystep_passed += window_width == 0;
      while (fifo.size() > window_width) {
        uint64_t tail_serial = fifo.back().first;
        const unsigned tail_count = fifo.back().second;
        log_trace("ttl: pop-tail (serial %" PRIu64 ", count %u)", tail_serial,
                  tail_count);
        fifo.pop_back();
        for (unsigned n = 0; n < tail_count; ++n) {
          log_trace("ttl: remove-tail %" PRIu64, tail_serial);
          generate_pair(tail_serial);
          err = remove(key, data);
          if (unlikely(err != MDBX_SUCCESS)) {
            if (err == MDBX_MAP_FULL && config.params.ignore_dbfull) {
              log_notice("ttl: tail-bailout due '%s'", mdbx_strerror(err));
              goto bailout;
            }
            failure_perror("mdbx_del(tail)", err);
          }
          if (unlikely(!keyvalue_maker.increment(tail_serial, 1)))
            failure("ttl: unexpected key-space overflow on the tail");
        }
        report(tail_count);
      }
    } else {
      log_trace("ttl: purge state");
      db_table_clear(dbi);
      fifo.clear();
      clear_wholetable_passed += 1;
      report(1);
    }

    err = breakable_restart();
    if (unlikely(err != MDBX_SUCCESS)) {
      log_notice("ttl: bailout at commit due '%s'", mdbx_strerror(err));
      break;
    }
    if (!speculum_verify()) {
      log_notice("ttl: bailout after tail-trim");
      return false;
    }

    if (!keyspace_overflow && (should_continue() || !clear_wholetable_passed ||
                               !clear_stepbystep_passed)) {
      unsigned underutilization_x256 =
          txn_underutilization_x256(txn_guard.get());
      if (dbfull_passed > underutilization_x256) {
        log_notice("ttl: skip head-grow to avoid one more dbfull (was %u, "
                   "underutilization %.2f%%)",
                   dbfull_passed, underutilization_x256 / 2.560);
        continue;
      }
      fifo.push_front(std::make_pair(serial, head_count));
    retry:
      for (unsigned n = 0; n < head_count; ++n) {
        log_trace("ttl: insert-head %" PRIu64, serial);
        generate_pair(serial);
        err = insert(key, data, insert_flags);
        if (unlikely(err != MDBX_SUCCESS)) {
          if ((err == MDBX_TXN_FULL || err == MDBX_MAP_FULL) &&
              config.params.ignore_dbfull) {
            log_notice("ttl: head-insert skip due '%s'", mdbx_strerror(err));
            txn_restart(true, false);
            serial = fifo.front().first;
            fifo.front().second = head_count = n;
            dbfull_passed += 1;
            goto retry;
          }
          failure_perror("mdbx_put(head)", err);
        }

        if (unlikely(!keyvalue_maker.increment(serial, 1))) {
          log_notice("ttl: unexpected key-space overflow");
          keyspace_overflow = true;
          txn_restart(true, false);
          serial = fifo.front().first;
          fifo.front().second = head_count = n;
          goto retry;
        }
      }
      err = breakable_restart();
      if (unlikely(err != MDBX_SUCCESS)) {
        log_notice("ttl: head-commit skip due '%s'", mdbx_strerror(err));
        serial = fifo.front().first;
        fifo.pop_front();
      }
      if (!speculum_verify()) {
        log_notice("ttl: bailout after head-grow");
        return false;
      }
      loops += 1;
    } else if (fifo.empty()) {
      log_notice("ttl: done %u whole loops, %" PRIu64 " ops, %" PRIu64 " items",
                 loops, nops_completed, serial);
      rc = true;
      break;
    } else {
      log_notice("ttl: done, wait for empty, skip head-grow");
    }
  }

bailout:
  if (!rc && err == MDBX_MAP_FULL && config.params.ignore_dbfull)
    rc = true;
  txn_end(true);
  if (dbi) {
    if (config.params.drop_table && !mode_readonly()) {
      txn_begin(false);
      db_table_drop(dbi);
      err = breakable_commit();
      if (unlikely(err != MDBX_SUCCESS)) {
        log_notice("ttl: bailout-clean due '%s'", mdbx_strerror(err));
        if (err != MDBX_MAP_FULL || !config.params.ignore_dbfull)
          rc = false;
      }
    } else
      db_table_close(dbi);
  }
  return rc;
}
