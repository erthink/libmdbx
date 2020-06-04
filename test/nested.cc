/*
 * Copyright 2017-2020 Leonid Yuriev <leo@yuriev.ru>
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
#include <cmath>

/* LY: тест "эмуляцией time-to-live" с вложенными транзакциями:
 *  - организуется "скользящее окно", которое каждую транзакцию сдвигается
 *    вперед вдоль числовой оси.
 *  - по переднему краю "скользящего окна" записи добавляются в таблицу,
 *    а по заднему удаляются.
 *  - количество добавляемых/удаляемых записей псевдослучайно зависит
 *    от номера транзакции, но с экспоненциальным распределением.
 *  - размер "скользящего окна" также псевдослучайно зависит от номера
 *    транзакции с "отрицательным" экспоненциальным распределением
 *    MAX_WIDTH - exp(rnd(N)), при уменьшении окна сдвигается задний
 *    край и удаляются записи позади него.
 *  - групповое добавление данных в начало окна и групповое удаление в конце,
 *    преимущественно выполняются во вложенных транзакциях.
 *  - меньшая часть запускаемых вложенных транзакций отменяется, с последующим
 *    продолжением итераций с состояния предыдущиего коммита.
 *
 *  Таким образом имитируется поведение таблицы с TTL: записи стохастически
 *  добавляются и удаляются, и изредка происходят массивные удаления. */

bool testcase_nested::setup() {
  if (!inherited::setup())
    return false;
  int err = db_open__begin__table_create_open_clean(dbi);
  if (unlikely(err != MDBX_SUCCESS)) {
    log_notice("nested: bailout-prepare due '%s'", mdbx_strerror(err));
    return false;
  }

  keyvalue_maker.setup(config.params, config.actor_id, 0 /* thread_number */);
  key = keygen::alloc(config.params.keylen_max);
  data = keygen::alloc(config.params.datalen_max);
  serial = 0;
  fifo.clear();
  speculum.clear();
  assert(stack.empty());
  stack.emplace(nullptr, serial, fifo, speculum);
  return true;
}

bool testcase_nested::teardown() {
  while (!stack.empty())
    pop_txn(true);

  bool ok = true;
  if (dbi) {
    if (config.params.drop_table && !mode_readonly()) {
      txn_begin(false);
      db_table_drop(dbi);
      int err = breakable_commit();
      if (unlikely(err != MDBX_SUCCESS)) {
        log_notice("nested: bailout-clean due '%s'", mdbx_strerror(err));
        ok = false;
      }
    } else
      db_table_close(dbi);
    dbi = 0;
  }
  return inherited::teardown() && ok;
}

void testcase_nested::push_txn() {
  MDBX_txn *txn;
  unsigned flags =
      prng32() & (MDBX_SAFE_NOSYNC | MDBX_NOMETASYNC | MDBX_MAPASYNC);
  int err = mdbx_txn_begin(db_guard.get(), txn_guard.get(), flags, &txn);
  if (unlikely(err != MDBX_SUCCESS))
    failure_perror("mdbx_txn_begin(nested)", err);
  stack.emplace(scoped_txn_guard(txn), serial, fifo, speculum);
  std::swap(txn_guard, std::get<0>(stack.top()));
  log_verbose("begin level#%zu txn #%" PRIu64 ", flags 0x%x, serial %" PRIu64,
              stack.size(), mdbx_txn_id(txn), flags, serial);
}

bool testcase_nested::pop_txn(bool abort) {
  assert(txn_guard && !stack.empty());
  bool should_continue = true;
  MDBX_txn *txn = txn_guard.release();
  bool commited = false;
  if (abort) {
    log_verbose(
        "abort level#%zu txn #%" PRIu64 ", undo serial %" PRIu64 " <- %" PRIu64,
        stack.size(), mdbx_txn_id(txn), serial, std::get<1>(stack.top()));
    int err = mdbx_txn_abort(txn);
    if (unlikely(err != MDBX_SUCCESS))
      failure_perror("mdbx_txn_abort()", err);
  } else {
    log_verbose("commit level#%zu txn, nested serial %" PRIu64 " -> %" PRIu64,
                stack.size(), serial, std::get<1>(stack.top()));
    int err = mdbx_txn_commit(txn);
    if (likely(err == MDBX_SUCCESS))
      commited = true;
    else {
      should_continue = false;
      if (err == MDBX_MAP_FULL && config.params.ignore_dbfull) {
        err = mdbx_txn_abort(txn);
        if (unlikely(err != MDBX_SUCCESS && err != MDBX_THREAD_MISMATCH &&
                     err != MDBX_BAD_TXN))
          failure_perror("mdbx_txn_abort()", err);
      } else
        failure_perror("mdbx_txn_commit()", err);
    }
  }

  std::swap(txn_guard, std::get<0>(stack.top()));
  if (!commited) {
    serial = std::get<1>(stack.top());
    std::swap(fifo, std::get<2>(stack.top()));
    std::swap(speculum, std::get<3>(stack.top()));
  }
  stack.pop();
  return should_continue;
}

bool testcase_nested::stochastic_breakable_restart_with_nested(
    bool force_restart) {
  log_trace(">> stochastic_breakable_restart_with_nested%s",
            force_restart ? ": force_restart" : "");

  if (force_restart)
    while (txn_guard)
      pop_txn(true);

  bool should_continue = true;
  while (!stack.empty() &&
         (flipcoin() || txn_underutilization_x256(txn_guard.get()) < 42))
    should_continue &= pop_txn();

  if (should_continue)
    while (stack.empty() ||
           (is_nested_txn_available() && flipcoin() && stack.size() < 5))
      push_txn();

  log_trace("<< stochastic_breakable_restart_with_nested: should_continue=%s",
            should_continue ? "yes" : "no");
  return should_continue;
}

bool testcase_nested::trim_tail(unsigned window_width) {
  if (window_width || flipcoin()) {
    clear_stepbystep_passed += window_width == 0;
    while (fifo.size() > window_width) {
      uint64_t tail_serial = fifo.back().first;
      const unsigned tail_count = fifo.back().second;
      log_verbose("nested: pop-tail (serial %" PRIu64 ", count %u)",
                  tail_serial, tail_count);
      fifo.pop_back();
      for (unsigned n = 0; n < tail_count; ++n) {
        log_trace("nested: remove-tail %" PRIu64, tail_serial);
        generate_pair(tail_serial);
        int err = remove(key, data);
        if (unlikely(err != MDBX_SUCCESS)) {
          if (err == MDBX_MAP_FULL && config.params.ignore_dbfull) {
            log_notice("nested: tail-bailout due '%s'", mdbx_strerror(err));
            return false;
          }
          failure_perror("mdbx_del(tail)", err);
        }
        if (unlikely(!keyvalue_maker.increment(tail_serial, 1)))
          failure("nested: unexpected key-space overflow on the tail");
      }
      report(tail_count);
    }
  } else if (!fifo.empty()) {
    log_verbose("nested: purge state %" PRIu64 " - %" PRIu64 ", fifo-items %zu",
                fifo.front().first, fifo.back().first + fifo.back().second,
                fifo.size());
    db_table_clear(dbi, txn_guard.get());
    fifo.clear();
    clear_wholetable_passed += 1;
    report(1);
  }
  return true;
}

bool testcase_nested::grow_head(unsigned head_count) {
  const unsigned insert_flags = (config.params.table_flags & MDBX_DUPSORT)
                                    ? MDBX_NODUPDATA
                                    : MDBX_NODUPDATA | MDBX_NOOVERWRITE;
retry:
  fifo.push_front(std::make_pair(serial, head_count));
  for (unsigned n = 0; n < head_count; ++n) {
    log_trace("nested: insert-head %" PRIu64, serial);
    generate_pair(serial);
    int err = insert(key, data, insert_flags);
    if (unlikely(err != MDBX_SUCCESS)) {
      if (err == MDBX_MAP_FULL && config.params.ignore_dbfull) {
        log_notice("nested: head-insert skip due '%s'", mdbx_strerror(err));
        head_count = n;
        stochastic_breakable_restart_with_nested(true);
        dbfull_passed += 1;
        goto retry;
      }
      failure_perror("mdbx_put(head)", err);
    }

    if (unlikely(!keyvalue_maker.increment(serial, 1))) {
      log_notice("nested: unexpected key-space overflow");
      keyspace_overflow = true;
      head_count = n;
      stochastic_breakable_restart_with_nested(true);
      goto retry;
    }
  }

  return true;
}

bool testcase_nested::run() {
  uint64_t seed =
      prng64_map2_white(config.params.keygen.seed) + config.actor_id;

  clear_wholetable_passed = 0;
  clear_stepbystep_passed = 0;
  dbfull_passed = 0;
  unsigned loops = 0;
  while (true) {
    const uint64_t salt = prng64_white(seed) /* mdbx_txn_id(txn_guard.get()) */;
    const unsigned window_width =
        (!should_continue() || flipcoin_x4()) ? 0 : edge2window(salt);
    const unsigned head_count = edge2count(salt);
    log_debug("nested: step #%" PRIu64 " (serial %" PRIu64
              ", window %u, count %u) salt %" PRIu64,
              nops_completed, serial, window_width, head_count, salt);

    if (!trim_tail(window_width))
      return false;
    if (!stochastic_breakable_restart_with_nested()) {
      log_notice("nested: bailout at commit/restart after tail-trim");
      return false;
    }
    if (!speculum_verify()) {
      log_notice("nested: bailout after tail-trim");
      return false;
    }

    if (!keyspace_overflow && (should_continue() || !clear_wholetable_passed ||
                               !clear_stepbystep_passed)) {
      unsigned underutilization_x256 =
          txn_underutilization_x256(txn_guard.get());
      if (dbfull_passed > underutilization_x256) {
        log_notice("nested: skip head-grow to avoid one more dbfull (was %u, "
                   "underutilization %.2f%%)",
                   dbfull_passed, underutilization_x256 / 2.560);
        continue;
      }
      if (!grow_head(head_count))
        return false;
      if (!stochastic_breakable_restart_with_nested())
        log_notice("nested: skip commit/restart after head-grow");
      if (!speculum_verify()) {
        log_notice("nested: bailout after head-grow");
        return false;
      }
      loops += 1;
    } else if (fifo.empty()) {
      log_notice("nested: done %u whole loops, %" PRIu64 " ops, %" PRIu64
                 " items",
                 loops, nops_completed, serial);
      break;
    } else {
      log_notice("nested: done, wait for empty, skip head-grow");
    }
  }

  while (!stack.empty())
    pop_txn(false);

  return speculum_verify();
}
