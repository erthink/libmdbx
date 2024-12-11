/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2024

#include "internals.h"

void spill_remove(MDBX_txn *txn, size_t idx, size_t npages) {
  tASSERT(txn, idx > 0 && idx <= MDBX_PNL_GETSIZE(txn->tw.spilled.list) && txn->tw.spilled.least_removed > 0);
  txn->tw.spilled.least_removed = (idx < txn->tw.spilled.least_removed) ? idx : txn->tw.spilled.least_removed;
  txn->tw.spilled.list[idx] |= 1;
  MDBX_PNL_SETSIZE(txn->tw.spilled.list,
                   MDBX_PNL_GETSIZE(txn->tw.spilled.list) - (idx == MDBX_PNL_GETSIZE(txn->tw.spilled.list)));

  while (unlikely(npages > 1)) {
    const pgno_t pgno = (txn->tw.spilled.list[idx] >> 1) + 1;
    if (MDBX_PNL_ASCENDING) {
      if (++idx > MDBX_PNL_GETSIZE(txn->tw.spilled.list) || (txn->tw.spilled.list[idx] >> 1) != pgno)
        return;
    } else {
      if (--idx < 1 || (txn->tw.spilled.list[idx] >> 1) != pgno)
        return;
      txn->tw.spilled.least_removed = (idx < txn->tw.spilled.least_removed) ? idx : txn->tw.spilled.least_removed;
    }
    txn->tw.spilled.list[idx] |= 1;
    MDBX_PNL_SETSIZE(txn->tw.spilled.list,
                     MDBX_PNL_GETSIZE(txn->tw.spilled.list) - (idx == MDBX_PNL_GETSIZE(txn->tw.spilled.list)));
    --npages;
  }
}

pnl_t spill_purge(MDBX_txn *txn) {
  tASSERT(txn, txn->tw.spilled.least_removed > 0);
  const pnl_t sl = txn->tw.spilled.list;
  if (txn->tw.spilled.least_removed != INT_MAX) {
    size_t len = MDBX_PNL_GETSIZE(sl), r, w;
    for (w = r = txn->tw.spilled.least_removed; r <= len; ++r) {
      sl[w] = sl[r];
      w += 1 - (sl[r] & 1);
    }
    for (size_t i = 1; i < w; ++i)
      tASSERT(txn, (sl[i] & 1) == 0);
    MDBX_PNL_SETSIZE(sl, w - 1);
    txn->tw.spilled.least_removed = INT_MAX;
  } else {
    for (size_t i = 1; i <= MDBX_PNL_GETSIZE(sl); ++i)
      tASSERT(txn, (sl[i] & 1) == 0);
  }
  return sl;
}

/*----------------------------------------------------------------------------*/

static int spill_page(MDBX_txn *txn, iov_ctx_t *ctx, page_t *dp, const size_t npages) {
  tASSERT(txn, !(txn->flags & MDBX_WRITEMAP));
#if MDBX_ENABLE_PGOP_STAT
  txn->env->lck->pgops.spill.weak += npages;
#endif /* MDBX_ENABLE_PGOP_STAT */
  const pgno_t pgno = dp->pgno;
  int err = iov_page(txn, ctx, dp, npages);
  if (likely(err == MDBX_SUCCESS))
    err = spill_append_span(&txn->tw.spilled.list, pgno, npages);
  return err;
}

/* Set unspillable LRU-label for dirty pages watched by txn.
 * Returns the number of pages marked as unspillable. */
static size_t spill_cursor_keep(const MDBX_txn *const txn, const MDBX_cursor *mc) {
  tASSERT(txn, (txn->flags & (MDBX_TXN_RDONLY | MDBX_WRITEMAP)) == 0);
  size_t keep = 0;
  while (!is_poor(mc)) {
    tASSERT(txn, mc->top >= 0);
    const page_t *mp;
    intptr_t i = 0;
    do {
      mp = mc->pg[i];
      tASSERT(txn, !is_subpage(mp));
      if (is_modifable(txn, mp)) {
        size_t const n = dpl_search(txn, mp->pgno);
        if (txn->tw.dirtylist->items[n].pgno == mp->pgno &&
            /* не считаем дважды */ dpl_age(txn, n)) {
          size_t *const ptr = ptr_disp(txn->tw.dirtylist->items[n].ptr, -(ptrdiff_t)sizeof(size_t));
          *ptr = txn->tw.dirtylru;
          tASSERT(txn, dpl_age(txn, n) == 0);
          ++keep;
        }
      }
    } while (++i <= mc->top);

    tASSERT(txn, is_leaf(mp));
    if (!mc->subcur || mc->ki[mc->top] >= page_numkeys(mp))
      break;
    if (!(node_flags(page_node(mp, mc->ki[mc->top])) & N_TREE))
      break;
    mc = &mc->subcur->cursor;
  }
  return keep;
}

static size_t spill_txn_keep(MDBX_txn *txn, MDBX_cursor *m0) {
  tASSERT(txn, (txn->flags & (MDBX_TXN_RDONLY | MDBX_WRITEMAP)) == 0);
  dpl_lru_turn(txn);
  size_t keep = m0 ? spill_cursor_keep(txn, m0) : 0;

  TXN_FOREACH_DBI_ALL(txn, dbi) {
    if (F_ISSET(txn->dbi_state[dbi], DBI_DIRTY | DBI_VALID) && txn->dbs[dbi].root != P_INVALID)
      for (MDBX_cursor *mc = txn->cursors[dbi]; mc; mc = mc->next)
        if (mc != m0)
          keep += spill_cursor_keep(txn, mc);
  }

  return keep;
}

/* Returns the spilling priority (0..255) for a dirty page:
 *      0 = should be spilled;
 *    ...
 *  > 255 = must not be spilled. */
MDBX_NOTHROW_PURE_FUNCTION static unsigned spill_prio(const MDBX_txn *txn, const size_t i, const uint32_t reciprocal) {
  dpl_t *const dl = txn->tw.dirtylist;
  const uint32_t age = dpl_age(txn, i);
  const size_t npages = dpl_npages(dl, i);
  const pgno_t pgno = dl->items[i].pgno;
  if (age == 0) {
    DEBUG("skip %s %zu page %" PRIaPGNO, "keep", npages, pgno);
    return 256;
  }

  page_t *const dp = dl->items[i].ptr;
  if (dp->flags & (P_LOOSE | P_SPILLED)) {
    DEBUG("skip %s %zu page %" PRIaPGNO, (dp->flags & P_LOOSE) ? "loose" : "parent-spilled", npages, pgno);
    return 256;
  }

  /* Can't spill twice,
   * make sure it's not already in a parent's spill list(s). */
  MDBX_txn *parent = txn->parent;
  if (parent && (parent->flags & MDBX_TXN_SPILLS)) {
    do
      if (spill_intersect(parent, pgno, npages)) {
        DEBUG("skip-2 parent-spilled %zu page %" PRIaPGNO, npages, pgno);
        dp->flags |= P_SPILLED;
        return 256;
      }
    while ((parent = parent->parent) != nullptr);
  }

  tASSERT(txn, age * (uint64_t)reciprocal < UINT32_MAX);
  unsigned prio = age * reciprocal >> 24;
  tASSERT(txn, prio < 256);
  if (likely(npages == 1))
    return prio = 256 - prio;

  /* make a large/overflow pages be likely to spill */
  size_t factor = npages | npages >> 1;
  factor |= factor >> 2;
  factor |= factor >> 4;
  factor |= factor >> 8;
  factor |= factor >> 16;
  factor = (size_t)prio * log2n_powerof2(factor + 1) + /* golden ratio */ 157;
  factor = (factor < 256) ? 255 - factor : 0;
  tASSERT(txn, factor < 256 && factor < (256 - prio));
  return prio = (unsigned)factor;
}

static size_t spill_gate(const MDBX_env *env, intptr_t part, const size_t total) {
  const intptr_t spill_min = env->options.spill_min_denominator
                                 ? (total + env->options.spill_min_denominator - 1) / env->options.spill_min_denominator
                                 : 1;
  const intptr_t spill_max =
      total - (env->options.spill_max_denominator ? total / env->options.spill_max_denominator : 0);
  part = (part < spill_max) ? part : spill_max;
  part = (part > spill_min) ? part : spill_min;
  eASSERT(env, part >= 0 && (size_t)part <= total);
  return (size_t)part;
}

__cold int spill_slowpath(MDBX_txn *const txn, MDBX_cursor *const m0, const intptr_t wanna_spill_entries,
                          const intptr_t wanna_spill_npages, const size_t need) {
  tASSERT(txn, (txn->flags & MDBX_TXN_RDONLY) == 0);

  int rc = MDBX_SUCCESS;
  if (unlikely(txn->tw.loose_count >=
               (txn->tw.dirtylist ? txn->tw.dirtylist->pages_including_loose : txn->tw.writemap_dirty_npages)))
    goto done;

  const size_t dirty_entries = txn->tw.dirtylist ? (txn->tw.dirtylist->length - txn->tw.loose_count) : 1;
  const size_t dirty_npages =
      (txn->tw.dirtylist ? txn->tw.dirtylist->pages_including_loose : txn->tw.writemap_dirty_npages) -
      txn->tw.loose_count;
  const size_t need_spill_entries = spill_gate(txn->env, wanna_spill_entries, dirty_entries);
  const size_t need_spill_npages = spill_gate(txn->env, wanna_spill_npages, dirty_npages);

  const size_t need_spill = (need_spill_entries > need_spill_npages) ? need_spill_entries : need_spill_npages;
  if (!need_spill)
    goto done;

  if (txn->flags & MDBX_WRITEMAP) {
    NOTICE("%s-spilling %zu dirty-entries, %zu dirty-npages", "msync", dirty_entries, dirty_npages);
    const MDBX_env *env = txn->env;
    tASSERT(txn, txn->tw.spilled.list == nullptr);
    rc = osal_msync(&txn->env->dxb_mmap, 0, pgno_align2os_bytes(env, txn->geo.first_unallocated), MDBX_SYNC_KICK);
    if (unlikely(rc != MDBX_SUCCESS))
      goto bailout;
#if MDBX_AVOID_MSYNC
    MDBX_ANALYSIS_ASSUME(txn->tw.dirtylist != nullptr);
    tASSERT(txn, dpl_check(txn));
    env->lck->unsynced_pages.weak += txn->tw.dirtylist->pages_including_loose - txn->tw.loose_count;
    dpl_clear(txn->tw.dirtylist);
    txn->tw.dirtyroom = env->options.dp_limit - txn->tw.loose_count;
    for (page_t *lp = txn->tw.loose_pages; lp != nullptr; lp = page_next(lp)) {
      tASSERT(txn, lp->flags == P_LOOSE);
      rc = dpl_append(txn, lp->pgno, lp, 1);
      if (unlikely(rc != MDBX_SUCCESS))
        goto bailout;
      MDBX_ASAN_UNPOISON_MEMORY_REGION(&page_next(lp), sizeof(page_t *));
      VALGRIND_MAKE_MEM_DEFINED(&page_next(lp), sizeof(page_t *));
    }
    tASSERT(txn, dpl_check(txn));
#else
    tASSERT(txn, txn->tw.dirtylist == nullptr);
    env->lck->unsynced_pages.weak += txn->tw.writemap_dirty_npages;
    txn->tw.writemap_spilled_npages += txn->tw.writemap_dirty_npages;
    txn->tw.writemap_dirty_npages = 0;
#endif /* MDBX_AVOID_MSYNC */
    goto done;
  }

  NOTICE("%s-spilling %zu dirty-entries, %zu dirty-npages", "write", need_spill_entries, need_spill_npages);
  MDBX_ANALYSIS_ASSUME(txn->tw.dirtylist != nullptr);
  tASSERT(txn, txn->tw.dirtylist->length - txn->tw.loose_count >= 1);
  tASSERT(txn, txn->tw.dirtylist->pages_including_loose - txn->tw.loose_count >= need_spill_npages);
  if (!txn->tw.spilled.list) {
    txn->tw.spilled.least_removed = INT_MAX;
    txn->tw.spilled.list = pnl_alloc(need_spill);
    if (unlikely(!txn->tw.spilled.list)) {
      rc = MDBX_ENOMEM;
    bailout:
      txn->flags |= MDBX_TXN_ERROR;
      return rc;
    }
  } else {
    /* purge deleted slots */
    spill_purge(txn);
    rc = pnl_reserve(&txn->tw.spilled.list, need_spill);
    (void)rc /* ignore since the resulting list may be shorter
     and pnl_append() will increase pnl on demand */
        ;
  }

  /* Сортируем чтобы запись на диск была полее последовательна */
  dpl_t *const dl = dpl_sort(txn);

  /* Preserve pages which may soon be dirtied again */
  const size_t unspillable = spill_txn_keep(txn, m0);
  if (unspillable + txn->tw.loose_count >= dl->length) {
#if xMDBX_DEBUG_SPILLING == 1 /* avoid false failure in debug mode  */
    if (likely(txn->tw.dirtyroom + txn->tw.loose_count >= need))
      return MDBX_SUCCESS;
#endif /* xMDBX_DEBUG_SPILLING */
    ERROR("all %zu dirty pages are unspillable since referenced "
          "by a cursor(s), use fewer cursors or increase "
          "MDBX_opt_txn_dp_limit",
          unspillable);
    goto done;
  }

  /* Подзадача: Вытолкнуть часть страниц на диск в соответствии с LRU,
   * но при этом учесть важные поправки:
   *  - лучше выталкивать старые large/overflow страницы, так будет освобождено
   *    больше памяти, а также так как они (в текущем понимании) гораздо реже
   *    повторно изменяются;
   *  - при прочих равных лучше выталкивать смежные страницы, так будет
   *    меньше I/O операций;
   *  - желательно потратить на это меньше времени чем std::partial_sort_copy;
   *
   * Решение:
   *  - Квантуем весь диапазон lru-меток до 256 значений и задействуем один
   *    проход 8-битного radix-sort. В результате получаем 256 уровней
   *    "свежести", в том числе значение lru-метки, старее которой страницы
   *    должны быть выгружены;
   *  - Двигаемся последовательно в сторону увеличения номеров страниц
   *    и выталкиваем страницы с lru-меткой старее отсекающего значения,
   *    пока не вытолкнем достаточно;
   *  - Встречая страницы смежные с выталкиваемыми для уменьшения кол-ва
   *    I/O операций выталкиваем и их, если они попадают в первую половину
   *    между выталкиваемыми и самыми свежими lru-метками;
   *  - дополнительно при сортировке умышленно старим large/overflow страницы,
   *    тем самым повышая их шансы на выталкивание. */

  /* get min/max of LRU-labels */
  uint32_t age_max = 0;
  for (size_t i = 1; i <= dl->length; ++i) {
    const uint32_t age = dpl_age(txn, i);
    age_max = (age_max >= age) ? age_max : age;
  }

  VERBOSE("lru-head %u, age-max %u", txn->tw.dirtylru, age_max);

  /* half of 8-bit radix-sort */
  pgno_t radix_entries[256], radix_npages[256];
  memset(&radix_entries, 0, sizeof(radix_entries));
  memset(&radix_npages, 0, sizeof(radix_npages));
  size_t spillable_entries = 0, spillable_npages = 0;
  const uint32_t reciprocal = (UINT32_C(255) << 24) / (age_max + 1);
  for (size_t i = 1; i <= dl->length; ++i) {
    const unsigned prio = spill_prio(txn, i, reciprocal);
    size_t *const ptr = ptr_disp(dl->items[i].ptr, -(ptrdiff_t)sizeof(size_t));
    TRACE("page %" PRIaPGNO ", lru %zu, is_multi %c, npages %u, age %u of %u, prio %u", dl->items[i].pgno, *ptr,
          (dl->items[i].npages > 1) ? 'Y' : 'N', dpl_npages(dl, i), dpl_age(txn, i), age_max, prio);
    if (prio < 256) {
      radix_entries[prio] += 1;
      spillable_entries += 1;
      const pgno_t npages = dpl_npages(dl, i);
      radix_npages[prio] += npages;
      spillable_npages += npages;
    }
  }

  tASSERT(txn, spillable_npages >= spillable_entries);
  pgno_t spilled_entries = 0, spilled_npages = 0;
  if (likely(spillable_entries > 0)) {
    size_t prio2spill = 0, prio2adjacent = 128, amount_entries = radix_entries[0], amount_npages = radix_npages[0];
    for (size_t i = 1; i < 256; i++) {
      if (amount_entries < need_spill_entries || amount_npages < need_spill_npages) {
        prio2spill = i;
        prio2adjacent = i + (257 - i) / 2;
        amount_entries += radix_entries[i];
        amount_npages += radix_npages[i];
      } else if (amount_entries + amount_entries < spillable_entries + need_spill_entries
                 /* РАВНОЗНАЧНО: amount - need_spill < spillable - amount */
                 || amount_npages + amount_npages < spillable_npages + need_spill_npages) {
        prio2adjacent = i;
        amount_entries += radix_entries[i];
        amount_npages += radix_npages[i];
      } else
        break;
    }

    VERBOSE("prio2spill %zu, prio2adjacent %zu, spillable %zu/%zu,"
            " wanna-spill %zu/%zu, amount %zu/%zu",
            prio2spill, prio2adjacent, spillable_entries, spillable_npages, need_spill_entries, need_spill_npages,
            amount_entries, amount_npages);
    tASSERT(txn, prio2spill < prio2adjacent && prio2adjacent <= 256);

    iov_ctx_t ctx;
    rc = iov_init(txn, &ctx, amount_entries, amount_npages,
#if defined(_WIN32) || defined(_WIN64)
                  txn->env->ioring.overlapped_fd ? txn->env->ioring.overlapped_fd :
#endif
                                                 txn->env->lazy_fd,
                  true);
    if (unlikely(rc != MDBX_SUCCESS))
      goto bailout;

    size_t r = 0, w = 0;
    pgno_t last = 0;
    while (r < dl->length && (spilled_entries < need_spill_entries || spilled_npages < need_spill_npages)) {
      dl->items[++w] = dl->items[++r];
      unsigned prio = spill_prio(txn, w, reciprocal);
      if (prio > prio2spill && (prio >= prio2adjacent || last != dl->items[w].pgno))
        continue;

      const size_t e = w;
      last = dpl_endpgno(dl, w);
      while (--w && dpl_endpgno(dl, w) == dl->items[w + 1].pgno && spill_prio(txn, w, reciprocal) < prio2adjacent)
        ;

      for (size_t i = w; ++i <= e;) {
        const unsigned npages = dpl_npages(dl, i);
        prio = spill_prio(txn, i, reciprocal);
        DEBUG("%sspill[%zu] %u page %" PRIaPGNO " (age %d, prio %u)", (prio > prio2spill) ? "co-" : "", i, npages,
              dl->items[i].pgno, dpl_age(txn, i), prio);
        tASSERT(txn, prio < 256);
        ++spilled_entries;
        spilled_npages += npages;
        rc = spill_page(txn, &ctx, dl->items[i].ptr, npages);
        if (unlikely(rc != MDBX_SUCCESS))
          goto failed;
      }
    }

    VERBOSE("spilled entries %u, spilled npages %u", spilled_entries, spilled_npages);
    tASSERT(txn, spillable_entries == 0 || spilled_entries > 0);
    tASSERT(txn, spilled_npages >= spilled_entries);

  failed:
    while (r < dl->length)
      dl->items[++w] = dl->items[++r];
    tASSERT(txn, r - w == spilled_entries || rc != MDBX_SUCCESS);

    dl->sorted = dpl_setlen(dl, w);
    txn->tw.dirtyroom += spilled_entries;
    txn->tw.dirtylist->pages_including_loose -= spilled_npages;
    tASSERT(txn, dpl_check(txn));

    if (!iov_empty(&ctx)) {
      tASSERT(txn, rc == MDBX_SUCCESS);
      rc = iov_write(&ctx);
    }
    if (unlikely(rc != MDBX_SUCCESS))
      goto bailout;

    txn->env->lck->unsynced_pages.weak += spilled_npages;
    pnl_sort(txn->tw.spilled.list, (size_t)txn->geo.first_unallocated << 1);
    txn->flags |= MDBX_TXN_SPILLS;
    NOTICE("spilled %u dirty-entries, %u dirty-npages, now have %zu dirty-room", spilled_entries, spilled_npages,
           txn->tw.dirtyroom);
  } else {
    tASSERT(txn, rc == MDBX_SUCCESS);
    for (size_t i = 1; i <= dl->length; ++i) {
      page_t *dp = dl->items[i].ptr;
      VERBOSE("unspillable[%zu]: pgno %u, npages %u, flags 0x%04X, age %u, prio %u", i, dp->pgno, dpl_npages(dl, i),
              dp->flags, dpl_age(txn, i), spill_prio(txn, i, reciprocal));
    }
  }

#if xMDBX_DEBUG_SPILLING == 2
  if (txn->tw.loose_count + txn->tw.dirtyroom <= need / 2 + 1)
    ERROR("dirty-list length: before %zu, after %zu, parent %zi, loose %zu; "
          "needed %zu, spillable %zu; "
          "spilled %u dirty-entries, now have %zu dirty-room",
          dl->length + spilled_entries, dl->length,
          (txn->parent && txn->parent->tw.dirtylist) ? (intptr_t)txn->parent->tw.dirtylist->length : -1,
          txn->tw.loose_count, need, spillable_entries, spilled_entries, txn->tw.dirtyroom);
  ENSURE(txn->env, txn->tw.loose_count + txn->tw.dirtyroom > need / 2);
#endif /* xMDBX_DEBUG_SPILLING */

done:
  return likely(txn->tw.dirtyroom + txn->tw.loose_count > ((need > CURSOR_STACK_SIZE) ? CURSOR_STACK_SIZE : need))
             ? MDBX_SUCCESS
             : MDBX_TXN_FULL;
}
