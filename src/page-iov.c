/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025

#include "internals.h"

int iov_init(MDBX_txn *const txn, iov_ctx_t *ctx, size_t items, size_t npages, mdbx_filehandle_t fd,
             bool check_coherence) {
  ctx->env = txn->env;
  ctx->ior = &txn->env->ioring;
  ctx->fd = fd;
  ctx->coherency_timestamp =
      (check_coherence || txn->env->lck->pgops.incoherence.weak) ? 0 : UINT64_MAX /* не выполнять сверку */;
  ctx->err = osal_ioring_prepare(ctx->ior, items, pgno_align2os_bytes(txn->env, npages));
  if (likely(ctx->err == MDBX_SUCCESS)) {
#if MDBX_NEED_WRITTEN_RANGE
    ctx->flush_begin = MAX_PAGENO;
    ctx->flush_end = MIN_PAGENO;
#endif /* MDBX_NEED_WRITTEN_RANGE */
    osal_ioring_reset(ctx->ior);
  }
  return ctx->err;
}

static void iov_callback4dirtypages(iov_ctx_t *ctx, size_t offset, void *data, size_t bytes) {
  MDBX_env *const env = ctx->env;
  eASSERT(env, (env->flags & MDBX_WRITEMAP) == 0);

  page_t *wp = (page_t *)data;
  eASSERT(env, wp->pgno == bytes2pgno(env, offset));
  eASSERT(env, bytes2pgno(env, bytes) >= (is_largepage(wp) ? wp->pages : 1u));
  eASSERT(env, (wp->flags & P_ILL_BITS) == 0);

  if (likely(ctx->err == MDBX_SUCCESS)) {
    const page_t *const rp = ptr_disp(env->dxb_mmap.base, offset);
    VALGRIND_MAKE_MEM_DEFINED(rp, bytes);
    MDBX_ASAN_UNPOISON_MEMORY_REGION(rp, bytes);
    osal_flush_incoherent_mmap(rp, bytes, globals.sys_pagesize);
    /* check with timeout as the workaround
     * for https://libmdbx.dqdkfa.ru/dead-github/issues/269
     *
     * Проблема проявляется только при неупорядоченности: если записанная
     * последней мета-страница "обгоняет" ранее записанные, т.е. когда
     * записанное в файл позже становится видимым в отображении раньше,
     * чем записанное ранее.
     *
     * Исходно здесь всегда выполнялась полная сверка. Это давало полную
     * гарантию защиты от проявления проблемы, но порождало накладные расходы.
     * В некоторых сценариях наблюдалось снижение производительности до 10-15%,
     * а в синтетических тестах до 30%. Конечно никто не вникал в причины,
     * а просто останавливался на мнении "libmdbx не быстрее LMDB",
     * например: https://clck.ru/3386er
     *
     * Поэтому после серии экспериментов и тестов реализовано следующее:
     * 0. Посредством опции сборки MDBX_FORCE_CHECK_MMAP_COHERENCY=1
     *    можно включить полную сверку после записи.
     *    Остальные пункты являются взвешенным компромиссом между полной
     *    гарантией обнаружения проблемы и бесполезными затратами на системах
     *    без этого недостатка.
     * 1. При старте транзакций проверяется соответствие выбранной мета-страницы
     *    корневым страницам b-tree проверяется. Эта проверка показала себя
     *    достаточной без сверки после записи. При обнаружении "некогерентности"
     *    эти случаи подсчитываются, а при их ненулевом счетчике выполняется
     *    полная сверка. Таким образом, произойдет переключение в режим полной
     *    сверки, если показавшая себя достаточной проверка заметит проявление
     *    проблемы хоты-бы раз.
     * 2. Сверка не выполняется при фиксации транзакции, так как:
     *    - при наличии проблемы "не-когерентности" (при отложенном копировании
     *      или обновлении PTE, после возврата из write-syscall), проверка
     *      в этом процессе не гарантирует актуальность данных в другом
     *      процессе, который может запустить транзакцию сразу после коммита;
     *    - сверка только последнего блока позволяет почти восстановить
     *      производительность в больших транзакциях, но одновременно размывает
     *      уверенность в отсутствии сбоев, чем обесценивает всю затею;
     *    - после записи данных будет записана мета-страница, соответствие
     *      которой корневым страницам b-tree проверяется при старте
     *      транзакций, и только эта проверка показала себя достаточной;
     * 3. При спиллинге производится полная сверка записанных страниц. Тут был
     *    соблазн сверять не полностью, а например начало и конец каждого блока.
     *    Но при спиллинге возможна ситуация повторного вытеснения страниц, в
     *    том числе large/overflow. При этом возникает риск прочитать в текущей
     *    транзакции старую версию страницы, до повторной записи. В этом случае
     *    могут возникать крайне редкие невоспроизводимые ошибки. С учетом того
     *    что спиллинг выполняет крайне редко, решено отказаться от экономии
     *    в пользу надежности. */
#ifndef MDBX_FORCE_CHECK_MMAP_COHERENCY
#define MDBX_FORCE_CHECK_MMAP_COHERENCY 0
#endif /* MDBX_FORCE_CHECK_MMAP_COHERENCY */
    if ((MDBX_FORCE_CHECK_MMAP_COHERENCY || ctx->coherency_timestamp != UINT64_MAX) &&
        unlikely(memcmp(wp, rp, bytes))) {
      ctx->coherency_timestamp = 0;
      env->lck->pgops.incoherence.weak =
          (env->lck->pgops.incoherence.weak >= INT32_MAX) ? INT32_MAX : env->lck->pgops.incoherence.weak + 1;
      WARNING("catch delayed/non-arrived page %" PRIaPGNO " %s", wp->pgno,
              "(workaround for incoherent flaw of unified page/buffer cache)");
      do
        if (coherency_timeout(&ctx->coherency_timestamp, wp->pgno, env) != MDBX_RESULT_TRUE) {
          ctx->err = MDBX_PROBLEM;
          break;
        }
      while (unlikely(memcmp(wp, rp, bytes)));
    }
  }

  if (likely(bytes == env->ps))
    page_shadow_release(env, wp, 1);
  else {
    do {
      eASSERT(env, wp->pgno == bytes2pgno(env, offset));
      eASSERT(env, (wp->flags & P_ILL_BITS) == 0);
      size_t npages = is_largepage(wp) ? wp->pages : 1u;
      size_t chunk = pgno2bytes(env, npages);
      eASSERT(env, bytes >= chunk);
      page_t *next = ptr_disp(wp, chunk);
      page_shadow_release(env, wp, npages);
      wp = next;
      offset += chunk;
      bytes -= chunk;
    } while (bytes);
  }
}

static void iov_complete(iov_ctx_t *ctx) {
  if ((ctx->env->flags & MDBX_WRITEMAP) == 0)
    osal_ioring_walk(ctx->ior, ctx, iov_callback4dirtypages);
  osal_ioring_reset(ctx->ior);
}

int iov_write(iov_ctx_t *ctx) {
  eASSERT(ctx->env, !iov_empty(ctx));
  osal_ioring_write_result_t r = osal_ioring_write(ctx->ior, ctx->fd);
#if MDBX_ENABLE_PGOP_STAT
  ctx->env->lck->pgops.wops.weak += r.wops;
#endif /* MDBX_ENABLE_PGOP_STAT */
  ctx->err = r.err;
  if (unlikely(ctx->err != MDBX_SUCCESS))
    ERROR("Write error: %s", mdbx_strerror(ctx->err));
  iov_complete(ctx);
  return ctx->err;
}

int iov_page(MDBX_txn *txn, iov_ctx_t *ctx, page_t *dp, size_t npages) {
  MDBX_env *const env = txn->env;
  tASSERT(txn, ctx->err == MDBX_SUCCESS);
  tASSERT(txn, dp->pgno >= MIN_PAGENO && dp->pgno < txn->geo.first_unallocated);
  tASSERT(txn, is_modifable(txn, dp));
  tASSERT(txn, !(dp->flags & ~(P_BRANCH | P_LEAF | P_DUPFIX | P_LARGE)));

  if (is_shadowed(txn, dp)) {
    tASSERT(txn, !(txn->flags & MDBX_WRITEMAP));
    dp->txnid = txn->txnid;
    tASSERT(txn, is_spilled(txn, dp));
#if MDBX_AVOID_MSYNC
  doit:;
#endif /* MDBX_AVOID_MSYNC */
    int err = osal_ioring_add(ctx->ior, pgno2bytes(env, dp->pgno), dp, pgno2bytes(env, npages));
    if (unlikely(err != MDBX_SUCCESS)) {
      ctx->err = err;
      if (unlikely(err != MDBX_RESULT_TRUE)) {
        iov_complete(ctx);
        return err;
      }
      err = iov_write(ctx);
      tASSERT(txn, iov_empty(ctx));
      if (likely(err == MDBX_SUCCESS)) {
        err = osal_ioring_add(ctx->ior, pgno2bytes(env, dp->pgno), dp, pgno2bytes(env, npages));
        if (unlikely(err != MDBX_SUCCESS)) {
          iov_complete(ctx);
          return ctx->err = err;
        }
      }
      tASSERT(txn, ctx->err == MDBX_SUCCESS);
    }
  } else {
    tASSERT(txn, txn->flags & MDBX_WRITEMAP);
#if MDBX_AVOID_MSYNC
    goto doit;
#endif /* MDBX_AVOID_MSYNC */
  }

#if MDBX_NEED_WRITTEN_RANGE
  ctx->flush_begin = (ctx->flush_begin < dp->pgno) ? ctx->flush_begin : dp->pgno;
  ctx->flush_end = (ctx->flush_end > dp->pgno + (pgno_t)npages) ? ctx->flush_end : dp->pgno + (pgno_t)npages;
#endif /* MDBX_NEED_WRITTEN_RANGE */
  return MDBX_SUCCESS;
}
