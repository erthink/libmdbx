/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025

#include "internals.h"

MDBX_txn *env_owned_wrtxn(const MDBX_env *env) {
  if (likely(env->basal_txn)) {
    const bool is_owned = (env->flags & MDBX_NOSTICKYTHREADS) ? (env->basal_txn->owner != 0)
                                                              : (env->basal_txn->owner == osal_thread_self());
    if (is_owned)
      return env->txn ? env->txn : env->basal_txn;
  }
  return nullptr;
}

int env_page_auxbuffer(MDBX_env *env) {
  const int err = env->page_auxbuf
                      ? MDBX_SUCCESS
                      : osal_memalign_alloc(globals.sys_pagesize, env->ps * (size_t)NUM_METAS, &env->page_auxbuf);
  if (likely(err == MDBX_SUCCESS)) {
    memset(env->page_auxbuf, -1, env->ps * (size_t)2);
    memset(ptr_disp(env->page_auxbuf, env->ps * (size_t)2), 0, env->ps);
  }
  return err;
}

__cold unsigned env_setup_pagesize(MDBX_env *env, const size_t pagesize) {
  STATIC_ASSERT(PTRDIFF_MAX > MAX_MAPSIZE);
  STATIC_ASSERT(MDBX_MIN_PAGESIZE > sizeof(page_t) + sizeof(meta_t));
  ENSURE(env, is_powerof2(pagesize));
  ENSURE(env, pagesize >= MDBX_MIN_PAGESIZE);
  ENSURE(env, pagesize <= MDBX_MAX_PAGESIZE);
  ENSURE(env, !env->page_auxbuf && env->ps != pagesize);
  env->ps = (unsigned)pagesize;

  STATIC_ASSERT(MAX_GC1OVPAGE(MDBX_MIN_PAGESIZE) > 4);
  STATIC_ASSERT(MAX_GC1OVPAGE(MDBX_MAX_PAGESIZE) < PAGELIST_LIMIT);
  const intptr_t maxgc_ov1page = (pagesize - PAGEHDRSZ) / sizeof(pgno_t) - 1;
  ENSURE(env, maxgc_ov1page > 42 && maxgc_ov1page < (intptr_t)PAGELIST_LIMIT / 4);
  env->maxgc_large1page = (unsigned)maxgc_ov1page;
  env->maxgc_per_branch = (unsigned)((pagesize - PAGEHDRSZ) / (sizeof(indx_t) + sizeof(node_t) + sizeof(txnid_t)));

  STATIC_ASSERT(LEAF_NODE_MAX(MDBX_MIN_PAGESIZE) > sizeof(tree_t) + NODESIZE + 42);
  STATIC_ASSERT(LEAF_NODE_MAX(MDBX_MAX_PAGESIZE) < UINT16_MAX);
  STATIC_ASSERT(LEAF_NODE_MAX(MDBX_MIN_PAGESIZE) >= BRANCH_NODE_MAX(MDBX_MIN_PAGESIZE));
  STATIC_ASSERT(BRANCH_NODE_MAX(MDBX_MAX_PAGESIZE) > NODESIZE + 42);
  STATIC_ASSERT(BRANCH_NODE_MAX(MDBX_MAX_PAGESIZE) < UINT16_MAX);
  const intptr_t branch_nodemax = BRANCH_NODE_MAX(pagesize);
  const intptr_t leaf_nodemax = LEAF_NODE_MAX(pagesize);
  ENSURE(env, branch_nodemax > (intptr_t)(NODESIZE + 42) && branch_nodemax % 2 == 0 &&
                  leaf_nodemax > (intptr_t)(sizeof(tree_t) + NODESIZE + 42) && leaf_nodemax >= branch_nodemax &&
                  leaf_nodemax < (int)UINT16_MAX && leaf_nodemax % 2 == 0);
  env->leaf_nodemax = (uint16_t)leaf_nodemax;
  env->branch_nodemax = (uint16_t)branch_nodemax;
  env->ps2ln = (uint8_t)log2n_powerof2(pagesize);
  eASSERT(env, pgno2bytes(env, 1) == pagesize);
  eASSERT(env, bytes2pgno(env, pagesize + pagesize) == 2);
  recalculate_merge_thresholds(env);
  recalculate_subpage_thresholds(env);
  env_options_adjust_dp_limit(env);
  return env->ps;
}

__cold int env_sync(MDBX_env *env, bool force, bool nonblock) {
  if (unlikely(env->flags & MDBX_RDONLY))
    return MDBX_EACCESS;

  MDBX_txn *const txn_owned = env_owned_wrtxn(env);
  bool should_unlock = false;
  int rc = MDBX_RESULT_TRUE /* means "nothing to sync" */;

retry:;
  unsigned flags = env->flags & ~(MDBX_NOMETASYNC | txn_shrink_allowed);
  if (unlikely((flags & (ENV_FATAL_ERROR | ENV_ACTIVE)) != ENV_ACTIVE)) {
    rc = (flags & ENV_FATAL_ERROR) ? MDBX_PANIC : MDBX_EPERM;
    goto bailout;
  }

  const troika_t troika = (txn_owned || should_unlock) ? env->basal_txn->wr.troika : meta_tap(env);
  const meta_ptr_t head = meta_recent(env, &troika);
  const uint64_t unsynced_pages = atomic_load64(&env->lck->unsynced_pages, mo_Relaxed);
  if (unsynced_pages == 0) {
    const uint32_t synched_meta_txnid_u32 = atomic_load32(&env->lck->meta_sync_txnid, mo_Relaxed);
    if (synched_meta_txnid_u32 == (uint32_t)head.txnid && head.is_steady)
      goto bailout;
  }

  if (should_unlock && (env->flags & MDBX_WRITEMAP) &&
      unlikely(head.ptr_c->geometry.first_unallocated > bytes2pgno(env, env->dxb_mmap.current))) {

    if (unlikely(env->stuck_meta >= 0) && troika.recent != (uint8_t)env->stuck_meta) {
      NOTICE("skip %s since wagering meta-page (%u) is mispatch the recent "
             "meta-page (%u)",
             "sync datafile", env->stuck_meta, troika.recent);
      rc = MDBX_RESULT_TRUE;
    } else {
      rc = dxb_resize(env, head.ptr_c->geometry.first_unallocated, head.ptr_c->geometry.now, head.ptr_c->geometry.upper,
                      implicit_grow);
      if (unlikely(rc != MDBX_SUCCESS))
        goto bailout;
    }
  }

  const size_t autosync_threshold = atomic_load32(&env->lck->autosync_threshold, mo_Relaxed);
  const uint64_t autosync_period = atomic_load64(&env->lck->autosync_period, mo_Relaxed);
  uint64_t eoos_timestamp;
  if (force || (autosync_threshold && unsynced_pages >= autosync_threshold) ||
      (autosync_period && (eoos_timestamp = atomic_load64(&env->lck->eoos_timestamp, mo_Relaxed)) &&
       osal_monotime() - eoos_timestamp >= autosync_period))
    flags &= MDBX_WRITEMAP /* clear flags for full steady sync */;

  if (!txn_owned) {
    if (!should_unlock) {
#if MDBX_ENABLE_PGOP_STAT
      unsigned wops = 0;
#endif /* MDBX_ENABLE_PGOP_STAT */

      int err;
      /* pre-sync to avoid latency for writer */
      if (unsynced_pages > /* FIXME: define threshold */ 42 && (flags & MDBX_SAFE_NOSYNC) == 0) {
        eASSERT(env, ((flags ^ env->flags) & MDBX_WRITEMAP) == 0);
        if (flags & MDBX_WRITEMAP) {
          /* Acquire guard to avoid collision with remap */
#if defined(_WIN32) || defined(_WIN64)
          imports.srwl_AcquireShared(&env->remap_guard);
#else
          err = osal_fastmutex_acquire(&env->remap_guard);
          if (unlikely(err != MDBX_SUCCESS))
            return err;
#endif
          const size_t usedbytes = pgno_align2os_bytes(env, head.ptr_c->geometry.first_unallocated);
          err = osal_msync(&env->dxb_mmap, 0, usedbytes, MDBX_SYNC_DATA);
#if defined(_WIN32) || defined(_WIN64)
          imports.srwl_ReleaseShared(&env->remap_guard);
#else
          int unlock_err = osal_fastmutex_release(&env->remap_guard);
          if (unlikely(unlock_err != MDBX_SUCCESS) && err == MDBX_SUCCESS)
            err = unlock_err;
#endif
        } else
          err = osal_fsync(env->lazy_fd, MDBX_SYNC_DATA);

        if (unlikely(err != MDBX_SUCCESS))
          return err;

#if MDBX_ENABLE_PGOP_STAT
        wops = 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
        /* pre-sync done */
        rc = MDBX_SUCCESS /* means "some data was synced" */;
      }

      err = lck_txn_lock(env, nonblock);
      if (unlikely(err != MDBX_SUCCESS))
        return err;

      should_unlock = true;
#if MDBX_ENABLE_PGOP_STAT
      env->lck->pgops.wops.weak += wops;
#endif /* MDBX_ENABLE_PGOP_STAT */
      env->basal_txn->wr.troika = meta_tap(env);
      eASSERT(env, !env->txn && !env->basal_txn->nested);
      goto retry;
    }
    eASSERT(env, head.txnid == recent_committed_txnid(env));
    env->basal_txn->txnid = head.txnid;
    txn_gc_detent(env->basal_txn);
    flags |= txn_shrink_allowed;
  }

  eASSERT(env, txn_owned || should_unlock);
  eASSERT(env, !txn_owned || (flags & txn_shrink_allowed) == 0);

  if (!head.is_steady && unlikely(env->stuck_meta >= 0) && troika.recent != (uint8_t)env->stuck_meta) {
    NOTICE("skip %s since wagering meta-page (%u) is mispatch the recent "
           "meta-page (%u)",
           "sync datafile", env->stuck_meta, troika.recent);
    rc = MDBX_RESULT_TRUE;
    goto bailout;
  }
  if (!head.is_steady || ((flags & MDBX_SAFE_NOSYNC) == 0 && unsynced_pages)) {
    DEBUG("meta-head %" PRIaPGNO ", %s, sync_pending %" PRIu64, data_page(head.ptr_c)->pgno,
          durable_caption(head.ptr_c), unsynced_pages);
    meta_t meta = *head.ptr_c;
    rc = dxb_sync_locked(env, flags, &meta, &env->basal_txn->wr.troika);
    if (unlikely(rc != MDBX_SUCCESS))
      goto bailout;
  }

  /* LY: sync meta-pages if MDBX_NOMETASYNC enabled
   *     and someone was not synced above. */
  if (atomic_load32(&env->lck->meta_sync_txnid, mo_Relaxed) != (uint32_t)head.txnid)
    rc = meta_sync(env, head);

bailout:
  if (should_unlock)
    lck_txn_unlock(env);
  return rc;
}

__cold int env_open(MDBX_env *env, mdbx_mode_t mode) {
  /* Использование O_DSYNC или FILE_FLAG_WRITE_THROUGH:
   *
   *   0) Если размер страниц БД меньше системной страницы ОЗУ, то ядру ОС
   *      придется чаще обновлять страницы в unified page cache.
   *
   *      Однако, O_DSYNC не предполагает отключение unified page cache,
   *      поэтому подобные затруднения будем считать проблемой ОС и/или
   *      ожидаемым пенальти из-за использования мелких страниц БД.
   *
   *   1) В режиме MDBX_SYNC_DURABLE - O_DSYNC для записи как данных,
   *      так и мета-страниц. Однако, на Linux отказ от O_DSYNC с последующим
   *      fdatasync() может быть выгоднее при использовании HDD, так как
   *      позволяет io-scheduler переупорядочить запись с учетом актуального
   *      расположения файла БД на носителе.
   *
   *   2) В режиме MDBX_NOMETASYNC - O_DSYNC можно использовать для данных,
   *      но в этом может не быть смысла, так как fdatasync() всё равно
   *      требуется для гарантии фиксации мета после предыдущей транзакции.
   *
   *      В итоге на нормальных системах (не Windows) есть два варианта:
   *       - при возможности O_DIRECT и/или io_ring для данных, скорее всего,
   *         есть смысл вызвать fdatasync() перед записью данных, а затем
   *         использовать O_DSYNC;
   *       - не использовать O_DSYNC и вызывать fdatasync() после записи данных.
   *
   *      На Windows же следует минимизировать использование FlushFileBuffers()
   *      из-за проблем с производительностью. Поэтому на Windows в режиме
   *      MDBX_NOMETASYNC:
   *       - мета обновляется через дескриптор без FILE_FLAG_WRITE_THROUGH;
   *       - перед началом записи данных вызывается FlushFileBuffers(), если
   *         meta_sync_txnid не совпадает с последней записанной мета;
   *       - данные записываются через дескриптор с FILE_FLAG_WRITE_THROUGH.
   *
   *   3) В режиме MDBX_SAFE_NOSYNC - O_DSYNC нет смысла использовать, пока не
   *      будет реализована возможность полностью асинхронной "догоняющей"
   *      записи в выделенном процессе-сервере с io-ring очередями внутри.
   *
   * -----
   *
   * Использование O_DIRECT или FILE_FLAG_NO_BUFFERING:
   *
   *   Назначение этих флагов в отключении файлового дескриптора от
   *   unified page cache, т.е. от отображенных в память данных в случае
   *   libmdbx.
   *
   *   Поэтому, использование direct i/o в libmdbx без MDBX_WRITEMAP лишено
   *   смысла и контр-продуктивно, ибо так мы провоцируем ядро ОС на
   *   не-когерентность отображения в память с содержимым файла на носителе,
   *   либо требуем дополнительных проверок и действий направленных на
   *   фактическое отключение O_DIRECT для отображенных в память данных.
   *
   *   В режиме MDBX_WRITEMAP когерентность отображенных данных обеспечивается
   *   физически. Поэтому использование direct i/o может иметь смысл, если у
   *   ядра ОС есть какие-то проблемы с msync(), в том числе с
   *   производительностью:
   *    - использование io_ring или gather-write может быть дешевле, чем
   *      просмотр PTE ядром и запись измененных/грязных;
   *    - но проблема в том, что записываемые из user mode страницы либо не
   *      будут помечены чистыми (и соответственно будут записаны ядром
   *      еще раз), либо ядру необходимо искать и чистить PTE при получении
   *      запроса на запись.
   *
   *   Поэтому O_DIRECT или FILE_FLAG_NO_BUFFERING используется:
   *    - только в режиме MDBX_SYNC_DURABLE с MDBX_WRITEMAP;
   *    - когда ps >= me_os_psize;
   *    - опция сборки MDBX_AVOID_MSYNC != 0, которая по-умолчанию включена
   *      только на Windows (см ниже).
   *
   * -----
   *
   * Использование FILE_FLAG_OVERLAPPED на Windows:
   *
   * У Windows очень плохо с I/O (за исключением прямых постраничных
   * scatter/gather, которые работают в обход проблемного unified page
   * cache и поэтому почти бесполезны в libmdbx).
   *
   * При этом всё еще хуже при использовании FlushFileBuffers(), что также
   * требуется после FlushViewOfFile() в режиме MDBX_WRITEMAP. Поэтому
   * на Windows вместо FlushViewOfFile() и FlushFileBuffers() следует
   * использовать запись через дескриптор с FILE_FLAG_WRITE_THROUGH.
   *
   * В свою очередь, запись с FILE_FLAG_WRITE_THROUGH дешевле/быстрее
   * при использовании FILE_FLAG_OVERLAPPED. В результате, на Windows
   * в durable-режимах запись данных всегда в overlapped-режиме,
   * при этом для записи мета требуется отдельный не-overlapped дескриптор.
   */

  env->pid = osal_getpid();
  int rc = osal_openfile((env->flags & MDBX_RDONLY) ? MDBX_OPEN_DXB_READ : MDBX_OPEN_DXB_LAZY, env, env->pathname.dxb,
                         &env->lazy_fd, mode);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

#if MDBX_LOCKING == MDBX_LOCKING_SYSV
  env->me_sysv_ipc.key = ftok(env->pathname.dxb, 42);
  if (unlikely(env->me_sysv_ipc.key == -1))
    return errno;
#endif /* MDBX_LOCKING */

  /* Set the position in files outside of the data to avoid corruption
   * due to erroneous use of file descriptors in the application code. */
  const uint64_t safe_parking_lot_offset = UINT64_C(0x7fffFFFF80000000);
  osal_fseek(env->lazy_fd, safe_parking_lot_offset);

  env->fd4meta = env->lazy_fd;
#if defined(_WIN32) || defined(_WIN64)
  eASSERT(env, env->ioring.overlapped_fd == 0);
  bool ior_direct = false;
  if (!(env->flags & (MDBX_RDONLY | MDBX_SAFE_NOSYNC | MDBX_NOMETASYNC | MDBX_EXCLUSIVE))) {
    if (MDBX_AVOID_MSYNC && (env->flags & MDBX_WRITEMAP)) {
      /* Запрошен режим MDBX_SYNC_DURABLE | MDBX_WRITEMAP при активной опции
       * MDBX_AVOID_MSYNC.
       *
       * 1) В этой комбинации наиболее выгодно использовать WriteFileGather(),
       * но для этого необходимо открыть файл с флагом FILE_FLAG_NO_BUFFERING и
       * после обеспечивать выравнивание адресов и размера данных на границу
       * системной страницы, что в свою очередь возможно если размер страницы БД
       * не меньше размера системной страницы ОЗУ. Поэтому для открытия файла в
       * нужном режиме требуется знать размер страницы БД.
       *
       * 2) Кроме этого, в Windows запись в заблокированный регион файла
       * возможно только через тот-же дескриптор. Поэтому изначальный захват
       * блокировок посредством lck_seize(), захват/освобождение блокировок
       * во время пишущих транзакций и запись данных должны выполнятся через
       * один дескриптор.
       *
       * Таким образом, требуется прочитать волатильный заголовок БД, чтобы
       * узнать размер страницы, чтобы открыть дескриптор файла в режиме нужном
       * для записи данных, чтобы использовать именно этот дескриптор для
       * изначального захвата блокировок. */
      meta_t header;
      uint64_t dxb_filesize;
      int err = dxb_read_header(env, &header, MDBX_SUCCESS, true);
      if ((err == MDBX_SUCCESS && header.pagesize >= globals.sys_pagesize) ||
          (err == MDBX_ENODATA && mode && env->ps >= globals.sys_pagesize &&
           osal_filesize(env->lazy_fd, &dxb_filesize) == MDBX_SUCCESS && dxb_filesize == 0))
        /* Может быть коллизия, если два процесса пытаются одновременно создать
         * БД с разным размером страницы, который у одного меньше системной
         * страницы, а у другого НЕ меньше. Эта допустимая, но очень странная
         * ситуация. Поэтому считаем её ошибочной и не пытаемся разрешить. */
        ior_direct = true;
    }

    rc = osal_openfile(ior_direct ? MDBX_OPEN_DXB_OVERLAPPED_DIRECT : MDBX_OPEN_DXB_OVERLAPPED, env, env->pathname.dxb,
                       &env->ioring.overlapped_fd, 0);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    env->dxb_lock_event = CreateEventW(nullptr, true, false, nullptr);
    if (unlikely(!env->dxb_lock_event))
      return (int)GetLastError();
    osal_fseek(env->ioring.overlapped_fd, safe_parking_lot_offset);
  }
#else
  if (mode == 0) {
    /* pickup mode for lck-file */
    struct stat st;
    if (unlikely(fstat(env->lazy_fd, &st)))
      return errno;
    mode = st.st_mode;
  }
  mode = (/* inherit read permissions for group and others */ mode & (S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH)) |
         /* always add read/write for owner */ S_IRUSR | S_IWUSR |
         ((mode & S_IRGRP) ? /* +write if readable by group */ S_IWGRP : 0) |
         ((mode & S_IROTH) ? /* +write if readable by others */ S_IWOTH : 0);
#endif /* !Windows */
  const int lck_rc = lck_setup(env, mode);
  if (unlikely(MDBX_IS_ERROR(lck_rc)))
    return lck_rc;
  if (env->lck_mmap.fd != INVALID_HANDLE_VALUE)
    osal_fseek(env->lck_mmap.fd, safe_parking_lot_offset);

  eASSERT(env, env->dsync_fd == INVALID_HANDLE_VALUE);
  if (!(env->flags & (MDBX_RDONLY | MDBX_SAFE_NOSYNC | DEPRECATED_MAPASYNC
#if defined(_WIN32) || defined(_WIN64)
                      | MDBX_EXCLUSIVE
#endif /* !Windows */
                      ))) {
    rc = osal_openfile(MDBX_OPEN_DXB_DSYNC, env, env->pathname.dxb, &env->dsync_fd, 0);
    if (unlikely(MDBX_IS_ERROR(rc)))
      return rc;
    if (env->dsync_fd != INVALID_HANDLE_VALUE) {
      if ((env->flags & MDBX_NOMETASYNC) == 0)
        env->fd4meta = env->dsync_fd;
      osal_fseek(env->dsync_fd, safe_parking_lot_offset);
    }
  }

  const MDBX_env_flags_t lazy_flags = MDBX_SAFE_NOSYNC | MDBX_UTTERLY_NOSYNC | MDBX_NOMETASYNC;
  const MDBX_env_flags_t mode_flags = lazy_flags | MDBX_LIFORECLAIM | MDBX_NORDAHEAD | MDBX_RDONLY | MDBX_WRITEMAP;

  lck_t *const lck = env->lck_mmap.lck;
  if (lck && lck_rc != MDBX_RESULT_TRUE && (env->flags & MDBX_RDONLY) == 0) {
    MDBX_env_flags_t snap_flags;
    while ((snap_flags = atomic_load32(&lck->envmode, mo_AcquireRelease)) == MDBX_RDONLY) {
      if (atomic_cas32(&lck->envmode, MDBX_RDONLY, (snap_flags = (env->flags & mode_flags)))) {
        /* The case:
         *  - let's assume that for some reason the DB file is smaller
         *    than it should be according to the geometry,
         *    but not smaller than the last page used;
         *  - the first process that opens the database (lck_rc == RESULT_TRUE)
         *    does this in readonly mode and therefore cannot bring
         *    the file size back to normal;
         *  - some next process (lck_rc != RESULT_TRUE) opens the DB in
         *    read-write mode and now is here.
         *
         * FIXME: Should we re-check and set the size of DB-file right here? */
        break;
      }
      atomic_yield();
    }

    if (env->flags & MDBX_ACCEDE) {
      /* Pickup current mode-flags (MDBX_LIFORECLAIM, MDBX_NORDAHEAD, etc). */
      const MDBX_env_flags_t diff =
          (snap_flags ^ env->flags) & ((snap_flags & lazy_flags) ? mode_flags : mode_flags & ~MDBX_WRITEMAP);
      env->flags ^= diff;
      NOTICE("accede mode-flags: 0x%X, 0x%X -> 0x%X", diff, env->flags ^ diff, env->flags);
    }

    /* Ранее упущенный не очевидный момент: При работе БД в режимах
     * не-синхронной/отложенной фиксации на диске, все процессы-писатели должны
     * иметь одинаковый режим MDBX_WRITEMAP.
     *
     * В противном случае, сброс на диск следует выполнять дважды: сначала
     * msync(), затем fdatasync(). При этом msync() не обязан отрабатывать
     * в процессах без MDBX_WRITEMAP, так как файл в память отображен только
     * для чтения. Поэтому, в общем случае, различия по MDBX_WRITEMAP не
     * позволяют выполнить фиксацию данных на диск, после их изменения в другом
     * процессе.
     *
     * В режиме MDBX_UTTERLY_NOSYNC позволять совместную работу с MDBX_WRITEMAP
     * также не следует, поскольку никакой процесс (в том числе последний) не
     * может гарантированно сбросить данные на диск, а следовательно не должен
     * помечать какую-либо транзакцию как steady.
     *
     * В результате, требуется либо запретить совместную работу процессам с
     * разным MDBX_WRITEMAP в режиме отложенной записи, либо отслеживать такое
     * смешивание и блокировать steady-пометки - что контрпродуктивно. */
    const MDBX_env_flags_t rigorous_flags = (snap_flags & lazy_flags)
                                                ? MDBX_SAFE_NOSYNC | MDBX_UTTERLY_NOSYNC | MDBX_WRITEMAP
                                                : MDBX_SAFE_NOSYNC | MDBX_UTTERLY_NOSYNC;
    const MDBX_env_flags_t rigorous_diff = (snap_flags ^ env->flags) & rigorous_flags;
    if (rigorous_diff) {
      ERROR("current mode/flags 0x%X incompatible with requested 0x%X, "
            "rigorous diff 0x%X",
            env->flags, snap_flags, rigorous_diff);
      return MDBX_INCOMPATIBLE;
    }
  }

  mincore_clean_cache(env);
  const int dxb_rc = dxb_setup(env, lck_rc, mode);
  if (MDBX_IS_ERROR(dxb_rc))
    return dxb_rc;

  rc = osal_check_fs_incore(env->lazy_fd);
  env->incore = false;
  if (rc == MDBX_RESULT_TRUE) {
    env->incore = true;
    NOTICE("%s", "in-core database");
    rc = MDBX_SUCCESS;
  } else if (unlikely(rc != MDBX_SUCCESS)) {
    ERROR("check_fs_incore(), err %d", rc);
    return rc;
  }

  if (unlikely(/* recovery mode */ env->stuck_meta >= 0) &&
      (lck_rc != /* exclusive */ MDBX_RESULT_TRUE || (env->flags & MDBX_EXCLUSIVE) == 0)) {
    ERROR("%s", "recovery requires exclusive mode");
    return MDBX_BUSY;
  }

  DEBUG("opened dbenv %p", (void *)env);
  env->flags |= ENV_ACTIVE;
  if (!lck || lck_rc == MDBX_RESULT_TRUE) {
    env->lck->envmode.weak = env->flags & mode_flags;
    env->lck->meta_sync_txnid.weak = (uint32_t)recent_committed_txnid(env);
    env->lck->readers_check_timestamp.weak = osal_monotime();
  }
  if (lck) {
    if (lck_rc == MDBX_RESULT_TRUE) {
      rc = lck_downgrade(env);
      DEBUG("lck-downgrade-%s: rc %i", (env->flags & MDBX_EXCLUSIVE) ? "partial" : "full", rc);
      if (rc != MDBX_SUCCESS)
        return rc;
    } else {
      rc = mvcc_cleanup_dead(env, false, nullptr);
      if (MDBX_IS_ERROR(rc))
        return rc;
    }
  }

  rc = (env->flags & MDBX_RDONLY) ? MDBX_SUCCESS
                                  : osal_ioring_create(&env->ioring
#if defined(_WIN32) || defined(_WIN64)
                                                       ,
                                                       ior_direct, env->ioring.overlapped_fd
#endif /* Windows */
                                    );
  return rc;
}

__cold int env_close(MDBX_env *env, bool resurrect_after_fork) {
  const unsigned flags = env->flags;
  env->flags &= ~ENV_INTERNAL_FLAGS;
  if (flags & ENV_TXKEY) {
    thread_key_delete(env->me_txkey);
    env->me_txkey = 0;
  }

  if (env->lck)
    munlock_all(env);

  rthc_lock();
  int rc = rthc_remove(env);
  rthc_unlock();

#if MDBX_ENABLE_DBI_LOCKFREE
  for (defer_free_item_t *next, *ptr = env->defer_free; ptr; ptr = next) {
    next = ptr->next;
    osal_free(ptr);
  }
  env->defer_free = nullptr;
#endif /* MDBX_ENABLE_DBI_LOCKFREE */

  if ((env->flags & MDBX_RDONLY) == 0)
    osal_ioring_destroy(&env->ioring);

  env->lck = nullptr;
  if (env->lck_mmap.lck)
    osal_munmap(&env->lck_mmap);

  if (env->dxb_mmap.base) {
    osal_munmap(&env->dxb_mmap);
#ifdef ENABLE_MEMCHECK
    VALGRIND_DISCARD(env->valgrind_handle);
    env->valgrind_handle = -1;
#endif /* ENABLE_MEMCHECK */
  }

#if defined(_WIN32) || defined(_WIN64)
  eASSERT(env, !env->ioring.overlapped_fd || env->ioring.overlapped_fd == INVALID_HANDLE_VALUE);
  if (env->dxb_lock_event != INVALID_HANDLE_VALUE) {
    CloseHandle(env->dxb_lock_event);
    env->dxb_lock_event = INVALID_HANDLE_VALUE;
  }
  eASSERT(env, !resurrect_after_fork);
  if (env->pathname_char) {
    osal_free(env->pathname_char);
    env->pathname_char = nullptr;
  }
#endif /* Windows */

  if (env->dsync_fd != INVALID_HANDLE_VALUE) {
    (void)osal_closefile(env->dsync_fd);
    env->dsync_fd = INVALID_HANDLE_VALUE;
  }

  if (env->lazy_fd != INVALID_HANDLE_VALUE) {
    (void)osal_closefile(env->lazy_fd);
    env->lazy_fd = INVALID_HANDLE_VALUE;
  }

  if (env->lck_mmap.fd != INVALID_HANDLE_VALUE) {
    (void)osal_closefile(env->lck_mmap.fd);
    env->lck_mmap.fd = INVALID_HANDLE_VALUE;
  }

  if (!resurrect_after_fork) {
    if (env->kvs) {
      for (size_t i = CORE_DBS; i < env->n_dbi; ++i)
        if (env->kvs[i].name.iov_len)
          osal_free(env->kvs[i].name.iov_base);
      osal_free(env->kvs);
      env->n_dbi = CORE_DBS;
      env->kvs = nullptr;
    }
    if (env->page_auxbuf) {
      osal_memalign_free(env->page_auxbuf);
      env->page_auxbuf = nullptr;
    }
    if (env->dbi_seqs) {
      osal_free(env->dbi_seqs);
      env->dbi_seqs = nullptr;
    }
    if (env->dbs_flags) {
      osal_free(env->dbs_flags);
      env->dbs_flags = nullptr;
    }
    if (env->pathname.buffer) {
      osal_free(env->pathname.buffer);
      env->pathname.buffer = nullptr;
    }
    if (env->basal_txn) {
      txn_basal_destroy(env->basal_txn);
      env->basal_txn = nullptr;
    }
  }
  env->stuck_meta = -1;
  return rc;
}
