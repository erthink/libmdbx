/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2024

#include "internals.h"

__cold int dxb_read_header(MDBX_env *env, meta_t *dest, const int lck_exclusive,
                           const mdbx_mode_t mode_bits) {
  memset(dest, 0, sizeof(meta_t));
  int rc = osal_filesize(env->lazy_fd, &env->dxb_mmap.filesize);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  unaligned_poke_u64(4, dest->sign, DATASIGN_WEAK);
  rc = MDBX_CORRUPTED;

  /* Read twice all meta pages so we can find the latest one. */
  unsigned loop_limit = NUM_METAS * 2;
  /* We don't know the page size on first time. So, just guess it. */
  unsigned guess_pagesize = 0;
  for (unsigned loop_count = 0; loop_count < loop_limit; ++loop_count) {
    const unsigned meta_number = loop_count % NUM_METAS;
    const unsigned offset =
        (guess_pagesize             ? guess_pagesize
         : (loop_count > NUM_METAS) ? env->ps
                                    : globals.sys_pagesize) *
        meta_number;

    char buffer[MDBX_MIN_PAGESIZE];
    unsigned retryleft = 42;
    while (1) {
      TRACE("reading meta[%d]: offset %u, bytes %u, retry-left %u", meta_number,
            offset, MDBX_MIN_PAGESIZE, retryleft);
      int err = osal_pread(env->lazy_fd, buffer, MDBX_MIN_PAGESIZE, offset);
      if (err == MDBX_ENODATA && offset == 0 && loop_count == 0 &&
          env->dxb_mmap.filesize == 0 &&
          mode_bits /* non-zero for DB creation */ != 0) {
        NOTICE("read meta: empty file (%d, %s)", err, mdbx_strerror(err));
        return err;
      }
#if defined(_WIN32) || defined(_WIN64)
      if (err == ERROR_LOCK_VIOLATION) {
        SleepEx(0, true);
        err = osal_pread(env->lazy_fd, buffer, MDBX_MIN_PAGESIZE, offset);
        if (err == ERROR_LOCK_VIOLATION && --retryleft) {
          WARNING("read meta[%u,%u]: %i, %s", offset, MDBX_MIN_PAGESIZE, err,
                  mdbx_strerror(err));
          continue;
        }
      }
#endif /* Windows */
      if (err != MDBX_SUCCESS) {
        ERROR("read meta[%u,%u]: %i, %s", offset, MDBX_MIN_PAGESIZE, err,
              mdbx_strerror(err));
        return err;
      }

      char again[MDBX_MIN_PAGESIZE];
      err = osal_pread(env->lazy_fd, again, MDBX_MIN_PAGESIZE, offset);
#if defined(_WIN32) || defined(_WIN64)
      if (err == ERROR_LOCK_VIOLATION) {
        SleepEx(0, true);
        err = osal_pread(env->lazy_fd, again, MDBX_MIN_PAGESIZE, offset);
        if (err == ERROR_LOCK_VIOLATION && --retryleft) {
          WARNING("read meta[%u,%u]: %i, %s", offset, MDBX_MIN_PAGESIZE, err,
                  mdbx_strerror(err));
          continue;
        }
      }
#endif /* Windows */
      if (err != MDBX_SUCCESS) {
        ERROR("read meta[%u,%u]: %i, %s", offset, MDBX_MIN_PAGESIZE, err,
              mdbx_strerror(err));
        return err;
      }

      if (memcmp(buffer, again, MDBX_MIN_PAGESIZE) == 0 || --retryleft == 0)
        break;

      VERBOSE("meta[%u] was updated, re-read it", meta_number);
    }

    if (!retryleft) {
      ERROR("meta[%u] is too volatile, skip it", meta_number);
      continue;
    }

    page_t *const page = (page_t *)buffer;
    meta_t *const meta = page_meta(page);
    rc = meta_validate(env, meta, page, meta_number, &guess_pagesize);
    if (rc != MDBX_SUCCESS)
      continue;

    bool latch;
    if (env->stuck_meta >= 0)
      latch = (meta_number == (unsigned)env->stuck_meta);
    else if (meta_bootid_match(meta))
      latch = meta_choice_recent(
          meta->unsafe_txnid, SIGN_IS_STEADY(meta->unsafe_sign),
          dest->unsafe_txnid, SIGN_IS_STEADY(dest->unsafe_sign));
    else
      latch = meta_choice_steady(
          meta->unsafe_txnid, SIGN_IS_STEADY(meta->unsafe_sign),
          dest->unsafe_txnid, SIGN_IS_STEADY(dest->unsafe_sign));
    if (latch) {
      *dest = *meta;
      if (!lck_exclusive && !meta_is_steady(dest))
        loop_limit += 1; /* LY: should re-read to hush race with update */
      VERBOSE("latch meta[%u]", meta_number);
    }
  }

  if (dest->pagesize == 0 ||
      (env->stuck_meta < 0 &&
       !(meta_is_steady(dest) ||
         meta_weak_acceptable(env, dest, lck_exclusive)))) {
    ERROR("%s", "no usable meta-pages, database is corrupted");
    if (rc == MDBX_SUCCESS) {
      /* TODO: try to restore the database by fully checking b-tree structure
       * for the each meta page, if the corresponding option was given */
      return MDBX_CORRUPTED;
    }
    return rc;
  }

  return MDBX_SUCCESS;
}

__cold int dxb_resize(MDBX_env *const env, const pgno_t used_pgno,
                      const pgno_t size_pgno, pgno_t limit_pgno,
                      const enum resize_mode mode) {
  /* Acquire guard to avoid collision between read and write txns
   * around geo_in_bytes and dxb_mmap */
#if defined(_WIN32) || defined(_WIN64)
  imports.srwl_AcquireExclusive(&env->remap_guard);
  int rc = MDBX_SUCCESS;
  mdbx_handle_array_t *suspended = nullptr;
  mdbx_handle_array_t array_onstack;
#else
  int rc = osal_fastmutex_acquire(&env->remap_guard);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;
#endif

  const size_t prev_size = env->dxb_mmap.current;
  const size_t prev_limit = env->dxb_mmap.limit;
  const pgno_t prev_limit_pgno = bytes2pgno(env, prev_limit);
  eASSERT(env, limit_pgno >= size_pgno);
  eASSERT(env, size_pgno >= used_pgno);
  if (mode < explicit_resize && size_pgno <= prev_limit_pgno) {
    /* The actual mapsize may be less since the geo.upper may be changed
     * by other process. Avoids remapping until it necessary. */
    limit_pgno = prev_limit_pgno;
  }
  const size_t limit_bytes = pgno_align2os_bytes(env, limit_pgno);
  const size_t size_bytes = pgno_align2os_bytes(env, size_pgno);
#if MDBX_ENABLE_MADVISE || defined(ENABLE_MEMCHECK)
  const void *const prev_map = env->dxb_mmap.base;
#endif /* MDBX_ENABLE_MADVISE || ENABLE_MEMCHECK */

  VERBOSE("resize/%d datafile/mapping: "
          "present %" PRIuPTR " -> %" PRIuPTR ", "
          "limit %" PRIuPTR " -> %" PRIuPTR,
          mode, prev_size, size_bytes, prev_limit, limit_bytes);

  eASSERT(env, limit_bytes >= size_bytes);
  eASSERT(env, bytes2pgno(env, size_bytes) >= size_pgno);
  eASSERT(env, bytes2pgno(env, limit_bytes) >= limit_pgno);

  unsigned mresize_flags =
      env->flags & (MDBX_RDONLY | MDBX_WRITEMAP | MDBX_UTTERLY_NOSYNC);
  if (mode >= impilict_shrink)
    mresize_flags |= txn_shrink_allowed;

  if (limit_bytes == env->dxb_mmap.limit &&
      size_bytes == env->dxb_mmap.current &&
      size_bytes == env->dxb_mmap.filesize)
    goto bailout;

  /* При использовании MDBX_NOSTICKYTHREADS с транзакциями могут работать любые
   * потоки и у нас нет информации о том, какие именно. Поэтому нет возможности
   * выполнить remap-действия требующие приостановки работающих с БД потоков. */
  if ((env->flags & MDBX_NOSTICKYTHREADS) == 0) {
#if defined(_WIN32) || defined(_WIN64)
    if ((size_bytes < env->dxb_mmap.current && mode > implicit_grow) ||
        limit_bytes != env->dxb_mmap.limit) {
      /* 1) Windows allows only extending a read-write section, but not a
       *    corresponding mapped view. Therefore in other cases we must suspend
       *    the local threads for safe remap.
       * 2) At least on Windows 10 1803 the entire mapped section is unavailable
       *    for short time during NtExtendSection() or VirtualAlloc() execution.
       * 3) Under Wine runtime environment on Linux a section extending is not
       *    supported.
       *
       * THEREFORE LOCAL THREADS SUSPENDING IS ALWAYS REQUIRED! */
      array_onstack.limit = ARRAY_LENGTH(array_onstack.handles);
      array_onstack.count = 0;
      suspended = &array_onstack;
      rc = osal_suspend_threads_before_remap(env, &suspended);
      if (rc != MDBX_SUCCESS) {
        ERROR("failed suspend-for-remap: errcode %d", rc);
        goto bailout;
      }
      mresize_flags |= (mode < explicit_resize)
                           ? MDBX_MRESIZE_MAY_UNMAP
                           : MDBX_MRESIZE_MAY_UNMAP | MDBX_MRESIZE_MAY_MOVE;
    }
#else  /* Windows */
    lck_t *const lck = env->lck_mmap.lck;
    if (mode == explicit_resize && limit_bytes != env->dxb_mmap.limit) {
      mresize_flags |= MDBX_MRESIZE_MAY_UNMAP | MDBX_MRESIZE_MAY_MOVE;
      if (lck) {
        int err = lck_rdt_lock(env) /* lock readers table until remap done */;
        if (unlikely(MDBX_IS_ERROR(err))) {
          rc = err;
          goto bailout;
        }

        /* looking for readers from this process */
        const size_t snap_nreaders =
            atomic_load32(&lck->rdt_length, mo_AcquireRelease);
        eASSERT(env, mode == explicit_resize);
        for (size_t i = 0; i < snap_nreaders; ++i) {
          if (lck->rdt[i].pid.weak == env->pid &&
              lck->rdt[i].tid.weak != osal_thread_self()) {
            /* the base address of the mapping can't be changed since
             * the other reader thread from this process exists. */
            lck_rdt_unlock(env);
            mresize_flags &= ~(MDBX_MRESIZE_MAY_UNMAP | MDBX_MRESIZE_MAY_MOVE);
            break;
          }
        }
      }
    }
#endif /* ! Windows */
  }

  const pgno_t aligned_munlock_pgno =
      (mresize_flags & (MDBX_MRESIZE_MAY_UNMAP | MDBX_MRESIZE_MAY_MOVE))
          ? 0
          : bytes2pgno(env, size_bytes);
  if (mresize_flags & (MDBX_MRESIZE_MAY_UNMAP | MDBX_MRESIZE_MAY_MOVE)) {
    mincore_clean_cache(env);
    if ((env->flags & MDBX_WRITEMAP) && env->lck->unsynced_pages.weak) {
#if MDBX_ENABLE_PGOP_STAT
      env->lck->pgops.msync.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
      rc = osal_msync(&env->dxb_mmap, 0, pgno_align2os_bytes(env, used_pgno),
                      MDBX_SYNC_NONE);
      if (unlikely(rc != MDBX_SUCCESS))
        goto bailout;
    }
  }
  munlock_after(env, aligned_munlock_pgno, size_bytes);

#if MDBX_ENABLE_MADVISE
  if (size_bytes < prev_size && mode > implicit_grow) {
    NOTICE("resize-MADV_%s %u..%u",
           (env->flags & MDBX_WRITEMAP) ? "REMOVE" : "DONTNEED", size_pgno,
           bytes2pgno(env, prev_size));
    const uint32_t munlocks_before =
        atomic_load32(&env->lck->mlcnt[1], mo_Relaxed);
    rc = MDBX_RESULT_TRUE;
#if defined(MADV_REMOVE)
    if (env->flags & MDBX_WRITEMAP)
      rc = madvise(ptr_disp(env->dxb_mmap.base, size_bytes),
                   prev_size - size_bytes, MADV_REMOVE)
               ? ignore_enosys(errno)
               : MDBX_SUCCESS;
#endif /* MADV_REMOVE */
#if defined(MADV_DONTNEED)
    if (rc == MDBX_RESULT_TRUE)
      rc = madvise(ptr_disp(env->dxb_mmap.base, size_bytes),
                   prev_size - size_bytes, MADV_DONTNEED)
               ? ignore_enosys(errno)
               : MDBX_SUCCESS;
#elif defined(POSIX_MADV_DONTNEED)
    if (rc == MDBX_RESULT_TRUE)
      rc = ignore_enosys(posix_madvise(ptr_disp(env->dxb_mmap.base, size_bytes),
                                       prev_size - size_bytes,
                                       POSIX_MADV_DONTNEED));
#elif defined(POSIX_FADV_DONTNEED)
    if (rc == MDBX_RESULT_TRUE)
      rc = ignore_enosys(posix_fadvise(env->lazy_fd, size_bytes,
                                       prev_size - size_bytes,
                                       POSIX_FADV_DONTNEED));
#endif /* MADV_DONTNEED */
    if (unlikely(MDBX_IS_ERROR(rc))) {
      const uint32_t mlocks_after =
          atomic_load32(&env->lck->mlcnt[0], mo_Relaxed);
      if (rc == MDBX_EINVAL) {
        const int severity =
            (mlocks_after - munlocks_before) ? MDBX_LOG_NOTICE : MDBX_LOG_WARN;
        if (LOG_ENABLED(severity))
          debug_log(severity, __func__, __LINE__,
                    "%s-madvise: ignore EINVAL (%d) since some pages maybe "
                    "locked (%u/%u mlcnt-processes)",
                    "resize", rc, mlocks_after, munlocks_before);
      } else {
        ERROR("%s-madvise(%s, %zu, +%zu), %u/%u mlcnt-processes, err %d",
              "mresize", "DONTNEED", size_bytes, prev_size - size_bytes,
              mlocks_after, munlocks_before, rc);
        goto bailout;
      }
    } else
      env->lck->discarded_tail.weak = size_pgno;
  }
#endif /* MDBX_ENABLE_MADVISE */

  rc = osal_mresize(mresize_flags, &env->dxb_mmap, size_bytes, limit_bytes);
  eASSERT(env, env->dxb_mmap.limit >= env->dxb_mmap.current);

#if MDBX_ENABLE_MADVISE
  if (rc == MDBX_SUCCESS) {
    eASSERT(env, limit_bytes == env->dxb_mmap.limit);
    eASSERT(env, size_bytes <= env->dxb_mmap.filesize);
    if (mode == explicit_resize)
      eASSERT(env, size_bytes == env->dxb_mmap.current);
    else
      eASSERT(env, size_bytes <= env->dxb_mmap.current);
    env->lck->discarded_tail.weak = size_pgno;
    const bool readahead =
        !(env->flags & MDBX_NORDAHEAD) &&
        mdbx_is_readahead_reasonable(size_bytes, -(intptr_t)prev_size);
    const bool force = limit_bytes != prev_limit ||
                       env->dxb_mmap.base != prev_map
#if defined(_WIN32) || defined(_WIN64)
                       || prev_size > size_bytes
#endif /* Windows */
        ;
    rc = dxb_set_readahead(env, size_pgno, readahead, force);
  }
#endif /* MDBX_ENABLE_MADVISE */

bailout:
  if (rc == MDBX_SUCCESS) {
    eASSERT(env, env->dxb_mmap.limit >= env->dxb_mmap.current);
    eASSERT(env, limit_bytes == env->dxb_mmap.limit);
    eASSERT(env, size_bytes <= env->dxb_mmap.filesize);
    if (mode == explicit_resize)
      eASSERT(env, size_bytes == env->dxb_mmap.current);
    else
      eASSERT(env, size_bytes <= env->dxb_mmap.current);
    /* update env-geo to avoid influences */
    env->geo_in_bytes.now = env->dxb_mmap.current;
    env->geo_in_bytes.upper = env->dxb_mmap.limit;
    env_options_adjust_defaults(env);
#ifdef ENABLE_MEMCHECK
    if (prev_limit != env->dxb_mmap.limit || prev_map != env->dxb_mmap.base) {
      VALGRIND_DISCARD(env->valgrind_handle);
      env->valgrind_handle = 0;
      if (env->dxb_mmap.limit)
        env->valgrind_handle = VALGRIND_CREATE_BLOCK(
            env->dxb_mmap.base, env->dxb_mmap.limit, "mdbx");
    }
#endif /* ENABLE_MEMCHECK */
  } else {
    if (rc != MDBX_UNABLE_EXTEND_MAPSIZE && rc != MDBX_EPERM) {
      ERROR("failed resize datafile/mapping: "
            "present %" PRIuPTR " -> %" PRIuPTR ", "
            "limit %" PRIuPTR " -> %" PRIuPTR ", errcode %d",
            prev_size, size_bytes, prev_limit, limit_bytes, rc);
    } else {
      WARNING("unable resize datafile/mapping: "
              "present %" PRIuPTR " -> %" PRIuPTR ", "
              "limit %" PRIuPTR " -> %" PRIuPTR ", errcode %d",
              prev_size, size_bytes, prev_limit, limit_bytes, rc);
      eASSERT(env, env->dxb_mmap.limit >= env->dxb_mmap.current);
    }
    if (!env->dxb_mmap.base) {
      env->flags |= ENV_FATAL_ERROR;
      if (env->txn)
        env->txn->flags |= MDBX_TXN_ERROR;
      rc = MDBX_PANIC;
    }
  }

#if defined(_WIN32) || defined(_WIN64)
  int err = MDBX_SUCCESS;
  imports.srwl_ReleaseExclusive(&env->remap_guard);
  if (suspended) {
    err = osal_resume_threads_after_remap(suspended);
    if (suspended != &array_onstack)
      osal_free(suspended);
  }
#else
  if (env->lck_mmap.lck &&
      (mresize_flags & (MDBX_MRESIZE_MAY_UNMAP | MDBX_MRESIZE_MAY_MOVE)) != 0)
    lck_rdt_unlock(env);
  int err = osal_fastmutex_release(&env->remap_guard);
#endif /* Windows */
  if (err != MDBX_SUCCESS) {
    FATAL("failed resume-after-remap: errcode %d", err);
    return MDBX_PANIC;
  }
  return rc;
}
#if defined(ENABLE_MEMCHECK) || defined(__SANITIZE_ADDRESS__)
void dxb_sanitize_tail(MDBX_env *env, MDBX_txn *txn) {
#if !defined(__SANITIZE_ADDRESS__)
  if (!RUNNING_ON_VALGRIND)
    return;
#endif
  if (txn) { /* transaction start */
    if (env->poison_edge < txn->geo.first_unallocated)
      env->poison_edge = txn->geo.first_unallocated;
    VALGRIND_MAKE_MEM_DEFINED(env->dxb_mmap.base,
                              pgno2bytes(env, txn->geo.first_unallocated));
    MDBX_ASAN_UNPOISON_MEMORY_REGION(
        env->dxb_mmap.base, pgno2bytes(env, txn->geo.first_unallocated));
    /* don't touch more, it should be already poisoned */
  } else { /* transaction end */
    bool should_unlock = false;
    pgno_t last = MAX_PAGENO + 1;
    if (env->pid != osal_getpid()) {
      /* resurrect after fork */
      return;
    } else if (env->txn && env_txn0_owned(env)) {
      /* inside write-txn */
      last = meta_recent(env, &env->basal_txn->tw.troika)
                 .ptr_v->geometry.first_unallocated;
    } else if (env->flags & MDBX_RDONLY) {
      /* read-only mode, no write-txn, no wlock mutex */
      last = NUM_METAS;
    } else if (lck_txn_lock(env, true) == MDBX_SUCCESS) {
      /* no write-txn */
      last = NUM_METAS;
      should_unlock = true;
    } else {
      /* write txn is running, therefore shouldn't poison any memory range */
      return;
    }

    last = mvcc_largest_this(env, last);
    const pgno_t edge = env->poison_edge;
    if (edge > last) {
      eASSERT(env, last >= NUM_METAS);
      env->poison_edge = last;
      VALGRIND_MAKE_MEM_NOACCESS(
          ptr_disp(env->dxb_mmap.base, pgno2bytes(env, last)),
          pgno2bytes(env, edge - last));
      MDBX_ASAN_POISON_MEMORY_REGION(
          ptr_disp(env->dxb_mmap.base, pgno2bytes(env, last)),
          pgno2bytes(env, edge - last));
    }
    if (should_unlock)
      lck_txn_unlock(env);
  }
}
#endif /* ENABLE_MEMCHECK || __SANITIZE_ADDRESS__ */

#if MDBX_ENABLE_MADVISE
/* Turn on/off readahead. It's harmful when the DB is larger than RAM. */
__cold int dxb_set_readahead(const MDBX_env *env, const pgno_t edge,
                             const bool enable, const bool force_whole) {
  eASSERT(env, edge >= NUM_METAS && edge <= MAX_PAGENO + 1);
  eASSERT(env, (enable & 1) == (enable != 0));
  const bool toggle = force_whole ||
                      ((enable ^ env->lck->readahead_anchor) & 1) ||
                      !env->lck->readahead_anchor;
  const pgno_t prev_edge = env->lck->readahead_anchor >> 1;
  const size_t limit = env->dxb_mmap.limit;
  size_t offset =
      toggle ? 0
             : pgno_align2os_bytes(env, (prev_edge < edge) ? prev_edge : edge);
  offset = (offset < limit) ? offset : limit;

  size_t length =
      pgno_align2os_bytes(env, (prev_edge < edge) ? edge : prev_edge);
  length = (length < limit) ? length : limit;
  length -= offset;

  eASSERT(env, 0 <= (intptr_t)length);
  if (length == 0)
    return MDBX_SUCCESS;

  NOTICE("readahead %s %u..%u", enable ? "ON" : "OFF", bytes2pgno(env, offset),
         bytes2pgno(env, offset + length));

#if defined(F_RDAHEAD)
  if (toggle && unlikely(fcntl(env->lazy_fd, F_RDAHEAD, enable) == -1))
    return errno;
#endif /* F_RDAHEAD */

  int err;
  void *const ptr = ptr_disp(env->dxb_mmap.base, offset);
  if (enable) {
#if defined(MADV_NORMAL)
    err =
        madvise(ptr, length, MADV_NORMAL) ? ignore_enosys(errno) : MDBX_SUCCESS;
    if (unlikely(MDBX_IS_ERROR(err)))
      return err;
#elif defined(POSIX_MADV_NORMAL)
    err = ignore_enosys(posix_madvise(ptr, length, POSIX_MADV_NORMAL));
    if (unlikely(MDBX_IS_ERROR(err)))
      return err;
#elif defined(POSIX_FADV_NORMAL) && defined(POSIX_FADV_WILLNEED)
    err = ignore_enosys(
        posix_fadvise(env->lazy_fd, offset, length, POSIX_FADV_NORMAL));
    if (unlikely(MDBX_IS_ERROR(err)))
      return err;
#elif defined(_WIN32) || defined(_WIN64)
    /* no madvise on Windows */
#else
#warning "FIXME"
#endif
    if (toggle) {
      /* NOTE: Seems there is a bug in the Mach/Darwin/OSX kernel,
       * because MADV_WILLNEED with offset != 0 may cause SIGBUS
       * on following access to the hinted region.
       * 19.6.0 Darwin Kernel Version 19.6.0: Tue Jan 12 22:13:05 PST 2021;
       * root:xnu-6153.141.16~1/RELEASE_X86_64 x86_64 */
#if defined(F_RDADVISE)
      struct radvisory hint;
      hint.ra_offset = offset;
      hint.ra_count =
          unlikely(length > INT_MAX && sizeof(length) > sizeof(hint.ra_count))
              ? INT_MAX
              : (int)length;
      (void)/* Ignore ENOTTY for DB on the ram-disk and so on */ fcntl(
          env->lazy_fd, F_RDADVISE, &hint);
#elif defined(MADV_WILLNEED)
      err = madvise(ptr, length, MADV_WILLNEED) ? ignore_enosys(errno)
                                                : MDBX_SUCCESS;
      if (unlikely(MDBX_IS_ERROR(err)))
        return err;
#elif defined(POSIX_MADV_WILLNEED)
      err = ignore_enosys(posix_madvise(ptr, length, POSIX_MADV_WILLNEED));
      if (unlikely(MDBX_IS_ERROR(err)))
        return err;
#elif defined(_WIN32) || defined(_WIN64)
      if (imports.PrefetchVirtualMemory) {
        WIN32_MEMORY_RANGE_ENTRY hint;
        hint.VirtualAddress = ptr;
        hint.NumberOfBytes = length;
        (void)imports.PrefetchVirtualMemory(GetCurrentProcess(), 1, &hint, 0);
      }
#elif defined(POSIX_FADV_WILLNEED)
      err = ignore_enosys(
          posix_fadvise(env->lazy_fd, offset, length, POSIX_FADV_WILLNEED));
      if (unlikely(MDBX_IS_ERROR(err)))
        return err;
#else
#warning "FIXME"
#endif
    }
  } else {
    mincore_clean_cache(env);
#if defined(MADV_RANDOM)
    err =
        madvise(ptr, length, MADV_RANDOM) ? ignore_enosys(errno) : MDBX_SUCCESS;
    if (unlikely(MDBX_IS_ERROR(err)))
      return err;
#elif defined(POSIX_MADV_RANDOM)
    err = ignore_enosys(posix_madvise(ptr, length, POSIX_MADV_RANDOM));
    if (unlikely(MDBX_IS_ERROR(err)))
      return err;
#elif defined(POSIX_FADV_RANDOM)
    err = ignore_enosys(
        posix_fadvise(env->lazy_fd, offset, length, POSIX_FADV_RANDOM));
    if (unlikely(MDBX_IS_ERROR(err)))
      return err;
#elif defined(_WIN32) || defined(_WIN64)
    /* no madvise on Windows */
#else
#warning "FIXME"
#endif /* MADV_RANDOM */
  }

  env->lck->readahead_anchor = (enable & 1) + (edge << 1);
  err = MDBX_SUCCESS;
  return err;
}
#endif /* MDBX_ENABLE_MADVISE */

__cold int dxb_setup(MDBX_env *env, const int lck_rc,
                     const mdbx_mode_t mode_bits) {
  meta_t header;
  eASSERT(env, !(env->flags & ENV_ACTIVE));
  int rc = MDBX_RESULT_FALSE;
  int err = dxb_read_header(env, &header, lck_rc, mode_bits);
  if (unlikely(err != MDBX_SUCCESS)) {
    if (lck_rc != /* lck exclusive */ MDBX_RESULT_TRUE || err != MDBX_ENODATA ||
        (env->flags & MDBX_RDONLY) != 0 ||
        /* recovery mode */ env->stuck_meta >= 0)
      return err;

    DEBUG("%s", "create new database");
    rc = /* new database */ MDBX_RESULT_TRUE;

    if (!env->geo_in_bytes.now) {
      /* set defaults if not configured */
      err = mdbx_env_set_geometry(env, 0, -1, -1, -1, -1, -1);
      if (unlikely(err != MDBX_SUCCESS))
        return err;
    }

    err = env_page_auxbuffer(env);
    if (unlikely(err != MDBX_SUCCESS))
      return err;

    header = *meta_init_triplet(env, env->page_auxbuf);
    err = osal_pwrite(env->lazy_fd, env->page_auxbuf,
                      env->ps * (size_t)NUM_METAS, 0);
    if (unlikely(err != MDBX_SUCCESS))
      return err;

    err = osal_ftruncate(env->lazy_fd, env->dxb_mmap.filesize =
                                           env->dxb_mmap.current =
                                               env->geo_in_bytes.now);
    if (unlikely(err != MDBX_SUCCESS))
      return err;

#ifndef NDEBUG /* just for checking */
    err = dxb_read_header(env, &header, lck_rc, mode_bits);
    if (unlikely(err != MDBX_SUCCESS))
      return err;
#endif
  }

  VERBOSE("header: root %" PRIaPGNO "/%" PRIaPGNO ", geo %" PRIaPGNO
          "/%" PRIaPGNO "-%" PRIaPGNO "/%" PRIaPGNO " +%u -%u, txn_id %" PRIaTXN
          ", %s",
          header.trees.main.root, header.trees.gc.root, header.geometry.lower,
          header.geometry.first_unallocated, header.geometry.now,
          header.geometry.upper, pv2pages(header.geometry.grow_pv),
          pv2pages(header.geometry.shrink_pv),
          unaligned_peek_u64(4, header.txnid_a), durable_caption(&header));

  if (unlikely((header.trees.gc.flags & DB_PERSISTENT_FLAGS) !=
               MDBX_INTEGERKEY)) {
    ERROR("unexpected/invalid db-flags 0x%x for %s", header.trees.gc.flags,
          "GC/FreeDB");
    return MDBX_INCOMPATIBLE;
  }
  env->dbs_flags[FREE_DBI] = DB_VALID | MDBX_INTEGERKEY;
  env->kvs[FREE_DBI].clc.k.cmp = cmp_int_align4; /* aligned MDBX_INTEGERKEY */
  env->kvs[FREE_DBI].clc.k.lmax = env->kvs[FREE_DBI].clc.k.lmin = 8;
  env->kvs[FREE_DBI].clc.v.cmp = cmp_lenfast;
  env->kvs[FREE_DBI].clc.v.lmin = 4;
  env->kvs[FREE_DBI].clc.v.lmax =
      mdbx_env_get_maxvalsize_ex(env, MDBX_INTEGERKEY);

  if (env->ps != header.pagesize)
    env_setup_pagesize(env, header.pagesize);
  if ((env->flags & MDBX_RDONLY) == 0) {
    err = env_page_auxbuffer(env);
    if (unlikely(err != MDBX_SUCCESS))
      return err;
  }

  const size_t used_bytes = pgno2bytes(env, header.geometry.first_unallocated);
  const size_t used_aligned2os_bytes =
      ceil_powerof2(used_bytes, globals.sys_pagesize);
  if ((env->flags & MDBX_RDONLY)    /* readonly */
      || lck_rc != MDBX_RESULT_TRUE /* not exclusive */
      || /* recovery mode */ env->stuck_meta >= 0) {
    /* use present params from db */
    const size_t pagesize = header.pagesize;
    err = mdbx_env_set_geometry(
        env, header.geometry.lower * pagesize, header.geometry.now * pagesize,
        header.geometry.upper * pagesize,
        pv2pages(header.geometry.grow_pv) * pagesize,
        pv2pages(header.geometry.shrink_pv) * pagesize, header.pagesize);
    if (unlikely(err != MDBX_SUCCESS)) {
      ERROR("%s: err %d", "could not apply geometry from db", err);
      return (err == MDBX_EINVAL) ? MDBX_INCOMPATIBLE : err;
    }
  } else if (env->geo_in_bytes.now) {
    /* silently growth to last used page */
    if (env->geo_in_bytes.now < used_aligned2os_bytes)
      env->geo_in_bytes.now = used_aligned2os_bytes;
    if (env->geo_in_bytes.upper < used_aligned2os_bytes)
      env->geo_in_bytes.upper = used_aligned2os_bytes;

    /* apply preconfigured params, but only if substantial changes:
     *  - upper or lower limit changes
     *  - shrink threshold or growth step
     * But ignore change just a 'now/current' size. */
    if (bytes_align2os_bytes(env, env->geo_in_bytes.upper) !=
            pgno2bytes(env, header.geometry.upper) ||
        bytes_align2os_bytes(env, env->geo_in_bytes.lower) !=
            pgno2bytes(env, header.geometry.lower) ||
        bytes_align2os_bytes(env, env->geo_in_bytes.shrink) !=
            pgno2bytes(env, pv2pages(header.geometry.shrink_pv)) ||
        bytes_align2os_bytes(env, env->geo_in_bytes.grow) !=
            pgno2bytes(env, pv2pages(header.geometry.grow_pv))) {

      if (env->geo_in_bytes.shrink && env->geo_in_bytes.now > used_bytes)
        /* pre-shrink if enabled */
        env->geo_in_bytes.now = used_bytes + env->geo_in_bytes.shrink -
                                used_bytes % env->geo_in_bytes.shrink;

      err = mdbx_env_set_geometry(
          env, env->geo_in_bytes.lower, env->geo_in_bytes.now,
          env->geo_in_bytes.upper, env->geo_in_bytes.grow,
          env->geo_in_bytes.shrink, header.pagesize);
      if (unlikely(err != MDBX_SUCCESS)) {
        ERROR("%s: err %d", "could not apply preconfigured db-geometry", err);
        return (err == MDBX_EINVAL) ? MDBX_INCOMPATIBLE : err;
      }

      /* update meta fields */
      header.geometry.now = bytes2pgno(env, env->geo_in_bytes.now);
      header.geometry.lower = bytes2pgno(env, env->geo_in_bytes.lower);
      header.geometry.upper = bytes2pgno(env, env->geo_in_bytes.upper);
      header.geometry.grow_pv =
          pages2pv(bytes2pgno(env, env->geo_in_bytes.grow));
      header.geometry.shrink_pv =
          pages2pv(bytes2pgno(env, env->geo_in_bytes.shrink));

      VERBOSE("amended: root %" PRIaPGNO "/%" PRIaPGNO ", geo %" PRIaPGNO
              "/%" PRIaPGNO "-%" PRIaPGNO "/%" PRIaPGNO
              " +%u -%u, txn_id %" PRIaTXN ", %s",
              header.trees.main.root, header.trees.gc.root,
              header.geometry.lower, header.geometry.first_unallocated,
              header.geometry.now, header.geometry.upper,
              pv2pages(header.geometry.grow_pv),
              pv2pages(header.geometry.shrink_pv),
              unaligned_peek_u64(4, header.txnid_a), durable_caption(&header));
    } else {
      /* fetch back 'now/current' size, since it was ignored during comparison
       * and may differ. */
      env->geo_in_bytes.now = pgno_align2os_bytes(env, header.geometry.now);
    }
    ENSURE(env, header.geometry.now >= header.geometry.first_unallocated);
  } else {
    /* geo-params are not pre-configured by user,
     * get current values from the meta. */
    env->geo_in_bytes.now = pgno2bytes(env, header.geometry.now);
    env->geo_in_bytes.lower = pgno2bytes(env, header.geometry.lower);
    env->geo_in_bytes.upper = pgno2bytes(env, header.geometry.upper);
    env->geo_in_bytes.grow = pgno2bytes(env, pv2pages(header.geometry.grow_pv));
    env->geo_in_bytes.shrink =
        pgno2bytes(env, pv2pages(header.geometry.shrink_pv));
  }

  ENSURE(env, pgno_align2os_bytes(env, header.geometry.now) ==
                  env->geo_in_bytes.now);
  ENSURE(env, env->geo_in_bytes.now >= used_bytes);
  const uint64_t filesize_before = env->dxb_mmap.filesize;
  if (unlikely(filesize_before != env->geo_in_bytes.now)) {
    if (lck_rc != /* lck exclusive */ MDBX_RESULT_TRUE) {
      VERBOSE("filesize mismatch (expect %" PRIuPTR "b/%" PRIaPGNO
              "p, have %" PRIu64 "b/%" PRIaPGNO "p), "
              "assume other process working",
              env->geo_in_bytes.now, bytes2pgno(env, env->geo_in_bytes.now),
              filesize_before, bytes2pgno(env, (size_t)filesize_before));
    } else {
      WARNING("filesize mismatch (expect %" PRIuSIZE "b/%" PRIaPGNO
              "p, have %" PRIu64 "b/%" PRIaPGNO "p)",
              env->geo_in_bytes.now, bytes2pgno(env, env->geo_in_bytes.now),
              filesize_before, bytes2pgno(env, (size_t)filesize_before));
      if (filesize_before < used_bytes) {
        ERROR("last-page beyond end-of-file (last %" PRIaPGNO
              ", have %" PRIaPGNO ")",
              header.geometry.first_unallocated,
              bytes2pgno(env, (size_t)filesize_before));
        return MDBX_CORRUPTED;
      }

      if (env->flags & MDBX_RDONLY) {
        if (filesize_before & (globals.sys_pagesize - 1)) {
          ERROR("%s", "filesize should be rounded-up to system page");
          return MDBX_WANNA_RECOVERY;
        }
        WARNING("%s", "ignore filesize mismatch in readonly-mode");
      } else {
        VERBOSE("will resize datafile to %" PRIuSIZE " bytes, %" PRIaPGNO
                " pages",
                env->geo_in_bytes.now, bytes2pgno(env, env->geo_in_bytes.now));
      }
    }
  }

  VERBOSE("current boot-id %" PRIx64 "-%" PRIx64 " (%savailable)",
          globals.bootid.x, globals.bootid.y,
          (globals.bootid.x | globals.bootid.y) ? "" : "not-");

#if MDBX_ENABLE_MADVISE
  /* calculate readahead hint before mmap with zero redundant pages */
  const bool readahead =
      !(env->flags & MDBX_NORDAHEAD) &&
      mdbx_is_readahead_reasonable(used_bytes, 0) == MDBX_RESULT_TRUE;
#endif /* MDBX_ENABLE_MADVISE */

  err = osal_mmap(env->flags, &env->dxb_mmap, env->geo_in_bytes.now,
                  env->geo_in_bytes.upper,
                  (lck_rc && env->stuck_meta < 0) ? MMAP_OPTION_TRUNCATE : 0);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

#if MDBX_ENABLE_MADVISE
#if defined(MADV_DONTDUMP)
  err = madvise(env->dxb_mmap.base, env->dxb_mmap.limit, MADV_DONTDUMP)
            ? ignore_enosys(errno)
            : MDBX_SUCCESS;
  if (unlikely(MDBX_IS_ERROR(err)))
    return err;
#endif /* MADV_DONTDUMP */
#if defined(MADV_DODUMP)
  if (globals.runtime_flags & MDBX_DBG_DUMP) {
    const size_t meta_length_aligned2os = pgno_align2os_bytes(env, NUM_METAS);
    err = madvise(env->dxb_mmap.base, meta_length_aligned2os, MADV_DODUMP)
              ? ignore_enosys(errno)
              : MDBX_SUCCESS;
    if (unlikely(MDBX_IS_ERROR(err)))
      return err;
  }
#endif /* MADV_DODUMP */
#endif /* MDBX_ENABLE_MADVISE */

#ifdef ENABLE_MEMCHECK
  env->valgrind_handle =
      VALGRIND_CREATE_BLOCK(env->dxb_mmap.base, env->dxb_mmap.limit, "mdbx");
#endif /* ENABLE_MEMCHECK */

  eASSERT(env, used_bytes >= pgno2bytes(env, NUM_METAS) &&
                   used_bytes <= env->dxb_mmap.limit);
#if defined(ENABLE_MEMCHECK) || defined(__SANITIZE_ADDRESS__)
  if (env->dxb_mmap.filesize > used_bytes &&
      env->dxb_mmap.filesize < env->dxb_mmap.limit) {
    VALGRIND_MAKE_MEM_NOACCESS(ptr_disp(env->dxb_mmap.base, used_bytes),
                               env->dxb_mmap.filesize - used_bytes);
    MDBX_ASAN_POISON_MEMORY_REGION(ptr_disp(env->dxb_mmap.base, used_bytes),
                                   env->dxb_mmap.filesize - used_bytes);
  }
  env->poison_edge =
      bytes2pgno(env, (env->dxb_mmap.filesize < env->dxb_mmap.limit)
                          ? env->dxb_mmap.filesize
                          : env->dxb_mmap.limit);
#endif /* ENABLE_MEMCHECK || __SANITIZE_ADDRESS__ */

  troika_t troika = meta_tap(env);
#if MDBX_DEBUG
  meta_troika_dump(env, &troika);
#endif
  //-------------------------------- validate/rollback head & steady meta-pages
  if (unlikely(env->stuck_meta >= 0)) {
    /* recovery mode */
    meta_t clone;
    meta_t const *const target = METAPAGE(env, env->stuck_meta);
    err = meta_validate_copy(env, target, &clone);
    if (unlikely(err != MDBX_SUCCESS)) {
      ERROR("target meta[%u] is corrupted",
            bytes2pgno(env, ptr_dist(data_page(target), env->dxb_mmap.base)));
      meta_troika_dump(env, &troika);
      return MDBX_CORRUPTED;
    }
  } else /* not recovery mode */
    while (1) {
      const unsigned meta_clash_mask = meta_eq_mask(&troika);
      if (unlikely(meta_clash_mask)) {
        ERROR("meta-pages are clashed: mask 0x%d", meta_clash_mask);
        meta_troika_dump(env, &troika);
        return MDBX_CORRUPTED;
      }

      if (lck_rc != /* lck exclusive */ MDBX_RESULT_TRUE) {
        /* non-exclusive mode,
         * meta-pages should be validated by a first process opened the DB */
        if (troika.recent == troika.prefer_steady)
          break;

        if (!env->lck_mmap.lck) {
          /* LY: without-lck (read-only) mode, so it is impossible that other
           * process made weak checkpoint. */
          ERROR("%s", "without-lck, unable recovery/rollback");
          meta_troika_dump(env, &troika);
          return MDBX_WANNA_RECOVERY;
        }

        /* LY: assume just have a collision with other running process,
         *     or someone make a weak checkpoint */
        VERBOSE("%s", "assume collision or online weak checkpoint");
        break;
      }
      eASSERT(env, lck_rc == MDBX_RESULT_TRUE);
      /* exclusive mode */

      const meta_ptr_t recent = meta_recent(env, &troika);
      const meta_ptr_t prefer_steady = meta_prefer_steady(env, &troika);
      meta_t clone;
      if (prefer_steady.is_steady) {
        err = meta_validate_copy(env, prefer_steady.ptr_c, &clone);
        if (unlikely(err != MDBX_SUCCESS)) {
          ERROR("meta[%u] with %s txnid %" PRIaTXN " is corrupted, %s needed",
                bytes2pgno(env,
                           ptr_dist(prefer_steady.ptr_c, env->dxb_mmap.base)),
                "steady", prefer_steady.txnid, "manual recovery");
          meta_troika_dump(env, &troika);
          return MDBX_CORRUPTED;
        }
        if (prefer_steady.ptr_c == recent.ptr_c)
          break;
      }

      const pgno_t pgno =
          bytes2pgno(env, ptr_dist(recent.ptr_c, env->dxb_mmap.base));
      const bool last_valid =
          meta_validate_copy(env, recent.ptr_c, &clone) == MDBX_SUCCESS;
      eASSERT(env,
              !prefer_steady.is_steady || recent.txnid != prefer_steady.txnid);
      if (unlikely(!last_valid)) {
        if (unlikely(!prefer_steady.is_steady)) {
          ERROR("%s for open or automatic rollback, %s",
                "there are no suitable meta-pages",
                "manual recovery is required");
          meta_troika_dump(env, &troika);
          return MDBX_CORRUPTED;
        }
        WARNING("meta[%u] with last txnid %" PRIaTXN
                " is corrupted, rollback needed",
                pgno, recent.txnid);
        meta_troika_dump(env, &troika);
        goto purge_meta_head;
      }

      if (meta_bootid_match(recent.ptr_c)) {
        if (env->flags & MDBX_RDONLY) {
          ERROR("%s, but boot-id(%016" PRIx64 "-%016" PRIx64 ") is MATCH: "
                "rollback NOT needed, steady-sync NEEDED%s",
                "opening after an unclean shutdown", globals.bootid.x,
                globals.bootid.y, ", but unable in read-only mode");
          meta_troika_dump(env, &troika);
          return MDBX_WANNA_RECOVERY;
        }
        WARNING("%s, but boot-id(%016" PRIx64 "-%016" PRIx64 ") is MATCH: "
                "rollback NOT needed, steady-sync NEEDED%s",
                "opening after an unclean shutdown", globals.bootid.x,
                globals.bootid.y, "");
        header = clone;
        env->lck->unsynced_pages.weak = header.geometry.first_unallocated;
        if (!env->lck->eoos_timestamp.weak)
          env->lck->eoos_timestamp.weak = osal_monotime();
        break;
      }
      if (unlikely(!prefer_steady.is_steady)) {
        ERROR("%s, but %s for automatic rollback: %s",
              "opening after an unclean shutdown",
              "there are no suitable meta-pages",
              "manual recovery is required");
        meta_troika_dump(env, &troika);
        return MDBX_CORRUPTED;
      }
      if (env->flags & MDBX_RDONLY) {
        ERROR("%s and rollback needed: (from head %" PRIaTXN
              " to steady %" PRIaTXN ")%s",
              "opening after an unclean shutdown", recent.txnid,
              prefer_steady.txnid, ", but unable in read-only mode");
        meta_troika_dump(env, &troika);
        return MDBX_WANNA_RECOVERY;
      }

    purge_meta_head:
      NOTICE("%s and doing automatic rollback: "
             "purge%s meta[%u] with%s txnid %" PRIaTXN,
             "opening after an unclean shutdown", last_valid ? "" : " invalid",
             pgno, last_valid ? " weak" : "", recent.txnid);
      meta_troika_dump(env, &troika);
      ENSURE(env, prefer_steady.is_steady);
      err = meta_override(env, pgno, 0,
                          last_valid ? recent.ptr_c : prefer_steady.ptr_c);
      if (err) {
        ERROR("rollback: overwrite meta[%u] with txnid %" PRIaTXN ", error %d",
              pgno, recent.txnid, err);
        return err;
      }
      troika = meta_tap(env);
      ENSURE(env, 0 == meta_txnid(recent.ptr_v));
      ENSURE(env, 0 == meta_eq_mask(&troika));
    }

  if (lck_rc == /* lck exclusive */ MDBX_RESULT_TRUE) {
    //-------------------------------------------------- shrink DB & update geo
    /* re-check size after mmap */
    if ((env->dxb_mmap.current & (globals.sys_pagesize - 1)) != 0 ||
        env->dxb_mmap.current < used_bytes) {
      ERROR("unacceptable/unexpected datafile size %" PRIuPTR,
            env->dxb_mmap.current);
      return MDBX_PROBLEM;
    }
    if (env->dxb_mmap.current != env->geo_in_bytes.now) {
      header.geometry.now = bytes2pgno(env, env->dxb_mmap.current);
      NOTICE("need update meta-geo to filesize %" PRIuPTR " bytes, %" PRIaPGNO
             " pages",
             env->dxb_mmap.current, header.geometry.now);
    }

    const meta_ptr_t recent = meta_recent(env, &troika);
    if (/* не учитываем различия в geo.first_unallocated */
        header.geometry.grow_pv != recent.ptr_c->geometry.grow_pv ||
        header.geometry.shrink_pv != recent.ptr_c->geometry.shrink_pv ||
        header.geometry.lower != recent.ptr_c->geometry.lower ||
        header.geometry.upper != recent.ptr_c->geometry.upper ||
        header.geometry.now != recent.ptr_c->geometry.now) {
      if ((env->flags & MDBX_RDONLY) != 0 ||
          /* recovery mode */ env->stuck_meta >= 0) {
        WARNING("skipped update meta.geo in %s mode: from l%" PRIaPGNO
                "-n%" PRIaPGNO "-u%" PRIaPGNO "/s%u-g%u, to l%" PRIaPGNO
                "-n%" PRIaPGNO "-u%" PRIaPGNO "/s%u-g%u",
                (env->stuck_meta < 0) ? "read-only" : "recovery",
                recent.ptr_c->geometry.lower, recent.ptr_c->geometry.now,
                recent.ptr_c->geometry.upper,
                pv2pages(recent.ptr_c->geometry.shrink_pv),
                pv2pages(recent.ptr_c->geometry.grow_pv), header.geometry.lower,
                header.geometry.now, header.geometry.upper,
                pv2pages(header.geometry.shrink_pv),
                pv2pages(header.geometry.grow_pv));
      } else {
        const txnid_t next_txnid = safe64_txnid_next(recent.txnid);
        if (unlikely(next_txnid > MAX_TXNID)) {
          ERROR("txnid overflow, raise %d", MDBX_TXN_FULL);
          return MDBX_TXN_FULL;
        }
        NOTICE("updating meta.geo: "
               "from l%" PRIaPGNO "-n%" PRIaPGNO "-u%" PRIaPGNO
               "/s%u-g%u (txn#%" PRIaTXN "), "
               "to l%" PRIaPGNO "-n%" PRIaPGNO "-u%" PRIaPGNO
               "/s%u-g%u (txn#%" PRIaTXN ")",
               recent.ptr_c->geometry.lower, recent.ptr_c->geometry.now,
               recent.ptr_c->geometry.upper,
               pv2pages(recent.ptr_c->geometry.shrink_pv),
               pv2pages(recent.ptr_c->geometry.grow_pv), recent.txnid,
               header.geometry.lower, header.geometry.now,
               header.geometry.upper, pv2pages(header.geometry.shrink_pv),
               pv2pages(header.geometry.grow_pv), next_txnid);

        ENSURE(env, header.unsafe_txnid == recent.txnid);
        meta_set_txnid(env, &header, next_txnid);
        err = dxb_sync_locked(env, env->flags | txn_shrink_allowed, &header,
                              &troika);
        if (err) {
          ERROR("error %d, while updating meta.geo: "
                "from l%" PRIaPGNO "-n%" PRIaPGNO "-u%" PRIaPGNO
                "/s%u-g%u (txn#%" PRIaTXN "), "
                "to l%" PRIaPGNO "-n%" PRIaPGNO "-u%" PRIaPGNO
                "/s%u-g%u (txn#%" PRIaTXN ")",
                err, recent.ptr_c->geometry.lower, recent.ptr_c->geometry.now,
                recent.ptr_c->geometry.upper,
                pv2pages(recent.ptr_c->geometry.shrink_pv),
                pv2pages(recent.ptr_c->geometry.grow_pv), recent.txnid,
                header.geometry.lower, header.geometry.now,
                header.geometry.upper, pv2pages(header.geometry.shrink_pv),
                pv2pages(header.geometry.grow_pv), header.unsafe_txnid);
          return err;
        }
      }
    }

    atomic_store32(&env->lck->discarded_tail,
                   bytes2pgno(env, used_aligned2os_bytes), mo_Relaxed);

    if ((env->flags & MDBX_RDONLY) == 0 && env->stuck_meta < 0 &&
        (globals.runtime_flags & MDBX_DBG_DONT_UPGRADE) == 0) {
      for (unsigned n = 0; n < NUM_METAS; ++n) {
        meta_t *const meta = METAPAGE(env, n);
        if (unlikely(unaligned_peek_u64(4, &meta->magic_and_version) !=
                     MDBX_DATA_MAGIC) ||
            (meta->dxbid.x | meta->dxbid.y) == 0 ||
            (meta->gc_flags & ~DB_PERSISTENT_FLAGS)) {
          const txnid_t txnid =
              meta_is_used(&troika, n) ? constmeta_txnid(meta) : 0;
          NOTICE("%s %s"
                 "meta[%u], txnid %" PRIaTXN,
                 "updating db-format/guid signature for",
                 meta_is_steady(meta) ? "stead-" : "weak-", n, txnid);
          err = meta_override(env, n, txnid, meta);
          if (unlikely(err != MDBX_SUCCESS) &&
              /* Just ignore the MDBX_PROBLEM error, since here it is
               * returned only in case of the attempt to upgrade an obsolete
               * meta-page that is invalid for current state of a DB,
               * e.g. after shrinking DB file */
              err != MDBX_PROBLEM) {
            ERROR("%s meta[%u], txnid %" PRIaTXN ", error %d",
                  "updating db-format signature for", n, txnid, err);
            return err;
          }
          troika = meta_tap(env);
        }
      }
    }
  } /* lck exclusive, lck_rc == MDBX_RESULT_TRUE */

  //---------------------------------------------------- setup madvise/readahead
#if MDBX_ENABLE_MADVISE
  if (used_aligned2os_bytes < env->dxb_mmap.current) {
#if defined(MADV_REMOVE)
    if (lck_rc && (env->flags & MDBX_WRITEMAP) != 0 &&
        /* not recovery mode */ env->stuck_meta < 0) {
      NOTICE("open-MADV_%s %u..%u", "REMOVE (deallocate file space)",
             env->lck->discarded_tail.weak,
             bytes2pgno(env, env->dxb_mmap.current));
      err = madvise(ptr_disp(env->dxb_mmap.base, used_aligned2os_bytes),
                    env->dxb_mmap.current - used_aligned2os_bytes, MADV_REMOVE)
                ? ignore_enosys(errno)
                : MDBX_SUCCESS;
      if (unlikely(MDBX_IS_ERROR(err)))
        return err;
    }
#endif /* MADV_REMOVE */
#if defined(MADV_DONTNEED)
    NOTICE("open-MADV_%s %u..%u", "DONTNEED", env->lck->discarded_tail.weak,
           bytes2pgno(env, env->dxb_mmap.current));
    err = madvise(ptr_disp(env->dxb_mmap.base, used_aligned2os_bytes),
                  env->dxb_mmap.current - used_aligned2os_bytes, MADV_DONTNEED)
              ? ignore_enosys(errno)
              : MDBX_SUCCESS;
    if (unlikely(MDBX_IS_ERROR(err)))
      return err;
#elif defined(POSIX_MADV_DONTNEED)
    err = ignore_enosys(posix_madvise(
        ptr_disp(env->dxb_mmap.base, used_aligned2os_bytes),
        env->dxb_mmap.current - used_aligned2os_bytes, POSIX_MADV_DONTNEED));
    if (unlikely(MDBX_IS_ERROR(err)))
      return err;
#elif defined(POSIX_FADV_DONTNEED)
    err = ignore_enosys(posix_fadvise(
        env->lazy_fd, used_aligned2os_bytes,
        env->dxb_mmap.current - used_aligned2os_bytes, POSIX_FADV_DONTNEED));
    if (unlikely(MDBX_IS_ERROR(err)))
      return err;
#endif /* MADV_DONTNEED */
  }

  err = dxb_set_readahead(env, bytes2pgno(env, used_bytes), readahead, true);
  if (unlikely(err != MDBX_SUCCESS))
    return err;
#endif /* MDBX_ENABLE_MADVISE */

  return rc;
}

int dxb_sync_locked(MDBX_env *env, unsigned flags, meta_t *const pending,
                    troika_t *const troika) {
  eASSERT(env, ((env->flags ^ flags) & MDBX_WRITEMAP) == 0);
  eASSERT(env, pending->trees.gc.flags == MDBX_INTEGERKEY);
  eASSERT(env, check_table_flags(pending->trees.main.flags));
  const meta_t *const meta0 = METAPAGE(env, 0);
  const meta_t *const meta1 = METAPAGE(env, 1);
  const meta_t *const meta2 = METAPAGE(env, 2);
  const meta_ptr_t head = meta_recent(env, troika);
  int rc;

  eASSERT(env,
          pending < METAPAGE(env, 0) || pending > METAPAGE(env, NUM_METAS));
  eASSERT(env, (env->flags & (MDBX_RDONLY | ENV_FATAL_ERROR)) == 0);
  eASSERT(env, pending->geometry.first_unallocated <= pending->geometry.now);

  if (flags & MDBX_SAFE_NOSYNC) {
    /* Check auto-sync conditions */
    const pgno_t autosync_threshold =
        atomic_load32(&env->lck->autosync_threshold, mo_Relaxed);
    const uint64_t autosync_period =
        atomic_load64(&env->lck->autosync_period, mo_Relaxed);
    uint64_t eoos_timestamp;
    if ((autosync_threshold &&
         atomic_load64(&env->lck->unsynced_pages, mo_Relaxed) >=
             autosync_threshold) ||
        (autosync_period &&
         (eoos_timestamp =
              atomic_load64(&env->lck->eoos_timestamp, mo_Relaxed)) &&
         osal_monotime() - eoos_timestamp >= autosync_period))
      flags &= MDBX_WRITEMAP | txn_shrink_allowed; /* force steady */
  }

  pgno_t shrink = 0;
  if (flags & txn_shrink_allowed) {
    const size_t prev_discarded_pgno =
        atomic_load32(&env->lck->discarded_tail, mo_Relaxed);
    if (prev_discarded_pgno < pending->geometry.first_unallocated)
      env->lck->discarded_tail.weak = pending->geometry.first_unallocated;
    else if (prev_discarded_pgno >=
             pending->geometry.first_unallocated + env->madv_threshold) {
      /* LY: check conditions to discard unused pages */
      const pgno_t largest_pgno = mvcc_snapshot_largest(
          env, (head.ptr_c->geometry.first_unallocated >
                pending->geometry.first_unallocated)
                   ? head.ptr_c->geometry.first_unallocated
                   : pending->geometry.first_unallocated);
      eASSERT(env, largest_pgno >= NUM_METAS);

#if defined(ENABLE_MEMCHECK) || defined(__SANITIZE_ADDRESS__)
      const pgno_t edge = env->poison_edge;
      if (edge > largest_pgno) {
        env->poison_edge = largest_pgno;
        VALGRIND_MAKE_MEM_NOACCESS(
            ptr_disp(env->dxb_mmap.base, pgno2bytes(env, largest_pgno)),
            pgno2bytes(env, edge - largest_pgno));
        MDBX_ASAN_POISON_MEMORY_REGION(
            ptr_disp(env->dxb_mmap.base, pgno2bytes(env, largest_pgno)),
            pgno2bytes(env, edge - largest_pgno));
      }
#endif /* ENABLE_MEMCHECK || __SANITIZE_ADDRESS__ */

#if MDBX_ENABLE_MADVISE &&                                                     \
    (defined(MADV_DONTNEED) || defined(POSIX_MADV_DONTNEED))
      const size_t discard_edge_pgno = pgno_align2os_pgno(env, largest_pgno);
      if (prev_discarded_pgno >= discard_edge_pgno + env->madv_threshold) {
        const size_t prev_discarded_bytes =
            pgno_align2os_bytes(env, prev_discarded_pgno);
        const size_t discard_edge_bytes = pgno2bytes(env, discard_edge_pgno);
        /* из-за выравнивания prev_discarded_bytes и discard_edge_bytes
         * могут быть равны */
        if (prev_discarded_bytes > discard_edge_bytes) {
          NOTICE("shrink-MADV_%s %zu..%zu", "DONTNEED", discard_edge_pgno,
                 prev_discarded_pgno);
          munlock_after(env, discard_edge_pgno,
                        bytes_align2os_bytes(env, env->dxb_mmap.current));
          const uint32_t munlocks_before =
              atomic_load32(&env->lck->mlcnt[1], mo_Relaxed);
#if defined(MADV_DONTNEED)
          int advise = MADV_DONTNEED;
#if defined(MADV_FREE) &&                                                      \
    0 /* MADV_FREE works for only anonymous vma at the moment */
          if ((env->flags & MDBX_WRITEMAP) &&
              global.linux_kernel_version > 0x04050000)
            advise = MADV_FREE;
#endif /* MADV_FREE */
          int err = madvise(ptr_disp(env->dxb_mmap.base, discard_edge_bytes),
                            prev_discarded_bytes - discard_edge_bytes, advise)
                        ? ignore_enosys(errno)
                        : MDBX_SUCCESS;
#else
          int err = ignore_enosys(posix_madvise(
              ptr_disp(env->dxb_mmap.base, discard_edge_bytes),
              prev_discarded_bytes - discard_edge_bytes, POSIX_MADV_DONTNEED));
#endif
          if (unlikely(MDBX_IS_ERROR(err))) {
            const uint32_t mlocks_after =
                atomic_load32(&env->lck->mlcnt[0], mo_Relaxed);
            if (err == MDBX_EINVAL) {
              const int severity = (mlocks_after - munlocks_before)
                                       ? MDBX_LOG_NOTICE
                                       : MDBX_LOG_WARN;
              if (LOG_ENABLED(severity))
                debug_log(
                    severity, __func__, __LINE__,
                    "%s-madvise: ignore EINVAL (%d) since some pages maybe "
                    "locked (%u/%u mlcnt-processes)",
                    "shrink", err, mlocks_after, munlocks_before);
            } else {
              ERROR("%s-madvise(%s, %zu, +%zu), %u/%u mlcnt-processes, err %d",
                    "shrink", "DONTNEED", discard_edge_bytes,
                    prev_discarded_bytes - discard_edge_bytes, mlocks_after,
                    munlocks_before, err);
              return err;
            }
          } else
            env->lck->discarded_tail.weak = discard_edge_pgno;
        }
      }
#endif /* MDBX_ENABLE_MADVISE && (MADV_DONTNEED || POSIX_MADV_DONTNEED) */

      /* LY: check conditions to shrink datafile */
      const pgno_t backlog_gap = 3 + pending->trees.gc.height * 3;
      pgno_t shrink_step = 0;
      if (pending->geometry.shrink_pv &&
          pending->geometry.now - pending->geometry.first_unallocated >
              (shrink_step = pv2pages(pending->geometry.shrink_pv)) +
                  backlog_gap) {
        if (pending->geometry.now > largest_pgno &&
            pending->geometry.now - largest_pgno > shrink_step + backlog_gap) {
          const pgno_t aligner =
              pending->geometry.grow_pv
                  ? /* grow_step */ pv2pages(pending->geometry.grow_pv)
                  : shrink_step;
          const pgno_t with_backlog_gap = largest_pgno + backlog_gap;
          const pgno_t aligned =
              pgno_align2os_pgno(env, (size_t)with_backlog_gap + aligner -
                                          with_backlog_gap % aligner);
          const pgno_t bottom = (aligned > pending->geometry.lower)
                                    ? aligned
                                    : pending->geometry.lower;
          if (pending->geometry.now > bottom) {
            if (TROIKA_HAVE_STEADY(troika))
              /* force steady, but only if steady-checkpoint is present */
              flags &= MDBX_WRITEMAP | txn_shrink_allowed;
            shrink = pending->geometry.now - bottom;
            pending->geometry.now = bottom;
            if (unlikely(head.txnid == pending->unsafe_txnid)) {
              const txnid_t txnid = safe64_txnid_next(pending->unsafe_txnid);
              NOTICE("force-forward pending-txn %" PRIaTXN " -> %" PRIaTXN,
                     pending->unsafe_txnid, txnid);
              ENSURE(env, !env->basal_txn || !env->txn);
              if (unlikely(txnid > MAX_TXNID)) {
                rc = MDBX_TXN_FULL;
                ERROR("txnid overflow, raise %d", rc);
                goto fail;
              }
              meta_set_txnid(env, pending, txnid);
              eASSERT(env, coherency_check_meta(env, pending, true));
            }
          }
        }
      }
    }
  }

  /* LY: step#1 - sync previously written/updated data-pages */
  rc = MDBX_RESULT_FALSE /* carry steady */;
  if (atomic_load64(&env->lck->unsynced_pages, mo_Relaxed)) {
    eASSERT(env, ((flags ^ env->flags) & MDBX_WRITEMAP) == 0);
    enum osal_syncmode_bits mode_bits = MDBX_SYNC_NONE;
    unsigned sync_op = 0;
    if ((flags & MDBX_SAFE_NOSYNC) == 0) {
      sync_op = 1;
      mode_bits = MDBX_SYNC_DATA;
      if (pending->geometry.first_unallocated >
          meta_prefer_steady(env, troika).ptr_c->geometry.now)
        mode_bits |= MDBX_SYNC_SIZE;
      if (flags & MDBX_NOMETASYNC)
        mode_bits |= MDBX_SYNC_IODQ;
    } else if (unlikely(env->incore))
      goto skip_incore_sync;
    if (flags & MDBX_WRITEMAP) {
#if MDBX_ENABLE_PGOP_STAT
      env->lck->pgops.msync.weak += sync_op;
#else
      (void)sync_op;
#endif /* MDBX_ENABLE_PGOP_STAT */
      rc = osal_msync(
          &env->dxb_mmap, 0,
          pgno_align2os_bytes(env, pending->geometry.first_unallocated),
          mode_bits);
    } else {
#if MDBX_ENABLE_PGOP_STAT
      env->lck->pgops.fsync.weak += sync_op;
#else
      (void)sync_op;
#endif /* MDBX_ENABLE_PGOP_STAT */
      rc = osal_fsync(env->lazy_fd, mode_bits);
    }
    if (unlikely(rc != MDBX_SUCCESS))
      goto fail;
    rc = (flags & MDBX_SAFE_NOSYNC) ? MDBX_RESULT_TRUE /* carry non-steady */
                                    : MDBX_RESULT_FALSE /* carry steady */;
  }
  eASSERT(env, coherency_check_meta(env, pending, true));

  /* Steady or Weak */
  if (rc == MDBX_RESULT_FALSE /* carry steady */) {
    meta_sign_as_steady(pending);
    atomic_store64(&env->lck->eoos_timestamp, 0, mo_Relaxed);
    atomic_store64(&env->lck->unsynced_pages, 0, mo_Relaxed);
  } else {
    assert(rc == MDBX_RESULT_TRUE /* carry non-steady */);
  skip_incore_sync:
    eASSERT(env, env->lck->unsynced_pages.weak > 0);
    /* Может быть нулевым если unsynced_pages > 0 в результате спиллинга.
     * eASSERT(env, env->lck->eoos_timestamp.weak != 0); */
    unaligned_poke_u64(4, pending->sign, DATASIGN_WEAK);
  }

  const bool legal4overwrite =
      head.txnid == pending->unsafe_txnid &&
      !memcmp(&head.ptr_c->trees, &pending->trees, sizeof(pending->trees)) &&
      !memcmp(&head.ptr_c->canary, &pending->canary, sizeof(pending->canary)) &&
      !memcmp(&head.ptr_c->geometry, &pending->geometry,
              sizeof(pending->geometry));
  meta_t *target = nullptr;
  if (head.txnid == pending->unsafe_txnid) {
    ENSURE(env, legal4overwrite);
    if (!head.is_steady && meta_is_steady(pending))
      target = (meta_t *)head.ptr_c;
    else {
      WARNING("%s", "skip update meta");
      return MDBX_SUCCESS;
    }
  } else {
    const unsigned troika_tail = troika->tail_and_flags & 3;
    ENSURE(env, troika_tail < NUM_METAS && troika_tail != troika->recent &&
                    troika_tail != troika->prefer_steady);
    target = (meta_t *)meta_tail(env, troika).ptr_c;
  }

  /* LY: step#2 - update meta-page. */
  DEBUG("writing meta%" PRIaPGNO " = root %" PRIaPGNO "/%" PRIaPGNO
        ", geo %" PRIaPGNO "/%" PRIaPGNO "-%" PRIaPGNO "/%" PRIaPGNO
        " +%u -%u, txn_id %" PRIaTXN ", %s",
        data_page(target)->pgno, pending->trees.main.root,
        pending->trees.gc.root, pending->geometry.lower,
        pending->geometry.first_unallocated, pending->geometry.now,
        pending->geometry.upper, pv2pages(pending->geometry.grow_pv),
        pv2pages(pending->geometry.shrink_pv), pending->unsafe_txnid,
        durable_caption(pending));

  DEBUG("meta0: %s, %s, txn_id %" PRIaTXN ", root %" PRIaPGNO "/%" PRIaPGNO,
        (meta0 == head.ptr_c) ? "head"
        : (meta0 == target)   ? "tail"
                              : "stay",
        durable_caption(meta0), constmeta_txnid(meta0), meta0->trees.main.root,
        meta0->trees.gc.root);
  DEBUG("meta1: %s, %s, txn_id %" PRIaTXN ", root %" PRIaPGNO "/%" PRIaPGNO,
        (meta1 == head.ptr_c) ? "head"
        : (meta1 == target)   ? "tail"
                              : "stay",
        durable_caption(meta1), constmeta_txnid(meta1), meta1->trees.main.root,
        meta1->trees.gc.root);
  DEBUG("meta2: %s, %s, txn_id %" PRIaTXN ", root %" PRIaPGNO "/%" PRIaPGNO,
        (meta2 == head.ptr_c) ? "head"
        : (meta2 == target)   ? "tail"
                              : "stay",
        durable_caption(meta2), constmeta_txnid(meta2), meta2->trees.main.root,
        meta2->trees.gc.root);

  eASSERT(env, pending->unsafe_txnid != constmeta_txnid(meta0) ||
                   (meta_is_steady(pending) && !meta_is_steady(meta0)));
  eASSERT(env, pending->unsafe_txnid != constmeta_txnid(meta1) ||
                   (meta_is_steady(pending) && !meta_is_steady(meta1)));
  eASSERT(env, pending->unsafe_txnid != constmeta_txnid(meta2) ||
                   (meta_is_steady(pending) && !meta_is_steady(meta2)));

  eASSERT(env, ((env->flags ^ flags) & MDBX_WRITEMAP) == 0);
  ENSURE(env, target == head.ptr_c ||
                  constmeta_txnid(target) < pending->unsafe_txnid);
  if (flags & MDBX_WRITEMAP) {
    jitter4testing(true);
    if (likely(target != head.ptr_c)) {
      /* LY: 'invalidate' the meta. */
      meta_update_begin(env, target, pending->unsafe_txnid);
      unaligned_poke_u64(4, target->sign, DATASIGN_WEAK);
#ifndef NDEBUG
      /* debug: provoke failure to catch a violators, but don't touch pagesize
       * to allow readers catch actual pagesize. */
      void *provoke_begin = &target->trees.gc.root;
      void *provoke_end = &target->sign;
      memset(provoke_begin, 0xCC, ptr_dist(provoke_end, provoke_begin));
      jitter4testing(false);
#endif

      /* LY: update info */
      target->geometry = pending->geometry;
      target->trees.gc = pending->trees.gc;
      target->trees.main = pending->trees.main;
      eASSERT(env, target->trees.gc.flags == MDBX_INTEGERKEY);
      eASSERT(env, check_table_flags(target->trees.main.flags));
      target->canary = pending->canary;
      memcpy(target->pages_retired, pending->pages_retired, 8);
      jitter4testing(true);

      /* LY: 'commit' the meta */
      meta_update_end(env, target, unaligned_peek_u64(4, pending->txnid_b));
      jitter4testing(true);
      eASSERT(env, coherency_check_meta(env, target, true));
    } else {
      /* dangerous case (target == head), only sign could
       * me updated, check assertions once again */
      eASSERT(env,
              legal4overwrite && !head.is_steady && meta_is_steady(pending));
    }
    memcpy(target->sign, pending->sign, 8);
    osal_flush_incoherent_cpu_writeback();
    jitter4testing(true);
    if (!env->incore) {
      if (!MDBX_AVOID_MSYNC) {
        /* sync meta-pages */
#if MDBX_ENABLE_PGOP_STAT
        env->lck->pgops.msync.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
        rc = osal_msync(&env->dxb_mmap, 0, pgno_align2os_bytes(env, NUM_METAS),
                        (flags & MDBX_NOMETASYNC)
                            ? MDBX_SYNC_NONE
                            : MDBX_SYNC_DATA | MDBX_SYNC_IODQ);
      } else {
#if MDBX_ENABLE_PGOP_STAT
        env->lck->pgops.wops.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
        const page_t *page = data_page(target);
        rc = osal_pwrite(env->fd4meta, page, env->ps,
                         ptr_dist(page, env->dxb_mmap.base));
        if (likely(rc == MDBX_SUCCESS)) {
          osal_flush_incoherent_mmap(target, sizeof(meta_t),
                                     globals.sys_pagesize);
          if ((flags & MDBX_NOMETASYNC) == 0 && env->fd4meta == env->lazy_fd) {
#if MDBX_ENABLE_PGOP_STAT
            env->lck->pgops.fsync.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
            rc = osal_fsync(env->lazy_fd, MDBX_SYNC_DATA | MDBX_SYNC_IODQ);
          }
        }
      }
      if (unlikely(rc != MDBX_SUCCESS))
        goto fail;
    }
  } else {
#if MDBX_ENABLE_PGOP_STAT
    env->lck->pgops.wops.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
    const meta_t undo_meta = *target;
    eASSERT(env, pending->trees.gc.flags == MDBX_INTEGERKEY);
    eASSERT(env, check_table_flags(pending->trees.main.flags));
    rc = osal_pwrite(env->fd4meta, pending, sizeof(meta_t),
                     ptr_dist(target, env->dxb_mmap.base));
    if (unlikely(rc != MDBX_SUCCESS)) {
    undo:
      DEBUG("%s", "write failed, disk error?");
      /* On a failure, the pagecache still contains the new data.
       * Try write some old data back, to prevent it from being used. */
      osal_pwrite(env->fd4meta, &undo_meta, sizeof(meta_t),
                  ptr_dist(target, env->dxb_mmap.base));
      goto fail;
    }
    osal_flush_incoherent_mmap(target, sizeof(meta_t), globals.sys_pagesize);
    /* sync meta-pages */
    if ((flags & MDBX_NOMETASYNC) == 0 && env->fd4meta == env->lazy_fd &&
        !env->incore) {
#if MDBX_ENABLE_PGOP_STAT
      env->lck->pgops.fsync.weak += 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
      rc = osal_fsync(env->lazy_fd, MDBX_SYNC_DATA | MDBX_SYNC_IODQ);
      if (rc != MDBX_SUCCESS)
        goto undo;
    }
  }

  uint64_t timestamp = 0;
  while ("workaround for https://libmdbx.dqdkfa.ru/dead-github/issues/269") {
    rc = coherency_check_written(
        env, pending->unsafe_txnid, target,
        bytes2pgno(env, ptr_dist(target, env->dxb_mmap.base)), &timestamp);
    if (likely(rc == MDBX_SUCCESS))
      break;
    if (unlikely(rc != MDBX_RESULT_TRUE))
      goto fail;
  }

  const uint32_t sync_txnid_dist =
      ((flags & MDBX_NOMETASYNC) == 0) ? 0
      : ((flags & MDBX_WRITEMAP) == 0 || MDBX_AVOID_MSYNC)
          ? MDBX_NOMETASYNC_LAZY_FD
          : MDBX_NOMETASYNC_LAZY_WRITEMAP;
  env->lck->meta_sync_txnid.weak =
      pending->txnid_a[__BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__].weak -
      sync_txnid_dist;

  *troika = meta_tap(env);
  for (MDBX_txn *txn = env->basal_txn; txn; txn = txn->nested)
    if (troika != &txn->tw.troika)
      txn->tw.troika = *troika;

  /* LY: shrink datafile if needed */
  if (unlikely(shrink)) {
    VERBOSE("shrink to %" PRIaPGNO " pages (-%" PRIaPGNO ")",
            pending->geometry.now, shrink);
    rc = dxb_resize(env, pending->geometry.first_unallocated,
                    pending->geometry.now, pending->geometry.upper,
                    impilict_shrink);
    if (rc != MDBX_SUCCESS && rc != MDBX_EPERM)
      goto fail;
    eASSERT(env, coherency_check_meta(env, target, true));
  }

  lck_t *const lck = env->lck_mmap.lck;
  if (likely(lck))
    /* toggle oldest refresh */
    atomic_store32(&lck->rdt_refresh_flag, false, mo_Relaxed);

  return MDBX_SUCCESS;

fail:
  env->flags |= ENV_FATAL_ERROR;
  return rc;
}
