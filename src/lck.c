/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2024

#include "internals.h"

__cold static int lck_setup_locked(MDBX_env *env) {
  int err = rthc_register(env);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  int lck_seize_rc = lck_seize(env);
  if (unlikely(MDBX_IS_ERROR(lck_seize_rc)))
    return lck_seize_rc;

  if (env->lck_mmap.fd == INVALID_HANDLE_VALUE) {
    env->lck = lckless_stub(env);
    env->max_readers = UINT_MAX;
    DEBUG("lck-setup:%s%s%s", " lck-less", (env->flags & MDBX_RDONLY) ? " readonly" : "",
          (lck_seize_rc == MDBX_RESULT_TRUE) ? " exclusive" : " cooperative");
    return lck_seize_rc;
  }

  DEBUG("lck-setup:%s%s%s", " with-lck", (env->flags & MDBX_RDONLY) ? " readonly" : "",
        (lck_seize_rc == MDBX_RESULT_TRUE) ? " exclusive" : " cooperative");

  MDBX_env *inprocess_neighbor = nullptr;
  err = rthc_uniq_check(&env->lck_mmap, &inprocess_neighbor);
  if (unlikely(MDBX_IS_ERROR(err)))
    return err;
  if (inprocess_neighbor) {
    if ((globals.runtime_flags & MDBX_DBG_LEGACY_MULTIOPEN) == 0 || (inprocess_neighbor->flags & MDBX_EXCLUSIVE) != 0)
      return MDBX_BUSY;
    if (lck_seize_rc == MDBX_RESULT_TRUE) {
      err = lck_downgrade(env);
      if (unlikely(err != MDBX_SUCCESS))
        return err;
      lck_seize_rc = MDBX_RESULT_FALSE;
    }
  }

  uint64_t size = 0;
  err = osal_filesize(env->lck_mmap.fd, &size);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  if (lck_seize_rc == MDBX_RESULT_TRUE) {
    size = ceil_powerof2(env->max_readers * sizeof(reader_slot_t) + sizeof(lck_t), globals.sys_pagesize);
    jitter4testing(false);
  } else {
    if (env->flags & MDBX_EXCLUSIVE)
      return MDBX_BUSY;
    if (size > INT_MAX || (size & (globals.sys_pagesize - 1)) != 0 || size < globals.sys_pagesize) {
      ERROR("lck-file has invalid size %" PRIu64 " bytes", size);
      return MDBX_PROBLEM;
    }
  }

  const size_t maxreaders = ((size_t)size - sizeof(lck_t)) / sizeof(reader_slot_t);
  if (maxreaders < 4) {
    ERROR("lck-size too small (up to %" PRIuPTR " readers)", maxreaders);
    return MDBX_PROBLEM;
  }
  env->max_readers = (maxreaders <= MDBX_READERS_LIMIT) ? (unsigned)maxreaders : (unsigned)MDBX_READERS_LIMIT;

  err = osal_mmap((env->flags & MDBX_EXCLUSIVE) | MDBX_WRITEMAP, &env->lck_mmap, (size_t)size, (size_t)size,
                  lck_seize_rc ? MMAP_OPTION_TRUNCATE | MMAP_OPTION_SEMAPHORE : MMAP_OPTION_SEMAPHORE);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

#ifdef MADV_DODUMP
  err = madvise(env->lck_mmap.lck, size, MADV_DODUMP) ? ignore_enosys(errno) : MDBX_SUCCESS;
  if (unlikely(MDBX_IS_ERROR(err)))
    return err;
#endif /* MADV_DODUMP */

#ifdef MADV_WILLNEED
  err = madvise(env->lck_mmap.lck, size, MADV_WILLNEED) ? ignore_enosys(errno) : MDBX_SUCCESS;
  if (unlikely(MDBX_IS_ERROR(err)))
    return err;
#elif defined(POSIX_MADV_WILLNEED)
  err = ignore_enosys(posix_madvise(env->lck_mmap.lck, size, POSIX_MADV_WILLNEED));
  if (unlikely(MDBX_IS_ERROR(err)))
    return err;
#endif /* MADV_WILLNEED */

  lck_t *lck = env->lck_mmap.lck;
  if (lck_seize_rc == MDBX_RESULT_TRUE) {
    /* If we succeed got exclusive lock, then nobody is using the lock region
     * and we should initialize it. */
    memset(lck, 0, (size_t)size);
    jitter4testing(false);
    lck->magic_and_version = MDBX_LOCK_MAGIC;
    lck->os_and_format = MDBX_LOCK_FORMAT;
#if MDBX_ENABLE_PGOP_STAT
    lck->pgops.wops.weak = 1;
#endif /* MDBX_ENABLE_PGOP_STAT */
    err = osal_msync(&env->lck_mmap, 0, (size_t)size, MDBX_SYNC_DATA | MDBX_SYNC_SIZE);
    if (unlikely(err != MDBX_SUCCESS)) {
      ERROR("initial-%s for lck-file failed, err %d", "msync/fsync", err);
      eASSERT(env, MDBX_IS_ERROR(err));
      return err;
    }
  } else {
    if (lck->magic_and_version != MDBX_LOCK_MAGIC) {
      const bool invalid = (lck->magic_and_version >> 8) != MDBX_MAGIC;
      ERROR("lock region has %s", invalid ? "invalid magic"
                                          : "incompatible version (only applications with nearly or the "
                                            "same versions of libmdbx can share the same database)");
      return invalid ? MDBX_INVALID : MDBX_VERSION_MISMATCH;
    }
    if (lck->os_and_format != MDBX_LOCK_FORMAT) {
      ERROR("lock region has os/format signature 0x%" PRIx32 ", expected 0x%" PRIx32, lck->os_and_format,
            MDBX_LOCK_FORMAT);
      return MDBX_VERSION_MISMATCH;
    }
  }

  err = lck_init(env, inprocess_neighbor, lck_seize_rc);
  if (unlikely(err != MDBX_SUCCESS)) {
    eASSERT(env, MDBX_IS_ERROR(err));
    return err;
  }

  env->lck = lck;
  eASSERT(env, !MDBX_IS_ERROR(lck_seize_rc));
  return lck_seize_rc;
}

__cold int lck_setup(MDBX_env *env, mdbx_mode_t mode) {
  eASSERT(env, env->lazy_fd != INVALID_HANDLE_VALUE);
  eASSERT(env, env->lck_mmap.fd == INVALID_HANDLE_VALUE);

  int err = osal_openfile(MDBX_OPEN_LCK, env, env->pathname.lck, &env->lck_mmap.fd, mode);
  if (err != MDBX_SUCCESS) {
    switch (err) {
    default:
      return err;
    case MDBX_ENOFILE:
    case MDBX_EACCESS:
    case MDBX_EPERM:
      if (!F_ISSET(env->flags, MDBX_RDONLY | MDBX_EXCLUSIVE))
        return err;
      break;
    case MDBX_EROFS:
      if ((env->flags & MDBX_RDONLY) == 0)
        return err;
      break;
    }

    if (err != MDBX_ENOFILE) {
      /* ENSURE the file system is read-only */
      err = osal_check_fs_rdonly(env->lazy_fd, env->pathname.lck, err);
      if (err != MDBX_SUCCESS &&
          /* ignore ERROR_NOT_SUPPORTED for exclusive mode */
          !(err == MDBX_ENOSYS && (env->flags & MDBX_EXCLUSIVE)))
        return err;
    }

    /* LY: without-lck mode (e.g. exclusive or on read-only filesystem) */
    env->lck_mmap.fd = INVALID_HANDLE_VALUE;
  }

  rthc_lock();
  err = lck_setup_locked(env);
  rthc_unlock();
  return err;
}

void mincore_clean_cache(const MDBX_env *const env) {
  memset(env->lck->mincore_cache.begin, -1, sizeof(env->lck->mincore_cache.begin));
}
