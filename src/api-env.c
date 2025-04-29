/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025

#include "internals.h"

__cold static intptr_t reasonable_db_maxsize(void) {
  static intptr_t cached_result;
  if (cached_result == 0) {
    intptr_t pagesize, total_ram_pages;
    if (unlikely(mdbx_get_sysraminfo(&pagesize, &total_ram_pages, nullptr) != MDBX_SUCCESS))
      /* the 32-bit limit is good enough for fallback */
      return cached_result = MAX_MAPSIZE32;

#if defined(__SANITIZE_ADDRESS__)
    total_ram_pages >>= 4;
#endif /* __SANITIZE_ADDRESS__ */
    if (RUNNING_ON_VALGRIND)
      total_ram_pages >>= 4;

    if (unlikely((size_t)total_ram_pages * 2 > MAX_MAPSIZE / (size_t)pagesize))
      return cached_result = MAX_MAPSIZE;
    assert(MAX_MAPSIZE >= (size_t)(total_ram_pages * pagesize * 2));

    /* Suggesting should not be more than golden ratio of the size of RAM. */
    cached_result = (intptr_t)((size_t)total_ram_pages * 207 >> 7) * pagesize;

    /* Round to the nearest human-readable granulation. */
    for (size_t unit = MEGABYTE; unit; unit <<= 5) {
      const size_t floor = floor_powerof2(cached_result, unit);
      const size_t ceil = ceil_powerof2(cached_result, unit);
      const size_t threshold = (size_t)cached_result >> 4;
      const bool down = cached_result - floor < ceil - cached_result || ceil > MAX_MAPSIZE;
      if (threshold < (down ? cached_result - floor : ceil - cached_result))
        break;
      cached_result = down ? floor : ceil;
    }
  }
  return cached_result;
}

__cold static int check_alternative_lck_absent(const pathchar_t *lck_pathname) {
  int err = osal_fileexists(lck_pathname);
  if (unlikely(err != MDBX_RESULT_FALSE)) {
    if (err == MDBX_RESULT_TRUE)
      err = MDBX_DUPLICATED_CLK;
    ERROR("Alternative/Duplicate LCK-file '%" MDBX_PRIsPATH "' error %d", lck_pathname, err);
  }
  return err;
}

__cold static int env_handle_pathname(MDBX_env *env, const pathchar_t *pathname, const mdbx_mode_t mode) {
  memset(&env->pathname, 0, sizeof(env->pathname));
  if (unlikely(!pathname || !*pathname))
    return MDBX_EINVAL;

  int rc;
#if defined(_WIN32) || defined(_WIN64)
  const DWORD dwAttrib = GetFileAttributesW(pathname);
  if (dwAttrib == INVALID_FILE_ATTRIBUTES) {
    rc = GetLastError();
    if (rc != MDBX_ENOFILE)
      return rc;
    if (mode == 0 || (env->flags & MDBX_RDONLY) != 0)
      /* can't open existing */
      return rc;

    /* auto-create directory if requested */
    if ((env->flags & MDBX_NOSUBDIR) == 0 && !CreateDirectoryW(pathname, nullptr)) {
      rc = GetLastError();
      if (rc != ERROR_ALREADY_EXISTS)
        return rc;
    }
  } else {
    /* ignore passed MDBX_NOSUBDIR flag and set it automatically */
    env->flags |= MDBX_NOSUBDIR;
    if (dwAttrib & FILE_ATTRIBUTE_DIRECTORY)
      env->flags -= MDBX_NOSUBDIR;
  }
#else
  struct stat st;
  if (stat(pathname, &st) != 0) {
    rc = errno;
    if (rc != MDBX_ENOFILE)
      return rc;
    if (mode == 0 || (env->flags & MDBX_RDONLY) != 0)
      /* can't open non-existing */
      return rc /* MDBX_ENOFILE */;

    /* auto-create directory if requested */
    const mdbx_mode_t dir_mode =
        (/* inherit read/write permissions for group and others */ mode & (S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH)) |
        /* always add read/write/search for owner */ S_IRWXU |
        ((mode & S_IRGRP) ? /* +search if readable by group */ S_IXGRP : 0) |
        ((mode & S_IROTH) ? /* +search if readable by others */ S_IXOTH : 0);
    if ((env->flags & MDBX_NOSUBDIR) == 0 && mkdir(pathname, dir_mode)) {
      rc = errno;
      if (rc != EEXIST)
        return rc;
    }
  } else {
    /* ignore passed MDBX_NOSUBDIR flag and set it automatically */
    env->flags |= MDBX_NOSUBDIR;
    if (S_ISDIR(st.st_mode))
      env->flags -= MDBX_NOSUBDIR;
  }
#endif

  static const pathchar_t dxb_name[] = MDBX_DATANAME;
  static const pathchar_t lck_name[] = MDBX_LOCKNAME;
  static const pathchar_t lock_suffix[] = MDBX_LOCK_SUFFIX;

#if defined(_WIN32) || defined(_WIN64)
  assert(dxb_name[0] == '\\' && lck_name[0] == '\\');
  const size_t pathname_len = wcslen(pathname);
#else
  assert(dxb_name[0] == '/' && lck_name[0] == '/');
  const size_t pathname_len = strlen(pathname);
#endif
  assert(!osal_isdirsep(lock_suffix[0]));
  size_t base_len = pathname_len;
  static const size_t dxb_name_len = ARRAY_LENGTH(dxb_name) - 1;
  if (env->flags & MDBX_NOSUBDIR) {
    if (base_len > dxb_name_len && osal_pathequal(pathname + base_len - dxb_name_len, dxb_name, dxb_name_len)) {
      env->flags -= MDBX_NOSUBDIR;
      base_len -= dxb_name_len;
    } else if (base_len == dxb_name_len - 1 && osal_isdirsep(dxb_name[0]) && osal_isdirsep(lck_name[0]) &&
               osal_pathequal(pathname + base_len - dxb_name_len + 1, dxb_name + 1, dxb_name_len - 1)) {
      env->flags -= MDBX_NOSUBDIR;
      base_len -= dxb_name_len - 1;
    }
  }

  const size_t suflen_with_NOSUBDIR = sizeof(lock_suffix) + sizeof(pathchar_t);
  const size_t suflen_without_NOSUBDIR = sizeof(lck_name) + sizeof(dxb_name);
  const size_t enough4any =
      (suflen_with_NOSUBDIR > suflen_without_NOSUBDIR) ? suflen_with_NOSUBDIR : suflen_without_NOSUBDIR;
  const size_t bytes_needed = sizeof(pathchar_t) * (base_len * 2 + pathname_len + 1) + enough4any;
  env->pathname.buffer = osal_malloc(bytes_needed);
  if (!env->pathname.buffer)
    return MDBX_ENOMEM;

  env->pathname.specified = env->pathname.buffer;
  env->pathname.dxb = env->pathname.specified + pathname_len + 1;
  env->pathname.lck = env->pathname.dxb + base_len + dxb_name_len + 1;
  rc = MDBX_SUCCESS;
  pathchar_t *const buf = env->pathname.buffer;
  if (base_len) {
    memcpy(buf, pathname, sizeof(pathchar_t) * pathname_len);
    if (env->flags & MDBX_NOSUBDIR) {
      const pathchar_t *const lck_ext = osal_fileext(lck_name, ARRAY_LENGTH(lck_name));
      if (lck_ext) {
        pathchar_t *pathname_ext = osal_fileext(buf, pathname_len);
        memcpy(pathname_ext ? pathname_ext : buf + pathname_len, lck_ext,
               sizeof(pathchar_t) * (ARRAY_END(lck_name) - lck_ext));
        rc = check_alternative_lck_absent(buf);
      }
    } else {
      memcpy(buf + base_len, dxb_name, sizeof(dxb_name));
      memcpy(buf + base_len + dxb_name_len, lock_suffix, sizeof(lock_suffix));
      rc = check_alternative_lck_absent(buf);
    }

    memcpy(env->pathname.dxb, pathname, sizeof(pathchar_t) * (base_len + 1));
    memcpy(env->pathname.lck, pathname, sizeof(pathchar_t) * base_len);
    if (env->flags & MDBX_NOSUBDIR) {
      memcpy(env->pathname.lck + base_len, lock_suffix, sizeof(lock_suffix));
    } else {
      memcpy(env->pathname.dxb + base_len, dxb_name, sizeof(dxb_name));
      memcpy(env->pathname.lck + base_len, lck_name, sizeof(lck_name));
    }
  } else {
    assert(!(env->flags & MDBX_NOSUBDIR));
    memcpy(buf, dxb_name + 1, sizeof(dxb_name) - sizeof(pathchar_t));
    memcpy(buf + dxb_name_len - 1, lock_suffix, sizeof(lock_suffix));
    rc = check_alternative_lck_absent(buf);

    memcpy(env->pathname.dxb, dxb_name + 1, sizeof(dxb_name) - sizeof(pathchar_t));
    memcpy(env->pathname.lck, lck_name + 1, sizeof(lck_name) - sizeof(pathchar_t));
  }

  memcpy(env->pathname.specified, pathname, sizeof(pathchar_t) * (pathname_len + 1));
  return rc;
}

/*----------------------------------------------------------------------------*/

__cold int mdbx_env_create(MDBX_env **penv) {
  if (unlikely(!penv))
    return LOG_IFERR(MDBX_EINVAL);
  *penv = nullptr;

#ifdef MDBX_HAVE_C11ATOMICS
  if (unlikely(!atomic_is_lock_free((const volatile uint32_t *)penv))) {
    ERROR("lock-free atomic ops for %u-bit types is required", 32);
    return LOG_IFERR(MDBX_INCOMPATIBLE);
  }
#if MDBX_64BIT_ATOMIC
  if (unlikely(!atomic_is_lock_free((const volatile uint64_t *)penv))) {
    ERROR("lock-free atomic ops for %u-bit types is required", 64);
    return LOG_IFERR(MDBX_INCOMPATIBLE);
  }
#endif /* MDBX_64BIT_ATOMIC */
#endif /* MDBX_HAVE_C11ATOMICS */

  if (unlikely(!is_powerof2(globals.sys_pagesize) || globals.sys_pagesize < MDBX_MIN_PAGESIZE)) {
    ERROR("unsuitable system pagesize %u", globals.sys_pagesize);
    return LOG_IFERR(MDBX_INCOMPATIBLE);
  }

#if defined(__linux__) || defined(__gnu_linux__)
  if (unlikely(globals.linux_kernel_version < 0x04000000)) {
    /* 2022-09-01: Прошло уже более двух лет после окончания какой-либо
     * поддержки самого "долгоиграющего" ядра 3.16.85 ветки 3.x */
    ERROR("too old linux kernel %u.%u.%u.%u, the >= 4.0.0 is required", globals.linux_kernel_version >> 24,
          (globals.linux_kernel_version >> 16) & 255, (globals.linux_kernel_version >> 8) & 255,
          globals.linux_kernel_version & 255);
    return LOG_IFERR(MDBX_INCOMPATIBLE);
  }
#endif /* Linux */

  MDBX_env *env = osal_calloc(1, sizeof(MDBX_env));
  if (unlikely(!env))
    return LOG_IFERR(MDBX_ENOMEM);

  env->max_readers = DEFAULT_READERS;
  env->max_dbi = env->n_dbi = CORE_DBS;
  env->lazy_fd = env->dsync_fd = env->fd4meta = env->lck_mmap.fd = INVALID_HANDLE_VALUE;
  env->stuck_meta = -1;

  env_options_init(env);
  env_setup_pagesize(env, (globals.sys_pagesize < MDBX_MAX_PAGESIZE) ? globals.sys_pagesize : MDBX_MAX_PAGESIZE);

  int rc = osal_fastmutex_init(&env->dbi_lock);
  if (unlikely(rc != MDBX_SUCCESS))
    goto bailout;

#if defined(_WIN32) || defined(_WIN64)
  imports.srwl_Init(&env->remap_guard);
  InitializeCriticalSection(&env->windowsbug_lock);
#else
  rc = osal_fastmutex_init(&env->remap_guard);
  if (unlikely(rc != MDBX_SUCCESS)) {
    osal_fastmutex_destroy(&env->dbi_lock);
    goto bailout;
  }

#if MDBX_LOCKING > MDBX_LOCKING_SYSV
  lck_t *const stub = lckless_stub(env);
  rc = lck_ipclock_stubinit(&stub->wrt_lock);
#endif /* MDBX_LOCKING */
  if (unlikely(rc != MDBX_SUCCESS)) {
    osal_fastmutex_destroy(&env->remap_guard);
    osal_fastmutex_destroy(&env->dbi_lock);
    goto bailout;
  }
#endif /* Windows */

  VALGRIND_CREATE_MEMPOOL(env, 0, 0);
  env->signature.weak = env_signature;
  *penv = env;
  return MDBX_SUCCESS;

bailout:
  osal_free(env);
  return LOG_IFERR(rc);
}

__cold int mdbx_env_turn_for_recovery(MDBX_env *env, unsigned target) {
  if (unlikely(target >= NUM_METAS))
    return LOG_IFERR(MDBX_EINVAL);
  int rc = check_env(env, true);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  if (unlikely((env->flags & (MDBX_EXCLUSIVE | MDBX_RDONLY)) != MDBX_EXCLUSIVE))
    return LOG_IFERR(MDBX_EPERM);

  const meta_t *const target_meta = METAPAGE(env, target);
  txnid_t new_txnid = constmeta_txnid(target_meta);
  if (new_txnid < MIN_TXNID)
    new_txnid = MIN_TXNID;
  for (unsigned n = 0; n < NUM_METAS; ++n) {
    if (n == target)
      continue;
    page_t *const page = pgno2page(env, n);
    meta_t meta = *page_meta(page);
    if (meta_validate(env, &meta, page, n, nullptr) != MDBX_SUCCESS) {
      int err = meta_override(env, n, 0, nullptr);
      if (unlikely(err != MDBX_SUCCESS))
        return LOG_IFERR(err);
    } else {
      txnid_t txnid = constmeta_txnid(&meta);
      if (new_txnid <= txnid)
        new_txnid = safe64_txnid_next(txnid);
    }
  }

  if (unlikely(new_txnid > MAX_TXNID)) {
    ERROR("txnid overflow, raise %d", MDBX_TXN_FULL);
    return LOG_IFERR(MDBX_TXN_FULL);
  }
  return LOG_IFERR(meta_override(env, target, new_txnid, target_meta));
}

__cold int mdbx_env_open_for_recovery(MDBX_env *env, const char *pathname, unsigned target_meta, bool writeable) {
#if defined(_WIN32) || defined(_WIN64)
  wchar_t *pathnameW = nullptr;
  int rc = osal_mb2w(pathname, &pathnameW);
  if (likely(rc == MDBX_SUCCESS)) {
    rc = mdbx_env_open_for_recoveryW(env, pathnameW, target_meta, writeable);
    osal_free(pathnameW);
  }
  return LOG_IFERR(rc);
}

__cold int mdbx_env_open_for_recoveryW(MDBX_env *env, const wchar_t *pathname, unsigned target_meta, bool writeable) {
#endif /* Windows */

  if (unlikely(target_meta >= NUM_METAS))
    return LOG_IFERR(MDBX_EINVAL);
  int rc = check_env(env, false);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);
  if (unlikely(env->dxb_mmap.base))
    return LOG_IFERR(MDBX_EPERM);

  env->stuck_meta = (int8_t)target_meta;
  return
#if defined(_WIN32) || defined(_WIN64)
      mdbx_env_openW
#else
      mdbx_env_open
#endif /* Windows */
      (env, pathname, writeable ? MDBX_EXCLUSIVE : MDBX_EXCLUSIVE | MDBX_RDONLY, 0);
}

__cold int mdbx_env_delete(const char *pathname, MDBX_env_delete_mode_t mode) {
#if defined(_WIN32) || defined(_WIN64)
  wchar_t *pathnameW = nullptr;
  int rc = osal_mb2w(pathname, &pathnameW);
  if (likely(rc == MDBX_SUCCESS)) {
    rc = mdbx_env_deleteW(pathnameW, mode);
    osal_free(pathnameW);
  }
  return LOG_IFERR(rc);
}

__cold int mdbx_env_deleteW(const wchar_t *pathname, MDBX_env_delete_mode_t mode) {
#endif /* Windows */

  switch (mode) {
  default:
    return LOG_IFERR(MDBX_EINVAL);
  case MDBX_ENV_JUST_DELETE:
  case MDBX_ENV_ENSURE_UNUSED:
  case MDBX_ENV_WAIT_FOR_UNUSED:
    break;
  }

#ifdef __e2k__ /* https://bugs.mcst.ru/bugzilla/show_bug.cgi?id=6011 */
  MDBX_env *const dummy_env = alloca(sizeof(MDBX_env));
#else
  MDBX_env dummy_env_silo, *const dummy_env = &dummy_env_silo;
#endif
  memset(dummy_env, 0, sizeof(*dummy_env));
  dummy_env->flags = (mode == MDBX_ENV_ENSURE_UNUSED) ? MDBX_EXCLUSIVE : MDBX_ENV_DEFAULTS;
  dummy_env->ps = (unsigned)mdbx_default_pagesize();

  STATIC_ASSERT(sizeof(dummy_env->flags) == sizeof(MDBX_env_flags_t));
  int rc = MDBX_RESULT_TRUE, err = env_handle_pathname(dummy_env, pathname, 0);
  if (likely(err == MDBX_SUCCESS)) {
    mdbx_filehandle_t clk_handle = INVALID_HANDLE_VALUE, dxb_handle = INVALID_HANDLE_VALUE;
    if (mode > MDBX_ENV_JUST_DELETE) {
      err = osal_openfile(MDBX_OPEN_DELETE, dummy_env, dummy_env->pathname.dxb, &dxb_handle, 0);
      err = (err == MDBX_ENOFILE) ? MDBX_SUCCESS : err;
      if (err == MDBX_SUCCESS) {
        err = osal_openfile(MDBX_OPEN_DELETE, dummy_env, dummy_env->pathname.lck, &clk_handle, 0);
        err = (err == MDBX_ENOFILE) ? MDBX_SUCCESS : err;
      }
      if (err == MDBX_SUCCESS && clk_handle != INVALID_HANDLE_VALUE)
        err = osal_lockfile(clk_handle, mode == MDBX_ENV_WAIT_FOR_UNUSED);
      if (err == MDBX_SUCCESS && dxb_handle != INVALID_HANDLE_VALUE)
        err = osal_lockfile(dxb_handle, mode == MDBX_ENV_WAIT_FOR_UNUSED);
    }

    if (err == MDBX_SUCCESS) {
      err = osal_removefile(dummy_env->pathname.dxb);
      if (err == MDBX_SUCCESS)
        rc = MDBX_SUCCESS;
      else if (err == MDBX_ENOFILE)
        err = MDBX_SUCCESS;
    }

    if (err == MDBX_SUCCESS) {
      err = osal_removefile(dummy_env->pathname.lck);
      if (err == MDBX_SUCCESS)
        rc = MDBX_SUCCESS;
      else if (err == MDBX_ENOFILE)
        err = MDBX_SUCCESS;
    }

    if (err == MDBX_SUCCESS && !(dummy_env->flags & MDBX_NOSUBDIR) &&
        (/* pathname != "." */ pathname[0] != '.' || pathname[1] != 0) &&
        (/* pathname != ".." */ pathname[0] != '.' || pathname[1] != '.' || pathname[2] != 0)) {
      err = osal_removedirectory(pathname);
      if (err == MDBX_SUCCESS)
        rc = MDBX_SUCCESS;
      else if (err == MDBX_ENOFILE)
        err = MDBX_SUCCESS;
    }

    if (dxb_handle != INVALID_HANDLE_VALUE)
      osal_closefile(dxb_handle);
    if (clk_handle != INVALID_HANDLE_VALUE)
      osal_closefile(clk_handle);
  } else if (err == MDBX_ENOFILE)
    err = MDBX_SUCCESS;

  osal_free(dummy_env->pathname.buffer);
  return LOG_IFERR((err == MDBX_SUCCESS) ? rc : err);
}

__cold int mdbx_env_open(MDBX_env *env, const char *pathname, MDBX_env_flags_t flags, mdbx_mode_t mode) {
#if defined(_WIN32) || defined(_WIN64)
  wchar_t *pathnameW = nullptr;
  int rc = osal_mb2w(pathname, &pathnameW);
  if (likely(rc == MDBX_SUCCESS)) {
    rc = mdbx_env_openW(env, pathnameW, flags, mode);
    osal_free(pathnameW);
    if (rc == MDBX_SUCCESS)
      /* force to make cache of the multi-byte pathname representation */
      mdbx_env_get_path(env, &pathname);
  }
  return LOG_IFERR(rc);
}

__cold int mdbx_env_openW(MDBX_env *env, const wchar_t *pathname, MDBX_env_flags_t flags, mdbx_mode_t mode) {
#endif /* Windows */

  int rc = check_env(env, false);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  if (unlikely(flags & ~ENV_USABLE_FLAGS))
    return LOG_IFERR(MDBX_EINVAL);

  if (unlikely(env->lazy_fd != INVALID_HANDLE_VALUE || (env->flags & ENV_ACTIVE) != 0 || env->dxb_mmap.base))
    return LOG_IFERR(MDBX_EPERM);

  /* Pickup previously mdbx_env_set_flags(),
   * but avoid MDBX_UTTERLY_NOSYNC by disjunction */
  const uint32_t saved_me_flags = env->flags;
  flags = combine_durability_flags(flags | DEPRECATED_COALESCE, env->flags);

  if (flags & MDBX_RDONLY) {
    /* Silently ignore irrelevant flags when we're only getting read access */
    flags &= ~(MDBX_WRITEMAP | DEPRECATED_MAPASYNC | MDBX_SAFE_NOSYNC | MDBX_NOMETASYNC | DEPRECATED_COALESCE |
               MDBX_LIFORECLAIM | MDBX_NOMEMINIT | MDBX_ACCEDE);
    mode = 0;
  } else {
#if MDBX_MMAP_INCOHERENT_FILE_WRITE
    /* Temporary `workaround` for OpenBSD kernel's flaw.
     * See https://libmdbx.dqdkfa.ru/dead-github/issues/67 */
    if ((flags & MDBX_WRITEMAP) == 0) {
      if (flags & MDBX_ACCEDE)
        flags |= MDBX_WRITEMAP;
      else {
        debug_log(MDBX_LOG_ERROR, __func__, __LINE__,
                  "System (i.e. OpenBSD) requires MDBX_WRITEMAP because "
                  "of an internal flaw(s) in a file/buffer/page cache.\n");
        return LOG_IFERR(42 /* ENOPROTOOPT */);
      }
    }
#endif /* MDBX_MMAP_INCOHERENT_FILE_WRITE */
  }

  env->flags = (flags & ~ENV_FATAL_ERROR);
  rc = env_handle_pathname(env, pathname, mode);
  if (unlikely(rc != MDBX_SUCCESS))
    goto bailout;

  env->kvs = osal_calloc(env->max_dbi, sizeof(env->kvs[0]));
  env->dbs_flags = osal_calloc(env->max_dbi, sizeof(env->dbs_flags[0]));
  env->dbi_seqs = osal_calloc(env->max_dbi, sizeof(env->dbi_seqs[0]));
  if (unlikely(!(env->kvs && env->dbs_flags && env->dbi_seqs))) {
    rc = MDBX_ENOMEM;
    goto bailout;
  }

  if ((flags & MDBX_RDONLY) == 0) {
    env->basal_txn = txn_basal_create(env->max_dbi);
    if (unlikely(!env->basal_txn)) {
      rc = MDBX_ENOMEM;
      goto bailout;
    }
    env->basal_txn->env = env;
    env_options_adjust_defaults(env);
  }

  rc = env_open(env, mode);
  if (unlikely(rc != MDBX_SUCCESS))
    goto bailout;

#if MDBX_DEBUG
  const troika_t troika = meta_tap(env);
  const meta_ptr_t head = meta_recent(env, &troika);
  const tree_t *db = &head.ptr_c->trees.main;

  DEBUG("opened database version %u, pagesize %u", (uint8_t)unaligned_peek_u64(4, head.ptr_c->magic_and_version),
        env->ps);
  DEBUG("using meta page %" PRIaPGNO ", txn %" PRIaTXN, data_page(head.ptr_c)->pgno, head.txnid);
  DEBUG("depth: %u", db->height);
  DEBUG("entries: %" PRIu64, db->items);
  DEBUG("branch pages: %" PRIaPGNO, db->branch_pages);
  DEBUG("leaf pages: %" PRIaPGNO, db->leaf_pages);
  DEBUG("large/overflow pages: %" PRIaPGNO, db->large_pages);
  DEBUG("root: %" PRIaPGNO, db->root);
  DEBUG("schema_altered: %" PRIaTXN, db->mod_txnid);
#endif /* MDBX_DEBUG */

  if (likely(rc == MDBX_SUCCESS)) {
    dxb_sanitize_tail(env, nullptr);
  } else {
  bailout:
    if (likely(env_close(env, false) == MDBX_SUCCESS)) {
      env->flags = saved_me_flags;
    } else {
      rc = MDBX_PANIC;
      env->flags = saved_me_flags | ENV_FATAL_ERROR;
    }
  }
  return LOG_IFERR(rc);
}

/*----------------------------------------------------------------------------*/

#if !(defined(_WIN32) || defined(_WIN64))
__cold int mdbx_env_resurrect_after_fork(MDBX_env *env) {
  if (unlikely(!env))
    return LOG_IFERR(MDBX_EINVAL);

  if (unlikely(env->signature.weak != env_signature))
    return LOG_IFERR(MDBX_EBADSIGN);

  if (unlikely(env->flags & ENV_FATAL_ERROR))
    return LOG_IFERR(MDBX_PANIC);

  if (unlikely((env->flags & ENV_ACTIVE) == 0))
    return MDBX_SUCCESS;

  const uint32_t new_pid = osal_getpid();
  if (unlikely(env->pid == new_pid))
    return MDBX_SUCCESS;

  if (!atomic_cas32(&env->signature, env_signature, ~env_signature))
    return LOG_IFERR(MDBX_EBADSIGN);

  if (env->txn)
    txn_abort(env->basal_txn);
  env->registered_reader_pid = 0;
  int rc = env_close(env, true);
  env->signature.weak = env_signature;
  if (likely(rc == MDBX_SUCCESS)) {
    rc = (env->flags & MDBX_EXCLUSIVE) ? MDBX_BUSY : env_open(env, 0);
    if (unlikely(rc != MDBX_SUCCESS && env_close(env, false) != MDBX_SUCCESS)) {
      rc = MDBX_PANIC;
      env->flags |= ENV_FATAL_ERROR;
    }
  }
  return LOG_IFERR(rc);
}
#endif /* Windows */

__cold int mdbx_env_close_ex(MDBX_env *env, bool dont_sync) {
  page_t *dp;
  int rc = MDBX_SUCCESS;

  if (unlikely(!env))
    return LOG_IFERR(MDBX_EINVAL);

  if (unlikely(env->signature.weak != env_signature))
    return LOG_IFERR(MDBX_EBADSIGN);

#if MDBX_ENV_CHECKPID || !(defined(_WIN32) || defined(_WIN64))
  /* Check the PID even if MDBX_ENV_CHECKPID=0 on non-Windows
   * platforms (i.e. where fork() is available).
   * This is required to legitimize a call after fork()
   * from a child process, that should be allowed to free resources. */
  if (unlikely(env->pid != osal_getpid()))
    env->flags |= ENV_FATAL_ERROR;
#endif /* MDBX_ENV_CHECKPID */

  if (env->dxb_mmap.base && (env->flags & (MDBX_RDONLY | ENV_FATAL_ERROR)) == 0 && env->basal_txn) {
    if (env->basal_txn->owner && env->basal_txn->owner != osal_thread_self())
      return LOG_IFERR(MDBX_BUSY);
  } else
    dont_sync = true;

  if (!atomic_cas32(&env->signature, env_signature, 0))
    return LOG_IFERR(MDBX_EBADSIGN);

  if (!dont_sync) {
#if defined(_WIN32) || defined(_WIN64)
    /* On windows, without blocking is impossible to determine whether another
     * process is running a writing transaction or not.
     * Because in the "owner died" condition kernel don't release
     * file lock immediately. */
    rc = env_sync(env, true, false);
    rc = (rc == MDBX_RESULT_TRUE) ? MDBX_SUCCESS : rc;
#else
    struct stat st;
    if (unlikely(fstat(env->lazy_fd, &st)))
      rc = errno;
    else if (st.st_nlink > 0 /* don't sync deleted files */) {
      rc = env_sync(env, true, true);
      rc = (rc == MDBX_BUSY || rc == EAGAIN || rc == EACCES || rc == EBUSY || rc == EWOULDBLOCK ||
            rc == MDBX_RESULT_TRUE)
               ? MDBX_SUCCESS
               : rc;
    }
#endif /* Windows */
  }

  if (env->basal_txn && (MDBX_TXN_CHECKOWNER ? env->basal_txn->owner == osal_thread_self() : !!env->basal_txn->owner))
    lck_txn_unlock(env);

  eASSERT(env, env->signature.weak == 0);
  rc = env_close(env, false) ? MDBX_PANIC : rc;
  ENSURE(env, osal_fastmutex_destroy(&env->dbi_lock) == MDBX_SUCCESS);
#if defined(_WIN32) || defined(_WIN64)
  /* remap_guard don't have destructor (Slim Reader/Writer Lock) */
  DeleteCriticalSection(&env->windowsbug_lock);
#else
  ENSURE(env, osal_fastmutex_destroy(&env->remap_guard) == MDBX_SUCCESS);
#endif /* Windows */

#if MDBX_LOCKING > MDBX_LOCKING_SYSV
  lck_t *const stub = lckless_stub(env);
  /* может вернуть ошибку в дочернем процессе после fork() */
  lck_ipclock_destroy(&stub->wrt_lock);
#endif /* MDBX_LOCKING */

  while ((dp = env->shadow_reserve) != nullptr) {
    MDBX_ASAN_UNPOISON_MEMORY_REGION(dp, env->ps);
    VALGRIND_MAKE_MEM_DEFINED(&page_next(dp), sizeof(page_t *));
    env->shadow_reserve = page_next(dp);
    void *const ptr = ptr_disp(dp, -(ptrdiff_t)sizeof(size_t));
    osal_free(ptr);
  }
  VALGRIND_DESTROY_MEMPOOL(env);
  osal_free(env);

  return LOG_IFERR(rc);
}

/*----------------------------------------------------------------------------*/

static int env_info_snap(const MDBX_env *env, const MDBX_txn *txn, MDBX_envinfo *out, const size_t bytes,
                         troika_t *const troika) {
  const size_t size_before_bootid = offsetof(MDBX_envinfo, mi_bootid);
  const size_t size_before_pgop_stat = offsetof(MDBX_envinfo, mi_pgop_stat);
  const size_t size_before_dxbid = offsetof(MDBX_envinfo, mi_dxbid);
  if (unlikely(env->flags & ENV_FATAL_ERROR))
    return MDBX_PANIC;

  /* is the environment open?
   * (https://libmdbx.dqdkfa.ru/dead-github/issues/171) */
  if (unlikely(!env->dxb_mmap.base)) {
    /* environment not yet opened */
#if 1
    /* default behavior: returns the available info but zeroed the rest */
    memset(out, 0, bytes);
    out->mi_geo.lower = env->geo_in_bytes.lower;
    out->mi_geo.upper = env->geo_in_bytes.upper;
    out->mi_geo.shrink = env->geo_in_bytes.shrink;
    out->mi_geo.grow = env->geo_in_bytes.grow;
    out->mi_geo.current = env->geo_in_bytes.now;
    out->mi_maxreaders = env->max_readers;
    out->mi_dxb_pagesize = env->ps;
    out->mi_sys_pagesize = globals.sys_pagesize;
    if (likely(bytes > size_before_bootid)) {
      out->mi_bootid.current.x = globals.bootid.x;
      out->mi_bootid.current.y = globals.bootid.y;
    }
    return MDBX_SUCCESS;
#else
    /* some users may prefer this behavior: return appropriate error */
    return MDBX_EPERM;
#endif
  }

  *troika = (txn && !(txn->flags & MDBX_TXN_RDONLY)) ? txn->wr.troika : meta_tap(env);
  const meta_ptr_t head = meta_recent(env, troika);
  const meta_t *const meta0 = METAPAGE(env, 0);
  const meta_t *const meta1 = METAPAGE(env, 1);
  const meta_t *const meta2 = METAPAGE(env, 2);
  out->mi_recent_txnid = head.txnid;
  out->mi_meta_txnid[0] = troika->txnid[0];
  out->mi_meta_sign[0] = unaligned_peek_u64(4, meta0->sign);
  out->mi_meta_txnid[1] = troika->txnid[1];
  out->mi_meta_sign[1] = unaligned_peek_u64(4, meta1->sign);
  out->mi_meta_txnid[2] = troika->txnid[2];
  out->mi_meta_sign[2] = unaligned_peek_u64(4, meta2->sign);
  if (likely(bytes > size_before_bootid)) {
    memcpy(&out->mi_bootid.meta[0], &meta0->bootid, 16);
    memcpy(&out->mi_bootid.meta[1], &meta1->bootid, 16);
    memcpy(&out->mi_bootid.meta[2], &meta2->bootid, 16);
    if (likely(bytes > size_before_dxbid))
      memcpy(&out->mi_dxbid, &meta0->dxbid, 16);
  }

  const volatile meta_t *txn_meta = head.ptr_v;
  out->mi_last_pgno = txn_meta->geometry.first_unallocated - 1;
  out->mi_geo.current = pgno2bytes(env, txn_meta->geometry.now);
  if (txn) {
    out->mi_last_pgno = txn->geo.first_unallocated - 1;
    out->mi_geo.current = pgno2bytes(env, txn->geo.end_pgno);

    const txnid_t wanna_meta_txnid = (txn->flags & MDBX_TXN_RDONLY) ? txn->txnid : txn->txnid - xMDBX_TXNID_STEP;
    txn_meta = (out->mi_meta_txnid[0] == wanna_meta_txnid) ? meta0 : txn_meta;
    txn_meta = (out->mi_meta_txnid[1] == wanna_meta_txnid) ? meta1 : txn_meta;
    txn_meta = (out->mi_meta_txnid[2] == wanna_meta_txnid) ? meta2 : txn_meta;
  }
  out->mi_geo.lower = pgno2bytes(env, txn_meta->geometry.lower);
  out->mi_geo.upper = pgno2bytes(env, txn_meta->geometry.upper);
  out->mi_geo.shrink = pgno2bytes(env, pv2pages(txn_meta->geometry.shrink_pv));
  out->mi_geo.grow = pgno2bytes(env, pv2pages(txn_meta->geometry.grow_pv));
  out->mi_mapsize = env->dxb_mmap.limit;

  const lck_t *const lck = env->lck;
  out->mi_maxreaders = env->max_readers;
  out->mi_numreaders = env->lck_mmap.lck ? atomic_load32(&lck->rdt_length, mo_Relaxed) : INT32_MAX;
  out->mi_dxb_pagesize = env->ps;
  out->mi_sys_pagesize = globals.sys_pagesize;

  if (likely(bytes > size_before_bootid)) {
    const uint64_t unsynced_pages =
        atomic_load64(&lck->unsynced_pages, mo_Relaxed) +
        ((uint32_t)out->mi_recent_txnid != atomic_load32(&lck->meta_sync_txnid, mo_Relaxed));
    out->mi_unsync_volume = pgno2bytes(env, (size_t)unsynced_pages);
    const uint64_t monotime_now = osal_monotime();
    uint64_t ts = atomic_load64(&lck->eoos_timestamp, mo_Relaxed);
    out->mi_since_sync_seconds16dot16 = ts ? osal_monotime_to_16dot16_noUnderflow(monotime_now - ts) : 0;
    ts = atomic_load64(&lck->readers_check_timestamp, mo_Relaxed);
    out->mi_since_reader_check_seconds16dot16 = ts ? osal_monotime_to_16dot16_noUnderflow(monotime_now - ts) : 0;
    out->mi_autosync_threshold = pgno2bytes(env, atomic_load32(&lck->autosync_threshold, mo_Relaxed));
    out->mi_autosync_period_seconds16dot16 =
        osal_monotime_to_16dot16_noUnderflow(atomic_load64(&lck->autosync_period, mo_Relaxed));
    out->mi_bootid.current.x = globals.bootid.x;
    out->mi_bootid.current.y = globals.bootid.y;
    out->mi_mode = env->lck_mmap.lck ? lck->envmode.weak : env->flags;
  }

  if (likely(bytes > size_before_pgop_stat)) {
#if MDBX_ENABLE_PGOP_STAT
    out->mi_pgop_stat.newly = atomic_load64(&lck->pgops.newly, mo_Relaxed);
    out->mi_pgop_stat.cow = atomic_load64(&lck->pgops.cow, mo_Relaxed);
    out->mi_pgop_stat.clone = atomic_load64(&lck->pgops.clone, mo_Relaxed);
    out->mi_pgop_stat.split = atomic_load64(&lck->pgops.split, mo_Relaxed);
    out->mi_pgop_stat.merge = atomic_load64(&lck->pgops.merge, mo_Relaxed);
    out->mi_pgop_stat.spill = atomic_load64(&lck->pgops.spill, mo_Relaxed);
    out->mi_pgop_stat.unspill = atomic_load64(&lck->pgops.unspill, mo_Relaxed);
    out->mi_pgop_stat.wops = atomic_load64(&lck->pgops.wops, mo_Relaxed);
    out->mi_pgop_stat.prefault = atomic_load64(&lck->pgops.prefault, mo_Relaxed);
    out->mi_pgop_stat.mincore = atomic_load64(&lck->pgops.mincore, mo_Relaxed);
    out->mi_pgop_stat.msync = atomic_load64(&lck->pgops.msync, mo_Relaxed);
    out->mi_pgop_stat.fsync = atomic_load64(&lck->pgops.fsync, mo_Relaxed);
#else
    memset(&out->mi_pgop_stat, 0, sizeof(out->mi_pgop_stat));
#endif /* MDBX_ENABLE_PGOP_STAT*/
  }

  txnid_t overall_latter_reader_txnid = out->mi_recent_txnid;
  txnid_t self_latter_reader_txnid = overall_latter_reader_txnid;
  if (env->lck_mmap.lck) {
    for (size_t i = 0; i < out->mi_numreaders; ++i) {
      const uint32_t pid = atomic_load32(&lck->rdt[i].pid, mo_AcquireRelease);
      if (pid) {
        const txnid_t txnid = safe64_read(&lck->rdt[i].txnid);
        if (overall_latter_reader_txnid > txnid)
          overall_latter_reader_txnid = txnid;
        if (pid == env->pid && self_latter_reader_txnid > txnid)
          self_latter_reader_txnid = txnid;
      }
    }
  }
  out->mi_self_latter_reader_txnid = self_latter_reader_txnid;
  out->mi_latter_reader_txnid = overall_latter_reader_txnid;

  osal_compiler_barrier();
  return MDBX_SUCCESS;
}

__cold int env_info(const MDBX_env *env, const MDBX_txn *txn, MDBX_envinfo *out, size_t bytes, troika_t *troika) {
  MDBX_envinfo snap;
  int rc = env_info_snap(env, txn, &snap, sizeof(snap), troika);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  eASSERT(env, sizeof(snap) >= bytes);
  while (1) {
    rc = env_info_snap(env, txn, out, bytes, troika);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    snap.mi_since_sync_seconds16dot16 = out->mi_since_sync_seconds16dot16;
    snap.mi_since_reader_check_seconds16dot16 = out->mi_since_reader_check_seconds16dot16;
    if (likely(memcmp(&snap, out, bytes) == 0))
      return MDBX_SUCCESS;
    memcpy(&snap, out, bytes);
  }
}

__cold int mdbx_env_info_ex(const MDBX_env *env, const MDBX_txn *txn, MDBX_envinfo *arg, size_t bytes) {
  if (unlikely((env == nullptr && txn == nullptr) || arg == nullptr))
    return LOG_IFERR(MDBX_EINVAL);

  const size_t size_before_bootid = offsetof(MDBX_envinfo, mi_bootid);
  const size_t size_before_pgop_stat = offsetof(MDBX_envinfo, mi_pgop_stat);
  const size_t size_before_dxbid = offsetof(MDBX_envinfo, mi_dxbid);
  if (unlikely(bytes != sizeof(MDBX_envinfo)) && bytes != size_before_bootid && bytes != size_before_pgop_stat &&
      bytes != size_before_dxbid)
    return LOG_IFERR(MDBX_EINVAL);

  if (txn) {
    int err = check_txn(txn, MDBX_TXN_BLOCKED - MDBX_TXN_ERROR);
    if (unlikely(err != MDBX_SUCCESS))
      return LOG_IFERR(err);
  }
  if (env) {
    int err = check_env(env, false);
    if (unlikely(err != MDBX_SUCCESS))
      return LOG_IFERR(err);
    if (txn && unlikely(txn->env != env))
      return LOG_IFERR(MDBX_EINVAL);
  } else {
    env = txn->env;
  }

  troika_t troika;
  return LOG_IFERR(env_info(env, txn, arg, bytes, &troika));
}

__cold int mdbx_preopen_snapinfo(const char *pathname, MDBX_envinfo *out, size_t bytes) {
#if defined(_WIN32) || defined(_WIN64)
  wchar_t *pathnameW = nullptr;
  int rc = osal_mb2w(pathname, &pathnameW);
  if (likely(rc == MDBX_SUCCESS)) {
    rc = mdbx_preopen_snapinfoW(pathnameW, out, bytes);
    osal_free(pathnameW);
  }
  return LOG_IFERR(rc);
}

__cold int mdbx_preopen_snapinfoW(const wchar_t *pathname, MDBX_envinfo *out, size_t bytes) {
#endif /* Windows */
  if (unlikely(!out))
    return LOG_IFERR(MDBX_EINVAL);

  const size_t size_before_bootid = offsetof(MDBX_envinfo, mi_bootid);
  const size_t size_before_pgop_stat = offsetof(MDBX_envinfo, mi_pgop_stat);
  const size_t size_before_dxbid = offsetof(MDBX_envinfo, mi_dxbid);
  if (unlikely(bytes != sizeof(MDBX_envinfo)) && bytes != size_before_bootid && bytes != size_before_pgop_stat &&
      bytes != size_before_dxbid)
    return LOG_IFERR(MDBX_EINVAL);

  memset(out, 0, bytes);
  if (likely(bytes > size_before_bootid)) {
    out->mi_bootid.current.x = globals.bootid.x;
    out->mi_bootid.current.y = globals.bootid.y;
  }

  MDBX_env env;
  memset(&env, 0, sizeof(env));
  env.pid = osal_getpid();
  if (unlikely(!is_powerof2(globals.sys_pagesize) || globals.sys_pagesize < MDBX_MIN_PAGESIZE)) {
    ERROR("unsuitable system pagesize %u", globals.sys_pagesize);
    return LOG_IFERR(MDBX_INCOMPATIBLE);
  }
  out->mi_sys_pagesize = globals.sys_pagesize;
  env.flags = MDBX_RDONLY | MDBX_NORDAHEAD | MDBX_ACCEDE | MDBX_VALIDATION;
  env.stuck_meta = -1;
  env.lck_mmap.fd = INVALID_HANDLE_VALUE;
  env.lazy_fd = INVALID_HANDLE_VALUE;
  env.dsync_fd = INVALID_HANDLE_VALUE;
  env.fd4meta = INVALID_HANDLE_VALUE;
#if defined(_WIN32) || defined(_WIN64)
  env.dxb_lock_event = INVALID_HANDLE_VALUE;
  env.ioring.overlapped_fd = INVALID_HANDLE_VALUE;
#endif /* Windows */
  env_options_init(&env);

  int rc = env_handle_pathname(&env, pathname, 0);
  if (unlikely(rc != MDBX_SUCCESS))
    goto bailout;
  rc = osal_openfile(MDBX_OPEN_DXB_READ, &env, env.pathname.dxb, &env.lazy_fd, 0);
  if (unlikely(rc != MDBX_SUCCESS))
    goto bailout;

  meta_t header;
  rc = dxb_read_header(&env, &header, 0, 0);
  if (unlikely(rc != MDBX_SUCCESS))
    goto bailout;

  out->mi_dxb_pagesize = env_setup_pagesize(&env, header.pagesize);
  out->mi_geo.lower = pgno2bytes(&env, header.geometry.lower);
  out->mi_geo.upper = pgno2bytes(&env, header.geometry.upper);
  out->mi_geo.shrink = pgno2bytes(&env, pv2pages(header.geometry.shrink_pv));
  out->mi_geo.grow = pgno2bytes(&env, pv2pages(header.geometry.grow_pv));
  out->mi_geo.current = pgno2bytes(&env, header.geometry.now);
  out->mi_last_pgno = header.geometry.first_unallocated - 1;

  const unsigned n = 0;
  out->mi_recent_txnid = constmeta_txnid(&header);
  out->mi_meta_sign[n] = unaligned_peek_u64(4, &header.sign);
  if (likely(bytes > size_before_bootid)) {
    memcpy(&out->mi_bootid.meta[n], &header.bootid, 16);
    if (likely(bytes > size_before_dxbid))
      memcpy(&out->mi_dxbid, &header.dxbid, 16);
  }

bailout:
  env_close(&env, false);
  return LOG_IFERR(rc);
}

/*----------------------------------------------------------------------------*/

__cold int mdbx_env_set_geometry(MDBX_env *env, intptr_t size_lower, intptr_t size_now, intptr_t size_upper,
                                 intptr_t growth_step, intptr_t shrink_threshold, intptr_t pagesize) {
  int rc = check_env(env, false);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  MDBX_txn *const txn_owned = env_owned_wrtxn(env);
  bool should_unlock = false;

#if MDBX_DEBUG && 0 /* минимальные шаги для проверки/отладки уже не нужны */
  if (growth_step < 0) {
    growth_step = 1;
    if (shrink_threshold < 0)
      shrink_threshold = 1;
  }
#endif /* MDBX_DEBUG */

  if (env->dxb_mmap.base) {
    /* env already mapped */
    if (unlikely(env->flags & MDBX_RDONLY))
      return LOG_IFERR(MDBX_EACCESS);

    if (!txn_owned) {
      int err = lck_txn_lock(env, false);
      if (unlikely(err != MDBX_SUCCESS))
        return LOG_IFERR(err);
      should_unlock = true;
      env->basal_txn->wr.troika = meta_tap(env);
      eASSERT(env, !env->txn && !env->basal_txn->nested);
      env->basal_txn->txnid = env->basal_txn->wr.troika.txnid[env->basal_txn->wr.troika.recent];
      txn_gc_detent(env->basal_txn);
    }

    /* get untouched params from current TXN or DB */
    if (pagesize <= 0 || pagesize >= INT_MAX)
      pagesize = env->ps;
    const geo_t *const geo = env->txn ? &env->txn->geo : &meta_recent(env, &env->basal_txn->wr.troika).ptr_c->geometry;
    if (size_lower < 0)
      size_lower = pgno2bytes(env, geo->lower);
    if (size_now < 0)
      size_now = pgno2bytes(env, geo->now);
    if (size_upper < 0)
      size_upper = pgno2bytes(env, geo->upper);
    if (growth_step < 0)
      growth_step = pgno2bytes(env, pv2pages(geo->grow_pv));
    if (shrink_threshold < 0)
      shrink_threshold = pgno2bytes(env, pv2pages(geo->shrink_pv));

    if (pagesize != (intptr_t)env->ps) {
      rc = MDBX_EINVAL;
      goto bailout;
    }
    const size_t usedbytes = pgno2bytes(env, mvcc_snapshot_largest(env, geo->first_unallocated));
    if ((size_t)size_upper < usedbytes) {
      rc = MDBX_MAP_FULL;
      goto bailout;
    }
    if ((size_t)size_now < usedbytes)
      size_now = usedbytes;
  } else {
    /* env NOT yet mapped */
    if (unlikely(env->txn))
      return LOG_IFERR(MDBX_PANIC);

    /* is requested some auto-value for pagesize ? */
    if (pagesize >= INT_MAX /* maximal */)
      pagesize = MDBX_MAX_PAGESIZE;
    else if (pagesize <= 0) {
      if (pagesize < 0 /* default */) {
        pagesize = globals.sys_pagesize;
        if ((uintptr_t)pagesize > MDBX_MAX_PAGESIZE)
          pagesize = MDBX_MAX_PAGESIZE;
        eASSERT(env, (uintptr_t)pagesize >= MDBX_MIN_PAGESIZE);
      } else if (pagesize == 0 /* minimal */)
        pagesize = MDBX_MIN_PAGESIZE;

      /* choose pagesize */
      intptr_t top = (size_now > size_lower) ? size_now : size_lower;
      if (size_upper > top)
        top = size_upper;
      if (top < 0 /* default */)
        top = reasonable_db_maxsize();
      else if (top == 0 /* minimal */)
        top = MIN_MAPSIZE;
      else if (top >= (intptr_t)MAX_MAPSIZE /* maximal */)
        top = MAX_MAPSIZE;

      while (top > pagesize * (int64_t)(MAX_PAGENO + 1) && pagesize < MDBX_MAX_PAGESIZE)
        pagesize <<= 1;
    }
  }

  if (pagesize < (intptr_t)MDBX_MIN_PAGESIZE || pagesize > (intptr_t)MDBX_MAX_PAGESIZE || !is_powerof2(pagesize)) {
    rc = MDBX_EINVAL;
    goto bailout;
  }

  const bool size_lower_default = size_lower < 0;
  if (size_lower <= 0) {
    size_lower = (size_lower == 0) ? MIN_MAPSIZE : pagesize * MDBX_WORDBITS;
    if (size_lower / pagesize < MIN_PAGENO)
      size_lower = MIN_PAGENO * pagesize;
  }
  if (size_lower >= INTPTR_MAX) {
    size_lower = reasonable_db_maxsize();
    if ((size_t)size_lower / pagesize > MAX_PAGENO + 1)
      size_lower = pagesize * (MAX_PAGENO + 1);
  }

  if (size_now >= INTPTR_MAX) {
    size_now = reasonable_db_maxsize();
    if ((size_t)size_now / pagesize > MAX_PAGENO + 1)
      size_now = pagesize * (MAX_PAGENO + 1);
  }

  if (size_upper <= 0) {
    if ((growth_step == 0 || size_upper == 0) && size_now >= size_lower)
      size_upper = size_now;
    else if (size_now <= 0 || size_now >= reasonable_db_maxsize() / 2)
      size_upper = reasonable_db_maxsize();
    else if ((size_t)size_now >= MAX_MAPSIZE32 / 2 && (size_t)size_now <= MAX_MAPSIZE32 / 4 * 3)
      size_upper = MAX_MAPSIZE32;
    else {
      size_upper = ceil_powerof2(((size_t)size_now < MAX_MAPSIZE / 4) ? size_now + size_now : size_now + size_now / 2,
                                 MEGABYTE * MDBX_WORDBITS * MDBX_WORDBITS / 32);
      if ((size_t)size_upper > MAX_MAPSIZE)
        size_upper = MAX_MAPSIZE;
    }
    if ((size_t)size_upper / pagesize > (MAX_PAGENO + 1))
      size_upper = pagesize * (MAX_PAGENO + 1);
  } else if (size_upper >= INTPTR_MAX) {
    size_upper = reasonable_db_maxsize();
    if ((size_t)size_upper / pagesize > MAX_PAGENO + 1)
      size_upper = pagesize * (MAX_PAGENO + 1);
  }

  if (unlikely(size_lower < (intptr_t)MIN_MAPSIZE || size_lower > size_upper)) {
    /* паранойа на случай переполнения при невероятных значениях */
    rc = MDBX_EINVAL;
    goto bailout;
  }

  if (size_now <= 0) {
    size_now = size_lower;
    if (size_upper >= size_lower && size_now > size_upper)
      size_now = size_upper;
  }

  if ((uint64_t)size_lower / pagesize < MIN_PAGENO) {
    size_lower = pagesize * MIN_PAGENO;
    if (unlikely(size_lower > size_upper)) {
      /* паранойа на случай переполнения при невероятных значениях */
      rc = MDBX_EINVAL;
      goto bailout;
    }
    if (size_now < size_lower)
      size_now = size_lower;
  }

  if (unlikely((size_t)size_upper > MAX_MAPSIZE || (uint64_t)size_upper / pagesize > MAX_PAGENO + 1)) {
    rc = MDBX_TOO_LARGE;
    goto bailout;
  }

  const size_t unit = (globals.sys_pagesize > (size_t)pagesize) ? globals.sys_pagesize : (size_t)pagesize;
  size_lower = ceil_powerof2(size_lower, unit);
  size_upper = ceil_powerof2(size_upper, unit);
  size_now = ceil_powerof2(size_now, unit);

  /* LY: подбираем значение size_upper:
   *  - кратное размеру страницы
   *  - без нарушения MAX_MAPSIZE и MAX_PAGENO */
  while (unlikely((size_t)size_upper > MAX_MAPSIZE || (uint64_t)size_upper / pagesize > MAX_PAGENO + 1)) {
    if ((size_t)size_upper < unit + MIN_MAPSIZE || (size_t)size_upper < (size_t)pagesize * (MIN_PAGENO + 1)) {
      /* паранойа на случай переполнения при невероятных значениях */
      rc = MDBX_EINVAL;
      goto bailout;
    }
    size_upper -= unit;
    if ((size_t)size_upper < (size_t)size_lower)
      size_lower = size_upper;
  }
  eASSERT(env, (size_upper - size_lower) % globals.sys_pagesize == 0);

  if (size_now < size_lower)
    size_now = size_lower;
  if (size_now > size_upper)
    size_now = size_upper;

  if (growth_step < 0) {
    growth_step = ((size_t)(size_upper - size_lower)) / 42;
    if (!size_lower_default && growth_step > size_lower && size_lower < (intptr_t)MEGABYTE)
      growth_step = size_lower;
    else if (growth_step / size_lower > 64)
      growth_step = size_lower << 6;
    if (growth_step < 65536)
      growth_step = 65536;
    if ((size_upper - size_lower) / growth_step > 65536)
      growth_step = (size_upper - size_lower) >> 16;
    const intptr_t growth_step_limit = MEGABYTE * ((MDBX_WORDBITS > 32) ? 4096 : 256);
    if (growth_step > growth_step_limit)
      growth_step = growth_step_limit;
  }
  if (growth_step == 0 && shrink_threshold > 0)
    growth_step = 1;
  growth_step = ceil_powerof2(growth_step, unit);

  if (shrink_threshold < 0)
    shrink_threshold = growth_step + growth_step;
  shrink_threshold = ceil_powerof2(shrink_threshold, unit);

  //----------------------------------------------------------------------------

  if (!env->dxb_mmap.base) {
    /* save user's geo-params for future open/create */
    if (pagesize != (intptr_t)env->ps)
      env_setup_pagesize(env, pagesize);
    env->geo_in_bytes.lower = size_lower;
    env->geo_in_bytes.now = size_now;
    env->geo_in_bytes.upper = size_upper;
    env->geo_in_bytes.grow = pgno2bytes(env, pv2pages(pages2pv(bytes2pgno(env, growth_step))));
    env->geo_in_bytes.shrink = pgno2bytes(env, pv2pages(pages2pv(bytes2pgno(env, shrink_threshold))));
    env_options_adjust_defaults(env);

    ENSURE(env, env->geo_in_bytes.lower >= MIN_MAPSIZE);
    ENSURE(env, env->geo_in_bytes.lower / (unsigned)pagesize >= MIN_PAGENO);
    ENSURE(env, env->geo_in_bytes.lower % (unsigned)pagesize == 0);
    ENSURE(env, env->geo_in_bytes.lower % globals.sys_pagesize == 0);

    ENSURE(env, env->geo_in_bytes.upper <= MAX_MAPSIZE);
    ENSURE(env, env->geo_in_bytes.upper / (unsigned)pagesize <= MAX_PAGENO + 1);
    ENSURE(env, env->geo_in_bytes.upper % (unsigned)pagesize == 0);
    ENSURE(env, env->geo_in_bytes.upper % globals.sys_pagesize == 0);

    ENSURE(env, env->geo_in_bytes.now >= env->geo_in_bytes.lower);
    ENSURE(env, env->geo_in_bytes.now <= env->geo_in_bytes.upper);
    ENSURE(env, env->geo_in_bytes.now % (unsigned)pagesize == 0);
    ENSURE(env, env->geo_in_bytes.now % globals.sys_pagesize == 0);

    ENSURE(env, env->geo_in_bytes.grow % (unsigned)pagesize == 0);
    ENSURE(env, env->geo_in_bytes.grow % globals.sys_pagesize == 0);
    ENSURE(env, env->geo_in_bytes.shrink % (unsigned)pagesize == 0);
    ENSURE(env, env->geo_in_bytes.shrink % globals.sys_pagesize == 0);

    rc = MDBX_SUCCESS;
  } else {
    /* apply new params to opened environment */
    ENSURE(env, pagesize == (intptr_t)env->ps);
    meta_t meta;
    memset(&meta, 0, sizeof(meta));
    if (!env->txn) {
      const meta_ptr_t head = meta_recent(env, &env->basal_txn->wr.troika);

      uint64_t timestamp = 0;
      while ("workaround for "
             "https://libmdbx.dqdkfa.ru/dead-github/issues/269") {
        rc = coherency_fetch_head(env->basal_txn, head, &timestamp);
        if (likely(rc == MDBX_SUCCESS))
          break;
        if (unlikely(rc != MDBX_RESULT_TRUE))
          goto bailout;
      }
      meta = *head.ptr_c;
      const txnid_t txnid = safe64_txnid_next(head.txnid);
      if (unlikely(txnid > MAX_TXNID)) {
        rc = MDBX_TXN_FULL;
        ERROR("txnid overflow, raise %d", rc);
        goto bailout;
      }
      meta_set_txnid(env, &meta, txnid);
    }

    const geo_t *const current_geo = &(env->txn ? env->txn : env->basal_txn)->geo;
    /* update env-geo to avoid influences */
    env->geo_in_bytes.now = pgno2bytes(env, current_geo->now);
    env->geo_in_bytes.lower = pgno2bytes(env, current_geo->lower);
    env->geo_in_bytes.upper = pgno2bytes(env, current_geo->upper);
    env->geo_in_bytes.grow = pgno2bytes(env, pv2pages(current_geo->grow_pv));
    env->geo_in_bytes.shrink = pgno2bytes(env, pv2pages(current_geo->shrink_pv));

    geo_t new_geo;
    new_geo.lower = bytes2pgno(env, size_lower);
    new_geo.now = bytes2pgno(env, size_now);
    new_geo.upper = bytes2pgno(env, size_upper);
    new_geo.grow_pv = pages2pv(bytes2pgno(env, growth_step));
    new_geo.shrink_pv = pages2pv(bytes2pgno(env, shrink_threshold));
    new_geo.first_unallocated = current_geo->first_unallocated;

    ENSURE(env, pgno_align2os_bytes(env, new_geo.lower) == (size_t)size_lower);
    ENSURE(env, pgno_align2os_bytes(env, new_geo.upper) == (size_t)size_upper);
    ENSURE(env, pgno_align2os_bytes(env, new_geo.now) == (size_t)size_now);
    ENSURE(env, new_geo.grow_pv == pages2pv(pv2pages(new_geo.grow_pv)));
    ENSURE(env, new_geo.shrink_pv == pages2pv(pv2pages(new_geo.shrink_pv)));

    ENSURE(env, (size_t)size_lower >= MIN_MAPSIZE);
    ENSURE(env, new_geo.lower >= MIN_PAGENO);
    ENSURE(env, (size_t)size_upper <= MAX_MAPSIZE);
    ENSURE(env, new_geo.upper <= MAX_PAGENO + 1);
    ENSURE(env, new_geo.now >= new_geo.first_unallocated);
    ENSURE(env, new_geo.upper >= new_geo.now);
    ENSURE(env, new_geo.now >= new_geo.lower);

    if (memcmp(current_geo, &new_geo, sizeof(geo_t)) != 0) {
#if defined(_WIN32) || defined(_WIN64)
      /* Was DB shrinking disabled before and now it will be enabled? */
      if (new_geo.lower < new_geo.upper && new_geo.shrink_pv &&
          !(current_geo->lower < current_geo->upper && current_geo->shrink_pv)) {
        if (!env->lck_mmap.lck) {
          rc = MDBX_EPERM;
          goto bailout;
        }
        int err = lck_rdt_lock(env);
        if (unlikely(MDBX_IS_ERROR(err))) {
          rc = err;
          goto bailout;
        }

        /* Check if there are any reading threads that do not use the SRWL */
        const size_t CurrentTid = GetCurrentThreadId();
        const reader_slot_t *const begin = env->lck_mmap.lck->rdt;
        const reader_slot_t *const end = begin + atomic_load32(&env->lck_mmap.lck->rdt_length, mo_AcquireRelease);
        for (const reader_slot_t *reader = begin; reader < end; ++reader) {
          if (reader->pid.weak == env->pid && reader->tid.weak != CurrentTid) {
            /* At least one thread may don't use SRWL */
            rc = MDBX_EPERM;
            break;
          }
        }

        lck_rdt_unlock(env);
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
      }
#endif /* Windows */

      if (new_geo.now != current_geo->now || new_geo.upper != current_geo->upper) {
        rc = dxb_resize(env, current_geo->first_unallocated, new_geo.now, new_geo.upper, explicit_resize);
        if (unlikely(rc != MDBX_SUCCESS))
          goto bailout;
      }
      if (env->txn) {
        env->txn->geo = new_geo;
        env->txn->flags |= MDBX_TXN_DIRTY;
      } else {
        meta.geometry = new_geo;
        rc = dxb_sync_locked(env, env->flags, &meta, &env->basal_txn->wr.troika);
        if (likely(rc == MDBX_SUCCESS)) {
          env->geo_in_bytes.now = pgno2bytes(env, new_geo.now = meta.geometry.now);
          env->geo_in_bytes.upper = pgno2bytes(env, new_geo.upper = meta.geometry.upper);
        }
      }
    }
    if (likely(rc == MDBX_SUCCESS)) {
      /* update env-geo to avoid influences */
      eASSERT(env, env->geo_in_bytes.now == pgno2bytes(env, new_geo.now));
      env->geo_in_bytes.lower = pgno2bytes(env, new_geo.lower);
      eASSERT(env, env->geo_in_bytes.upper == pgno2bytes(env, new_geo.upper));
      env->geo_in_bytes.grow = pgno2bytes(env, pv2pages(new_geo.grow_pv));
      env->geo_in_bytes.shrink = pgno2bytes(env, pv2pages(new_geo.shrink_pv));
    }
  }

bailout:
  if (should_unlock)
    lck_txn_unlock(env);
  return LOG_IFERR(rc);
}

__cold int mdbx_env_sync_ex(MDBX_env *env, bool force, bool nonblock) {
  int rc = check_env(env, true);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  return LOG_IFERR(env_sync(env, force, nonblock));
}

/*----------------------------------------------------------------------------*/

static void stat_add(const tree_t *db, MDBX_stat *const st, const size_t bytes) {
  st->ms_depth += db->height;
  st->ms_branch_pages += db->branch_pages;
  st->ms_leaf_pages += db->leaf_pages;
  st->ms_overflow_pages += db->large_pages;
  st->ms_entries += db->items;
  if (likely(bytes >= offsetof(MDBX_stat, ms_mod_txnid) + sizeof(st->ms_mod_txnid)))
    st->ms_mod_txnid = (st->ms_mod_txnid > db->mod_txnid) ? st->ms_mod_txnid : db->mod_txnid;
}

static int stat_acc(const MDBX_txn *txn, MDBX_stat *st, size_t bytes) {
  memset(st, 0, bytes);

  int err = check_txn(txn, MDBX_TXN_BLOCKED);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  cursor_couple_t cx;
  err = cursor_init(&cx.outer, (MDBX_txn *)txn, MAIN_DBI);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  const MDBX_env *const env = txn->env;
  st->ms_psize = env->ps;
  TXN_FOREACH_DBI_FROM(txn, dbi,
                       /* assuming GC is internal and not subject for accounting */ MAIN_DBI) {
    if ((txn->dbi_state[dbi] & (DBI_VALID | DBI_STALE)) == DBI_VALID)
      stat_add(txn->dbs + dbi, st, bytes);
  }

  if (!(txn->dbs[MAIN_DBI].flags & MDBX_DUPSORT) && txn->dbs[MAIN_DBI].items /* TODO: use `md_subs` field */) {

    /* scan and account not opened named tables */
    err = tree_search(&cx.outer, nullptr, Z_FIRST);
    while (err == MDBX_SUCCESS) {
      const page_t *mp = cx.outer.pg[cx.outer.top];
      for (size_t i = 0; i < page_numkeys(mp); i++) {
        const node_t *node = page_node(mp, i);
        if (node_flags(node) != N_TREE)
          continue;
        if (unlikely(node_ds(node) != sizeof(tree_t))) {
          ERROR("%s/%d: %s %zu", "MDBX_CORRUPTED", MDBX_CORRUPTED, "invalid table node size", node_ds(node));
          return MDBX_CORRUPTED;
        }

        /* skip opened and already accounted */
        const MDBX_val name = {node_key(node), node_ks(node)};
        TXN_FOREACH_DBI_USER(txn, dbi) {
          if ((txn->dbi_state[dbi] & (DBI_VALID | DBI_STALE)) == DBI_VALID &&
              env->kvs[MAIN_DBI].clc.k.cmp(&name, &env->kvs[dbi].name) == 0) {
            node = nullptr;
            break;
          }
        }

        if (node) {
          tree_t db;
          memcpy(&db, node_data(node), sizeof(db));
          stat_add(&db, st, bytes);
        }
      }
      err = cursor_sibling_right(&cx.outer);
    }
    if (unlikely(err != MDBX_NOTFOUND))
      return err;
  }

  return MDBX_SUCCESS;
}

__cold int mdbx_env_stat_ex(const MDBX_env *env, const MDBX_txn *txn, MDBX_stat *dest, size_t bytes) {
  if (unlikely(!dest))
    return LOG_IFERR(MDBX_EINVAL);
  const size_t size_before_modtxnid = offsetof(MDBX_stat, ms_mod_txnid);
  if (unlikely(bytes != sizeof(MDBX_stat)) && bytes != size_before_modtxnid)
    return LOG_IFERR(MDBX_EINVAL);

  if (likely(txn)) {
    if (env && unlikely(txn->env != env))
      return LOG_IFERR(MDBX_EINVAL);
    return LOG_IFERR(stat_acc(txn, dest, bytes));
  }

  int err = check_env(env, true);
  if (unlikely(err != MDBX_SUCCESS))
    return LOG_IFERR(err);

  MDBX_txn *txn_owned = env_owned_wrtxn(env);
  if (txn_owned)
    /* inside write-txn */
    return LOG_IFERR(stat_acc(txn_owned, dest, bytes));

  err = mdbx_txn_begin((MDBX_env *)env, nullptr, MDBX_TXN_RDONLY, &txn_owned);
  if (unlikely(err != MDBX_SUCCESS))
    return LOG_IFERR(err);

  const int rc = stat_acc(txn_owned, dest, bytes);
  err = mdbx_txn_abort(txn_owned);
  if (unlikely(err != MDBX_SUCCESS))
    return LOG_IFERR(err);
  return LOG_IFERR(rc);
}
