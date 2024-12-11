/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2024

#include "internals.h"

__cold size_t mdbx_default_pagesize(void) {
  size_t pagesize = globals.sys_pagesize;
  ENSURE(nullptr, is_powerof2(pagesize));
  pagesize = (pagesize >= MDBX_MIN_PAGESIZE) ? pagesize : MDBX_MIN_PAGESIZE;
  pagesize = (pagesize <= MDBX_MAX_PAGESIZE) ? pagesize : MDBX_MAX_PAGESIZE;
  return pagesize;
}

__cold intptr_t mdbx_limits_dbsize_min(intptr_t pagesize) {
  if (pagesize < 1)
    pagesize = (intptr_t)mdbx_default_pagesize();
  else if (unlikely(pagesize < (intptr_t)MDBX_MIN_PAGESIZE || pagesize > (intptr_t)MDBX_MAX_PAGESIZE ||
                    !is_powerof2((size_t)pagesize)))
    return -1;

  return MIN_PAGENO * pagesize;
}

__cold intptr_t mdbx_limits_dbsize_max(intptr_t pagesize) {
  if (pagesize < 1)
    pagesize = (intptr_t)mdbx_default_pagesize();
  else if (unlikely(pagesize < (intptr_t)MDBX_MIN_PAGESIZE || pagesize > (intptr_t)MDBX_MAX_PAGESIZE ||
                    !is_powerof2((size_t)pagesize)))
    return -1;

  STATIC_ASSERT(MAX_MAPSIZE < INTPTR_MAX);
  const uint64_t limit = (1 + (uint64_t)MAX_PAGENO) * pagesize;
  return (limit < MAX_MAPSIZE) ? (intptr_t)limit : (intptr_t)MAX_MAPSIZE;
}

__cold intptr_t mdbx_limits_txnsize_max(intptr_t pagesize) {
  if (pagesize < 1)
    pagesize = (intptr_t)mdbx_default_pagesize();
  else if (unlikely(pagesize < (intptr_t)MDBX_MIN_PAGESIZE || pagesize > (intptr_t)MDBX_MAX_PAGESIZE ||
                    !is_powerof2((size_t)pagesize)))
    return -1;

  STATIC_ASSERT(MAX_MAPSIZE < INTPTR_MAX);
  const uint64_t pgl_limit = pagesize * (uint64_t)(PAGELIST_LIMIT / MDBX_GOLD_RATIO_DBL);
  const uint64_t map_limit = (uint64_t)(MAX_MAPSIZE / MDBX_GOLD_RATIO_DBL);
  return (pgl_limit < map_limit) ? (intptr_t)pgl_limit : (intptr_t)map_limit;
}

__cold intptr_t mdbx_limits_keysize_max(intptr_t pagesize, MDBX_db_flags_t flags) {
  if (pagesize < 1)
    pagesize = (intptr_t)mdbx_default_pagesize();
  if (unlikely(pagesize < (intptr_t)MDBX_MIN_PAGESIZE || pagesize > (intptr_t)MDBX_MAX_PAGESIZE ||
               !is_powerof2((size_t)pagesize)))
    return -1;

  return keysize_max(pagesize, flags);
}

__cold int mdbx_env_get_maxkeysize_ex(const MDBX_env *env, MDBX_db_flags_t flags) {
  if (unlikely(!env || env->signature.weak != env_signature))
    return -1;

  return (int)mdbx_limits_keysize_max((intptr_t)env->ps, flags);
}

__cold int mdbx_env_get_maxkeysize(const MDBX_env *env) { return mdbx_env_get_maxkeysize_ex(env, MDBX_DUPSORT); }

__cold intptr_t mdbx_limits_keysize_min(MDBX_db_flags_t flags) { return keysize_min(flags); }

__cold intptr_t mdbx_limits_valsize_max(intptr_t pagesize, MDBX_db_flags_t flags) {
  if (pagesize < 1)
    pagesize = (intptr_t)mdbx_default_pagesize();
  if (unlikely(pagesize < (intptr_t)MDBX_MIN_PAGESIZE || pagesize > (intptr_t)MDBX_MAX_PAGESIZE ||
               !is_powerof2((size_t)pagesize)))
    return -1;

  return valsize_max(pagesize, flags);
}

__cold int mdbx_env_get_maxvalsize_ex(const MDBX_env *env, MDBX_db_flags_t flags) {
  if (unlikely(!env || env->signature.weak != env_signature))
    return -1;

  return (int)mdbx_limits_valsize_max((intptr_t)env->ps, flags);
}

__cold intptr_t mdbx_limits_valsize_min(MDBX_db_flags_t flags) { return valsize_min(flags); }

__cold intptr_t mdbx_limits_pairsize4page_max(intptr_t pagesize, MDBX_db_flags_t flags) {
  if (pagesize < 1)
    pagesize = (intptr_t)mdbx_default_pagesize();
  if (unlikely(pagesize < (intptr_t)MDBX_MIN_PAGESIZE || pagesize > (intptr_t)MDBX_MAX_PAGESIZE ||
               !is_powerof2((size_t)pagesize)))
    return -1;

  if (flags & (MDBX_DUPSORT | MDBX_DUPFIXED | MDBX_INTEGERDUP | MDBX_REVERSEDUP))
    return BRANCH_NODE_MAX(pagesize) - NODESIZE;

  return LEAF_NODE_MAX(pagesize) - NODESIZE;
}

__cold int mdbx_env_get_pairsize4page_max(const MDBX_env *env, MDBX_db_flags_t flags) {
  if (unlikely(!env || env->signature.weak != env_signature))
    return -1;

  return (int)mdbx_limits_pairsize4page_max((intptr_t)env->ps, flags);
}

__cold intptr_t mdbx_limits_valsize4page_max(intptr_t pagesize, MDBX_db_flags_t flags) {
  if (pagesize < 1)
    pagesize = (intptr_t)mdbx_default_pagesize();
  if (unlikely(pagesize < (intptr_t)MDBX_MIN_PAGESIZE || pagesize > (intptr_t)MDBX_MAX_PAGESIZE ||
               !is_powerof2((size_t)pagesize)))
    return -1;

  if (flags & (MDBX_DUPSORT | MDBX_DUPFIXED | MDBX_INTEGERDUP | MDBX_REVERSEDUP))
    return valsize_max(pagesize, flags);

  return PAGESPACE(pagesize);
}

__cold int mdbx_env_get_valsize4page_max(const MDBX_env *env, MDBX_db_flags_t flags) {
  if (unlikely(!env || env->signature.weak != env_signature))
    return -1;

  return (int)mdbx_limits_valsize4page_max((intptr_t)env->ps, flags);
}

/*----------------------------------------------------------------------------*/

__cold static void stat_add(const tree_t *db, MDBX_stat *const st, const size_t bytes) {
  st->ms_depth += db->height;
  st->ms_branch_pages += db->branch_pages;
  st->ms_leaf_pages += db->leaf_pages;
  st->ms_overflow_pages += db->large_pages;
  st->ms_entries += db->items;
  if (likely(bytes >= offsetof(MDBX_stat, ms_mod_txnid) + sizeof(st->ms_mod_txnid)))
    st->ms_mod_txnid = (st->ms_mod_txnid > db->mod_txnid) ? st->ms_mod_txnid : db->mod_txnid;
}

__cold static int stat_acc(const MDBX_txn *txn, MDBX_stat *st, size_t bytes) {
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

  if (env->txn && env_txn0_owned(env))
    /* inside write-txn */
    return LOG_IFERR(stat_acc(env->txn, dest, bytes));

  MDBX_txn *tmp_txn;
  err = mdbx_txn_begin((MDBX_env *)env, nullptr, MDBX_TXN_RDONLY, &tmp_txn);
  if (unlikely(err != MDBX_SUCCESS))
    return LOG_IFERR(err);

  const int rc = stat_acc(tmp_txn, dest, bytes);
  err = mdbx_txn_abort(tmp_txn);
  if (unlikely(err != MDBX_SUCCESS))
    return LOG_IFERR(err);
  return LOG_IFERR(rc);
}

/*----------------------------------------------------------------------------*/

static size_t estimate_rss(size_t database_bytes) {
  return database_bytes + database_bytes / 64 + (512 + MDBX_WORDBITS * 16) * MEGABYTE;
}

__cold int mdbx_env_warmup(const MDBX_env *env, const MDBX_txn *txn, MDBX_warmup_flags_t flags,
                           unsigned timeout_seconds_16dot16) {
  if (unlikely(env == nullptr && txn == nullptr))
    return LOG_IFERR(MDBX_EINVAL);
  if (unlikely(flags > (MDBX_warmup_force | MDBX_warmup_oomsafe | MDBX_warmup_lock | MDBX_warmup_touchlimit |
                        MDBX_warmup_release)))
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

  const uint64_t timeout_monotime = (timeout_seconds_16dot16 && (flags & MDBX_warmup_force))
                                        ? osal_monotime() + osal_16dot16_to_monotime(timeout_seconds_16dot16)
                                        : 0;

  if (flags & MDBX_warmup_release)
    munlock_all(env);

  pgno_t used_pgno;
  if (txn) {
    used_pgno = txn->geo.first_unallocated;
  } else {
    const troika_t troika = meta_tap(env);
    used_pgno = meta_recent(env, &troika).ptr_v->geometry.first_unallocated;
  }
  const size_t used_range = pgno_align2os_bytes(env, used_pgno);
  const pgno_t mlock_pgno = bytes2pgno(env, used_range);

  int rc = MDBX_SUCCESS;
  if (flags & MDBX_warmup_touchlimit) {
    const size_t estimated_rss = estimate_rss(used_range);
#if defined(_WIN32) || defined(_WIN64)
    SIZE_T current_ws_lower, current_ws_upper;
    if (GetProcessWorkingSetSize(GetCurrentProcess(), &current_ws_lower, &current_ws_upper) &&
        current_ws_lower < estimated_rss) {
      const SIZE_T ws_lower = estimated_rss;
      const SIZE_T ws_upper =
          (MDBX_WORDBITS == 32 && ws_lower > MEGABYTE * 2048) ? ws_lower : ws_lower + MDBX_WORDBITS * MEGABYTE * 32;
      if (!SetProcessWorkingSetSize(GetCurrentProcess(), ws_lower, ws_upper)) {
        rc = (int)GetLastError();
        WARNING("SetProcessWorkingSetSize(%zu, %zu) error %d", ws_lower, ws_upper, rc);
      }
    }
#endif /* Windows */
#ifdef RLIMIT_RSS
    struct rlimit rss;
    if (getrlimit(RLIMIT_RSS, &rss) == 0 && rss.rlim_cur < estimated_rss) {
      rss.rlim_cur = estimated_rss;
      if (rss.rlim_max < estimated_rss)
        rss.rlim_max = estimated_rss;
      if (setrlimit(RLIMIT_RSS, &rss)) {
        rc = errno;
        WARNING("setrlimit(%s, {%zu, %zu}) error %d", "RLIMIT_RSS", (size_t)rss.rlim_cur, (size_t)rss.rlim_max, rc);
      }
    }
#endif /* RLIMIT_RSS */
#ifdef RLIMIT_MEMLOCK
    if (flags & MDBX_warmup_lock) {
      struct rlimit memlock;
      if (getrlimit(RLIMIT_MEMLOCK, &memlock) == 0 && memlock.rlim_cur < estimated_rss) {
        memlock.rlim_cur = estimated_rss;
        if (memlock.rlim_max < estimated_rss)
          memlock.rlim_max = estimated_rss;
        if (setrlimit(RLIMIT_MEMLOCK, &memlock)) {
          rc = errno;
          WARNING("setrlimit(%s, {%zu, %zu}) error %d", "RLIMIT_MEMLOCK", (size_t)memlock.rlim_cur,
                  (size_t)memlock.rlim_max, rc);
        }
      }
    }
#endif /* RLIMIT_MEMLOCK */
    (void)estimated_rss;
  }

#if defined(MLOCK_ONFAULT) &&                                                                                          \
    ((defined(_GNU_SOURCE) && __GLIBC_PREREQ(2, 27)) || (defined(__ANDROID_API__) && __ANDROID_API__ >= 30)) &&        \
    (defined(__linux__) || defined(__gnu_linux__))
  if ((flags & MDBX_warmup_lock) != 0 && globals.linux_kernel_version >= 0x04040000 &&
      atomic_load32(&env->mlocked_pgno, mo_AcquireRelease) < mlock_pgno) {
    if (mlock2(env->dxb_mmap.base, used_range, MLOCK_ONFAULT)) {
      rc = errno;
      WARNING("mlock2(%zu, %s) error %d", used_range, "MLOCK_ONFAULT", rc);
    } else {
      update_mlcnt(env, mlock_pgno, true);
      rc = MDBX_SUCCESS;
    }
    if (rc != EINVAL)
      flags -= MDBX_warmup_lock;
  }
#endif /* MLOCK_ONFAULT */

  int err = MDBX_ENOSYS;
  err = dxb_set_readahead(env, used_pgno, true, true);
  if (err != MDBX_SUCCESS && rc == MDBX_SUCCESS)
    rc = err;

  if ((flags & MDBX_warmup_force) != 0 && (rc == MDBX_SUCCESS || rc == MDBX_ENOSYS)) {
    const volatile uint8_t *ptr = env->dxb_mmap.base;
    size_t offset = 0, unused = 42;
#if !(defined(_WIN32) || defined(_WIN64))
    if (flags & MDBX_warmup_oomsafe) {
      const int null_fd = open("/dev/null", O_WRONLY);
      if (unlikely(null_fd < 0))
        rc = errno;
      else {
        struct iovec iov[MDBX_AUXILARY_IOV_MAX];
        for (;;) {
          unsigned i;
          for (i = 0; i < MDBX_AUXILARY_IOV_MAX && offset < used_range; ++i) {
            iov[i].iov_base = (void *)(ptr + offset);
            iov[i].iov_len = 1;
            offset += globals.sys_pagesize;
          }
          if (unlikely(writev(null_fd, iov, i) < 0)) {
            rc = errno;
            if (rc == EFAULT)
              rc = ENOMEM;
            break;
          }
          if (offset >= used_range) {
            rc = MDBX_SUCCESS;
            break;
          }
          if (timeout_seconds_16dot16 && osal_monotime() > timeout_monotime) {
            rc = MDBX_RESULT_TRUE;
            break;
          }
        }
        close(null_fd);
      }
    } else
#endif /* Windows */
      for (;;) {
        unused += ptr[offset];
        offset += globals.sys_pagesize;
        if (offset >= used_range) {
          rc = MDBX_SUCCESS;
          break;
        }
        if (timeout_seconds_16dot16 && osal_monotime() > timeout_monotime) {
          rc = MDBX_RESULT_TRUE;
          break;
        }
      }
    (void)unused;
  }

  if ((flags & MDBX_warmup_lock) != 0 && (rc == MDBX_SUCCESS || rc == MDBX_ENOSYS) &&
      atomic_load32(&env->mlocked_pgno, mo_AcquireRelease) < mlock_pgno) {
#if defined(_WIN32) || defined(_WIN64)
    if (VirtualLock(env->dxb_mmap.base, used_range)) {
      update_mlcnt(env, mlock_pgno, true);
      rc = MDBX_SUCCESS;
    } else {
      rc = (int)GetLastError();
      WARNING("%s(%zu) error %d", "VirtualLock", used_range, rc);
    }
#elif defined(_POSIX_MEMLOCK_RANGE)
    if (mlock(env->dxb_mmap.base, used_range) == 0) {
      update_mlcnt(env, mlock_pgno, true);
      rc = MDBX_SUCCESS;
    } else {
      rc = errno;
      WARNING("%s(%zu) error %d", "mlock", used_range, rc);
    }
#else
    rc = MDBX_ENOSYS;
#endif
  }

  return LOG_IFERR(rc);
}

/*----------------------------------------------------------------------------*/

__cold int mdbx_env_get_fd(const MDBX_env *env, mdbx_filehandle_t *arg) {
  int rc = check_env(env, true);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  if (unlikely(!arg))
    return LOG_IFERR(MDBX_EINVAL);

  *arg = env->lazy_fd;
  return MDBX_SUCCESS;
}

__cold int mdbx_env_set_flags(MDBX_env *env, MDBX_env_flags_t flags, bool onoff) {
  int rc = check_env(env, false);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  if (unlikely(flags & ((env->flags & ENV_ACTIVE) ? ~ENV_CHANGEABLE_FLAGS : ~ENV_USABLE_FLAGS)))
    return LOG_IFERR(MDBX_EPERM);

  if (unlikely(env->flags & MDBX_RDONLY))
    return LOG_IFERR(MDBX_EACCESS);

  const bool lock_needed = (env->flags & ENV_ACTIVE) && !env_txn0_owned(env);
  bool should_unlock = false;
  if (lock_needed) {
    rc = lck_txn_lock(env, false);
    if (unlikely(rc != MDBX_SUCCESS))
      return LOG_IFERR(rc);
    should_unlock = true;
  }

  if (onoff)
    env->flags = combine_durability_flags(env->flags, flags);
  else
    env->flags &= ~flags;

  if (should_unlock)
    lck_txn_unlock(env);
  return MDBX_SUCCESS;
}

__cold int mdbx_env_get_flags(const MDBX_env *env, unsigned *arg) {
  if (unlikely(!arg))
    return LOG_IFERR(MDBX_EINVAL);

  int rc = check_env(env, false);
  if (unlikely(rc != MDBX_SUCCESS)) {
    *arg = 0;
    return LOG_IFERR(rc);
  }

  *arg = env->flags & ENV_USABLE_FLAGS;
  return MDBX_SUCCESS;
}

__cold int mdbx_env_set_userctx(MDBX_env *env, void *ctx) {
  int rc = check_env(env, false);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  env->userctx = ctx;
  return MDBX_SUCCESS;
}

__cold void *mdbx_env_get_userctx(const MDBX_env *env) { return env ? env->userctx : nullptr; }

__cold int mdbx_env_set_assert(MDBX_env *env, MDBX_assert_func *func) {
  int rc = check_env(env, false);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

#if MDBX_DEBUG
  env->assert_func = func;
  return MDBX_SUCCESS;
#else
  (void)func;
  return LOG_IFERR(MDBX_ENOSYS);
#endif
}

__cold int mdbx_env_set_hsr(MDBX_env *env, MDBX_hsr_func *hsr) {
  int rc = check_env(env, false);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  env->hsr_callback = hsr;
  return MDBX_SUCCESS;
}

__cold MDBX_hsr_func *mdbx_env_get_hsr(const MDBX_env *env) {
  return likely(env && env->signature.weak == env_signature) ? env->hsr_callback : nullptr;
}

#if defined(_WIN32) || defined(_WIN64)
__cold int mdbx_env_get_pathW(const MDBX_env *env, const wchar_t **arg) {
  int rc = check_env(env, true);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  if (unlikely(!arg))
    return LOG_IFERR(MDBX_EINVAL);

  *arg = env->pathname.specified;
  return MDBX_SUCCESS;
}
#endif /* Windows */

__cold int mdbx_env_get_path(const MDBX_env *env, const char **arg) {
  int rc = check_env(env, true);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);

  if (unlikely(!arg))
    return LOG_IFERR(MDBX_EINVAL);

#if defined(_WIN32) || defined(_WIN64)
  if (!env->pathname_char) {
    *arg = nullptr;
    DWORD flags = /* WC_ERR_INVALID_CHARS */ 0x80;
    size_t mb_len =
        WideCharToMultiByte(CP_THREAD_ACP, flags, env->pathname.specified, -1, nullptr, 0, nullptr, nullptr);
    rc = mb_len ? MDBX_SUCCESS : (int)GetLastError();
    if (rc == ERROR_INVALID_FLAGS) {
      mb_len = WideCharToMultiByte(CP_THREAD_ACP, flags = 0, env->pathname.specified, -1, nullptr, 0, nullptr, nullptr);
      rc = mb_len ? MDBX_SUCCESS : (int)GetLastError();
    }
    if (unlikely(rc != MDBX_SUCCESS))
      return LOG_IFERR(rc);

    char *const mb_pathname = osal_malloc(mb_len);
    if (!mb_pathname)
      return LOG_IFERR(MDBX_ENOMEM);
    if (mb_len != (size_t)WideCharToMultiByte(CP_THREAD_ACP, flags, env->pathname.specified, -1, mb_pathname,
                                              (int)mb_len, nullptr, nullptr)) {
      rc = (int)GetLastError();
      osal_free(mb_pathname);
      return LOG_IFERR(rc);
    }
    if (env->pathname_char ||
        InterlockedCompareExchangePointer((PVOID volatile *)&env->pathname_char, mb_pathname, nullptr))
      osal_free(mb_pathname);
  }
  *arg = env->pathname_char;
#else
  *arg = env->pathname.specified;
#endif /* Windows */
  return MDBX_SUCCESS;
}

/*------------------------------------------------------------------------------
 * Legacy API */

#ifndef LIBMDBX_NO_EXPORTS_LEGACY_API

LIBMDBX_API int mdbx_txn_begin(MDBX_env *env, MDBX_txn *parent, MDBX_txn_flags_t flags, MDBX_txn **ret) {
  return __inline_mdbx_txn_begin(env, parent, flags, ret);
}

LIBMDBX_API int mdbx_txn_commit(MDBX_txn *txn) { return __inline_mdbx_txn_commit(txn); }

LIBMDBX_API __cold int mdbx_env_stat(const MDBX_env *env, MDBX_stat *stat, size_t bytes) {
  return __inline_mdbx_env_stat(env, stat, bytes);
}

LIBMDBX_API __cold int mdbx_env_info(const MDBX_env *env, MDBX_envinfo *info, size_t bytes) {
  return __inline_mdbx_env_info(env, info, bytes);
}

LIBMDBX_API int mdbx_dbi_flags(const MDBX_txn *txn, MDBX_dbi dbi, unsigned *flags) {
  return __inline_mdbx_dbi_flags(txn, dbi, flags);
}

LIBMDBX_API __cold int mdbx_env_sync(MDBX_env *env) { return __inline_mdbx_env_sync(env); }

LIBMDBX_API __cold int mdbx_env_sync_poll(MDBX_env *env) { return __inline_mdbx_env_sync_poll(env); }

LIBMDBX_API __cold int mdbx_env_close(MDBX_env *env) { return __inline_mdbx_env_close(env); }

LIBMDBX_API __cold int mdbx_env_set_mapsize(MDBX_env *env, size_t size) {
  return __inline_mdbx_env_set_mapsize(env, size);
}

LIBMDBX_API __cold int mdbx_env_set_maxdbs(MDBX_env *env, MDBX_dbi dbs) {
  return __inline_mdbx_env_set_maxdbs(env, dbs);
}

LIBMDBX_API __cold int mdbx_env_get_maxdbs(const MDBX_env *env, MDBX_dbi *dbs) {
  return __inline_mdbx_env_get_maxdbs(env, dbs);
}

LIBMDBX_API __cold int mdbx_env_set_maxreaders(MDBX_env *env, unsigned readers) {
  return __inline_mdbx_env_set_maxreaders(env, readers);
}

LIBMDBX_API __cold int mdbx_env_get_maxreaders(const MDBX_env *env, unsigned *readers) {
  return __inline_mdbx_env_get_maxreaders(env, readers);
}

LIBMDBX_API __cold int mdbx_env_set_syncbytes(MDBX_env *env, size_t threshold) {
  return __inline_mdbx_env_set_syncbytes(env, threshold);
}

LIBMDBX_API __cold int mdbx_env_get_syncbytes(const MDBX_env *env, size_t *threshold) {
  return __inline_mdbx_env_get_syncbytes(env, threshold);
}

LIBMDBX_API __cold int mdbx_env_set_syncperiod(MDBX_env *env, unsigned seconds_16dot16) {
  return __inline_mdbx_env_set_syncperiod(env, seconds_16dot16);
}

LIBMDBX_API __cold int mdbx_env_get_syncperiod(const MDBX_env *env, unsigned *seconds_16dot16) {
  return __inline_mdbx_env_get_syncperiod(env, seconds_16dot16);
}

LIBMDBX_API __cold uint64_t mdbx_key_from_int64(const int64_t i64) { return __inline_mdbx_key_from_int64(i64); }

LIBMDBX_API __cold uint32_t mdbx_key_from_int32(const int32_t i32) { return __inline_mdbx_key_from_int32(i32); }

LIBMDBX_API __cold intptr_t mdbx_limits_pgsize_min(void) { return __inline_mdbx_limits_pgsize_min(); }

LIBMDBX_API __cold intptr_t mdbx_limits_pgsize_max(void) { return __inline_mdbx_limits_pgsize_max(); }

#endif /* LIBMDBX_NO_EXPORTS_LEGACY_API */
