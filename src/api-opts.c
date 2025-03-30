/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025

#include "internals.h"

static pgno_t env_max_pgno(const MDBX_env *env) {
  return env->ps ? bytes2pgno(env, env->geo_in_bytes.upper ? env->geo_in_bytes.upper : MAX_MAPSIZE) : PAGELIST_LIMIT;
}

__cold pgno_t default_dp_limit(const MDBX_env *env) {
  /* auto-setup dp_limit by "The42" ;-) */
  intptr_t total_ram_pages, avail_ram_pages;
  int err = mdbx_get_sysraminfo(nullptr, &total_ram_pages, &avail_ram_pages);
  pgno_t dp_limit = 1024;
  if (unlikely(err != MDBX_SUCCESS))
    ERROR("mdbx_get_sysraminfo(), rc %d", err);
  else {
    size_t estimate = (size_t)(total_ram_pages + avail_ram_pages) / 42;
    if (env->ps) {
      if (env->ps > globals.sys_pagesize)
        estimate /= env->ps / globals.sys_pagesize;
      else if (env->ps < globals.sys_pagesize)
        estimate *= globals.sys_pagesize / env->ps;
    }
    dp_limit = (pgno_t)estimate;
  }

  dp_limit = (dp_limit < PAGELIST_LIMIT) ? dp_limit : PAGELIST_LIMIT;
  const pgno_t max_pgno = env_max_pgno(env);
  if (dp_limit > max_pgno - NUM_METAS)
    dp_limit = max_pgno - NUM_METAS;
  dp_limit = (dp_limit > CURSOR_STACK_SIZE * 4) ? dp_limit : CURSOR_STACK_SIZE * 4;
  return dp_limit;
}

__cold static pgno_t default_rp_augment_limit(const MDBX_env *env) {
  const size_t timeframe = /* 16 секунд */ 16 << 16;
  const size_t remain_1sec =
      (env->options.gc_time_limit < timeframe) ? timeframe - (size_t)env->options.gc_time_limit : 0;
  const size_t minimum = (env->maxgc_large1page * 2 > MDBX_PNL_INITIAL) ? env->maxgc_large1page * 2 : MDBX_PNL_INITIAL;
  const size_t one_third = env->geo_in_bytes.now / 3 >> env->ps2ln;
  const size_t augment_limit =
      (one_third > minimum) ? minimum + (one_third - minimum) / timeframe * remain_1sec : minimum;
  eASSERT(env, augment_limit < PAGELIST_LIMIT);
  return pnl_bytes2size(pnl_size2bytes(augment_limit));
}

static bool default_prefault_write(const MDBX_env *env) {
  return !MDBX_MMAP_INCOHERENT_FILE_WRITE && !env->incore &&
         (env->flags & (MDBX_WRITEMAP | MDBX_RDONLY)) == MDBX_WRITEMAP;
}

static bool default_prefer_waf_insteadof_balance(const MDBX_env *env) {
  (void)env;
  return false;
}

static uint16_t default_subpage_limit(const MDBX_env *env) {
  (void)env;
  return 65535 /* 100% */;
}

static uint16_t default_subpage_room_threshold(const MDBX_env *env) {
  (void)env;
  return 0 /* 0% */;
}

static uint16_t default_subpage_reserve_prereq(const MDBX_env *env) {
  (void)env;
  return 27525 /* 42% */;
}

static uint16_t default_subpage_reserve_limit(const MDBX_env *env) {
  (void)env;
  return 2753 /* 4.2% */;
}

static uint16_t default_merge_threshold_16dot16_percent(const MDBX_env *env) {
  (void)env;
  return 65536 / 4 /* 25% */;
}

static pgno_t default_dp_reserve_limit(const MDBX_env *env) {
  (void)env;
  return MDBX_PNL_INITIAL;
}

static pgno_t default_dp_initial(const MDBX_env *env) {
  (void)env;
  return MDBX_PNL_INITIAL;
}

static uint8_t default_spill_max_denominator(const MDBX_env *env) {
  (void)env;
  return 8;
}

static uint8_t default_spill_min_denominator(const MDBX_env *env) {
  (void)env;
  return 8;
}

static uint8_t default_spill_parent4child_denominator(const MDBX_env *env) {
  (void)env;
  return 0;
}

static uint8_t default_dp_loose_limit(const MDBX_env *env) {
  (void)env;
  return 64;
}

void env_options_init(MDBX_env *env) {
  env->options.rp_augment_limit = default_rp_augment_limit(env);
  env->options.dp_reserve_limit = default_dp_reserve_limit(env);
  env->options.dp_initial = default_dp_initial(env);
  env->options.dp_limit = default_dp_limit(env);
  env->options.spill_max_denominator = default_spill_max_denominator(env);
  env->options.spill_min_denominator = default_spill_min_denominator(env);
  env->options.spill_parent4child_denominator = default_spill_parent4child_denominator(env);
  env->options.dp_loose_limit = default_dp_loose_limit(env);
  env->options.merge_threshold_16dot16_percent = default_merge_threshold_16dot16_percent(env);
  if (default_prefer_waf_insteadof_balance(env))
    env->options.prefer_waf_insteadof_balance = true;

#if !(defined(_WIN32) || defined(_WIN64))
  env->options.writethrough_threshold =
#if defined(__linux__) || defined(__gnu_linux__)
      globals.running_on_WSL1 ? MAX_PAGENO :
#endif /* Linux */
                              MDBX_WRITETHROUGH_THRESHOLD_DEFAULT;
#endif /* Windows */

  env->options.subpage.limit = default_subpage_limit(env);
  env->options.subpage.room_threshold = default_subpage_room_threshold(env);
  env->options.subpage.reserve_prereq = default_subpage_reserve_prereq(env);
  env->options.subpage.reserve_limit = default_subpage_reserve_limit(env);
}

void env_options_adjust_dp_limit(MDBX_env *env) {
  if (!env->options.flags.non_auto.dp_limit)
    env->options.dp_limit = default_dp_limit(env);
  else {
    const pgno_t max_pgno = env_max_pgno(env);
    if (env->options.dp_limit > max_pgno - NUM_METAS)
      env->options.dp_limit = max_pgno - NUM_METAS;
    if (env->options.dp_limit < CURSOR_STACK_SIZE * 4)
      env->options.dp_limit = CURSOR_STACK_SIZE * 4;
  }
#ifdef MDBX_DEBUG_DPL_LIMIT
  env->options.dp_limit = MDBX_DEBUG_DPL_LIMIT;
#endif /* MDBX_DEBUG_DPL_LIMIT */
  if (env->options.dp_initial > env->options.dp_limit && env->options.dp_initial > default_dp_initial(env))
    env->options.dp_initial = env->options.dp_limit;
  env->options.need_dp_limit_adjust = false;
}

void env_options_adjust_defaults(MDBX_env *env) {
  if (!env->options.flags.non_auto.rp_augment_limit)
    env->options.rp_augment_limit = default_rp_augment_limit(env);
  if (!env->options.flags.non_auto.prefault_write)
    env->options.prefault_write = default_prefault_write(env);

  env->options.need_dp_limit_adjust = true;
  if (!env->txn)
    env_options_adjust_dp_limit(env);

  const size_t basis = env->geo_in_bytes.now;
  /* TODO: use options? */
  const unsigned factor = 9;
  size_t threshold = (basis < ((size_t)65536 << factor))  ? 65536        /* minimal threshold */
                     : (basis > (MEGABYTE * 4 << factor)) ? MEGABYTE * 4 /* maximal threshold */
                                                          : basis >> factor;
  threshold =
      (threshold < env->geo_in_bytes.shrink || !env->geo_in_bytes.shrink) ? threshold : env->geo_in_bytes.shrink;
  env->madv_threshold = bytes2pgno(env, bytes_align2os_bytes(env, threshold));
}

//------------------------------------------------------------------------------

__cold int mdbx_env_set_option(MDBX_env *env, const MDBX_option_t option, uint64_t value) {
  int err = check_env(env, false);
  if (unlikely(err != MDBX_SUCCESS))
    return LOG_IFERR(err);

  const bool lock_needed = ((env->flags & ENV_ACTIVE) && env->basal_txn && !env_owned_wrtxn(env));
  bool should_unlock = false;
  switch (option) {
  case MDBX_opt_sync_bytes:
    if (value == /* default */ UINT64_MAX)
      value = MAX_WRITE;
    if (unlikely(env->flags & MDBX_RDONLY))
      return LOG_IFERR(MDBX_EACCESS);
    if (unlikely(!(env->flags & ENV_ACTIVE)))
      return LOG_IFERR(MDBX_EPERM);
    if (unlikely(value > SIZE_MAX - 65536))
      return LOG_IFERR(MDBX_EINVAL);
    value = bytes2pgno(env, (size_t)value + env->ps - 1);
    if ((uint32_t)value != atomic_load32(&env->lck->autosync_threshold, mo_AcquireRelease) &&
        atomic_store32(&env->lck->autosync_threshold, (uint32_t)value, mo_Relaxed)
        /* Дергаем sync(force=off) только если задано новое не-нулевое значение
         * и мы вне транзакции */
        && lock_needed) {
      err = env_sync(env, false, false);
      if (err == /* нечего сбрасывать на диск */ MDBX_RESULT_TRUE)
        err = MDBX_SUCCESS;
    }
    break;

  case MDBX_opt_sync_period:
    if (value == /* default */ UINT64_MAX)
      value = 2780315 /* 42.42424 секунды */;
    if (unlikely(env->flags & MDBX_RDONLY))
      return LOG_IFERR(MDBX_EACCESS);
    if (unlikely(!(env->flags & ENV_ACTIVE)))
      return LOG_IFERR(MDBX_EPERM);
    if (unlikely(value > UINT32_MAX))
      return LOG_IFERR(MDBX_EINVAL);
    value = osal_16dot16_to_monotime((uint32_t)value);
    if (value != atomic_load64(&env->lck->autosync_period, mo_AcquireRelease) &&
        atomic_store64(&env->lck->autosync_period, value, mo_Relaxed)
        /* Дергаем sync(force=off) только если задано новое не-нулевое значение
         * и мы вне транзакции */
        && lock_needed) {
      err = env_sync(env, false, false);
      if (err == /* нечего сбрасывать на диск */ MDBX_RESULT_TRUE)
        err = MDBX_SUCCESS;
    }
    break;

  case MDBX_opt_max_db:
    if (value == /* default */ UINT64_MAX)
      value = 42;
    if (unlikely(value > MDBX_MAX_DBI))
      return LOG_IFERR(MDBX_EINVAL);
    if (unlikely(env->dxb_mmap.base))
      return LOG_IFERR(MDBX_EPERM);
    env->max_dbi = (unsigned)value + CORE_DBS;
    break;

  case MDBX_opt_max_readers:
    if (value == /* default */ UINT64_MAX)
      value = MDBX_READERS_LIMIT;
    if (unlikely(value < 1 || value > MDBX_READERS_LIMIT))
      return LOG_IFERR(MDBX_EINVAL);
    if (unlikely(env->dxb_mmap.base))
      return LOG_IFERR(MDBX_EPERM);
    env->max_readers = (unsigned)value;
    break;

  case MDBX_opt_dp_reserve_limit:
    if (value == /* default */ UINT64_MAX)
      value = default_dp_reserve_limit(env);
    if (unlikely(value > INT_MAX))
      return LOG_IFERR(MDBX_EINVAL);
    if (env->options.dp_reserve_limit != (unsigned)value) {
      if (lock_needed) {
        err = lck_txn_lock(env, false);
        if (unlikely(err != MDBX_SUCCESS))
          return LOG_IFERR(err);
        should_unlock = true;
      }
      env->options.dp_reserve_limit = (unsigned)value;
      while (env->shadow_reserve_len > env->options.dp_reserve_limit) {
        eASSERT(env, env->shadow_reserve != nullptr);
        page_t *dp = env->shadow_reserve;
        MDBX_ASAN_UNPOISON_MEMORY_REGION(dp, env->ps);
        VALGRIND_MAKE_MEM_DEFINED(&page_next(dp), sizeof(page_t *));
        env->shadow_reserve = page_next(dp);
        void *const ptr = ptr_disp(dp, -(ptrdiff_t)sizeof(size_t));
        osal_free(ptr);
        env->shadow_reserve_len -= 1;
      }
    }
    break;

  case MDBX_opt_rp_augment_limit:
    if (value == /* default */ UINT64_MAX) {
      env->options.flags.non_auto.rp_augment_limit = 0;
      env->options.rp_augment_limit = default_rp_augment_limit(env);
    } else if (unlikely(value > PAGELIST_LIMIT))
      return LOG_IFERR(MDBX_EINVAL);
    else {
      env->options.flags.non_auto.rp_augment_limit = 1;
      env->options.rp_augment_limit = (unsigned)value;
    }
    break;

  case MDBX_opt_gc_time_limit:
    if (value == /* default */ UINT64_MAX)
      value = 0;
    if (unlikely(value > UINT32_MAX))
      return LOG_IFERR(MDBX_EINVAL);
    if (unlikely(env->flags & MDBX_RDONLY))
      return LOG_IFERR(MDBX_EACCESS);
    value = osal_16dot16_to_monotime((uint32_t)value);
    if (value != env->options.gc_time_limit) {
      if (env->txn && lock_needed)
        return LOG_IFERR(MDBX_EPERM);
      env->options.gc_time_limit = value;
      if (!env->options.flags.non_auto.rp_augment_limit)
        env->options.rp_augment_limit = default_rp_augment_limit(env);
    }
    break;

  case MDBX_opt_txn_dp_limit:
  case MDBX_opt_txn_dp_initial:
    if (value != /* default */ UINT64_MAX && unlikely(value > PAGELIST_LIMIT || value < CURSOR_STACK_SIZE * 4))
      return LOG_IFERR(MDBX_EINVAL);
    if (unlikely(env->flags & MDBX_RDONLY))
      return LOG_IFERR(MDBX_EACCESS);
    if (lock_needed) {
      err = lck_txn_lock(env, false);
      if (unlikely(err != MDBX_SUCCESS))
        return LOG_IFERR(err);
      should_unlock = true;
    }
    if (env->txn)
      err = MDBX_EPERM /* unable change during transaction */;
    else {
      const pgno_t max_pgno = env_max_pgno(env);
      if (option == MDBX_opt_txn_dp_initial) {
        if (value == /* default */ UINT64_MAX)
          env->options.dp_initial = default_dp_initial(env);
        else {
          env->options.dp_initial = (pgno_t)value;
          if (env->options.dp_initial > max_pgno)
            env->options.dp_initial = (max_pgno > CURSOR_STACK_SIZE * 4) ? max_pgno : CURSOR_STACK_SIZE * 4;
        }
      }
      if (option == MDBX_opt_txn_dp_limit) {
        if (value == /* default */ UINT64_MAX) {
          env->options.flags.non_auto.dp_limit = 0;
        } else {
          env->options.flags.non_auto.dp_limit = 1;
          env->options.dp_limit = (pgno_t)value;
        }
        env_options_adjust_dp_limit(env);
      }
    }
    break;

  case MDBX_opt_spill_max_denominator:
    if (value == /* default */ UINT64_MAX)
      value = default_spill_max_denominator(env);
    if (unlikely(value > 255))
      return LOG_IFERR(MDBX_EINVAL);
    env->options.spill_max_denominator = (uint8_t)value;
    break;
  case MDBX_opt_spill_min_denominator:
    if (value == /* default */ UINT64_MAX)
      value = default_spill_min_denominator(env);
    if (unlikely(value > 255))
      return LOG_IFERR(MDBX_EINVAL);
    env->options.spill_min_denominator = (uint8_t)value;
    break;
  case MDBX_opt_spill_parent4child_denominator:
    if (value == /* default */ UINT64_MAX)
      value = default_spill_parent4child_denominator(env);
    if (unlikely(value > 255))
      return LOG_IFERR(MDBX_EINVAL);
    env->options.spill_parent4child_denominator = (uint8_t)value;
    break;

  case MDBX_opt_loose_limit:
    if (value == /* default */ UINT64_MAX)
      value = default_dp_loose_limit(env);
    if (unlikely(value > 255))
      return LOG_IFERR(MDBX_EINVAL);
    env->options.dp_loose_limit = (uint8_t)value;
    break;

  case MDBX_opt_merge_threshold_16dot16_percent:
    if (value == /* default */ UINT64_MAX)
      value = default_merge_threshold_16dot16_percent(env);
    if (unlikely(value < 8192 || value > 32768))
      return LOG_IFERR(MDBX_EINVAL);
    env->options.merge_threshold_16dot16_percent = (unsigned)value;
    recalculate_merge_thresholds(env);
    break;

  case MDBX_opt_writethrough_threshold:
#if defined(_WIN32) || defined(_WIN64)
    /* позволяем "установить" значение по-умолчанию и совпадающее
     * с поведением соответствующим текущей установке MDBX_NOMETASYNC */
    if (value == /* default */ UINT64_MAX && value != ((env->flags & MDBX_NOMETASYNC) ? 0 : UINT_MAX))
      err = MDBX_EINVAL;
#else
    if (value == /* default */ UINT64_MAX)
      value = MDBX_WRITETHROUGH_THRESHOLD_DEFAULT;
    if (value != (unsigned)value)
      err = MDBX_EINVAL;
    else
      env->options.writethrough_threshold = (unsigned)value;
#endif
    break;

  case MDBX_opt_prefault_write_enable:
    if (value == /* default */ UINT64_MAX) {
      env->options.prefault_write = default_prefault_write(env);
      env->options.flags.non_auto.prefault_write = false;
    } else if (value > 1)
      err = MDBX_EINVAL;
    else {
      env->options.prefault_write = value != 0;
      env->options.flags.non_auto.prefault_write = true;
    }
    break;

  case MDBX_opt_prefer_waf_insteadof_balance:
    if (value == /* default */ UINT64_MAX)
      env->options.prefer_waf_insteadof_balance = default_prefer_waf_insteadof_balance(env);
    else if (value > 1)
      err = MDBX_EINVAL;
    else
      env->options.prefer_waf_insteadof_balance = value != 0;
    break;

  case MDBX_opt_subpage_limit:
    if (value == /* default */ UINT64_MAX) {
      env->options.subpage.limit = default_subpage_limit(env);
      recalculate_subpage_thresholds(env);
    } else if (value > 65535)
      err = MDBX_EINVAL;
    else {
      env->options.subpage.limit = (uint16_t)value;
      recalculate_subpage_thresholds(env);
    }
    break;

  case MDBX_opt_subpage_room_threshold:
    if (value == /* default */ UINT64_MAX) {
      env->options.subpage.room_threshold = default_subpage_room_threshold(env);
      recalculate_subpage_thresholds(env);
    } else if (value > 65535)
      err = MDBX_EINVAL;
    else {
      env->options.subpage.room_threshold = (uint16_t)value;
      recalculate_subpage_thresholds(env);
    }
    break;

  case MDBX_opt_subpage_reserve_prereq:
    if (value == /* default */ UINT64_MAX) {
      env->options.subpage.reserve_prereq = default_subpage_reserve_prereq(env);
      recalculate_subpage_thresholds(env);
    } else if (value > 65535)
      err = MDBX_EINVAL;
    else {
      env->options.subpage.reserve_prereq = (uint16_t)value;
      recalculate_subpage_thresholds(env);
    }
    break;

  case MDBX_opt_subpage_reserve_limit:
    if (value == /* default */ UINT64_MAX) {
      env->options.subpage.reserve_limit = default_subpage_reserve_limit(env);
      recalculate_subpage_thresholds(env);
    } else if (value > 65535)
      err = MDBX_EINVAL;
    else {
      env->options.subpage.reserve_limit = (uint16_t)value;
      recalculate_subpage_thresholds(env);
    }
    break;

  default:
    return LOG_IFERR(MDBX_EINVAL);
  }

  if (should_unlock)
    lck_txn_unlock(env);
  return LOG_IFERR(err);
}

__cold int mdbx_env_get_option(const MDBX_env *env, const MDBX_option_t option, uint64_t *pvalue) {
  int err = check_env(env, false);
  if (unlikely(err != MDBX_SUCCESS))
    return LOG_IFERR(err);
  if (unlikely(!pvalue))
    return LOG_IFERR(MDBX_EINVAL);

  switch (option) {
  case MDBX_opt_sync_bytes:
    if (unlikely(!(env->flags & ENV_ACTIVE)))
      return LOG_IFERR(MDBX_EPERM);
    *pvalue = pgno2bytes(env, atomic_load32(&env->lck->autosync_threshold, mo_Relaxed));
    break;

  case MDBX_opt_sync_period:
    if (unlikely(!(env->flags & ENV_ACTIVE)))
      return LOG_IFERR(MDBX_EPERM);
    *pvalue = osal_monotime_to_16dot16(atomic_load64(&env->lck->autosync_period, mo_Relaxed));
    break;

  case MDBX_opt_max_db:
    *pvalue = env->max_dbi - CORE_DBS;
    break;

  case MDBX_opt_max_readers:
    *pvalue = env->max_readers;
    break;

  case MDBX_opt_dp_reserve_limit:
    *pvalue = env->options.dp_reserve_limit;
    break;

  case MDBX_opt_rp_augment_limit:
    *pvalue = env->options.rp_augment_limit;
    break;

  case MDBX_opt_gc_time_limit:
    *pvalue = osal_monotime_to_16dot16(env->options.gc_time_limit);
    break;

  case MDBX_opt_txn_dp_limit:
    *pvalue = env->options.dp_limit;
    break;
  case MDBX_opt_txn_dp_initial:
    *pvalue = env->options.dp_initial;
    break;

  case MDBX_opt_spill_max_denominator:
    *pvalue = env->options.spill_max_denominator;
    break;
  case MDBX_opt_spill_min_denominator:
    *pvalue = env->options.spill_min_denominator;
    break;
  case MDBX_opt_spill_parent4child_denominator:
    *pvalue = env->options.spill_parent4child_denominator;
    break;

  case MDBX_opt_loose_limit:
    *pvalue = env->options.dp_loose_limit;
    break;

  case MDBX_opt_merge_threshold_16dot16_percent:
    *pvalue = env->options.merge_threshold_16dot16_percent;
    break;

  case MDBX_opt_writethrough_threshold:
#if defined(_WIN32) || defined(_WIN64)
    *pvalue = (env->flags & MDBX_NOMETASYNC) ? 0 : INT_MAX;
#else
    *pvalue = env->options.writethrough_threshold;
#endif
    break;

  case MDBX_opt_prefault_write_enable:
    *pvalue = env->options.prefault_write;
    break;

  case MDBX_opt_prefer_waf_insteadof_balance:
    *pvalue = env->options.prefer_waf_insteadof_balance;
    break;

  case MDBX_opt_subpage_limit:
    *pvalue = env->options.subpage.limit;
    break;

  case MDBX_opt_subpage_room_threshold:
    *pvalue = env->options.subpage.room_threshold;
    break;

  case MDBX_opt_subpage_reserve_prereq:
    *pvalue = env->options.subpage.reserve_prereq;
    break;

  case MDBX_opt_subpage_reserve_limit:
    *pvalue = env->options.subpage.reserve_limit;
    break;

  default:
    return LOG_IFERR(MDBX_EINVAL);
  }

  return MDBX_SUCCESS;
}
