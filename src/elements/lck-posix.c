/*
 * Copyright 2015-2019 Leonid Yuriev <leo@yuriev.ru>
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

#include "internals.h"

/*----------------------------------------------------------------------------*/
/* global constructor/destructor */

#if defined(__linux__) || defined(__gnu_linux__)
#include <sys/utsname.h>
#ifndef MDBX_ALLOY
uint32_t mdbx_linux_kernel_version;
#endif /* MDBX_ALLOY */
#endif /* Linux */

static __cold __attribute__((__constructor__)) void
mdbx_global_constructor(void) {
#if defined(__linux__) || defined(__gnu_linux__)
  struct utsname buffer;
  if (uname(&buffer) == 0) {
    int i = 0;
    char *p = buffer.release;
    while (*p && i < 4) {
      if (*p >= '0' && *p <= '9') {
        long number = strtol(p, &p, 10);
        if (number > 0) {
          if (number > 255)
            number = 255;
          mdbx_linux_kernel_version += number << (24 - i * 8);
        }
        ++i;
      } else {
        ++p;
      }
    }
  }
#endif /* Linux */

  mdbx_rthc_global_init();
}

static __cold __attribute__((__destructor__)) void
mdbx_global_destructor(void) {
  mdbx_rthc_global_dtor();
}

/*----------------------------------------------------------------------------*/
/* lck */

/* Описание реализации блокировок для POSIX & Linux:
 *
 * lck-файл отображается в память, в нём организуется таблица читателей и
 * размещаются совместно используемые posix-мьютексы (futex). Посредством
 * этих мьютексов (см struct MDBX_lockinfo) реализуются:
 *  - Блокировка таблицы читателей для регистрации,
 *    т.е. функции mdbx_rdt_lock() и mdbx_rdt_unlock().
 *  - Блокировка БД для пишущих транзакций,
 *    т.е. функции mdbx_txn_lock() и mdbx_txn_unlock().
 *
 * Остальной функционал реализуется отдельно посредством файловых блокировок:
 *  - Первоначальный захват БД в режиме exclusive/shared и последующий перевод
 *    в операционный режим, функции mdbx_lck_seize() и mdbx_lck_downgrade().
 *  - Проверка присутствие процессов-читателей,
 *    т.е. функции mdbx_rpid_set(), mdbx_rpid_clear() и mdbx_rpid_check().
 *
 * Для блокировки файлов используется fcntl(F_SETLK), так как:
 *  - lockf() оперирует только эксклюзивной блокировкой и требует
 *    открытия файла в RW-режиме.
 *  - flock() не гарантирует атомарности при смене блокировок
 *    и оперирует только всем файлом целиком.
 *  - Для контроля процессов-читателей используются однобайтовые
 *    range-блокировки lck-файла посредством fcntl(F_SETLK). При этом
 *    в качестве позиции используется pid процесса-читателя.
 *  - Для первоначального захвата и shared/exclusive выполняется блокировка
 *    основного файла БД и при успехе lck-файла.
 *
 * ----------------------------------------------------------------------------
 * УДЕРЖИВАЕМЫЕ БЛОКИРОВКИ В ЗАВИСИМОСТИ ОТ РЕЖИМА И СОСТОЯНИЯ
 *
 * Эксклюзивный режим без lck-файла:
 *   = заблокирован весь dxb-файл посредством F_RDLCK или F_WRLCK,
 *     в зависимости от MDBX_RDONLY.
 *
 * Не-операционный режим на время пере-инициализации и разрушении lck-файла:
 *   = F_WRLCK блокировка первого байта lck-файла, другие процессы ждут её
 *     снятия при получении F_RDLCK через F_SETLKW.
 *   - блокировки dxb-файла могут меняться до снятие эксклюзивной блокировки
 *    lck-файла:
 *       + для НЕ-эксклюзивного режима блокировка pid-байта в dxb-файле
 *         посредством F_RDLCK или F_WRLCK, в зависимости от MDBX_RDONLY.
 *       + для ЭКСКЛЮЗИВНОГО режима блокировка pid-байта всего dxb-файла
 *         посредством F_RDLCK или F_WRLCK, в зависимости от MDBX_RDONLY.
 *
 * ОПЕРАЦИОННЫЙ режим с lck-файлом:
 *   = F_RDLCK блокировка первого байта lck-файла, другие процессы не могут
 *     получить F_WRLCK и таким образом видят что БД используется.
 *   + F_WRLCK блокировка pid-байта в clk-файле после первой транзакции чтения.
 *   + для НЕ-эксклюзивного режима блокировка pid-байта в dxb-файле
 *     посредством F_RDLCK или F_WRLCK, в зависимости от MDBX_RDONLY.
 *   + для ЭКСКЛЮЗИВНОГО режима блокировка pid-байта всего dxb-файла
 *     посредством F_RDLCK или F_WRLCK, в зависимости от MDBX_RDONLY.
 */

#if MDBX_USE_OFDLOCKS
static int op_setlk, op_setlkw, op_getlk;
static void __cold choice_fcntl() {
  assert(!op_setlk && !op_setlkw && !op_getlk);
  if ((mdbx_runtime_flags & MDBX_DBG_LEGACY_MULTIOPEN) == 0
#if defined(__linux__) || defined(__gnu_linux__)
      && mdbx_linux_kernel_version >
             0x030f0000 /* OFD locks are available since 3.15, but engages here
             only for 3.16 and larer kernels (LTS) for reliability reasons */
#endif                  /* linux */
  ) {
    op_setlk = F_OFD_SETLK;
    op_setlkw = F_OFD_SETLKW;
    op_getlk = F_OFD_GETLK;
    return;
  }
  op_setlk = F_SETLK;
  op_setlkw = F_SETLKW;
  op_getlk = F_GETLK;
}
#else
#define op_setlk F_SETLK
#define op_setlkw F_SETLKW
#define op_getlk F_GETLK
#endif /* MDBX_USE_OFDLOCKS */

#ifndef OFF_T_MAX
#define OFF_T_MAX                                                              \
  ((sizeof(off_t) > 4 ? INT64_MAX : INT32_MAX) & ~(size_t)0xffff)
#endif

static int lck_op(mdbx_filehandle_t fd, int cmd, int lck, off_t offset,
                  off_t len) {
  mdbx_jitter4testing(true);
  for (;;) {
    struct flock lock_op;
    memset(&lock_op, 0, sizeof(lock_op));
    lock_op.l_type = lck;
    lock_op.l_whence = SEEK_SET;
    lock_op.l_start = offset;
    lock_op.l_len = len;
    int rc = fcntl(fd, cmd, &lock_op);
    mdbx_jitter4testing(true);
    if (rc != -1) {
      if (cmd == op_getlk) {
        /* Checks reader by pid. Returns:
         *   MDBX_RESULT_TRUE   - if pid is live (unable to acquire lock)
         *   MDBX_RESULT_FALSE  - if pid is dead (lock acquired). */
        return (lock_op.l_type == F_UNLCK) ? MDBX_RESULT_FALSE
                                           : MDBX_RESULT_TRUE;
      }
      return MDBX_SUCCESS;
    }
    rc = errno;
    if (rc != EINTR || cmd == op_setlkw) {
      mdbx_assert(nullptr, MDBX_IS_ERROR(rc));
      return rc;
    }
  }
}

MDBX_INTERNAL_FUNC int mdbx_rpid_set(MDBX_env *env) {
  assert(env->me_lfd != INVALID_HANDLE_VALUE);
  assert(env->me_pid > 0);
  if (unlikely(mdbx_getpid() != env->me_pid))
    return MDBX_PANIC;
  return lck_op(env->me_lfd, op_setlk, F_WRLCK, env->me_pid, 1);
}

MDBX_INTERNAL_FUNC int mdbx_rpid_clear(MDBX_env *env) {
  assert(env->me_lfd != INVALID_HANDLE_VALUE);
  assert(env->me_pid > 0);
  return lck_op(env->me_lfd, op_setlk, F_UNLCK, env->me_pid, 1);
}

MDBX_INTERNAL_FUNC int mdbx_rpid_check(MDBX_env *env, uint32_t pid) {
  assert(env->me_lfd != INVALID_HANDLE_VALUE);
  assert(pid > 0);
  return lck_op(env->me_lfd, op_getlk, F_WRLCK, pid, 1);
}

/*---------------------------------------------------------------------------*/

MDBX_INTERNAL_FUNC int __cold mdbx_lck_seize(MDBX_env *env) {
  assert(env->me_fd != INVALID_HANDLE_VALUE);
  if (unlikely(mdbx_getpid() != env->me_pid))
    return MDBX_PANIC;
#if MDBX_USE_OFDLOCKS
  if (unlikely(op_setlk == 0))
    choice_fcntl();
#endif /* MDBX_USE_OFDLOCKS */

  int rc;
  if (env->me_lfd == INVALID_HANDLE_VALUE) {
    /* LY: without-lck mode (e.g. exclusive or on read-only filesystem) */
    rc =
        lck_op(env->me_fd, op_setlk,
               (env->me_flags & MDBX_RDONLY) ? F_RDLCK : F_WRLCK, 0, OFF_T_MAX);
    if (rc != MDBX_SUCCESS) {
      mdbx_error("%s(%s) failed: errcode %u", __func__, "without-lck", rc);
      mdbx_assert(env, MDBX_IS_ERROR(rc));
      return rc;
    }
    return MDBX_RESULT_TRUE /* Done: return with exclusive locking. */;
  }

  /* Firstly try to get exclusive locking.  */
  rc = lck_op(env->me_lfd, op_setlk, F_WRLCK, 0, 1);
  if (rc == MDBX_SUCCESS) {
  continue_dxb_exclusive:
    rc =
        lck_op(env->me_fd, op_setlk,
               (env->me_flags & MDBX_RDONLY) ? F_RDLCK : F_WRLCK, 0, OFF_T_MAX);
    if (rc == MDBX_SUCCESS)
      return MDBX_RESULT_TRUE /* Done: return with exclusive locking. */;

    /* the cause may be a collision with POSIX's file-lock recovery. */
    if (!(rc == EAGAIN || rc == EACCES || rc == EBUSY || rc == EWOULDBLOCK ||
          rc == EDEADLK)) {
      mdbx_error("%s(%s) failed: errcode %u", __func__, "dxb-exclusive", rc);
      mdbx_assert(env, MDBX_IS_ERROR(rc));
      return rc;
    }

    /* Fallback to lck-shared */
    rc = lck_op(env->me_lfd, op_setlk, F_RDLCK, 0, 1);
    if (rc != MDBX_SUCCESS) {
      mdbx_error("%s(%s) failed: errcode %u", __func__, "fallback-shared", rc);
      mdbx_assert(env, MDBX_IS_ERROR(rc));
      return rc;
    }
    /* Done: return with shared locking. */
    return MDBX_RESULT_FALSE;
  }

  /* Wait for lck-shared now. */
  /* Here may be await during transient processes, for instance until another
   * competing process doesn't call lck_downgrade(). */
  rc = lck_op(env->me_lfd, op_setlkw, F_RDLCK, 0, 1);
  if (rc != MDBX_SUCCESS) {
    mdbx_error("%s(%s) failed: errcode %u", __func__, "try-shared", rc);
    mdbx_assert(env, MDBX_IS_ERROR(rc));
    return rc;
  }

  /* Lock against another process operating in without-lck or exclusive mode. */
  rc =
      lck_op(env->me_fd, op_setlk,
             (env->me_flags & MDBX_RDONLY) ? F_RDLCK : F_WRLCK, env->me_pid, 1);
  if (rc != MDBX_SUCCESS) {
    mdbx_error("%s(%s) failed: errcode %u", __func__,
               "lock-against-without-lck", rc);
    mdbx_assert(env, MDBX_IS_ERROR(rc));
    return rc;
  }

  /* got shared, retry exclusive */
  rc = lck_op(env->me_lfd, op_setlk, F_WRLCK, 0, 1);
  if (rc == MDBX_SUCCESS)
    goto continue_dxb_exclusive;

  if (rc == EAGAIN || rc == EACCES || rc == EBUSY || rc == EWOULDBLOCK ||
      rc == EDEADLK)
    return MDBX_RESULT_FALSE /* Done: exclusive is unavailable,
                                but shared locks are alive. */
        ;

  mdbx_error("%s(%s) failed: errcode %u", __func__, "try-exclusive", rc);
  mdbx_assert(env, MDBX_IS_ERROR(rc));
  return rc;
}

MDBX_INTERNAL_FUNC int mdbx_lck_downgrade(MDBX_env *env) {
  assert(env->me_lfd != INVALID_HANDLE_VALUE);
  if (unlikely(mdbx_getpid() != env->me_pid))
    return MDBX_PANIC;

  int rc = MDBX_SUCCESS;
  if ((env->me_flags & MDBX_EXCLUSIVE) == 0) {
    rc = lck_op(env->me_fd, op_setlk, F_UNLCK, 0, env->me_pid);
    if (rc == MDBX_SUCCESS)
      rc = lck_op(env->me_fd, op_setlk, F_UNLCK, env->me_pid + 1,
                  OFF_T_MAX - env->me_pid - 1);
  }
  if (rc == MDBX_SUCCESS)
    rc = lck_op(env->me_lfd, op_setlk, F_RDLCK, 0, 1);
  if (unlikely(rc != 0)) {
    mdbx_error("%s(%s) failed: errcode %u", __func__, "lck", rc);
    assert(MDBX_IS_ERROR(rc));
  }
  return rc;
}

MDBX_INTERNAL_FUNC int __cold mdbx_lck_destroy(MDBX_env *env,
                                               MDBX_env *inprocess_neighbor) {
  if (unlikely(mdbx_getpid() != env->me_pid))
    return MDBX_PANIC;

  int rc = MDBX_SUCCESS;
  if (env->me_lfd != INVALID_HANDLE_VALUE && !inprocess_neighbor &&
      env->me_lck &&
      /* try get exclusive access */
      lck_op(env->me_lfd, op_setlk, F_WRLCK, 0, OFF_T_MAX) == 0 &&
      lck_op(env->me_fd, op_setlk,
             (env->me_flags & MDBX_RDONLY) ? F_RDLCK : F_WRLCK, 0, OFF_T_MAX)) {
    mdbx_verbose("%s: got exclusive, drown mutexes", __func__);
    rc = pthread_mutex_destroy(&env->me_lck->mti_rmutex);
    if (rc == 0)
      rc = pthread_mutex_destroy(&env->me_lck->mti_wmutex);
    mdbx_assert(env, rc == 0);
    if (rc == 0) {
      memset(env->me_lck, 0x81, sizeof(MDBX_lockinfo));
      msync(env->me_lck, env->me_os_psize, MS_ASYNC);
    }
    mdbx_jitter4testing(false);
  }

  /* 1) POSIX's fcntl() locks (i.e. when op_setlk == F_SETLK) should be restored
   * after file was closed.
   *
   * 2) File locks would be released (by kernel) while the file-descriptors will
   * be closed. But to avoid false-positive EACCESS and EDEADLK from the kernel,
   * locks should be released here explicitly with properly order. */

  /* close dxb and restore lock */
  if (env->me_fd != INVALID_HANDLE_VALUE) {
    if (unlikely(close(env->me_fd) != 0) && rc == MDBX_SUCCESS)
      rc = errno;
    env->me_fd = INVALID_HANDLE_VALUE;
    if (op_setlk == F_SETLK && inprocess_neighbor && rc == MDBX_SUCCESS) {
      /* restore file-lock */
      rc = lck_op(
          inprocess_neighbor->me_fd, F_SETLKW,
          (inprocess_neighbor->me_flags & MDBX_RDONLY) ? F_RDLCK : F_WRLCK,
          (inprocess_neighbor->me_flags & MDBX_EXCLUSIVE)
              ? 0
              : inprocess_neighbor->me_pid,
          (inprocess_neighbor->me_flags & MDBX_EXCLUSIVE) ? OFF_T_MAX : 1);
    }
  }

  /* close clk and restore locks */
  if (env->me_lfd != INVALID_HANDLE_VALUE) {
    if (unlikely(close(env->me_lfd) != 0) && rc == MDBX_SUCCESS)
      rc = errno;
    env->me_lfd = INVALID_HANDLE_VALUE;
    if (op_setlk == F_SETLK && inprocess_neighbor && rc == MDBX_SUCCESS) {
      /* restore file-locks */
      rc = lck_op(inprocess_neighbor->me_lfd, F_SETLKW, F_RDLCK, 0, 1);
      if (rc == MDBX_SUCCESS && inprocess_neighbor->me_live_reader)
        rc = mdbx_rpid_set(inprocess_neighbor);
    }
  }

  if (inprocess_neighbor && rc != MDBX_SUCCESS)
    inprocess_neighbor->me_flags |= MDBX_FATAL_ERROR;
  return rc;
}

/*---------------------------------------------------------------------------*/

static int mdbx_mutex_failed(MDBX_env *env, pthread_mutex_t *mutex,
                             const int rc);

MDBX_INTERNAL_FUNC int __cold mdbx_lck_init(MDBX_env *env,
                                            MDBX_env *inprocess_neighbor,
                                            int global_uniqueness_flag) {
  if (inprocess_neighbor)
    return MDBX_SUCCESS /* currently don't need any initialization
      if LCK already opened/used inside current process */
        ;

    /* FIXME: Unfortunately, there is no other reliable way but to long testing
     * on each platform. On the other hand, behavior like FreeBSD is incorrect
     * and we can expect it to be rare. Moreover, even on FreeBSD without
     * additional in-process initialization, the probability of an problem
     * occurring is vanishingly small, and the symptom is a return of EINVAL
     * while locking a mutex. In other words, in the worst case, the problem
     * results in an EINVAL error at the start of the transaction, but NOT data
     * loss, nor database corruption, nor other fatal troubles. Thus, the code
     * below I am inclined to think the workaround for erroneous platforms (like
     * FreeBSD), rather than a defect of libmdbx. */
#if defined(__FreeBSD__)
  /* seems that shared mutexes on FreeBSD required in-process initialization */
  (void)global_uniqueness_flag;
#else
  /* shared mutexes on many other platforms (including Darwin and Linux's
   * futexes) doesn't need any addition in-process initialization */
  if (global_uniqueness_flag != MDBX_RESULT_TRUE)
    return MDBX_SUCCESS;
#endif

  pthread_mutexattr_t ma;
  int rc = pthread_mutexattr_init(&ma);
  if (rc)
    return rc;

  rc = pthread_mutexattr_setpshared(&ma, PTHREAD_PROCESS_SHARED);
  if (rc)
    goto bailout;

#if MDBX_USE_ROBUST
#if defined(__GLIBC__) && !__GLIBC_PREREQ(2, 12) &&                            \
    !defined(pthread_mutex_consistent) && _POSIX_C_SOURCE < 200809L
  rc = pthread_mutexattr_setrobust_np(&ma, PTHREAD_MUTEX_ROBUST_NP);
#else
  rc = pthread_mutexattr_setrobust(&ma, PTHREAD_MUTEX_ROBUST);
#endif
  if (rc)
    goto bailout;
#endif /* MDBX_USE_ROBUST */

#if _POSIX_C_SOURCE >= 199506L && !defined(MDBX_SAFE4QEMU)
  rc = pthread_mutexattr_setprotocol(&ma, PTHREAD_PRIO_INHERIT);
  if (rc == ENOTSUP)
    rc = pthread_mutexattr_setprotocol(&ma, PTHREAD_PRIO_NONE);
  if (rc)
    goto bailout;
#endif /* PTHREAD_PRIO_INHERIT */

  rc = pthread_mutexattr_settype(&ma, PTHREAD_MUTEX_ERRORCHECK);
  if (rc)
    goto bailout;

  rc = pthread_mutex_init(&env->me_lck->mti_rmutex, &ma);
  if (rc)
    goto bailout;
  rc = pthread_mutex_init(&env->me_lck->mti_wmutex, &ma);

bailout:
  pthread_mutexattr_destroy(&ma);
  return rc;
}

static int mdbx_robust_lock(MDBX_env *env, pthread_mutex_t *mutex) {
  mdbx_jitter4testing(true);
  int rc = pthread_mutex_lock(mutex);
  if (unlikely(rc != 0))
    rc = mdbx_mutex_failed(env, mutex, rc);
  return rc;
}

static int mdbx_robust_trylock(MDBX_env *env, pthread_mutex_t *mutex) {
  mdbx_jitter4testing(true);
  int rc = pthread_mutex_trylock(mutex);
  if (unlikely(rc != 0 && rc != EBUSY))
    rc = mdbx_mutex_failed(env, mutex, rc);
  return (rc != EBUSY) ? rc : MDBX_BUSY;
}

static int mdbx_robust_unlock(MDBX_env *env, pthread_mutex_t *mutex) {
  int rc = pthread_mutex_unlock(mutex);
  mdbx_jitter4testing(true);
  if (unlikely(rc != 0))
    env->me_flags |= MDBX_FATAL_ERROR;
  return rc;
}

MDBX_INTERNAL_FUNC int mdbx_rdt_lock(MDBX_env *env) {
  mdbx_trace("%s", ">>");
  int rc = mdbx_robust_lock(env, &env->me_lck->mti_rmutex);
  mdbx_trace("<< rc %d", rc);
  return rc;
}

MDBX_INTERNAL_FUNC void mdbx_rdt_unlock(MDBX_env *env) {
  mdbx_trace("%s", ">>");
  int rc = mdbx_robust_unlock(env, &env->me_lck->mti_rmutex);
  mdbx_trace("<< rc %d", rc);
  if (unlikely(MDBX_IS_ERROR(rc)))
    mdbx_panic("%s() failed: errcode %d\n", __func__, rc);
}

int mdbx_txn_lock(MDBX_env *env, bool dontwait) {
  mdbx_trace("%s", ">>");
  int rc = dontwait ? mdbx_robust_trylock(env, env->me_wmutex)
                    : mdbx_robust_lock(env, env->me_wmutex);
  mdbx_trace("<< rc %d", rc);
  return MDBX_IS_ERROR(rc) ? rc : MDBX_SUCCESS;
}

void mdbx_txn_unlock(MDBX_env *env) {
  mdbx_trace("%s", ">>");
  int rc = mdbx_robust_unlock(env, env->me_wmutex);
  mdbx_trace("<< rc %d", rc);
  if (unlikely(MDBX_IS_ERROR(rc)))
    mdbx_panic("%s() failed: errcode %d\n", __func__, rc);
}

static int __cold mdbx_mutex_failed(MDBX_env *env, pthread_mutex_t *mutex,
                                    const int err) {
  int rc = err;
#if MDBX_USE_ROBUST
  if (err == EOWNERDEAD) {
    /* We own the mutex. Clean up after dead previous owner. */

    int rlocked = (env->me_lck && mutex == &env->me_lck->mti_rmutex);
    rc = MDBX_SUCCESS;
    if (!rlocked) {
      if (unlikely(env->me_txn)) {
        /* env is hosed if the dead thread was ours */
        env->me_flags |= MDBX_FATAL_ERROR;
        env->me_txn = NULL;
        rc = MDBX_PANIC;
      }
    }
    mdbx_notice("%cmutex owner died, %s", (rlocked ? 'r' : 'w'),
                (rc ? "this process' env is hosed" : "recovering"));

    int check_rc = mdbx_reader_check0(env, rlocked, NULL);
    check_rc = (check_rc == MDBX_SUCCESS) ? MDBX_RESULT_TRUE : check_rc;

#if defined(__GLIBC__) && !__GLIBC_PREREQ(2, 12) &&                            \
    !defined(pthread_mutex_consistent) && _POSIX_C_SOURCE < 200809L
    int mreco_rc = pthread_mutex_consistent_np(mutex);
#else
    int mreco_rc = pthread_mutex_consistent(mutex);
#endif
    check_rc = (mreco_rc == 0) ? check_rc : mreco_rc;

    if (unlikely(mreco_rc))
      mdbx_error("mutex recovery failed, %s", mdbx_strerror(mreco_rc));

    rc = (rc == MDBX_SUCCESS) ? check_rc : rc;
    if (MDBX_IS_ERROR(rc))
      pthread_mutex_unlock(mutex);
    return rc;
  }
#else
  (void)mutex;
#endif /* MDBX_USE_ROBUST */

  mdbx_error("mutex (un)lock failed, %s", mdbx_strerror(err));
  if (rc != EDEADLK)
    env->me_flags |= MDBX_FATAL_ERROR;
  return rc;
}
