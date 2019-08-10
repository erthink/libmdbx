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

#include "./bits.h"

/* Some platforms define the EOWNERDEAD error code
 * even though they don't support Robust Mutexes.
 * Compile with -DMDBX_USE_ROBUST=0. */
#ifndef MDBX_USE_ROBUST
#if defined(EOWNERDEAD) || _POSIX_C_SOURCE >= 200809L
#define MDBX_USE_ROBUST 1
#else
#define MDBX_USE_ROBUST 0
#endif
#endif /* MDBX_USE_ROBUST */

/*----------------------------------------------------------------------------*/
/* rthc */

static __cold __attribute__((constructor)) void mdbx_global_constructor(void) {
  mdbx_rthc_global_init();
}

static __cold __attribute__((destructor)) void mdbx_global_destructor(void) {
  mdbx_rthc_global_dtor();
}

/*----------------------------------------------------------------------------*/
/* lck */

/* Описание реализации блокировок для POSIX:
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
 * Для блокировки файлов Используется только fcntl(F_SETLK), так как:
 *  - lockf() оперирует только эксклюзивной блокировкой и требует
 *    открытия файла в RW-режиме.
 *  - flock() не гарантирует атомарности при смене блокировок
 *    и оперирует только всем файлом целиком.
 *  - Для контроля процессов-читателей используются однобайтовые
 *    range-блокировки lck-файла посредством fcntl(F_SETLK). При этом
 *    в качестве позиции используется pid процесса-читателя.
 *  - Для первоначального захвата и shared/exclusive выполняется блокировка
 *    основного файла БД и при успехе lck-файла.
 */

#ifndef OFF_T_MAX
#define OFF_T_MAX                                                              \
  ((sizeof(off_t) > 4 ? INT64_MAX : INT32_MAX) & ~(size_t)0xffff)
#endif
#ifndef PID_T_MAX
#define PID_T_MAX INT_MAX
#endif

#if defined(F_OFD_SETLK) && defined(F_OFD_SETLKW) && defined(F_OFD_GETLK)
#define OP_SETLK F_OFD_SETLK
#define OP_SETLKW F_OFD_SETLKW
#define OP_GETLK F_OFD_GETLK
#else
#define OP_SETLK F_SETLK
#define OP_SETLKW F_SETLKW
#define OP_GETLK F_GETLK
#endif /* OFD locks */

static int mdbx_lck_op(mdbx_filehandle_t fd, int cmd, short lck, off_t offset,
                       off_t len) {
  for (;;) {
    struct flock lock_op;
    memset(&lock_op, 0, sizeof(lock_op));
    lock_op.l_type = lck;
    lock_op.l_whence = SEEK_SET;
    lock_op.l_start = offset;
    lock_op.l_len = len;
    if (fcntl(fd, cmd, &lock_op) == 0) {
      if (cmd == OP_GETLK) {
        /* Checks reader by pid. Returns:
         *   MDBX_RESULT_TRUE   - if pid is live (unable to acquire lock)
         *   MDBX_RESULT_FALSE  - if pid is dead (lock acquired). */
        return (lock_op.l_type == F_UNLCK) ? MDBX_RESULT_FALSE
                                           : MDBX_RESULT_TRUE;
      }
      return 0;
    }
    int rc = errno;
    if (rc != EINTR)
      return rc;
  }
}

int mdbx_rpid_set(MDBX_env *env) {
  assert(env->me_lfd != INVALID_HANDLE_VALUE);
  assert(env->me_pid > 0 && env->me_pid <= PID_T_MAX);
  return mdbx_lck_op(env->me_lfd, OP_SETLK, F_WRLCK, env->me_pid, 1);
}

int mdbx_rpid_clear(MDBX_env *env) {
  assert(env->me_lfd != INVALID_HANDLE_VALUE);
  assert(env->me_pid > 0 && env->me_pid <= PID_T_MAX);
  return mdbx_lck_op(env->me_lfd, OP_SETLKW, F_UNLCK, env->me_pid, 1);
}

int mdbx_rpid_check(MDBX_env *env, mdbx_pid_t pid) {
  assert(env->me_lfd != INVALID_HANDLE_VALUE);
  assert(pid > 0 && pid <= PID_T_MAX);
  assert(PID_T_MAX < OFF_T_MAX);
  return mdbx_lck_op(env->me_lfd, OP_GETLK, F_WRLCK, pid, 1);
}

int __cold mdbx_lck_seize(MDBX_env *env) {
  assert(env->me_fd != INVALID_HANDLE_VALUE);
  assert(env->me_pid > 0 && env->me_pid <= PID_T_MAX);

  if (env->me_lfd == INVALID_HANDLE_VALUE) {
    /* LY: without-lck mode (e.g. exclusive or on read-only filesystem) */
    int rc = mdbx_lck_op(env->me_fd, OP_SETLK,
                         (env->me_flags & MDBX_RDONLY) ? F_RDLCK : F_WRLCK, 0,
                         OFF_T_MAX);
    if (rc != 0) {
      mdbx_error("%s(%s) failed: errcode %u", mdbx_func_, "without-lck", rc);
      return rc;
    }
    return MDBX_RESULT_TRUE;
  }

  /* try exclusive access */
  int rc = mdbx_lck_op(env->me_fd, OP_SETLK,
                       (env->me_flags & MDBX_RDONLY) ? F_RDLCK : F_WRLCK, 0,
                       OFF_T_MAX);
  if (rc == 0) {
    /* got dxb-exclusive, try lck-exclusive */
    rc = mdbx_lck_op(env->me_lfd, OP_SETLK, F_WRLCK, 0, OFF_T_MAX);
    if (rc == 0) {
      /* got both exclusive */
      return MDBX_RESULT_TRUE;
    }
    mdbx_error("%s(%s) failed: errcode %u", mdbx_func_,
               "lck-after-dxb-exclusive", rc);
    assert(MDBX_IS_ERROR(rc));
    goto bailout;
  }

  if (rc == EAGAIN || rc == EACCES || rc == EBUSY || rc == EWOULDBLOCK) {
    rc = mdbx_lck_op(env->me_fd, OP_SETLKW,
                     (env->me_flags & MDBX_RDONLY) ? F_RDLCK : F_WRLCK,
                     env->me_pid, 1);
    if (rc == 0) {
      /* got dxb-shared-rw */
      return MDBX_RESULT_FALSE;
    }
    mdbx_error("%s(%s) failed: errcode %u", mdbx_func_,
               "lock-against-without-lck", rc);
    assert(MDBX_IS_ERROR(rc));
  }

bailout:
  (void)mdbx_lck_op(env->me_lfd, OP_SETLK, F_UNLCK, 0, OFF_T_MAX);
  (void)mdbx_lck_op(env->me_fd, OP_SETLK, F_UNLCK, 0, OFF_T_MAX);
  assert(MDBX_IS_ERROR(rc));
  return rc;
}

int mdbx_lck_downgrade(MDBX_env *env, bool complete) {
  assert(env->me_lfd != INVALID_HANDLE_VALUE);
  int rc = mdbx_lck_op(env->me_lfd, OP_SETLK, F_UNLCK, 0, OFF_T_MAX);
  if (unlikely(rc != 0)) {
    mdbx_error("%s(%s) failed: errcode %u", mdbx_func_, "lck", rc);
    goto bailout;
  }
  if (complete) {
    rc = mdbx_lck_op(env->me_fd, OP_SETLK,
                     (env->me_flags & MDBX_RDONLY) ? F_RDLCK : F_WRLCK,
                     env->me_pid, 1);
    if (unlikely(rc != 0)) {
      mdbx_error("%s(%s) failed: errcode %u", mdbx_func_, "dxb", rc);
      goto bailout;
    }
  }
  return MDBX_SUCCESS;

bailout:
  (void)mdbx_lck_op(env->me_lfd, OP_SETLK, F_UNLCK, 0, OFF_T_MAX);
  (void)mdbx_lck_op(env->me_fd, OP_SETLK, F_UNLCK, 0, OFF_T_MAX);
  assert(MDBX_IS_ERROR(rc));
  return rc;
}

/*---------------------------------------------------------------------------*/

static int mdbx_mutex_failed(MDBX_env *env, pthread_mutex_t *mutex,
                             const int rc);

int __cold mdbx_lck_init(MDBX_env *env) {
  pthread_mutexattr_t ma;
  int rc = pthread_mutexattr_init(&ma);
  if (rc)
    return rc;

  rc = pthread_mutexattr_setpshared(&ma, PTHREAD_PROCESS_SHARED);
  if (rc)
    goto bailout;

#if MDBX_USE_ROBUST
  rc = pthread_mutexattr_setrobust(&ma, PTHREAD_MUTEX_ROBUST);
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

void __cold mdbx_lck_destroy(MDBX_env *env) {
  if (env->me_lfd != INVALID_HANDLE_VALUE && env->me_lck) {
    /* try get exclusive access */
    if (mdbx_lck_op(env->me_fd, OP_SETLK,
                    (env->me_flags & MDBX_RDONLY) ? F_RDLCK : F_WRLCK, 0,
                    OFF_T_MAX) == 0 &&
        mdbx_lck_op(env->me_lfd, OP_SETLK, F_WRLCK, 0, OFF_T_MAX) == 0) {
      mdbx_info("%s: got exclusive, drown mutexes", mdbx_func_);
      int rc = pthread_mutex_destroy(&env->me_lck->mti_rmutex);
      if (rc == 0)
        rc = pthread_mutex_destroy(&env->me_lck->mti_wmutex);
      assert(rc == 0);
      (void)rc;
      /* file locks would be released (by kernel)
       * while the me_lfd will be closed */
    }
  }
}

static int mdbx_robust_lock(MDBX_env *env, pthread_mutex_t *mutex) {
  int rc = pthread_mutex_lock(mutex);
  if (unlikely(rc != 0))
    rc = mdbx_mutex_failed(env, mutex, rc);
  return rc;
}

static int mdbx_robust_trylock(MDBX_env *env, pthread_mutex_t *mutex) {
  int rc = pthread_mutex_trylock(mutex);
  if (unlikely(rc != 0 && rc != EBUSY))
    rc = mdbx_mutex_failed(env, mutex, rc);
  return (rc != EBUSY) ? rc : MDBX_BUSY;
}

static int mdbx_robust_unlock(MDBX_env *env, pthread_mutex_t *mutex) {
  int rc = pthread_mutex_unlock(mutex);
  if (unlikely(rc != 0))
    rc = mdbx_mutex_failed(env, mutex, rc);
  return rc;
}

int mdbx_rdt_lock(MDBX_env *env) {
  mdbx_trace(">>");
  int rc = mdbx_robust_lock(env, &env->me_lck->mti_rmutex);
  mdbx_trace("<< rc %d", rc);
  return rc;
}

void mdbx_rdt_unlock(MDBX_env *env) {
  mdbx_trace(">>");
  int rc = mdbx_robust_unlock(env, &env->me_lck->mti_rmutex);
  mdbx_trace("<< rc %d", rc);
  if (unlikely(MDBX_IS_ERROR(rc)))
    mdbx_panic("%s() failed: errcode %d\n", mdbx_func_, rc);
}

int mdbx_txn_lock(MDBX_env *env, bool dontwait) {
  mdbx_trace(">>");
  int rc = dontwait ? mdbx_robust_trylock(env, env->me_wmutex)
                    : mdbx_robust_lock(env, env->me_wmutex);
  mdbx_trace("<< rc %d", rc);
  return MDBX_IS_ERROR(rc) ? rc : MDBX_SUCCESS;
}

void mdbx_txn_unlock(MDBX_env *env) {
  mdbx_trace(">>");
  int rc = mdbx_robust_unlock(env, env->me_wmutex);
  mdbx_trace("<< rc %d", rc);
  if (unlikely(MDBX_IS_ERROR(rc)))
    mdbx_panic("%s() failed: errcode %d\n", mdbx_func_, rc);
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

    int mreco_rc = pthread_mutex_consistent(mutex);
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
