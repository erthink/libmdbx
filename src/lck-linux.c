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

#if !(defined(__linux__) || defined(__gnu_linux__))
#error "This implementation of locking only supports Linux,\
 where is no interaction between the types of lock placed\
 by flock() and fcntl()."
#endif

#include "./bits.h"
#include <sys/utsname.h>

/* Some platforms define the EOWNERDEAD error code
 * even though they don't support Robust Mutexes.
 * Compile with -DMDBX_USE_ROBUST=0. */
#ifndef MDBX_USE_ROBUST
/* Howard Chu: Android currently lacks Robust Mutex support */
#if defined(EOWNERDEAD) &&                                                     \
    !defined(__ANDROID__) /* LY: glibc before 2.10 has a troubles              \
                                 with Robust Mutex too. */                     \
    && (!defined(__GLIBC__) || __GLIBC_PREREQ(2, 10) ||                        \
        _POSIX_C_SOURCE >= 200809L)
#define MDBX_USE_ROBUST 1
#else
#define MDBX_USE_ROBUST 0
#endif
#endif /* MDBX_USE_ROBUST */

uint32_t linux_kernel_version;
static int cmd_setlk = F_SETLK, cmd_setlkw = F_SETLKW, cmd_getlk = F_GETLK;

/*----------------------------------------------------------------------------*/
/* rthc */

static __cold __attribute__((constructor)) void mdbx_global_constructor(void) {
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
          linux_kernel_version += number << (24 - i * 8);
        }
        ++i;
      } else {
        ++p;
      }
    }
  }

#if defined(F_OFD_SETLK) && defined(F_OFD_SETLKW) && defined(F_OFD_GETLK)
  if (linux_kernel_version >
      0x030f0000 /* OFD locks are available since 3.15, but engages here only
        for 3.16 and larer kernels (LTS) for reliability reasons */) {
    cmd_setlk = F_OFD_SETLK;
    cmd_setlkw = F_OFD_SETLKW;
    cmd_getlk = F_OFD_GETLK;
  }
#endif /* OFD locks */
  mdbx_rthc_global_init();
}

static __cold __attribute__((destructor)) void mdbx_global_destructor(void) {
  mdbx_rthc_global_dtor();
}

/*----------------------------------------------------------------------------*/
/* lck */

/* Описание реализации блокировок для Linux:
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
 * Используется два вида файловых блокировок flock() и fcntl(F_SETLK),
 * как для lck-файла, так и для основного файла БД:
 *  - Для контроля процессов-читателей используются однобайтовые
 *    range-блокировки lck-файла посредством fcntl(F_SETLK). При этом
 *    в качестве позиции используется pid процесса-читателя.
 *  - Для первоначального захвата и shared/exlcusive блокировок используется
 *    комбинация flock() и fcntl(F_SETLK) блокировки одного байта lck-файла
 *    в нулевой позиции (нулевая позиция не используется механизмом контроля
 *    процессов-читателей, так как pid пользовательского процесса в Linux
 *    всегда больше 0).
 *  - Кроме этого, flock() блокировка основного файла БД используется при работе
 *    в режимах без lck-файла, как в в read-only, так и в эксклюзивном.
 *  - Блокировки flock() и fcntl(F_SETLK) в Linux работают независимо. Поэтому
 *    их комбинирование позволяет предотвратить совместное использование БД
 *    через NFS, что позволяет fcntl(F_SETLK), одновременно защитившись
 *    от проблем не-аторманости flock() при переходе между эксклюзивным
 *    и атомарным режимами блокировок.
 */

#ifndef OFF_T_MAX
#define OFF_T_MAX                                                              \
  ((sizeof(off_t) > 4 ? INT64_MAX : INT32_MAX) & ~(size_t)0xffff)
#endif
#define LCK_WHOLE OFF_T_MAX

static int mdbx_lck_op(mdbx_filehandle_t fd, int op, short lck, off_t offset,
                       off_t len) {
  for (;;) {
    struct flock lock_op;
    memset(&lock_op, 0, sizeof(lock_op));
    lock_op.l_type = lck;
    lock_op.l_whence = SEEK_SET;
    lock_op.l_start = offset;
    lock_op.l_len = len;
    if (fcntl(fd, op, &lock_op) == 0) {
      if (op == cmd_getlk) {
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

static __inline int mdbx_lck_exclusive(int lfd, bool fallback2shared) {
  assert(lfd != INVALID_HANDLE_VALUE);
  if (flock(lfd, LOCK_EX | LOCK_NB))
    return errno;
  int rc = mdbx_lck_op(lfd, cmd_setlk, F_WRLCK, 0, 1);
  if (rc != 0 && fallback2shared) {
    while (flock(lfd, LOCK_SH)) {
      int rc = errno;
      if (rc != EINTR)
        return rc;
    }
  }
  return rc;
}

static __inline int mdbx_lck_shared(int lfd) {
  assert(lfd != INVALID_HANDLE_VALUE);
  while (flock(lfd, LOCK_SH)) {
    int rc = errno;
    if (rc != EINTR)
      return rc;
  }
  return mdbx_lck_op(lfd, cmd_setlkw, F_RDLCK, 0, 1);
}

int mdbx_lck_downgrade(MDBX_env *env, bool complete) {
  assert(env->me_lfd != INVALID_HANDLE_VALUE);
  return complete ? mdbx_lck_shared(env->me_lfd) : MDBX_SUCCESS;
}

int mdbx_rpid_set(MDBX_env *env) {
  assert(env->me_lfd != INVALID_HANDLE_VALUE);
  return mdbx_lck_op(env->me_lfd, cmd_setlk, F_WRLCK, env->me_pid, 1);
}

int mdbx_rpid_clear(MDBX_env *env) {
  assert(env->me_lfd != INVALID_HANDLE_VALUE);
  return mdbx_lck_op(env->me_lfd, cmd_setlkw, F_UNLCK, env->me_pid, 1);
}

int mdbx_rpid_check(MDBX_env *env, mdbx_pid_t pid) {
  assert(env->me_lfd != INVALID_HANDLE_VALUE);
  return mdbx_lck_op(env->me_lfd, cmd_getlk, F_WRLCK, pid, 1);
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

void __cold mdbx_lck_destroy(MDBX_env *env) {
  if (env->me_lfd != INVALID_HANDLE_VALUE) {
    /* try get exclusive access */
    if (env->me_lck && mdbx_lck_exclusive(env->me_lfd, false) == 0) {
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

static int __cold internal_seize_lck(int lfd) {
  assert(lfd != INVALID_HANDLE_VALUE);

  /* try exclusive access */
  int rc = mdbx_lck_exclusive(lfd, false);
  if (rc == 0)
    /* got exclusive */
    return MDBX_RESULT_TRUE;
  if (rc == EAGAIN || rc == EACCES || rc == EBUSY || rc == EWOULDBLOCK) {
    /* get shared access */
    rc = mdbx_lck_shared(lfd);
    if (rc == 0) {
      /* got shared, try exclusive again */
      rc = mdbx_lck_exclusive(lfd, true);
      if (rc == 0)
        /* now got exclusive */
        return MDBX_RESULT_TRUE;
      if (rc == EAGAIN || rc == EACCES || rc == EBUSY || rc == EWOULDBLOCK)
        /* unable exclusive, but stay shared */
        return MDBX_RESULT_FALSE;
    }
  }
  assert(MDBX_IS_ERROR(rc));
  return rc;
}

int __cold mdbx_lck_seize(MDBX_env *env) {
  assert(env->me_fd != INVALID_HANDLE_VALUE);

  if (env->me_lfd == INVALID_HANDLE_VALUE) {
    /* LY: without-lck mode (e.g. exclusive or on read-only filesystem) */
    int rc = mdbx_lck_op(env->me_fd, cmd_setlk,
                         (env->me_flags & MDBX_RDONLY) ? F_RDLCK : F_WRLCK, 0,
                         LCK_WHOLE);
    if (rc != 0) {
      mdbx_error("%s(%s) failed: errcode %u", mdbx_func_, "without-lck", rc);
      return rc;
    }
    return MDBX_RESULT_TRUE;
  }

  if ((env->me_flags & MDBX_RDONLY) == 0) {
    /* Check that another process don't operates in without-lck mode. */
    int rc = mdbx_lck_op(env->me_fd, cmd_setlk, F_WRLCK, env->me_pid, 1);
    if (rc != 0) {
      mdbx_error("%s(%s) failed: errcode %u", mdbx_func_,
                 "lock-against-without-lck", rc);
      return rc;
    }
  }

  return internal_seize_lck(env->me_lfd);
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
