/*
 * Copyright 2015-2018 Leonid Yuriev <leo@yuriev.ru>
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
/* Howard Chu: Android currently lacks Robust Mutex support */
#if defined(EOWNERDEAD) &&                                                     \
    !defined(ANDROID) /* LY: glibc before 2.10 has a troubles with Robust      \
                         Mutex too. */                                         \
    && __GLIBC_PREREQ(2, 10)
#define MDBX_USE_ROBUST 1
#else
#define MDBX_USE_ROBUST 0
#endif
#endif /* MDBX_USE_ROBUST */

/*----------------------------------------------------------------------------*/
/* rthc */

static pthread_mutex_t mdbx_rthc_mutex = PTHREAD_MUTEX_INITIALIZER;

void mdbx_rthc_lock(void) {
  mdbx_ensure(NULL, pthread_mutex_lock(&mdbx_rthc_mutex) == 0);
}

void mdbx_rthc_unlock(void) {
  mdbx_ensure(NULL, pthread_mutex_unlock(&mdbx_rthc_mutex) == 0);
}

void __attribute__((destructor)) mdbx_global_destructor(void) {
  mdbx_rthc_cleanup();
}

/*----------------------------------------------------------------------------*/
/* lck */

#ifndef OFF_T_MAX
#define OFF_T_MAX (sizeof(off_t) > 4 ? INT64_MAX : INT32_MAX)
#endif
#define LCK_WHOLE OFF_T_MAX

static int mdbx_lck_op(mdbx_filehandle_t fd, int op, int lck, off_t offset,
                       off_t len) {
  for (;;) {
    int rc;
    struct flock lock_op;
    memset(&lock_op, 0, sizeof(lock_op));
    lock_op.l_type = lck;
    lock_op.l_whence = SEEK_SET;
    lock_op.l_start = offset;
    lock_op.l_len = len;
    if ((rc = fcntl(fd, op, &lock_op)) == 0) {
      if (op == F_GETLK && lock_op.l_type != F_UNLCK)
        rc = -lock_op.l_pid;
    } else if ((rc = errno) == EINTR) {
      continue;
    }
    return rc;
  }
}

static __inline int mdbx_lck_exclusive(int lfd) {
  assert(lfd != INVALID_HANDLE_VALUE);
  if (flock(lfd, LOCK_EX | LOCK_NB))
    return errno;
  return mdbx_lck_op(lfd, F_SETLK, F_WRLCK, 0, 1);
}

static __inline int mdbx_lck_shared(int lfd) {
  assert(lfd != INVALID_HANDLE_VALUE);
  while (flock(lfd, LOCK_SH)) {
    int rc = errno;
    if (rc != EINTR)
      return rc;
  }
  return mdbx_lck_op(lfd, F_SETLKW, F_RDLCK, 0, 1);
}

int mdbx_lck_downgrade(MDBX_env *env, bool complete) {
  return complete ? mdbx_lck_shared(env->me_lfd) : MDBX_SUCCESS;
}

int mdbx_lck_upgrade(MDBX_env *env) { return mdbx_lck_exclusive(env->me_lfd); }

int mdbx_rpid_set(MDBX_env *env) {
  return mdbx_lck_op(env->me_lfd, F_SETLK, F_WRLCK, env->me_pid, 1);
}

int mdbx_rpid_clear(MDBX_env *env) {
  return mdbx_lck_op(env->me_lfd, F_SETLKW, F_UNLCK, env->me_pid, 1);
}

/* Checks reader by pid.
 *
 * Returns:
 *   MDBX_RESULT_TRUE, if pid is live (unable to acquire lock)
 *   MDBX_RESULT_FALSE, if pid is dead (lock acquired)
 *   or otherwise the errcode. */
int mdbx_rpid_check(MDBX_env *env, mdbx_pid_t pid) {
  int rc = mdbx_lck_op(env->me_lfd, F_GETLK, F_WRLCK, pid, 1);
  if (rc == 0)
    return MDBX_RESULT_FALSE;
  if (rc < 0 && -rc == pid)
    return MDBX_RESULT_TRUE;
  return rc;
}

/*---------------------------------------------------------------------------*/

static int mdbx_mutex_failed(MDBX_env *env, pthread_mutex_t *mutex, int rc);

int mdbx_lck_init(MDBX_env *env) {
  pthread_mutexattr_t ma;
  int rc = pthread_mutexattr_init(&ma);
  if (rc)
    return rc;

  rc = pthread_mutexattr_setpshared(&ma, PTHREAD_PROCESS_SHARED);
  if (rc)
    goto bailout;

#if MDBX_USE_ROBUST
#if __GLIBC_PREREQ(2, 12)
  rc = pthread_mutexattr_setrobust(&ma, PTHREAD_MUTEX_ROBUST);
#else
  rc = pthread_mutexattr_setrobust_np(&ma, PTHREAD_MUTEX_ROBUST_NP);
#endif
  if (rc)
    goto bailout;
#endif /* MDBX_USE_ROBUST */

#if _POSIX_C_SOURCE >= 199506L
  rc = pthread_mutexattr_setprotocol(&ma, PTHREAD_PRIO_INHERIT);
  if (rc == ENOTSUP)
    rc = pthread_mutexattr_setprotocol(&ma, PTHREAD_PRIO_NONE);
  if (rc)
    goto bailout;
#endif /* PTHREAD_PRIO_INHERIT */

  rc = pthread_mutex_init(&env->me_lck->mti_rmutex, &ma);
  if (rc)
    goto bailout;
  rc = pthread_mutex_init(&env->me_lck->mti_wmutex, &ma);

bailout:
  pthread_mutexattr_destroy(&ma);
  return rc;
}

void mdbx_lck_destroy(MDBX_env *env) {
  if (env->me_lfd != INVALID_HANDLE_VALUE) {
    /* try get exclusive access */
    if (env->me_lck && mdbx_lck_exclusive(env->me_lfd) == 0) {
      mdbx_info("%s: got exclusive, drown mutexes", mdbx_func_);
      int rc = pthread_mutex_destroy(&env->me_lck->mti_rmutex);
      if (rc == 0)
        rc = pthread_mutex_destroy(&env->me_lck->mti_wmutex);
      assert(rc == 0);
      (void)rc;
      /* lock would be released (by kernel) while the me_lfd will be closed */
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
  int rc = dontwait ? mdbx_robust_trylock(env, &env->me_lck->mti_wmutex)
                    : mdbx_robust_lock(env, &env->me_lck->mti_wmutex);
  mdbx_trace("<< rc %d", rc);
  return MDBX_IS_ERROR(rc) ? rc : MDBX_SUCCESS;
}

void mdbx_txn_unlock(MDBX_env *env) {
  mdbx_trace(">>");
  int rc = mdbx_robust_unlock(env, &env->me_lck->mti_wmutex);
  mdbx_trace("<< rc %d", rc);
  if (unlikely(MDBX_IS_ERROR(rc)))
    mdbx_panic("%s() failed: errcode %d\n", mdbx_func_, rc);
}

static int internal_seize_lck(int lfd) {
  assert(lfd != INVALID_HANDLE_VALUE);

  /* try exclusive access */
  int rc = mdbx_lck_exclusive(lfd);
  if (rc == 0)
    /* got exclusive */
    return MDBX_RESULT_TRUE;
  if (rc == EAGAIN || rc == EACCES || rc == EBUSY || rc == EWOULDBLOCK) {
    /* get shared access */
    rc = mdbx_lck_shared(lfd);
    if (rc == 0) {
      /* got shared, try exclusive again */
      rc = mdbx_lck_exclusive(lfd);
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

int mdbx_lck_seize(MDBX_env *env) {
  assert(env->me_fd != INVALID_HANDLE_VALUE);

  if (env->me_lfd == INVALID_HANDLE_VALUE) {
    /* LY: without-lck mode (e.g. on read-only filesystem) */
    int rc = mdbx_lck_op(env->me_fd, F_SETLK, F_RDLCK, 0, LCK_WHOLE);
    if (rc != 0) {
      mdbx_error("%s(%s) failed: errcode %u", mdbx_func_, "without-lck", rc);
      return rc;
    }
    return MDBX_RESULT_FALSE;
  }

  if ((env->me_flags & MDBX_RDONLY) == 0) {
    /* Check that another process don't operates in without-lck mode. */
    int rc = mdbx_lck_op(env->me_fd, F_SETLK, F_WRLCK, env->me_pid, 1);
    if (rc != 0) {
      mdbx_error("%s(%s) failed: errcode %u", mdbx_func_,
                 "lock-against-without-lck", rc);
      return rc;
    }
  }

  return internal_seize_lck(env->me_lfd);
}

#if !__GLIBC_PREREQ(2, 12) && !defined(pthread_mutex_consistent)
#define pthread_mutex_consistent(mutex) pthread_mutex_consistent_np(mutex)
#endif

static int __cold mdbx_mutex_failed(MDBX_env *env, pthread_mutex_t *mutex,
                                    int rc) {
#if MDBX_USE_ROBUST
  if (rc == EOWNERDEAD) {
    /* We own the mutex. Clean up after dead previous owner. */

    int rlocked = (mutex == &env->me_lck->mti_rmutex);
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
#endif /* MDBX_USE_ROBUST */

  mdbx_error("mutex (un)lock failed, %s", mdbx_strerror(rc));
  if (rc != EDEADLK) {
    env->me_flags |= MDBX_FATAL_ERROR;
    rc = MDBX_PANIC;
  }
  return rc;
}
