/*
 * Copyright 2015-2017 Leonid Yuriev <leo@yuriev.ru>
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
 * Compile with -DMDB_USE_ROBUST=0. */
#ifndef MDB_USE_ROBUST
/* Howard Chu: Android currently lacks Robust Mutex support */
#if defined(EOWNERDEAD) &&                                                     \
    !defined(ANDROID) /* LY: glibc before 2.10 has a troubles with Robust      \
                         Mutex too. */                                         \
    && __GLIBC_PREREQ(2, 10)
#define MDB_USE_ROBUST 1
#else
#define MDB_USE_ROBUST 0
#endif
#endif /* MDB_USE_ROBUST */

/*----------------------------------------------------------------------------*/
/* rthc */

static mdbx_mutex_t mdbx_rthc_mutex = PTHREAD_MUTEX_INITIALIZER;

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

static int mdbx_lck_op(mdbx_filehandle_t fd, int op, int lck, off_t offset);
static int mdbx_mutex_failed(MDB_env *env, pthread_mutex_t *mutex, int rc);

int mdbx_lck_init(MDB_env *env) {
  pthread_mutexattr_t ma;
  int rc = pthread_mutexattr_init(&ma);
  if (rc)
    return rc;

  rc = pthread_mutexattr_setpshared(&ma, PTHREAD_PROCESS_SHARED);
  if (rc)
    goto bailout;

#if MDB_USE_ROBUST
#if __GLIBC_PREREQ(2, 12)
  rc = pthread_mutexattr_setrobust(&ma, PTHREAD_MUTEX_ROBUST);
#else
  rc = pthread_mutexattr_setrobust_np(&ma, PTHREAD_MUTEX_ROBUST_NP);
#endif
  if (rc)
    goto bailout;
#endif /* MDB_USE_ROBUST */

#if _POSIX_C_SOURCE >= 199506L
  rc = pthread_mutexattr_setprotocol(&ma, PTHREAD_PRIO_INHERIT);
  if (rc == ENOTSUP)
    rc = pthread_mutexattr_setprotocol(&ma, PTHREAD_PRIO_NONE);
  if (rc)
    goto bailout;
#endif /* PTHREAD_PRIO_INHERIT */

  rc = pthread_mutex_init(&env->me_txns->mti_rmutex, &ma);
  if (rc)
    goto bailout;
  rc = pthread_mutex_init(&env->me_txns->mti_wmutex, &ma);

bailout:
  pthread_mutexattr_destroy(&ma);
  return rc;
}

void mdbx_lck_destroy(MDB_env *env) {
  if (env->me_lfd != INVALID_HANDLE_VALUE) {
    /* try get exclusive access */
    if (env->me_txns && mdbx_lck_op(env->me_lfd, F_SETLK, F_WRLCK, 0) == 0) {
      /* got exclusive, drown mutexes */
      int rc = pthread_mutex_destroy(&env->me_txns->mti_rmutex);
      if (rc == 0)
        rc = pthread_mutex_destroy(&env->me_txns->mti_wmutex);
      assert(rc == 0);
      (void)rc;
      /* lock would be released (by kernel) while the me_lfd will be closed */
    }
  }
}

static int mdbx_robust_lock(MDB_env *env, pthread_mutex_t *mutex) {
  int rc = pthread_mutex_lock(mutex);
  if (unlikely(rc != 0))
    rc = mdbx_mutex_failed(env, mutex, rc);
  return rc;
}

static int mdbx_robust_unlock(MDB_env *env, pthread_mutex_t *mutex) {
  int rc = pthread_mutex_unlock(mutex);
  if (unlikely(rc != 0))
    rc = mdbx_mutex_failed(env, mutex, rc);
  return rc;
}

int mdbx_rdt_lock(MDB_env *env) {
  return mdbx_robust_lock(env, &env->me_txns->mti_rmutex);
}

void mdbx_rdt_unlock(MDB_env *env) {
  int rc = mdbx_robust_unlock(env, &env->me_txns->mti_rmutex);
  if (unlikely(MDBX_IS_ERROR(rc)))
    mdbx_panic("%s() failed: errcode %d\n", mdbx_func_, rc);
}

int mdbx_txn_lock(MDB_env *env) {
  int rc = mdbx_robust_lock(env, &env->me_txns->mti_wmutex);
  return MDBX_IS_ERROR(rc) ? rc : MDB_SUCCESS;
}

void mdbx_txn_unlock(MDB_env *env) {
  int rc = mdbx_robust_unlock(env, &env->me_txns->mti_wmutex);
  if (unlikely(MDBX_IS_ERROR(rc)))
    mdbx_panic("%s() failed: errcode %d\n", mdbx_func_, rc);
}

int mdbx_lck_seize(MDB_env *env) {
  /* try exclusive access */
  int rc = mdbx_lck_op(env->me_lfd, F_SETLK, F_WRLCK, 0);
  if (rc == 0)
    /* got exclusive */
    return MDBX_RESULT_TRUE;
  if (rc == EAGAIN || rc == EACCES || rc == EBUSY) {
    /* get shared access */
    rc = mdbx_lck_op(env->me_lfd, F_SETLKW, F_RDLCK, 0);
    if (rc == 0) {
      /* got shared, try exclusive again */
      rc = mdbx_lck_op(env->me_lfd, F_SETLK, F_WRLCK, 0);
      if (rc == 0)
        /* now got exclusive */
        return MDBX_RESULT_TRUE;
      if (rc == EAGAIN || rc == EACCES || rc == EBUSY)
        /* unable exclusive, but stay shared */
        return MDBX_RESULT_FALSE;
    }
  }
  assert(rc != MDBX_RESULT_FALSE && rc != MDBX_RESULT_TRUE);
  return rc;
}

int mdbx_lck_downgrade(MDB_env *env) {
  return mdbx_lck_op(env->me_lfd, F_SETLK, F_RDLCK, 0);
}

int mdbx_rpid_set(MDB_env *env) {
  return mdbx_lck_op(env->me_lfd, F_SETLK, F_WRLCK, env->me_pid);
}

int mdbx_rpid_clear(MDB_env *env) {
  return mdbx_lck_op(env->me_lfd, F_SETLKW, F_UNLCK, env->me_pid);
}

/* Checks reader by pid.
 *
 * Returns:
 *   MDBX_RESULT_TRUE, if pid is live (unable to acquire lock)
 *   MDBX_RESULT_FALSE, if pid is dead (lock acquired)
 *   or otherwise the errcode. */
int mdbx_rpid_check(MDB_env *env, mdbx_pid_t pid) {
  int rc = mdbx_lck_op(env->me_lfd, F_GETLK, F_WRLCK, pid);
  if (rc == 0)
    return MDBX_RESULT_FALSE;
  if (rc < 0 && -rc == pid)
    return MDBX_RESULT_TRUE;
  return rc;
}

static int mdbx_lck_op(mdbx_filehandle_t fd, int op, int lck, off_t offset) {
  for (;;) {
    int rc;
    struct flock lock_op;
    memset(&lock_op, 0, sizeof(lock_op));
    lock_op.l_type = lck;
    lock_op.l_whence = SEEK_SET;
    lock_op.l_start = offset;
    lock_op.l_len = 1;
    if ((rc = fcntl(fd, op, &lock_op)) == 0) {
      if (op == F_GETLK && lock_op.l_type != F_UNLCK)
        rc = -lock_op.l_pid;
    } else if ((rc = errno) == EINTR) {
      continue;
    }
    return rc;
  }
}

#if !__GLIBC_PREREQ(2, 12) && !defined(pthread_mutex_consistent)
#define pthread_mutex_consistent(mutex) pthread_mutex_consistent_np(mutex)
#endif

static int __cold mdbx_mutex_failed(MDB_env *env, mdbx_mutex_t *mutex, int rc) {
#if MDB_USE_ROBUST
  if (rc == EOWNERDEAD) {
    /* We own the mutex. Clean up after dead previous owner. */

    int rlocked = (mutex == &env->me_txns->mti_rmutex);
    rc = MDB_SUCCESS;
    if (!rlocked) {
      if (unlikely(env->me_txn)) {
        /* env is hosed if the dead thread was ours */
        env->me_flags |= MDB_FATAL_ERROR;
        env->me_txn = NULL;
        rc = MDB_PANIC;
      }
    }
    mdbx_notice("%cmutex owner died, %s", (rlocked ? 'r' : 'w'),
                (rc ? "this process' env is hosed" : "recovering"));

    int check_rc = mdbx_reader_check0(env, rlocked, NULL);
    int mreco_rc = pthread_mutex_consistent(mutex);
    check_rc = (mreco_rc == 0) ? check_rc : mreco_rc;

    if (unlikely(mreco_rc))
      mdbx_error("mutex recovery failed, %s", mdbx_strerror(mreco_rc));

    rc = (rc == MDB_SUCCESS) ? check_rc : rc;
    if (MDBX_IS_ERROR(rc))
      pthread_mutex_unlock(mutex);
    return rc;
  }
#endif /* MDB_USE_ROBUST */

  mdbx_error("lock mutex failed, %s", mdbx_strerror(rc));
  if (rc != EDEADLK) {
    env->me_flags |= MDB_FATAL_ERROR;
    rc = MDB_PANIC;
  }
  return rc;
}
