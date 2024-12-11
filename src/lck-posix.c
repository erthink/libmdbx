/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2024

#if !(defined(_WIN32) || defined(_WIN64))
/*----------------------------------------------------------------------------*
 * POSIX/non-Windows LCK-implementation */

#include "internals.h"

#if MDBX_LOCKING == MDBX_LOCKING_SYSV
#include <sys/sem.h>
#endif /* MDBX_LOCKING == MDBX_LOCKING_SYSV */

/* Описание реализации блокировок для POSIX & Linux:
 *
 * lck-файл отображается в память, в нём организуется таблица читателей и
 * размещаются совместно используемые posix-мьютексы (futex). Посредством
 * этих мьютексов (см struct lck_t) реализуются:
 *  - Блокировка таблицы читателей для регистрации,
 *    т.е. функции lck_rdt_lock() и lck_rdt_unlock().
 *  - Блокировка БД для пишущих транзакций,
 *    т.е. функции lck_txn_lock() и lck_txn_unlock().
 *
 * Остальной функционал реализуется отдельно посредством файловых блокировок:
 *  - Первоначальный захват БД в режиме exclusive/shared и последующий перевод
 *    в операционный режим, функции lck_seize() и lck_downgrade().
 *  - Проверка присутствие процессов-читателей,
 *    т.е. функции lck_rpid_set(), lck_rpid_clear() и lck_rpid_check().
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
 *       + для ЭКСКЛЮЗИВНОГО режима блокировка всего dxb-файла
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
__cold static void choice_fcntl(void) {
  assert(!op_setlk && !op_setlkw && !op_getlk);
  if ((globals.runtime_flags & MDBX_DBG_LEGACY_MULTIOPEN) == 0
#if defined(__linux__) || defined(__gnu_linux__)
      && globals.linux_kernel_version > 0x030f0000 /* OFD locks are available since 3.15, but engages here
                                                      only for 3.16 and later kernels (i.e. LTS) because
                                                      of reliability reasons */
#endif                                             /* linux */
  ) {
    op_setlk = MDBX_F_OFD_SETLK;
    op_setlkw = MDBX_F_OFD_SETLKW;
    op_getlk = MDBX_F_OFD_GETLK;
    return;
  }
  op_setlk = MDBX_F_SETLK;
  op_setlkw = MDBX_F_SETLKW;
  op_getlk = MDBX_F_GETLK;
}
#else
#define op_setlk MDBX_F_SETLK
#define op_setlkw MDBX_F_SETLKW
#define op_getlk MDBX_F_GETLK
#endif /* MDBX_USE_OFDLOCKS */

static int lck_op(const mdbx_filehandle_t fd, int cmd, const int lck, const off_t offset, off_t len) {
  STATIC_ASSERT(sizeof(off_t) >= sizeof(void *) && sizeof(off_t) >= sizeof(size_t));
#ifdef __ANDROID_API__
  STATIC_ASSERT_MSG((sizeof(off_t) * 8 == MDBX_WORDBITS), "The bitness of system `off_t` type is mismatch. Please "
                                                          "fix build and/or NDK configuration.");
#endif /* Android */
  assert(offset >= 0 && len > 0);
  assert((uint64_t)offset < (uint64_t)INT64_MAX && (uint64_t)len < (uint64_t)INT64_MAX &&
         (uint64_t)(offset + len) > (uint64_t)offset);

  assert((uint64_t)offset < (uint64_t)OFF_T_MAX && (uint64_t)len <= (uint64_t)OFF_T_MAX &&
         (uint64_t)(offset + len) <= (uint64_t)OFF_T_MAX);

  assert((uint64_t)((off_t)((uint64_t)offset + (uint64_t)len)) == ((uint64_t)offset + (uint64_t)len));

  jitter4testing(true);
  for (;;) {
    MDBX_STRUCT_FLOCK lock_op;
    STATIC_ASSERT_MSG(sizeof(off_t) <= sizeof(lock_op.l_start) && sizeof(off_t) <= sizeof(lock_op.l_len) &&
                          OFF_T_MAX == (off_t)OFF_T_MAX,
                      "Support for large/64-bit-sized files is misconfigured "
                      "for the target system and/or toolchain. "
                      "Please fix it or at least disable it completely.");
    memset(&lock_op, 0, sizeof(lock_op));
    lock_op.l_type = lck;
    lock_op.l_whence = SEEK_SET;
    lock_op.l_start = offset;
    lock_op.l_len = len;
    int rc = MDBX_FCNTL(fd, cmd, &lock_op);
    jitter4testing(true);
    if (rc != -1) {
      if (cmd == op_getlk) {
        /* Checks reader by pid. Returns:
         *   MDBX_RESULT_TRUE   - if pid is live (reader holds a lock).
         *   MDBX_RESULT_FALSE  - if pid is dead (a lock could be placed). */
        return (lock_op.l_type == F_UNLCK) ? MDBX_RESULT_FALSE : MDBX_RESULT_TRUE;
      }
      return MDBX_SUCCESS;
    }
    rc = errno;
#if MDBX_USE_OFDLOCKS
    if (rc == EINVAL && (cmd == MDBX_F_OFD_SETLK || cmd == MDBX_F_OFD_SETLKW || cmd == MDBX_F_OFD_GETLK)) {
      /* fallback to non-OFD locks */
      if (cmd == MDBX_F_OFD_SETLK)
        cmd = MDBX_F_SETLK;
      else if (cmd == MDBX_F_OFD_SETLKW)
        cmd = MDBX_F_SETLKW;
      else
        cmd = MDBX_F_GETLK;
      op_setlk = MDBX_F_SETLK;
      op_setlkw = MDBX_F_SETLKW;
      op_getlk = MDBX_F_GETLK;
      continue;
    }
#endif /* MDBX_USE_OFDLOCKS */
    if (rc != EINTR || cmd == op_setlkw) {
      assert(MDBX_IS_ERROR(rc));
      return rc;
    }
  }
}

MDBX_INTERNAL int osal_lockfile(mdbx_filehandle_t fd, bool wait) {
#if MDBX_USE_OFDLOCKS
  if (unlikely(op_setlk == 0))
    choice_fcntl();
#endif /* MDBX_USE_OFDLOCKS */
  return lck_op(fd, wait ? op_setlkw : op_setlk, F_WRLCK, 0, OFF_T_MAX);
}

MDBX_INTERNAL int lck_rpid_set(MDBX_env *env) {
  assert(env->lck_mmap.fd != INVALID_HANDLE_VALUE);
  assert(env->pid > 0);
  if (unlikely(osal_getpid() != env->pid))
    return MDBX_PANIC;
  return lck_op(env->lck_mmap.fd, op_setlk, F_WRLCK, env->pid, 1);
}

MDBX_INTERNAL int lck_rpid_clear(MDBX_env *env) {
  assert(env->lck_mmap.fd != INVALID_HANDLE_VALUE);
  assert(env->pid > 0);
  return lck_op(env->lck_mmap.fd, op_setlk, F_UNLCK, env->pid, 1);
}

MDBX_INTERNAL int lck_rpid_check(MDBX_env *env, uint32_t pid) {
  assert(env->lck_mmap.fd != INVALID_HANDLE_VALUE);
  assert(pid > 0);
  return lck_op(env->lck_mmap.fd, op_getlk, F_WRLCK, pid, 1);
}

/*---------------------------------------------------------------------------*/

#if MDBX_LOCKING > MDBX_LOCKING_SYSV
MDBX_INTERNAL int lck_ipclock_stubinit(osal_ipclock_t *ipc) {
#if MDBX_LOCKING == MDBX_LOCKING_POSIX1988
  return sem_init(ipc, false, 1) ? errno : 0;
#elif MDBX_LOCKING == MDBX_LOCKING_POSIX2001 || MDBX_LOCKING == MDBX_LOCKING_POSIX2008
  return pthread_mutex_init(ipc, nullptr);
#else
#error "FIXME"
#endif
}

MDBX_INTERNAL int lck_ipclock_destroy(osal_ipclock_t *ipc) {
#if MDBX_LOCKING == MDBX_LOCKING_POSIX1988
  return sem_destroy(ipc) ? errno : 0;
#elif MDBX_LOCKING == MDBX_LOCKING_POSIX2001 || MDBX_LOCKING == MDBX_LOCKING_POSIX2008
  return pthread_mutex_destroy(ipc);
#else
#error "FIXME"
#endif
}
#endif /* MDBX_LOCKING > MDBX_LOCKING_SYSV */

static int check_fstat(MDBX_env *env) {
  struct stat st;

  int rc = MDBX_SUCCESS;
  if (fstat(env->lazy_fd, &st)) {
    rc = errno;
    ERROR("fstat(%s), err %d", "DXB", rc);
    return rc;
  }

  if (!S_ISREG(st.st_mode) || st.st_nlink < 1) {
#ifdef EBADFD
    rc = EBADFD;
#else
    rc = EPERM;
#endif
    ERROR("%s %s, err %d", "DXB", (st.st_nlink < 1) ? "file was removed" : "not a regular file", rc);
    return rc;
  }

  if (st.st_size < (off_t)(MDBX_MIN_PAGESIZE * NUM_METAS)) {
    VERBOSE("dxb-file is too short (%u), exclusive-lock needed", (unsigned)st.st_size);
    rc = MDBX_RESULT_TRUE;
  }

  //----------------------------------------------------------------------------

  if (fstat(env->lck_mmap.fd, &st)) {
    rc = errno;
    ERROR("fstat(%s), err %d", "LCK", rc);
    return rc;
  }

  if (!S_ISREG(st.st_mode) || st.st_nlink < 1) {
#ifdef EBADFD
    rc = EBADFD;
#else
    rc = EPERM;
#endif
    ERROR("%s %s, err %d", "LCK", (st.st_nlink < 1) ? "file was removed" : "not a regular file", rc);
    return rc;
  }

  /* Checking file size for detect the situation when we got the shared lock
   * immediately after lck_destroy(). */
  if (st.st_size < (off_t)(sizeof(lck_t) + sizeof(reader_slot_t))) {
    VERBOSE("lck-file is too short (%u), exclusive-lock needed", (unsigned)st.st_size);
    rc = MDBX_RESULT_TRUE;
  }

  return rc;
}

__cold MDBX_INTERNAL int lck_seize(MDBX_env *env) {
  assert(env->lazy_fd != INVALID_HANDLE_VALUE);
  if (unlikely(osal_getpid() != env->pid))
    return MDBX_PANIC;

  int rc = MDBX_SUCCESS;
#if defined(__linux__) || defined(__gnu_linux__)
  if (unlikely(globals.running_on_WSL1)) {
    rc = ENOLCK /* No record locks available */;
    ERROR("%s, err %u",
          "WSL1 (Windows Subsystem for Linux) is mad and trouble-full, "
          "injecting failure to avoid data loss",
          rc);
    return rc;
  }
#endif /* Linux */

#if MDBX_USE_OFDLOCKS
  if (unlikely(op_setlk == 0))
    choice_fcntl();
#endif /* MDBX_USE_OFDLOCKS */

  if (env->lck_mmap.fd == INVALID_HANDLE_VALUE) {
    /* LY: without-lck mode (e.g. exclusive or on read-only filesystem) */
    rc = lck_op(env->lazy_fd, op_setlk, (env->flags & MDBX_RDONLY) ? F_RDLCK : F_WRLCK, 0, OFF_T_MAX);
    if (rc != MDBX_SUCCESS) {
      ERROR("%s, err %u", "without-lck", rc);
      eASSERT(env, MDBX_IS_ERROR(rc));
      return rc;
    }
    return MDBX_RESULT_TRUE /* Done: return with exclusive locking. */;
  }
#if defined(_POSIX_PRIORITY_SCHEDULING) && _POSIX_PRIORITY_SCHEDULING > 0
  sched_yield();
#endif

retry:
  if (rc == MDBX_RESULT_TRUE) {
    rc = lck_op(env->lck_mmap.fd, op_setlk, F_UNLCK, 0, 1);
    if (rc != MDBX_SUCCESS) {
      ERROR("%s, err %u", "unlock-before-retry", rc);
      eASSERT(env, MDBX_IS_ERROR(rc));
      return rc;
    }
  }

  /* Firstly try to get exclusive locking.  */
  rc = lck_op(env->lck_mmap.fd, op_setlk, F_WRLCK, 0, 1);
  if (rc == MDBX_SUCCESS) {
    rc = check_fstat(env);
    if (MDBX_IS_ERROR(rc))
      return rc;

  continue_dxb_exclusive:
    rc = lck_op(env->lazy_fd, op_setlk, (env->flags & MDBX_RDONLY) ? F_RDLCK : F_WRLCK, 0, OFF_T_MAX);
    if (rc == MDBX_SUCCESS)
      return MDBX_RESULT_TRUE /* Done: return with exclusive locking. */;

    int err = check_fstat(env);
    if (MDBX_IS_ERROR(err))
      return err;

    /* the cause may be a collision with POSIX's file-lock recovery. */
    if (!(rc == EAGAIN || rc == EACCES || rc == EBUSY || rc == EWOULDBLOCK || rc == EDEADLK)) {
      ERROR("%s, err %u", "dxb-exclusive", rc);
      eASSERT(env, MDBX_IS_ERROR(rc));
      return rc;
    }

    /* Fallback to lck-shared */
  } else if (!(rc == EAGAIN || rc == EACCES || rc == EBUSY || rc == EWOULDBLOCK || rc == EDEADLK)) {
    ERROR("%s, err %u", "try-exclusive", rc);
    eASSERT(env, MDBX_IS_ERROR(rc));
    return rc;
  }

  /* Here could be one of two:
   *  - lck_destroy() from the another process was hold the lock
   *    during a destruction.
   *  - either lck_seize() from the another process was got the exclusive
   *    lock and doing initialization.
   * For distinguish these cases will use size of the lck-file later. */

  /* Wait for lck-shared now. */
  /* Here may be await during transient processes, for instance until another
   * competing process doesn't call lck_downgrade(). */
  rc = lck_op(env->lck_mmap.fd, op_setlkw, F_RDLCK, 0, 1);
  if (rc != MDBX_SUCCESS) {
    ERROR("%s, err %u", "try-shared", rc);
    eASSERT(env, MDBX_IS_ERROR(rc));
    return rc;
  }

  rc = check_fstat(env);
  if (rc == MDBX_RESULT_TRUE)
    goto retry;
  if (rc != MDBX_SUCCESS) {
    ERROR("%s, err %u", "lck_fstat", rc);
    return rc;
  }

  /* got shared, retry exclusive */
  rc = lck_op(env->lck_mmap.fd, op_setlk, F_WRLCK, 0, 1);
  if (rc == MDBX_SUCCESS)
    goto continue_dxb_exclusive;

  if (!(rc == EAGAIN || rc == EACCES || rc == EBUSY || rc == EWOULDBLOCK || rc == EDEADLK)) {
    ERROR("%s, err %u", "try-exclusive", rc);
    eASSERT(env, MDBX_IS_ERROR(rc));
    return rc;
  }

  /* Lock against another process operating in without-lck or exclusive mode. */
  rc = lck_op(env->lazy_fd, op_setlk, (env->flags & MDBX_RDONLY) ? F_RDLCK : F_WRLCK, env->pid, 1);
  if (rc != MDBX_SUCCESS) {
    ERROR("%s, err %u", "lock-against-without-lck", rc);
    eASSERT(env, MDBX_IS_ERROR(rc));
    return rc;
  }

  /* Done: return with shared locking. */
  return MDBX_RESULT_FALSE;
}

MDBX_INTERNAL int lck_downgrade(MDBX_env *env) {
  assert(env->lck_mmap.fd != INVALID_HANDLE_VALUE);
  if (unlikely(osal_getpid() != env->pid))
    return MDBX_PANIC;

  int rc = MDBX_SUCCESS;
  if ((env->flags & MDBX_EXCLUSIVE) == 0) {
    rc = lck_op(env->lazy_fd, op_setlk, F_UNLCK, 0, env->pid);
    if (rc == MDBX_SUCCESS)
      rc = lck_op(env->lazy_fd, op_setlk, F_UNLCK, env->pid + 1, OFF_T_MAX - env->pid - 1);
  }
  if (rc == MDBX_SUCCESS)
    rc = lck_op(env->lck_mmap.fd, op_setlk, F_RDLCK, 0, 1);
  if (unlikely(rc != 0)) {
    ERROR("%s, err %u", "lck", rc);
    assert(MDBX_IS_ERROR(rc));
  }
  return rc;
}

MDBX_INTERNAL int lck_upgrade(MDBX_env *env, bool dont_wait) {
  assert(env->lck_mmap.fd != INVALID_HANDLE_VALUE);
  if (unlikely(osal_getpid() != env->pid))
    return MDBX_PANIC;

  const int cmd = dont_wait ? op_setlk : op_setlkw;
  int rc = lck_op(env->lck_mmap.fd, cmd, F_WRLCK, 0, 1);
  if (rc == MDBX_SUCCESS && (env->flags & MDBX_EXCLUSIVE) == 0) {
    rc = (env->pid > 1) ? lck_op(env->lazy_fd, cmd, F_WRLCK, 0, env->pid - 1) : MDBX_SUCCESS;
    if (rc == MDBX_SUCCESS) {
      rc = lck_op(env->lazy_fd, cmd, F_WRLCK, env->pid + 1, OFF_T_MAX - env->pid - 1);
      if (rc != MDBX_SUCCESS && env->pid > 1 && lck_op(env->lazy_fd, op_setlk, F_UNLCK, 0, env->pid - 1))
        rc = MDBX_PANIC;
    }
    if (rc != MDBX_SUCCESS && lck_op(env->lck_mmap.fd, op_setlk, F_RDLCK, 0, 1))
      rc = MDBX_PANIC;
  }
  if (unlikely(rc != 0)) {
    ERROR("%s, err %u", "lck", rc);
    assert(MDBX_IS_ERROR(rc));
  }
  return rc;
}

__cold MDBX_INTERNAL int lck_destroy(MDBX_env *env, MDBX_env *inprocess_neighbor, const uint32_t current_pid) {
  eASSERT(env, osal_getpid() == current_pid);
  int rc = MDBX_SUCCESS;
  struct stat lck_info;
  lck_t *lck = env->lck;
  if (lck && lck == env->lck_mmap.lck && !inprocess_neighbor &&
      /* try get exclusive access */
      lck_op(env->lck_mmap.fd, op_setlk, F_WRLCK, 0, OFF_T_MAX) == 0 &&
      /* if LCK was not removed */
      fstat(env->lck_mmap.fd, &lck_info) == 0 && lck_info.st_nlink > 0 &&
      lck_op(env->lazy_fd, op_setlk, (env->flags & MDBX_RDONLY) ? F_RDLCK : F_WRLCK, 0, OFF_T_MAX) == 0) {

    VERBOSE("%p got exclusive, drown ipc-locks", (void *)env);
    eASSERT(env, current_pid == env->pid);
#if MDBX_LOCKING == MDBX_LOCKING_SYSV
    if (env->me_sysv_ipc.semid != -1)
      rc = semctl(env->me_sysv_ipc.semid, 2, IPC_RMID) ? errno : 0;
#else
    rc = lck_ipclock_destroy(&lck->rdt_lock);
    if (rc == 0)
      rc = lck_ipclock_destroy(&lck->wrt_lock);
#endif /* MDBX_LOCKING */

    eASSERT(env, rc == 0);
    if (rc == 0) {
      const bool synced = lck->unsynced_pages.weak == 0;
      osal_munmap(&env->lck_mmap);
      if (synced && env->lck_mmap.fd != INVALID_HANDLE_VALUE)
        rc = ftruncate(env->lck_mmap.fd, 0) ? errno : 0;
    }

    jitter4testing(false);
  }

  if (current_pid != env->pid) {
    eASSERT(env, !inprocess_neighbor);
    NOTICE("drown env %p after-fork pid %d -> %d", __Wpedantic_format_voidptr(env), env->pid, current_pid);
    inprocess_neighbor = nullptr;
  }

  /* 1) POSIX's fcntl() locks (i.e. when op_setlk == F_SETLK) should be restored
   * after file was closed.
   *
   * 2) File locks would be released (by kernel) while the file-descriptors will
   * be closed. But to avoid false-positive EACCESS and EDEADLK from the kernel,
   * locks should be released here explicitly with properly order. */

  /* close dxb and restore lock */
  if (env->dsync_fd != INVALID_HANDLE_VALUE) {
    if (unlikely(close(env->dsync_fd) != 0) && rc == MDBX_SUCCESS)
      rc = errno;
    env->dsync_fd = INVALID_HANDLE_VALUE;
  }
  if (env->lazy_fd != INVALID_HANDLE_VALUE) {
    if (unlikely(close(env->lazy_fd) != 0) && rc == MDBX_SUCCESS)
      rc = errno;
    env->lazy_fd = INVALID_HANDLE_VALUE;
    if (op_setlk == F_SETLK && inprocess_neighbor && rc == MDBX_SUCCESS) {
      /* restore file-lock */
      rc = lck_op(inprocess_neighbor->lazy_fd, F_SETLKW, (inprocess_neighbor->flags & MDBX_RDONLY) ? F_RDLCK : F_WRLCK,
                  (inprocess_neighbor->flags & MDBX_EXCLUSIVE) ? 0 : inprocess_neighbor->pid,
                  (inprocess_neighbor->flags & MDBX_EXCLUSIVE) ? OFF_T_MAX : 1);
    }
  }

  /* close clk and restore locks */
  if (env->lck_mmap.fd != INVALID_HANDLE_VALUE) {
    if (unlikely(close(env->lck_mmap.fd) != 0) && rc == MDBX_SUCCESS)
      rc = errno;
    env->lck_mmap.fd = INVALID_HANDLE_VALUE;
    if (op_setlk == F_SETLK && inprocess_neighbor && rc == MDBX_SUCCESS) {
      /* restore file-locks */
      rc = lck_op(inprocess_neighbor->lck_mmap.fd, F_SETLKW, F_RDLCK, 0, 1);
      if (rc == MDBX_SUCCESS && inprocess_neighbor->registered_reader_pid)
        rc = lck_rpid_set(inprocess_neighbor);
    }
  }

  if (inprocess_neighbor && rc != MDBX_SUCCESS)
    inprocess_neighbor->flags |= ENV_FATAL_ERROR;
  return rc;
}

/*---------------------------------------------------------------------------*/

__cold MDBX_INTERNAL int lck_init(MDBX_env *env, MDBX_env *inprocess_neighbor, int global_uniqueness_flag) {
#if MDBX_LOCKING == MDBX_LOCKING_SYSV
  int semid = -1;
  /* don't initialize semaphores twice */
  (void)inprocess_neighbor;
  if (global_uniqueness_flag == MDBX_RESULT_TRUE) {
    struct stat st;
    if (fstat(env->lazy_fd, &st))
      return errno;
  sysv_retry_create:
    semid = semget(env->me_sysv_ipc.key, 2, IPC_CREAT | IPC_EXCL | (st.st_mode & (S_IRWXU | S_IRWXG | S_IRWXO)));
    if (unlikely(semid == -1)) {
      int err = errno;
      if (err != EEXIST)
        return err;

      /* remove and re-create semaphore set */
      semid = semget(env->me_sysv_ipc.key, 2, 0);
      if (semid == -1) {
        err = errno;
        if (err != ENOENT)
          return err;
        goto sysv_retry_create;
      }
      if (semctl(semid, 2, IPC_RMID)) {
        err = errno;
        if (err != EIDRM)
          return err;
      }
      goto sysv_retry_create;
    }

    unsigned short val_array[2] = {1, 1};
    if (semctl(semid, 2, SETALL, val_array))
      return errno;
  } else {
    semid = semget(env->me_sysv_ipc.key, 2, 0);
    if (semid == -1)
      return errno;

    /* check read & write access */
    struct semid_ds data[2];
    if (semctl(semid, 2, IPC_STAT, data) || semctl(semid, 2, IPC_SET, data))
      return errno;
  }

  env->me_sysv_ipc.semid = semid;
  return MDBX_SUCCESS;

#elif MDBX_LOCKING == MDBX_LOCKING_FUTEX
  (void)inprocess_neighbor;
  if (global_uniqueness_flag != MDBX_RESULT_TRUE)
    return MDBX_SUCCESS;
#error "FIXME: Not implemented"
#elif MDBX_LOCKING == MDBX_LOCKING_POSIX1988

  /* don't initialize semaphores twice */
  (void)inprocess_neighbor;
  if (global_uniqueness_flag == MDBX_RESULT_TRUE) {
    if (sem_init(&env->lck_mmap.lck->rdt_lock, true, 1))
      return errno;
    if (sem_init(&env->lck_mmap.lck->wrt_lock, true, 1))
      return errno;
  }
  return MDBX_SUCCESS;

#elif MDBX_LOCKING == MDBX_LOCKING_POSIX2001 || MDBX_LOCKING == MDBX_LOCKING_POSIX2008
  if (inprocess_neighbor)
    return MDBX_SUCCESS /* don't need any initialization for mutexes
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

#if MDBX_LOCKING == MDBX_LOCKING_POSIX2008
#if defined(PTHREAD_MUTEX_ROBUST) || defined(pthread_mutexattr_setrobust)
  rc = pthread_mutexattr_setrobust(&ma, PTHREAD_MUTEX_ROBUST);
#elif defined(PTHREAD_MUTEX_ROBUST_NP) || defined(pthread_mutexattr_setrobust_np)
  rc = pthread_mutexattr_setrobust_np(&ma, PTHREAD_MUTEX_ROBUST_NP);
#elif _POSIX_THREAD_PROCESS_SHARED < 200809L
  rc = pthread_mutexattr_setrobust_np(&ma, PTHREAD_MUTEX_ROBUST_NP);
#else
  rc = pthread_mutexattr_setrobust(&ma, PTHREAD_MUTEX_ROBUST);
#endif
  if (rc)
    goto bailout;
#endif /* MDBX_LOCKING == MDBX_LOCKING_POSIX2008 */

#if defined(_POSIX_THREAD_PRIO_INHERIT) && _POSIX_THREAD_PRIO_INHERIT >= 0 && !defined(MDBX_SAFE4QEMU)
  rc = pthread_mutexattr_setprotocol(&ma, PTHREAD_PRIO_INHERIT);
  if (rc == ENOTSUP)
    rc = pthread_mutexattr_setprotocol(&ma, PTHREAD_PRIO_NONE);
  if (rc && rc != ENOTSUP)
    goto bailout;
#endif /* PTHREAD_PRIO_INHERIT */

  rc = pthread_mutexattr_settype(&ma, PTHREAD_MUTEX_ERRORCHECK);
  if (rc && rc != ENOTSUP)
    goto bailout;

  rc = pthread_mutex_init(&env->lck_mmap.lck->rdt_lock, &ma);
  if (rc)
    goto bailout;
  rc = pthread_mutex_init(&env->lck_mmap.lck->wrt_lock, &ma);

bailout:
  pthread_mutexattr_destroy(&ma);
  return rc;
#else
#error "FIXME"
#endif /* MDBX_LOCKING > 0 */
}

__cold static int osal_ipclock_failed(MDBX_env *env, osal_ipclock_t *ipc, const int err) {
  int rc = err;
#if MDBX_LOCKING == MDBX_LOCKING_POSIX2008 || MDBX_LOCKING == MDBX_LOCKING_SYSV

#ifndef EOWNERDEAD
#define EOWNERDEAD MDBX_RESULT_TRUE
#endif /* EOWNERDEAD */

  if (err == EOWNERDEAD) {
    /* We own the mutex. Clean up after dead previous owner. */
    const bool rlocked = ipc == &env->lck->rdt_lock;
    rc = MDBX_SUCCESS;
    if (!rlocked) {
      if (unlikely(env->txn)) {
        /* env is hosed if the dead thread was ours */
        env->flags |= ENV_FATAL_ERROR;
        env->txn = nullptr;
        rc = MDBX_PANIC;
      }
    }
    WARNING("%clock owner died, %s", (rlocked ? 'r' : 'w'), (rc ? "this process' env is hosed" : "recovering"));

    int check_rc = mvcc_cleanup_dead(env, rlocked, nullptr);
    check_rc = (check_rc == MDBX_SUCCESS) ? MDBX_RESULT_TRUE : check_rc;

#if MDBX_LOCKING == MDBX_LOCKING_SYSV
    rc = (rc == MDBX_SUCCESS) ? check_rc : rc;
#else
#if defined(PTHREAD_MUTEX_ROBUST) || defined(pthread_mutex_consistent)
    int mreco_rc = pthread_mutex_consistent(ipc);
#elif defined(PTHREAD_MUTEX_ROBUST_NP) || defined(pthread_mutex_consistent_np)
    int mreco_rc = pthread_mutex_consistent_np(ipc);
#elif _POSIX_THREAD_PROCESS_SHARED < 200809L
    int mreco_rc = pthread_mutex_consistent_np(ipc);
#else
    int mreco_rc = pthread_mutex_consistent(ipc);
#endif
    check_rc = (mreco_rc == 0) ? check_rc : mreco_rc;

    if (unlikely(mreco_rc))
      ERROR("lock recovery failed, %s", mdbx_strerror(mreco_rc));

    rc = (rc == MDBX_SUCCESS) ? check_rc : rc;
    if (MDBX_IS_ERROR(rc))
      pthread_mutex_unlock(ipc);
#endif /* MDBX_LOCKING == MDBX_LOCKING_POSIX2008 */
    return rc;
  }
#elif MDBX_LOCKING == MDBX_LOCKING_POSIX2001
  (void)ipc;
#elif MDBX_LOCKING == MDBX_LOCKING_POSIX1988
  (void)ipc;
#elif MDBX_LOCKING == MDBX_LOCKING_FUTEX
#ifdef _MSC_VER
#pragma message("warning: TODO")
#else
#warning "TODO"
#endif
  (void)ipc;
#else
#error "FIXME"
#endif /* MDBX_LOCKING */

  ERROR("mutex (un)lock failed, %s", mdbx_strerror(err));
  if (rc != EDEADLK)
    env->flags |= ENV_FATAL_ERROR;
  return rc;
}

#if defined(__ANDROID_API__) || defined(ANDROID) || defined(BIONIC)
MDBX_INTERNAL int osal_check_tid4bionic(void) {
  /* avoid 32-bit Bionic bug/hang with 32-pit TID */
  if (sizeof(pthread_mutex_t) < sizeof(pid_t) + sizeof(unsigned)) {
    pid_t tid = gettid();
    if (unlikely(tid > 0xffff)) {
      FATAL("Raise the ENOSYS(%d) error to avoid hang due "
            "the 32-bit Bionic/Android bug with tid/thread_id 0x%08x(%i) "
            "that don’t fit in 16 bits, see "
            "https://android.googlesource.com/platform/bionic/+/master/"
            "docs/32-bit-abi.md#is-too-small-for-large-pids",
            ENOSYS, tid, tid);
      return ENOSYS;
    }
  }
  return 0;
}
#endif /* __ANDROID_API__ || ANDROID) || BIONIC */

static int osal_ipclock_lock(MDBX_env *env, osal_ipclock_t *ipc, const bool dont_wait) {
#if MDBX_LOCKING == MDBX_LOCKING_POSIX2001 || MDBX_LOCKING == MDBX_LOCKING_POSIX2008
  int rc = osal_check_tid4bionic();
  if (likely(rc == 0))
    rc = dont_wait ? pthread_mutex_trylock(ipc) : pthread_mutex_lock(ipc);
  rc = (rc == EBUSY && dont_wait) ? MDBX_BUSY : rc;
#elif MDBX_LOCKING == MDBX_LOCKING_POSIX1988
  int rc = MDBX_SUCCESS;
  if (dont_wait) {
    if (sem_trywait(ipc)) {
      rc = errno;
      if (rc == EAGAIN)
        rc = MDBX_BUSY;
    }
  } else if (sem_wait(ipc))
    rc = errno;
#elif MDBX_LOCKING == MDBX_LOCKING_SYSV
  struct sembuf op = {
      .sem_num = (ipc != &env->lck->wrt_lock), .sem_op = -1, .sem_flg = dont_wait ? IPC_NOWAIT | SEM_UNDO : SEM_UNDO};
  int rc;
  if (semop(env->me_sysv_ipc.semid, &op, 1)) {
    rc = errno;
    if (dont_wait && rc == EAGAIN)
      rc = MDBX_BUSY;
  } else {
    rc = *ipc ? EOWNERDEAD : MDBX_SUCCESS;
    *ipc = env->pid;
  }
#else
#error "FIXME"
#endif /* MDBX_LOCKING */

  if (unlikely(rc != MDBX_SUCCESS && rc != MDBX_BUSY))
    rc = osal_ipclock_failed(env, ipc, rc);
  return rc;
}

int osal_ipclock_unlock(MDBX_env *env, osal_ipclock_t *ipc) {
  int err = MDBX_ENOSYS;
#if MDBX_LOCKING == MDBX_LOCKING_POSIX2001 || MDBX_LOCKING == MDBX_LOCKING_POSIX2008
  err = pthread_mutex_unlock(ipc);
#elif MDBX_LOCKING == MDBX_LOCKING_POSIX1988
  err = sem_post(ipc) ? errno : MDBX_SUCCESS;
#elif MDBX_LOCKING == MDBX_LOCKING_SYSV
  if (unlikely(*ipc != (pid_t)env->pid))
    err = EPERM;
  else {
    *ipc = 0;
    struct sembuf op = {.sem_num = (ipc != &env->lck->wrt_lock), .sem_op = 1, .sem_flg = SEM_UNDO};
    err = semop(env->me_sysv_ipc.semid, &op, 1) ? errno : MDBX_SUCCESS;
  }
#else
#error "FIXME"
#endif /* MDBX_LOCKING */
  int rc = err;
  if (unlikely(rc != MDBX_SUCCESS)) {
    const uint32_t current_pid = osal_getpid();
    if (current_pid == env->pid || LOG_ENABLED(MDBX_LOG_NOTICE))
      debug_log((current_pid == env->pid) ? MDBX_LOG_FATAL : (rc = MDBX_SUCCESS, MDBX_LOG_NOTICE), "ipc-unlock()",
                __LINE__, "failed: env %p, lck-%s %p, err %d\n", __Wpedantic_format_voidptr(env),
                (env->lck == env->lck_mmap.lck) ? "mmap" : "stub", __Wpedantic_format_voidptr(env->lck), err);
  }
  return rc;
}

MDBX_INTERNAL int lck_rdt_lock(MDBX_env *env) {
  TRACE("%s", ">>");
  jitter4testing(true);
  int rc = osal_ipclock_lock(env, &env->lck->rdt_lock, false);
  TRACE("<< rc %d", rc);
  return rc;
}

MDBX_INTERNAL void lck_rdt_unlock(MDBX_env *env) {
  TRACE("%s", ">>");
  int err = osal_ipclock_unlock(env, &env->lck->rdt_lock);
  TRACE("<< err %d", err);
  if (unlikely(err != MDBX_SUCCESS))
    mdbx_panic("%s() failed: err %d\n", __func__, err);
  jitter4testing(true);
}

int lck_txn_lock(MDBX_env *env, bool dont_wait) {
  TRACE("%swait %s", dont_wait ? "dont-" : "", ">>");
  jitter4testing(true);
  const int err = osal_ipclock_lock(env, &env->lck->wrt_lock, dont_wait);
  int rc = err;
  if (likely(!MDBX_IS_ERROR(err))) {
    eASSERT(env, !env->basal_txn->owner || err == /* если другой поток в этом-же процессе завершился
                                                     не освободив блокировку */
                                               MDBX_RESULT_TRUE);
    env->basal_txn->owner = osal_thread_self();
    rc = MDBX_SUCCESS;
  }
  TRACE("<< err %d, rc %d", err, rc);
  return rc;
}

void lck_txn_unlock(MDBX_env *env) {
  TRACE("%s", ">>");
  eASSERT(env, env->basal_txn->owner == osal_thread_self());
  env->basal_txn->owner = 0;
  int err = osal_ipclock_unlock(env, &env->lck->wrt_lock);
  TRACE("<< err %d", err);
  if (unlikely(err != MDBX_SUCCESS))
    mdbx_panic("%s() failed: err %d\n", __func__, err);
  jitter4testing(true);
}

#endif /* !Windows LCK-implementation */
