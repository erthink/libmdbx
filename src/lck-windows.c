/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2024

#if defined(_WIN32) || defined(_WIN64)

/* PREAMBLE FOR WINDOWS:
 *
 * We are not concerned for performance here.
 * If you are running Windows a performance could NOT be the goal.
 * Otherwise please use Linux. */

#include "internals.h"

#define LCK_SHARED 0
#define LCK_EXCLUSIVE LOCKFILE_EXCLUSIVE_LOCK
#define LCK_WAITFOR 0
#define LCK_DONTWAIT LOCKFILE_FAIL_IMMEDIATELY

static int flock_with_event(HANDLE fd, HANDLE event, unsigned flags,
                            size_t offset, size_t bytes) {
  TRACE("lock>>: fd %p, event %p, flags 0x%x offset %zu, bytes %zu >>", fd,
        event, flags, offset, bytes);
  OVERLAPPED ov;
  ov.Internal = 0;
  ov.InternalHigh = 0;
  ov.hEvent = event;
  ov.Offset = (DWORD)offset;
  ov.OffsetHigh = HIGH_DWORD(offset);
  if (LockFileEx(fd, flags, 0, (DWORD)bytes, HIGH_DWORD(bytes), &ov)) {
    TRACE("lock<<: fd %p, event %p, flags 0x%x offset %zu, bytes %zu << %s", fd,
          event, flags, offset, bytes, "done");
    return MDBX_SUCCESS;
  }

  DWORD rc = GetLastError();
  if (rc == ERROR_IO_PENDING) {
    if (event) {
      if (GetOverlappedResult(fd, &ov, &rc, true)) {
        TRACE("lock<<: fd %p, event %p, flags 0x%x offset %zu, bytes %zu << %s",
              fd, event, flags, offset, bytes, "overlapped-done");
        return MDBX_SUCCESS;
      }
      rc = GetLastError();
    } else
      CancelIo(fd);
  }
  TRACE("lock<<: fd %p, event %p, flags 0x%x offset %zu, bytes %zu << err %d",
        fd, event, flags, offset, bytes, (int)rc);
  return (int)rc;
}

static inline int flock(HANDLE fd, unsigned flags, size_t offset,
                        size_t bytes) {
  return flock_with_event(fd, 0, flags, offset, bytes);
}

static inline int flock_data(const MDBX_env *env, unsigned flags, size_t offset,
                             size_t bytes) {
  const HANDLE fd4data =
      env->ioring.overlapped_fd ? env->ioring.overlapped_fd : env->lazy_fd;
  return flock_with_event(fd4data, env->dxb_lock_event, flags, offset, bytes);
}

static int funlock(mdbx_filehandle_t fd, size_t offset, size_t bytes) {
  TRACE("unlock: fd %p, offset %zu, bytes %zu", fd, offset, bytes);
  return UnlockFile(fd, (DWORD)offset, HIGH_DWORD(offset), (DWORD)bytes,
                    HIGH_DWORD(bytes))
             ? MDBX_SUCCESS
             : (int)GetLastError();
}

/*----------------------------------------------------------------------------*/
/* global `write` lock for write-txt processing,
 * exclusive locking both meta-pages) */

#ifdef _WIN64
#define DXB_MAXLEN UINT64_C(0x7fffFFFFfff00000)
#else
#define DXB_MAXLEN UINT32_C(0x7ff00000)
#endif
#define DXB_BODY (env->ps * (size_t)NUM_METAS), DXB_MAXLEN
#define DXB_WHOLE 0, DXB_MAXLEN

int lck_txn_lock(MDBX_env *env, bool dontwait) {
  if (dontwait) {
    if (!TryEnterCriticalSection(&env->windowsbug_lock))
      return MDBX_BUSY;
  } else {
    __try {
      EnterCriticalSection(&env->windowsbug_lock);
    }
    __except ((GetExceptionCode() ==
                 0xC0000194 /* STATUS_POSSIBLE_DEADLOCK / EXCEPTION_POSSIBLE_DEADLOCK */)
                    ? EXCEPTION_EXECUTE_HANDLER
                    : EXCEPTION_CONTINUE_SEARCH) {
      return MDBX_EDEADLK;
    }
  }

  eASSERT(env, !env->basal_txn->owner);
  if (env->flags & MDBX_EXCLUSIVE)
    goto done;

  const HANDLE fd4data =
      env->ioring.overlapped_fd ? env->ioring.overlapped_fd : env->lazy_fd;
  int rc = flock_with_event(fd4data, env->dxb_lock_event,
                            dontwait ? (LCK_EXCLUSIVE | LCK_DONTWAIT)
                                     : (LCK_EXCLUSIVE | LCK_WAITFOR),
                            DXB_BODY);
  if (rc == ERROR_LOCK_VIOLATION && dontwait) {
    SleepEx(0, true);
    rc = flock_with_event(fd4data, env->dxb_lock_event,
                          LCK_EXCLUSIVE | LCK_DONTWAIT, DXB_BODY);
    if (rc == ERROR_LOCK_VIOLATION) {
      SleepEx(0, true);
      rc = flock_with_event(fd4data, env->dxb_lock_event,
                            LCK_EXCLUSIVE | LCK_DONTWAIT, DXB_BODY);
    }
  }
  if (rc == MDBX_SUCCESS) {
  done:
    /* Zap: Failing to release lock 'env->windowsbug_lock'
     *      in function 'mdbx_txn_lock' */
    MDBX_SUPPRESS_GOOFY_MSVC_ANALYZER(26115);
    env->basal_txn->owner = osal_thread_self();
    return MDBX_SUCCESS;
  }

  LeaveCriticalSection(&env->windowsbug_lock);
  return (!dontwait || rc != ERROR_LOCK_VIOLATION) ? rc : MDBX_BUSY;
}

void lck_txn_unlock(MDBX_env *env) {
  eASSERT(env, env->basal_txn->owner == osal_thread_self());
  if ((env->flags & MDBX_EXCLUSIVE) == 0) {
    const HANDLE fd4data =
        env->ioring.overlapped_fd ? env->ioring.overlapped_fd : env->lazy_fd;
    int err = funlock(fd4data, DXB_BODY);
    if (err != MDBX_SUCCESS)
      mdbx_panic("%s failed: err %u", __func__, err);
  }
  env->basal_txn->owner = 0;
  LeaveCriticalSection(&env->windowsbug_lock);
}

/*----------------------------------------------------------------------------*/
/* global `read` lock for readers registration,
 * exclusive locking `rdt_length` (second) cacheline */

#define LCK_LO_OFFSET 0
#define LCK_LO_LEN offsetof(lck_t, rdt_length)
#define LCK_UP_OFFSET LCK_LO_LEN
#define LCK_UP_LEN (sizeof(lck_t) - LCK_UP_OFFSET)
#define LCK_LOWER LCK_LO_OFFSET, LCK_LO_LEN
#define LCK_UPPER LCK_UP_OFFSET, LCK_UP_LEN

MDBX_INTERNAL int lck_rdt_lock(MDBX_env *env) {
  imports.srwl_AcquireShared(&env->remap_guard);
  if (env->lck_mmap.fd == INVALID_HANDLE_VALUE)
    return MDBX_SUCCESS; /* readonly database in readonly filesystem */

  /* transition from S-? (used) to S-E (locked),
   * e.g. exclusive lock upper-part */
  if (env->flags & MDBX_EXCLUSIVE)
    return MDBX_SUCCESS;

  int rc = flock(env->lck_mmap.fd, LCK_EXCLUSIVE | LCK_WAITFOR, LCK_UPPER);
  if (rc == MDBX_SUCCESS)
    return MDBX_SUCCESS;

  imports.srwl_ReleaseShared(&env->remap_guard);
  return rc;
}

MDBX_INTERNAL void lck_rdt_unlock(MDBX_env *env) {
  if (env->lck_mmap.fd != INVALID_HANDLE_VALUE &&
      (env->flags & MDBX_EXCLUSIVE) == 0) {
    /* transition from S-E (locked) to S-? (used), e.g. unlock upper-part */
    int err = funlock(env->lck_mmap.fd, LCK_UPPER);
    if (err != MDBX_SUCCESS)
      mdbx_panic("%s failed: err %u", __func__, err);
  }
  imports.srwl_ReleaseShared(&env->remap_guard);
}

MDBX_INTERNAL int osal_lockfile(mdbx_filehandle_t fd, bool wait) {
  return flock(
      fd, wait ? LCK_EXCLUSIVE | LCK_WAITFOR : LCK_EXCLUSIVE | LCK_DONTWAIT, 0,
      DXB_MAXLEN);
}

static int suspend_and_append(mdbx_handle_array_t **array,
                              const DWORD ThreadId) {
  const unsigned limit = (*array)->limit;
  if ((*array)->count == limit) {
    mdbx_handle_array_t *const ptr =
        osal_realloc((limit > ARRAY_LENGTH((*array)->handles))
                         ? *array
                         : /* don't free initial array on the stack */ nullptr,
                     sizeof(mdbx_handle_array_t) +
                         sizeof(HANDLE) * (limit * (size_t)2 -
                                           ARRAY_LENGTH((*array)->handles)));
    if (!ptr)
      return MDBX_ENOMEM;
    if (limit == ARRAY_LENGTH((*array)->handles))
      *ptr = **array;
    *array = ptr;
    (*array)->limit = limit * 2;
  }

  HANDLE hThread = OpenThread(THREAD_SUSPEND_RESUME | THREAD_QUERY_INFORMATION,
                              FALSE, ThreadId);
  if (hThread == nullptr)
    return (int)GetLastError();

  if (SuspendThread(hThread) == (DWORD)-1) {
    int err = (int)GetLastError();
    DWORD ExitCode;
    if (err == /* workaround for Win10 UCRT bug */ ERROR_ACCESS_DENIED ||
        !GetExitCodeThread(hThread, &ExitCode) || ExitCode != STILL_ACTIVE)
      err = MDBX_SUCCESS;
    CloseHandle(hThread);
    return err;
  }

  (*array)->handles[(*array)->count++] = hThread;
  return MDBX_SUCCESS;
}

MDBX_INTERNAL int
osal_suspend_threads_before_remap(MDBX_env *env, mdbx_handle_array_t **array) {
  eASSERT(env, (env->flags & MDBX_NOSTICKYTHREADS) == 0);
  const uintptr_t CurrentTid = GetCurrentThreadId();
  int rc;
  if (env->lck_mmap.lck) {
    /* Scan LCK for threads of the current process */
    const reader_slot_t *const begin = env->lck_mmap.lck->rdt;
    const reader_slot_t *const end =
        begin +
        atomic_load32(&env->lck_mmap.lck->rdt_length, mo_AcquireRelease);
    const uintptr_t WriteTxnOwner = env->basal_txn ? env->basal_txn->owner : 0;
    for (const reader_slot_t *reader = begin; reader < end; ++reader) {
      if (reader->pid.weak != env->pid || !reader->tid.weak ||
          reader->tid.weak >= MDBX_TID_TXN_OUSTED) {
      skip_lck:
        continue;
      }
      if (reader->tid.weak == CurrentTid || reader->tid.weak == WriteTxnOwner)
        goto skip_lck;

      rc = suspend_and_append(array, (mdbx_tid_t)reader->tid.weak);
      if (rc != MDBX_SUCCESS) {
      bailout_lck:
        (void)osal_resume_threads_after_remap(*array);
        return rc;
      }
    }
    if (WriteTxnOwner && WriteTxnOwner != CurrentTid) {
      rc = suspend_and_append(array, (mdbx_tid_t)WriteTxnOwner);
      if (rc != MDBX_SUCCESS)
        goto bailout_lck;
    }
  } else {
    /* Without LCK (i.e. read-only mode).
     * Walk through a snapshot of all running threads */
    eASSERT(env, env->flags & (MDBX_EXCLUSIVE | MDBX_RDONLY));
    const HANDLE hSnapshot = CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0);
    if (hSnapshot == INVALID_HANDLE_VALUE)
      return (int)GetLastError();

    THREADENTRY32 entry;
    entry.dwSize = sizeof(THREADENTRY32);

    if (!Thread32First(hSnapshot, &entry)) {
      rc = (int)GetLastError();
    bailout_toolhelp:
      CloseHandle(hSnapshot);
      (void)osal_resume_threads_after_remap(*array);
      return rc;
    }

    do {
      if (entry.th32OwnerProcessID != env->pid ||
          entry.th32ThreadID == CurrentTid)
        continue;

      rc = suspend_and_append(array, entry.th32ThreadID);
      if (rc != MDBX_SUCCESS)
        goto bailout_toolhelp;

    } while (Thread32Next(hSnapshot, &entry));

    rc = (int)GetLastError();
    if (rc != ERROR_NO_MORE_FILES)
      goto bailout_toolhelp;
    CloseHandle(hSnapshot);
  }

  return MDBX_SUCCESS;
}

MDBX_INTERNAL int osal_resume_threads_after_remap(mdbx_handle_array_t *array) {
  int rc = MDBX_SUCCESS;
  for (unsigned i = 0; i < array->count; ++i) {
    const HANDLE hThread = array->handles[i];
    if (ResumeThread(hThread) == (DWORD)-1) {
      const int err = (int)GetLastError();
      DWORD ExitCode;
      if (err != /* workaround for Win10 UCRT bug */ ERROR_ACCESS_DENIED &&
          GetExitCodeThread(hThread, &ExitCode) && ExitCode == STILL_ACTIVE)
        rc = err;
    }
    CloseHandle(hThread);
  }
  return rc;
}

/*----------------------------------------------------------------------------*/
/* global `initial` lock for lockfile initialization,
 * exclusive/shared locking first cacheline */

/* Briefly description of locking schema/algorithm:
 *  - Windows does not support upgrading or downgrading for file locking.
 *  - Therefore upgrading/downgrading is emulated by shared and exclusive
 *    locking of upper and lower halves.
 *  - In other words, we have FSM with possible 9 states,
 *    i.e. free/shared/exclusive x free/shared/exclusive == 9.
 *    Only 6 states of FSM are used, which 2 of ones are transitive.
 *
 * States:
 *  LO HI
 *   ?-?  = free, i.e. unlocked
 *   S-?  = used, i.e. shared lock
 *   E-?  = exclusive-read, i.e. operational exclusive
 *   ?-S
 *   ?-E  = middle (transitive state)
 *   S-S
 *   S-E  = locked (transitive state)
 *   E-S
 *   E-E  = exclusive-write, i.e. exclusive due (re)initialization
 *
 *  The lck_seize() moves the locking-FSM from the initial free/unlocked
 *  state to the "exclusive write" (and returns MDBX_RESULT_TRUE) if possible,
 *  or to the "used" (and returns MDBX_RESULT_FALSE).
 *
 *  The lck_downgrade() moves the locking-FSM from "exclusive write"
 *  state to the "used" (i.e. shared) state.
 *
 *  The lck_upgrade() moves the locking-FSM from "used" (i.e. shared)
 *  state to the "exclusive write" state.
 */

static void lck_unlock(MDBX_env *env) {
  int err;

  if (env->lck_mmap.fd != INVALID_HANDLE_VALUE) {
    /* double `unlock` for robustly remove overlapped shared/exclusive locks */
    do
      err = funlock(env->lck_mmap.fd, LCK_LOWER);
    while (err == MDBX_SUCCESS);
    assert(err == ERROR_NOT_LOCKED ||
           (globals.running_under_Wine && err == ERROR_LOCK_VIOLATION));
    SetLastError(ERROR_SUCCESS);

    do
      err = funlock(env->lck_mmap.fd, LCK_UPPER);
    while (err == MDBX_SUCCESS);
    assert(err == ERROR_NOT_LOCKED ||
           (globals.running_under_Wine && err == ERROR_LOCK_VIOLATION));
    SetLastError(ERROR_SUCCESS);
  }

  const HANDLE fd4data =
      env->ioring.overlapped_fd ? env->ioring.overlapped_fd : env->lazy_fd;
  if (fd4data != INVALID_HANDLE_VALUE) {
    /* explicitly unlock to avoid latency for other processes (windows kernel
     * releases such locks via deferred queues) */
    do
      err = funlock(fd4data, DXB_BODY);
    while (err == MDBX_SUCCESS);
    assert(err == ERROR_NOT_LOCKED ||
           (globals.running_under_Wine && err == ERROR_LOCK_VIOLATION));
    SetLastError(ERROR_SUCCESS);

    do
      err = funlock(fd4data, DXB_WHOLE);
    while (err == MDBX_SUCCESS);
    assert(err == ERROR_NOT_LOCKED ||
           (globals.running_under_Wine && err == ERROR_LOCK_VIOLATION));
    SetLastError(ERROR_SUCCESS);
  }
}

/* Seize state as 'exclusive-write' (E-E and returns MDBX_RESULT_TRUE)
 * or as 'used' (S-? and returns MDBX_RESULT_FALSE).
 * Otherwise returns an error. */
static int internal_seize_lck(HANDLE lfd) {
  assert(lfd != INVALID_HANDLE_VALUE);

  /* 1) now on ?-? (free), get ?-E (middle) */
  jitter4testing(false);
  int rc = flock(lfd, LCK_EXCLUSIVE | LCK_WAITFOR, LCK_UPPER);
  if (rc != MDBX_SUCCESS) {
    /* 2) something went wrong, give up */;
    ERROR("%s, err %u", "?-?(free) >> ?-E(middle)", rc);
    return rc;
  }

  /* 3) now on ?-E (middle), try E-E (exclusive-write) */
  jitter4testing(false);
  rc = flock(lfd, LCK_EXCLUSIVE | LCK_DONTWAIT, LCK_LOWER);
  if (rc == MDBX_SUCCESS)
    return MDBX_RESULT_TRUE /* 4) got E-E (exclusive-write), done */;

  /* 5) still on ?-E (middle) */
  jitter4testing(false);
  if (rc != ERROR_SHARING_VIOLATION && rc != ERROR_LOCK_VIOLATION) {
    /* 6) something went wrong, give up */
    rc = funlock(lfd, LCK_UPPER);
    if (rc != MDBX_SUCCESS)
      mdbx_panic("%s(%s) failed: err %u", __func__, "?-E(middle) >> ?-?(free)",
                 rc);
    return rc;
  }

  /* 7) still on ?-E (middle), try S-E (locked) */
  jitter4testing(false);
  rc = flock(lfd, LCK_SHARED | LCK_DONTWAIT, LCK_LOWER);

  jitter4testing(false);
  if (rc != MDBX_SUCCESS)
    ERROR("%s, err %u", "?-E(middle) >> S-E(locked)", rc);

  /* 8) now on S-E (locked) or still on ?-E (middle),
   *    transition to S-? (used) or ?-? (free) */
  int err = funlock(lfd, LCK_UPPER);
  if (err != MDBX_SUCCESS)
    mdbx_panic("%s(%s) failed: err %u", __func__,
               "X-E(locked/middle) >> X-?(used/free)", err);

  /* 9) now on S-? (used, DONE) or ?-? (free, FAILURE) */
  return rc;
}

MDBX_INTERNAL int lck_seize(MDBX_env *env) {
  const HANDLE fd4data =
      env->ioring.overlapped_fd ? env->ioring.overlapped_fd : env->lazy_fd;
  assert(fd4data != INVALID_HANDLE_VALUE);
  if (env->flags & MDBX_EXCLUSIVE)
    return MDBX_RESULT_TRUE /* nope since files were must be opened
                               non-shareable */
        ;

  if (env->lck_mmap.fd == INVALID_HANDLE_VALUE) {
    /* LY: without-lck mode (e.g. on read-only filesystem) */
    jitter4testing(false);
    int rc = flock_data(env, LCK_SHARED | LCK_DONTWAIT, DXB_WHOLE);
    if (rc != MDBX_SUCCESS)
      ERROR("%s, err %u", "without-lck", rc);
    return rc;
  }

  int rc = internal_seize_lck(env->lck_mmap.fd);
  jitter4testing(false);
  if (rc == MDBX_RESULT_TRUE && (env->flags & MDBX_RDONLY) == 0) {
    /* Check that another process don't operates in without-lck mode.
     * Doing such check by exclusive locking the body-part of db. Should be
     * noted:
     *  - we need an exclusive lock for do so;
     *  - we can't lock meta-pages, otherwise other process could get an error
     *    while opening db in valid (non-conflict) mode. */
    int err = flock_data(env, LCK_EXCLUSIVE | LCK_DONTWAIT, DXB_WHOLE);
    if (err != MDBX_SUCCESS) {
      ERROR("%s, err %u", "lock-against-without-lck", err);
      jitter4testing(false);
      lck_unlock(env);
      return err;
    }
    jitter4testing(false);
    err = funlock(fd4data, DXB_WHOLE);
    if (err != MDBX_SUCCESS)
      mdbx_panic("%s(%s) failed: err %u", __func__,
                 "unlock-against-without-lck", err);
  }

  return rc;
}

MDBX_INTERNAL int lck_downgrade(MDBX_env *env) {
  const HANDLE fd4data =
      env->ioring.overlapped_fd ? env->ioring.overlapped_fd : env->lazy_fd;
  /* Transite from exclusive-write state (E-E) to used (S-?) */
  assert(fd4data != INVALID_HANDLE_VALUE);
  assert(env->lck_mmap.fd != INVALID_HANDLE_VALUE);

  if (env->flags & MDBX_EXCLUSIVE)
    return MDBX_SUCCESS /* nope since files were must be opened non-shareable */
        ;
  /* 1) now at E-E (exclusive-write), transition to ?_E (middle) */
  int rc = funlock(env->lck_mmap.fd, LCK_LOWER);
  if (rc != MDBX_SUCCESS)
    mdbx_panic("%s(%s) failed: err %u", __func__,
               "E-E(exclusive-write) >> ?-E(middle)", rc);

  /* 2) now at ?-E (middle), transition to S-E (locked) */
  rc = flock(env->lck_mmap.fd, LCK_SHARED | LCK_DONTWAIT, LCK_LOWER);
  if (rc != MDBX_SUCCESS) {
    /* 3) something went wrong, give up */;
    ERROR("%s, err %u", "?-E(middle) >> S-E(locked)", rc);
    return rc;
  }

  /* 4) got S-E (locked), continue transition to S-? (used) */
  rc = funlock(env->lck_mmap.fd, LCK_UPPER);
  if (rc != MDBX_SUCCESS)
    mdbx_panic("%s(%s) failed: err %u", __func__, "S-E(locked) >> S-?(used)",
               rc);

  return MDBX_SUCCESS /* 5) now at S-? (used), done */;
}

MDBX_INTERNAL int lck_upgrade(MDBX_env *env, bool dont_wait) {
  /* Transite from used state (S-?) to exclusive-write (E-E) */
  assert(env->lck_mmap.fd != INVALID_HANDLE_VALUE);

  if (env->flags & MDBX_EXCLUSIVE)
    return MDBX_SUCCESS /* nope since files were must be opened non-shareable */
        ;

  /* 1) now on S-? (used), try S-E (locked) */
  jitter4testing(false);
  int rc = flock(env->lck_mmap.fd,
                 dont_wait ? LCK_EXCLUSIVE | LCK_DONTWAIT : LCK_EXCLUSIVE,
                 LCK_UPPER);
  if (rc != MDBX_SUCCESS) {
    /* 2) something went wrong, give up */;
    VERBOSE("%s, err %u", "S-?(used) >> S-E(locked)", rc);
    return rc;
  }

  /* 3) now on S-E (locked), transition to ?-E (middle) */
  rc = funlock(env->lck_mmap.fd, LCK_LOWER);
  if (rc != MDBX_SUCCESS)
    mdbx_panic("%s(%s) failed: err %u", __func__, "S-E(locked) >> ?-E(middle)",
               rc);

  /* 4) now on ?-E (middle), try E-E (exclusive-write) */
  jitter4testing(false);
  rc = flock(env->lck_mmap.fd,
             dont_wait ? LCK_EXCLUSIVE | LCK_DONTWAIT : LCK_EXCLUSIVE,
             LCK_LOWER);
  if (rc != MDBX_SUCCESS) {
    /* 5) something went wrong, give up */;
    VERBOSE("%s, err %u", "?-E(middle) >> E-E(exclusive-write)", rc);
    return rc;
  }

  return MDBX_SUCCESS /* 6) now at E-E (exclusive-write), done */;
}

MDBX_INTERNAL int lck_init(MDBX_env *env, MDBX_env *inprocess_neighbor,
                           int global_uniqueness_flag) {
  (void)env;
  (void)inprocess_neighbor;
  (void)global_uniqueness_flag;
  if (imports.SetFileIoOverlappedRange && !(env->flags & MDBX_RDONLY)) {
    HANDLE token = INVALID_HANDLE_VALUE;
    TOKEN_PRIVILEGES privileges;
    privileges.PrivilegeCount = 1;
    privileges.Privileges[0].Attributes = SE_PRIVILEGE_ENABLED;
    if (!OpenProcessToken(GetCurrentProcess(), TOKEN_ADJUST_PRIVILEGES,
                          &token) ||
        !LookupPrivilegeValue(nullptr, SE_LOCK_MEMORY_NAME,
                              &privileges.Privileges[0].Luid) ||
        !AdjustTokenPrivileges(token, FALSE, &privileges, sizeof(privileges),
                               nullptr, nullptr) ||
        GetLastError() != ERROR_SUCCESS)
      imports.SetFileIoOverlappedRange = nullptr;

    if (token != INVALID_HANDLE_VALUE)
      CloseHandle(token);
  }
  return MDBX_SUCCESS;
}

MDBX_INTERNAL int lck_destroy(MDBX_env *env, MDBX_env *inprocess_neighbor,
                              const uint32_t current_pid) {
  (void)current_pid;
  /* LY: should unmap before releasing the locks to avoid race condition and
   * STATUS_USER_MAPPED_FILE/ERROR_USER_MAPPED_FILE */
  if (env->dxb_mmap.base)
    osal_munmap(&env->dxb_mmap);
  if (env->lck_mmap.lck) {
    const bool synced = env->lck_mmap.lck->unsynced_pages.weak == 0;
    osal_munmap(&env->lck_mmap);
    if (synced && !inprocess_neighbor &&
        env->lck_mmap.fd != INVALID_HANDLE_VALUE &&
        lck_upgrade(env, true) == MDBX_SUCCESS)
      /* this will fail if LCK is used/mmapped by other process(es) */
      osal_ftruncate(env->lck_mmap.fd, 0);
  }
  lck_unlock(env);
  return MDBX_SUCCESS;
}

/*----------------------------------------------------------------------------*/
/* reader checking (by pid) */

MDBX_INTERNAL int lck_rpid_set(MDBX_env *env) {
  (void)env;
  return MDBX_SUCCESS;
}

MDBX_INTERNAL int lck_rpid_clear(MDBX_env *env) {
  (void)env;
  return MDBX_SUCCESS;
}

/* Checks reader by pid.
 *
 * Returns:
 *   MDBX_RESULT_TRUE, if pid is live (unable to acquire lock)
 *   MDBX_RESULT_FALSE, if pid is dead (lock acquired)
 *   or otherwise the errcode. */
MDBX_INTERNAL int lck_rpid_check(MDBX_env *env, uint32_t pid) {
  (void)env;
  HANDLE hProcess = OpenProcess(SYNCHRONIZE, FALSE, pid);
  int rc;
  if (likely(hProcess)) {
    rc = WaitForSingleObject(hProcess, 0);
    if (unlikely(rc == (int)WAIT_FAILED))
      rc = (int)GetLastError();
    CloseHandle(hProcess);
  } else {
    rc = (int)GetLastError();
  }

  switch (rc) {
  case ERROR_INVALID_PARAMETER:
    /* pid seems invalid */
    return MDBX_RESULT_FALSE;
  case WAIT_OBJECT_0:
    /* process just exited */
    return MDBX_RESULT_FALSE;
  case ERROR_ACCESS_DENIED:
    /* The ERROR_ACCESS_DENIED would be returned for CSRSS-processes, etc.
     * assume pid exists */
    return MDBX_RESULT_TRUE;
  case WAIT_TIMEOUT:
    /* pid running */
    return MDBX_RESULT_TRUE;
  default:
    /* failure */
    return rc;
  }
}

#endif /* Windows */
