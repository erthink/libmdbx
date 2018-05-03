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

/* PREAMBLE FOR WINDOWS:
 *
 * We are not concerned for performance here.
 * If you are running Windows a performance could NOT be the goal.
 * Otherwise please use Linux.
 *
 * Regards,
 * LY
 */

/*----------------------------------------------------------------------------*/
/* rthc */

static void NTAPI tls_callback(PVOID module, DWORD reason, PVOID reserved) {
  (void)reserved;
  switch (reason) {
  case DLL_PROCESS_ATTACH:
    mdbx_rthc_global_init();
    break;
  case DLL_PROCESS_DETACH:
    mdbx_rthc_global_dtor();
    break;

  case DLL_THREAD_ATTACH:
    break;
  case DLL_THREAD_DETACH:
    mdbx_rthc_thread_dtor(module);
    break;
  }
}

/* *INDENT-OFF* */
/* clang-format off */
#if defined(_MSC_VER)
#  pragma const_seg(push)
#  pragma data_seg(push)

#  ifdef _WIN64
     /* kick a linker to create the TLS directory if not already done */
#    pragma comment(linker, "/INCLUDE:_tls_used")
     /* Force some symbol references. */
#    pragma comment(linker, "/INCLUDE:mdbx_tls_callback")
     /* specific const-segment for WIN64 */
#    pragma const_seg(".CRT$XLB")
     const
#  else
     /* kick a linker to create the TLS directory if not already done */
#    pragma comment(linker, "/INCLUDE:__tls_used")
     /* Force some symbol references. */
#    pragma comment(linker, "/INCLUDE:_mdbx_tls_callback")
     /* specific data-segment for WIN32 */
#    pragma data_seg(".CRT$XLB")
#  endif

   PIMAGE_TLS_CALLBACK mdbx_tls_callback = tls_callback;
#  pragma data_seg(pop)
#  pragma const_seg(pop)

#elif defined(__GNUC__)
#  ifdef _WIN64
     const
#  endif
   PIMAGE_TLS_CALLBACK mdbx_tls_callback __attribute__((section(".CRT$XLB"), used))
     = tls_callback;
#else
#  error FIXME
#endif
/* *INDENT-ON* */
/* clang-format on */

/*----------------------------------------------------------------------------*/

#define LCK_SHARED 0
#define LCK_EXCLUSIVE LOCKFILE_EXCLUSIVE_LOCK
#define LCK_WAITFOR 0
#define LCK_DONTWAIT LOCKFILE_FAIL_IMMEDIATELY

static __inline BOOL flock(mdbx_filehandle_t fd, DWORD flags, uint64_t offset,
                           size_t bytes) {
  OVERLAPPED ov;
  ov.hEvent = 0;
  ov.Offset = (DWORD)offset;
  ov.OffsetHigh = HIGH_DWORD(offset);
  return LockFileEx(fd, flags, 0, (DWORD)bytes, HIGH_DWORD(bytes), &ov);
}

static __inline BOOL funlock(mdbx_filehandle_t fd, uint64_t offset,
                             size_t bytes) {
  return UnlockFile(fd, (DWORD)offset, HIGH_DWORD(offset), (DWORD)bytes,
                    HIGH_DWORD(bytes));
}

/*----------------------------------------------------------------------------*/
/* global `write` lock for write-txt processing,
 * exclusive locking both meta-pages) */

#define LCK_MAXLEN (1u + (size_t)(MAXSSIZE_T))
#define LCK_META_OFFSET 0
#define LCK_META_LEN 0x10000u
#define LCK_BODY_OFFSET LCK_META_LEN
#define LCK_BODY_LEN (LCK_MAXLEN - LCK_BODY_OFFSET)
#define LCK_META LCK_META_OFFSET, LCK_META_LEN
#define LCK_BODY LCK_BODY_OFFSET, LCK_BODY_LEN
#define LCK_WHOLE 0, LCK_MAXLEN

int mdbx_txn_lock(MDBX_env *env, bool dontwait) {
  if (dontwait) {
    if (!TryEnterCriticalSection(&env->me_windowsbug_lock))
      return MDBX_BUSY;
  } else {
    EnterCriticalSection(&env->me_windowsbug_lock);
  }

  if (flock(env->me_fd, dontwait ? (LCK_EXCLUSIVE | LCK_DONTWAIT)
                                 : (LCK_EXCLUSIVE | LCK_WAITFOR),
            LCK_BODY))
    return MDBX_SUCCESS;
  int rc = GetLastError();
  LeaveCriticalSection(&env->me_windowsbug_lock);
  return (!dontwait || rc != ERROR_LOCK_VIOLATION) ? rc : MDBX_BUSY;
}

void mdbx_txn_unlock(MDBX_env *env) {
  int rc = funlock(env->me_fd, LCK_BODY);
  LeaveCriticalSection(&env->me_windowsbug_lock);
  if (!rc)
    mdbx_panic("%s failed: errcode %u", mdbx_func_, GetLastError());
}

/*----------------------------------------------------------------------------*/
/* global `read` lock for readers registration,
 * exclusive locking `mti_numreaders` (second) cacheline */

#define LCK_LO_OFFSET 0
#define LCK_LO_LEN offsetof(MDBX_lockinfo, mti_numreaders)
#define LCK_UP_OFFSET LCK_LO_LEN
#define LCK_UP_LEN (MDBX_LOCKINFO_WHOLE_SIZE - LCK_UP_OFFSET)
#define LCK_LOWER LCK_LO_OFFSET, LCK_LO_LEN
#define LCK_UPPER LCK_UP_OFFSET, LCK_UP_LEN

int mdbx_rdt_lock(MDBX_env *env) {
  AcquireSRWLockShared(&env->me_remap_guard);
  if (env->me_lfd == INVALID_HANDLE_VALUE)
    return MDBX_SUCCESS; /* readonly database in readonly filesystem */

  /* transite from S-? (used) to S-E (locked), e.g. exclusive lock upper-part */
  if (flock(env->me_lfd, LCK_EXCLUSIVE | LCK_WAITFOR, LCK_UPPER))
    return MDBX_SUCCESS;

  int rc = GetLastError();
  ReleaseSRWLockShared(&env->me_remap_guard);
  return rc;
}

void mdbx_rdt_unlock(MDBX_env *env) {
  if (env->me_lfd != INVALID_HANDLE_VALUE) {
    /* transite from S-E (locked) to S-? (used), e.g. unlock upper-part */
    if (!funlock(env->me_lfd, LCK_UPPER))
      mdbx_panic("%s failed: errcode %u", mdbx_func_, GetLastError());
  }
  ReleaseSRWLockShared(&env->me_remap_guard);
}

static int suspend_and_append(mdbx_handle_array_t **array,
                              const DWORD ThreadId) {
  const unsigned limit = (*array)->limit;
  if ((*array)->count == limit) {
    void *ptr = realloc((limit > ARRAY_LENGTH((*array)->handles))
                            ? *array
                            : /* don't free initial array on the stack */ NULL,
                        sizeof(mdbx_handle_array_t) +
                            sizeof(HANDLE) *
                                (limit * 2 - ARRAY_LENGTH((*array)->handles)));
    if (!ptr)
      return MDBX_ENOMEM;
    (*array) = (mdbx_handle_array_t *)ptr;
    (*array)->limit = limit * 2;
  }

  HANDLE hThread = OpenThread(THREAD_SUSPEND_RESUME, FALSE, ThreadId);
  if (hThread == NULL)
    return GetLastError();
  if (SuspendThread(hThread) == -1) {
    CloseHandle(hThread);
    return GetLastError();
  }

  (*array)->handles[(*array)->count++] = hThread;
  return MDBX_SUCCESS;
}

int mdbx_suspend_threads_before_remap(MDBX_env *env,
                                      mdbx_handle_array_t **array) {
  const mdbx_pid_t CurrentTid = GetCurrentThreadId();
  int rc;
  if (env->me_lck) {
    /* Scan LCK for threads of the current process */
    const MDBX_reader *const begin = env->me_lck->mti_readers;
    const MDBX_reader *const end = begin + env->me_lck->mti_numreaders;
    const mdbx_tid_t WriteTxnOwner = env->me_txn0 ? env->me_txn0->mt_owner : 0;
    for (const MDBX_reader *reader = begin; reader < end; ++reader) {
      if (reader->mr_pid != env->me_pid || !reader->mr_tid) {
      skip_lck:
        continue;
      }
      if (reader->mr_tid == CurrentTid || reader->mr_tid == WriteTxnOwner)
        goto skip_lck;
      if (env->me_flags & MDBX_NOTLS) {
        /* Skip duplicates in no-tls mode */
        for (const MDBX_reader *scan = reader; --scan >= begin;)
          if (scan->mr_tid == reader->mr_tid)
            goto skip_lck;
      }

      rc = suspend_and_append(array, reader->mr_tid);
      if (rc != MDBX_SUCCESS) {
      bailout_lck:
        (void)mdbx_resume_threads_after_remap(*array);
        return rc;
      }
    }
    if (WriteTxnOwner && WriteTxnOwner != CurrentTid) {
      rc = suspend_and_append(array, WriteTxnOwner);
      if (rc != MDBX_SUCCESS)
        goto bailout_lck;
    }
  } else {
    /* Without LCK (i.e. read-only mode).
     * Walk thougth a snapshot of all running threads */
    mdbx_assert(env, env->me_txn0 == NULL);
    const HANDLE hSnapshot = CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0);
    if (hSnapshot == INVALID_HANDLE_VALUE)
      return GetLastError();

    THREADENTRY32 entry;
    entry.dwSize = sizeof(THREADENTRY32);

    if (!Thread32First(hSnapshot, &entry)) {
      rc = GetLastError();
    bailout_toolhelp:
      CloseHandle(hSnapshot);
      (void)mdbx_resume_threads_after_remap(*array);
      return rc;
    }

    do {
      if (entry.th32OwnerProcessID != env->me_pid ||
          entry.th32ThreadID == CurrentTid)
        continue;

      rc = suspend_and_append(array, entry.th32ThreadID);
      if (rc != MDBX_SUCCESS)
        goto bailout_toolhelp;

    } while (Thread32Next(hSnapshot, &entry));

    rc = GetLastError();
    if (rc != ERROR_NO_MORE_FILES)
      goto bailout_toolhelp;
    CloseHandle(hSnapshot);
  }

  return MDBX_SUCCESS;
}

int mdbx_resume_threads_after_remap(mdbx_handle_array_t *array) {
  int rc = MDBX_SUCCESS;
  for (unsigned i = 0; i < array->count; ++i) {
    if (ResumeThread(array->handles[i]) == -1)
      rc = GetLastError();
    CloseHandle(array->handles[i]);
  }
  return rc;
}

/*----------------------------------------------------------------------------*/
/* global `initial` lock for lockfile initialization,
 * exclusive/shared locking first cacheline */

/* FIXME: locking schema/algo descritpion.
 ?-?  = free
 S-?  = used
 E-?  = exclusive-read
 ?-S
 ?-E  = middle
 S-S
 S-E  = locked
 E-S
 E-E  = exclusive-write
*/

int mdbx_lck_init(MDBX_env *env) {
  (void)env;
  return MDBX_SUCCESS;
}

/* Seize state as 'exclusive-write' (E-E and returns MDBX_RESULT_TRUE)
 * or as 'used' (S-? and returns MDBX_RESULT_FALSE), otherwise returns an error
 */
static int internal_seize_lck(HANDLE lfd) {
  int rc;
  assert(lfd != INVALID_HANDLE_VALUE);

  /* 1) now on ?-? (free), get ?-E (middle) */
  mdbx_jitter4testing(false);
  if (!flock(lfd, LCK_EXCLUSIVE | LCK_WAITFOR, LCK_UPPER)) {
    rc = GetLastError() /* 2) something went wrong, give up */;
    mdbx_error("%s(%s) failed: errcode %u", mdbx_func_,
               "?-?(free) >> ?-E(middle)", rc);
    return rc;
  }

  /* 3) now on ?-E (middle), try E-E (exclusive-write) */
  mdbx_jitter4testing(false);
  if (flock(lfd, LCK_EXCLUSIVE | LCK_DONTWAIT, LCK_LOWER))
    return MDBX_RESULT_TRUE /* 4) got E-E (exclusive-write), done */;

  /* 5) still on ?-E (middle) */
  rc = GetLastError();
  mdbx_jitter4testing(false);
  if (rc != ERROR_SHARING_VIOLATION && rc != ERROR_LOCK_VIOLATION) {
    /* 6) something went wrong, give up */
    if (!funlock(lfd, LCK_UPPER))
      mdbx_panic("%s(%s) failed: errcode %u", mdbx_func_,
                 "?-E(middle) >> ?-?(free)", GetLastError());
    return rc;
  }

  /* 7) still on ?-E (middle), try S-E (locked) */
  mdbx_jitter4testing(false);
  rc = flock(lfd, LCK_SHARED | LCK_DONTWAIT, LCK_LOWER) ? MDBX_RESULT_FALSE
                                                        : GetLastError();

  mdbx_jitter4testing(false);
  if (rc != MDBX_RESULT_FALSE)
    mdbx_error("%s(%s) failed: errcode %u", mdbx_func_,
               "?-E(middle) >> S-E(locked)", rc);

  /* 8) now on S-E (locked) or still on ?-E (middle),
  *    transite to S-? (used) or ?-? (free) */
  if (!funlock(lfd, LCK_UPPER))
    mdbx_panic("%s(%s) failed: errcode %u", mdbx_func_,
               "X-E(locked/middle) >> X-?(used/free)", GetLastError());

  /* 9) now on S-? (used, DONE) or ?-? (free, FAILURE) */
  return rc;
}

int mdbx_lck_seize(MDBX_env *env) {
  int rc;

  assert(env->me_fd != INVALID_HANDLE_VALUE);
  if (env->me_lfd == INVALID_HANDLE_VALUE) {
    /* LY: without-lck mode (e.g. on read-only filesystem) */
    mdbx_jitter4testing(false);
    if (!flock(env->me_fd, LCK_SHARED | LCK_DONTWAIT, LCK_WHOLE)) {
      rc = GetLastError();
      mdbx_error("%s(%s) failed: errcode %u", mdbx_func_, "without-lck", rc);
      return rc;
    }
    return MDBX_RESULT_FALSE;
  }

  rc = internal_seize_lck(env->me_lfd);
  mdbx_jitter4testing(false);
  if (rc == MDBX_RESULT_TRUE && (env->me_flags & MDBX_RDONLY) == 0) {
    /* Check that another process don't operates in without-lck mode.
     * Doing such check by exclusive locking the body-part of db. Should be
     * noted:
     *  - we need an exclusive lock for do so;
     *  - we can't lock meta-pages, otherwise other process could get an error
     *    while opening db in valid (non-conflict) mode. */
    if (!flock(env->me_fd, LCK_EXCLUSIVE | LCK_DONTWAIT, LCK_BODY)) {
      rc = GetLastError();
      mdbx_error("%s(%s) failed: errcode %u", mdbx_func_,
                 "lock-against-without-lck", rc);
      mdbx_jitter4testing(false);
      mdbx_lck_destroy(env);
    } else {
      mdbx_jitter4testing(false);
      if (!funlock(env->me_fd, LCK_BODY))
        mdbx_panic("%s(%s) failed: errcode %u", mdbx_func_,
                   "unlock-against-without-lck", GetLastError());
    }
  }

  return rc;
}

int mdbx_lck_downgrade(MDBX_env *env, bool complete) {
  /* Transite from exclusive state (E-?) to used (S-?) */
  assert(env->me_fd != INVALID_HANDLE_VALUE);
  assert(env->me_lfd != INVALID_HANDLE_VALUE);

  /* 1) must be at E-E (exclusive-write) */
  if (!complete) {
    /* transite from E-E to E_? (exclusive-read) */
    if (!funlock(env->me_lfd, LCK_UPPER))
      mdbx_panic("%s(%s) failed: errcode %u", mdbx_func_,
                 "E-E(exclusive-write) >> E-?(exclusive-read)", GetLastError());
    return MDBX_SUCCESS /* 2) now at E-? (exclusive-read), done */;
  }

  /* 3) now at E-E (exclusive-write), transite to ?_E (middle) */
  if (!funlock(env->me_lfd, LCK_LOWER))
    mdbx_panic("%s(%s) failed: errcode %u", mdbx_func_,
               "E-E(exclusive-write) >> ?-E(middle)", GetLastError());

  /* 4) now at ?-E (middle), transite to S-E (locked) */
  if (!flock(env->me_lfd, LCK_SHARED | LCK_DONTWAIT, LCK_LOWER)) {
    int rc = GetLastError() /* 5) something went wrong, give up */;
    mdbx_error("%s(%s) failed: errcode %u", mdbx_func_,
               "?-E(middle) >> S-E(locked)", rc);
    return rc;
  }

  /* 6) got S-E (locked), continue transition to S-? (used) */
  if (!funlock(env->me_lfd, LCK_UPPER))
    mdbx_panic("%s(%s) failed: errcode %u", mdbx_func_,
               "S-E(locked) >> S-?(used)", GetLastError());

  return MDBX_SUCCESS /* 7) now at S-? (used), done */;
}

int mdbx_lck_upgrade(MDBX_env *env) {
  /* Transite from locked state (S-E) to exclusive-write (E-E) */
  assert(env->me_fd != INVALID_HANDLE_VALUE);
  assert(env->me_lfd != INVALID_HANDLE_VALUE);

  /* 1) must be at S-E (locked), transite to ?_E (middle) */
  if (!funlock(env->me_lfd, LCK_LOWER))
    mdbx_panic("%s(%s) failed: errcode %u", mdbx_func_,
               "S-E(locked) >> ?-E(middle)", GetLastError());

  /* 3) now on ?-E (middle), try E-E (exclusive-write) */
  mdbx_jitter4testing(false);
  if (flock(env->me_lfd, LCK_EXCLUSIVE | LCK_DONTWAIT, LCK_LOWER))
    return MDBX_RESULT_TRUE; /* 4) got E-E (exclusive-write), done */

  /* 5) still on ?-E (middle) */
  int rc = GetLastError();
  mdbx_jitter4testing(false);
  if (rc != ERROR_SHARING_VIOLATION && rc != ERROR_LOCK_VIOLATION) {
    /* 6) something went wrong, report but continue */
    mdbx_error("%s(%s) failed: errcode %u", mdbx_func_,
               "?-E(middle) >> E-E(exclusive-write)", rc);
  }

  /* 7) still on ?-E (middle), try restore S-E (locked) */
  mdbx_jitter4testing(false);
  rc = flock(env->me_lfd, LCK_SHARED | LCK_DONTWAIT, LCK_LOWER)
           ? MDBX_RESULT_FALSE
           : GetLastError();

  mdbx_jitter4testing(false);
  if (rc != MDBX_RESULT_FALSE) {
    mdbx_fatal("%s(%s) failed: errcode %u", mdbx_func_,
               "?-E(middle) >> S-E(locked)", rc);
    return rc;
  }

  /* 8) now on S-E (locked) */
  return MDBX_RESULT_FALSE;
}

void mdbx_lck_destroy(MDBX_env *env) {
  int rc;

  if (env->me_lfd != INVALID_HANDLE_VALUE) {
    /* double `unlock` for robustly remove overlapped shared/exclusive locks */
    while (funlock(env->me_lfd, LCK_LOWER))
      ;
    rc = GetLastError();
    assert(rc == ERROR_NOT_LOCKED);
    (void)rc;
    SetLastError(ERROR_SUCCESS);

    while (funlock(env->me_lfd, LCK_UPPER))
      ;
    rc = GetLastError();
    assert(rc == ERROR_NOT_LOCKED);
    (void)rc;
    SetLastError(ERROR_SUCCESS);
  }

  if (env->me_fd != INVALID_HANDLE_VALUE) {
    /* explicitly unlock to avoid latency for other processes (windows kernel
     * releases such locks via deferred queues) */
    while (funlock(env->me_fd, LCK_BODY))
      ;
    rc = GetLastError();
    assert(rc == ERROR_NOT_LOCKED);
    (void)rc;
    SetLastError(ERROR_SUCCESS);

    while (funlock(env->me_fd, LCK_META))
      ;
    rc = GetLastError();
    assert(rc == ERROR_NOT_LOCKED);
    (void)rc;
    SetLastError(ERROR_SUCCESS);

    while (funlock(env->me_fd, LCK_WHOLE))
      ;
    rc = GetLastError();
    assert(rc == ERROR_NOT_LOCKED);
    (void)rc;
    SetLastError(ERROR_SUCCESS);
  }
}

/*----------------------------------------------------------------------------*/
/* reader checking (by pid) */

int mdbx_rpid_set(MDBX_env *env) {
  (void)env;
  return MDBX_SUCCESS;
}

int mdbx_rpid_clear(MDBX_env *env) {
  (void)env;
  return MDBX_SUCCESS;
}

/* Checks reader by pid.
 *
 * Returns:
 *   MDBX_RESULT_TRUE, if pid is live (unable to acquire lock)
 *   MDBX_RESULT_FALSE, if pid is dead (lock acquired)
 *   or otherwise the errcode. */
int mdbx_rpid_check(MDBX_env *env, mdbx_pid_t pid) {
  (void)env;
  HANDLE hProcess = OpenProcess(SYNCHRONIZE, FALSE, pid);
  int rc;
  if (likely(hProcess)) {
    rc = WaitForSingleObject(hProcess, 0);
    if (unlikely(rc == WAIT_FAILED))
      rc = GetLastError();
    CloseHandle(hProcess);
  } else {
    rc = GetLastError();
  }

  switch (rc) {
  case ERROR_INVALID_PARAMETER:
    /* pid seem invalid */
    return MDBX_RESULT_FALSE;
  case WAIT_OBJECT_0:
    /* process just exited */
    return MDBX_RESULT_FALSE;
  case WAIT_TIMEOUT:
    /* pid running */
    return MDBX_RESULT_TRUE;
  default:
    /* failure */
    return rc;
  }
}
