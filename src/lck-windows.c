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

static CRITICAL_SECTION rthc_critical_section;

static void NTAPI tls_callback(PVOID module, DWORD reason, PVOID reserved) {
  (void)module;
  (void)reserved;
  switch (reason) {
  case DLL_PROCESS_ATTACH:
    InitializeCriticalSection(&rthc_critical_section);
    break;
  case DLL_PROCESS_DETACH:
    DeleteCriticalSection(&rthc_critical_section);
    break;

  case DLL_THREAD_ATTACH:
    break;
  case DLL_THREAD_DETACH:
    mdbx_rthc_cleanup();
    break;
  }
}

void mdbx_rthc_lock(void) { EnterCriticalSection(&rthc_critical_section); }

void mdbx_rthc_unlock(void) { LeaveCriticalSection(&rthc_critical_section); }

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

static BOOL flock(mdbx_filehandle_t fd, DWORD flags, off_t offset,
                  size_t bytes) {
  OVERLAPPED ov;
  ov.hEvent = 0;
  ov.Offset = (DWORD)offset;
  ov.OffsetHigh = HIGH_DWORD(offset);

#ifdef MDBX_WINDOWS_UnlockFile_CRUTCH
  if (LockFileEx(fd, flags, 0, (DWORD)bytes, HIGH_DWORD(bytes), &ov))
    return true;

  if ((flags & LOCKFILE_FAIL_IMMEDIATELY) == 0)
    return false;

  int rc = GetLastError();
  if (rc != ERROR_SHARING_VIOLATION && rc != ERROR_LOCK_VIOLATION)
    return false;

  /* FIXME: Windows kernel is ugly and mad... */
  SwitchToThread();
  Sleep(42);
  SwitchToThread();
#endif /* MDBX_WINDOWS_UnlockFile_CRUTCH */
  return LockFileEx(fd, flags, 0, (DWORD)bytes, HIGH_DWORD(bytes), &ov);
}

static BOOL funlock(mdbx_filehandle_t fd, off_t offset, size_t bytes) {
  if (!UnlockFile(fd, (DWORD)offset, HIGH_DWORD(offset), (DWORD)bytes,
                  HIGH_DWORD(bytes)))
    return false;

#ifdef MDBX_WINDOWS_UnlockFile_CRUTCH
  /* FIXME: Windows kernel is ugly and mad... */
  SwitchToThread();
  Sleep(42);
  SwitchToThread();
#endif /* MDBX_WINDOWS_UnlockFile_CRUTCH */
  return true;
}

/*----------------------------------------------------------------------------*/
/* global `write` lock for write-txt processing,
 * exclusive locking both meta-pages) */

int mdbx_txn_lock(MDB_env *env) {
  if (flock(env->me_fd, LCK_EXCLUSIVE | LCK_WAITFOR, 0, env->me_psize * 2))
    return MDB_SUCCESS;
  return GetLastError();
}

void mdbx_txn_unlock(MDB_env *env) {
  if (!funlock(env->me_fd, 0, env->me_psize * 2))
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

int mdbx_rdt_lock(MDB_env *env) {
  if (env->me_lfd == INVALID_HANDLE_VALUE)
    return MDB_SUCCESS; /* readonly database in readonly filesystem */

  /* transite from S-? (used) to S-E (locked), e.g. exlcusive lock upper-part */
  if (flock(env->me_lfd, LCK_EXCLUSIVE | LCK_WAITFOR, LCK_UPPER))
    return MDB_SUCCESS;
  return GetLastError();
}

void mdbx_rdt_unlock(MDB_env *env) {
  if (env->me_lfd != INVALID_HANDLE_VALUE) {
    /* transite from S-E (locked) to S-? (used), e.g. unlock upper-part */
    if (!funlock(env->me_lfd, LCK_UPPER))
      mdbx_panic("%s failed: errcode %u", mdbx_func_, GetLastError());
  }
}

/*----------------------------------------------------------------------------*/
/* global `initial` lock for lockfile initialization,
*  exclusive/shared locking first cacheline */

/* FIXME: locking scheme/algo descritpion.
 ?-?  = free
 S-?  = used
 E-?
 ?-S
 ?-E  = middle
 S-S
 S-E  = locked
 E-S
 E-E  = exclusive
*/

int mdbx_lck_init(MDB_env *env) {
  (void)env;
  return MDB_SUCCESS;
}

/* Seize state as exclusive (E-E and returns MDBX_RESULT_TRUE)
 * or used (S-? and returns MDBX_RESULT_FALSE), otherwise returns an error */
int mdbx_lck_seize(MDB_env *env) {
  /* 1) now on ?-? (free), get ?-E (middle) */
  if (!flock(env->me_lfd, LCK_EXCLUSIVE | LCK_WAITFOR, LCK_UPPER))
    return GetLastError() /* 2) something went wrong, give up */;

  /* 3) now on ?-E (middle), try E-E (exclusive) */
  if (flock(env->me_lfd, LCK_EXCLUSIVE | LCK_DONTWAIT, LCK_LOWER))
    return MDBX_RESULT_TRUE; /* 4) got E-E (exclusive), done */

  /* 5) still on ?-E (middle) */
  int rc = GetLastError();
  if (rc != ERROR_SHARING_VIOLATION && rc != ERROR_LOCK_VIOLATION) {
    /* 6) something went wrong, give up */
    if (!funlock(env->me_lfd, LCK_UPPER)) {
      rc = GetLastError();
      mdbx_panic("%s(%s) failed: errcode %u", mdbx_func_,
                 "?-E(middle) >> ?-?(free)", rc);
    }
    return rc;
  }

  /* 7) still on ?-E (middle), try S-E (locked) */
  rc = flock(env->me_lfd, LCK_EXCLUSIVE | LCK_DONTWAIT, LCK_LOWER)
           ? MDBX_RESULT_FALSE
           : GetLastError();

  /* 8) now on S-E (locked) or still on ?-E (middle),
   *    transite to S-? (used) or ?-? (free) */
  if (!funlock(env->me_lfd, LCK_UPPER)) {
    rc = GetLastError();
    mdbx_panic("%s(%s) failed: errcode %u", mdbx_func_,
               "X-E(locked/middle) >> X-?(used/free)", rc);
  }

  /* 9) now on S-? (used, DONE) or ?-? (free, FAILURE) */
  return rc;
}

/* Transite from exclusive state (E-E) to used (S-?) */
int mdbx_lck_downgrade(MDB_env *env) {
  int rc;

  /* 1) now at E-E (exclusive), continue transition to ?_E (middle) */
  if (!funlock(env->me_lfd, LCK_LOWER)) {
    rc = GetLastError();
    mdbx_panic("%s(%s) failed: errcode %u", mdbx_func_,
               "E-E(exclusive) >> ?-E(middle)", rc);
  }

  /* 2) now at ?-E (middle), transite to S-E (locked) */
  if (!flock(env->me_lfd, LCK_SHARED | LCK_DONTWAIT, LCK_LOWER)) {
    rc = GetLastError() /* 3) something went wrong, give up */;
    return rc;
  }

  /* 4) got S-E (locked), continue transition to S-? (used) */
  if (!funlock(env->me_lfd, LCK_UPPER)) {
    rc = GetLastError();
    mdbx_panic("%s(%s) failed: errcode %u", mdbx_func_,
               "S-E(locked) >> S-?(used)", rc);
  }
  return MDB_SUCCESS /* 5) now at S-? (used), done */;
}

void mdbx_lck_destroy(MDB_env *env) {
  int rc;

  if (env->me_fd != INVALID_HANDLE_VALUE) {
    /* explicitly unlock to avoid latency for other processes (windows kernel
     * releases such locks via deferred queues) */
    while (funlock(env->me_fd, 0, env->me_psize * 2))
      ;
    rc = GetLastError();
    assert(rc == ERROR_NOT_LOCKED);
    (void)rc;
    SetLastError(ERROR_SUCCESS);
  }

  if (env->me_lfd != INVALID_HANDLE_VALUE) {
    /* double `unlock` for robustly remove overlapped shared/exclusive locks */
    while (funlock(env->me_lfd, LCK_LOWER))
      ;
    assert(rc == ERROR_NOT_LOCKED);
    (void)rc;
    SetLastError(ERROR_SUCCESS);

    while (funlock(env->me_lfd, LCK_UPPER))
      ;
    assert(rc == ERROR_NOT_LOCKED);
    (void)rc;
    SetLastError(ERROR_SUCCESS);
  }
}

/*----------------------------------------------------------------------------*/
/* reader checking (by pid) */

int mdbx_rpid_set(MDB_env *env) {
  (void)env;
  return MDB_SUCCESS;
}

int mdbx_rpid_clear(MDB_env *env) {
  (void)env;
  return MDB_SUCCESS;
}

int mdbx_rpid_check(MDB_env *env, mdbx_pid_t pid) {
  (void)env;
  HANDLE hProcess = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, FALSE, pid);
  int rc;
  if (hProcess) {
    rc = WaitForSingleObject(hProcess, 0);
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
