/* https://en.wikipedia.org/wiki/Operating_system_abstraction_layer */

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

#if defined(_WIN32) || defined(_WIN64)

#include <winioctl.h>

static int waitstatus2errcode(DWORD result) {
  switch (result) {
  case WAIT_OBJECT_0:
    return MDBX_SUCCESS;
  case WAIT_FAILED:
    return GetLastError();
  case WAIT_ABANDONED:
    return ERROR_ABANDONED_WAIT_0;
  case WAIT_IO_COMPLETION:
    return ERROR_USER_APC;
  case WAIT_TIMEOUT:
    return ERROR_TIMEOUT;
  default:
    return ERROR_UNHANDLED_ERROR;
  }
}

/* Map a result from an NTAPI call to WIN32 error code. */
static int ntstatus2errcode(NTSTATUS status) {
  DWORD dummy;
  OVERLAPPED ov;
  memset(&ov, 0, sizeof(ov));
  ov.Internal = status;
  return GetOverlappedResult(NULL, &ov, &dummy, FALSE) ? MDBX_SUCCESS
                                                       : GetLastError();
}

/* We use native NT APIs to setup the memory map, so that we can
 * let the DB file grow incrementally instead of always preallocating
 * the full size. These APIs are defined in <wdm.h> and <ntifs.h>
 * but those headers are meant for driver-level development and
 * conflict with the regular user-level headers, so we explicitly
 * declare them here. Using these APIs also means we must link to
 * ntdll.dll, which is not linked by default in user code. */

extern NTSTATUS NTAPI NtCreateSection(
    OUT PHANDLE SectionHandle, IN ACCESS_MASK DesiredAccess,
    IN OPTIONAL POBJECT_ATTRIBUTES ObjectAttributes,
    IN OPTIONAL PLARGE_INTEGER MaximumSize, IN ULONG SectionPageProtection,
    IN ULONG AllocationAttributes, IN OPTIONAL HANDLE FileHandle);

typedef struct _SECTION_BASIC_INFORMATION {
  ULONG Unknown;
  ULONG SectionAttributes;
  LARGE_INTEGER SectionSize;
} SECTION_BASIC_INFORMATION, *PSECTION_BASIC_INFORMATION;

typedef enum _SECTION_INFORMATION_CLASS {
  SectionBasicInformation,
  SectionImageInformation,
  SectionRelocationInformation, // name:wow64:whNtQuerySection_SectionRelocationInformation
  MaxSectionInfoClass
} SECTION_INFORMATION_CLASS;

extern NTSTATUS NTAPI NtQuerySection(
    IN HANDLE SectionHandle, IN SECTION_INFORMATION_CLASS InformationClass,
    OUT PVOID InformationBuffer, IN ULONG InformationBufferSize,
    OUT PULONG ResultLength OPTIONAL);

extern NTSTATUS NTAPI NtExtendSection(IN HANDLE SectionHandle,
                                      IN PLARGE_INTEGER NewSectionSize);

typedef enum _SECTION_INHERIT { ViewShare = 1, ViewUnmap = 2 } SECTION_INHERIT;

extern NTSTATUS NTAPI NtMapViewOfSection(
    IN HANDLE SectionHandle, IN HANDLE ProcessHandle, IN OUT PVOID *BaseAddress,
    IN ULONG_PTR ZeroBits, IN SIZE_T CommitSize,
    IN OUT OPTIONAL PLARGE_INTEGER SectionOffset, IN OUT PSIZE_T ViewSize,
    IN SECTION_INHERIT InheritDisposition, IN ULONG AllocationType,
    IN ULONG Win32Protect);

extern NTSTATUS NTAPI NtUnmapViewOfSection(IN HANDLE ProcessHandle,
                                           IN OPTIONAL PVOID BaseAddress);

extern NTSTATUS NTAPI NtClose(HANDLE Handle);

extern NTSTATUS NTAPI NtAllocateVirtualMemory(
    IN HANDLE ProcessHandle, IN OUT PVOID *BaseAddress, IN ULONG_PTR ZeroBits,
    IN OUT PSIZE_T RegionSize, IN ULONG AllocationType, IN ULONG Protect);

extern NTSTATUS NTAPI NtFreeVirtualMemory(IN HANDLE ProcessHandle,
                                          IN PVOID *BaseAddress,
                                          IN OUT PSIZE_T RegionSize,
                                          IN ULONG FreeType);

#ifndef WOF_CURRENT_VERSION
typedef struct _WOF_EXTERNAL_INFO {
  DWORD Version;
  DWORD Provider;
} WOF_EXTERNAL_INFO, *PWOF_EXTERNAL_INFO;
#endif /* WOF_CURRENT_VERSION */

#ifndef WIM_PROVIDER_CURRENT_VERSION
#define WIM_PROVIDER_HASH_SIZE 20

typedef struct _WIM_PROVIDER_EXTERNAL_INFO {
  DWORD Version;
  DWORD Flags;
  LARGE_INTEGER DataSourceId;
  BYTE ResourceHash[WIM_PROVIDER_HASH_SIZE];
} WIM_PROVIDER_EXTERNAL_INFO, *PWIM_PROVIDER_EXTERNAL_INFO;
#endif /* WIM_PROVIDER_CURRENT_VERSION */

#ifndef FILE_PROVIDER_CURRENT_VERSION
typedef struct _FILE_PROVIDER_EXTERNAL_INFO_V1 {
  ULONG Version;
  ULONG Algorithm;
  ULONG Flags;
} FILE_PROVIDER_EXTERNAL_INFO_V1, *PFILE_PROVIDER_EXTERNAL_INFO_V1;
#endif /* FILE_PROVIDER_CURRENT_VERSION */

#ifndef STATUS_OBJECT_NOT_EXTERNALLY_BACKED
#define STATUS_OBJECT_NOT_EXTERNALLY_BACKED ((NTSTATUS)0xC000046DL)
#endif
#ifndef STATUS_INVALID_DEVICE_REQUEST
#define STATUS_INVALID_DEVICE_REQUEST ((NTSTATUS)0xC0000010L)
#endif

#ifndef FILE_DEVICE_FILE_SYSTEM
#define FILE_DEVICE_FILE_SYSTEM 0x00000009
#endif

#ifndef FSCTL_GET_EXTERNAL_BACKING
#define FSCTL_GET_EXTERNAL_BACKING                                             \
  CTL_CODE(FILE_DEVICE_FILE_SYSTEM, 196, METHOD_BUFFERED, FILE_ANY_ACCESS)
#endif

#endif /* _WIN32 || _WIN64 */

/*----------------------------------------------------------------------------*/

#if _POSIX_C_SOURCE > 200212 &&                                                \
    /* workaround for avoid musl libc wrong prototype */ (                     \
        defined(__GLIBC__) || defined(__GNU_LIBRARY__))
/* Prototype should match libc runtime. ISO POSIX (2003) & LSB 1.x-3.x */
__extern_C void __assert_fail(const char *assertion, const char *file,
                              unsigned line, const char *function)
#ifdef __THROW
    __THROW
#else
    __nothrow
#endif /* __THROW */
    __noreturn;

#elif defined(__APPLE__) || defined(__MACH__)
__extern_C void __assert_rtn(const char *function, const char *file, int line,
                             const char *assertion) /* __nothrow */
#ifdef __dead2
    __dead2
#else
    __noreturn
#endif /* __dead2 */
#ifdef __disable_tail_calls
    __disable_tail_calls
#endif /* __disable_tail_calls */
    ;

#define __assert_fail(assertion, file, line, function)                         \
  __assert_rtn(function, file, line, assertion)
#elif defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__) ||   \
    defined(__BSD__) || defined(__NETBSD__) || defined(__bsdi__) ||            \
    defined(__DragonFly__)
__extern_C void __assert(const char *function, const char *file, int line,
                         const char *assertion) /* __nothrow */
#ifdef __dead2
    __dead2
#else
    __noreturn
#endif /* __dead2 */
#ifdef __disable_tail_calls
    __disable_tail_calls
#endif /* __disable_tail_calls */
    ;
#define __assert_fail(assertion, file, line, function)                         \
  __assert(function, file, line, assertion)

#endif /* __assert_fail */

MDBX_INTERNAL_FUNC void __cold mdbx_assert_fail(const MDBX_env *env,
                                                const char *msg,
                                                const char *func, int line) {
#if MDBX_DEBUG
  if (env && env->me_assert_func) {
    env->me_assert_func(env, msg, func, line);
    return;
  }
#else
  (void)env;
#endif /* MDBX_DEBUG */

  if (mdbx_debug_logger)
    mdbx_debug_log(MDBX_LOG_FATAL, func, line, "assert: %s\n", msg);
  else {
#if defined(_WIN32) || defined(_WIN64)
    char *message = nullptr;
    const int num = mdbx_asprintf(&message, "\r\nMDBX-ASSERTION: %s, %s:%u",
                                  msg, func ? func : "unknown", line);
    if (num < 1 || !message)
      message = "<troubles with assertion-message preparation>";
    OutputDebugStringA(message);
    if (IsDebuggerPresent())
      DebugBreak();
#else
    __assert_fail(msg, "mdbx", line, func);
#endif
  }

#if defined(_WIN32) || defined(_WIN64)
  FatalExit(ERROR_UNHANDLED_ERROR);
#else
  abort();
#endif
}

MDBX_INTERNAL_FUNC __cold void mdbx_panic(const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);

  char *message = nullptr;
  const int num = mdbx_vasprintf(&message, fmt, ap);
  va_end(ap);
  const char *const const_message =
      (num < 1 || !message) ? "<troubles with panic-message preparation>"
                            : message;

#if defined(_WIN32) || defined(_WIN64)
  OutputDebugStringA("\r\nMDBX-PANIC: ");
  OutputDebugStringA(const_message);
  if (IsDebuggerPresent())
    DebugBreak();
  FatalExit(ERROR_UNHANDLED_ERROR);
#else
  __assert_fail(const_message, "mdbx", 0, "panic");
  abort();
#endif
}

/*----------------------------------------------------------------------------*/

#ifndef mdbx_vasprintf
MDBX_INTERNAL_FUNC int mdbx_vasprintf(char **strp, const char *fmt,
                                      va_list ap) {
  va_list ones;
  va_copy(ones, ap);
  int needed = vsnprintf(nullptr, 0, fmt, ap);

  if (unlikely(needed < 0 || needed >= INT_MAX)) {
    *strp = nullptr;
    va_end(ones);
    return needed;
  }

  *strp = mdbx_malloc(needed + 1);
  if (unlikely(*strp == nullptr)) {
    va_end(ones);
#if defined(_WIN32) || defined(_WIN64)
    SetLastError(MDBX_ENOMEM);
#else
    errno = MDBX_ENOMEM;
#endif
    return -1;
  }

  int actual = vsnprintf(*strp, needed + 1, fmt, ones);
  va_end(ones);

  assert(actual == needed);
  if (unlikely(actual < 0)) {
    mdbx_free(*strp);
    *strp = nullptr;
  }
  return actual;
}
#endif /* mdbx_vasprintf */

#ifndef mdbx_asprintf
MDBX_INTERNAL_FUNC int mdbx_asprintf(char **strp, const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  int rc = mdbx_vasprintf(strp, fmt, ap);
  va_end(ap);
  return rc;
}
#endif /* mdbx_asprintf */

#ifndef mdbx_memalign_alloc
MDBX_INTERNAL_FUNC int mdbx_memalign_alloc(size_t alignment, size_t bytes,
                                           void **result) {
#if defined(_WIN32) || defined(_WIN64)
  (void)alignment;
  *result = VirtualAlloc(NULL, bytes, MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE);
  return *result ? MDBX_SUCCESS : MDBX_ENOMEM /* ERROR_OUTOFMEMORY */;
#elif defined(_ISOC11_SOURCE)
  *result = aligned_alloc(alignment, bytes);
  return *result ? MDBX_SUCCESS : errno;
#elif _POSIX_VERSION >= 200112L
  *result = nullptr;
  return posix_memalign(result, alignment, bytes);
#elif __GLIBC_PREREQ(2, 16) || __STDC_VERSION__ >= 201112L
  *result = memalign(alignment, bytes);
  return *result ? MDBX_SUCCESS : errno;
#else
#error FIXME
#endif
}
#endif /* mdbx_memalign_alloc */

#ifndef mdbx_memalign_free
MDBX_INTERNAL_FUNC void mdbx_memalign_free(void *ptr) {
#if defined(_WIN32) || defined(_WIN64)
  VirtualFree(ptr, 0, MEM_RELEASE);
#else
  mdbx_free(ptr);
#endif
}
#endif /* mdbx_memalign_free */

#ifndef mdbx_strdup
char *mdbx_strdup(const char *str) {
  if (!str)
    return NULL;
  size_t bytes = strlen(str) + 1;
  char *dup = mdbx_malloc(bytes);
  if (dup)
    memcpy(dup, str, bytes);
  return dup;
}
#endif /* mdbx_strdup */

/*----------------------------------------------------------------------------*/

MDBX_INTERNAL_FUNC int mdbx_condmutex_init(mdbx_condmutex_t *condmutex) {
#if defined(_WIN32) || defined(_WIN64)
  int rc = MDBX_SUCCESS;
  condmutex->event = NULL;
  condmutex->mutex = CreateMutex(NULL, FALSE, NULL);
  if (!condmutex->mutex)
    return GetLastError();

  condmutex->event = CreateEvent(NULL, FALSE, FALSE, NULL);
  if (!condmutex->event) {
    rc = GetLastError();
    (void)CloseHandle(condmutex->mutex);
    condmutex->mutex = NULL;
  }
  return rc;
#else
  memset(condmutex, 0, sizeof(mdbx_condmutex_t));
  int rc = pthread_mutex_init(&condmutex->mutex, NULL);
  if (rc == 0) {
    rc = pthread_cond_init(&condmutex->cond, NULL);
    if (rc != 0)
      (void)pthread_mutex_destroy(&condmutex->mutex);
  }
  return rc;
#endif
}

static bool is_allzeros(const void *ptr, size_t bytes) {
  const uint8_t *u8 = ptr;
  for (size_t i = 0; i < bytes; ++i)
    if (u8[i] != 0)
      return false;
  return true;
}

MDBX_INTERNAL_FUNC int mdbx_condmutex_destroy(mdbx_condmutex_t *condmutex) {
  int rc = MDBX_EINVAL;
#if defined(_WIN32) || defined(_WIN64)
  if (condmutex->event) {
    rc = CloseHandle(condmutex->event) ? MDBX_SUCCESS : GetLastError();
    if (rc == MDBX_SUCCESS)
      condmutex->event = NULL;
  }
  if (condmutex->mutex) {
    rc = CloseHandle(condmutex->mutex) ? MDBX_SUCCESS : GetLastError();
    if (rc == MDBX_SUCCESS)
      condmutex->mutex = NULL;
  }
#else
  if (!is_allzeros(&condmutex->cond, sizeof(condmutex->cond))) {
    rc = pthread_cond_destroy(&condmutex->cond);
    if (rc == 0)
      memset(&condmutex->cond, 0, sizeof(condmutex->cond));
  }
  if (!is_allzeros(&condmutex->mutex, sizeof(condmutex->mutex))) {
    rc = pthread_mutex_destroy(&condmutex->mutex);
    if (rc == 0)
      memset(&condmutex->mutex, 0, sizeof(condmutex->mutex));
  }
#endif
  return rc;
}

MDBX_INTERNAL_FUNC int mdbx_condmutex_lock(mdbx_condmutex_t *condmutex) {
#if defined(_WIN32) || defined(_WIN64)
  DWORD code = WaitForSingleObject(condmutex->mutex, INFINITE);
  return waitstatus2errcode(code);
#else
  return pthread_mutex_lock(&condmutex->mutex);
#endif
}

MDBX_INTERNAL_FUNC int mdbx_condmutex_unlock(mdbx_condmutex_t *condmutex) {
#if defined(_WIN32) || defined(_WIN64)
  return ReleaseMutex(condmutex->mutex) ? MDBX_SUCCESS : GetLastError();
#else
  return pthread_mutex_unlock(&condmutex->mutex);
#endif
}

MDBX_INTERNAL_FUNC int mdbx_condmutex_signal(mdbx_condmutex_t *condmutex) {
#if defined(_WIN32) || defined(_WIN64)
  return SetEvent(condmutex->event) ? MDBX_SUCCESS : GetLastError();
#else
  return pthread_cond_signal(&condmutex->cond);
#endif
}

MDBX_INTERNAL_FUNC int mdbx_condmutex_wait(mdbx_condmutex_t *condmutex) {
#if defined(_WIN32) || defined(_WIN64)
  DWORD code =
      SignalObjectAndWait(condmutex->mutex, condmutex->event, INFINITE, FALSE);
  if (code == WAIT_OBJECT_0)
    code = WaitForSingleObject(condmutex->mutex, INFINITE);
  return waitstatus2errcode(code);
#else
  return pthread_cond_wait(&condmutex->cond, &condmutex->mutex);
#endif
}

/*----------------------------------------------------------------------------*/

MDBX_INTERNAL_FUNC int mdbx_fastmutex_init(mdbx_fastmutex_t *fastmutex) {
#if defined(_WIN32) || defined(_WIN64)
  InitializeCriticalSection(fastmutex);
  return MDBX_SUCCESS;
#else
  return pthread_mutex_init(fastmutex, NULL);
#endif
}

MDBX_INTERNAL_FUNC int mdbx_fastmutex_destroy(mdbx_fastmutex_t *fastmutex) {
#if defined(_WIN32) || defined(_WIN64)
  DeleteCriticalSection(fastmutex);
  return MDBX_SUCCESS;
#else
  return pthread_mutex_destroy(fastmutex);
#endif
}

MDBX_INTERNAL_FUNC int mdbx_fastmutex_acquire(mdbx_fastmutex_t *fastmutex) {
#if defined(_WIN32) || defined(_WIN64)
  EnterCriticalSection(fastmutex);
  return MDBX_SUCCESS;
#else
  return pthread_mutex_lock(fastmutex);
#endif
}

MDBX_INTERNAL_FUNC int mdbx_fastmutex_release(mdbx_fastmutex_t *fastmutex) {
#if defined(_WIN32) || defined(_WIN64)
  LeaveCriticalSection(fastmutex);
  return MDBX_SUCCESS;
#else
  return pthread_mutex_unlock(fastmutex);
#endif
}

/*----------------------------------------------------------------------------*/

MDBX_INTERNAL_FUNC int mdbx_removefile(const char *pathname) {
#if defined(_WIN32) || defined(_WIN64)
  return DeleteFileA(pathname) ? MDBX_SUCCESS : GetLastError();
#else
  return unlink(pathname) ? errno : MDBX_SUCCESS;
#endif
}

MDBX_INTERNAL_FUNC int mdbx_openfile(const char *pathname, int flags,
                                     mode_t mode, mdbx_filehandle_t *fd,
                                     bool exclusive) {
  *fd = INVALID_HANDLE_VALUE;
#if defined(_WIN32) || defined(_WIN64)
  (void)mode;
  size_t wlen = mbstowcs(nullptr, pathname, INT_MAX);
  if (wlen < 1 || wlen > /* MAX_PATH */ INT16_MAX)
    return ERROR_INVALID_NAME;
  wchar_t *const pathnameW = _alloca((wlen + 1) * sizeof(wchar_t));
  if (wlen != mbstowcs(pathnameW, pathname, wlen + 1))
    return ERROR_INVALID_NAME;

  DWORD DesiredAccess, ShareMode;
  DWORD FlagsAndAttributes = FILE_ATTRIBUTE_NORMAL;
  switch (flags & (O_RDONLY | O_WRONLY | O_RDWR)) {
  default:
    return ERROR_INVALID_PARAMETER;
  case O_RDONLY:
    DesiredAccess = GENERIC_READ;
    ShareMode =
        exclusive ? FILE_SHARE_READ : (FILE_SHARE_READ | FILE_SHARE_WRITE);
    break;
  case O_WRONLY: /* assume for MDBX_env_copy() and friends output */
    DesiredAccess = GENERIC_WRITE;
    ShareMode = 0;
    FlagsAndAttributes |= FILE_FLAG_NO_BUFFERING | FILE_FLAG_WRITE_THROUGH;
    break;
  case O_RDWR:
    DesiredAccess = GENERIC_READ | GENERIC_WRITE;
    ShareMode = exclusive ? 0 : (FILE_SHARE_READ | FILE_SHARE_WRITE);
    break;
  }

  DWORD CreationDisposition;
  switch (flags & (O_EXCL | O_CREAT)) {
  default:
    return ERROR_INVALID_PARAMETER;
  case 0:
    CreationDisposition = OPEN_EXISTING;
    break;
  case O_EXCL | O_CREAT:
    CreationDisposition = CREATE_NEW;
    FlagsAndAttributes |= FILE_ATTRIBUTE_NOT_CONTENT_INDEXED;
    break;
  case O_CREAT:
    CreationDisposition = OPEN_ALWAYS;
    FlagsAndAttributes |= FILE_ATTRIBUTE_NOT_CONTENT_INDEXED;
    break;
  }

  *fd = CreateFileW(pathnameW, DesiredAccess, ShareMode, NULL,
                    CreationDisposition, FlagsAndAttributes, NULL);

  if (*fd == INVALID_HANDLE_VALUE)
    return GetLastError();
  if ((flags & O_CREAT) && GetLastError() != ERROR_ALREADY_EXISTS) {
    /* set FILE_ATTRIBUTE_NOT_CONTENT_INDEXED for new file */
    DWORD FileAttributes = GetFileAttributesA(pathname);
    if (FileAttributes == INVALID_FILE_ATTRIBUTES ||
        !SetFileAttributesA(pathname, FileAttributes |
                                          FILE_ATTRIBUTE_NOT_CONTENT_INDEXED)) {
      int rc = GetLastError();
      CloseHandle(*fd);
      *fd = INVALID_HANDLE_VALUE;
      return rc;
    }
  }
#else
  (void)exclusive;
#ifdef O_CLOEXEC
  flags |= O_CLOEXEC;
#endif /* O_CLOEXEC */
  *fd = open(pathname, flags, mode);
  if (*fd < 0)
    return errno;

#if defined(FD_CLOEXEC) && !defined(O_CLOEXEC)
  int fd_flags = fcntl(*fd, F_GETFD);
  if (fd_flags != -1)
    (void)fcntl(*fd, F_SETFD, fd_flags | FD_CLOEXEC);
#endif /* FD_CLOEXEC && !O_CLOEXEC */

  if ((flags & (O_RDONLY | O_WRONLY | O_RDWR)) == O_WRONLY) {
    /* assume for MDBX_env_copy() and friends output */
#if defined(O_DIRECT)
    int fd_flags = fcntl(*fd, F_GETFD);
    if (fd_flags != -1)
      (void)fcntl(*fd, F_SETFL, fd_flags | O_DIRECT);
#endif /* O_DIRECT */
#if defined(F_NOCACHE)
    (void)fcntl(*fd, F_NOCACHE, 1);
#endif /* F_NOCACHE */
  }
#endif

  return MDBX_SUCCESS;
}

MDBX_INTERNAL_FUNC int mdbx_closefile(mdbx_filehandle_t fd) {
#if defined(_WIN32) || defined(_WIN64)
  return CloseHandle(fd) ? MDBX_SUCCESS : GetLastError();
#else
  return (close(fd) == 0) ? MDBX_SUCCESS : errno;
#endif
}

MDBX_INTERNAL_FUNC int mdbx_pread(mdbx_filehandle_t fd, void *buf, size_t bytes,
                                  uint64_t offset) {
  if (bytes > MAX_WRITE)
    return MDBX_EINVAL;
#if defined(_WIN32) || defined(_WIN64)
  OVERLAPPED ov;
  ov.hEvent = 0;
  ov.Offset = (DWORD)offset;
  ov.OffsetHigh = HIGH_DWORD(offset);

  DWORD read = 0;
  if (unlikely(!ReadFile(fd, buf, (DWORD)bytes, &read, &ov))) {
    int rc = GetLastError();
    return (rc == MDBX_SUCCESS) ? /* paranoia */ ERROR_READ_FAULT : rc;
  }
#else
  STATIC_ASSERT_MSG(sizeof(off_t) >= sizeof(size_t),
                    "libmdbx requires 64-bit file I/O on 64-bit systems");
  intptr_t read = pread(fd, buf, bytes, offset);
  if (read < 0) {
    int rc = errno;
    return (rc == MDBX_SUCCESS) ? /* paranoia */ MDBX_EIO : rc;
  }
#endif
  return (bytes == (size_t)read) ? MDBX_SUCCESS : MDBX_ENODATA;
}

MDBX_INTERNAL_FUNC int mdbx_pwrite(mdbx_filehandle_t fd, const void *buf,
                                   size_t bytes, uint64_t offset) {
  while (true) {
#if defined(_WIN32) || defined(_WIN64)
    OVERLAPPED ov;
    ov.hEvent = 0;
    ov.Offset = (DWORD)offset;
    ov.OffsetHigh = HIGH_DWORD(offset);

    DWORD written;
    if (unlikely(!WriteFile(
            fd, buf, likely(bytes <= MAX_WRITE) ? (DWORD)bytes : MAX_WRITE,
            &written, &ov)))
      return GetLastError();
    if (likely(bytes == written))
      return MDBX_SUCCESS;
#else
    STATIC_ASSERT_MSG(sizeof(off_t) >= sizeof(size_t),
                      "libmdbx requires 64-bit file I/O on 64-bit systems");
    const intptr_t written =
        pwrite(fd, buf, likely(bytes <= MAX_WRITE) ? bytes : MAX_WRITE, offset);
    if (likely(bytes == (size_t)written))
      return MDBX_SUCCESS;
    if (written < 0) {
      const int rc = errno;
      if (rc != EINTR)
        return rc;
      continue;
    }
#endif
    bytes -= written;
    offset += written;
    buf = (char *)buf + written;
  }
}

MDBX_INTERNAL_FUNC int mdbx_write(mdbx_filehandle_t fd, const void *buf,
                                  size_t bytes) {
  while (true) {
#if defined(_WIN32) || defined(_WIN64)
    DWORD written;
    if (unlikely(!WriteFile(
            fd, buf, likely(bytes <= MAX_WRITE) ? (DWORD)bytes : MAX_WRITE,
            &written, nullptr)))
      return GetLastError();
    if (likely(bytes == written))
      return MDBX_SUCCESS;
#else
    STATIC_ASSERT_MSG(sizeof(off_t) >= sizeof(size_t),
                      "libmdbx requires 64-bit file I/O on 64-bit systems");
    const intptr_t written =
        write(fd, buf, likely(bytes <= MAX_WRITE) ? bytes : MAX_WRITE);
    if (likely(bytes == (size_t)written))
      return MDBX_SUCCESS;
    if (written < 0) {
      const int rc = errno;
      if (rc != EINTR)
        return rc;
      continue;
    }
#endif
    bytes -= written;
    buf = (char *)buf + written;
  }
}

int mdbx_pwritev(mdbx_filehandle_t fd, struct iovec *iov, int iovcnt,
                 uint64_t offset, size_t expected_written) {
#if defined(_WIN32) || defined(_WIN64) || defined(__APPLE__)
  size_t written = 0;
  for (int i = 0; i < iovcnt; ++i) {
    int rc = mdbx_pwrite(fd, iov[i].iov_base, iov[i].iov_len, offset);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    written += iov[i].iov_len;
    offset += iov[i].iov_len;
  }
  return (expected_written == written) ? MDBX_SUCCESS
                                       : MDBX_EIO /* ERROR_WRITE_FAULT */;
#else
  int rc;
  intptr_t written;
  do {
    STATIC_ASSERT_MSG(sizeof(off_t) >= sizeof(size_t),
                      "libmdbx requires 64-bit file I/O on 64-bit systems");
    written = pwritev(fd, iov, iovcnt, offset);
    if (likely(expected_written == (size_t)written))
      return MDBX_SUCCESS;
    rc = errno;
  } while (rc == EINTR);
  return (written < 0) ? rc : MDBX_EIO /* Use which error code? */;
#endif
}

MDBX_INTERNAL_FUNC int mdbx_filesync(mdbx_filehandle_t fd,
                                     enum mdbx_syncmode_bits mode_bits) {
#if defined(_WIN32) || defined(_WIN64)
  return ((mode_bits & (MDBX_SYNC_DATA | MDBX_SYNC_IODQ)) == 0 ||
          FlushFileBuffers(fd))
             ? MDBX_SUCCESS
             : GetLastError();
#else

#if defined(__APPLE__) &&                                                      \
    MDBX_OSX_SPEED_INSTEADOF_DURABILITY == MDBX_OSX_WANNA_DURABILITY
  if (mode_bits & MDBX_SYNC_IODQ)
    return likely(fcntl(fd, F_FULLFSYNC) != -1) ? MDBX_SUCCESS : errno;
#endif /* MacOS */
#if defined(__linux__) || defined(__gnu_linux__)
  if (mode_bits == MDBX_SYNC_SIZE && mdbx_linux_kernel_version >= 0x03060000)
    return MDBX_SUCCESS;
#endif /* Linux */
  int rc;
  do {
#if defined(_POSIX_SYNCHRONIZED_IO) && _POSIX_SYNCHRONIZED_IO > 0
    /* LY: This code is always safe and without appreciable performance
     * degradation, even on a kernel with fdatasync's bug.
     *
     * For more info about of a corresponding fdatasync() bug
     * see http://www.spinics.net/lists/linux-ext4/msg33714.html */
    if ((mode_bits & MDBX_SYNC_SIZE) == 0) {
      if (fdatasync(fd) == 0)
        return MDBX_SUCCESS;
    } else
#else
    (void)mode_bits;
#endif
        if (fsync(fd) == 0)
      return MDBX_SUCCESS;
    rc = errno;
  } while (rc == EINTR);
  return rc;
#endif
}

int mdbx_filesize(mdbx_filehandle_t fd, uint64_t *length) {
#if defined(_WIN32) || defined(_WIN64)
  BY_HANDLE_FILE_INFORMATION info;
  if (!GetFileInformationByHandle(fd, &info))
    return GetLastError();
  *length = info.nFileSizeLow | (uint64_t)info.nFileSizeHigh << 32;
#else
  struct stat st;

  STATIC_ASSERT_MSG(sizeof(off_t) <= sizeof(uint64_t),
                    "libmdbx requires 64-bit file I/O on 64-bit systems");
  if (fstat(fd, &st))
    return errno;

  *length = st.st_size;
#endif
  return MDBX_SUCCESS;
}

MDBX_INTERNAL_FUNC int mdbx_is_pipe(mdbx_filehandle_t fd) {
#if defined(_WIN32) || defined(_WIN64)
  switch (GetFileType(fd)) {
  case FILE_TYPE_DISK:
    return MDBX_RESULT_FALSE;
  case FILE_TYPE_CHAR:
  case FILE_TYPE_PIPE:
    return MDBX_RESULT_TRUE;
  default:
    return GetLastError();
  }
#else
  struct stat info;
  if (fstat(fd, &info))
    return errno;
  switch (info.st_mode & S_IFMT) {
  case S_IFBLK:
  case S_IFREG:
    return MDBX_RESULT_FALSE;
  case S_IFCHR:
  case S_IFIFO:
  case S_IFSOCK:
    return MDBX_RESULT_TRUE;
  case S_IFDIR:
  case S_IFLNK:
  default:
    return MDBX_INCOMPATIBLE;
  }
#endif
}

MDBX_INTERNAL_FUNC int mdbx_ftruncate(mdbx_filehandle_t fd, uint64_t length) {
#if defined(_WIN32) || defined(_WIN64)
  if (mdbx_SetFileInformationByHandle) {
    FILE_END_OF_FILE_INFO EndOfFileInfo;
    EndOfFileInfo.EndOfFile.QuadPart = length;
    return mdbx_SetFileInformationByHandle(fd, FileEndOfFileInfo,
                                           &EndOfFileInfo,
                                           sizeof(FILE_END_OF_FILE_INFO))
               ? MDBX_SUCCESS
               : GetLastError();
  } else {
    LARGE_INTEGER li;
    li.QuadPart = length;
    return (SetFilePointerEx(fd, li, NULL, FILE_BEGIN) && SetEndOfFile(fd))
               ? MDBX_SUCCESS
               : GetLastError();
  }
#else
  STATIC_ASSERT_MSG(sizeof(off_t) >= sizeof(size_t),
                    "libmdbx requires 64-bit file I/O on 64-bit systems");
  return ftruncate(fd, length) == 0 ? MDBX_SUCCESS : errno;
#endif
}

MDBX_INTERNAL_FUNC int mdbx_fseek(mdbx_filehandle_t fd, uint64_t pos) {
#if defined(_WIN32) || defined(_WIN64)
  LARGE_INTEGER li;
  li.QuadPart = pos;
  return SetFilePointerEx(fd, li, NULL, FILE_BEGIN) ? MDBX_SUCCESS
                                                    : GetLastError();
#else
  STATIC_ASSERT_MSG(sizeof(off_t) >= sizeof(size_t),
                    "libmdbx requires 64-bit file I/O on 64-bit systems");
  return (lseek(fd, pos, SEEK_SET) < 0) ? errno : MDBX_SUCCESS;
#endif
}

/*----------------------------------------------------------------------------*/

MDBX_INTERNAL_FUNC int
mdbx_thread_create(mdbx_thread_t *thread,
                   THREAD_RESULT(THREAD_CALL *start_routine)(void *),
                   void *arg) {
#if defined(_WIN32) || defined(_WIN64)
  *thread = CreateThread(NULL, 0, start_routine, arg, 0, NULL);
  return *thread ? MDBX_SUCCESS : GetLastError();
#else
  return pthread_create(thread, NULL, start_routine, arg);
#endif
}

MDBX_INTERNAL_FUNC int mdbx_thread_join(mdbx_thread_t thread) {
#if defined(_WIN32) || defined(_WIN64)
  DWORD code = WaitForSingleObject(thread, INFINITE);
  return waitstatus2errcode(code);
#else
  void *unused_retval = &unused_retval;
  return pthread_join(thread, &unused_retval);
#endif
}

/*----------------------------------------------------------------------------*/

MDBX_INTERNAL_FUNC int mdbx_msync(mdbx_mmap_t *map, size_t offset,
                                  size_t length, int async) {
  uint8_t *ptr = (uint8_t *)map->address + offset;
#if defined(_WIN32) || defined(_WIN64)
  if (FlushViewOfFile(ptr, length) && (async || FlushFileBuffers(map->fd)))
    return MDBX_SUCCESS;
  return GetLastError();
#else
#if defined(__linux__) || defined(__gnu_linux__)
  if (async && mdbx_linux_kernel_version > 0x02061300)
    /* Since Linux 2.6.19, MS_ASYNC is in fact a no-op,
       since the kernel properly tracks dirty pages and flushes them to storage
       as necessary. */
    return MDBX_SUCCESS;
#endif /* Linux */
  const int mode = async ? MS_ASYNC : MS_SYNC;
  int rc = (msync(ptr, length, mode) == 0) ? MDBX_SUCCESS : errno;
#if defined(__APPLE__) &&                                                      \
    MDBX_OSX_SPEED_INSTEADOF_DURABILITY == MDBX_OSX_WANNA_DURABILITY
  if (rc == MDBX_SUCCESS && mode == MS_SYNC)
    rc = likely(fcntl(map->fd, F_FULLFSYNC) != -1) ? MDBX_SUCCESS : errno;
#endif /* MacOS */
  return rc;
#endif
}

MDBX_INTERNAL_FUNC int mdbx_check4nonlocal(mdbx_filehandle_t handle,
                                           int flags) {
#if defined(_WIN32) || defined(_WIN64)
  if (GetFileType(handle) != FILE_TYPE_DISK)
    return ERROR_FILE_OFFLINE;

  if (mdbx_GetFileInformationByHandleEx) {
    FILE_REMOTE_PROTOCOL_INFO RemoteProtocolInfo;
    if (mdbx_GetFileInformationByHandleEx(handle, FileRemoteProtocolInfo,
                                          &RemoteProtocolInfo,
                                          sizeof(RemoteProtocolInfo))) {

      if ((RemoteProtocolInfo.Flags & REMOTE_PROTOCOL_INFO_FLAG_OFFLINE) &&
          !(flags & MDBX_RDONLY))
        return ERROR_FILE_OFFLINE;
      if (!(RemoteProtocolInfo.Flags & REMOTE_PROTOCOL_INFO_FLAG_LOOPBACK) &&
          !(flags & MDBX_EXCLUSIVE))
        return ERROR_REMOTE_STORAGE_MEDIA_ERROR;
    }
  }

  if (mdbx_NtFsControlFile) {
    NTSTATUS rc;
    struct {
      WOF_EXTERNAL_INFO wof_info;
      union {
        WIM_PROVIDER_EXTERNAL_INFO wim_info;
        FILE_PROVIDER_EXTERNAL_INFO_V1 file_info;
      };
      size_t reserved_for_microsoft_madness[42];
    } GetExternalBacking_OutputBuffer;
    IO_STATUS_BLOCK StatusBlock;
    rc = mdbx_NtFsControlFile(handle, NULL, NULL, NULL, &StatusBlock,
                              FSCTL_GET_EXTERNAL_BACKING, NULL, 0,
                              &GetExternalBacking_OutputBuffer,
                              sizeof(GetExternalBacking_OutputBuffer));
    if (NT_SUCCESS(rc)) {
      if (!(flags & MDBX_EXCLUSIVE))
        return ERROR_REMOTE_STORAGE_MEDIA_ERROR;
    } else if (rc != STATUS_OBJECT_NOT_EXTERNALLY_BACKED &&
               rc != STATUS_INVALID_DEVICE_REQUEST)
      return ntstatus2errcode(rc);
  }

  if (mdbx_GetVolumeInformationByHandleW && mdbx_GetFinalPathNameByHandleW) {
    WCHAR *PathBuffer = mdbx_malloc(sizeof(WCHAR) * INT16_MAX);
    if (!PathBuffer)
      return MDBX_ENOMEM;

    int rc = MDBX_SUCCESS;
    DWORD VolumeSerialNumber, FileSystemFlags;
    if (!mdbx_GetVolumeInformationByHandleW(handle, PathBuffer, INT16_MAX,
                                            &VolumeSerialNumber, NULL,
                                            &FileSystemFlags, NULL, 0)) {
      rc = GetLastError();
      goto bailout;
    }

    if ((flags & MDBX_RDONLY) == 0) {
      if (FileSystemFlags &
          (FILE_SEQUENTIAL_WRITE_ONCE | FILE_READ_ONLY_VOLUME |
           FILE_VOLUME_IS_COMPRESSED)) {
        rc = ERROR_REMOTE_STORAGE_MEDIA_ERROR;
        goto bailout;
      }
    }

    if (!mdbx_GetFinalPathNameByHandleW(handle, PathBuffer, INT16_MAX,
                                        FILE_NAME_NORMALIZED |
                                            VOLUME_NAME_NT)) {
      rc = GetLastError();
      goto bailout;
    }

    if (_wcsnicmp(PathBuffer, L"\\Device\\Mup\\", 12) == 0) {
      if (!(flags & MDBX_EXCLUSIVE)) {
        rc = ERROR_REMOTE_STORAGE_MEDIA_ERROR;
        goto bailout;
      }
    } else if (mdbx_GetFinalPathNameByHandleW(handle, PathBuffer, INT16_MAX,
                                              FILE_NAME_NORMALIZED |
                                                  VOLUME_NAME_DOS)) {
      UINT DriveType = GetDriveTypeW(PathBuffer);
      if (DriveType == DRIVE_NO_ROOT_DIR &&
          _wcsnicmp(PathBuffer, L"\\\\?\\", 4) == 0 &&
          _wcsnicmp(PathBuffer + 5, L":\\", 2) == 0) {
        PathBuffer[7] = 0;
        DriveType = GetDriveTypeW(PathBuffer + 4);
      }
      switch (DriveType) {
      case DRIVE_CDROM:
        if (flags & MDBX_RDONLY)
          break;
      // fall through
      case DRIVE_UNKNOWN:
      case DRIVE_NO_ROOT_DIR:
      case DRIVE_REMOTE:
      default:
        if (!(flags & MDBX_EXCLUSIVE))
          rc = ERROR_REMOTE_STORAGE_MEDIA_ERROR;
      // fall through
      case DRIVE_REMOVABLE:
      case DRIVE_FIXED:
      case DRIVE_RAMDISK:
        break;
      }
    }
  bailout:
    mdbx_free(PathBuffer);
    return rc;
  }
#else
  (void)handle;
  /* TODO: check for NFS handle ? */
  (void)flags;
#endif
  return MDBX_SUCCESS;
}

MDBX_INTERNAL_FUNC int mdbx_mmap(const int flags, mdbx_mmap_t *map,
                                 const size_t size, const size_t limit,
                                 const bool truncate) {
  assert(size <= limit);
  map->limit = 0;
  map->current = 0;
  map->address = nullptr;
#if defined(_WIN32) || defined(_WIN64)
  map->section = NULL;
  map->filesize = 0;
#endif /* Windows */

  int err = mdbx_check4nonlocal(map->fd, flags);
  if (unlikely(err != MDBX_SUCCESS))
    return err;

  if ((flags & MDBX_RDONLY) == 0 && truncate) {
    err = mdbx_ftruncate(map->fd, size);
    if (err != MDBX_SUCCESS)
      return err;
#if defined(_WIN32) || defined(_WIN64)
    map->filesize = size;
#else
    map->current = size;
#endif
  } else {
    uint64_t filesize;
    err = mdbx_filesize(map->fd, &filesize);
    if (err != MDBX_SUCCESS)
      return err;
#if defined(_WIN32) || defined(_WIN64)
    map->filesize = filesize;
#else
    map->current = (filesize > limit) ? limit : (size_t)filesize;
#endif
  }

#if defined(_WIN32) || defined(_WIN64)
  LARGE_INTEGER SectionSize;
  SectionSize.QuadPart = size;
  err = NtCreateSection(
      &map->section,
      /* DesiredAccess */
      (flags & MDBX_WRITEMAP)
          ? SECTION_QUERY | SECTION_MAP_READ | SECTION_EXTEND_SIZE |
                SECTION_MAP_WRITE
          : SECTION_QUERY | SECTION_MAP_READ | SECTION_EXTEND_SIZE,
      /* ObjectAttributes */ NULL, /* MaximumSize (InitialSize) */ &SectionSize,
      /* SectionPageProtection */
      (flags & MDBX_RDONLY) ? PAGE_READONLY : PAGE_READWRITE,
      /* AllocationAttributes */ SEC_RESERVE, map->fd);
  if (!NT_SUCCESS(err))
    return ntstatus2errcode(err);

  SIZE_T ViewSize = (flags & MDBX_RDONLY) ? 0 : limit;
  err = NtMapViewOfSection(
      map->section, GetCurrentProcess(), &map->address,
      /* ZeroBits */ 0,
      /* CommitSize */ 0,
      /* SectionOffset */ NULL, &ViewSize,
      /* InheritDisposition */ ViewUnmap,
      /* AllocationType */ (flags & MDBX_RDONLY) ? 0 : MEM_RESERVE,
      /* Win32Protect */
      (flags & MDBX_WRITEMAP) ? PAGE_READWRITE : PAGE_READONLY);
  if (!NT_SUCCESS(err)) {
    NtClose(map->section);
    map->section = 0;
    map->address = nullptr;
    return ntstatus2errcode(err);
  }
  assert(map->address != MAP_FAILED);

  map->current = (size_t)SectionSize.QuadPart;
  map->limit = ViewSize;

#else

  map->address = mmap(
      NULL, limit, (flags & MDBX_WRITEMAP) ? PROT_READ | PROT_WRITE : PROT_READ,
      MAP_SHARED, map->fd, 0);

  if (unlikely(map->address == MAP_FAILED)) {
    map->limit = 0;
    map->current = 0;
    map->address = nullptr;
    return errno;
  }
  map->limit = limit;

#ifdef MADV_DONTFORK
  if (unlikely(madvise(map->address, map->limit, MADV_DONTFORK) != 0))
    return errno;
#endif
#ifdef MADV_NOHUGEPAGE
  (void)madvise(map->address, map->limit, MADV_NOHUGEPAGE);
#endif

#endif

  return MDBX_SUCCESS;
}

MDBX_INTERNAL_FUNC int mdbx_munmap(mdbx_mmap_t *map) {
#if defined(_WIN32) || defined(_WIN64)
  if (map->section)
    NtClose(map->section);
  NTSTATUS rc = NtUnmapViewOfSection(GetCurrentProcess(), map->address);
  if (!NT_SUCCESS(rc))
    ntstatus2errcode(rc);
#else
  if (unlikely(munmap(map->address, map->limit)))
    return errno;
#endif

  map->limit = 0;
  map->current = 0;
  map->address = nullptr;
  return MDBX_SUCCESS;
}

MDBX_INTERNAL_FUNC int mdbx_mresize(int flags, mdbx_mmap_t *map, size_t size,
                                    size_t limit) {
  assert(size <= limit);
#if defined(_WIN32) || defined(_WIN64)
  assert(size != map->current || limit != map->limit || size < map->filesize);

  NTSTATUS status;
  LARGE_INTEGER SectionSize;
  int err, rc = MDBX_SUCCESS;

  if (!(flags & MDBX_RDONLY) && limit == map->limit && size > map->current) {
    /* growth rw-section */
    SectionSize.QuadPart = size;
    status = NtExtendSection(map->section, &SectionSize);
    if (NT_SUCCESS(status)) {
      map->current = size;
      if (map->filesize < size)
        map->filesize = size;
    }
    return ntstatus2errcode(status);
  }

  if (limit > map->limit) {
    /* check ability of address space for growth before umnap */
    PVOID BaseAddress = (PBYTE)map->address + map->limit;
    SIZE_T RegionSize = limit - map->limit;
    status = NtAllocateVirtualMemory(GetCurrentProcess(), &BaseAddress, 0,
                                     &RegionSize, MEM_RESERVE, PAGE_NOACCESS);
    if (!NT_SUCCESS(status))
      return ntstatus2errcode(status);

    status = NtFreeVirtualMemory(GetCurrentProcess(), &BaseAddress, &RegionSize,
                                 MEM_RELEASE);
    if (!NT_SUCCESS(status))
      return ntstatus2errcode(status);
  }

  /* Windows unable:
   *  - shrink a mapped file;
   *  - change size of mapped view;
   *  - extend read-only mapping;
   * Therefore we should unmap/map entire section. */
  status = NtUnmapViewOfSection(GetCurrentProcess(), map->address);
  if (!NT_SUCCESS(status))
    return ntstatus2errcode(status);
  status = NtClose(map->section);
  map->section = NULL;
  PVOID ReservedAddress = NULL;
  SIZE_T ReservedSize = limit;

  if (!NT_SUCCESS(status)) {
  bailout_ntstatus:
    err = ntstatus2errcode(status);
  bailout:
    map->address = NULL;
    map->current = map->limit = 0;
    if (ReservedAddress)
      (void)NtFreeVirtualMemory(GetCurrentProcess(), &ReservedAddress,
                                &ReservedSize, MEM_RELEASE);
    return err;
  }

  /* resizing of the file may take a while,
   * therefore we reserve address space to avoid occupy it by other threads */
  ReservedAddress = map->address;
  status = NtAllocateVirtualMemory(GetCurrentProcess(), &ReservedAddress, 0,
                                   &ReservedSize, MEM_RESERVE, PAGE_NOACCESS);
  if (!NT_SUCCESS(status)) {
    ReservedAddress = NULL;
    if (status != /* STATUS_CONFLICTING_ADDRESSES */ 0xC0000018)
      goto bailout_ntstatus /* no way to recovery */;

    /* assume we can change base address if mapping size changed or prev address
     * couldn't be used */
    map->address = NULL;
  }

retry_file_and_section:
  err = mdbx_filesize(map->fd, &map->filesize);
  if (err != MDBX_SUCCESS)
    goto bailout;

  if ((flags & MDBX_RDONLY) == 0 && map->filesize != size) {
    err = mdbx_ftruncate(map->fd, size);
    if (err == MDBX_SUCCESS)
      map->filesize = size;
    /* ignore error, because Windows unable shrink file
     * that already mapped (by another process) */
  }

  SectionSize.QuadPart = size;
  status = NtCreateSection(
      &map->section,
      /* DesiredAccess */
      (flags & MDBX_WRITEMAP)
          ? SECTION_QUERY | SECTION_MAP_READ | SECTION_EXTEND_SIZE |
                SECTION_MAP_WRITE
          : SECTION_QUERY | SECTION_MAP_READ | SECTION_EXTEND_SIZE,
      /* ObjectAttributes */ NULL,
      /* MaximumSize (InitialSize) */ &SectionSize,
      /* SectionPageProtection */
      (flags & MDBX_RDONLY) ? PAGE_READONLY : PAGE_READWRITE,
      /* AllocationAttributes */ SEC_RESERVE, map->fd);

  if (!NT_SUCCESS(status))
    goto bailout_ntstatus;

  if (ReservedAddress) {
    /* release reserved address space */
    status = NtFreeVirtualMemory(GetCurrentProcess(), &ReservedAddress,
                                 &ReservedSize, MEM_RELEASE);
    ReservedAddress = NULL;
    if (!NT_SUCCESS(status))
      goto bailout_ntstatus;
  }

retry_mapview:;
  SIZE_T ViewSize = (flags & MDBX_RDONLY) ? size : limit;
  status = NtMapViewOfSection(
      map->section, GetCurrentProcess(), &map->address,
      /* ZeroBits */ 0,
      /* CommitSize */ 0,
      /* SectionOffset */ NULL, &ViewSize,
      /* InheritDisposition */ ViewUnmap,
      /* AllocationType */ (flags & MDBX_RDONLY) ? 0 : MEM_RESERVE,
      /* Win32Protect */
      (flags & MDBX_WRITEMAP) ? PAGE_READWRITE : PAGE_READONLY);

  if (!NT_SUCCESS(status)) {
    if (status == /* STATUS_CONFLICTING_ADDRESSES */ 0xC0000018 &&
        map->address) {
      /* try remap at another base address */
      map->address = NULL;
      goto retry_mapview;
    }
    NtClose(map->section);
    map->section = NULL;

    if (map->address && (size != map->current || limit != map->limit)) {
      /* try remap with previously size and limit,
       * but will return MDBX_RESULT_TRUE on success */
      rc = MDBX_RESULT_TRUE;
      size = map->current;
      limit = map->limit;
      goto retry_file_and_section;
    }

    /* no way to recovery */
    goto bailout_ntstatus;
  }
  assert(map->address != MAP_FAILED);

  map->current = (size_t)SectionSize.QuadPart;
  map->limit = ViewSize;
#else

  uint64_t filesize;
  int rc = mdbx_filesize(map->fd, &filesize);
  if (rc != MDBX_SUCCESS)
    return rc;

  if (flags & MDBX_RDONLY) {
    map->current = (filesize > limit) ? limit : (size_t)filesize;
    if (map->current != size)
      rc = MDBX_RESULT_TRUE;
  } else if (filesize != size) {
    rc = mdbx_ftruncate(map->fd, size);
    if (rc != MDBX_SUCCESS)
      return rc;
    map->current = size;
  }

  if (limit != map->limit) {
#if defined(_GNU_SOURCE) && (defined(__linux__) || defined(__gnu_linux__))
    void *ptr = mremap(map->address, map->limit, limit,
                       /* LY: in case changing the mapping size calling code
                          must guarantees the absence of competing threads,
                          and a willingness to another base address */
                       MREMAP_MAYMOVE);
    if (ptr == MAP_FAILED) {
      rc = errno;
      return (rc == EAGAIN || rc == ENOMEM) ? MDBX_RESULT_TRUE : rc;
    }
    map->address = ptr;
    map->limit = limit;

#ifdef MADV_DONTFORK
    if (unlikely(madvise(map->address, map->limit, MADV_DONTFORK) != 0))
      return errno;
#endif

#ifdef MADV_NOHUGEPAGE
    (void)madvise(map->address, map->limit, MADV_NOHUGEPAGE);
#endif

#else
    rc = MDBX_RESULT_TRUE;
#endif /* _GNU_SOURCE && __linux__ */
  }
#endif
  return rc;
}

/*----------------------------------------------------------------------------*/

MDBX_INTERNAL_FUNC __cold void mdbx_osal_jitter(bool tiny) {
  for (;;) {
#if defined(_M_IX86) || defined(_M_X64) || defined(__i386__) ||                \
    defined(__x86_64__)
    const unsigned salt = 277u * (unsigned)__rdtsc();
#else
    const unsigned salt = rand();
#endif

    const unsigned coin = salt % (tiny ? 29u : 43u);
    if (coin < 43 / 3)
      break;
#if defined(_WIN32) || defined(_WIN64)
    SwitchToThread();
    if (coin > 43 * 2 / 3)
      Sleep(1);
#else
    sched_yield();
    if (coin > 43 * 2 / 3)
      usleep(coin);
#endif
  }
}

#if defined(_WIN32) || defined(_WIN64)
#elif defined(__APPLE__) || defined(__MACH__)
#include <mach/mach_time.h>
#elif defined(__linux__) || defined(__gnu_linux__)
static __cold clockid_t choice_monoclock(void) {
  struct timespec probe;
#if defined(CLOCK_BOOTTIME)
  if (clock_gettime(CLOCK_BOOTTIME, &probe) == 0)
    return CLOCK_BOOTTIME;
#elif defined(CLOCK_MONOTONIC_RAW)
  if (clock_gettime(CLOCK_MONOTONIC_RAW, &probe) == 0)
    return CLOCK_MONOTONIC_RAW;
#elif defined(CLOCK_MONOTONIC_COARSE)
  if (clock_gettime(CLOCK_MONOTONIC_COARSE, &probe) == 0)
    return CLOCK_MONOTONIC_COARSE;
#endif
  return CLOCK_MONOTONIC;
}
#endif

/*----------------------------------------------------------------------------*/

#if defined(_WIN32) || defined(_WIN64)
static LARGE_INTEGER performance_frequency;
#elif defined(__APPLE__) || defined(__MACH__)
static uint64_t ratio_16dot16_to_monotine;
#endif

MDBX_INTERNAL_FUNC uint64_t
mdbx_osal_16dot16_to_monotime(uint32_t seconds_16dot16) {
#if defined(_WIN32) || defined(_WIN64)
  if (unlikely(performance_frequency.QuadPart == 0))
    QueryPerformanceFrequency(&performance_frequency);
  const uint64_t ratio = performance_frequency.QuadPart;
#elif defined(__APPLE__) || defined(__MACH__)
  if (unlikely(ratio_16dot16_to_monotine == 0)) {
    mach_timebase_info_data_t ti;
    mach_timebase_info(&ti);
    ratio_16dot16_to_monotine = UINT64_C(1000000000) * ti.denom / ti.numer;
  }
  const uint64_t ratio = ratio_16dot16_to_monotine;
#else
  const uint64_t ratio = UINT64_C(1000000000);
#endif
  return (ratio * seconds_16dot16 + 32768) >> 16;
}

MDBX_INTERNAL_FUNC uint32_t mdbx_osal_monotime_to_16dot16(uint64_t monotime) {
  static uint64_t limit;
  if (unlikely(monotime > limit)) {
    if (limit != 0)
      return UINT32_MAX;
    limit = mdbx_osal_16dot16_to_monotime(UINT32_MAX - 1);
    if (monotime > limit)
      return UINT32_MAX;
  }
#if defined(_WIN32) || defined(_WIN64)
  return (uint32_t)((monotime << 16) / performance_frequency.QuadPart);
#elif defined(__APPLE__) || defined(__MACH__)
  return (uint32_t)((monotime << 16) / ratio_16dot16_to_monotine);
#else
  return (uint32_t)(monotime * 128 / 1953125);
#endif
}

MDBX_INTERNAL_FUNC uint64_t mdbx_osal_monotime(void) {
#if defined(_WIN32) || defined(_WIN64)
  LARGE_INTEGER counter;
  counter.QuadPart = 0;
  QueryPerformanceCounter(&counter);
  return counter.QuadPart;
#elif defined(__APPLE__) || defined(__MACH__)
  return mach_absolute_time();
#else

#if defined(__linux__) || defined(__gnu_linux__)
  static clockid_t posix_clockid = -1;
  if (unlikely(posix_clockid < 0))
    posix_clockid = choice_monoclock();
#elif defined(CLOCK_MONOTONIC)
#define posix_clockid CLOCK_MONOTONIC
#else
#define posix_clockid CLOCK_REALTIME
#endif

  struct timespec ts;
  if (unlikely(clock_gettime(posix_clockid, &ts) != 0)) {
    ts.tv_nsec = 0;
    ts.tv_sec = 0;
  }
  return ts.tv_sec * UINT64_C(1000000000) + ts.tv_nsec;
#endif
}

/*----------------------------------------------------------------------------*/

static void bootid_shake(bin128_t *p) {
  /* Bob Jenkins's PRNG: https://burtleburtle.net/bob/rand/smallprng.html */
  const uint32_t e = p->a - (p->b << 23 | p->b >> 9);
  p->a = p->b ^ (p->c << 16 | p->c >> 16);
  p->b = p->c + (p->d << 11 | p->d >> 21);
  p->c = p->d + e;
  p->d = e + p->a;
}

static void bootid_collect(bin128_t *p, const void *s, size_t n) {
  p->y += UINT64_C(64526882297375213);
  bootid_shake(p);
  for (size_t i = 0; i < n; ++i) {
    bootid_shake(p);
    p->y ^= UINT64_C(48797879452804441) * ((const uint8_t *)s)[i];
    bootid_shake(p);
    p->y += 14621231;
  }
  bootid_shake(p);

  /* minor non-linear tomfoolery */
  const unsigned z = p->x % 61;
  p->y = p->y << z | p->y >> (64 - z);
  bootid_shake(p);
  bootid_shake(p);
  const unsigned q = p->x % 59;
  p->y = p->y << q | p->y >> (64 - q);
  bootid_shake(p);
  bootid_shake(p);
  bootid_shake(p);
}

#if defined(_WIN32) || defined(_WIN64)

static uint64_t windows_systemtime_ms() {
  FILETIME ft;
  GetSystemTimeAsFileTime(&ft);
  return ((uint64_t)ft.dwHighDateTime << 32 | ft.dwLowDateTime) / 10000ul;
}

static uint64_t windows_bootime(void) {
  unsigned confirmed = 0;
  uint64_t boottime = 0;
  uint64_t up0 = mdbx_GetTickCount64();
  uint64_t st0 = windows_systemtime_ms();
  for (uint64_t fuse = st0; up0 && st0 < fuse + 1000 * 1000u / 42;) {
    YieldProcessor();
    const uint64_t up1 = mdbx_GetTickCount64();
    const uint64_t st1 = windows_systemtime_ms();
    if (st1 > fuse && st1 == st0 && up1 == up0) {
      uint64_t diff = st1 - up1;
      if (boottime == diff) {
        if (++confirmed > 4)
          return boottime;
      } else {
        confirmed = 0;
        boottime = diff;
      }
      fuse = st1;
      Sleep(1);
    }
    st0 = st1;
    up0 = up1;
  }
  return 0;
}

static LSTATUS mdbx_RegGetValue(HKEY hkey, LPCWSTR lpSubKey, LPCWSTR lpValue,
                                DWORD dwFlags, LPDWORD pdwType, PVOID pvData,
                                LPDWORD pcbData) {
  LSTATUS rc =
      RegGetValueW(hkey, lpSubKey, lpValue, dwFlags, pdwType, pvData, pcbData);
  if (rc != ERROR_FILE_NOT_FOUND)
    return rc;

  rc = RegGetValueW(hkey, lpSubKey, lpValue,
                    dwFlags | 0x00010000 /* RRF_SUBKEY_WOW6464KEY */, pdwType,
                    pvData, pcbData);
  if (rc != ERROR_FILE_NOT_FOUND)
    return rc;
  return RegGetValueW(hkey, lpSubKey, lpValue,
                      dwFlags | 0x00020000 /* RRF_SUBKEY_WOW6432KEY */, pdwType,
                      pvData, pcbData);
}
#endif

static __cold __maybe_unused bool bootid_parse_uuid(bin128_t *s, const void *p,
                                                    const size_t n) {
  if (n > 31) {
    unsigned bits = 0;
    for (unsigned i = 0; i < n; ++i) /* try parse an UUID in text form */ {
      uint8_t c = ((const uint8_t *)p)[i];
      if (c >= '0' && c <= '9')
        c -= '0';
      else if (c >= 'a' && c <= 'f')
        c -= 'a' - 10;
      else if (c >= 'A' && c <= 'F')
        c -= 'A' - 10;
      else
        continue;
      assert(c <= 15);
      c ^= s->y >> 60;
      s->y = s->y << 4 | s->x >> 60;
      s->x = s->x << 4 | c;
      bits += 4;
    }
    if (bits > 42 * 3)
      /* UUID parsed successfully */
      return true;
  }

  if (n > 15) /* is enough handle it as a binary? */ {
    if (n == sizeof(bin128_t)) {
      bin128_t aligned;
      memcpy(&aligned, p, sizeof(bin128_t));
      s->x += aligned.x;
      s->y += aligned.y;
    } else
      bootid_collect(s, p, n);
    return true;
  }

  if (n)
    bootid_collect(s, p, n);
  return false;
}

__cold MDBX_INTERNAL_FUNC bin128_t mdbx_osal_bootid(void) {
  bin128_t bin = {{0, 0}};
  bool got_machineid = false, got_bootime = false, got_bootseq = false;

#if defined(__linux__) || defined(__gnu_linux__)
  {
    const int fd =
        open("/proc/sys/kernel/random/boot_id", O_RDONLY | O_NOFOLLOW);
    if (fd != -1) {
      struct statvfs fs;
      char buf[42];
      const ssize_t len =
          (fstatvfs(fd, &fs) == 0 && fs.f_fsid == /* procfs */ 0x00009FA0)
              ? read(fd, buf, sizeof(buf))
              : -1;
      close(fd);
      if (len > 0 && bootid_parse_uuid(&bin, buf, len))
        return bin;
    }
  }
#endif /* Linux */

#if defined(__APPLE__) || defined(__MACH__)
  {
    char buf[42];
    size_t len = sizeof(buf);
    if (!sysctlbyname("kern.bootsessionuuid", buf, &len, nullptr, 0) &&
        bootid_parse_uuid(&bin, buf, len))
      return bin;

#if defined(__MAC_OS_X_VERSION_MIN_REQUIRED) &&                                \
    __MAC_OS_X_VERSION_MIN_REQUIRED > 1050
    uuid_t uuid;
    struct timespec wait = {0, 1000000000u / 42};
    if (!gethostuuid(uuid, &wait) &&
        bootid_parse_uuid(&bin, uuid, sizeof(uuid)))
      got_machineid = true;
#endif /* > 10.5 */

    struct timeval boottime;
    len = sizeof(boottime);
    if (!sysctlbyname("kern.boottime", &boottime, &len, nullptr, 0) &&
        len == sizeof(boottime) && boottime.tv_sec)
      got_bootime = true;
  }
#endif /* Apple/Darwin */

#if defined(_WIN32) || defined(_WIN64)
  {
    union buf {
      DWORD BootId;
      DWORD BaseTime;
      SYSTEM_TIMEOFDAY_INFORMATION SysTimeOfDayInfo;
      struct {
        LARGE_INTEGER BootTime;
        LARGE_INTEGER CurrentTime;
        LARGE_INTEGER TimeZoneBias;
        ULONG TimeZoneId;
        ULONG Reserved;
        ULONGLONG BootTimeBias;
        ULONGLONG SleepTimeBias;
      } SysTimeOfDayInfoHacked;
      wchar_t MachineGuid[42];
      char DigitalProductId[248];
    } buf;

    static const wchar_t HKLM_MicrosoftCryptography[] =
        L"SOFTWARE\\Microsoft\\Cryptography";
    DWORD len = sizeof(buf);
    /* Windows is madness and must die */
    if (mdbx_RegGetValue(HKEY_LOCAL_MACHINE, HKLM_MicrosoftCryptography,
                         L"MachineGuid", RRF_RT_ANY, NULL, &buf.MachineGuid,
                         &len) == ERROR_SUCCESS &&
        len > 42 && len < sizeof(buf))
      got_machineid = bootid_parse_uuid(&bin, &buf.MachineGuid, len);

    if (!got_machineid) {
      /* again, Windows is madness */
      static const wchar_t HKLM_WindowsNT[] =
          L"SOFTWARE\\Microsoft\\Windows NT\\CurrentVersion";
      static const wchar_t HKLM_WindowsNT_DPK[] =
          L"SOFTWARE\\Microsoft\\Windows "
          L"NT\\CurrentVersion\\DefaultProductKey";
      static const wchar_t HKLM_WindowsNT_DPK2[] =
          L"SOFTWARE\\Microsoft\\Windows "
          L"NT\\CurrentVersion\\DefaultProductKey2";

      len = sizeof(buf);
      if (mdbx_RegGetValue(HKEY_LOCAL_MACHINE, HKLM_WindowsNT,
                           L"DigitalProductId", RRF_RT_ANY, NULL,
                           &buf.DigitalProductId, &len) == ERROR_SUCCESS &&
          len > 42 && len < sizeof(buf)) {
        bootid_collect(&bin, &buf.DigitalProductId, len);
        got_machineid = true;
      }
      len = sizeof(buf);
      if (mdbx_RegGetValue(HKEY_LOCAL_MACHINE, HKLM_WindowsNT_DPK,
                           L"DigitalProductId", RRF_RT_ANY, NULL,
                           &buf.DigitalProductId, &len) == ERROR_SUCCESS &&
          len > 42 && len < sizeof(buf)) {
        bootid_collect(&bin, &buf.DigitalProductId, len);
        got_machineid = true;
      }
      len = sizeof(buf);
      if (mdbx_RegGetValue(HKEY_LOCAL_MACHINE, HKLM_WindowsNT_DPK2,
                           L"DigitalProductId", RRF_RT_ANY, NULL,
                           &buf.DigitalProductId, &len) == ERROR_SUCCESS &&
          len > 42 && len < sizeof(buf)) {
        bootid_collect(&bin, &buf.DigitalProductId, len);
        got_machineid = true;
      }
    }

    static const wchar_t HKLM_PrefetcherParams[] =
        L"SYSTEM\\CurrentControlSet\\Control\\Session Manager\\Memory "
        L"Management\\PrefetchParameters";
    len = sizeof(buf);
    if (mdbx_RegGetValue(HKEY_LOCAL_MACHINE, HKLM_PrefetcherParams, L"BootId",
                         RRF_RT_DWORD, NULL, &buf.BootId,
                         &len) == ERROR_SUCCESS &&
        len > 1 && len < sizeof(buf)) {
      bootid_collect(&bin, &buf.BootId, len);
      got_bootseq = true;
    }

    len = sizeof(buf);
    if (mdbx_RegGetValue(HKEY_LOCAL_MACHINE, HKLM_PrefetcherParams, L"BaseTime",
                         RRF_RT_DWORD, NULL, &buf.BaseTime,
                         &len) == ERROR_SUCCESS &&
        len >= sizeof(buf.BaseTime) && buf.BaseTime) {
      bootid_collect(&bin, &buf.BaseTime, len);
      got_bootime = true;
    }

    /* BootTime from SYSTEM_TIMEOFDAY_INFORMATION */
    NTSTATUS status = NtQuerySystemInformation(
        0x03 /* SystemTmeOfDayInformation */, &buf.SysTimeOfDayInfo,
        sizeof(buf.SysTimeOfDayInfo), &len);
    if (NT_SUCCESS(status) &&
        len >= offsetof(union buf, SysTimeOfDayInfoHacked.BootTime) +
                   sizeof(buf.SysTimeOfDayInfoHacked.BootTime) &&
        buf.SysTimeOfDayInfoHacked.BootTime.QuadPart) {
      bootid_collect(&bin, &buf.SysTimeOfDayInfoHacked.BootTime,
                     sizeof(buf.SysTimeOfDayInfoHacked.BootTime));
      got_bootime = true;
    }

    if (!got_bootime) {
      uint64_t boottime = windows_bootime();
      if (boottime) {
        bootid_collect(&bin, &boottime, sizeof(boottime));
        got_bootime = true;
      }
    }
  }
#endif /* Windows */

#if defined(CTL_HW) && defined(HW_UUID)
  if (!got_machineid) {
    static const int mib[] = {CTL_HW, HW_UUID};
    char buf[42];
    size_t len = sizeof(buf);
    if (sysctl(
#ifdef SYSCTL_LEGACY_NONCONST_MIB
            (int *)
#endif
                mib,
            ARRAY_LENGTH(mib), &buf, &len, NULL, 0) == 0)
      got_machineid = bootid_parse_uuid(&bin, buf, len);
  }
#endif /* CTL_HW && HW_UUID */

#if defined(CTL_KERN) && defined(KERN_HOSTUUID)
  if (!got_machineid) {
    static const int mib[] = {CTL_KERN, KERN_HOSTUUID};
    char buf[42];
    size_t len = sizeof(buf);
    if (sysctl(
#ifdef SYSCTL_LEGACY_NONCONST_MIB
            (int *)
#endif
                mib,
            ARRAY_LENGTH(mib), &buf, &len, NULL, 0) == 0)
      got_machineid = bootid_parse_uuid(&bin, buf, len);
  }
#endif /* CTL_KERN && KERN_HOSTUUID */

#if defined(__NetBSD__)
  if (!got_machineid) {
    char buf[42];
    size_t len = sizeof(buf);
    if (sysctlbyname("machdep.dmi.system-uuid", buf, &len, NULL, 0) == 0)
      got_machineid = bootid_parse_uuid(&bin, buf, len);
  }
#endif /* __NetBSD__ */

#if _XOPEN_SOURCE_EXTENDED
  if (!got_machineid) {
    const int hostid = gethostid();
    if (hostid > 0) {
      bootid_collect(&bin, &hostid, sizeof(hostid));
      got_machineid = true;
    }
  }
#endif /* _XOPEN_SOURCE_EXTENDED */

  if (!got_machineid) {
  lack:
    bin.x = bin.y = 0;
    return bin;
  }

  /*--------------------------------------------------------------------------*/

#if defined(CTL_KERN) && defined(KERN_BOOTTIME)
  if (!got_bootime) {
    static const int mib[] = {CTL_KERN, KERN_BOOTTIME};
    struct timeval boottime;
    size_t len = sizeof(boottime);
    if (sysctl(
#ifdef SYSCTL_LEGACY_NONCONST_MIB
            (int *)
#endif
                mib,
            ARRAY_LENGTH(mib), &boottime, &len, NULL, 0) == 0 &&
        len == sizeof(boottime) && boottime.tv_sec) {
      bootid_collect(&bin, &boottime, len);
      got_bootime = true;
    }
  }
#endif /* CTL_KERN && KERN_BOOTTIME */

#if defined(__sun) || defined(__SVR4) || defined(__svr4__)
  if (!got_bootime) {
    kstat_ctl_t *kc = kstat_open();
    if (kc) {
      kstat_t *kp = kstat_lookup(kc, "unix", 0, "system_misc");
      if (kp && kstat_read(kc, kp, 0) != -1) {
        kstat_named_t *kn = (kstat_named_t *)kstat_data_lookup(kp, "boot_time");
        if (kn) {
          switch (kn->data_type) {
          case KSTAT_DATA_INT32:
          case KSTAT_DATA_UINT32:
            bootid_collect(&bin, &kn->value, sizeof(int32_t));
            got_boottime = true;
          case KSTAT_DATA_INT64:
          case KSTAT_DATA_UINT64:
            bootid_collect(&bin, &kn->value, sizeof(int64_t));
            got_boottime = true;
          }
        }
      }
      kstat_close(kc);
    }
  }
#endif /* SunOS / Solaris */

#if _XOPEN_SOURCE_EXTENDED && defined(BOOT_TIME)
  if (!got_bootime) {
    setutxent();
    const struct utmpx id = {.ut_type = BOOT_TIME};
    const struct utmpx *entry = getutxid(&id);
    if (entry) {
      bootid_collect(&bin, entry, sizeof(*entry));
      got_bootime = true;
      while (unlikely((entry = getutxid(&id)) != nullptr)) {
        /* have multiple reboot records, assuming we can distinguish next
         * bootsession even if RTC is wrong or absent */
        bootid_collect(&bin, entry, sizeof(*entry));
        got_bootseq = true;
      }
    }
    endutxent();
  }
#endif /* _XOPEN_SOURCE_EXTENDED && BOOT_TIME */

  if (!got_bootseq) {
    if (!got_bootime || !MDBX_TRUST_RTC)
      goto lack;

#if defined(_WIN32) || defined(_WIN64)
    FILETIME now;
    GetSystemTimeAsFileTime(&now);
    if (0x1CCCCCC > now.dwHighDateTime)
#else
    struct timespec mono, real;
    if (clock_gettime(CLOCK_MONOTONIC, &mono) ||
        clock_gettime(CLOCK_REALTIME, &real) ||
        /* wrong time, RTC is mad or absent */
        1555555555l > real.tv_sec ||
        /* seems no adjustment by RTC/NTP, i.e. a fake time */
        real.tv_sec < mono.tv_sec || 1234567890l > real.tv_sec - mono.tv_sec ||
        (real.tv_sec - mono.tv_sec) % 900u == 0)
#endif
      goto lack;
  }

  return bin;
}
