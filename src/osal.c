/* https://en.wikipedia.org/wiki/Operating_system_abstraction_layer */

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

#if defined(_WIN32) || defined(_WIN64)
static int waitstatus2errcode(DWORD result) {
  switch (result) {
  case WAIT_OBJECT_0:
    return MDB_SUCCESS;
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
#endif /* _WIN32 || _WIN64 */

/*----------------------------------------------------------------------------*/

#ifndef _MSC_VER
/* Prototype should match libc runtime. ISO POSIX (2003) & LSB 3.1 */
__nothrow __noreturn void __assert_fail(const char *assertion, const char *file,
                                        unsigned line, const char *function);
#else
__extern_C __declspec(dllimport) void __cdecl _assert(char const *message,
                                                      char const *filename,
                                                      unsigned line);
#endif /* _MSC_VER */

#ifndef mdbx_assert_fail
void __cold mdbx_assert_fail(MDB_env *env, const char *msg, const char *func,
                             int line) {
#if MDB_DEBUG
  if (env && env->me_assert_func) {
    env->me_assert_func(env, msg, func, line);
    return;
  }
#else
  (void)env;
#endif /* MDB_DEBUG */

  if (mdbx_debug_logger)
    mdbx_debug_log(MDBX_DBG_ASSERT, func, line, "assert: %s\n", msg);
#ifndef _MSC_VER
  __assert_fail(msg, "mdbx", line, func);
#else
  _assert(msg, func, line);
#endif /* _MSC_VER */
}
#endif /* mdbx_assert_fail */

__cold void mdbx_panic(const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
#ifdef _MSC_VER
  if (IsDebuggerPresent()) {
    OutputDebugString("\r\n" FIXME "\r\n");
    FatalExit(ERROR_UNHANDLED_ERROR);
  }
#elif _XOPEN_SOURCE >= 700 || _POSIX_C_SOURCE >= 200809L ||                    \
    (__GLIBC_PREREQ(1, 0) && !__GLIBC_PREREQ(2, 10) && defined(_GNU_SOURCE))
  vdprintf(STDERR_FILENO, fmt, ap);
#else
#error FIXME
#endif
  va_end(ap);
  abort();
}

/*----------------------------------------------------------------------------*/

#ifndef mdbx_asprintf
int mdbx_asprintf(char **strp, const char *fmt, ...) {
  va_list ap, ones;

  va_start(ap, fmt);
  va_copy(ones, ap);
#ifdef _MSC_VER
  int needed = _vscprintf(fmt, ap);
#elif defined(vsnprintf) || defined(_BSD_SOURCE) || _XOPEN_SOURCE >= 500 ||    \
    defined(_ISOC99_SOURCE) || _POSIX_C_SOURCE >= 200112L
  int needed = vsnprintf(nullptr, 0, fmt, ap);
#else
#error FIXME
#endif
  va_end(ap);

  if (unlikely(needed < 0 || needed >= INT_MAX)) {
    *strp = NULL;
    va_end(ones);
    return needed;
  }

  *strp = malloc(needed + 1);
  if (unlikely(*strp == NULL)) {
    va_end(ones);
    return -ENOMEM;
  }

#if defined(vsnprintf) || defined(_BSD_SOURCE) || _XOPEN_SOURCE >= 500 ||      \
    defined(_ISOC99_SOURCE) || _POSIX_C_SOURCE >= 200112L
  int actual = vsnprintf(*strp, needed + 1, fmt, ones);
#else
#error FIXME
#endif
  va_end(ones);

  assert(actual == needed);
  if (unlikely(actual < 0)) {
    free(*strp);
    *strp = NULL;
  }
  return actual;
}
#endif /* mdbx_asprintf */

#ifndef mdbx_memalign_alloc
int mdbx_memalign_alloc(size_t alignment, size_t bytes, void **result) {
#if _MSC_VER
  *result = _aligned_malloc(bytes, alignment);
  return *result ? MDB_SUCCESS : ERROR_OUTOFMEMORY;
#elif __GLIBC_PREREQ(2, 16) || __STDC_VERSION__ >= 201112L
  *result = memalign(alignment, bytes);
  return *result ? MDB_SUCCESS : errno;
#elif _POSIX_VERSION >= 200112L
  *result = NULL;
  return posix_memalign(result, alignment, bytes);
#else
#error FIXME
#endif
}
#endif /* mdbx_memalign_alloc */

#ifndef mdbx_memalign_free
void mdbx_memalign_free(void *ptr) {
#if _MSC_VER
  _aligned_free(ptr);
#else
  free(ptr);
#endif
}
#endif /* mdbx_memalign_free */

/*----------------------------------------------------------------------------*/

int mdbx_mutex_init(mdbx_mutex_t *mutex) {
#if defined(_WIN32) || defined(_WIN64)
  *mutex = CreateMutex(NULL, FALSE, NULL);
  return *mutex ? MDB_SUCCESS : GetLastError();
#else
  return pthread_mutex_init(mutex, NULL);
#endif
}

int mdbx_mutex_destroy(mdbx_mutex_t *mutex) {
#if defined(_WIN32) || defined(_WIN64)
  return CloseHandle(*mutex) ? MDB_SUCCESS : GetLastError();
#else
  return pthread_mutex_destroy(mutex);
#endif
}

int mdbx_mutex_lock(mdbx_mutex_t *mutex) {
#if defined(_WIN32) || defined(_WIN64)
  DWORD code = WaitForSingleObject(*mutex, INFINITE);
  return waitstatus2errcode(code);
#else
  return pthread_mutex_lock(mutex);
#endif
}

int mdbx_mutex_unlock(mdbx_mutex_t *mutex) {
#if defined(_WIN32) || defined(_WIN64)
  return ReleaseMutex(*mutex) ? MDB_SUCCESS : GetLastError();
#else
  return pthread_mutex_unlock(mutex);
#endif
}

/*----------------------------------------------------------------------------*/

int mdbx_cond_init(mdbx_cond_t *cond) {
#if defined(_WIN32) || defined(_WIN64)
  *cond = CreateEvent(NULL, FALSE, FALSE, NULL);
  return *cond ? MDB_SUCCESS : GetLastError();
#else
  return pthread_cond_init(cond, NULL);
#endif
}

#ifndef mdbx_cond_destroy
int mdbx_cond_destroy(mdbx_cond_t *cond) {
#if defined(_WIN32) || defined(_WIN64)
  return CloseHandle(*cond) ? MDB_SUCCESS : GetLastError();
#else
  return pthread_cond_destroy(cond);
#endif
}
#endif /* mdbx_cond_destroy */

int mdbx_cond_signal(mdbx_cond_t *cond) {
#if defined(_WIN32) || defined(_WIN64)
  return SetEvent(*cond) ? MDB_SUCCESS : GetLastError();
#else
  return pthread_cond_signal(cond);
#endif
}

int mdbx_cond_wait(mdbx_cond_t *cond, mdbx_mutex_t *mutex) {
#if defined(_WIN32) || defined(_WIN64)
  DWORD code = SignalObjectAndWait(*mutex, *cond, INFINITE, FALSE);
  if (code == WAIT_OBJECT_0)
    code = WaitForSingleObject(*mutex, INFINITE);
  return waitstatus2errcode(code);
#else
  return pthread_cond_wait(cond, mutex);
#endif
}

/*----------------------------------------------------------------------------*/

int mdbx_openfile(const char *pathname, int flags, mode_t mode,
                  mdbx_filehandle_t *fd) {
  *fd = INVALID_HANDLE_VALUE;
#if defined(_WIN32) || defined(_WIN64)
  (void)mode;

  DWORD DesiredAccess;
  DWORD ShareMode = FILE_SHARE_READ | FILE_SHARE_WRITE;
  DWORD FlagsAndAttributes = FILE_ATTRIBUTE_NORMAL;
  switch (flags & (O_RDONLY | O_WRONLY | O_RDWR)) {
  default:
    return ERROR_INVALID_PARAMETER;
  case O_RDONLY:
    DesiredAccess = GENERIC_READ;
    break;
  case O_WRONLY: /* assume for mdb_env_copy() and friends output */
    DesiredAccess = GENERIC_WRITE;
    ShareMode = 0;
    FlagsAndAttributes |= FILE_FLAG_NO_BUFFERING | FILE_FLAG_WRITE_THROUGH;
    break;
  case O_RDWR:
    DesiredAccess = GENERIC_READ | GENERIC_WRITE;
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

  *fd = CreateFileA(pathname, DesiredAccess, ShareMode, NULL,
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

#ifdef O_CLOEXEC
  flags |= O_CLOEXEC;
#endif
  *fd = open(pathname, flags, mode);
  if (*fd < 0)
    return errno;
#if defined(FD_CLOEXEC) && defined(F_GETFD)
  flags = fcntl(*fd, F_GETFD);
  if (flags >= 0)
    (void)fcntl(*fd, F_SETFD, flags | FD_CLOEXEC);
#endif
#endif
  return MDB_SUCCESS;
}

int mdbx_closefile(mdbx_filehandle_t fd) {
#if defined(_WIN32) || defined(_WIN64)
  return CloseHandle(fd) ? MDB_SUCCESS : GetLastError();
#else
  return (close(fd) == 0) ? MDB_SUCCESS : errno;
#endif
}

int mdbx_pread(mdbx_filehandle_t fd, void *buf, size_t bytes, off_t offset) {
#if defined(_WIN32) || defined(_WIN64)
  if (bytes > MAX_WRITE)
    return ERROR_INVALID_PARAMETER;

  OVERLAPPED ov;
  ov.hEvent = 0;
  ov.Offset = (DWORD)offset;
  ov.OffsetHigh = HIGH_DWORD(offset);

  DWORD read;
  if (unlikely(!ReadFile(fd, buf, (DWORD)bytes, &read, &ov))) {
    int rc = GetLastError();
    if (rc == ERROR_HANDLE_EOF && read == 0 && offset == 0)
      return ENOENT;
    return rc;
  }
  return (read == bytes) ? MDB_SUCCESS : ERROR_READ_FAULT;
#else
  ssize_t read = pread(fd, buf, bytes, offset);
  if (likely(bytes == (size_t)read))
    return MDB_SUCCESS;
  if (read < 0)
    return errno;
  return (read == 0 && offset == 0) ? ENOENT : EIO;
#endif
}

int mdbx_pwrite(mdbx_filehandle_t fd, const void *buf, size_t bytes,
                off_t offset) {
#if defined(_WIN32) || defined(_WIN64)
  if (bytes > MAX_WRITE)
    return ERROR_INVALID_PARAMETER;

  OVERLAPPED ov;
  ov.hEvent = 0;
  ov.Offset = (DWORD)offset;
  ov.OffsetHigh = HIGH_DWORD(offset);

  DWORD written;
  if (likely(WriteFile(fd, buf, (DWORD)bytes, &written, &ov)))
    return (bytes == written) ? MDB_SUCCESS : ERROR_WRITE_FAULT;
  return GetLastError();
#else
  int rc;
  ssize_t written;
  do {
    written = pwrite(fd, buf, bytes, offset);
    if (likely(bytes == (size_t)written))
      return MDB_SUCCESS;
    rc = errno;
  } while (rc == EINTR);
  return (written < 0) ? rc : EIO /* Use which error code (ENOSPC)? */;
#endif
}

int mdbx_pwritev(mdbx_filehandle_t fd, struct iovec *iov, int iovcnt,
                 off_t offset, size_t expected_written) {
#if defined(_WIN32) || defined(_WIN64)
  size_t written = 0;
  for (int i = 0; i > iovcnt; ++i) {
    int rc = mdbx_pwrite(fd, iov[i].iov_base, iov[i].iov_len, offset);
    if (unlikely(rc != MDB_SUCCESS))
      return rc;
    written += iov[i].iov_len;
    offset += iov[i].iov_len;
  }
  return (expected_written == written) ? MDB_SUCCESS : ERROR_WRITE_FAULT;
#else
  int rc;
  ssize_t written;
  do {
    written = pwritev(fd, iov, iovcnt, offset);
    if (likely(expected_written == (size_t)written))
      return MDB_SUCCESS;
    rc = errno;
  } while (rc == EINTR);
  return (written < 0) ? rc : EIO /* Use which error code? */;
#endif
}

int mdbx_write(mdbx_filehandle_t fd, const void *buf, size_t bytes) {
#ifdef SIGPIPE
  sigset_t set, old;
  sigemptyset(&set);
  sigaddset(&set, SIGPIPE);
  int rc = rc = pthread_sigmask(SIG_BLOCK, &set, &old);
  if (rc != 0)
    return rc;
#endif

  const char *ptr = buf;
  for (;;) {
    size_t chunk = (MAX_WRITE < bytes) ? MAX_WRITE : bytes;
#if defined(_WIN32) || defined(_WIN64)
    DWORD written;
    if (unlikely(!WriteFile(fd, ptr, (DWORD)chunk, &written, NULL)))
      return GetLastError();
#else
    ssize_t written = write(fd, ptr, chunk);
    if (written < 0) {
      int rc = errno;
#ifdef SIGPIPE
      if (rc == EPIPE) {
        /* Collect the pending SIGPIPE, otherwise at least OS X
         * gives it to the process on thread-exit (ITS#8504).
         */
        int tmp;
        sigwait(&set, &tmp);
        written = 0;
        continue;
      }
      pthread_sigmask(SIG_SETMASK, &old, NULL);
#endif
      return rc;
    }
#endif
    if (likely(bytes == (size_t)written)) {
#ifdef SIGPIPE
      pthread_sigmask(SIG_SETMASK, &old, NULL);
#endif
      return MDB_SUCCESS;
    }
    ptr += written;
    bytes -= written;
  }
}

int mdbx_filesync(mdbx_filehandle_t fd, bool syncmeta) {
#if defined(_WIN32) || defined(_WIN64)
  (void)syncmeta;
  return FlushFileBuffers(fd) ? 0 : -1;
#elif __GLIBC_PREREQ(2, 16) || _BSD_SOURCE || _XOPEN_SOURCE ||                 \
    (__GLIBC_PREREQ(2, 8) && _POSIX_C_SOURCE >= 200112L)
#if _POSIX_C_SOURCE >= 199309L || _XOPEN_SOURCE >= 500 ||                      \
    defined(_POSIX_SYNCHRONIZED_IO)
  if (!syncmeta)
    return (fdatasync(fd) == 0) ? MDB_SUCCESS : errno;
#endif
  (void)syncmeta;
  return (fsync(fd) == 0) ? MDB_SUCCESS : errno;
#else
#error FIXME
#endif
}

int mdbx_filesize(mdbx_filehandle_t fd, off_t *length) {
#if defined(_WIN32) || defined(_WIN64)
  BY_HANDLE_FILE_INFORMATION info;
  if (!GetFileInformationByHandle(fd, &info))
    return GetLastError();
  *length = info.nFileSizeLow | (uint64_t)info.nFileIndexHigh << 32;
#else
  struct stat st;

  if (fstat(fd, &st))
    return errno;

  *length = st.st_size;
#endif
  return MDB_SUCCESS;
}

int mdbx_ftruncate(mdbx_filehandle_t fd, off_t length) {
#if defined(_WIN32) || defined(_WIN64)
  LARGE_INTEGER li;
  li.QuadPart = length;
  return (SetFilePointerEx(fd, li, NULL, FILE_BEGIN) && SetEndOfFile(fd))
             ? MDB_SUCCESS
             : GetLastError();
#else
  return ftruncate(fd, length) == 0 ? MDB_SUCCESS : errno;
#endif
}

/*----------------------------------------------------------------------------*/

int mdbx_thread_key_create(mdbx_thread_key_t *key) {
#if defined(_WIN32) || defined(_WIN64)
  *key = TlsAlloc();
  return (*key != TLS_OUT_OF_INDEXES) ? MDB_SUCCESS : GetLastError();
#else
  return pthread_key_create(key, mdbx_rthc_dtor);
#endif
}

void mdbx_thread_key_delete(mdbx_thread_key_t key) {
#if defined(_WIN32) || defined(_WIN64)
  mdbx_ensure(NULL, TlsFree(key));
#else
  mdbx_ensure(NULL, pthread_key_delete(key) == 0);
#endif
}

void *mdbx_thread_rthc_get(mdbx_thread_key_t key) {
#if defined(_WIN32) || defined(_WIN64)
  return TlsGetValue(key);
#else
  return pthread_getspecific(key);
#endif
}

void mdbx_thread_rthc_set(mdbx_thread_key_t key, const void *value) {
#if defined(_WIN32) || defined(_WIN64)
  mdbx_ensure(NULL, TlsSetValue(key, (void *)value));
#else
  mdbx_ensure(NULL, pthread_setspecific(key, value) == 0);
#endif
}

mdbx_tid_t mdbx_thread_self(void) {
#if defined(_WIN32) || defined(_WIN64)
  return GetCurrentThreadId();
#else
  return pthread_self();
#endif
}

int mdbx_thread_create(mdbx_thread_t *thread,
                       THREAD_RESULT(THREAD_CALL *start_routine)(void *),
                       void *arg) {
#if defined(_WIN32) || defined(_WIN64)
  *thread = CreateThread(NULL, 0, start_routine, arg, 0, NULL);
  return *thread ? MDB_SUCCESS : GetLastError();
#else
  return pthread_create(thread, NULL, start_routine, arg);
#endif
}

int mdbx_thread_join(mdbx_thread_t thread) {
#if defined(_WIN32) || defined(_WIN64)
  DWORD code = WaitForSingleObject(thread, INFINITE);
  return waitstatus2errcode(code);
#else
  void *unused_retval = &unused_retval;
  return pthread_join(thread, &unused_retval);
#endif
}

/*----------------------------------------------------------------------------*/

int mdbx_msync(void *addr, size_t length, int async) {
#if defined(_WIN32) || defined(_WIN64)
  if (async)
    return MDB_SUCCESS;
  return FlushViewOfFile(addr, length) ? 0 : GetLastError();
#else
  return (msync(addr, length, async ? MS_ASYNC : MS_SYNC) == 0) ? MDB_SUCCESS
                                                                : errno;
#endif
}

int mdbx_mremap_size(void **address, size_t old_size, size_t new_size) {
#if defined(_WIN32) || defined(_WIN64)
  *address = MAP_FAILED;
  (void)old_size;
  (void)new_size;
  return ERROR_NOT_SUPPORTED;
#else
  *address = mremap(*address, old_size, new_size, 0, address);
  return (*address != MAP_FAILED) ? MDB_SUCCESS : errno;
#endif
}

int mdbx_mmap(void **address, size_t length, int rw, mdbx_filehandle_t fd) {
#if defined(_WIN32) || defined(_WIN64)
  HANDLE h = CreateFileMapping(fd, NULL, rw ? PAGE_READWRITE : PAGE_READONLY,
                               HIGH_DWORD(length), (DWORD)length, NULL);
  if (!h)
    return GetLastError();
  *address = MapViewOfFileEx(h, rw ? FILE_MAP_WRITE : FILE_MAP_READ, 0, 0,
                             length, *address);
  int rc = (*address != MAP_FAILED) ? MDB_SUCCESS : GetLastError();
  CloseHandle(h);
  return rc;
#else
  *address = mmap(NULL, length, rw ? PROT_READ | PROT_WRITE : PROT_READ,
                  MAP_SHARED, fd, 0);
  return (*address != MAP_FAILED) ? MDB_SUCCESS : errno;
#endif
}

int mdbx_munmap(void *address, size_t length) {
#if defined(_WIN32) || defined(_WIN64)
  (void)length;
  return UnmapViewOfFile(address) ? MDB_SUCCESS : GetLastError();
#else
  return (munmap(address, length) == 0) ? MDB_SUCCESS : errno;
#endif
}

int mdbx_mlock(const void *address, size_t length) {
#if defined(_WIN32) || defined(_WIN64)
  return VirtualLock((void *)address, length) ? MDB_SUCCESS : GetLastError();
#else
  return (mlock(address, length) == 0) ? MDB_SUCCESS : errno;
#endif
}
