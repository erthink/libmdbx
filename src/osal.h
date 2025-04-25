/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025
///
/// https://en.wikipedia.org/wiki/Operating_system_abstraction_layer

#pragma once

#include "essentials.h"

/*----------------------------------------------------------------------------*/
/* Memory/Compiler barriers, cache coherence */

#if __has_include(<sys/cachectl.h>)
#include <sys/cachectl.h>
#elif defined(__mips) || defined(__mips__) || defined(__mips64) || defined(__mips64__) || defined(_M_MRX000) ||        \
    defined(_MIPS_) || defined(__MWERKS__) || defined(__sgi)
/* MIPS should have explicit cache control */
#include <sys/cachectl.h>
#endif

MDBX_MAYBE_UNUSED static inline void osal_compiler_barrier(void) {
#if defined(__clang__) || defined(__GNUC__)
  __asm__ __volatile__("" ::: "memory");
#elif defined(_MSC_VER)
  _ReadWriteBarrier();
#elif defined(__INTEL_COMPILER) /* LY: Intel Compiler may mimic GCC and MSC */
  __memory_barrier();
#elif defined(__SUNPRO_C) || defined(__sun) || defined(sun)
  __compiler_barrier();
#elif (defined(_HPUX_SOURCE) || defined(__hpux) || defined(__HP_aCC)) && (defined(HP_IA64) || defined(__ia64))
  _Asm_sched_fence(/* LY: no-arg meaning 'all expect ALU', e.g. 0x3D3D */);
#elif defined(_AIX) || defined(__ppc__) || defined(__powerpc__) || defined(__ppc64__) || defined(__powerpc64__)
  __fence();
#else
#error "Could not guess the kind of compiler, please report to us."
#endif
}

MDBX_MAYBE_UNUSED static inline void osal_memory_barrier(void) {
#ifdef MDBX_HAVE_C11ATOMICS
  atomic_thread_fence(memory_order_seq_cst);
#elif defined(__ATOMIC_SEQ_CST)
#ifdef __clang__
  __c11_atomic_thread_fence(__ATOMIC_SEQ_CST);
#else
  __atomic_thread_fence(__ATOMIC_SEQ_CST);
#endif
#elif defined(__clang__) || defined(__GNUC__)
  __sync_synchronize();
#elif defined(_WIN32) || defined(_WIN64)
  MemoryBarrier();
#elif defined(__INTEL_COMPILER) /* LY: Intel Compiler may mimic GCC and MSC */
#if defined(__ia32__)
  _mm_mfence();
#else
  __mf();
#endif
#elif defined(__SUNPRO_C) || defined(__sun) || defined(sun)
  __machine_rw_barrier();
#elif (defined(_HPUX_SOURCE) || defined(__hpux) || defined(__HP_aCC)) && (defined(HP_IA64) || defined(__ia64))
  _Asm_mf();
#elif defined(_AIX) || defined(__ppc__) || defined(__powerpc__) || defined(__ppc64__) || defined(__powerpc64__)
  __lwsync();
#else
#error "Could not guess the kind of compiler, please report to us."
#endif
}

/*----------------------------------------------------------------------------*/
/* system-depended definitions */

#if defined(_WIN32) || defined(_WIN64)
#define HAVE_SYS_STAT_H
#define HAVE_SYS_TYPES_H
typedef HANDLE osal_thread_t;
typedef unsigned osal_thread_key_t;
#define MAP_FAILED nullptr
#define HIGH_DWORD(v) ((DWORD)((sizeof(v) > 4) ? ((uint64_t)(v) >> 32) : 0))
#define THREAD_CALL WINAPI
#define THREAD_RESULT DWORD
typedef struct {
  HANDLE mutex;
  HANDLE event[2];
} osal_condpair_t;
typedef CRITICAL_SECTION osal_fastmutex_t;

#if !defined(_MSC_VER) && !defined(__try)
/* *INDENT-OFF* */
/* clang-format off */
#define __try
#define __except(COND) if (/* (void)(COND), */ false)
/* *INDENT-ON* */
/* clang-format on */
#endif /* stub for MSVC's __try/__except */

#if MDBX_WITHOUT_MSVC_CRT

#ifndef osal_malloc
static inline void *osal_malloc(size_t bytes) { return HeapAlloc(GetProcessHeap(), 0, bytes); }
#endif /* osal_malloc */

#ifndef osal_calloc
static inline void *osal_calloc(size_t nelem, size_t size) {
  return HeapAlloc(GetProcessHeap(), HEAP_ZERO_MEMORY, nelem * size);
}
#endif /* osal_calloc */

#ifndef osal_realloc
static inline void *osal_realloc(void *ptr, size_t bytes) {
  return ptr ? HeapReAlloc(GetProcessHeap(), 0, ptr, bytes) : HeapAlloc(GetProcessHeap(), 0, bytes);
}
#endif /* osal_realloc */

#ifndef osal_free
static inline void osal_free(void *ptr) { HeapFree(GetProcessHeap(), 0, ptr); }
#endif /* osal_free */

#else /* MDBX_WITHOUT_MSVC_CRT */

#define osal_malloc malloc
#define osal_calloc calloc
#define osal_realloc realloc
#define osal_free free
#define osal_strdup _strdup

#endif /* MDBX_WITHOUT_MSVC_CRT */

#ifndef snprintf
#define snprintf _snprintf /* ntdll */
#endif

#ifndef vsnprintf
#define vsnprintf _vsnprintf /* ntdll */
#endif

#else /*----------------------------------------------------------------------*/

typedef pthread_t osal_thread_t;
typedef pthread_key_t osal_thread_key_t;
#define INVALID_HANDLE_VALUE (-1)
#define THREAD_CALL
#define THREAD_RESULT void *
typedef struct {
  pthread_mutex_t mutex;
  pthread_cond_t cond[2];
} osal_condpair_t;
typedef pthread_mutex_t osal_fastmutex_t;
#define osal_malloc malloc
#define osal_calloc calloc
#define osal_realloc realloc
#define osal_free free
#define osal_strdup strdup
#endif /* Platform */

#if __GLIBC_PREREQ(2, 12) || defined(__FreeBSD__) || defined(malloc_usable_size)
#define osal_malloc_usable_size(ptr) malloc_usable_size(ptr)
#elif defined(__APPLE__)
#define osal_malloc_usable_size(ptr) malloc_size(ptr)
#elif defined(_MSC_VER) && !MDBX_WITHOUT_MSVC_CRT
#define osal_malloc_usable_size(ptr) _msize(ptr)
#endif /* osal_malloc_usable_size */

/*----------------------------------------------------------------------------*/
/* OS abstraction layer stuff */

#if defined(_WIN32) || defined(_WIN64)
typedef wchar_t pathchar_t;
#define MDBX_PRIsPATH "ls"
#else
typedef char pathchar_t;
#define MDBX_PRIsPATH "s"
#endif

typedef struct osal_mmap {
  union {
    void *base;
    struct shared_lck *lck;
  };
  mdbx_filehandle_t fd;
  size_t limit;   /* mapping length, but NOT a size of file nor DB */
  size_t current; /* mapped region size, i.e. the size of file and DB */
  uint64_t filesize /* in-process cache of a file size */;
#if defined(_WIN32) || defined(_WIN64)
  HANDLE section; /* memory-mapped section handle */
#endif
} osal_mmap_t;

#ifndef MDBX_HAVE_PWRITEV
#if defined(_WIN32) || defined(_WIN64)

#define MDBX_HAVE_PWRITEV 0

#elif defined(__ANDROID_API__)

#if __ANDROID_API__ < 24
#define MDBX_HAVE_PWRITEV 0
#else
#define MDBX_HAVE_PWRITEV 1
#endif

#elif defined(__APPLE__) || defined(__MACH__) || defined(_DARWIN_C_SOURCE)

#if defined(MAC_OS_X_VERSION_MIN_REQUIRED) && defined(MAC_OS_VERSION_11_0) &&                                          \
    MAC_OS_X_VERSION_MIN_REQUIRED >= MAC_OS_VERSION_11_0
/* FIXME: add checks for IOS versions, etc */
#define MDBX_HAVE_PWRITEV 1
#else
#define MDBX_HAVE_PWRITEV 0
#endif

#elif defined(_SC_IOV_MAX) || (defined(IOV_MAX) && IOV_MAX > 1)
#define MDBX_HAVE_PWRITEV 1
#else
#define MDBX_HAVE_PWRITEV 0
#endif
#endif /* MDBX_HAVE_PWRITEV */

typedef struct ior_item {
#if defined(_WIN32) || defined(_WIN64)
  OVERLAPPED ov;
#define ior_sgv_gap4terminator 1
#define ior_sgv_element FILE_SEGMENT_ELEMENT
#else
  size_t offset;
#if MDBX_HAVE_PWRITEV
  size_t sgvcnt;
#define ior_sgv_gap4terminator 0
#define ior_sgv_element struct iovec
#endif /* MDBX_HAVE_PWRITEV */
#endif /* !Windows */
  union {
    MDBX_val single;
#if defined(ior_sgv_element)
    ior_sgv_element sgv[1 + ior_sgv_gap4terminator];
#endif /* ior_sgv_element */
  };
} ior_item_t;

typedef struct osal_ioring {
  unsigned slots_left;
  unsigned allocated;
#if defined(_WIN32) || defined(_WIN64)
#define IOR_STATE_LOCKED 1
  HANDLE overlapped_fd;
  unsigned pagesize;
  unsigned last_sgvcnt;
  size_t last_bytes;
  uint8_t direct, state, pagesize_ln2;
  unsigned event_stack;
  HANDLE *event_pool;
  volatile LONG async_waiting;
  volatile LONG async_completed;
  HANDLE async_done;

#define ior_last_sgvcnt(ior, item) (ior)->last_sgvcnt
#define ior_last_bytes(ior, item) (ior)->last_bytes
#elif MDBX_HAVE_PWRITEV
  unsigned last_bytes;
#define ior_last_sgvcnt(ior, item) (item)->sgvcnt
#define ior_last_bytes(ior, item) (ior)->last_bytes
#else
#define ior_last_sgvcnt(ior, item) (1)
#define ior_last_bytes(ior, item) (item)->single.iov_len
#endif /* !Windows */
  ior_item_t *last;
  ior_item_t *pool;
  char *boundary;
} osal_ioring_t;

/* Actually this is not ioring for now, but on the way. */
MDBX_INTERNAL int osal_ioring_create(osal_ioring_t *
#if defined(_WIN32) || defined(_WIN64)
                                     ,
                                     bool enable_direct, mdbx_filehandle_t overlapped_fd
#endif /* Windows */
);
MDBX_INTERNAL int osal_ioring_resize(osal_ioring_t *, size_t items);
MDBX_INTERNAL void osal_ioring_destroy(osal_ioring_t *);
MDBX_INTERNAL void osal_ioring_reset(osal_ioring_t *);
MDBX_INTERNAL int osal_ioring_add(osal_ioring_t *ctx, const size_t offset, void *data, const size_t bytes);
typedef struct osal_ioring_write_result {
  int err;
  unsigned wops;
} osal_ioring_write_result_t;
MDBX_INTERNAL osal_ioring_write_result_t osal_ioring_write(osal_ioring_t *ior, mdbx_filehandle_t fd);

MDBX_INTERNAL void osal_ioring_walk(osal_ioring_t *ior, iov_ctx_t *ctx,
                                    void (*callback)(iov_ctx_t *ctx, size_t offset, void *data, size_t bytes));

MDBX_MAYBE_UNUSED static inline unsigned osal_ioring_left(const osal_ioring_t *ior) { return ior->slots_left; }

MDBX_MAYBE_UNUSED static inline unsigned osal_ioring_used(const osal_ioring_t *ior) {
  return ior->allocated - ior->slots_left;
}

MDBX_MAYBE_UNUSED static inline int osal_ioring_prepare(osal_ioring_t *ior, size_t items, size_t bytes) {
  items = (items > 32) ? items : 32;
#if defined(_WIN32) || defined(_WIN64)
  if (ior->direct) {
    const size_t npages = bytes >> ior->pagesize_ln2;
    items = (items > npages) ? items : npages;
  }
#else
  (void)bytes;
#endif
  items = (items < 65536) ? items : 65536;
  if (likely(ior->allocated >= items))
    return MDBX_SUCCESS;
  return osal_ioring_resize(ior, items);
}

/*----------------------------------------------------------------------------*/
/* libc compatibility stuff */

#if (!defined(__GLIBC__) && __GLIBC_PREREQ(2, 1)) && (defined(_GNU_SOURCE) || defined(_BSD_SOURCE))
#define osal_asprintf asprintf
#define osal_vasprintf vasprintf
#else
MDBX_MAYBE_UNUSED MDBX_INTERNAL MDBX_PRINTF_ARGS(2, 3) int osal_asprintf(char **strp, const char *fmt, ...);
MDBX_INTERNAL int osal_vasprintf(char **strp, const char *fmt, va_list ap);
#endif

#if !defined(MADV_DODUMP) && defined(MADV_CORE)
#define MADV_DODUMP MADV_CORE
#endif /* MADV_CORE -> MADV_DODUMP */

#if !defined(MADV_DONTDUMP) && defined(MADV_NOCORE)
#define MADV_DONTDUMP MADV_NOCORE
#endif /* MADV_NOCORE -> MADV_DONTDUMP */

MDBX_MAYBE_UNUSED MDBX_INTERNAL void osal_jitter(bool tiny);

/* max bytes to write in one call */
#if defined(_WIN64)
#define MAX_WRITE UINT32_C(0x10000000)
#elif defined(_WIN32)
#define MAX_WRITE UINT32_C(0x04000000)
#else
#define MAX_WRITE UINT32_C(0x3f000000)

#if defined(F_GETLK64) && defined(F_SETLK64) && defined(F_SETLKW64) && !defined(__ANDROID_API__)
#define MDBX_F_SETLK F_SETLK64
#define MDBX_F_SETLKW F_SETLKW64
#define MDBX_F_GETLK F_GETLK64
#if (__GLIBC_PREREQ(2, 28) && (defined(__USE_LARGEFILE64) || defined(__LARGEFILE64_SOURCE) ||                          \
                               defined(_USE_LARGEFILE64) || defined(_LARGEFILE64_SOURCE))) ||                          \
    defined(fcntl64)
#define MDBX_FCNTL fcntl64
#else
#define MDBX_FCNTL fcntl
#endif
#define MDBX_STRUCT_FLOCK struct flock64
#ifndef OFF_T_MAX
#define OFF_T_MAX UINT64_C(0x7fffFFFFfff00000)
#endif /* OFF_T_MAX */
#else
#define MDBX_F_SETLK F_SETLK
#define MDBX_F_SETLKW F_SETLKW
#define MDBX_F_GETLK F_GETLK
#define MDBX_FCNTL fcntl
#define MDBX_STRUCT_FLOCK struct flock
#endif /* MDBX_F_SETLK, MDBX_F_SETLKW, MDBX_F_GETLK */

#if defined(F_OFD_SETLK64) && defined(F_OFD_SETLKW64) && defined(F_OFD_GETLK64) && !defined(__ANDROID_API__)
#define MDBX_F_OFD_SETLK F_OFD_SETLK64
#define MDBX_F_OFD_SETLKW F_OFD_SETLKW64
#define MDBX_F_OFD_GETLK F_OFD_GETLK64
#else
#define MDBX_F_OFD_SETLK F_OFD_SETLK
#define MDBX_F_OFD_SETLKW F_OFD_SETLKW
#define MDBX_F_OFD_GETLK F_OFD_GETLK
#ifndef OFF_T_MAX
#define OFF_T_MAX (((sizeof(off_t) > 4) ? INT64_MAX : INT32_MAX) & ~(size_t)0xFffff)
#endif /* OFF_T_MAX */
#endif /* MDBX_F_OFD_SETLK64, MDBX_F_OFD_SETLKW64, MDBX_F_OFD_GETLK64 */

#endif /* !Windows */

#ifndef osal_strdup
LIBMDBX_API char *osal_strdup(const char *str);
#endif

MDBX_MAYBE_UNUSED static inline int osal_get_errno(void) {
#if defined(_WIN32) || defined(_WIN64)
  DWORD rc = GetLastError();
#else
  int rc = errno;
#endif
  return rc;
}

#ifndef osal_memalign_alloc
MDBX_INTERNAL int osal_memalign_alloc(size_t alignment, size_t bytes, void **result);
#endif
#ifndef osal_memalign_free
MDBX_INTERNAL void osal_memalign_free(void *ptr);
#endif

MDBX_INTERNAL int osal_condpair_init(osal_condpair_t *condpair);
MDBX_INTERNAL int osal_condpair_lock(osal_condpair_t *condpair);
MDBX_INTERNAL int osal_condpair_unlock(osal_condpair_t *condpair);
MDBX_INTERNAL int osal_condpair_signal(osal_condpair_t *condpair, bool part);
MDBX_INTERNAL int osal_condpair_wait(osal_condpair_t *condpair, bool part);
MDBX_INTERNAL int osal_condpair_destroy(osal_condpair_t *condpair);

MDBX_INTERNAL int osal_fastmutex_init(osal_fastmutex_t *fastmutex);
MDBX_INTERNAL int osal_fastmutex_acquire(osal_fastmutex_t *fastmutex);
MDBX_INTERNAL int osal_fastmutex_release(osal_fastmutex_t *fastmutex);
MDBX_INTERNAL int osal_fastmutex_destroy(osal_fastmutex_t *fastmutex);

MDBX_INTERNAL int osal_pwritev(mdbx_filehandle_t fd, struct iovec *iov, size_t sgvcnt, uint64_t offset);
MDBX_INTERNAL int osal_pread(mdbx_filehandle_t fd, void *buf, size_t count, uint64_t offset);
MDBX_INTERNAL int osal_pwrite(mdbx_filehandle_t fd, const void *buf, size_t count, uint64_t offset);
MDBX_INTERNAL int osal_write(mdbx_filehandle_t fd, const void *buf, size_t count);

MDBX_INTERNAL int osal_thread_create(osal_thread_t *thread, THREAD_RESULT(THREAD_CALL *start_routine)(void *),
                                     void *arg);
MDBX_INTERNAL int osal_thread_join(osal_thread_t thread);

enum osal_syncmode_bits {
  MDBX_SYNC_NONE = 0,
  MDBX_SYNC_KICK = 1,
  MDBX_SYNC_DATA = 2,
  MDBX_SYNC_SIZE = 4,
  MDBX_SYNC_IODQ = 8
};

MDBX_INTERNAL int osal_fsync(mdbx_filehandle_t fd, const enum osal_syncmode_bits mode_bits);
MDBX_INTERNAL int osal_ftruncate(mdbx_filehandle_t fd, uint64_t length);
MDBX_INTERNAL int osal_fseek(mdbx_filehandle_t fd, uint64_t pos);
MDBX_INTERNAL int osal_filesize(mdbx_filehandle_t fd, uint64_t *length);

enum osal_openfile_purpose {
  MDBX_OPEN_DXB_READ,
  MDBX_OPEN_DXB_LAZY,
  MDBX_OPEN_DXB_DSYNC,
#if defined(_WIN32) || defined(_WIN64)
  MDBX_OPEN_DXB_OVERLAPPED,
  MDBX_OPEN_DXB_OVERLAPPED_DIRECT,
#endif /* Windows */
  MDBX_OPEN_LCK,
  MDBX_OPEN_COPY,
  MDBX_OPEN_DELETE
};

MDBX_MAYBE_UNUSED static inline bool osal_isdirsep(pathchar_t c) {
  return
#if defined(_WIN32) || defined(_WIN64)
      c == '\\' ||
#endif
      c == '/';
}

MDBX_INTERNAL bool osal_pathequal(const pathchar_t *l, const pathchar_t *r, size_t len);
MDBX_INTERNAL pathchar_t *osal_fileext(const pathchar_t *pathname, size_t len);
MDBX_INTERNAL int osal_fileexists(const pathchar_t *pathname);
MDBX_INTERNAL int osal_openfile(const enum osal_openfile_purpose purpose, const MDBX_env *env,
                                const pathchar_t *pathname, mdbx_filehandle_t *fd, mdbx_mode_t unix_mode_bits);
MDBX_INTERNAL int osal_closefile(mdbx_filehandle_t fd);
MDBX_INTERNAL int osal_removefile(const pathchar_t *pathname);
MDBX_INTERNAL int osal_removedirectory(const pathchar_t *pathname);
MDBX_INTERNAL int osal_is_pipe(mdbx_filehandle_t fd);
MDBX_INTERNAL int osal_lockfile(mdbx_filehandle_t fd, bool wait);

#define MMAP_OPTION_TRUNCATE 1
#define MMAP_OPTION_SEMAPHORE 2
MDBX_INTERNAL int osal_mmap(const int flags, osal_mmap_t *map, size_t size, const size_t limit, const unsigned options,
                            const pathchar_t *pathname4logging);
MDBX_INTERNAL int osal_munmap(osal_mmap_t *map);
#define MDBX_MRESIZE_MAY_MOVE 0x00000100
#define MDBX_MRESIZE_MAY_UNMAP 0x00000200
MDBX_INTERNAL int osal_mresize(const int flags, osal_mmap_t *map, size_t size, size_t limit);
#if defined(_WIN32) || defined(_WIN64)
typedef struct {
  unsigned limit, count;
  HANDLE handles[31];
} mdbx_handle_array_t;
MDBX_INTERNAL int osal_suspend_threads_before_remap(MDBX_env *env, mdbx_handle_array_t **array);
MDBX_INTERNAL int osal_resume_threads_after_remap(mdbx_handle_array_t *array);
#endif /* Windows */
MDBX_INTERNAL int osal_msync(const osal_mmap_t *map, size_t offset, size_t length, enum osal_syncmode_bits mode_bits);
MDBX_INTERNAL int osal_check_fs_rdonly(mdbx_filehandle_t handle, const pathchar_t *pathname, int err);
MDBX_INTERNAL int osal_check_fs_incore(mdbx_filehandle_t handle);
MDBX_INTERNAL int osal_check_fs_local(mdbx_filehandle_t handle, int flags);

MDBX_MAYBE_UNUSED static inline uint32_t osal_getpid(void) {
  STATIC_ASSERT(sizeof(mdbx_pid_t) <= sizeof(uint32_t));
#if defined(_WIN32) || defined(_WIN64)
  return GetCurrentProcessId();
#else
  STATIC_ASSERT(sizeof(pid_t) <= sizeof(uint32_t));
  return getpid();
#endif
}

MDBX_MAYBE_UNUSED static inline uintptr_t osal_thread_self(void) {
  mdbx_tid_t thunk;
  STATIC_ASSERT(sizeof(uintptr_t) >= sizeof(thunk));
#if defined(_WIN32) || defined(_WIN64)
  thunk = GetCurrentThreadId();
#else
  thunk = pthread_self();
#endif
  return (uintptr_t)thunk;
}

#if !defined(_WIN32) && !defined(_WIN64)
#if defined(__ANDROID_API__) || defined(ANDROID) || defined(BIONIC)
MDBX_INTERNAL int osal_check_tid4bionic(void);
#else
static inline int osal_check_tid4bionic(void) { return 0; }
#endif /* __ANDROID_API__ || ANDROID) || BIONIC */

MDBX_MAYBE_UNUSED static inline int osal_pthread_mutex_lock(pthread_mutex_t *mutex) {
  int err = osal_check_tid4bionic();
  return unlikely(err) ? err : pthread_mutex_lock(mutex);
}
#endif /* !Windows */

MDBX_INTERNAL uint64_t osal_monotime(void);
MDBX_INTERNAL uint64_t osal_cputime(size_t *optional_page_faults);
MDBX_INTERNAL uint64_t osal_16dot16_to_monotime(uint32_t seconds_16dot16);
MDBX_INTERNAL uint32_t osal_monotime_to_16dot16(uint64_t monotime);

MDBX_MAYBE_UNUSED static inline uint32_t osal_monotime_to_16dot16_noUnderflow(uint64_t monotime) {
  uint32_t seconds_16dot16 = osal_monotime_to_16dot16(monotime);
  return seconds_16dot16 ? seconds_16dot16 : /* fix underflow */ (monotime > 0);
}

/*----------------------------------------------------------------------------*/

MDBX_INTERNAL void osal_ctor(void);
MDBX_INTERNAL void osal_dtor(void);

#if defined(_WIN32) || defined(_WIN64)
MDBX_INTERNAL int osal_mb2w(const char *const src, wchar_t **const pdst);
#endif /* Windows */

typedef union bin128 {
  __anonymous_struct_extension__ struct {
    uint64_t x, y;
  };
  __anonymous_struct_extension__ struct {
    uint32_t a, b, c, d;
  };
} bin128_t;

MDBX_INTERNAL bin128_t osal_guid(const MDBX_env *);

/*----------------------------------------------------------------------------*/

MDBX_MAYBE_UNUSED MDBX_NOTHROW_PURE_FUNCTION static inline uint64_t osal_bswap64(uint64_t v) {
#if __GNUC_PREREQ(4, 4) || __CLANG_PREREQ(4, 0) || __has_builtin(__builtin_bswap64)
  return __builtin_bswap64(v);
#elif defined(_MSC_VER) && !defined(__clang__)
  return _byteswap_uint64(v);
#elif defined(__bswap_64)
  return __bswap_64(v);
#elif defined(bswap_64)
  return bswap_64(v);
#else
  return v << 56 | v >> 56 | ((v << 40) & UINT64_C(0x00ff000000000000)) | ((v << 24) & UINT64_C(0x0000ff0000000000)) |
         ((v << 8) & UINT64_C(0x000000ff00000000)) | ((v >> 8) & UINT64_C(0x00000000ff000000)) |
         ((v >> 24) & UINT64_C(0x0000000000ff0000)) | ((v >> 40) & UINT64_C(0x000000000000ff00));
#endif
}

MDBX_MAYBE_UNUSED MDBX_NOTHROW_PURE_FUNCTION static inline uint32_t osal_bswap32(uint32_t v) {
#if __GNUC_PREREQ(4, 4) || __CLANG_PREREQ(4, 0) || __has_builtin(__builtin_bswap32)
  return __builtin_bswap32(v);
#elif defined(_MSC_VER) && !defined(__clang__)
  return _byteswap_ulong(v);
#elif defined(__bswap_32)
  return __bswap_32(v);
#elif defined(bswap_32)
  return bswap_32(v);
#else
  return v << 24 | v >> 24 | ((v << 8) & UINT32_C(0x00ff0000)) | ((v >> 8) & UINT32_C(0x0000ff00));
#endif
}
