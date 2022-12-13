/*******************************************************************************
 *******************************************************************************
 *******************************************************************************
 *
 *
 *         ####   #####    #####     #     ####   #    #   ####
 *        #    #  #    #     #       #    #    #  ##   #  #
 *        #    #  #    #     #       #    #    #  # #  #   ####
 *        #    #  #####      #       #    #    #  #  # #       #
 *        #    #  #          #       #    #    #  #   ##  #    #
 *         ####   #          #       #     ####   #    #   ####
 *
 *
 */

/** \defgroup build_option Build options
 * The libmdbx build options.
 @{ */

/** Using fcntl(F_FULLFSYNC) with 5-10 times slowdown */
#define MDBX_OSX_WANNA_DURABILITY 0
/** Using fsync() with chance of data lost on power failure */
#define MDBX_OSX_WANNA_SPEED 1

#ifndef MDBX_OSX_SPEED_INSTEADOF_DURABILITY
/** Choices \ref MDBX_OSX_WANNA_DURABILITY or \ref MDBX_OSX_WANNA_SPEED
 * for OSX & iOS */
#define MDBX_OSX_SPEED_INSTEADOF_DURABILITY MDBX_OSX_WANNA_DURABILITY
#endif /* MDBX_OSX_SPEED_INSTEADOF_DURABILITY */

/** Controls checking PID against reuse DB environment after the fork() */
#ifndef MDBX_ENV_CHECKPID
#if defined(MADV_DONTFORK) || defined(_WIN32) || defined(_WIN64)
/* PID check could be omitted:
 *  - on Linux when madvise(MADV_DONTFORK) is available, i.e. after the fork()
 *    mapped pages will not be available for child process.
 *  - in Windows where fork() not available. */
#define MDBX_ENV_CHECKPID 0
#else
#define MDBX_ENV_CHECKPID 1
#endif
#define MDBX_ENV_CHECKPID_CONFIG "AUTO=" MDBX_STRINGIFY(MDBX_ENV_CHECKPID)
#elif !(MDBX_ENV_CHECKPID == 0 || MDBX_ENV_CHECKPID == 1)
#error MDBX_ENV_CHECKPID must be defined as 0 or 1
#else
#define MDBX_ENV_CHECKPID_CONFIG MDBX_STRINGIFY(MDBX_ENV_CHECKPID)
#endif /* MDBX_ENV_CHECKPID */

/** Controls checking transaction owner thread against misuse transactions from
 * other threads. */
#ifndef MDBX_TXN_CHECKOWNER
#define MDBX_TXN_CHECKOWNER 1
#define MDBX_TXN_CHECKOWNER_CONFIG "AUTO=" MDBX_STRINGIFY(MDBX_TXN_CHECKOWNER)
#elif !(MDBX_TXN_CHECKOWNER == 0 || MDBX_TXN_CHECKOWNER == 1)
#error MDBX_TXN_CHECKOWNER must be defined as 0 or 1
#else
#define MDBX_TXN_CHECKOWNER_CONFIG MDBX_STRINGIFY(MDBX_TXN_CHECKOWNER)
#endif /* MDBX_TXN_CHECKOWNER */

/** Does a system have battery-backed Real-Time Clock or just a fake. */
#ifndef MDBX_TRUST_RTC
#if defined(__linux__) || defined(__gnu_linux__) || defined(__NetBSD__) ||     \
    defined(__OpenBSD__)
#define MDBX_TRUST_RTC 0 /* a lot of embedded systems have a fake RTC */
#else
#define MDBX_TRUST_RTC 1
#endif
#define MDBX_TRUST_RTC_CONFIG "AUTO=" MDBX_STRINGIFY(MDBX_TRUST_RTC)
#elif !(MDBX_TRUST_RTC == 0 || MDBX_TRUST_RTC == 1)
#error MDBX_TRUST_RTC must be defined as 0 or 1
#else
#define MDBX_TRUST_RTC_CONFIG MDBX_STRINGIFY(MDBX_TRUST_RTC)
#endif /* MDBX_TRUST_RTC */

/** Controls online database auto-compactification during write-transactions. */
#ifndef MDBX_ENABLE_REFUND
#define MDBX_ENABLE_REFUND 1
#elif !(MDBX_ENABLE_REFUND == 0 || MDBX_ENABLE_REFUND == 1)
#error MDBX_ENABLE_REFUND must be defined as 0 or 1
#endif /* MDBX_ENABLE_REFUND */

/** Controls profiling of GC search and updates. */
#ifndef MDBX_ENABLE_PROFGC
#define MDBX_ENABLE_PROFGC 0
#elif !(MDBX_ENABLE_PROFGC == 0 || MDBX_ENABLE_PROFGC == 1)
#error MDBX_ENABLE_PROFGC must be defined as 0 or 1
#endif /* MDBX_ENABLE_PROFGC */

/** Controls gathering statistics for page operations. */
#ifndef MDBX_ENABLE_PGOP_STAT
#define MDBX_ENABLE_PGOP_STAT 1
#elif !(MDBX_ENABLE_PGOP_STAT == 0 || MDBX_ENABLE_PGOP_STAT == 1)
#error MDBX_ENABLE_PGOP_STAT must be defined as 0 or 1
#endif /* MDBX_ENABLE_PGOP_STAT */

/** Controls using Unix' mincore() to determine whether DB-pages
 * are resident in memory. */
#ifndef MDBX_ENABLE_MINCORE
#if MDBX_ENABLE_PREFAULT &&                                                    \
    (defined(MINCORE_INCORE) || !(defined(_WIN32) || defined(_WIN64)))
#define MDBX_ENABLE_MINCORE 1
#else
#define MDBX_ENABLE_MINCORE 0
#endif
#elif !(MDBX_ENABLE_MINCORE == 0 || MDBX_ENABLE_MINCORE == 1)
#error MDBX_ENABLE_MINCORE must be defined as 0 or 1
#endif /* MDBX_ENABLE_MINCORE */

/** Enables chunking long list of retired pages during huge transactions commit
 * to avoid use sequences of pages. */
#ifndef MDBX_ENABLE_BIGFOOT
#if MDBX_WORDBITS >= 64 || defined(DOXYGEN)
#define MDBX_ENABLE_BIGFOOT 1
#else
#define MDBX_ENABLE_BIGFOOT 0
#endif
#elif !(MDBX_ENABLE_BIGFOOT == 0 || MDBX_ENABLE_BIGFOOT == 1)
#error MDBX_ENABLE_BIGFOOT must be defined as 0 or 1
#endif /* MDBX_ENABLE_BIGFOOT */

/** Controls using of POSIX' madvise() and/or similar hints. */
#ifndef MDBX_ENABLE_MADVISE
#define MDBX_ENABLE_MADVISE 1
#elif !(MDBX_ENABLE_MADVISE == 0 || MDBX_ENABLE_MADVISE == 1)
#error MDBX_ENABLE_MADVISE must be defined as 0 or 1
#endif /* MDBX_ENABLE_MADVISE */

/** Disable some checks to reduce an overhead and detection probability of
 * database corruption to a values closer to the LMDB. */
#ifndef MDBX_DISABLE_VALIDATION
#define MDBX_DISABLE_VALIDATION 0
#elif !(MDBX_DISABLE_VALIDATION == 0 || MDBX_DISABLE_VALIDATION == 1)
#error MDBX_DISABLE_VALIDATION must be defined as 0 or 1
#endif /* MDBX_DISABLE_VALIDATION */

#ifndef MDBX_PNL_PREALLOC_FOR_RADIXSORT
#define MDBX_PNL_PREALLOC_FOR_RADIXSORT 1
#elif !(MDBX_PNL_PREALLOC_FOR_RADIXSORT == 0 ||                                \
        MDBX_PNL_PREALLOC_FOR_RADIXSORT == 1)
#error MDBX_PNL_PREALLOC_FOR_RADIXSORT must be defined as 0 or 1
#endif /* MDBX_PNL_PREALLOC_FOR_RADIXSORT */

#ifndef MDBX_DPL_PREALLOC_FOR_RADIXSORT
#define MDBX_DPL_PREALLOC_FOR_RADIXSORT 1
#elif !(MDBX_DPL_PREALLOC_FOR_RADIXSORT == 0 ||                                \
        MDBX_DPL_PREALLOC_FOR_RADIXSORT == 1)
#error MDBX_DPL_PREALLOC_FOR_RADIXSORT must be defined as 0 or 1
#endif /* MDBX_DPL_PREALLOC_FOR_RADIXSORT */

/** Controls dirty pages tracking, spilling and persisting in MDBX_WRITEMAP
 * mode. 0/OFF = Don't track dirty pages at all, don't spill ones, and use
 * msync() to persist data. This is by-default on Linux and other systems where
 * kernel provides properly LRU tracking and effective flushing on-demand. 1/ON
 * = Tracking of dirty pages but with LRU labels for spilling and explicit
 * persist ones by write(). This may be reasonable for systems which low
 * performance of msync() and/or LRU tracking. */
#ifndef MDBX_AVOID_MSYNC
#if defined(_WIN32) || defined(_WIN64)
#define MDBX_AVOID_MSYNC 1
#else
#define MDBX_AVOID_MSYNC 0
#endif
#elif !(MDBX_AVOID_MSYNC == 0 || MDBX_AVOID_MSYNC == 1)
#error MDBX_AVOID_MSYNC must be defined as 0 or 1
#endif /* MDBX_AVOID_MSYNC */

/** Controls sort order of internal page number lists.
 * This mostly experimental/advanced option with not for regular MDBX users.
 * \warning The database format depend on this option and libmdbx builded with
 * different option value are incompatible. */
#ifndef MDBX_PNL_ASCENDING
#define MDBX_PNL_ASCENDING 0
#elif !(MDBX_PNL_ASCENDING == 0 || MDBX_PNL_ASCENDING == 1)
#error MDBX_PNL_ASCENDING must be defined as 0 or 1
#endif /* MDBX_PNL_ASCENDING */

/** Avoid dependence from MSVC CRT and use ntdll.dll instead. */
#ifndef MDBX_WITHOUT_MSVC_CRT
#define MDBX_WITHOUT_MSVC_CRT 1
#elif !(MDBX_WITHOUT_MSVC_CRT == 0 || MDBX_WITHOUT_MSVC_CRT == 1)
#error MDBX_WITHOUT_MSVC_CRT must be defined as 0 or 1
#endif /* MDBX_WITHOUT_MSVC_CRT */

/** Size of buffer used during copying a environment/database file. */
#ifndef MDBX_ENVCOPY_WRITEBUF
#define MDBX_ENVCOPY_WRITEBUF 1048576u
#elif MDBX_ENVCOPY_WRITEBUF < 65536u || MDBX_ENVCOPY_WRITEBUF > 1073741824u || \
    MDBX_ENVCOPY_WRITEBUF % 65536u
#error MDBX_ENVCOPY_WRITEBUF must be defined in range 65536..1073741824 and be multiple of 65536
#endif /* MDBX_ENVCOPY_WRITEBUF */

/** Forces assertion checking */
#ifndef MDBX_FORCE_ASSERTIONS
#define MDBX_FORCE_ASSERTIONS 0
#elif !(MDBX_FORCE_ASSERTIONS == 0 || MDBX_FORCE_ASSERTIONS == 1)
#error MDBX_FORCE_ASSERTIONS must be defined as 0 or 1
#endif /* MDBX_FORCE_ASSERTIONS */

/** Presumed malloc size overhead for each allocation
 * to adjust allocations to be more aligned. */
#ifndef MDBX_ASSUME_MALLOC_OVERHEAD
#ifdef __SIZEOF_POINTER__
#define MDBX_ASSUME_MALLOC_OVERHEAD (__SIZEOF_POINTER__ * 2u)
#else
#define MDBX_ASSUME_MALLOC_OVERHEAD (sizeof(void *) * 2u)
#endif
#elif MDBX_ASSUME_MALLOC_OVERHEAD < 0 || MDBX_ASSUME_MALLOC_OVERHEAD > 64 ||   \
    MDBX_ASSUME_MALLOC_OVERHEAD % 4
#error MDBX_ASSUME_MALLOC_OVERHEAD must be defined in range 0..64 and be multiple of 4
#endif /* MDBX_ASSUME_MALLOC_OVERHEAD */

/** If defined then enables integration with Valgrind,
 * a memory analyzing tool. */
#ifndef MDBX_USE_VALGRIND
#endif /* MDBX_USE_VALGRIND */

/** If defined then enables use C11 atomics,
 *  otherwise detects ones availability automatically. */
#ifndef MDBX_HAVE_C11ATOMICS
#endif /* MDBX_HAVE_C11ATOMICS */

/** If defined then enables use the GCC's `__builtin_cpu_supports()`
 *  for runtime dispatching depending on the CPU's capabilities. */
#ifndef MDBX_HAVE_BUILTIN_CPU_SUPPORTS
#if defined(__APPLE__) || defined(BIONIC)
/* Never use any modern features on Apple's or Google's OSes
 * since a lot of troubles with compatibility and/or performance */
#define MDBX_HAVE_BUILTIN_CPU_SUPPORTS 0
#elif defined(__e2k__)
#define MDBX_HAVE_BUILTIN_CPU_SUPPORTS 0
#elif __has_builtin(__builtin_cpu_supports) ||                                 \
    defined(__BUILTIN_CPU_SUPPORTS__) ||                                       \
    (defined(__ia32__) && __GNUC_PREREQ(4, 8) && __GLIBC_PREREQ(2, 23))
#define MDBX_HAVE_BUILTIN_CPU_SUPPORTS 1
#else
#define MDBX_HAVE_BUILTIN_CPU_SUPPORTS 0
#endif
#elif !(MDBX_HAVE_BUILTIN_CPU_SUPPORTS == 0 ||                                 \
        MDBX_HAVE_BUILTIN_CPU_SUPPORTS == 1)
#error MDBX_HAVE_BUILTIN_CPU_SUPPORTS must be defined as 0 or 1
#endif /* MDBX_HAVE_BUILTIN_CPU_SUPPORTS */

//------------------------------------------------------------------------------

/** Win32 File Locking API for \ref MDBX_LOCKING */
#define MDBX_LOCKING_WIN32FILES -1

/** SystemV IPC semaphores for \ref MDBX_LOCKING */
#define MDBX_LOCKING_SYSV 5

/** POSIX-1 Shared anonymous semaphores for \ref MDBX_LOCKING */
#define MDBX_LOCKING_POSIX1988 1988

/** POSIX-2001 Shared Mutexes for \ref MDBX_LOCKING */
#define MDBX_LOCKING_POSIX2001 2001

/** POSIX-2008 Robust Mutexes for \ref MDBX_LOCKING */
#define MDBX_LOCKING_POSIX2008 2008

/** BeOS Benaphores, aka Futexes for \ref MDBX_LOCKING */
#define MDBX_LOCKING_BENAPHORE 1995

/** Advanced: Choices the locking implementation (autodetection by default). */
#if defined(_WIN32) || defined(_WIN64)
#define MDBX_LOCKING MDBX_LOCKING_WIN32FILES
#else
#ifndef MDBX_LOCKING
#if defined(_POSIX_THREAD_PROCESS_SHARED) &&                                   \
    _POSIX_THREAD_PROCESS_SHARED >= 200112L && !defined(__FreeBSD__)

/* Some platforms define the EOWNERDEAD error code even though they
 * don't support Robust Mutexes. If doubt compile with -MDBX_LOCKING=2001. */
#if defined(EOWNERDEAD) && _POSIX_THREAD_PROCESS_SHARED >= 200809L &&          \
    ((defined(_POSIX_THREAD_ROBUST_PRIO_INHERIT) &&                            \
      _POSIX_THREAD_ROBUST_PRIO_INHERIT > 0) ||                                \
     (defined(_POSIX_THREAD_ROBUST_PRIO_PROTECT) &&                            \
      _POSIX_THREAD_ROBUST_PRIO_PROTECT > 0) ||                                \
     defined(PTHREAD_MUTEX_ROBUST) || defined(PTHREAD_MUTEX_ROBUST_NP)) &&     \
    (!defined(__GLIBC__) ||                                                    \
     __GLIBC_PREREQ(2, 10) /* troubles with Robust mutexes before 2.10 */)
#define MDBX_LOCKING MDBX_LOCKING_POSIX2008
#else
#define MDBX_LOCKING MDBX_LOCKING_POSIX2001
#endif
#elif defined(__sun) || defined(__SVR4) || defined(__svr4__)
#define MDBX_LOCKING MDBX_LOCKING_POSIX1988
#else
#define MDBX_LOCKING MDBX_LOCKING_SYSV
#endif
#define MDBX_LOCKING_CONFIG "AUTO=" MDBX_STRINGIFY(MDBX_LOCKING)
#else
#define MDBX_LOCKING_CONFIG MDBX_STRINGIFY(MDBX_LOCKING)
#endif /* MDBX_LOCKING */
#endif /* !Windows */

/** Advanced: Using POSIX OFD-locks (autodetection by default). */
#ifndef MDBX_USE_OFDLOCKS
#if ((defined(F_OFD_SETLK) && defined(F_OFD_SETLKW) &&                         \
      defined(F_OFD_GETLK)) ||                                                 \
     (defined(F_OFD_SETLK64) && defined(F_OFD_SETLKW64) &&                     \
      defined(F_OFD_GETLK64))) &&                                              \
    !defined(MDBX_SAFE4QEMU) &&                                                \
    !defined(__sun) /* OFD-lock are broken on Solaris */
#define MDBX_USE_OFDLOCKS 1
#else
#define MDBX_USE_OFDLOCKS 0
#endif
#define MDBX_USE_OFDLOCKS_CONFIG "AUTO=" MDBX_STRINGIFY(MDBX_USE_OFDLOCKS)
#elif !(MDBX_USE_OFDLOCKS == 0 || MDBX_USE_OFDLOCKS == 1)
#error MDBX_USE_OFDLOCKS must be defined as 0 or 1
#else
#define MDBX_USE_OFDLOCKS_CONFIG MDBX_STRINGIFY(MDBX_USE_OFDLOCKS)
#endif /* MDBX_USE_OFDLOCKS */

/** Advanced: Using sendfile() syscall (autodetection by default). */
#ifndef MDBX_USE_SENDFILE
#if ((defined(__linux__) || defined(__gnu_linux__)) &&                         \
     !defined(__ANDROID_API__)) ||                                             \
    (defined(__ANDROID_API__) && __ANDROID_API__ >= 21)
#define MDBX_USE_SENDFILE 1
#else
#define MDBX_USE_SENDFILE 0
#endif
#elif !(MDBX_USE_SENDFILE == 0 || MDBX_USE_SENDFILE == 1)
#error MDBX_USE_SENDFILE must be defined as 0 or 1
#endif /* MDBX_USE_SENDFILE */

/** Advanced: Using copy_file_range() syscall (autodetection by default). */
#ifndef MDBX_USE_COPYFILERANGE
#if __GLIBC_PREREQ(2, 27) && defined(_GNU_SOURCE)
#define MDBX_USE_COPYFILERANGE 1
#else
#define MDBX_USE_COPYFILERANGE 0
#endif
#elif !(MDBX_USE_COPYFILERANGE == 0 || MDBX_USE_COPYFILERANGE == 1)
#error MDBX_USE_COPYFILERANGE must be defined as 0 or 1
#endif /* MDBX_USE_COPYFILERANGE */

/** Advanced: Using sync_file_range() syscall (autodetection by default). */
#ifndef MDBX_USE_SYNCFILERANGE
#if ((defined(__linux__) || defined(__gnu_linux__)) &&                         \
     defined(SYNC_FILE_RANGE_WRITE) && !defined(__ANDROID_API__)) ||           \
    (defined(__ANDROID_API__) && __ANDROID_API__ >= 26)
#define MDBX_USE_SYNCFILERANGE 1
#else
#define MDBX_USE_SYNCFILERANGE 0
#endif
#elif !(MDBX_USE_SYNCFILERANGE == 0 || MDBX_USE_SYNCFILERANGE == 1)
#error MDBX_USE_SYNCFILERANGE must be defined as 0 or 1
#endif /* MDBX_USE_SYNCFILERANGE */

//------------------------------------------------------------------------------

#ifndef MDBX_CPU_WRITEBACK_INCOHERENT
#if defined(__ia32__) || defined(__e2k__) || defined(__hppa) ||                \
    defined(__hppa__) || defined(DOXYGEN)
#define MDBX_CPU_WRITEBACK_INCOHERENT 0
#else
#define MDBX_CPU_WRITEBACK_INCOHERENT 1
#endif
#elif !(MDBX_CPU_WRITEBACK_INCOHERENT == 0 ||                                  \
        MDBX_CPU_WRITEBACK_INCOHERENT == 1)
#error MDBX_CPU_WRITEBACK_INCOHERENT must be defined as 0 or 1
#endif /* MDBX_CPU_WRITEBACK_INCOHERENT */

#ifndef MDBX_MMAP_INCOHERENT_FILE_WRITE
#ifdef __OpenBSD__
#define MDBX_MMAP_INCOHERENT_FILE_WRITE 1
#else
#define MDBX_MMAP_INCOHERENT_FILE_WRITE 0
#endif
#elif !(MDBX_MMAP_INCOHERENT_FILE_WRITE == 0 ||                                \
        MDBX_MMAP_INCOHERENT_FILE_WRITE == 1)
#error MDBX_MMAP_INCOHERENT_FILE_WRITE must be defined as 0 or 1
#endif /* MDBX_MMAP_INCOHERENT_FILE_WRITE */

#ifndef MDBX_MMAP_INCOHERENT_CPU_CACHE
#if defined(__mips) || defined(__mips__) || defined(__mips64) ||               \
    defined(__mips64__) || defined(_M_MRX000) || defined(_MIPS_) ||            \
    defined(__MWERKS__) || defined(__sgi)
/* MIPS has cache coherency issues. */
#define MDBX_MMAP_INCOHERENT_CPU_CACHE 1
#else
/* LY: assume no relevant mmap/dcache issues. */
#define MDBX_MMAP_INCOHERENT_CPU_CACHE 0
#endif
#elif !(MDBX_MMAP_INCOHERENT_CPU_CACHE == 0 ||                                 \
        MDBX_MMAP_INCOHERENT_CPU_CACHE == 1)
#error MDBX_MMAP_INCOHERENT_CPU_CACHE must be defined as 0 or 1
#endif /* MDBX_MMAP_INCOHERENT_CPU_CACHE */

#ifndef MDBX_MMAP_USE_MS_ASYNC
#if MDBX_MMAP_INCOHERENT_FILE_WRITE || MDBX_MMAP_INCOHERENT_CPU_CACHE
#define MDBX_MMAP_USE_MS_ASYNC 1
#else
#define MDBX_MMAP_USE_MS_ASYNC 0
#endif
#elif !(MDBX_MMAP_USE_MS_ASYNC == 0 || MDBX_MMAP_USE_MS_ASYNC == 1)
#error MDBX_MMAP_USE_MS_ASYNC must be defined as 0 or 1
#endif /* MDBX_MMAP_USE_MS_ASYNC */

#ifndef MDBX_64BIT_ATOMIC
#if MDBX_WORDBITS >= 64 || defined(DOXYGEN)
#define MDBX_64BIT_ATOMIC 1
#else
#define MDBX_64BIT_ATOMIC 0
#endif
#define MDBX_64BIT_ATOMIC_CONFIG "AUTO=" MDBX_STRINGIFY(MDBX_64BIT_ATOMIC)
#elif !(MDBX_64BIT_ATOMIC == 0 || MDBX_64BIT_ATOMIC == 1)
#error MDBX_64BIT_ATOMIC must be defined as 0 or 1
#else
#define MDBX_64BIT_ATOMIC_CONFIG MDBX_STRINGIFY(MDBX_64BIT_ATOMIC)
#endif /* MDBX_64BIT_ATOMIC */

#ifndef MDBX_64BIT_CAS
#if defined(__GCC_ATOMIC_LLONG_LOCK_FREE)
#if __GCC_ATOMIC_LLONG_LOCK_FREE > 1
#define MDBX_64BIT_CAS 1
#else
#define MDBX_64BIT_CAS 0
#endif
#elif defined(__CLANG_ATOMIC_LLONG_LOCK_FREE)
#if __CLANG_ATOMIC_LLONG_LOCK_FREE > 1
#define MDBX_64BIT_CAS 1
#else
#define MDBX_64BIT_CAS 0
#endif
#elif defined(ATOMIC_LLONG_LOCK_FREE)
#if ATOMIC_LLONG_LOCK_FREE > 1
#define MDBX_64BIT_CAS 1
#else
#define MDBX_64BIT_CAS 0
#endif
#elif defined(_MSC_VER) || defined(__APPLE__) || defined(DOXYGEN)
#define MDBX_64BIT_CAS 1
#elif !(MDBX_64BIT_CAS == 0 || MDBX_64BIT_CAS == 1)
#error MDBX_64BIT_CAS must be defined as 0 or 1
#else
#define MDBX_64BIT_CAS MDBX_64BIT_ATOMIC
#endif
#define MDBX_64BIT_CAS_CONFIG "AUTO=" MDBX_STRINGIFY(MDBX_64BIT_CAS)
#else
#define MDBX_64BIT_CAS_CONFIG MDBX_STRINGIFY(MDBX_64BIT_CAS)
#endif /* MDBX_64BIT_CAS */

#ifndef MDBX_UNALIGNED_OK
#if defined(__ALIGNED__) || defined(__SANITIZE_UNDEFINED__) ||                 \
    defined(ENABLE_UBSAN)
#define MDBX_UNALIGNED_OK 0 /* no unaligned access allowed */
#elif defined(__ARM_FEATURE_UNALIGNED)
#define MDBX_UNALIGNED_OK 4 /* ok unaligned for 32-bit words */
#elif defined(__e2k__) || defined(__elbrus__)
#if __iset__ > 4
#define MDBX_UNALIGNED_OK 8 /* ok unaligned for 64-bit words */
#else
#define MDBX_UNALIGNED_OK 4 /* ok unaligned for 32-bit words */
#endif
#elif defined(__ia32__)
#define MDBX_UNALIGNED_OK 8 /* ok unaligned for 64-bit words */
#elif __CLANG_PREREQ(5, 0) || __GNUC_PREREQ(5, 0)
/* expecting an optimization will well done, also this
 * hushes false-positives from UBSAN (undefined behaviour sanitizer) */
#define MDBX_UNALIGNED_OK 0
#else
#define MDBX_UNALIGNED_OK 0 /* no unaligned access allowed */
#endif
#elif MDBX_UNALIGNED_OK == 1
#undef MDBX_UNALIGNED_OK
#define MDBX_UNALIGNED_OK 32 /* any unaligned access allowed */
#endif                       /* MDBX_UNALIGNED_OK */

#ifndef MDBX_CACHELINE_SIZE
#if defined(SYSTEM_CACHE_ALIGNMENT_SIZE)
#define MDBX_CACHELINE_SIZE SYSTEM_CACHE_ALIGNMENT_SIZE
#elif defined(__ia64__) || defined(__ia64) || defined(_M_IA64)
#define MDBX_CACHELINE_SIZE 128
#else
#define MDBX_CACHELINE_SIZE 64
#endif
#endif /* MDBX_CACHELINE_SIZE */

/** @} end of build options */
/*******************************************************************************
 *******************************************************************************
 ******************************************************************************/

#ifndef DOXYGEN

/* In case the MDBX_DEBUG is undefined set it corresponding to NDEBUG */
#ifndef MDBX_DEBUG
#ifdef NDEBUG
#define MDBX_DEBUG 0
#else
#define MDBX_DEBUG 1
#endif
#endif /* MDBX_DEBUG */

#else

/* !!! Actually this is a fake definitions for Doxygen !!! */

/** Controls enabling of debugging features.
 *
 *  - `MDBX_DEBUG = 0` (by default) Disables any debugging features at all,
 *                     including logging and assertion controls.
 *                     Logging level and corresponding debug flags changing
 *                     by \ref mdbx_setup_debug() will not have effect.
 *  - `MDBX_DEBUG > 0` Enables code for the debugging features (logging,
 *                     assertions checking and internal audit).
 *                     Simultaneously sets the default logging level
 *                     to the `MDBX_DEBUG` value.
 *                     Also enables \ref MDBX_DBG_AUDIT if `MDBX_DEBUG >= 2`.
 *
 * \ingroup build_option */
#define MDBX_DEBUG 0...7

/** Disables using of GNU libc extensions. */
#define MDBX_DISABLE_GNU_SOURCE 0 or 1

#endif /* DOXYGEN */
