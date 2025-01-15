/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025

#include "internals.h"

static void mdbx_init(void);
static void mdbx_fini(void);

/*----------------------------------------------------------------------------*/
/* mdbx constructor/destructor */

#if defined(_WIN32) || defined(_WIN64)

#if MDBX_BUILD_SHARED_LIBRARY
#if MDBX_WITHOUT_MSVC_CRT && defined(NDEBUG)
/* DEBUG/CHECKED builds still require MSVC's CRT for runtime checks.
 *
 * Define dll's entry point only for Release build when NDEBUG is defined and
 * MDBX_WITHOUT_MSVC_CRT=ON. if the entry point isn't defined then MSVC's will
 * automatically use DllMainCRTStartup() from CRT library, which also
 * automatically call DllMain() from our mdbx.dll */
#pragma comment(linker, "/ENTRY:DllMain")
#endif /* MDBX_WITHOUT_MSVC_CRT */

BOOL APIENTRY DllMain(HANDLE module, DWORD reason, LPVOID reserved)
#else
#if !MDBX_MANUAL_MODULE_HANDLER
static
#endif /* !MDBX_MANUAL_MODULE_HANDLER */
    void NTAPI
    mdbx_module_handler(PVOID module, DWORD reason, PVOID reserved)
#endif /* MDBX_BUILD_SHARED_LIBRARY */
{
  (void)reserved;
  switch (reason) {
  case DLL_PROCESS_ATTACH:
    windows_import();
    mdbx_init();
    break;
  case DLL_PROCESS_DETACH:
    mdbx_fini();
    break;

  case DLL_THREAD_ATTACH:
    break;
  case DLL_THREAD_DETACH:
    rthc_thread_dtor(module);
    break;
  }
#if MDBX_BUILD_SHARED_LIBRARY
  return TRUE;
#endif
}

#if !MDBX_BUILD_SHARED_LIBRARY && !MDBX_MANUAL_MODULE_HANDLER
/* *INDENT-OFF* */
/* clang-format off */
#if defined(_MSC_VER)
#  pragma const_seg(push)
#  pragma data_seg(push)

#  ifndef _M_IX86
     /* kick a linker to create the TLS directory if not already done */
#    pragma comment(linker, "/INCLUDE:_tls_used")
     /* Force some symbol references. */
#    pragma comment(linker, "/INCLUDE:mdbx_tls_anchor")
     /* specific const-segment for WIN64 */
#    pragma const_seg(".CRT$XLB")
     const
#  else
     /* kick a linker to create the TLS directory if not already done */
#    pragma comment(linker, "/INCLUDE:__tls_used")
     /* Force some symbol references. */
#    pragma comment(linker, "/INCLUDE:_mdbx_tls_anchor")
     /* specific data-segment for WIN32 */
#    pragma data_seg(".CRT$XLB")
#  endif

   __declspec(allocate(".CRT$XLB")) PIMAGE_TLS_CALLBACK mdbx_tls_anchor = mdbx_module_handler;
#  pragma data_seg(pop)
#  pragma const_seg(pop)

#elif defined(__GNUC__)
#  ifndef _M_IX86
     const
#  endif
   PIMAGE_TLS_CALLBACK mdbx_tls_anchor __attribute__((__section__(".CRT$XLB"), used)) = mdbx_module_handler;
#else
#  error FIXME
#endif
/* *INDENT-ON* */
/* clang-format on */
#endif /* !MDBX_BUILD_SHARED_LIBRARY && !MDBX_MANUAL_MODULE_HANDLER */

#else

#if defined(__linux__) || defined(__gnu_linux__)
#include <sys/utsname.h>

MDBX_EXCLUDE_FOR_GPROF
__cold static uint8_t probe_for_WSL(const char *tag) {
  const char *const WSL = strstr(tag, "WSL");
  if (WSL && WSL[3] >= '2' && WSL[3] <= '9')
    return WSL[3] - '0';
  const char *const wsl = strstr(tag, "wsl");
  if (wsl && wsl[3] >= '2' && wsl[3] <= '9')
    return wsl[3] - '0';
  if (WSL || wsl || strcasestr(tag, "Microsoft"))
    /* Expecting no new kernel within WSL1, either it will explicitly
     * marked by an appropriate WSL-version hint. */
    return (globals.linux_kernel_version < /* 4.19.x */ 0x04130000) ? 1 : 2;
  return 0;
}
#endif /* Linux */

#ifdef ENABLE_GPROF
extern void _mcleanup(void);
extern void monstartup(unsigned long, unsigned long);
extern void _init(void);
extern void _fini(void);
extern void __gmon_start__(void) __attribute__((__weak__));
#endif /* ENABLE_GPROF */

MDBX_EXCLUDE_FOR_GPROF
__cold static __attribute__((__constructor__)) void mdbx_global_constructor(void) {
#ifdef ENABLE_GPROF
  if (!&__gmon_start__)
    monstartup((uintptr_t)&_init, (uintptr_t)&_fini);
#endif /* ENABLE_GPROF */

#if defined(__linux__) || defined(__gnu_linux__)
  struct utsname buffer;
  if (uname(&buffer) == 0) {
    int i = 0;
    char *p = buffer.release;
    while (*p && i < 4) {
      if (*p >= '0' && *p <= '9') {
        long number = strtol(p, &p, 10);
        if (number > 0) {
          if (number > 255)
            number = 255;
          globals.linux_kernel_version += number << (24 - i * 8);
        }
        ++i;
      } else {
        ++p;
      }
    }
    /* "Official" way of detecting WSL1 but not WSL2
     * https://github.com/Microsoft/WSL/issues/423#issuecomment-221627364
     *
     * WARNING: False negative detection of WSL1 will result in DATA LOSS!
     * So, the REQUIREMENTS for this code:
     *  1. MUST detect WSL1 without false-negatives.
     *  2. DESIRABLE detect WSL2 but without the risk of violating the first. */
    globals.running_on_WSL1 =
        probe_for_WSL(buffer.version) == 1 || probe_for_WSL(buffer.sysname) == 1 || probe_for_WSL(buffer.release) == 1;
  }
#endif /* Linux */

  mdbx_init();
}

MDBX_EXCLUDE_FOR_GPROF
__cold static __attribute__((__destructor__)) void mdbx_global_destructor(void) {
  mdbx_fini();
#ifdef ENABLE_GPROF
  if (!&__gmon_start__)
    _mcleanup();
#endif /* ENABLE_GPROF */
}

#endif /* ! Windows */

/******************************************************************************/

struct libmdbx_globals globals;

__cold static void mdbx_init(void) {
  globals.runtime_flags = ((MDBX_DEBUG) > 0) * MDBX_DBG_ASSERT + ((MDBX_DEBUG) > 1) * MDBX_DBG_AUDIT;
  globals.loglevel = MDBX_LOG_FATAL;
  ENSURE(nullptr, osal_fastmutex_init(&globals.debug_lock) == 0);
  osal_ctor();
  assert(globals.sys_pagesize > 0 && (globals.sys_pagesize & (globals.sys_pagesize - 1)) == 0);
  rthc_ctor();
#if MDBX_DEBUG
  ENSURE(nullptr, troika_verify_fsm());
  ENSURE(nullptr, pv2pages_verify());
#endif /* MDBX_DEBUG*/
}

MDBX_EXCLUDE_FOR_GPROF
__cold static void mdbx_fini(void) {
  const uint32_t current_pid = osal_getpid();
  TRACE(">> pid %d", current_pid);
  rthc_dtor(current_pid);
  osal_dtor();
  TRACE("<< pid %d\n", current_pid);
  ENSURE(nullptr, osal_fastmutex_destroy(&globals.debug_lock) == 0);
}

/******************************************************************************/

/* *INDENT-OFF* */
/* clang-format off */

__dll_export
#ifdef __attribute_used__
    __attribute_used__
#elif defined(__GNUC__) || __has_attribute(__used__)
    __attribute__((__used__))
#endif
#ifdef __attribute_externally_visible__
        __attribute_externally_visible__
#elif (defined(__GNUC__) && !defined(__clang__)) ||                            \
    __has_attribute(__externally_visible__)
    __attribute__((__externally_visible__))
#endif
    const struct MDBX_build_info mdbx_build = {
#ifdef MDBX_BUILD_TIMESTAMP
    MDBX_BUILD_TIMESTAMP
#else
    "\"" __DATE__ " " __TIME__ "\""
#endif /* MDBX_BUILD_TIMESTAMP */

    ,
#ifdef MDBX_BUILD_TARGET
    MDBX_BUILD_TARGET
#else
  #if defined(__ANDROID_API__)
    "Android" MDBX_STRINGIFY(__ANDROID_API__)
  #elif defined(__linux__) || defined(__gnu_linux__)
    "Linux"
  #elif defined(EMSCRIPTEN) || defined(__EMSCRIPTEN__)
    "webassembly"
  #elif defined(__CYGWIN__)
    "CYGWIN"
  #elif defined(_WIN64) || defined(_WIN32) || defined(__TOS_WIN__) \
      || defined(__WINDOWS__)
    "Windows"
  #elif defined(__APPLE__)
    #if (defined(TARGET_OS_IPHONE) && TARGET_OS_IPHONE) \
      || (defined(TARGET_IPHONE_SIMULATOR) && TARGET_IPHONE_SIMULATOR)
      "iOS"
    #else
      "MacOS"
    #endif
  #elif defined(__FreeBSD__)
    "FreeBSD"
  #elif defined(__DragonFly__)
    "DragonFlyBSD"
  #elif defined(__NetBSD__)
    "NetBSD"
  #elif defined(__OpenBSD__)
    "OpenBSD"
  #elif defined(__bsdi__)
    "UnixBSDI"
  #elif defined(__MACH__)
    "MACH"
  #elif (defined(_HPUX_SOURCE) || defined(__hpux) || defined(__HP_aCC))
    "HPUX"
  #elif defined(_AIX)
    "AIX"
  #elif defined(__sun) && defined(__SVR4)
    "Solaris"
  #elif defined(__BSD__) || defined(BSD)
    "UnixBSD"
  #elif defined(__unix__) || defined(UNIX) || defined(__unix) \
      || defined(__UNIX) || defined(__UNIX__)
    "UNIX"
  #elif defined(_POSIX_VERSION)
    "POSIX" MDBX_STRINGIFY(_POSIX_VERSION)
  #else
    "UnknownOS"
  #endif /* Target OS */

    "-"

  #if defined(__amd64__)
    "AMD64"
  #elif defined(__ia32__)
    "IA32"
  #elif defined(__e2k__) || defined(__elbrus__)
    "Elbrus"
  #elif defined(__alpha__) || defined(__alpha) || defined(_M_ALPHA)
    "Alpha"
  #elif defined(__aarch64__) || defined(_M_ARM64)
    "ARM64"
  #elif defined(__arm__) || defined(__thumb__) || defined(__TARGET_ARCH_ARM) \
      || defined(__TARGET_ARCH_THUMB) || defined(_ARM) || defined(_M_ARM) \
      || defined(_M_ARMT) || defined(__arm)
    "ARM"
  #elif defined(__mips64) || defined(__mips64__) || (defined(__mips) && (__mips >= 64))
    "MIPS64"
  #elif defined(__mips__) || defined(__mips) || defined(_R4000) || defined(__MIPS__)
    "MIPS"
  #elif defined(__hppa64__) || defined(__HPPA64__) || defined(__hppa64)
    "PARISC64"
  #elif defined(__hppa__) || defined(__HPPA__) || defined(__hppa)
    "PARISC"
  #elif defined(__ia64__) || defined(__ia64) || defined(_IA64) \
      || defined(__IA64__) || defined(_M_IA64) || defined(__itanium__)
    "Itanium"
  #elif defined(__powerpc64__) || defined(__ppc64__) || defined(__ppc64) \
      || defined(__powerpc64) || defined(_ARCH_PPC64)
    "PowerPC64"
  #elif defined(__powerpc__) || defined(__ppc__) || defined(__powerpc) \
      || defined(__ppc) || defined(_ARCH_PPC) || defined(__PPC__) || defined(__POWERPC__)
    "PowerPC"
  #elif defined(__sparc64__) || defined(__sparc64)
    "SPARC64"
  #elif defined(__sparc__) || defined(__sparc)
    "SPARC"
  #elif defined(__s390__) || defined(__s390) || defined(__zarch__) || defined(__zarch)
    "S390"
  #else
    "UnknownARCH"
  #endif
#endif /* MDBX_BUILD_TARGET */

#ifdef MDBX_BUILD_TYPE
# if defined(_MSC_VER)
#   pragma message("Configuration-depended MDBX_BUILD_TYPE: " MDBX_BUILD_TYPE)
# endif
    "-" MDBX_BUILD_TYPE
#endif /* MDBX_BUILD_TYPE */
    ,
    "MDBX_DEBUG=" MDBX_STRINGIFY(MDBX_DEBUG)
#ifdef ENABLE_GPROF
    " ENABLE_GPROF"
#endif /* ENABLE_GPROF */
    " MDBX_WORDBITS=" MDBX_STRINGIFY(MDBX_WORDBITS)
    " BYTE_ORDER="
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    "LITTLE_ENDIAN"
#elif __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
    "BIG_ENDIAN"
#else
    #error "FIXME: Unsupported byte order"
#endif /* __BYTE_ORDER__ */
    " MDBX_ENABLE_BIGFOOT=" MDBX_STRINGIFY(MDBX_ENABLE_BIGFOOT)
    " MDBX_ENV_CHECKPID=" MDBX_ENV_CHECKPID_CONFIG
    " MDBX_TXN_CHECKOWNER=" MDBX_TXN_CHECKOWNER_CONFIG
    " MDBX_64BIT_ATOMIC=" MDBX_64BIT_ATOMIC_CONFIG
    " MDBX_64BIT_CAS=" MDBX_64BIT_CAS_CONFIG
    " MDBX_TRUST_RTC=" MDBX_TRUST_RTC_CONFIG
    " MDBX_AVOID_MSYNC=" MDBX_STRINGIFY(MDBX_AVOID_MSYNC)
    " MDBX_ENABLE_REFUND=" MDBX_STRINGIFY(MDBX_ENABLE_REFUND)
    " MDBX_USE_MINCORE=" MDBX_STRINGIFY(MDBX_USE_MINCORE)
    " MDBX_ENABLE_PGOP_STAT=" MDBX_STRINGIFY(MDBX_ENABLE_PGOP_STAT)
    " MDBX_ENABLE_PROFGC=" MDBX_STRINGIFY(MDBX_ENABLE_PROFGC)
#if MDBX_DISABLE_VALIDATION
    " MDBX_DISABLE_VALIDATION=YES"
#endif /* MDBX_DISABLE_VALIDATION */
#ifdef __SANITIZE_ADDRESS__
    " SANITIZE_ADDRESS=YES"
#endif /* __SANITIZE_ADDRESS__ */
#ifdef ENABLE_MEMCHECK
    " ENABLE_MEMCHECK=YES"
#endif /* ENABLE_MEMCHECK */
#if MDBX_FORCE_ASSERTIONS
    " MDBX_FORCE_ASSERTIONS=YES"
#endif /* MDBX_FORCE_ASSERTIONS */
#ifdef _GNU_SOURCE
    " _GNU_SOURCE=YES"
#else
    " _GNU_SOURCE=NO"
#endif /* _GNU_SOURCE */
#ifdef __APPLE__
    " MDBX_APPLE_SPEED_INSTEADOF_DURABILITY=" MDBX_STRINGIFY(MDBX_APPLE_SPEED_INSTEADOF_DURABILITY)
#endif /* MacOS */
#if defined(_WIN32) || defined(_WIN64)
    " MDBX_WITHOUT_MSVC_CRT=" MDBX_STRINGIFY(MDBX_WITHOUT_MSVC_CRT)
    " MDBX_BUILD_SHARED_LIBRARY=" MDBX_STRINGIFY(MDBX_BUILD_SHARED_LIBRARY)
#if !MDBX_BUILD_SHARED_LIBRARY
    " MDBX_MANUAL_MODULE_HANDLER=" MDBX_STRINGIFY(MDBX_MANUAL_MODULE_HANDLER)
#endif
    " WINVER=" MDBX_STRINGIFY(WINVER)
#else /* Windows */
    " MDBX_LOCKING=" MDBX_LOCKING_CONFIG
    " MDBX_USE_OFDLOCKS=" MDBX_USE_OFDLOCKS_CONFIG
#endif /* !Windows */
    " MDBX_CACHELINE_SIZE=" MDBX_STRINGIFY(MDBX_CACHELINE_SIZE)
    " MDBX_CPU_WRITEBACK_INCOHERENT=" MDBX_STRINGIFY(MDBX_CPU_WRITEBACK_INCOHERENT)
    " MDBX_MMAP_INCOHERENT_CPU_CACHE=" MDBX_STRINGIFY(MDBX_MMAP_INCOHERENT_CPU_CACHE)
    " MDBX_MMAP_INCOHERENT_FILE_WRITE=" MDBX_STRINGIFY(MDBX_MMAP_INCOHERENT_FILE_WRITE)
    " MDBX_UNALIGNED_OK=" MDBX_STRINGIFY(MDBX_UNALIGNED_OK)
    " MDBX_PNL_ASCENDING=" MDBX_STRINGIFY(MDBX_PNL_ASCENDING)
    ,
#ifdef MDBX_BUILD_COMPILER
    MDBX_BUILD_COMPILER
#else
  #ifdef __INTEL_COMPILER
    "Intel C/C++ " MDBX_STRINGIFY(__INTEL_COMPILER)
  #elif defined(__apple_build_version__)
    "Apple clang " MDBX_STRINGIFY(__apple_build_version__)
  #elif defined(__ibmxl__)
    "IBM clang C " MDBX_STRINGIFY(__ibmxl_version__) "." MDBX_STRINGIFY(__ibmxl_release__)
    "." MDBX_STRINGIFY(__ibmxl_modification__) "." MDBX_STRINGIFY(__ibmxl_ptf_fix_level__)
  #elif defined(__clang__)
    "clang " MDBX_STRINGIFY(__clang_version__)
  #elif defined(__MINGW64__)
    "MINGW-64 " MDBX_STRINGIFY(__MINGW64_MAJOR_VERSION) "." MDBX_STRINGIFY(__MINGW64_MINOR_VERSION)
  #elif defined(__MINGW32__)
    "MINGW-32 " MDBX_STRINGIFY(__MINGW32_MAJOR_VERSION) "." MDBX_STRINGIFY(__MINGW32_MINOR_VERSION)
  #elif defined(__MINGW__)
    "MINGW " MDBX_STRINGIFY(__MINGW_MAJOR_VERSION) "." MDBX_STRINGIFY(__MINGW_MINOR_VERSION)
  #elif defined(__IBMC__)
    "IBM C " MDBX_STRINGIFY(__IBMC__)
  #elif defined(__GNUC__)
    "GNU C/C++ "
    #ifdef __VERSION__
      __VERSION__
    #else
      MDBX_STRINGIFY(__GNUC__) "." MDBX_STRINGIFY(__GNUC_MINOR__) "." MDBX_STRINGIFY(__GNUC_PATCHLEVEL__)
    #endif
  #elif defined(_MSC_VER)
    "MSVC " MDBX_STRINGIFY(_MSC_FULL_VER) "-" MDBX_STRINGIFY(_MSC_BUILD)
  #else
    "Unknown compiler"
  #endif
#endif /* MDBX_BUILD_COMPILER */
    ,
#ifdef MDBX_BUILD_FLAGS_CONFIG
    MDBX_BUILD_FLAGS_CONFIG
#endif /* MDBX_BUILD_FLAGS_CONFIG */
#if defined(MDBX_BUILD_FLAGS_CONFIG) && defined(MDBX_BUILD_FLAGS)
    " "
#endif
#ifdef MDBX_BUILD_FLAGS
    MDBX_BUILD_FLAGS
#endif /* MDBX_BUILD_FLAGS */
#if !(defined(MDBX_BUILD_FLAGS_CONFIG) || defined(MDBX_BUILD_FLAGS))
    "undefined (please use correct build script)"
#ifdef _MSC_VER
#pragma message("warning: Build flags undefined. Please use correct build script")
#else
#warning "Build flags undefined. Please use correct build script"
#endif // _MSC_VER
#endif
  , MDBX_BUILD_METADATA
};

#ifdef __SANITIZE_ADDRESS__
#if !defined(_MSC_VER) || __has_attribute(weak)
LIBMDBX_API __attribute__((__weak__))
#endif
const char *__asan_default_options(void) {
  return "symbolize=1:allow_addr2line=1:"
#if MDBX_DEBUG
         "debug=1:"
         "verbosity=2:"
#endif /* MDBX_DEBUG */
         "log_threads=1:"
         "report_globals=1:"
         "replace_str=1:replace_intrin=1:"
         "malloc_context_size=9:"
#if !defined(__APPLE__)
         "detect_leaks=1:"
#endif
         "check_printf=1:"
         "detect_deadlocks=1:"
#ifndef LTO_ENABLED
         "check_initialization_order=1:"
#endif
         "detect_stack_use_after_return=1:"
         "intercept_tls_get_addr=1:"
         "decorate_proc_maps=1:"
         "abort_on_error=1";
}
#endif /* __SANITIZE_ADDRESS__ */

/* *INDENT-ON* */
/* clang-format on */
