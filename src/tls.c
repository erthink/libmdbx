/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025

#include "internals.h"

typedef struct rthc_entry {
  MDBX_env *env;
} rthc_entry_t;

#if MDBX_DEBUG
#define RTHC_INITIAL_LIMIT 1
#else
#define RTHC_INITIAL_LIMIT 16
#endif

static unsigned rthc_count, rthc_limit = RTHC_INITIAL_LIMIT;
static rthc_entry_t rthc_table_static[RTHC_INITIAL_LIMIT];
static rthc_entry_t *rthc_table = rthc_table_static;

static int uniq_peek(const osal_mmap_t *pending, osal_mmap_t *scan) {
  int rc;
  uint64_t bait;
  lck_t *const pending_lck = pending->lck;
  lck_t *const scan_lck = scan->lck;
  if (pending_lck) {
    bait = atomic_load64(&pending_lck->bait_uniqueness, mo_AcquireRelease);
    rc = MDBX_SUCCESS;
  } else {
    bait = 0 /* hush MSVC warning */;
    rc = osal_msync(scan, 0, sizeof(lck_t), MDBX_SYNC_DATA);
    if (rc == MDBX_SUCCESS)
      rc = osal_pread(pending->fd, &bait, sizeof(scan_lck->bait_uniqueness), offsetof(lck_t, bait_uniqueness));
  }
  if (likely(rc == MDBX_SUCCESS) && bait == atomic_load64(&scan_lck->bait_uniqueness, mo_AcquireRelease))
    rc = MDBX_RESULT_TRUE;

  TRACE("uniq-peek: %s, bait 0x%016" PRIx64 ",%s rc %d", pending_lck ? "mem" : "file", bait,
        (rc == MDBX_RESULT_TRUE) ? " found," : (rc ? " FAILED," : ""), rc);
  return rc;
}

static int uniq_poke(const osal_mmap_t *pending, osal_mmap_t *scan, uint64_t *abra) {
  if (*abra == 0) {
    const uintptr_t tid = osal_thread_self();
    uintptr_t uit = 0;
    memcpy(&uit, &tid, (sizeof(tid) < sizeof(uit)) ? sizeof(tid) : sizeof(uit));
    *abra = rrxmrrxmsx_0(osal_monotime() + UINT64_C(5873865991930747) * uit);
  }
  const uint64_t cadabra =
      rrxmrrxmsx_0(*abra + UINT64_C(7680760450171793) * (unsigned)osal_getpid()) << 24 | *abra >> 40;
  lck_t *const scan_lck = scan->lck;
  atomic_store64(&scan_lck->bait_uniqueness, cadabra, mo_AcquireRelease);
  *abra = *abra * UINT64_C(6364136223846793005) + 1;
  return uniq_peek(pending, scan);
}

__cold int rthc_uniq_check(const osal_mmap_t *pending, MDBX_env **found) {
  *found = nullptr;
  uint64_t salt = 0;
  for (size_t i = 0; i < rthc_count; ++i) {
    MDBX_env *const scan = rthc_table[i].env;
    if (!scan->lck_mmap.lck || &scan->lck_mmap == pending)
      continue;
    int err = atomic_load64(&scan->lck_mmap.lck->bait_uniqueness, mo_AcquireRelease)
                  ? uniq_peek(pending, &scan->lck_mmap)
                  : uniq_poke(pending, &scan->lck_mmap, &salt);
    if (err == MDBX_ENODATA) {
      uint64_t length = 0;
      if (likely(osal_filesize(pending->fd, &length) == MDBX_SUCCESS && length == 0)) {
        /* LY: skip checking since LCK-file is empty, i.e. just created. */
        DEBUG("%s", "unique (new/empty lck)");
        return MDBX_SUCCESS;
      }
    }
    if (err == MDBX_RESULT_TRUE)
      err = uniq_poke(pending, &scan->lck_mmap, &salt);
    if (err == MDBX_RESULT_TRUE) {
      (void)osal_msync(&scan->lck_mmap, 0, sizeof(lck_t), MDBX_SYNC_KICK);
      err = uniq_poke(pending, &scan->lck_mmap, &salt);
    }
    if (err == MDBX_RESULT_TRUE) {
      err = uniq_poke(pending, &scan->lck_mmap, &salt);
      *found = scan;
      DEBUG("found %p", __Wpedantic_format_voidptr(*found));
      return MDBX_SUCCESS;
    }
    if (unlikely(err != MDBX_SUCCESS)) {
      DEBUG("failed rc %d", err);
      return err;
    }
  }

  DEBUG("%s", "unique");
  return MDBX_SUCCESS;
}

//------------------------------------------------------------------------------

#if defined(_WIN32) || defined(_WIN64)
static CRITICAL_SECTION rthc_critical_section;
#else

static pthread_mutex_t rthc_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t rthc_cond = PTHREAD_COND_INITIALIZER;
static osal_thread_key_t rthc_key;
static mdbx_atomic_uint32_t rthc_pending;

static inline uint64_t rthc_signature(const void *addr, uint8_t kind) {
  uint64_t salt = osal_thread_self() * UINT64_C(0xA2F0EEC059629A17) ^ UINT64_C(0x01E07C6FDB596497) * (uintptr_t)(addr);
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
  return salt << 8 | kind;
#elif __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
  return (uint64_t)kind << 56 | salt >> 8;
#else
#error "FIXME: Unsupported byte order"
#endif /* __BYTE_ORDER__ */
}

#define MDBX_THREAD_RTHC_REGISTERED(addr) rthc_signature(addr, 0x0D)
#define MDBX_THREAD_RTHC_COUNTED(addr) rthc_signature(addr, 0xC0)
static __thread uint64_t rthc_thread_state
#if __has_attribute(tls_model) && (defined(__PIC__) || defined(__pic__) || MDBX_BUILD_SHARED_LIBRARY)
    __attribute__((tls_model("local-dynamic")))
#endif
    ;

#if defined(__APPLE__) && defined(__SANITIZE_ADDRESS__) && !defined(MDBX_ATTRIBUTE_NO_SANITIZE_ADDRESS)
/* Avoid ASAN-trap due the target TLS-variable feed by Darwin's tlv_free() */
#define MDBX_ATTRIBUTE_NO_SANITIZE_ADDRESS __attribute__((__no_sanitize_address__, __noinline__))
#else
#define MDBX_ATTRIBUTE_NO_SANITIZE_ADDRESS inline
#endif

MDBX_ATTRIBUTE_NO_SANITIZE_ADDRESS static uint64_t rthc_read(const void *rthc) { return *(volatile uint64_t *)rthc; }

MDBX_ATTRIBUTE_NO_SANITIZE_ADDRESS static uint64_t rthc_compare_and_clean(const void *rthc, const uint64_t signature) {
#if MDBX_64BIT_CAS
  return atomic_cas64((mdbx_atomic_uint64_t *)rthc, signature, 0);
#elif __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
  return atomic_cas32((mdbx_atomic_uint32_t *)rthc, (uint32_t)signature, 0);
#elif __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
  return atomic_cas32((mdbx_atomic_uint32_t *)rthc, (uint32_t)(signature >> 32), 0);
#else
#error "FIXME: Unsupported byte order"
#endif
}

static inline int rthc_atexit(void (*dtor)(void *), void *obj, void *dso_symbol) {
#ifndef MDBX_HAVE_CXA_THREAD_ATEXIT_IMPL
#if defined(LIBCXXABI_HAS_CXA_THREAD_ATEXIT_IMPL) || defined(HAVE___CXA_THREAD_ATEXIT_IMPL) ||                         \
    __GLIBC_PREREQ(2, 18) || defined(BIONIC)
#define MDBX_HAVE_CXA_THREAD_ATEXIT_IMPL 1
#else
#define MDBX_HAVE_CXA_THREAD_ATEXIT_IMPL 0
#endif
#endif /* MDBX_HAVE_CXA_THREAD_ATEXIT_IMPL */

#ifndef MDBX_HAVE_CXA_THREAD_ATEXIT
#if defined(LIBCXXABI_HAS_CXA_THREAD_ATEXIT) || defined(HAVE___CXA_THREAD_ATEXIT)
#define MDBX_HAVE_CXA_THREAD_ATEXIT 1
#elif !MDBX_HAVE_CXA_THREAD_ATEXIT_IMPL && (defined(__linux__) || defined(__gnu_linux__))
#define MDBX_HAVE_CXA_THREAD_ATEXIT 1
#else
#define MDBX_HAVE_CXA_THREAD_ATEXIT 0
#endif
#endif /* MDBX_HAVE_CXA_THREAD_ATEXIT */

  int rc = MDBX_ENOSYS;
#if MDBX_HAVE_CXA_THREAD_ATEXIT_IMPL && !MDBX_HAVE_CXA_THREAD_ATEXIT
#define __cxa_thread_atexit __cxa_thread_atexit_impl
#endif
#if MDBX_HAVE_CXA_THREAD_ATEXIT || defined(__cxa_thread_atexit)
  extern int __cxa_thread_atexit(void (*dtor)(void *), void *obj, void *dso_symbol) MDBX_WEAK_IMPORT_ATTRIBUTE;
  if (&__cxa_thread_atexit)
    rc = __cxa_thread_atexit(dtor, obj, dso_symbol);
#elif defined(__APPLE__) || defined(_DARWIN_C_SOURCE)
  extern void _tlv_atexit(void (*termfunc)(void *objAddr), void *objAddr) MDBX_WEAK_IMPORT_ATTRIBUTE;
  if (&_tlv_atexit) {
    (void)dso_symbol;
    _tlv_atexit(dtor, obj);
    rc = 0;
  }
#else
  (void)dtor;
  (void)obj;
  (void)dso_symbol;
#endif
  return rc;
}

__cold void workaround_glibc_bug21031(void) {
  /* Workaround for https://sourceware.org/bugzilla/show_bug.cgi?id=21031
   *
   * Due race between pthread_key_delete() and __nptl_deallocate_tsd()
   * The destructor(s) of thread-local-storage object(s) may be running
   * in another thread(s) and be blocked or not finished yet.
   * In such case we get a SEGFAULT after unload this library DSO.
   *
   * So just by yielding a few timeslices we give a chance
   * to such destructor(s) for completion and avoids segfault. */
  sched_yield();
  sched_yield();
  sched_yield();
}
#endif /* !Windows */

void rthc_lock(void) {
#if defined(_WIN32) || defined(_WIN64)
  EnterCriticalSection(&rthc_critical_section);
#else
  ENSURE(nullptr, osal_pthread_mutex_lock(&rthc_mutex) == 0);
#endif
}

void rthc_unlock(void) {
#if defined(_WIN32) || defined(_WIN64)
  LeaveCriticalSection(&rthc_critical_section);
#else
  ENSURE(nullptr, pthread_mutex_unlock(&rthc_mutex) == 0);
#endif
}

static inline int thread_key_create(osal_thread_key_t *key) {
  int rc;
#if defined(_WIN32) || defined(_WIN64)
  *key = TlsAlloc();
  rc = (*key != TLS_OUT_OF_INDEXES) ? MDBX_SUCCESS : GetLastError();
#else
  rc = pthread_key_create(key, nullptr);
#endif
  TRACE("&key = %p, value %" PRIuPTR ", rc %d", __Wpedantic_format_voidptr(key), (uintptr_t)*key, rc);
  return rc;
}

void thread_rthc_set(osal_thread_key_t key, const void *value) {
#if defined(_WIN32) || defined(_WIN64)
  ENSURE(nullptr, TlsSetValue(key, (void *)value));
#else
  const uint64_t sign_registered = MDBX_THREAD_RTHC_REGISTERED(&rthc_thread_state);
  const uint64_t sign_counted = MDBX_THREAD_RTHC_COUNTED(&rthc_thread_state);
  if (value && unlikely(rthc_thread_state != sign_registered && rthc_thread_state != sign_counted)) {
    rthc_thread_state = sign_registered;
    TRACE("thread registered 0x%" PRIxPTR, osal_thread_self());
    if (rthc_atexit(rthc_thread_dtor, &rthc_thread_state, (void *)&mdbx_version /* dso_anchor */)) {
      ENSURE(nullptr, pthread_setspecific(rthc_key, &rthc_thread_state) == 0);
      rthc_thread_state = sign_counted;
      const unsigned count_before = atomic_add32(&rthc_pending, 1);
      ENSURE(nullptr, count_before < INT_MAX);
      NOTICE("fallback to pthreads' tsd, key %" PRIuPTR ", count %u", (uintptr_t)rthc_key, count_before);
      (void)count_before;
    }
  }
  ENSURE(nullptr, pthread_setspecific(key, value) == 0);
#endif
}

/* dtor called for thread, i.e. for all mdbx's environment objects */
__cold void rthc_thread_dtor(void *rthc) {
  rthc_lock();
  const uint32_t current_pid = osal_getpid();
#if defined(_WIN32) || defined(_WIN64)
  TRACE(">> pid %d, thread 0x%" PRIxPTR ", module %p", current_pid, osal_thread_self(), rthc);
#else
  TRACE(">> pid %d, thread 0x%" PRIxPTR ", rthc %p", current_pid, osal_thread_self(), rthc);
#endif

  for (size_t i = 0; i < rthc_count; ++i) {
    MDBX_env *const env = rthc_table[i].env;
    if (env->pid != current_pid)
      continue;
    if (!(env->flags & ENV_TXKEY))
      continue;
    reader_slot_t *const reader = thread_rthc_get(env->me_txkey);
    reader_slot_t *const begin = &env->lck_mmap.lck->rdt[0];
    reader_slot_t *const end = &env->lck_mmap.lck->rdt[env->max_readers];
    if (reader < begin || reader >= end)
      continue;
#if !defined(_WIN32) && !defined(_WIN64)
    if (pthread_setspecific(env->me_txkey, nullptr) != 0) {
      TRACE("== thread 0x%" PRIxPTR ", rthc %p: ignore race with tsd-key deletion", osal_thread_self(),
            __Wpedantic_format_voidptr(reader));
      continue /* ignore race with tsd-key deletion by mdbx_env_close() */;
    }
#endif

    TRACE("== thread 0x%" PRIxPTR ", rthc %p, [%zi], %p ... %p (%+i), rtch-pid %i, "
          "current-pid %i",
          osal_thread_self(), __Wpedantic_format_voidptr(reader), i, __Wpedantic_format_voidptr(begin),
          __Wpedantic_format_voidptr(end), (int)(reader - begin), reader->pid.weak, current_pid);
    if (atomic_load32(&reader->pid, mo_Relaxed) == current_pid) {
      TRACE("==== thread 0x%" PRIxPTR ", rthc %p, cleanup", osal_thread_self(), __Wpedantic_format_voidptr(reader));
      (void)atomic_cas32(&reader->pid, current_pid, 0);
      atomic_store32(&env->lck->rdt_refresh_flag, true, mo_Relaxed);
    }
  }

#if defined(_WIN32) || defined(_WIN64)
  TRACE("<< thread 0x%" PRIxPTR ", module %p", osal_thread_self(), rthc);
  rthc_unlock();
#else
  const uint64_t sign_registered = MDBX_THREAD_RTHC_REGISTERED(rthc);
  const uint64_t sign_counted = MDBX_THREAD_RTHC_COUNTED(rthc);
  const uint64_t state = rthc_read(rthc);
  if (state == sign_registered && rthc_compare_and_clean(rthc, sign_registered)) {
    TRACE("== thread 0x%" PRIxPTR ", rthc %p, pid %d, self-status %s (0x%08" PRIx64 ")", osal_thread_self(), rthc,
          osal_getpid(), "registered", state);
  } else if (state == sign_counted && rthc_compare_and_clean(rthc, sign_counted)) {
    TRACE("== thread 0x%" PRIxPTR ", rthc %p, pid %d, self-status %s (0x%08" PRIx64 ")", osal_thread_self(), rthc,
          osal_getpid(), "counted", state);
    ENSURE(nullptr, atomic_sub32(&rthc_pending, 1) > 0);
  } else {
    WARNING("thread 0x%" PRIxPTR ", rthc %p, pid %d, self-status %s (0x%08" PRIx64 ")", osal_thread_self(), rthc,
            osal_getpid(), "wrong", state);
  }

  if (atomic_load32(&rthc_pending, mo_AcquireRelease) == 0) {
    TRACE("== thread 0x%" PRIxPTR ", rthc %p, pid %d, wake", osal_thread_self(), rthc, osal_getpid());
    ENSURE(nullptr, pthread_cond_broadcast(&rthc_cond) == 0);
  }

  TRACE("<< thread 0x%" PRIxPTR ", rthc %p", osal_thread_self(), rthc);
  /* Allow tail call optimization, i.e. gcc should generate the jmp instruction
   * instead of a call for pthread_mutex_unlock() and therefore CPU could not
   * return to current DSO's code section, which may be unloaded immediately
   * after the mutex got released. */
  pthread_mutex_unlock(&rthc_mutex);
#endif
}

__cold int rthc_register(MDBX_env *const env) {
  TRACE(">> env %p, rthc_count %u, rthc_limit %u", __Wpedantic_format_voidptr(env), rthc_count, rthc_limit);

  int rc = MDBX_SUCCESS;
  for (size_t i = 0; i < rthc_count; ++i)
    if (unlikely(rthc_table[i].env == env)) {
      rc = MDBX_PANIC;
      goto bailout;
    }

  env->me_txkey = 0;
  if (unlikely(rthc_count == rthc_limit)) {
    rthc_entry_t *new_table =
        osal_realloc((rthc_table == rthc_table_static) ? nullptr : rthc_table, sizeof(rthc_entry_t) * rthc_limit * 2);
    if (unlikely(new_table == nullptr)) {
      rc = MDBX_ENOMEM;
      goto bailout;
    }
    if (rthc_table == rthc_table_static)
      memcpy(new_table, rthc_table, sizeof(rthc_entry_t) * rthc_limit);
    rthc_table = new_table;
    rthc_limit *= 2;
  }

  if ((env->flags & MDBX_NOSTICKYTHREADS) == 0) {
    rc = thread_key_create(&env->me_txkey);
    if (unlikely(rc != MDBX_SUCCESS))
      goto bailout;
    env->flags |= ENV_TXKEY;
  }

  rthc_table[rthc_count].env = env;
  TRACE("== [%i] = env %p, key %" PRIuPTR, rthc_count, __Wpedantic_format_voidptr(env), (uintptr_t)env->me_txkey);
  ++rthc_count;

bailout:
  TRACE("<< env %p, key %" PRIuPTR ", rthc_count %u, rthc_limit %u, rc %d", __Wpedantic_format_voidptr(env),
        (uintptr_t)env->me_txkey, rthc_count, rthc_limit, rc);
  return rc;
}

__cold static int rthc_drown(MDBX_env *const env) {
  const uint32_t current_pid = osal_getpid();
  int rc = MDBX_SUCCESS;
  MDBX_env *inprocess_neighbor = nullptr;
  if (likely(env->lck_mmap.lck && current_pid == env->pid)) {
    reader_slot_t *const begin = &env->lck_mmap.lck->rdt[0];
    reader_slot_t *const end = &env->lck_mmap.lck->rdt[env->max_readers];
    TRACE("== %s env %p pid %d, readers %p ...%p, current-pid %d", (current_pid == env->pid) ? "cleanup" : "skip",
          __Wpedantic_format_voidptr(env), env->pid, __Wpedantic_format_voidptr(begin), __Wpedantic_format_voidptr(end),
          current_pid);
    bool cleaned = false;
    for (reader_slot_t *r = begin; r < end; ++r) {
      if (atomic_load32(&r->pid, mo_Relaxed) == current_pid) {
        atomic_store32(&r->pid, 0, mo_AcquireRelease);
        TRACE("== cleanup %p", __Wpedantic_format_voidptr(r));
        cleaned = true;
      }
    }
    if (cleaned)
      atomic_store32(&env->lck_mmap.lck->rdt_refresh_flag, true, mo_Relaxed);
    rc = rthc_uniq_check(&env->lck_mmap, &inprocess_neighbor);
    if (!inprocess_neighbor && env->registered_reader_pid && env->lck_mmap.fd != INVALID_HANDLE_VALUE) {
      int err = lck_rpid_clear(env);
      rc = rc ? rc : err;
    }
  }
  int err = lck_destroy(env, inprocess_neighbor, current_pid);
  env->pid = 0;
  return rc ? rc : err;
}

__cold int rthc_remove(MDBX_env *const env) {
  TRACE(">>> env %p, key %zu, rthc_count %u, rthc_limit %u", __Wpedantic_format_voidptr(env), (uintptr_t)env->me_txkey,
        rthc_count, rthc_limit);

  int rc = MDBX_SUCCESS;
  if (likely(env->pid))
    rc = rthc_drown(env);

  for (size_t i = 0; i < rthc_count; ++i) {
    if (rthc_table[i].env == env) {
      if (--rthc_count > 0)
        rthc_table[i] = rthc_table[rthc_count];
      else if (rthc_table != rthc_table_static) {
        void *tmp = rthc_table;
        rthc_table = rthc_table_static;
        rthc_limit = RTHC_INITIAL_LIMIT;
        osal_memory_barrier();
        osal_free(tmp);
      }
      break;
    }
  }

  TRACE("<<< %p, key %zu, rthc_count %u, rthc_limit %u", __Wpedantic_format_voidptr(env), (uintptr_t)env->me_txkey,
        rthc_count, rthc_limit);
  return rc;
}

#if !defined(_WIN32) && !defined(_WIN64)
__cold void rthc_afterfork(void) {
  NOTICE("drown %d rthc entries", rthc_count);
  for (size_t i = 0; i < rthc_count; ++i) {
    MDBX_env *const env = rthc_table[i].env;
    NOTICE("drown env %p", __Wpedantic_format_voidptr(env));
    if (env->lck_mmap.lck)
      osal_munmap(&env->lck_mmap);
    if (env->dxb_mmap.base) {
      osal_munmap(&env->dxb_mmap);
#ifdef ENABLE_MEMCHECK
      VALGRIND_DISCARD(env->valgrind_handle);
      env->valgrind_handle = -1;
#endif /* ENABLE_MEMCHECK */
    }
    env->lck = lckless_stub(env);
    rthc_drown(env);
  }
  if (rthc_table != rthc_table_static)
    osal_free(rthc_table);
  rthc_count = 0;
  rthc_table = rthc_table_static;
  rthc_limit = RTHC_INITIAL_LIMIT;
  rthc_pending.weak = 0;
}
#endif /* ! Windows */

__cold void rthc_ctor(void) {
#if defined(_WIN32) || defined(_WIN64)
  InitializeCriticalSection(&rthc_critical_section);
#else
  ENSURE(nullptr, pthread_atfork(nullptr, nullptr, rthc_afterfork) == 0);
  ENSURE(nullptr, pthread_key_create(&rthc_key, rthc_thread_dtor) == 0);
  TRACE("pid %d, &mdbx_rthc_key = %p, value 0x%x", osal_getpid(), __Wpedantic_format_voidptr(&rthc_key),
        (unsigned)rthc_key);
#endif
}

__cold void rthc_dtor(const uint32_t current_pid) {
  rthc_lock();
#if !defined(_WIN32) && !defined(_WIN64)
  uint64_t *rthc = pthread_getspecific(rthc_key);
  TRACE("== thread 0x%" PRIxPTR ", rthc %p, pid %d, self-status 0x%08" PRIx64 ", left %d", osal_thread_self(),
        __Wpedantic_format_voidptr(rthc), current_pid, rthc ? rthc_read(rthc) : ~UINT64_C(0),
        atomic_load32(&rthc_pending, mo_Relaxed));
  if (rthc) {
    const uint64_t sign_registered = MDBX_THREAD_RTHC_REGISTERED(rthc);
    const uint64_t sign_counted = MDBX_THREAD_RTHC_COUNTED(rthc);
    const uint64_t state = rthc_read(rthc);
    if (state == sign_registered && rthc_compare_and_clean(rthc, sign_registered)) {
      TRACE("== thread 0x%" PRIxPTR ", rthc %p, pid %d, self-status %s (0x%08" PRIx64 ")", osal_thread_self(),
            __Wpedantic_format_voidptr(rthc), current_pid, "registered", state);
    } else if (state == sign_counted && rthc_compare_and_clean(rthc, sign_counted)) {
      TRACE("== thread 0x%" PRIxPTR ", rthc %p, pid %d, self-status %s (0x%08" PRIx64 ")", osal_thread_self(),
            __Wpedantic_format_voidptr(rthc), current_pid, "counted", state);
      ENSURE(nullptr, atomic_sub32(&rthc_pending, 1) > 0);
    } else {
      WARNING("thread 0x%" PRIxPTR ", rthc %p, pid %d, self-status %s (0x%08" PRIx64 ")", osal_thread_self(),
              __Wpedantic_format_voidptr(rthc), current_pid, "wrong", state);
    }
  }

  struct timespec abstime;
  ENSURE(nullptr, clock_gettime(CLOCK_REALTIME, &abstime) == 0);
  abstime.tv_nsec += 1000000000l / 10;
  if (abstime.tv_nsec >= 1000000000l) {
    abstime.tv_nsec -= 1000000000l;
    abstime.tv_sec += 1;
  }
#if MDBX_DEBUG > 0
  abstime.tv_sec += 600;
#endif

  for (unsigned left; (left = atomic_load32(&rthc_pending, mo_AcquireRelease)) > 0;) {
    NOTICE("tls-cleanup: pid %d, pending %u, wait for...", current_pid, left);
    const int rc = pthread_cond_timedwait(&rthc_cond, &rthc_mutex, &abstime);
    if (rc && rc != EINTR)
      break;
  }
  thread_key_delete(rthc_key);
#endif

  for (size_t i = 0; i < rthc_count; ++i) {
    MDBX_env *const env = rthc_table[i].env;
    if (env->pid != current_pid)
      continue;
    if (!(env->flags & ENV_TXKEY))
      continue;
    env->flags -= ENV_TXKEY;
    reader_slot_t *const begin = &env->lck_mmap.lck->rdt[0];
    reader_slot_t *const end = &env->lck_mmap.lck->rdt[env->max_readers];
    thread_key_delete(env->me_txkey);
    bool cleaned = false;
    for (reader_slot_t *reader = begin; reader < end; ++reader) {
      TRACE("== [%zi] = key %" PRIuPTR ", %p ... %p, rthc %p (%+i), "
            "rthc-pid %i, current-pid %i",
            i, (uintptr_t)env->me_txkey, __Wpedantic_format_voidptr(begin), __Wpedantic_format_voidptr(end),
            __Wpedantic_format_voidptr(reader), (int)(reader - begin), reader->pid.weak, current_pid);
      if (atomic_load32(&reader->pid, mo_Relaxed) == current_pid) {
        (void)atomic_cas32(&reader->pid, current_pid, 0);
        TRACE("== cleanup %p", __Wpedantic_format_voidptr(reader));
        cleaned = true;
      }
    }
    if (cleaned)
      atomic_store32(&env->lck->rdt_refresh_flag, true, mo_Relaxed);
  }

  rthc_limit = rthc_count = 0;
  if (rthc_table != rthc_table_static)
    osal_free(rthc_table);
  rthc_table = nullptr;
  rthc_unlock();

#if defined(_WIN32) || defined(_WIN64)
  DeleteCriticalSection(&rthc_critical_section);
#else
  /* LY: yielding a few timeslices to give a more chance
   * to racing destructor(s) for completion. */
  workaround_glibc_bug21031();
#endif
}
