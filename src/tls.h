/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2024

#pragma once

#include "essentials.h"

MDBX_INTERNAL void rthc_ctor(void);
MDBX_INTERNAL void rthc_dtor(const uint32_t current_pid);
MDBX_INTERNAL void rthc_lock(void);
MDBX_INTERNAL void rthc_unlock(void);

MDBX_INTERNAL int rthc_register(MDBX_env *const env);
MDBX_INTERNAL int rthc_remove(MDBX_env *const env);
MDBX_INTERNAL int rthc_uniq_check(const osal_mmap_t *pending, MDBX_env **found);

/* dtor called for thread, i.e. for all mdbx's environment objects */
MDBX_INTERNAL void rthc_thread_dtor(void *rthc);

static inline void *thread_rthc_get(osal_thread_key_t key) {
#if defined(_WIN32) || defined(_WIN64)
  return TlsGetValue(key);
#else
  return pthread_getspecific(key);
#endif
}

MDBX_INTERNAL void thread_rthc_set(osal_thread_key_t key, const void *value);

#if !defined(_WIN32) && !defined(_WIN64)
MDBX_INTERNAL void rthc_afterfork(void);
MDBX_INTERNAL void workaround_glibc_bug21031(void);
#endif /* !Windows */

static inline void thread_key_delete(osal_thread_key_t key) {
  TRACE("key = %" PRIuPTR, (uintptr_t)key);
#if defined(_WIN32) || defined(_WIN64)
  ENSURE(nullptr, TlsFree(key));
#else
  ENSURE(nullptr, pthread_key_delete(key) == 0);
  workaround_glibc_bug21031();
#endif
}
