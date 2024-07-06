/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2024

#if defined(_WIN32) || defined(_WIN64)

#include "internals.h"

//------------------------------------------------------------------------------
// Stub for slim read-write lock
// Portion Copyright (C) 1995-2002 Brad Wilson

static void WINAPI stub_srwlock_Init(osal_srwlock_t *srwl) {
  srwl->readerCount = srwl->writerCount = 0;
}

static void WINAPI stub_srwlock_AcquireShared(osal_srwlock_t *srwl) {
  while (true) {
    assert(srwl->writerCount >= 0 && srwl->readerCount >= 0);

    //  If there's a writer already, spin without unnecessarily
    //  interlocking the CPUs
    if (srwl->writerCount != 0) {
      SwitchToThread();
      continue;
    }

    //  Add to the readers list
    _InterlockedIncrement(&srwl->readerCount);

    // Check for writers again (we may have been preempted). If
    // there are no writers writing or waiting, then we're done.
    if (srwl->writerCount == 0)
      break;

    // Remove from the readers list, spin, try again
    _InterlockedDecrement(&srwl->readerCount);
    SwitchToThread();
  }
}

static void WINAPI stub_srwlock_ReleaseShared(osal_srwlock_t *srwl) {
  assert(srwl->readerCount > 0);
  _InterlockedDecrement(&srwl->readerCount);
}

static void WINAPI stub_srwlock_AcquireExclusive(osal_srwlock_t *srwl) {
  while (true) {
    assert(srwl->writerCount >= 0 && srwl->readerCount >= 0);

    //  If there's a writer already, spin without unnecessarily
    //  interlocking the CPUs
    if (srwl->writerCount != 0) {
      SwitchToThread();
      continue;
    }

    // See if we can become the writer (expensive, because it inter-
    // locks the CPUs, so writing should be an infrequent process)
    if (_InterlockedExchange(&srwl->writerCount, 1) == 0)
      break;
  }

  // Now we're the writer, but there may be outstanding readers.
  // Spin until there aren't any more; new readers will wait now
  // that we're the writer.
  while (srwl->readerCount != 0) {
    assert(srwl->writerCount >= 0 && srwl->readerCount >= 0);
    SwitchToThread();
  }
}

static void WINAPI stub_srwlock_ReleaseExclusive(osal_srwlock_t *srwl) {
  assert(srwl->writerCount == 1 && srwl->readerCount >= 0);
  srwl->writerCount = 0;
}

static uint64_t WINAPI stub_GetTickCount64(void) {
  LARGE_INTEGER Counter, Frequency;
  return (QueryPerformanceFrequency(&Frequency) &&
          QueryPerformanceCounter(&Counter))
             ? Counter.QuadPart * 1000ul / Frequency.QuadPart
             : 0;
}

//------------------------------------------------------------------------------

struct libmdbx_imports imports;

#if __GNUC_PREREQ(8, 0)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-function-type"
#endif /* GCC/MINGW */

#define MDBX_IMPORT(HANDLE, ENTRY)                                             \
  imports.ENTRY = (MDBX_##ENTRY)GetProcAddress(HANDLE, #ENTRY)

void windows_import(void) {
  const HINSTANCE hNtdll = GetModuleHandleA("ntdll.dll");
  if (hNtdll) {
    globals.running_under_Wine = !!GetProcAddress(hNtdll, "wine_get_version");
    if (!globals.running_under_Wine) {
      MDBX_IMPORT(hNtdll, NtFsControlFile);
      MDBX_IMPORT(hNtdll, NtExtendSection);
      ENSURE(nullptr, imports.NtExtendSection);
    }
  }

  const HINSTANCE hKernel32dll = GetModuleHandleA("kernel32.dll");
  if (hKernel32dll) {
    MDBX_IMPORT(hKernel32dll, GetFileInformationByHandleEx);
    MDBX_IMPORT(hKernel32dll, GetTickCount64);
    if (!imports.GetTickCount64)
      imports.GetTickCount64 = stub_GetTickCount64;
    if (!globals.running_under_Wine) {
      MDBX_IMPORT(hKernel32dll, SetFileInformationByHandle);
      MDBX_IMPORT(hKernel32dll, GetVolumeInformationByHandleW);
      MDBX_IMPORT(hKernel32dll, GetFinalPathNameByHandleW);
      MDBX_IMPORT(hKernel32dll, PrefetchVirtualMemory);
      MDBX_IMPORT(hKernel32dll, SetFileIoOverlappedRange);
    }
  }

  const osal_srwlock_t_function srwlock_init =
      (osal_srwlock_t_function)(hKernel32dll
                                    ? GetProcAddress(hKernel32dll,
                                                     "InitializeSRWLock")
                                    : nullptr);
  if (srwlock_init) {
    imports.srwl_Init = srwlock_init;
    imports.srwl_AcquireShared = (osal_srwlock_t_function)GetProcAddress(
        hKernel32dll, "AcquireSRWLockShared");
    imports.srwl_ReleaseShared = (osal_srwlock_t_function)GetProcAddress(
        hKernel32dll, "ReleaseSRWLockShared");
    imports.srwl_AcquireExclusive = (osal_srwlock_t_function)GetProcAddress(
        hKernel32dll, "AcquireSRWLockExclusive");
    imports.srwl_ReleaseExclusive = (osal_srwlock_t_function)GetProcAddress(
        hKernel32dll, "ReleaseSRWLockExclusive");
  } else {
    imports.srwl_Init = stub_srwlock_Init;
    imports.srwl_AcquireShared = stub_srwlock_AcquireShared;
    imports.srwl_ReleaseShared = stub_srwlock_ReleaseShared;
    imports.srwl_AcquireExclusive = stub_srwlock_AcquireExclusive;
    imports.srwl_ReleaseExclusive = stub_srwlock_ReleaseExclusive;
  }

  const HINSTANCE hAdvapi32dll = GetModuleHandleA("advapi32.dll");
  if (hAdvapi32dll) {
    MDBX_IMPORT(hAdvapi32dll, RegGetValueA);
  }

  const HINSTANCE hOle32dll = GetModuleHandleA("ole32.dll");
  if (hOle32dll) {
    MDBX_IMPORT(hOle32dll, CoCreateGuid);
  }
}

#undef MDBX_IMPORT

#if __GNUC_PREREQ(8, 0)
#pragma GCC diagnostic pop
#endif /* GCC/MINGW */

#endif /* Windows */
