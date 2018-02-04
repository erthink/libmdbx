/*
 * Copyright 2017-2018 Leonid Yuriev <leo@yuriev.ru>
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

#include "test.h"

static std::unordered_map<unsigned, HANDLE> events;
static HANDLE hBarrierSemaphore, hBarrierEvent;

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

void osal_wait4barrier(void) {
  DWORD rc = WaitForSingleObject(hBarrierSemaphore, 0);
  switch (rc) {
  default:
    failure_perror("WaitForSingleObject(BarrierSemaphore)",
                   waitstatus2errcode(rc));
  case WAIT_OBJECT_0:
    rc = WaitForSingleObject(hBarrierEvent, INFINITE);
    if (rc != WAIT_OBJECT_0)
      failure_perror("WaitForSingleObject(BarrierEvent)",
                     waitstatus2errcode(rc));
    break;
  case WAIT_TIMEOUT:
    if (!SetEvent(hBarrierEvent))
      failure_perror("SetEvent(BarrierEvent)", GetLastError());
    break;
  }
}

static HANDLE make_inharitable(HANDLE hHandle) {
  assert(hHandle != NULL && hHandle != INVALID_HANDLE_VALUE);
  if (!DuplicateHandle(GetCurrentProcess(), hHandle, GetCurrentProcess(),
                       &hHandle, 0, TRUE,
                       DUPLICATE_CLOSE_SOURCE | DUPLICATE_SAME_ACCESS))
    failure_perror("DuplicateHandle()", GetLastError());
  return hHandle;
}

void osal_setup(const std::vector<actor_config> &actors) {
  assert(events.empty());
  const size_t n = actors.size() + 1;
  events.reserve(n);

  for (unsigned i = 0; i < n; ++i) {
    HANDLE hEvent = CreateEvent(NULL, TRUE, FALSE, NULL);
    if (!hEvent)
      failure_perror("CreateEvent()", GetLastError());
    hEvent = make_inharitable(hEvent);
    log_trace("osal_setup: event %" PRIuPTR " -> %p", i, hEvent);
    events[i] = hEvent;
  }

  hBarrierSemaphore = CreateSemaphore(NULL, 0, (LONG)actors.size(), NULL);
  if (!hBarrierSemaphore)
    failure_perror("CreateSemaphore(BarrierSemaphore)", GetLastError());
  hBarrierSemaphore = make_inharitable(hBarrierSemaphore);

  hBarrierEvent = CreateEvent(NULL, TRUE, FALSE, NULL);
  if (!hBarrierEvent)
    failure_perror("CreateEvent(BarrierEvent)", GetLastError());
  hBarrierEvent = make_inharitable(hBarrierEvent);
}

void osal_broadcast(unsigned id) {
  log_trace("osal_broadcast: event %u", id);
  if (!SetEvent(events.at(id)))
    failure_perror("SetEvent()", GetLastError());
}

int osal_waitfor(unsigned id) {
  log_trace("osal_waitfor: event %u", id);
  DWORD rc = WaitForSingleObject(events.at(id), INFINITE);
  return waitstatus2errcode(rc);
}

mdbx_pid_t osal_getpid(void) { return GetCurrentProcessId(); }

int osal_delay(unsigned seconds) {
  Sleep(seconds * 1000u);
  return 0;
}

//-----------------------------------------------------------------------------

const std::string
actor_config::osal_serialize(simple_checksum &checksum) const {
  checksum.push(hBarrierSemaphore);
  checksum.push(hBarrierEvent);

  HANDLE hWait = INVALID_HANDLE_VALUE;
  if (wait4id) {
    hWait = events.at(wait4id);
    checksum.push(hWait);
  }

  HANDLE hSignal = INVALID_HANDLE_VALUE;
  if (wanna_event4signalling()) {
    hSignal = events.at(actor_id);
    checksum.push(hSignal);
  }

  return format("%p.%p.%p.%p", hBarrierSemaphore, hBarrierEvent, hWait,
                hSignal);
}

bool actor_config::osal_deserialize(const char *str, const char *end,
                                    simple_checksum &checksum) {

  std::string copy(str, end - str);
  TRACE(">> osal_deserialize(%s)\n", copy.c_str());

  assert(hBarrierSemaphore == 0);
  assert(hBarrierEvent == 0);
  assert(events.empty());

  HANDLE hWait, hSignal;
  if (sscanf_s(copy.c_str(), "%p.%p.%p.%p", &hBarrierSemaphore, &hBarrierEvent,
               &hWait, &hSignal) != 4) {
    TRACE("<< osal_deserialize: failed\n");
    return false;
  }

  checksum.push(hBarrierSemaphore);
  checksum.push(hBarrierEvent);

  if (wait4id) {
    checksum.push(hWait);
    events[wait4id] = hWait;
  }

  if (wanna_event4signalling()) {
    checksum.push(hSignal);
    events[actor_id] = hSignal;
  }

  TRACE("<< osal_deserialize: OK\n");
  return true;
}

//-----------------------------------------------------------------------------

typedef std::pair<HANDLE, actor_status> child;
static std::unordered_map<mdbx_pid_t, child> childs;

int osal_actor_start(const actor_config &config, mdbx_pid_t &pid) {
  if (childs.size() == MAXIMUM_WAIT_OBJECTS)
    failure("Could't manage more that %u actors on Windows\n",
            MAXIMUM_WAIT_OBJECTS);

  _flushall();

  STARTUPINFOA StartupInfo;
  GetStartupInfoA(&StartupInfo);

  char exename[_MAX_PATH];
  DWORD exename_size = sizeof(exename);
  if (!QueryFullProcessImageNameA(GetCurrentProcess(), 0, exename,
                                  &exename_size))
    failure_perror("QueryFullProcessImageName()", GetLastError());

  std::string cmdline = "test_mdbx.child " + thunk_param(config);

  PROCESS_INFORMATION ProcessInformation;
  if (!CreateProcessA(exename, const_cast<char *>(cmdline.c_str()),
                      NULL, // Retuned process handle is not inheritable.
                      NULL, // Retuned thread handle is not inheritable.
                      TRUE, // Child inherits all inheritable handles.
                      NORMAL_PRIORITY_CLASS | INHERIT_PARENT_AFFINITY,
                      NULL, // Inherit the parent's environment.
                      NULL, // Inherit the parent's current directory.
                      &StartupInfo, &ProcessInformation))
    return GetLastError();

  CloseHandle(ProcessInformation.hThread);
  pid = ProcessInformation.dwProcessId;
  childs[pid] = std::make_pair(ProcessInformation.hProcess, as_running);
  return 0;
}

actor_status osal_actor_info(const mdbx_pid_t pid) {
  actor_status status = childs.at(pid).second;
  if (status > as_running)
    return status;

  DWORD ExitCode;
  if (!GetExitCodeProcess(childs.at(pid).first, &ExitCode))
    failure_perror("GetExitCodeProcess()", GetLastError());

  switch (ExitCode) {
  case STILL_ACTIVE:
    return as_running;
  case EXIT_SUCCESS:
    status = as_successful;
    break;
  // case EXCEPTION_BREAKPOINT:
  case EXCEPTION_SINGLE_STEP:
    status = as_debuging;
    break;
  case STATUS_CONTROL_C_EXIT:
  case EXCEPTION_NONCONTINUABLE_EXCEPTION:
    status = as_killed;
    break;
  default:
    status = as_failed;
    break;
  }

  childs.at(pid).second = status;
  return status;
}

void osal_killall_actors(void) {
  for (auto &pair : childs)
    TerminateProcess(pair.second.first, STATUS_CONTROL_C_EXIT);
}

int osal_actor_poll(mdbx_pid_t &pid, unsigned timeout) {
  std::vector<HANDLE> handles;
  handles.reserve(childs.size());
  for (const auto &pair : childs)
    if (pair.second.second <= as_running)
      handles.push_back(pair.second.first);

  DWORD rc =
      MsgWaitForMultipleObjectsEx((DWORD)handles.size(), &handles[0],
                                  (timeout > 60) ? 60 * 1000 : timeout * 1000,
                                  QS_ALLINPUT | QS_ALLPOSTMESSAGE, 0);

  if (rc >= WAIT_OBJECT_0 && rc < WAIT_OBJECT_0 + handles.size()) {
    pid = 0;
    for (const auto &pair : childs)
      if (pair.second.first == handles[rc - WAIT_OBJECT_0]) {
        pid = pair.first;
        break;
      }
    return 0;
  }

  if (rc == WAIT_TIMEOUT) {
    pid = 0;
    return 0;
  }

  return waitstatus2errcode(rc);
}

void osal_yield(void) { SwitchToThread(); }

void osal_udelay(unsigned us) {
  chrono::time until, now = chrono::now_motonic();
  until.fixedpoint = now.fixedpoint + chrono::from_us(us).fixedpoint;

  static unsigned threshold_us;
  if (threshold_us == 0) {
#if 1
    unsigned timeslice_ms = 1;
    while (timeBeginPeriod(timeslice_ms) == TIMERR_NOCANDO)
      ++timeslice_ms;
    threshold_us = timeslice_ms * 1500u;
#else
    ULONGLONG InterruptTimePrecise_100ns;
    QueryInterruptTimePrecise(&InterruptTimePrecise_100ns);
    threshold_us = InterruptTimePrecise_100ns / 5;
#endif
    assert(threshold_us > 0);
  }

  do {
    if (us > threshold_us && us > 1000) {
      DWORD rc = SleepEx(us / 1000, TRUE);
      if (rc)
        failure_perror("SleepEx()", waitstatus2errcode(rc));
      us = 0;
    }

    YieldProcessor();
    now = chrono::now_motonic();
  } while (now.fixedpoint < until.fixedpoint);
}

bool osal_istty(int fd) { return _isatty(fd) != 0; }
