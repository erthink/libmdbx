/*
 * Copyright 2017-2022 Leonid Yuriev <leo@yuriev.ru>
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

#include "test.h++"

#if !(defined(_WIN32) || defined(_WIN64))

#include <atomic>
#include <pthread.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>

#ifndef MDBX_LOCKING
#error "Oops, MDBX_LOCKING is undefined!"
#endif

#if defined(__APPLE__) && (MDBX_LOCKING == MDBX_LOCKING_POSIX2001 ||           \
                           MDBX_LOCKING == MDBX_LOCKING_POSIX2008)
#include "stub/pthread_barrier.c"
#endif /* __APPLE__ && MDBX_LOCKING >= MDBX_LOCKING_POSIX2001 */

#if defined(__ANDROID_API__) && __ANDROID_API__ < 24 &&                        \
    (MDBX_LOCKING == MDBX_LOCKING_POSIX2001 ||                                 \
     MDBX_LOCKING == MDBX_LOCKING_POSIX2008)
#include "stub/pthread_barrier.c"
#endif /* __ANDROID_API__ < 24 && MDBX_LOCKING >= MDBX_LOCKING_POSIX2001 */

#if MDBX_LOCKING == MDBX_LOCKING_SYSV
#include <sys/ipc.h>
#include <sys/sem.h>
#endif /* MDBX_LOCKING == MDBX_LOCKING_SYSV */

#if MDBX_LOCKING == MDBX_LOCKING_POSIX1988
#include <semaphore.h>

#if __cplusplus >= 201103L
#include <atomic>
MDBX_MAYBE_UNUSED static __inline int atomic_decrement(std::atomic_int *p) {
  return std::atomic_fetch_sub(p, 1) - 1;
}
#else
MDBX_MAYBE_UNUSED static __inline int atomic_decrement(volatile int *p) {
#if defined(__GNUC__) || defined(__clang__)
  return __sync_sub_and_fetch(p, 1);
#elif defined(_MSC_VER)
  STATIC_ASSERT(sizeof(volatile long) == sizeof(volatile int));
  return _InterlockedDecrement((volatile long *)p);
#elif defined(__APPLE__)
  return OSAtomicDecrement32Barrier((volatile int *)p);
#else
#error FIXME: Unsupported compiler
#endif
}
#endif /* C++11 */
#endif /* MDBX_LOCKING == MDBX_LOCKING_POSIX1988 */

#if MDBX_LOCKING == MDBX_LOCKING_SYSV
static int ipc;
static pid_t ipc_overlord_pid;
static void ipc_remove(void) {
  if (ipc_overlord_pid == getpid())
    semctl(ipc, 0, IPC_RMID, nullptr);
}

#else
struct shared_t {
#if MDBX_LOCKING == MDBX_LOCKING_POSIX2001 ||                                  \
    MDBX_LOCKING == MDBX_LOCKING_POSIX2008
  pthread_barrier_t barrier;
  pthread_mutex_t mutex;
  size_t count;
  pthread_cond_t events[1];
#elif MDBX_LOCKING == MDBX_LOCKING_POSIX1988
  struct {
#if __cplusplus >= 201103L
    std::atomic_int countdown;
#else
    volatile int countdown;
#endif /* C++11 */
    sem_t sema;
  } barrier;
  size_t count;
  sem_t events[1];
#else
#error "FIXME"
#endif /* MDBX_LOCKING */
};
static shared_t *shared;
#endif /* MDBX_LOCKING != MDBX_LOCKING_SYSV */

void osal_wait4barrier(void) {
#if MDBX_LOCKING == MDBX_LOCKING_SYSV
  struct sembuf op;
  op.sem_num = 0;
  op.sem_op = -1;
  op.sem_flg = IPC_NOWAIT;
  if (semop(ipc, &op, 1))
    failure_perror("semop(dec)", errno);
  op.sem_op = 0;
  op.sem_flg = 0;
  if (semop(ipc, &op, 1))
    failure_perror("semop(wait)", errno);
#elif MDBX_LOCKING == MDBX_LOCKING_POSIX2001 ||                                \
    MDBX_LOCKING == MDBX_LOCKING_POSIX2008
  assert(shared != nullptr && shared != MAP_FAILED);
  int err = pthread_barrier_wait(&shared->barrier);
  if (err != 0 && err != PTHREAD_BARRIER_SERIAL_THREAD)
    failure_perror("pthread_barrier_wait(shared)", err);
#elif MDBX_LOCKING == MDBX_LOCKING_POSIX1988
  assert(shared != nullptr && shared != MAP_FAILED);
  int err = (atomic_decrement(&shared->barrier.countdown) > 0 &&
             sem_wait(&shared->barrier.sema))
                ? errno
                : 0;
  if (err != 0)
    failure_perror("sem_wait(shared)", err);
  if (sem_post(&shared->barrier.sema))
    failure_perror("sem_post(shared)", errno);
#else
#error "FIXME"
#endif /* MDBX_LOCKING */
}

void osal_setup(const std::vector<actor_config> &actors) {
#if MDBX_LOCKING == MDBX_LOCKING_SYSV
  if (ipc_overlord_pid)
    failure("ipc already created by %ld pid", (long)ipc_overlord_pid);
  ipc_overlord_pid = getpid();
#ifndef SEM_A
#define SEM_A S_IRUSR
#endif
#ifndef SEM_R
#define SEM_R S_IWUSR
#endif
  ipc = semget(IPC_PRIVATE, actors.size() + 2, IPC_CREAT | SEM_A | SEM_R);
  if (ipc < 0)
    failure_perror("semget(IPC_PRIVATE, shared_sems)", errno);
  if (atexit(ipc_remove))
    failure_perror("atexit(ipc_remove)", errno);
  if (semctl(ipc, 0, SETVAL, (int)(actors.size() + 1)))
    failure_perror("semctl(SETVAL.0, shared_sems)", errno);
  for (size_t i = 1; i < actors.size() + 2; ++i)
    if (semctl(ipc, i, SETVAL, 1))
      failure_perror("semctl(SETVAL.N, shared_sems)", errno);
#else
  assert(shared == nullptr);
  shared = (shared_t *)mmap(
      nullptr, sizeof(shared_t) + actors.size() * sizeof(shared->events[0]),
      PROT_READ | PROT_WRITE,
      MAP_SHARED | MAP_ANONYMOUS
#ifdef MAP_HASSEMAPHORE
          | MAP_HASSEMAPHORE
#endif
      ,
      -1, 0);
  if (MAP_FAILED == (void *)shared)
    failure_perror("mmap(shared)", errno);

  shared->count = actors.size() + 1;

#if MDBX_LOCKING == MDBX_LOCKING_POSIX2001 ||                                  \
    MDBX_LOCKING == MDBX_LOCKING_POSIX2008
  pthread_barrierattr_t barrierattr;
  int err = pthread_barrierattr_init(&barrierattr);
  if (err)
    failure_perror("pthread_barrierattr_init()", err);
  err = pthread_barrierattr_setpshared(&barrierattr, PTHREAD_PROCESS_SHARED);
  if (err)
    failure_perror("pthread_barrierattr_setpshared()", err);

  err = pthread_barrier_init(&shared->barrier, &barrierattr,
                             unsigned(shared->count));
  if (err)
    failure_perror("pthread_barrier_init(shared)", err);
  pthread_barrierattr_destroy(&barrierattr);

  pthread_mutexattr_t mutexattr;
  err = pthread_mutexattr_init(&mutexattr);
  if (err)
    failure_perror("pthread_mutexattr_init()", err);
  err = pthread_mutexattr_setpshared(&mutexattr, PTHREAD_PROCESS_SHARED);
  if (err)
    failure_perror("pthread_mutexattr_setpshared()", err);

  pthread_condattr_t condattr;
  err = pthread_condattr_init(&condattr);
  if (err)
    failure_perror("pthread_condattr_init()", err);
  err = pthread_condattr_setpshared(&condattr, PTHREAD_PROCESS_SHARED);
  if (err)
    failure_perror("pthread_condattr_setpshared()", err);

  err = pthread_mutex_init(&shared->mutex, &mutexattr);
  if (err)
    failure_perror("pthread_mutex_init(shared)", err);

  for (size_t i = 0; i < shared->count; ++i) {
    pthread_cond_t *event = &shared->events[i];
    err = pthread_cond_init(event, &condattr);
    if (err)
      failure_perror("pthread_cond_init(shared)", err);
    log_trace("osal_setup: event(shared pthread_cond) %" PRIuPTR " -> %p", i,
              __Wpedantic_format_voidptr(event));
  }
  pthread_condattr_destroy(&condattr);
  pthread_mutexattr_destroy(&mutexattr);
#elif MDBX_LOCKING == MDBX_LOCKING_POSIX1988
  shared->barrier.countdown = shared->count;
  if (sem_init(&shared->barrier.sema, true, 1))
    failure_perror("sem_init(shared.barrier)", errno);
  for (size_t i = 0; i < shared->count; ++i) {
    sem_t *event = &shared->events[i];
    if (sem_init(event, true, 0))
      failure_perror("sem_init(shared.event)", errno);
    log_trace("osal_setup: event(shared sem_init) %" PRIuPTR " -> %p", i,
              __Wpedantic_format_voidptr(event));
  }
#else
#error "FIXME"
#endif /* MDBX_LOCKING */
#endif /* MDBX_LOCKING != MDBX_LOCKING_SYSV */
}

void osal_broadcast(unsigned id) {
  log_trace("osal_broadcast: event %u", id);
#if MDBX_LOCKING == MDBX_LOCKING_SYSV
  if (semctl(ipc, id + 1, SETVAL, 0))
    failure_perror("semctl(SETVAL)", errno);
#else
  assert(shared != nullptr && shared != MAP_FAILED);
  if (id >= shared->count)
    failure("osal_broadcast: id > limit");
#if MDBX_LOCKING == MDBX_LOCKING_POSIX2001 ||                                  \
    MDBX_LOCKING == MDBX_LOCKING_POSIX2008
  int err = pthread_cond_broadcast(shared->events + id);
  if (err)
    failure_perror("pthread_cond_broadcast(shared)", err);
#elif MDBX_LOCKING == MDBX_LOCKING_POSIX1988
  if (sem_post(shared->events + id))
    failure_perror("sem_post(shared)", errno);
#else
#error "FIXME"
#endif /* MDBX_LOCKING */
#endif /* MDBX_LOCKING != MDBX_LOCKING_SYSV */
}

int osal_waitfor(unsigned id) {
  log_trace("osal_waitfor: event %u", id);
#if MDBX_LOCKING == MDBX_LOCKING_SYSV
  struct sembuf op;
  memset(&op, 0, sizeof(op));
  op.sem_num = (short)(id + 1);
  int rc = semop(ipc, &op, 1) ? errno : MDBX_SUCCESS;
#else
  assert(shared != nullptr && shared != MAP_FAILED);
  if (id >= shared->count)
    failure("osal_waitfor: id > limit");

#if MDBX_LOCKING == MDBX_LOCKING_POSIX2001 ||                                  \
    MDBX_LOCKING == MDBX_LOCKING_POSIX2008
  int rc = pthread_mutex_lock(&shared->mutex);
  if (rc != 0)
    failure_perror("pthread_mutex_lock(shared)", rc);

  rc = pthread_cond_wait(shared->events + id, &shared->mutex);
  if (rc && rc != EINTR)
    failure_perror("pthread_cond_wait(shared)", rc);

  rc = pthread_mutex_unlock(&shared->mutex);
  if (rc != 0)
    failure_perror("pthread_mutex_unlock(shared)", rc);
#elif MDBX_LOCKING == MDBX_LOCKING_POSIX1988
  int rc = sem_wait(shared->events + id) ? errno : 0;
  if (rc == 0 && sem_post(shared->events + id))
    failure_perror("sem_post(shared)", errno);
#else
#error "FIXME"
#endif /* MDBX_LOCKING */
#endif /* MDBX_LOCKING != MDBX_LOCKING_SYSV */

  return (rc == 0) ? true : false;
}

//-----------------------------------------------------------------------------

const std::string
actor_config::osal_serialize(simple_checksum &checksum) const {
  (void)checksum;
  /* not used in workload, but just for testing */
  return "unix.fork";
}

bool actor_config::osal_deserialize(const char *str, const char *end,
                                    simple_checksum &checksum) {
  (void)checksum;
  /* not used in workload, but just for testing */
  return strncmp(str, "unix.fork", 9) == 0 && str + 9 == end;
}

//-----------------------------------------------------------------------------

static pid_t overlord_pid;

static std::atomic_int sigusr1_head, sigusr2_head;
static void handler_SIGUSR(int signum) {
  switch (signum) {
  case SIGUSR1:
    ++sigusr1_head;
    return;
  case SIGUSR2:
    ++sigusr2_head;
    return;
  default:
    abort();
  }
}

bool osal_progress_push(bool active) {
  if (overlord_pid) {
    if (kill(overlord_pid, active ? SIGUSR1 : SIGUSR2))
      failure_perror("osal_progress_push: kill(overload)", errno);
    return true;
  }

  return false;
}

//-----------------------------------------------------------------------------

static std::unordered_map<pid_t, actor_status> children;

static std::atomic_int sigalarm_head;
static void handler_SIGCHLD(int signum) {
  if (signum == SIGALRM)
    ++sigalarm_head;
}

mdbx_pid_t osal_getpid(void) { return getpid(); }

int osal_delay(unsigned seconds) { return sleep(seconds) ? errno : 0; }

int osal_actor_start(const actor_config &config, mdbx_pid_t &pid) {
  if (children.empty()) {
    struct sigaction act;
    memset(&act, 0, sizeof(act));
    act.sa_handler = handler_SIGCHLD;
    sigaction(SIGCHLD, &act, nullptr);
    sigaction(SIGALRM, &act, nullptr);
    act.sa_handler = handler_SIGUSR;
    sigaction(SIGUSR1, &act, nullptr);
    sigaction(SIGUSR2, &act, nullptr);

    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGCHLD);
    sigaddset(&mask, SIGUSR1);
    sigaddset(&mask, SIGUSR2);
    sigprocmask(SIG_UNBLOCK, &mask, nullptr);
  }

  pid = fork();

  if (pid == 0) {
    overlord_pid = getppid();
    const bool result = test_execute(config);
    exit(result ? EXIT_SUCCESS : EXIT_FAILURE);
  }

  if (pid < 0)
    return errno;

  log_trace("osal_actor_start: fork pid %ld for %u", (long)pid,
            config.actor_id);
  children[pid] = as_running;
  return 0;
}

actor_status osal_actor_info(const mdbx_pid_t pid) { return children.at(pid); }

void osal_killall_actors(void) {
  for (auto &pair : children) {
    kill(pair.first, SIGKILL);
    pair.second = as_killed;
  }
}

static const char *signal_name(const int sig) {
  if (sig == SIGHUP)
    return "HUP";
  if (sig == SIGINT)
    return "INT";
  if (sig == SIGQUIT)
    return "QUIT";
  if (sig == SIGILL)
    return "ILL";
  if (sig == SIGTRAP)
    return "TRAP";
  if (sig == SIGABRT)
    return "ABRT";
  if (sig == SIGBUS)
    return "BUS";
  if (sig == SIGKILL)
    return "KILL";
  if (sig == SIGUSR1)
    return "USR1";
  if (sig == SIGSEGV)
    return "SEGV";
  if (sig == SIGUSR2)
    return "USR2";
  if (sig == SIGPIPE)
    return "PIPE";
  if (sig == SIGALRM)
    return "ALRM";
  if (sig == SIGTERM)
    return "TERM";
  if (sig == SIGCHLD)
    return "CHLD";
  if (sig == SIGCONT)
    return "CONT";
  if (sig == SIGSTOP)
    return "STOP";
#ifdef SIGPOLL
  if (sig == SIGPOLL)
    return "POLL";
#endif
#ifdef SIGFPE
  if (sig == SIGFPE)
    return "FPE";
#endif
#ifdef SIGEMT
  if (sig == SIGEMT)
    return "EMT";
#endif
#ifdef SIGSTKFLT
  if (sig == SIGSTKFLT)
    return "STKFLT";
#endif
#ifdef SIGTSTP
  if (sig == SIGTSTP)
    return "TSTP";
#endif
#ifdef SIGTTIN
  if (sig == SIGTTIN)
    return "TTIN";
#endif
#ifdef SIGTTOU
  if (sig == SIGTTOU)
    return "TTOU";
#endif
#ifdef SIGURG
  if (sig == SIGURG)
    return "URG";
#endif
#ifdef SIGXCPU
  if (sig == SIGXCPU)
    return "XCPU";
#endif
#ifdef SIGXFSZ
  if (sig == SIGXFSZ)
    return "XFSZ";
#endif
#ifdef SIGVTALRM
  if (sig == SIGVTALRM)
    return "VTALRM";
#endif
#ifdef SIGPROF
  if (sig == SIGPROF)
    return "PROF";
#endif
#ifdef SIGWINCH
  if (sig == SIGWINCH)
    return "WINCH";
#endif
#ifdef SIGIO
  if (sig == SIGIO)
    return "IO";
#endif
#ifdef SIGPWR
  if (sig == SIGPWR)
    return "PWR";
#endif
#ifdef SIGSYS
  if (sig == SIGSYS)
    return "SYS";
#endif
  static char buf[32];
  snprintf(buf, sizeof(buf), "%u", sig);
  return buf;
}

int osal_actor_poll(mdbx_pid_t &pid, unsigned timeout) {
  static sig_atomic_t sigalarm_tail;
  alarm(0) /* cancel prev timeout */;
  sigalarm_tail = sigalarm_head /* reset timeout flag */;

  int options = WNOHANG;
  if (timeout) {
    alarm((timeout > INT_MAX) ? INT_MAX : timeout);
    options = 0;
  }

#ifdef WUNTRACED
  options |= WUNTRACED;
#endif
#ifdef WCONTINUED
  options |= WCONTINUED;
#endif

  while (sigalarm_tail == sigalarm_head) {
    int status;
    pid = waitpid(0, &status, options);
    const int err = errno;

    if (pid > 0) {
      if (WIFEXITED(status))
        children[pid] =
            (WEXITSTATUS(status) == EXIT_SUCCESS) ? as_successful : as_failed;
      else if (WIFSIGNALED(status)) {
#ifdef WCOREDUMP
        if (WCOREDUMP(status))
          children[pid] = as_coredump;
        else
#endif /* WCOREDUMP */
          switch (WTERMSIG(status)) {
          case SIGABRT:
          case SIGBUS:
          case SIGFPE:
          case SIGILL:
          case SIGSEGV:
            log_notice("child pid %lu terminated by SIG%s", (long)pid,
                       signal_name(WTERMSIG(status)));
            children[pid] = as_coredump;
            break;
          default:
            log_notice("child pid %lu killed by SIG%s", (long)pid,
                       signal_name(WTERMSIG(status)));
            children[pid] = as_killed;
          }
      } else if (WIFSTOPPED(status))
        children[pid] = as_debugging;
      else if (WIFCONTINUED(status))
        children[pid] = as_running;
      else {
        assert(false);
      }
      return 0;
    }

    static sig_atomic_t sigusr1_tail, sigusr2_tail;
    if (sigusr1_tail != sigusr1_head) {
      sigusr1_tail = sigusr1_head;
      logging::progress_canary(true);
      if (pid < 0 && err == EINTR)
        continue;
    }
    if (sigusr2_tail != sigusr2_head) {
      sigusr2_tail = sigusr2_head;
      logging::progress_canary(false);
      if (pid < 0 && err == EINTR)
        continue;
    }

    if (pid == 0)
      break;

    if (err != EINTR)
      return err;
  }
  return 0 /* timeout */;
}

void osal_yield(void) {
  if (sched_yield())
    failure_perror("sched_yield()", errno);
}

void osal_udelay(size_t us) {
  chrono::time until, now = chrono::now_monotonic();
  until.fixedpoint = now.fixedpoint + chrono::from_us(us).fixedpoint;
  struct timespec ts;

  static size_t threshold_us;
  if (threshold_us == 0) {
#if defined(_POSIX_CPUTIME) && _POSIX_CPUTIME > -1 &&                          \
    defined(CLOCK_PROCESS_CPUTIME_ID)
    if (clock_getres(CLOCK_PROCESS_CPUTIME_ID, &ts)) {
      int rc = errno;
      log_warning("clock_getres(CLOCK_PROCESS_CPUTIME_ID), failed errno %d",
                  rc);
    }
#endif /* CLOCK_PROCESS_CPUTIME_ID */
    if (threshold_us == 0 && clock_getres(CLOCK_MONOTONIC, &ts)) {
      int rc = errno;
      failure_perror("clock_getres(CLOCK_MONOTONIC)", rc);
    }
    chrono::time threshold = chrono::from_timespec(ts);
    assert(threshold.seconds() == 0);

    threshold_us = chrono::fractional2us(threshold.fractional);
    if (threshold_us < 1000)
      threshold_us = 1000;
  }

  ts.tv_sec = ts.tv_nsec = 0;
  if (us > threshold_us) {
    ts.tv_sec = us / 1000000u;
    ts.tv_nsec = (us % 1000000u) * 1000u;
  }

  do {
    if (us > threshold_us) {
      if (nanosleep(&ts, &ts)) {
        int rc = errno;
        /* if (rc == EINTR) { ... } ? */
        failure_perror("usleep()", rc);
      }
      us = ts.tv_sec * 1000000u + ts.tv_nsec / 1000u;
    }
    cpu_relax();

    now = chrono::now_monotonic();
  } while (until.fixedpoint > now.fixedpoint);
}

bool osal_istty(int fd) { return isatty(fd) == 1; }

std::string osal_tempdir(void) {
  const char *tempdir = getenv("TMPDIR");
  if (!tempdir)
    tempdir = getenv("TMP");
  if (!tempdir)
    tempdir = getenv("TEMPDIR");
  if (!tempdir)
    tempdir = getenv("TEMP");
  if (tempdir && *tempdir) {
    std::string dir(tempdir);
    if (dir.back() != '/')
      dir.append("/");
    return dir;
  }
  if (access("/dev/shm/", R_OK | W_OK | X_OK) == 0)
    return "/dev/shm/";
  return "";
}

#endif /* !Windows */
