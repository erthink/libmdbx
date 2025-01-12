/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2024
/// \copyright SPDX-License-Identifier: Apache-2.0

#include "test.h++"

#if !defined(_WIN32) && !defined(_WIN64)

#include <sys/types.h>
#include <sys/wait.h>

class testcase_smoke4fork : public testcase {
  using inherited = testcase;

protected:
  bool dbi_invalid{true};
  bool dbi_stable{false};
  unsigned dbi_state{0};

public:
  testcase_smoke4fork(const actor_config &config, const mdbx_pid_t pid) : testcase(config, pid) {}
  virtual void txn_end(bool abort) override;
  bool run() override;
  virtual bool smoke() = 0;
  bool open_dbi();
};

bool testcase_smoke4fork::open_dbi() {
  if (!dbi || dbi_invalid) {
    if (dbi_stable || (mdbx_txn_flags(txn_guard.get()) & MDBX_TXN_RDONLY) == 0) {
      dbi = db_table_open(!dbi_stable);
      dbi_invalid = false;
    }
  }

  dbi_state = 0;
  if (dbi && !dbi_invalid) {
    unsigned unused_dbi_flags;
    int err = mdbx_dbi_flags_ex(txn_guard.get(), dbi, &unused_dbi_flags, &dbi_state);
    if (unlikely(err != MDBX_SUCCESS))
      failure_perror("mdbx_dbi_flags_ex()", err);
    if ((dbi_state & (MDBX_DBI_CREAT | MDBX_DBI_FRESH)) == 0)
      dbi_stable = true;
  }
  return !dbi_invalid;
}

void testcase_smoke4fork::txn_end(bool abort) {
  if (dbi) {
    if (abort) {
      if (dbi_state & MDBX_DBI_CREAT)
        dbi_stable = false;
      if (dbi_state & MDBX_DBI_FRESH)
        dbi_invalid = true;
    } else {
      if (dbi_state & (MDBX_DBI_CREAT | MDBX_DBI_FRESH))
        dbi_stable = true;
    }
    dbi_state = 0;
  }
  inherited::txn_end(abort);
}

bool testcase_smoke4fork::run() {
  static std::vector<pid_t> history;
  const pid_t current_pid = getpid();
  if (history.empty() || current_pid != history.front()) {
    history.push_back(current_pid);
    if (history.size() > /* TODO: add test option */ 2) {
      log_notice("force exit to avoid fork-bomb: deep %zu, pid stack", history.size());
      for (const auto pid : history)
        logging::feed(" %d", pid);
      logging::ln();
      log_flush();
      exit(0);
    }
  }
  const int deep = (int)history.size();

  int err = db_open__begin__table_create_open_clean(dbi);
  if (unlikely(err != MDBX_SUCCESS)) {
    log_notice("fork[deep %d, pid %d]: bailout-prepare due '%s'", deep, current_pid, mdbx_strerror(err));
    return false;
  }
  open_dbi();

  if (flipcoin()) {
    if (!smoke()) {
      log_notice("%s[deep %d, pid %d] probe %s", "pre-fork", deep, current_pid, "failed");
      return false;
    }
    log_verbose("%s[deep %d, pid %d] probe %s", "pre-fork", deep, current_pid, "done");
  } else {
    log_verbose("%s[deep %d, pid %d] probe %s", "pre-fork", deep, current_pid, "skipped");
#ifdef __SANITIZE_ADDRESS__
    const bool commit_txn_to_avoid_memleak = true;
#else
    const bool commit_txn_to_avoid_memleak = !RUNNING_ON_VALGRIND && flipcoin();
#endif
    if (commit_txn_to_avoid_memleak && txn_guard)
      txn_end(false);
  }

  log_flush();
  const pid_t child = fork();
  if (child < 0)
    failure_perror("fork()", errno);

  if (child == 0) {
    const pid_t new_pid = getpid();
    log_verbose(">>> %s, deep %d, parent-pid %d, child-pid %d", "mdbx_env_resurrect_after_fork()", deep, current_pid,
                new_pid);
    log_flush();
    int err = mdbx_env_resurrect_after_fork(db_guard.get());
    log_verbose("<<< %s, deep %d, parent-pid %d, child-pid %d, err %d", "mdbx_env_resurrect_after_fork()", deep,
                current_pid, new_pid, err);
    log_flush();
    if (err != MDBX_SUCCESS)
      failure_perror("mdbx_env_resurrect_after_fork()", err);
    if (txn_guard) {
      if (dbi_state & MDBX_DBI_CREAT)
        dbi_invalid = true;
      // if (dbi_state & MDBX_DBI_FRESH)
      //   dbi_invalid = true;
      dbi_state = 0;
      mdbx_txn_abort(txn_guard.release());
    }
    if (!smoke()) {
      log_notice("%s[deep %d, pid %d] probe %s", "fork-child", deep, new_pid, "failed");
      return false;
    }
    log_verbose("%s[deep %d, pid %d] probe %s", "fork-child", deep, new_pid, "done");
    log_flush();
    return true;
  }

  if (txn_guard)
    txn_end(false);

  int status = 0xdeadbeef;
  if (waitpid(child, &status, 0) != child)
    failure_perror("waitpid()", errno);

  if (WIFEXITED(status)) {
    const int code = WEXITSTATUS(status);
    if (code != EXIT_SUCCESS) {
      log_notice("%s[deep %d, pid %d] child-pid %d failed, err %d", "fork-child", deep, current_pid, child, code);
      return false;
    }
    log_notice("%s[deep %d, pid %d] child-pid %d done", "fork-child", deep, current_pid, child);
  } else if (WIFSIGNALED(status)) {
    const int sig = WTERMSIG(status);
    switch (sig) {
    case SIGABRT:
    case SIGBUS:
    case SIGFPE:
    case SIGILL:
    case SIGSEGV:
      log_notice("%s[deep %d, pid %d] child-pid %d %s by SIG%s", "fork-child", deep, current_pid, child, "terminated",
                 signal_name(sig));
      break;
    default:
      log_notice("%s[deep %d, pid %d] child-id %d %s by SIG%s", "fork-child", deep, current_pid, child, "killed",
                 signal_name(sig));
    }
    return false;
  } else {
    assert(false);
  }

  if (!smoke()) {
    log_notice("%s[deep %d, pid %d] probe %s", "post-fork", deep, current_pid, "failed");
    return false;
  }
  log_verbose("%s[deep %d, pid %d] probe %s", "post-fork", deep, current_pid, "done");
  return true;
}

//-----------------------------------------------------------------------------

class testcase_forkread : public testcase_smoke4fork {
  using inherited = testcase_smoke4fork;

public:
  testcase_forkread(const actor_config &config, const mdbx_pid_t pid) : testcase_smoke4fork(config, pid) {}
  bool smoke() override;
};
REGISTER_TESTCASE(forkread);

bool testcase_forkread::smoke() {
  MDBX_envinfo env_info;
  int err = mdbx_env_info_ex(db_guard.get(), txn_guard.get(), &env_info, sizeof(env_info));
  if (err)
    failure_perror("mdbx_env_info_ex()", err);

  if (!txn_guard)
    txn_begin(true);

  MDBX_txn_info txn_info;
  err = mdbx_txn_info(txn_guard.get(), &txn_info, sizeof(txn_info));
  if (err)
    failure_perror("mdbx_txn_info()", err);
  fetch_canary();
  err = mdbx_env_info_ex(db_guard.get(), txn_guard.get(), &env_info, sizeof(env_info));
  if (err)
    failure_perror("mdbx_env_info_ex()", err);

  uint64_t seq;
  if (dbi_invalid) {
    err = mdbx_dbi_sequence(txn_guard.get(), dbi, &seq, 0);
    if (unlikely(err != (dbi ? MDBX_BAD_DBI : MDBX_SUCCESS)))
      failure("unexpected '%s' from mdbx_dbi_sequence(get, bad_dbi %d)", mdbx_strerror(err), dbi);
    open_dbi();
  }
  if (!dbi_invalid) {
    err = mdbx_dbi_sequence(txn_guard.get(), dbi, &seq, 0);
    if (unlikely(err != MDBX_SUCCESS))
      failure("unexpected '%s' from mdbx_dbi_sequence(get, dbi %d)", mdbx_strerror(err), dbi);
  }
  txn_end(false);
  return true;
}

//-----------------------------------------------------------------------------

class testcase_forkwrite : public testcase_forkread {
  using inherited = testcase_forkread;

public:
  testcase_forkwrite(const actor_config &config, const mdbx_pid_t pid) : testcase_forkread(config, pid) {}
  bool smoke() override;
};
REGISTER_TESTCASE(forkwrite);

bool testcase_forkwrite::smoke() {
  const bool firstly_read = flipcoin();
  if (firstly_read) {
    if (!testcase_forkread::smoke())
      return false;
  }

  if (!txn_guard)
    txn_begin(false);

  uint64_t seq;
  if (dbi_invalid) {
    int err = mdbx_dbi_sequence(txn_guard.get(), dbi, &seq, 0);
    if (unlikely(err != (dbi ? MDBX_BAD_DBI : MDBX_EACCESS)))
      failure("unexpected '%s' from mdbx_dbi_sequence(get, bad_dbi %d)", mdbx_strerror(err), dbi);
    open_dbi();
  }
  if (!dbi_invalid) {
    int err = mdbx_dbi_sequence(txn_guard.get(), dbi, &seq, 1);
    if (unlikely(err != MDBX_SUCCESS))
      failure("unexpected '%s' from mdbx_dbi_sequence(inc, dbi %d)", mdbx_strerror(err), dbi);
  }
  txn_end(false);

  if (!firstly_read && !testcase_forkread::smoke())
    return false;
  return true;
}

#endif /* Windows */
