/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025
/// \copyright SPDX-License-Identifier: Apache-2.0

#pragma once

#include "base.h++"

void osal_setup(const std::vector<actor_config> &actors);
void osal_broadcast(unsigned id);
int osal_waitfor(unsigned id);

int osal_actor_start(const actor_config &config, mdbx_pid_t &pid);
actor_status osal_actor_info(const mdbx_pid_t pid);
void osal_killall_actors(void);
int osal_actor_poll(mdbx_pid_t &pid, unsigned timeout);
void osal_wait4barrier(void);

bool osal_progress_push(bool active);
bool osal_multiactor_mode(void);

int osal_delay(unsigned seconds);
void osal_udelay(size_t us);
void osal_yield(void);
bool osal_istty(int fd);
std::string osal_tempdir(void);

#ifdef _MSC_VER
#ifndef STDIN_FILENO
#define STDIN_FILENO _fileno(stdin)
#endif
#ifndef STDOUT_FILENO
#define STDOUT_FILENO _fileno(stdout)
#endif
#ifndef STDERR_FILENO
#define STDERR_FILENO _fileno(stderr)
#endif
#endif /* _MSC_VER */

#if !defined(_WIN32) && !defined(_WIN64)
const char *signal_name(const int sig);
#endif /* Windows */
