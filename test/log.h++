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

#pragma once

#include "base.h++"
#include "chrono.h++"

MDBX_NORETURN void usage(void);
MDBX_NORETURN void MDBX_PRINTF_ARGS(1, 2) failure(const char *fmt, ...);
MDBX_NORETURN void failure_perror(const char *what, int errnum);
const char *test_strerror(int errnum);

namespace logging {

enum loglevel {
  extra = MDBX_LOG_EXTRA,
  trace = MDBX_LOG_TRACE,
  debug = MDBX_LOG_DEBUG,
  verbose = MDBX_LOG_VERBOSE,
  notice = MDBX_LOG_NOTICE,
  warning = MDBX_LOG_WARN,
  error = MDBX_LOG_ERROR,
  failure = MDBX_LOG_FATAL
};

inline bool lower(loglevel left, loglevel right) {
  static_assert(MDBX_LOG_EXTRA > MDBX_LOG_FATAL, "WTF?");
  return left > right;
}

inline bool same_or_higher(loglevel left, loglevel right) {
  return left <= right;
}

const char *level2str(const loglevel level);
void setup(loglevel priority, const std::string &prefix);
void setup(const std::string &prefix);
void setlevel(loglevel priority);

void output_nocheckloglevel_ap(const loglevel priority, const char *format,
                               va_list ap);
bool MDBX_PRINTF_ARGS(2, 3)
    output(const loglevel priority, const char *format, ...);
bool feed_ap(const char *format, va_list ap);
bool MDBX_PRINTF_ARGS(1, 2) feed(const char *format, ...);

void inline MDBX_PRINTF_ARGS(2, 3)
    output_nocheckloglevel(const loglevel priority, const char *format, ...) {
  va_list ap;
  va_start(ap, format);
  output_nocheckloglevel_ap(priority, format, ap);
  va_end(ap);
}

void progress_canary(bool active);

class local_suffix {
protected:
  size_t trim_pos;
  int indent;

public:
  local_suffix(const local_suffix &) = delete;
  local_suffix(const local_suffix &&) = delete;
  const local_suffix &operator=(const local_suffix &) = delete;

  local_suffix(const char *c_str);
  local_suffix(const std::string &str);
  void push();
  void pop();
  ~local_suffix();
};

} // namespace logging

void MDBX_PRINTF_ARGS(1, 2) static inline log_null(const char *msg, ...) {
  return (void)msg;
}
void MDBX_PRINTF_ARGS(1, 2) log_extra(const char *msg, ...);
void MDBX_PRINTF_ARGS(1, 2) log_trace(const char *msg, ...);
void MDBX_PRINTF_ARGS(1, 2) log_debug(const char *msg, ...);
void MDBX_PRINTF_ARGS(1, 2) log_verbose(const char *msg, ...);
void MDBX_PRINTF_ARGS(1, 2) log_notice(const char *msg, ...);
void MDBX_PRINTF_ARGS(1, 2) log_warning(const char *msg, ...);
void MDBX_PRINTF_ARGS(1, 2) log_error(const char *msg, ...);

void log_trouble(const char *where, const char *what, int errnum);
void log_flush(void);
bool log_enabled(const logging::loglevel priority);

#ifdef _DEBUG
#define TRACE(...) log_trace(__VA_ARGS__)
#else
#define TRACE(...) log_null(__VA_ARGS__)
#endif
