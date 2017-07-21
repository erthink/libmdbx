/*
 * Copyright 2017 Leonid Yuriev <leo@yuriev.ru>
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

#include "base.h"

void __noreturn usage(void);

#ifdef __GNUC__
#define __printf_args(format_index, first_arg)                                 \
  __attribute__((format(printf, format_index, first_arg)))
#else
#define __printf_args(format_index, first_arg)
#endif

void __noreturn __printf_args(1, 2) failure(const char *fmt, ...);

void __noreturn failure_perror(const char *what, int errnum);
const char *test_strerror(int errnum);

namespace logging {

enum loglevel {
  extra,
  trace,
  verbose,
  info,
  notice,
  warning,
  error,
  failure,
};

const char *level2str(const loglevel level);
void setup(loglevel level, const std::string &prefix);
void setup(const std::string &prefix);

bool output(const loglevel priority, const char *format, va_list ap);
bool __printf_args(2, 3)
    output(const loglevel priority, const char *format, ...);
bool feed(const char *format, va_list ap);
bool __printf_args(1, 2) feed(const char *format, ...);

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

} /* namespace log */

void __printf_args(1, 2) log_extra(const char *msg, ...);
void __printf_args(1, 2) log_trace(const char *msg, ...);
void __printf_args(1, 2) log_verbose(const char *msg, ...);
void __printf_args(1, 2) log_info(const char *msg, ...);
void __printf_args(1, 2) log_notice(const char *msg, ...);
void __printf_args(1, 2) log_warning(const char *msg, ...);
void __printf_args(1, 2) log_error(const char *msg, ...);

void log_trouble(const char *where, const char *what, int errnum);
bool log_enabled(const logging::loglevel priority);

#ifdef _DEBUG
#define TRACE(...) log_trace(__VA_ARGS__)
#else
#define TRACE(...) __noop(__VA_ARGS__)
#endif
