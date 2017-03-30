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

void __noreturn
#ifdef __GNUC__
    __attribute__((format(printf, 1, 2)))
#endif
    failure(const char *fmt, ...);

void __noreturn failure_perror(const char *what, int errnum);
const char *test_strerror(int errnum);

namespace loggging {

enum loglevel {
  trace,
  info,
  notice,
  warning,
  error,
  failure,
};

const char *level2str(const loglevel level);
void setup(loglevel level, const std::string &prefix);
void setup(const std::string &prefix);

void output(loglevel priority, const char *format, va_list ap);

} /* namespace log */

void log_trace(const char *msg, ...);
void log_info(const char *msg, ...);
void log_notice(const char *msg, ...);
void log_warning(const char *msg, ...);
void log_error(const char *msg, ...);

void log_touble(const char *where, const char *what, int errnum);

#ifdef _DEBUG
#define TRACE(...) log_trace(__VA_ARGS__)
#else
#define TRACE(...) __noop(__VA_ARGS__)
#endif
