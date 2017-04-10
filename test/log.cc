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

#include "test.h"

void failure(const char *fmt, ...) {
  va_list ap;
  fflush(NULL);
  va_start(ap, fmt);
  loggging::output(loggging::failure, fmt, ap);
  va_end(ap);
  fflush(NULL);
  exit(EXIT_FAILURE);
}

const char *test_strerror(int errnum) {
  static __thread char buf[1024];
  return mdbx_strerror_r(errnum, buf, sizeof(buf));
}

void __noreturn failure_perror(const char *what, int errnum) {
  failure("%s failed: %s (%d)\n", what, test_strerror(errnum), errnum);
}

//-----------------------------------------------------------------------------

namespace loggging {

static std::string prefix;
static loglevel level;

void setup(loglevel _level, const std::string &_prefix) {
  level = (_level > error) ? failure : _level;
  prefix = _prefix;
}

void setup(const std::string &_prefix) { prefix = _prefix; }

const char *level2str(const loglevel level) {
  switch (level) {
  default:
    return "invalid/unknown";
  case trace:
    return "trace";
  case info:
    return "info";
  case notice:
    return "notice";
  case warning:
    return "warning";
  case error:
    return "error";
  case failure:
    return "failure";
  }
}

void output(loglevel priority, const char *format, va_list ap) {
  if (priority >= level) {
    FILE *out = (priority >= error) ? stderr : stdout;
    fprintf(out, "[ %u %-10s %6s ] " /* TODO */, osal_getpid(), prefix.c_str(),
            level2str(priority));
    vfprintf(out, format, ap);
    size_t len = strlen(format);
    if (len && format[len - 1] != '\n')
      putc('\n', out);
  }
}

} /* namespace log */

void log_trace(const char *msg, ...) {
  if (loggging::trace >= loggging::level) {
    va_list ap;
    va_start(ap, msg);
    loggging::output(loggging::trace, msg, ap);
    va_end(ap);
  }
}

void log_info(const char *msg, ...) {
  if (loggging::info >= loggging::level) {
    va_list ap;
    va_start(ap, msg);
    loggging::output(loggging::info, msg, ap);
    va_end(ap);
  }
}

void log_notice(const char *msg, ...) {
  if (loggging::notice >= loggging::level) {
    va_list ap;
    va_start(ap, msg);
    loggging::output(loggging::notice, msg, ap);
    va_end(ap);
  }
}

void log_warning(const char *msg, ...) {
  if (loggging::warning >= loggging::level) {
    va_list ap;
    va_start(ap, msg);
    loggging::output(loggging::warning, msg, ap);
    va_end(ap);
  }
}

void log_error(const char *msg, ...) {
  if (loggging::error >= loggging::level) {
    va_list ap;
    va_start(ap, msg);
    loggging::output(loggging::error, msg, ap);
    va_end(ap);
  }
}

void log_touble(const char *where, const char *what, int errnum) {
  log_error("%s: %s %s", where, what, test_strerror(errnum));
}
