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

static void fflushall() { fflush(nullptr); }

void failure(const char *fmt, ...) {
  va_list ap;
  fflush(NULL);
  va_start(ap, fmt);
  logging::output(logging::failure, fmt, ap);
  va_end(ap);
  fflushall();
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

namespace logging {

static std::string prefix;
static std::string suffix;
static loglevel level;
static FILE *last;

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
    last = (priority >= error) ? stderr : stdout;
    fprintf(last, "[ %u %-10s %6s ] %s" /* TODO */, osal_getpid(),
            prefix.c_str(), level2str(priority), suffix.c_str());
    vfprintf(last, format, ap);

    size_t len = strlen(format);
    char end = len ? format[len - 1] : '\0';
    switch (end) {
    default:
      putc('\n', last);
    case '\n':
      if (priority > info)
        fflushall();
      break;
    case ' ':
    case '_':
    case ':':
    case '|':
    case ',':
    case '\t':
    case '\b':
    case '\r':
    case '\0':
      return;
    }
  }
  last = nullptr;
}

void feed(const char *format, ...) {
  if (last) {
    va_list ap;
    va_start(ap, format);
    vfprintf(last, format, ap);
    va_end(ap);

    size_t len = strlen(format);
    if (len && format[len - 1] == '\n')
      last = nullptr;
  }
}

local_suffix::local_suffix(const char *c_str)
    : trim_pos(suffix.size()), indent(0) {
  suffix.append(c_str);
}

local_suffix::local_suffix(const std::string &str)
    : trim_pos(suffix.size()), indent(0) {
  suffix.append(str);
}

void local_suffix::push() {
  indent += 1;
  suffix.push_back('\t');
}

void local_suffix::pop() {
  assert(indent > 0);
  if (indent > 0) {
    indent -= 1;
    suffix.pop_back();
  }
}

local_suffix::~local_suffix() { suffix.erase(trim_pos); }

} /* namespace log */

void log_trace(const char *msg, ...) {
  if (logging::trace >= logging::level) {
    va_list ap;
    va_start(ap, msg);
    logging::output(logging::trace, msg, ap);
    va_end(ap);
  } else
    logging::last = nullptr;
}

void log_info(const char *msg, ...) {
  if (logging::info >= logging::level) {
    va_list ap;
    va_start(ap, msg);
    logging::output(logging::info, msg, ap);
    va_end(ap);
  } else
    logging::last = nullptr;
}

void log_notice(const char *msg, ...) {
  if (logging::notice >= logging::level) {
    va_list ap;
    va_start(ap, msg);
    logging::output(logging::notice, msg, ap);
    va_end(ap);
  } else
    logging::last = nullptr;
}

void log_warning(const char *msg, ...) {
  if (logging::warning >= logging::level) {
    va_list ap;
    va_start(ap, msg);
    logging::output(logging::warning, msg, ap);
    va_end(ap);
  } else
    logging::last = nullptr;
}

void log_error(const char *msg, ...) {
  if (logging::error >= logging::level) {
    va_list ap;
    va_start(ap, msg);
    logging::output(logging::error, msg, ap);
    va_end(ap);
  } else
    logging::last = nullptr;
}

void log_touble(const char *where, const char *what, int errnum) {
  log_error("%s: %s %s", where, what, test_strerror(errnum));
}
