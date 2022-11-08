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

static void fflushall() { fflush(nullptr); }

void failure(const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  fflushall();
  logging::output_nocheckloglevel_ap(logging::failure, fmt, ap);
  va_end(ap);
  fflushall();
  exit(EXIT_FAILURE);
}

const char *test_strerror(int errnum) {
  static __thread char buf[1024];
  return mdbx_strerror_r(errnum, buf, sizeof(buf));
}

MDBX_NORETURN void failure_perror(const char *what, int errnum) {
  failure("%s failed: %s (%d)\n", what, test_strerror(errnum), errnum);
}

//-----------------------------------------------------------------------------

static void mdbx_logger(MDBX_log_level_t priority, const char *function,
                        int line, const char *fmt,
                        va_list args) MDBX_CXX17_NOEXCEPT {
  if (function) {
    if (priority == MDBX_LOG_FATAL)
      log_error("mdbx: fatal failure: %s, %d", function, line);
    logging::output_nocheckloglevel(
        logging::loglevel(priority),
        strncmp(function, "mdbx_", 5) == 0 ? "%s: " : "mdbx %s: ", function);
    logging::feed_ap(fmt, args);
  } else
    logging::feed_ap(fmt, args);
}

namespace logging {

static std::string prefix;
static std::string suffix;
static loglevel level;
static FILE *last;

void setlevel(loglevel priority) {
  level = priority;
  int rc = mdbx_setup_debug(MDBX_log_level_t(priority),
                            MDBX_DBG_ASSERT | MDBX_DBG_AUDIT | MDBX_DBG_JITTER |
                                MDBX_DBG_DUMP,
                            mdbx_logger);
  log_trace("set mdbx debug-opts: 0x%02x", rc);
}

void setup(loglevel priority, const std::string &_prefix) {
  setlevel(priority);
  prefix = _prefix;
}

void setup(const std::string &_prefix) { prefix = _prefix; }

const char *level2str(const loglevel alevel) {
  switch (alevel) {
  default:
    return "invalid/unknown";
  case extra:
    return "extra";
  case trace:
    return "trace";
  case debug:
    return "debug";
  case verbose:
    return "verbose";
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

bool output(const loglevel priority, const char *format, ...) {
  if (lower(priority, level))
    return false;

  va_list ap;
  va_start(ap, format);
  output_nocheckloglevel_ap(priority, format, ap);
  va_end(ap);
  return true;
}

void output_nocheckloglevel_ap(const logging::loglevel priority,
                               const char *format, va_list ap) {
  if (last) {
    putc('\n', last);
    fflush(last);
    if (last == stderr) {
      putc('\n', stdout);
      fflush(stdout);
    }
    last = nullptr;
  }

  chrono::time now = chrono::now_realtime();
  struct tm tm;
#ifdef _MSC_VER
  int rc = _localtime32_s(&tm, (const __time32_t *)&now.utc);
#elif defined(_WIN32) || defined(_WIN64)
  const time_t time_proxy = now.utc;
  int rc = localtime_s(&tm, &time_proxy);
#else
  time_t time = now.utc;
  int rc = localtime_r(&time, &tm) ? MDBX_SUCCESS : errno;
#endif
  if (rc != MDBX_SUCCESS)
    failure_perror("localtime_r()", rc);

  last = stdout;
  fprintf(last,
          "[ %02d%02d%02d-%02d:%02d:%02d.%06d_%05lu %-10s %.4s ] %s" /* TODO */,
          tm.tm_year - 100, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min,
          tm.tm_sec, chrono::fractional2us(now.fractional), (long)osal_getpid(),
          prefix.c_str(), level2str(priority), suffix.c_str());

  va_list ones;
  memset(&ones, 0, sizeof(ones)) /* zap MSVC and other stupid compilers */;
  if (same_or_higher(priority, error))
    va_copy(ones, ap);
  vfprintf(last, format, ap);

  size_t len = strlen(format);
  char end = len ? format[len - 1] : '\0';

  switch (end) {
  default:
    putc('\n', last);
  // fall through
  case '\n':
    fflush(last);
    last = nullptr;
  // fall through
  case ' ':
  case '_':
  case ':':
  case '|':
  case ',':
  case '\t':
  case '\b':
  case '\r':
  case '\0':
    break;
  }

  if (same_or_higher(priority, error)) {
    if (last != stderr) {
      fprintf(stderr, "[ %05lu %-10s %.4s ] %s", (long)osal_getpid(),
              prefix.c_str(), level2str(priority), suffix.c_str());
      vfprintf(stderr, format, ones);
      if (end == '\n')
        fflush(stderr);
      else
        last = stderr;
    }
    va_end(ones);
  }
}

bool feed_ap(const char *format, va_list ap) {
  if (!last)
    return false;

  if (last == stderr) {
    va_list ones;
    va_copy(ones, ap);
    vfprintf(stdout, format, ones);
    va_end(ones);
  }
  vfprintf(last, format, ap);
  size_t len = strlen(format);
  if (len && format[len - 1] == '\n') {
    fflush(last);
    if (last == stderr)
      fflush(stdout);
    last = nullptr;
  }
  return true;
}

bool feed(const char *format, ...) {
  if (!last)
    return false;

  va_list ap;
  va_start(ap, format);
  feed_ap(format, ap);
  va_end(ap);
  return true;
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

void progress_canary(bool active) {
  static chrono::time progress_timestamp;
  chrono::time now = chrono::now_monotonic();

  if (now.fixedpoint - progress_timestamp.fixedpoint <
      chrono::from_ms(42).fixedpoint)
    return;

  if (osal_progress_push(active)) {
    progress_timestamp = now;
    return;
  }

  if (progress_timestamp.fixedpoint == 0) {
    putc('>', stderr);
    progress_timestamp = now;
  } else if (global::config::console_mode) {
    if (active) {
      static int last_point = -1;
      int point = (now.fixedpoint >> 29) & 3;
      if (point != last_point) {
        progress_timestamp = now;
        fprintf(stderr, "%c\b", "-\\|/"[last_point = point]);
      }
    } else if (now.fixedpoint - progress_timestamp.fixedpoint >
               chrono::from_seconds(2).fixedpoint) {
      progress_timestamp = now;
      fprintf(stderr, "%c\b", "@*"[now.utc & 1]);
    }
  } else {
    static int count;
    if (active && now.fixedpoint - progress_timestamp.fixedpoint >
                      chrono::from_seconds(1).fixedpoint) {
      putc('.', stderr);
      progress_timestamp = now;
      ++count;
    } else if (now.fixedpoint - progress_timestamp.fixedpoint >
               chrono::from_seconds(5).fixedpoint) {
      putc("@*"[now.utc & 1], stderr);
      progress_timestamp = now;
      ++count;
    }
    if (count == 60) {
      count = 0;
      putc('\n', stderr);
    }
  }
  fflush(stderr);
}

} // namespace logging

void log_extra(const char *msg, ...) {
  if (logging::same_or_higher(logging::extra, logging::level)) {
    va_list ap;
    va_start(ap, msg);
    logging::output_nocheckloglevel_ap(logging::extra, msg, ap);
    va_end(ap);
  } else
    logging::last = nullptr;
}

void log_trace(const char *msg, ...) {
  if (logging::same_or_higher(logging::trace, logging::level)) {
    va_list ap;
    va_start(ap, msg);
    logging::output_nocheckloglevel_ap(logging::trace, msg, ap);
    va_end(ap);
  } else
    logging::last = nullptr;
}

void log_debug(const char *msg, ...) {
  if (logging::same_or_higher(logging::debug, logging::level)) {
    va_list ap;
    va_start(ap, msg);
    logging::output_nocheckloglevel_ap(logging::debug, msg, ap);
    va_end(ap);
  } else
    logging::last = nullptr;
}

void log_verbose(const char *msg, ...) {
  if (logging::same_or_higher(logging::verbose, logging::level)) {
    va_list ap;
    va_start(ap, msg);
    logging::output_nocheckloglevel_ap(logging::verbose, msg, ap);
    va_end(ap);
  } else
    logging::last = nullptr;
}

void log_notice(const char *msg, ...) {
  if (logging::same_or_higher(logging::notice, logging::level)) {
    va_list ap;
    va_start(ap, msg);
    logging::output_nocheckloglevel_ap(logging::notice, msg, ap);
    va_end(ap);
  } else
    logging::last = nullptr;
}

void log_warning(const char *msg, ...) {
  if (logging::same_or_higher(logging::warning, logging::level)) {
    va_list ap;
    va_start(ap, msg);
    logging::output_nocheckloglevel_ap(logging::warning, msg, ap);
    va_end(ap);
  } else
    logging::last = nullptr;
}

void log_error(const char *msg, ...) {
  if (logging::same_or_higher(logging::error, logging::level)) {
    va_list ap;
    va_start(ap, msg);
    logging::output_nocheckloglevel_ap(logging::error, msg, ap);
    va_end(ap);
  } else
    logging::last = nullptr;
}

void log_trouble(const char *where, const char *what, int errnum) {
  log_error("%s: %s %s", where, what, test_strerror(errnum));
}

bool log_enabled(const logging::loglevel priority) {
  return logging::same_or_higher(priority, logging::level);
}

void log_flush(void) { fflushall(); }
