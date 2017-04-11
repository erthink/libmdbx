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
#include "log.h"
#include "utils.h"

namespace chrono {

typedef union time {
  uint64_t fixedpoint;
  struct __packed {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    uint32_t fractional;
    uint32_t utc;
#else
    uint32_t utc;
    uint32_t fractional;
#endif
  };
} time;

uint32_t ns2fractional(uint32_t);
uint32_t fractional2ns(uint32_t);
uint32_t us2fractional(uint32_t);
uint32_t fractional2us(uint32_t);
uint32_t ms2fractional(uint32_t);
uint32_t fractional2ms(uint32_t);

time from_ns(uint64_t us);
time from_us(uint64_t ns);
time from_ms(uint64_t ms);

inline time from_utc(time_t utc) {
  assert(utc < UINT32_MAX);
  time result;
  result.fixedpoint = ((uint64_t)utc) << 32;
  return result;
}

#if defined(HAVE_TIMESPEC_TV_NSEC) || defined(__timespec_defined) ||           \
    defined(CLOCK_REALTIME)
inline time from_timespec(const struct timespec &ts) {
  time result;
  result.fixedpoint =
      ((uint64_t)ts.tv_sec << 32) | ns2fractional((uint32_t)ts.tv_nsec);
  return result;
}
#endif /* HAVE_TIMESPEC_TV_NSEC */

#if defined(HAVE_TIMEVAL_TV_USEC) || defined(_STRUCT_TIMEVAL)
inline time from_timeval(const struct timeval &tv) {
  time result;
  result.fixedpoint =
      ((uint64_t)tv.tv_sec << 32) | us2fractional((uint32_t)tv.tv_usec);
  return result;
}
#endif /* HAVE_TIMEVAL_TV_USEC */

time now();

} /* namespace chrono */
