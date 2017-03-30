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

std::string format(const char *fmt, ...) {
  va_list ap, ones;
  va_start(ap, fmt);
  va_copy(ones, ap);
#ifdef _MSC_VER
  int needed = _vscprintf(fmt, ap);
#else
  int needed = vsnprintf(nullptr, 0, fmt, ap);
#endif
  assert(needed >= 0);
  va_end(ap);
  std::string result;
  result.reserve((size_t)needed + 1);
  result.resize((size_t)needed, '\0');
  int actual = vsnprintf((char *)result.data(), result.capacity(), fmt, ones);
  assert(actual == needed);
  (void)actual;
  va_end(ones);
  return result;
}

std::string data2hex(const void *ptr, size_t bytes, simple_checksum &checksum) {
  std::string result;
  if (bytes > 0) {
    const uint8_t *data = (const uint8_t *)ptr;
    checksum.push(data, bytes);
    result.reserve(bytes * 2);
    const uint8_t *const end = data + bytes;
    do {
      char h = *data >> 4;
      char l = *data & 15;
      result.push_back((l < 10) ? l + '0' : l - 10 + 'a');
      result.push_back((h < 10) ? h + '0' : h - 10 + 'a');
    } while (++data < end);
  }
  assert(result.size() == bytes * 2);
  return result;
}

bool hex2data(const char *hex_begin, const char *hex_end, void *ptr,
              size_t bytes, simple_checksum &checksum) {
  if (bytes * 2 != (size_t)(hex_end - hex_begin))
    return false;

  uint8_t *data = (uint8_t *)ptr;
  for (const char *hex = hex_begin; hex != hex_end; hex += 2, ++data) {
    unsigned l = hex[0], h = hex[1];

    if (l >= '0' && l <= '9')
      l = l - '0';
    else if (l >= 'A' && l <= 'F')
      l = l - 'A' + 10;
    else if (l >= 'a' && l <= 'f')
      l = l - 'a' + 10;
    else
      return false;

    if (h >= '0' && h <= '9')
      h = h - '0';
    else if (h >= 'A' && h <= 'F')
      h = h - 'A' + 10;
    else if (h >= 'a' && h <= 'f')
      h = h - 'a' + 10;
    else
      return false;

    uint32_t c = l + (h << 4);
    checksum.push(c);
    *data = c;
  }
  return true;
}

//-----------------------------------------------------------------------------
