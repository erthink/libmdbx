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

namespace keygen {

/* Под "генерацией ключей" здесь понимается генерация обоих значений для
 * пар key-value, т.е. не только ключей, но и ассоциированных с ними данных.
 */

/* Генерацию ключей нельзя отнести к простым задачам, так как требования
 * примерно следующие:
 *  - генерация разного количества уникальных ключей различной длины
 *    в задаваемом диапазоне;
 *  - возможность выбора как псевдо-случайного порядка ключей,
 *    так и по некоторым специфическим законам (ограниченными упорядоченными
 *    последовательностями, в шахматном порядке по граница диапазона и т.д.);
 *  - возможность генерации дубликатов с задаваемым законом распределения;
 *  - возможность генерации непересекающимися кластерами для параллельного
 *    использования в нескольких потоках;
 *  - использовать минимум ресурсов, как CPU, так и RAM, в том числе
 *    включая cache pollution и ram bandwidth.
 *
 * При этом заведомо известно, что для MDBX не имеет значения:
 *  - используемый алфавит (значения байтов);
 *  - частотное распределение по алфавиту;
 *  - абсолютное значение ключей или разность между отдельными значениями;
 *
 * Соответственно, схема генерации следующая:
 *  - для ключей вводится плоская одномерная "координата" uint64_t;
 *  - все преобразования (назначение диапазонов, переупорядочивание,
 *    коррекция распределения) выполняются только над "координатой";
 *  - итоговая "координата" преобразуется в 8-байтное суррогатное значение
 *    ключа, при этом опционально суррогат может усекаться до ненулевых байт;
 *  - для получения ключей длиной более 8 байт суррогат дополняется
 *    фиксированной последовательностью;
 */

typedef uint64_t serial_t;

struct params_t {
  uint8_t minlen;
  uint8_t flags;
  uint16_t maxlen;
};

struct result_t {
  MDB_val value;
  size_t limit;
  union {
    uint8_t bytes[sizeof(uint64_t)];
    uint32_t u32;
    uint64_t u64;
  };
};

void make(const serial_t serial, const params_t &params, result_t &out);

static __inline void make(const serial_t serial, const params_t &params,
                          result_t &out, size_t limit) {
  out.limit = limit;
  make(serial, params, out);
}

size_t ffs_fallback(serial_t serial);

static __inline size_t ffs(serial_t serial) {
  size_t rc;
#ifdef __GNUC__
  if (sizeof(serial) <= sizeof(int))
    rc = __builtin_ffs((int)serial);
  else if (sizeof(serial) == sizeof(long))
    rc = __builtin_ffsl((long)serial);
  else if (sizeof(serial) == sizeof(long long))
    rc = __builtin_ffsll((long long)serial);
  else
    return ffs_fallback(serial);
#elif defined(_MSC_VER)
  unsigned long index;
  if (sizeof(serial) <= sizeof(unsigned long))
    rc = _BitScanReverse(&index, (unsigned long)serial) ? index : 0;
  else if (sizeof(serial) <= sizeof(unsigned __int64)) {
#if defined(_M_ARM64) || defined(_M_X64)
    rc = _BitScanReverse64(&index, (unsigned __int64)serial) ? index : 0;
#else
    size_t base = 0;
    unsigned long value = (unsigned long)serial;
    if ((unsigned __int64)serial > ULONG_MAX) {
      base = 32;
      value = (unsigned long)(serial >> 32);
    }
    rc = (_BitScanReverse(&index, value) ? index : 0) + base;
#endif /* _M_ARM64 || _M_X64 */
  } else
    return ffs_fallback(serial);
#else
  return ffs_fallback(serial);
#endif
  assert(rc == ffs_fallback(serial));
  return rc;
}

static __inline size_t length(const serial_t serial) {
  return (ffs(serial) + 7) >> 3;
}

} /* namespace keygen */
