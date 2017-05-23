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
#include "config.h"
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
 * Соответственно, в общих чертах, схема генерации следующая:
 *  - вводится плоская одномерная "координата" uint64_t;
 *  - генерация специфических паттернов (последовательностей)
 *    реализуется посредством соответствующих преобразований "координат", при
 *    этом все подобные преобразования выполняются только над "координатой";
 *  - итоговая "координата" преобразуется в 8-байтное суррогатное значение
 *    ключа;
 *  - для получения ключей длиной МЕНЕЕ 8 байт суррогат может усекаться
 *    до ненулевых байт, в том числе до нулевой длины;
 *  - для получения ключей длиной БОЛЕЕ 8 байт суррогат дополняется
 *    нулями или псевдослучайной последовательностью;
 *
 * Механизм генерации паттернов:
 *  - реализованный механизм является компромиссом между скоростью/простотой
 *    и гибкостью, необходимой для получения последовательностей, которых
 *    будет достаточно для проверки сценариев разделения и слияния страниц
 *    с данными внутри mdbx;
 *  - псевдо-случайные паттерны реализуются посредством набора инъективных
 *    отображающих функций;
 *  - не-псевдо-случайные паттерны реализуются посредством параметризируемого
 *    трех-этапного преобразования:
 *      1) смещение (сложение) по модулю;
 *      2) циклический сдвиг;
 *      3) добавление абсолютного смещения (базы);
 */

typedef uint64_t serial_t;

enum {
  serial_minwith = 8,
  serial_maxwith = sizeof(serial_t) * 8,
  serial_allones = ~(serial_t)0
};

struct result {
  MDBX_val value;
  size_t limit;
  union {
    uint8_t bytes[sizeof(uint64_t)];
    uint32_t u32;
    uint64_t u64;
  };
};

//-----------------------------------------------------------------------------

struct buffer_deleter : public std::unary_function<void, result *> {
  void operator()(result *buffer) const { free(buffer); }
};

typedef std::unique_ptr<result, buffer_deleter> buffer;

buffer alloc(size_t limit);

class maker {
  config::keygen_params_pod mapping;
  serial_t base;
  serial_t salt;

  struct essentials {
    uint8_t minlen;
    uint8_t flags;
    uint16_t maxlen;
  } key_essentials, value_essentials;

  static void mk(const serial_t serial, const essentials &params, result &out);

public:
  maker() { memset(this, 0, sizeof(*this)); }

  void pair(serial_t serial, const buffer &key, buffer &value,
            serial_t value_age);
  void setup(const config::actor_params_pod &actor, unsigned thread_number);

  bool increment(serial_t &serial, int delta);
};

size_t length(serial_t serial);

} /* namespace keygen */
