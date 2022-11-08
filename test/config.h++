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
#include "log.h++"
#include "utils.h++"

#define ACTOR_ID_MAX INT16_MAX

enum actor_testcase {
  ac_none,
  ac_hill,
  ac_deadread,
  ac_deadwrite,
  ac_jitter,
  ac_try,
  ac_copy,
  ac_append,
  ac_ttl,
  ac_nested
};

enum actor_status {
  as_unknown,
  as_debugging,
  as_running,
  as_successful,
  as_killed,
  as_failed,
  as_coredump,
};

const char *testcase2str(const actor_testcase);
const char *status2str(actor_status status);

enum keygen_case {
  kc_random, /* [ 6.. 2.. 7.. 4.. 0.. 1.. 5.. 3.. ] */
  kc_dashes, /* [ 0123.. 4567.. ] */
  kc_custom,
  /* TODO: more cases */
};

const char *keygencase2str(const keygen_case);

//-----------------------------------------------------------------------------

namespace config {

enum scale_mode { no_scale, decimal, binary, duration };

bool parse_option(int argc, char *const argv[], int &narg, const char *option,
                  const char **value, const char *default_value = nullptr);

bool parse_option(int argc, char *const argv[], int &narg, const char *option,
                  std::string &value, bool allow_empty = false);

bool parse_option(int argc, char *const argv[], int &narg, const char *option,
                  std::string &value, bool allow_empty,
                  const char *default_value);

bool parse_option(int argc, char *const argv[], int &narg, const char *option,
                  bool &value);

struct option_verb {
  const char *const verb;
  unsigned mask;
};

template <typename MASK>
bool parse_option(int argc, char *const argv[], int &narg, const char *option,
                  MASK &mask, const option_verb *verbs) {
  static_assert(sizeof(MASK) <= sizeof(unsigned), "WTF?");
  unsigned u = unsigned(mask);
  if (parse_option<unsigned>(argc, argv, narg, option, u, verbs)) {
    mask = MASK(u);
    return true;
  }
  return false;
}

template <>
bool parse_option<unsigned>(int argc, char *const argv[], int &narg,
                            const char *option, unsigned &mask,
                            const option_verb *verbs);

bool parse_option(int argc, char *const argv[], int &narg, const char *option,
                  uint64_t &value, const scale_mode scale,
                  const uint64_t minval = 0, const uint64_t maxval = INT64_MAX,
                  const uint64_t default_value = 0);

bool parse_option(int argc, char *const argv[], int &narg, const char *option,
                  unsigned &value, const scale_mode scale,
                  const unsigned minval = 0, const unsigned maxval = INT32_MAX,
                  const unsigned default_value = 0);

bool parse_option(int argc, char *const argv[], int &narg, const char *option,
                  uint8_t &value, const uint8_t minval = 0,
                  const uint8_t maxval = 255, const uint8_t default_value = 0);

bool parse_option(int argc, char *const argv[], int &narg, const char *option,
                  int64_t &value, const int64_t minval, const int64_t maxval,
                  const int64_t default_value = -1);

bool parse_option(int argc, char *const argv[], int &narg, const char *option,
                  int32_t &value, const int32_t minval, const int32_t maxval,
                  const int32_t default_value = -1);

inline bool parse_option_intptr(int argc, char *const argv[], int &narg,
                                const char *option, intptr_t &value,
                                const intptr_t minval, const intptr_t maxval,
                                const intptr_t default_value = -1) {
  static_assert(sizeof(intptr_t) == 4 || sizeof(intptr_t) == 8, "WTF?");
  if (sizeof(intptr_t) == 8)
    return parse_option(argc, argv, narg, option,
                        *reinterpret_cast<int64_t *>(&value), int64_t(minval),
                        int64_t(maxval), int64_t(default_value));
  else
    return parse_option(argc, argv, narg, option,
                        *reinterpret_cast<int32_t *>(&value), int32_t(minval),
                        int32_t(maxval), int32_t(default_value));
}

bool parse_option(int argc, char *const argv[], int &narg, const char *option,
                  logging::loglevel &);
//-----------------------------------------------------------------------------

struct keygen_params_pod {
  /* Параметры генератора пар key-value. Также может быть полезным описание
   * алгоритма генерации в keygen.h
   *
   * Ключи и значения генерируются по задаваемым параметрам на основе "плоской"
   * исходной координаты. При этом, в общем случае, в процессе тестов исходная
   * координата последовательно итерируется в заданном диапазоне, а необходимые
   * паттерны/последовательности/узоры получаются за счет преобразования
   * исходной координаты, согласно описанным ниже параметрам.
   *
   * Стоит отметить, что порядок описания параметров для удобства совпадает с
   * порядком их использования, т.е. с порядком соответствующих преобразований.
   *
   * Второе важное замечание касается ограничений одновременной координированной
   * генерации паттеров как для ключей, так и для значений. Суть в том, что
   * такая возможность не нужна по следующим причинам:
   *  - libmdbx поддерживает два существенно различающихся вида таблиц,
   *    "уникальные" (без дубликатов и без multi-value), и так называемые
   *    "с дубликатами" (c multi-value).
   *  - Для таблиц "без дубликатов" только размер связанных с ключами значений
   *    (данных) оказывает влияния на работу движка, непосредственно содержимое
   *    данных не анализируется движком и не оказывает влияния на его работу.
   *  - Для таблиц "с дубликатами", при наличии более одного значения для
   *    некоторого ключа, формируется дочернее btree-поддерево. Это дерево
   *    формируется во вложенной странице или отдельном "кусте" страниц,
   *    и обслуживается независимо от окружения родительского ключа.
   *  - Таким образом, паттерн генерации значений имеет смысл только для
   *    таблиц "с дубликатами" и только в контексте одного значения ключа.
   *    Иначе говоря, не имеет смысла взаимная координация при генерации
   *    значений для разных ключей. Поэтому генерацию значений следует
   *    рассматривать только в контексте связки с одним значением ключа.
   *  - Тем не менее, во всех случаях достаточно важным является равновероятное
   *    распределение всех возможных сочетаний длин ключей и данных.
   *
   * width:
   *  Большинство тестов предполагают создание или итерирование некоторого
   *  количества записей. При этом требуется итерирование или генерация
   *  значений и ключей из некоторого ограниченного пространства вариантов.
   *
   *  Параметр width задает такую ширину пространства вариантов в битах.
   *  Таким образом мощность пространства вариантов (пока) всегда равна
   *  степени двойки. Это ограничение можно снять, но ценой увеличения
   *  вычислительной сложности, включая потерю простоты и прозрачности.
   *
   *  С другой стороны, не-n-битовый width может быть полезен:
   *   - Позволит генерировать ключи/значения в точно задаваемом диапазоне.
   *     Например, перебрать в псевдо-случайном порядке 10001 значение.
   *   - Позволит поровну разделять заданное пространство (диапазон)
   *     ключей/значений между количеством потоков некратным степени двойки.
   *
   * mesh и seed:
   *  Позволяют получить псевдо-случайные последовательности ключей/значений.
   *  Параметр mesh задает сколько младших бит исходной плоской координаты
   *  будет "перемешано" (инъективно отображено), а параметр seed позволяет
   *  выбрать конкретный вариант "перемешивания".
   *
   *  Перемешивание выполняется при ненулевом значении mesh. Перемешивание
   *  реализуется посредством применения двух инъективных функций для
   *  заданного количества бит:
   *   - применяется первая инъективная функция;
   *   - к результату добавляется salt полученный из seed;
   *   - применяется вторая инъективная функция;
   *
   *  Следует отметить, что mesh умышленно позволяет перемешать только младшую
   *  часть, что при ненулевом значении split (см далее) не позволяет получать
   *  псевдо-случайные значений ключей без псевдо-случайности в значениях.
   *
   *  Такое ограничение соответствуют внутренней алгоритмике libmdbx. Проще
   *  говоря, мы можем проверить движок псевдо-случайной последовательностью
   *  ключей на таблицах без дубликатов (без multi-value), а затем проверить
   *  корректность работу псевдо-случайной последовательностью значений на
   *  таблицах с дубликатами (с multi-value), опционально добавляя
   *  псевдо-случайности к последовательности ключей. Однако, нет смысла
   *  генерировать псевдо-случайные ключи, одновременно с формированием
   *  какого-либо паттерна в значениях, так как содержимое в данных либо
   *  не будет иметь значения (для таблиц без дубликатов), либо будет
   *  обрабатываться в отдельных btree-поддеревьях.
   *
   * rotate и offset:
   *  Для проверки слияния и разделения страниц внутри движка требуются
   *  генерация ключей/значений в виде не-смежных последовательностей, как-бы
   *  в виде "пунктира", который постепенно заполняет весь заданный диапазон.
   *
   *  Параметры позволяют генерировать такой "пунктир". Соответственно rotate
   *  задает циклический сдвиг вправо, а offset задает смещение, точнее говоря
   *  сложение по модулю внутри диапазона заданного посредством width.
   *
   *  Например, при rotate равном 1 (циклический сдвиг вправо на 1 бит),
   *  четные и нечетные исходные значения сложатся в две линейные
   *  последовательности, которые постепенно закроют старшую и младшую
   *  половины диапазона.
   *
   * split:
   *  Для таблиц без дубликатов (без multi-value ключей) фактически требуется
   *  генерация только ключей, а данные могут быть постоянным. Но для таблиц с
   *  дубликатами (с multi-value ключами) также требуется генерация значений.
   *
   *  Ненулевое значение параметра split фактически включает генерацию значений,
   *  при этом значение split определяет сколько бит исходного абстрактного
   *  номера будет отрезано для генерации значения.
   */

  uint8_t width{0};
  uint8_t mesh{0};
  uint8_t rotate{0};
  uint8_t split{0};
  uint32_t seed{0};
  uint64_t offset{0};
  keygen_case keycase{kc_random};
  bool zero_fill{false};
};

struct actor_params_pod {
  MDBX_env_flags_t mode_flags{MDBX_ENV_DEFAULTS};
  MDBX_db_flags_t table_flags{MDBX_DB_DEFAULTS};
  intptr_t size_lower{0};
  intptr_t size_now{0};
  intptr_t size_upper{0};
  int shrink_threshold{0};
  int growth_step{0};
  int pagesize{0};

  unsigned test_duration{0};
  unsigned test_nops{0};
  unsigned nrepeat{0};
  unsigned nthreads{0};

  unsigned keylen_min{0}, keylen_max{0};
  unsigned datalen_min{0}, datalen_max{0};

  unsigned batch_read{0};
  unsigned batch_write{0};

  unsigned delaystart{0};
  unsigned waitfor_nops{0};
  unsigned inject_writefaultn{0};

  unsigned max_readers{0};
  unsigned max_tables{0};
  keygen_params_pod keygen;

  uint8_t loglevel{0};
  bool drop_table{false};
  bool ignore_dbfull{false};
  bool speculum{false};
  bool random_writemap{true};

  uint64_t serial_base() const {
    // FIXME: TODO
    return 0;
  }
  static MDBX_PURE_FUNCTION uint64_t serial_mask(unsigned bits) {
    assert(bits > 0 && bits <= 64);
    return (~(uint64_t)0u) >> (64 - bits);
  }
};

struct actor_config_pod {
  unsigned actor_id{0}, space_id{0};
  actor_testcase testcase{ac_none};
  unsigned wait4id{0};
  unsigned signal_nops{0};

  actor_config_pod() = default;
  actor_config_pod(unsigned actor_id, actor_testcase testcase,
                   unsigned space_id, unsigned wait4id)
      : actor_id(actor_id), space_id(space_id), testcase(testcase),
        wait4id(wait4id) {}
};

extern const struct option_verb mode_bits[];
extern const struct option_verb table_bits[];
void dump(const char *title = "config-dump: ");

} /* namespace config */

struct actor_params : public config::actor_params_pod {
  std::string pathname_log;
  std::string pathname_db;
  actor_params() = default;

  void set_defaults(const std::string &tmpdir);
  bool make_keygen_linear();
  unsigned mdbx_keylen_min() const;
  unsigned mdbx_keylen_max() const;
  unsigned mdbx_datalen_min() const;
  unsigned mdbx_datalen_max() const;
};

struct actor_config : public config::actor_config_pod {
  actor_params params;

  bool wanna_event4signalling() const { return true /* TODO ? */; }

  actor_config() = default;
  actor_config(actor_testcase testcase, const actor_params &params,
               unsigned space_id, unsigned wait4id);

  actor_config(const char *str) : actor_config() {
    if (!deserialize(str, *this))
      failure("Invalid internal parameter '%s'\n", str);
  }

  const std::string osal_serialize(simple_checksum &) const;
  bool osal_deserialize(const char *str, const char *end, simple_checksum &);

  const std::string serialize(const char *prefix) const;
  static bool deserialize(const char *str, actor_config &config);

  bool is_waitable(size_t nops) const {
    switch (testcase) {
    case ac_hill:
      if (!params.test_nops || params.test_nops >= nops)
        return true;
      __fallthrough;
    default:
      return false;
    }
  }
};
