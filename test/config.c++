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

#if defined(_MSC_VER) && !defined(strcasecmp)
#define strcasecmp(str, len) _stricmp(str, len)
#endif /* _MSC_VER && strcasecmp() */

namespace config {

bool parse_option(int argc, char *const argv[], int &narg, const char *option,
                  const char **value, const char *default_value) {
  assert(narg < argc);
  const char *current = argv[narg];
  const size_t optlen = strlen(option);

  if (strncmp(current, "--", 2) || strncmp(current + 2, option, optlen))
    return false;

  if (!value) {
    if (current[optlen + 2] == '=')
      failure("Option '--%s' doesn't accept any value\n", option);
    return true;
  }

  *value = nullptr;
  if (current[optlen + 2] == '=') {
    *value = &current[optlen + 3];
    return true;
  }

  if (narg + 1 < argc && strncmp("--", argv[narg + 1], 2) != 0) {
    *value = argv[narg + 1];
    if (strcmp(*value, "default") == 0) {
      if (!default_value)
        failure("Option '--%s' doesn't accept default value\n", option);
      *value = default_value;
    }
    ++narg;
    return true;
  }

  if (default_value) {
    *value = default_value;
    return true;
  }

  failure("No value given for '--%s' option\n", option);
}

bool parse_option(int argc, char *const argv[], int &narg, const char *option,
                  std::string &value, bool allow_empty) {
  return parse_option(argc, argv, narg, option, value, allow_empty,
                      allow_empty ? "" : nullptr);
}

bool parse_option(int argc, char *const argv[], int &narg, const char *option,
                  std::string &value, bool allow_empty,
                  const char *default_value) {
  const char *value_cstr;
  if (!parse_option(argc, argv, narg, option, &value_cstr, default_value))
    return false;

  if (!allow_empty && strlen(value_cstr) == 0)
    failure("Value for option '--%s' couldn't be empty\n", option);

  value = value_cstr;
  return true;
}

template <>
bool parse_option<unsigned>(int argc, char *const argv[], int &narg,
                            const char *option, unsigned &mask,
                            const option_verb *verbs) {
  const char *list;
  if (!parse_option(argc, argv, narg, option, &list))
    return false;

  unsigned clear = 0;
  while (*list) {
    if (*list == ',' || *list == ' ' || *list == '\t') {
      ++list;
      continue;
    }

    const char *const comma = strchr(list, ',');
    const bool strikethrough = *list == '-' || *list == '~';
    if (strikethrough || *list == '+')
      ++list;
    else
      mask = clear;
    const size_t len = (comma) ? comma - list : strlen(list);
    const option_verb *scan = verbs;

    while (true) {
      if (!scan->verb)
        failure("Unknown verb '%.*s', for option '--%s'\n", (int)len, list,
                option);
      if (strlen(scan->verb) == len && strncmp(list, scan->verb, len) == 0) {
        mask = strikethrough ? mask & ~scan->mask : mask | scan->mask;
        clear = strikethrough ? clear & ~scan->mask : clear | scan->mask;
        list += len;
        break;
      }
      ++scan;
    }
  }

  return true;
}

bool parse_option(int argc, char *const argv[], int &narg, const char *option,
                  uint64_t &value, const scale_mode scale,
                  const uint64_t minval, const uint64_t maxval,
                  const uint64_t default_value) {

  const char *value_cstr;
  if (!parse_option(argc, argv, narg, option, &value_cstr))
    return false;

  if (default_value && strcmp(value_cstr, "default") == 0) {
    value = default_value;
    return true;
  }

  if (strcmp(value_cstr, "min") == 0 || strcmp(value_cstr, "minimal") == 0) {
    value = minval;
    return true;
  }

  if (strcmp(value_cstr, "max") == 0 || strcmp(value_cstr, "maximal") == 0) {
    value = maxval;
    return true;
  }

  char *suffix = nullptr;
  errno = 0;
  unsigned long long raw = strtoull(value_cstr, &suffix, 0);
  if ((suffix && *suffix) || errno) {
    suffix = nullptr;
    errno = 0;
    raw = strtoull(value_cstr, &suffix, 10);
  }
  if (errno)
    failure("Option '--%s' expects a numeric value (%s)\n", option,
            test_strerror(errno));

  uint64_t multiplier = 1;
  if (suffix && *suffix) {
    if (scale == no_scale)
      failure("Option '--%s' doesn't accepts suffixes, so '%s' is unexpected\n",
              option, suffix);
    if (strcmp(suffix, "K") == 0 || strcasecmp(suffix, "Kilo") == 0)
      multiplier = (scale == decimal) ? UINT64_C(1000) : UINT64_C(1024);
    else if (strcmp(suffix, "M") == 0 || strcasecmp(suffix, "Mega") == 0)
      multiplier =
          (scale == decimal) ? UINT64_C(1000) * 1000 : UINT64_C(1024) * 1024;
    else if (strcmp(suffix, "G") == 0 || strcasecmp(suffix, "Giga") == 0)
      multiplier = (scale == decimal) ? UINT64_C(1000) * 1000 * 1000
                                      : UINT64_C(1024) * 1024 * 1024;
    else if (strcmp(suffix, "T") == 0 || strcasecmp(suffix, "Tera") == 0)
      multiplier = (scale == decimal) ? UINT64_C(1000) * 1000 * 1000 * 1000
                                      : UINT64_C(1024) * 1024 * 1024 * 1024;
    else if (scale == duration &&
             (strcmp(suffix, "s") == 0 || strcasecmp(suffix, "Seconds") == 0))
      multiplier = 1;
    else if (scale == duration &&
             (strcmp(suffix, "m") == 0 || strcasecmp(suffix, "Minutes") == 0))
      multiplier = 60;
    else if (scale == duration &&
             (strcmp(suffix, "h") == 0 || strcasecmp(suffix, "Hours") == 0))
      multiplier = 3600;
    else if (scale == duration &&
             (strcmp(suffix, "d") == 0 || strcasecmp(suffix, "Days") == 0))
      multiplier = 3600 * 24;
    else
      failure(
          "Option '--%s' expects a numeric value with Kilo/Mega/Giga/Tera %s"
          "suffixes, but '%s' is unexpected\n",
          option, (scale == duration) ? "or Seconds/Minutes/Hours/Days " : "",
          suffix);
  }

  if (raw >= UINT64_MAX / multiplier)
    failure("The value for option '--%s' is too huge\n", option);

  value = raw * multiplier;
  if (maxval && value > maxval)
    failure("The maximal value for option '--%s' is %" PRIu64 "\n", option,
            maxval);
  if (value < minval)
    failure("The minimal value for option '--%s' is %" PRIu64 "\n", option,
            minval);
  return true;
}

bool parse_option(int argc, char *const argv[], int &narg, const char *option,
                  unsigned &value, const scale_mode scale,
                  const unsigned minval, const unsigned maxval,
                  const unsigned default_value) {

  uint64_t huge;
  if (!parse_option(argc, argv, narg, option, huge, scale, minval, maxval,
                    default_value))
    return false;
  value = unsigned(huge);
  return true;
}

bool parse_option(int argc, char *const argv[], int &narg, const char *option,
                  uint8_t &value, const uint8_t minval, const uint8_t maxval,
                  const uint8_t default_value) {

  uint64_t huge;
  if (!parse_option(argc, argv, narg, option, huge, no_scale, minval, maxval,
                    default_value))
    return false;
  value = uint8_t(huge);
  return true;
}

bool parse_option(int argc, char *const argv[], int &narg, const char *option,
                  int64_t &value, const int64_t minval, const int64_t maxval,
                  const int64_t default_value) {
  uint64_t proxy = uint64_t(value);
  if (parse_option(argc, argv, narg, option, proxy, config::binary,
                   uint64_t(minval), uint64_t(maxval),
                   uint64_t(default_value))) {
    value = int64_t(proxy);
    return true;
  }
  return false;
}

bool parse_option(int argc, char *const argv[], int &narg, const char *option,
                  int32_t &value, const int32_t minval, const int32_t maxval,
                  const int32_t default_value) {
  uint64_t proxy = uint64_t(value);
  if (parse_option(argc, argv, narg, option, proxy, config::binary,
                   uint64_t(minval), uint64_t(maxval),
                   uint64_t(default_value))) {
    value = int32_t(proxy);
    return true;
  }
  return false;
}

bool parse_option(int argc, char *const argv[], int &narg, const char *option,
                  logging::loglevel &loglevel) {
  const char *value_cstr;
  if (!parse_option(argc, argv, narg, option, &value_cstr))
    return false;

  if (strcmp(value_cstr, "min") == 0 || strcmp(value_cstr, "minimal") == 0 ||
      strcmp(value_cstr, "fatal") == 0) {
    loglevel = logging::failure;
    return true;
  }

  if (strcmp(value_cstr, "error") == 0 || strcmp(value_cstr, "err") == 0) {
    loglevel = logging::error;
    return true;
  }

  if (strcmp(value_cstr, "warning") == 0 || strcmp(value_cstr, "warn") == 0) {
    loglevel = logging::error;
    return true;
  }

  if (strcmp(value_cstr, "default") == 0 || strcmp(value_cstr, "notice") == 0) {
    loglevel = logging::notice;
    return true;
  }

  if (strcmp(value_cstr, "verbose") == 0) {
    loglevel = logging::verbose;
    return true;
  }

  if (strcmp(value_cstr, "debug") == 0) {
    loglevel = logging::debug;
    return true;
  }

  if (strcmp(value_cstr, "trace") == 0) {
    loglevel = logging::trace;
    return true;
  }

  if (strcmp(value_cstr, "max") == 0 || strcmp(value_cstr, "maximal") == 0 ||
      strcmp(value_cstr, "extra") == 0) {
    loglevel = logging::extra;
    return true;
  }

  char *suffix = nullptr;
  unsigned long long raw = strtoull(value_cstr, &suffix, 0);
  if ((suffix && *suffix) || errno) {
    suffix = nullptr;
    errno = 0;
    raw = strtoull(value_cstr, &suffix, 10);
  }
  if ((!suffix || !*suffix) && !errno && raw < 8) {
    loglevel = static_cast<logging::loglevel>(raw);
    return true;
  }

  failure("Unknown log-level '%s', for option '--%s'\n", value_cstr, option);
}

bool parse_option(int argc, char *const argv[], int &narg, const char *option,
                  bool &value) {
  const char *value_cstr = nullptr;
  if (!parse_option(argc, argv, narg, option, &value_cstr, "yes")) {
    const char *current = argv[narg];
    if (strncmp(current, "--no-", 5) == 0 && strcmp(current + 5, option) == 0) {
      value = false;
      return true;
    }
    if (strncmp(current, "--dont-", 7) == 0 &&
        strcmp(current + 7, option) == 0) {
      value = false;
      return true;
    }
    return false;
  }

  if (!value_cstr) {
    value = true;
    return true;
  }

  if (strcasecmp(value_cstr, "yes") == 0 || strcasecmp(value_cstr, "1") == 0) {
    value = true;
    return true;
  }

  if (strcasecmp(value_cstr, "no") == 0 || strcasecmp(value_cstr, "0") == 0) {
    value = false;
    return true;
  }

  failure(
      "Option '--%s' expects a 'boolean' value Yes/No, so '%s' is unexpected\n",
      option, value_cstr);
}

//-----------------------------------------------------------------------------

const struct option_verb mode_bits[] = {
    {"rdonly", unsigned(MDBX_RDONLY)},
    {"nosync-utterly", unsigned(MDBX_UTTERLY_NOSYNC)},
    {"nosubdir", unsigned(MDBX_NOSUBDIR)},
    {"nosync-safe", unsigned(MDBX_SAFE_NOSYNC)},
    {"nometasync", unsigned(MDBX_NOMETASYNC)},
    {"writemap", unsigned(MDBX_WRITEMAP)},
    {"notls", unsigned(MDBX_NOTLS)},
    {"nordahead", unsigned(MDBX_NORDAHEAD)},
    {"nomeminit", unsigned(MDBX_NOMEMINIT)},
    {"lifo", unsigned(MDBX_LIFORECLAIM)},
    {"perturb", unsigned(MDBX_PAGEPERTURB)},
    {"accede", unsigned(MDBX_ACCEDE)},
    {"exclusive", unsigned(MDBX_EXCLUSIVE)},
    {nullptr, 0}};

const struct option_verb table_bits[] = {
    {"key.reverse", unsigned(MDBX_REVERSEKEY)},
    {"key.integer", unsigned(MDBX_INTEGERKEY)},
    {"data.integer", unsigned(MDBX_INTEGERDUP | MDBX_DUPFIXED | MDBX_DUPSORT)},
    {"data.fixed", unsigned(MDBX_DUPFIXED | MDBX_DUPSORT)},
    {"data.reverse", unsigned(MDBX_REVERSEDUP | MDBX_DUPSORT)},
    {"data.dups", unsigned(MDBX_DUPSORT)},
    {nullptr, 0}};

static void dump_verbs(const char *caption, size_t bits,
                       const struct option_verb *verbs) {
  log_verbose("%s: 0x%" PRIx64 " = ", caption, (uint64_t)bits);

  const char *comma = "";
  while (verbs->mask && bits) {
    if ((bits & verbs->mask) == verbs->mask) {
      logging::feed("%s%s", comma, verbs->verb);
      bits -= verbs->mask;
      comma = ", ";
    }
    ++verbs;
  }

  logging::feed("%s\n", (*comma == '\0') ? "none" : "");
}

static void dump_duration(const char *caption, unsigned duration) {
  log_verbose("%s: ", caption);
  if (duration) {
    if (duration > 24 * 3600)
      logging::feed("%u_", duration / (24 * 3600));
    if (duration > 3600)
      logging::feed("%02u:", (duration % (24 * 3600)) / 3600);
    logging::feed("%02u:%02u", (duration % 3600) / 60, duration % 60);
  } else {
    logging::feed("INFINITE");
  }
  logging::feed("\n");
}

void dump(const char *title) {
  logging::local_suffix indent(title);

  for (auto i = global::actors.begin(); i != global::actors.end(); ++i) {
    log_verbose("#%u, testcase %s, space_id/table %u\n", i->actor_id,
                testcase2str(i->testcase), i->space_id);
    indent.push();

    if (i->params.loglevel) {
      log_verbose("log: level %u, %s\n", i->params.loglevel,
                  i->params.pathname_log.empty()
                      ? "console"
                      : i->params.pathname_log.c_str());
    }

    log_verbose("database: %s, size %" PRIuPTR "[%" PRIiPTR "..%" PRIiPTR
                ", %i %i, %i]\n",
                i->params.pathname_db.c_str(), i->params.size_now,
                i->params.size_lower, i->params.size_upper,
                i->params.shrink_threshold, i->params.growth_step,
                i->params.pagesize);

    dump_verbs("mode", i->params.mode_flags, mode_bits);
    log_verbose("random-writemap: %s\n",
                i->params.random_writemap ? "Yes" : "No");
    dump_verbs("table", i->params.table_flags, table_bits);

    if (i->params.test_nops)
      log_verbose("iterations/records %u\n", i->params.test_nops);
    else
      dump_duration("duration", i->params.test_duration);

    if (i->params.nrepeat)
      log_verbose("repeat %u\n", i->params.nrepeat);
    else
      log_verbose("repeat ETERNALLY\n");

    log_verbose("threads %u\n", i->params.nthreads);

    log_verbose(
        "keygen.params: case %s, width %u, mesh %u, rotate %u, offset %" PRIu64
        ", split %u/%u\n",
        keygencase2str(i->params.keygen.keycase), i->params.keygen.width,
        i->params.keygen.mesh, i->params.keygen.rotate, i->params.keygen.offset,
        i->params.keygen.split,
        i->params.keygen.width - i->params.keygen.split);
    log_verbose("keygen.seed: %u\n", i->params.keygen.seed);
    log_verbose("keygen.zerofill: %s\n",
                i->params.keygen.zero_fill ? "Yes" : "No");
    log_verbose("key: minlen %u, maxlen %u\n", i->params.keylen_min,
                i->params.keylen_max);
    log_verbose("data: minlen %u, maxlen %u\n", i->params.datalen_min,
                i->params.datalen_max);

    log_verbose("batch: read %u, write %u\n", i->params.batch_read,
                i->params.batch_write);

    if (i->params.waitfor_nops)
      log_verbose("wait: actor %u for %u ops\n", i->wait4id,
                  i->params.waitfor_nops);
    else if (i->params.delaystart)
      dump_duration("delay", i->params.delaystart);
    else
      log_verbose("no-delay\n");

    if (i->params.inject_writefaultn)
      log_verbose("inject-writefault on %u ops\n",
                  i->params.inject_writefaultn);
    else
      log_verbose("no-inject-writefault\n");

    log_verbose("limits: readers %u, tables %u, txn-bytes %zu\n",
                i->params.max_readers, i->params.max_tables,
                mdbx_limits_txnsize_max(i->params.pagesize));

    log_verbose("drop table: %s\n", i->params.drop_table ? "Yes" : "No");
    log_verbose("ignore MDBX_MAP_FULL error: %s\n",
                i->params.ignore_dbfull ? "Yes" : "No");
    log_verbose("verifying by speculum: %s\n",
                i->params.speculum ? "Yes" : "No");

    indent.pop();
  }

  dump_duration("timeout", global::config::timeout_duration_seconds);
  log_verbose("cleanup: before %s, after %s\n",
              global::config::cleanup_before ? "Yes" : "No",
              global::config::cleanup_after ? "Yes" : "No");

  log_verbose("failfast: %s\n", global::config::failfast ? "Yes" : "No");
  log_verbose("progress indicator: %s\n",
              global::config::progress_indicator ? "Yes" : "No");
  log_verbose("console mode: %s\n",
              global::config::console_mode ? "Yes" : "No");
  log_verbose("geometry jitter: %s\n",
              global::config::geometry_jitter ? "Yes" : "No");
}

} /* namespace config */

//-----------------------------------------------------------------------------

using namespace config;

actor_config::actor_config(actor_testcase testcase, const actor_params &params,
                           unsigned space_id, unsigned wait4id)
    : actor_config_pod(1 + unsigned(global::actors.size()), testcase, space_id,
                       wait4id),
      params(params) {}

const std::string actor_config::serialize(const char *prefix) const {
  simple_checksum checksum;
  std::string result;

  if (prefix)
    result.append(prefix);

  checksum.push(params.pathname_db);
  result.append(params.pathname_db);
  result.push_back('|');

  checksum.push(params.pathname_log);
  result.append(params.pathname_log);
  result.push_back('|');

#if __cplusplus > 201400
  static_assert(std::is_trivially_copyable<actor_params_pod>::value,
                "actor_params_pod should by POD");
#else
  static_assert(std::is_standard_layout<actor_params_pod>::value,
                "actor_params_pod should by POD");
#endif
  result.append(data2hex(static_cast<const actor_params_pod *>(&params),
                         sizeof(actor_params_pod), checksum));
  result.push_back('|');

#if __cplusplus > 201400
  static_assert(std::is_trivially_copyable<actor_config_pod>::value,
                "actor_config_pod should by POD");
#else
  static_assert(std::is_standard_layout<actor_config_pod>::value,
                "actor_config_pod should by POD");
#endif
  result.append(data2hex(static_cast<const actor_config_pod *>(this),
                         sizeof(actor_config_pod), checksum));
  result.push_back('|');
  result.push_back(global::config::progress_indicator ? 'Y' : 'N');
  checksum.push(global::config::progress_indicator);
  result.push_back(global::config::console_mode ? 'Y' : 'N');
  checksum.push(global::config::console_mode);
  result.push_back(global::config::geometry_jitter ? 'Y' : 'N');
  checksum.push(global::config::geometry_jitter);
  result.push_back('|');

  result.append(osal_serialize(checksum));
  result.push_back('|');

  result.append(std::to_string(checksum.value));
  return result;
}

bool actor_config::deserialize(const char *str, actor_config &config) {
  simple_checksum checksum;

  TRACE(">> actor_config::deserialize: %s\n", str);

  const char *slash = strchr(str, '|');
  if (!slash) {
    TRACE("<< actor_config::deserialize: slash-1\n");
    return false;
  }
  config.params.pathname_db.assign(str, slash - str);
  checksum.push(config.params.pathname_db);
  str = slash + 1;

  slash = strchr(str, '|');
  if (!slash) {
    TRACE("<< actor_config::deserialize: slash-2\n");
    return false;
  }
  config.params.pathname_log.assign(str, slash - str);
  checksum.push(config.params.pathname_log);
  str = slash + 1;

  slash = strchr(str, '|');
  if (!slash) {
    TRACE("<< actor_config::deserialize: slash-3\n");
    return false;
  }
#if __cplusplus > 201400
  static_assert(std::is_trivially_copyable<actor_params_pod>::value,
                "actor_params_pod should by POD");
#else
  static_assert(std::is_standard_layout<actor_params_pod>::value,
                "actor_params_pod should by POD");
#endif
  if (!hex2data(str, slash, static_cast<actor_params_pod *>(&config.params),
                sizeof(actor_params_pod), checksum)) {
    TRACE("<< actor_config::deserialize: actor_params_pod(%.*s)\n",
          (int)(slash - str), str);
    return false;
  }
  str = slash + 1;

  slash = strchr(str, '|');
  if (!slash) {
    TRACE("<< actor_config::deserialize: slash-4\n");
    return false;
  }
#if __cplusplus > 201400
  static_assert(std::is_trivially_copyable<actor_config_pod>::value,
                "actor_config_pod should by POD");
#else
  static_assert(std::is_standard_layout<actor_config_pod>::value,
                "actor_config_pod should by POD");
#endif
  if (!hex2data(str, slash, static_cast<actor_config_pod *>(&config),
                sizeof(actor_config_pod), checksum)) {
    TRACE("<< actor_config::deserialize: actor_config_pod(%.*s)\n",
          (int)(slash - str), str);
    return false;
  }
  str = slash + 1;

  slash = strchr(str, '|');
  if (!slash) {
    TRACE("<< actor_config::deserialize: slash-5\n");
    return false;
  }
  if ((str[0] == 'Y' || str[0] == 'N') && (str[1] == 'Y' || str[1] == 'N') &&
      (str[2] == 'Y' || str[2] == 'N')) {
    global::config::progress_indicator = str[0] == 'Y';
    checksum.push(global::config::progress_indicator);
    global::config::console_mode = str[1] == 'Y';
    checksum.push(global::config::console_mode);
    global::config::geometry_jitter = str[2] != 'N';
    checksum.push(global::config::geometry_jitter);
    str = slash + 1;

    slash = strchr(str, '|');
    if (!slash) {
      TRACE("<< actor_config::deserialize: slash-6\n");
      return false;
    }
  }

  if (!config.osal_deserialize(str, slash, checksum)) {
    TRACE("<< actor_config::deserialize: osal\n");
    return false;
  }
  str = slash + 1;

  uint64_t verify = std::stoull(std::string(str));
  if (checksum.value != verify) {
    TRACE("<< actor_config::deserialize: checksum mismatch\n");
    return false;
  }

  TRACE("<< actor_config::deserialize: OK\n");
  return true;
}

unsigned actor_params::mdbx_keylen_min() const {
  return (table_flags & MDBX_INTEGERKEY) ? 4 : 0;
}

unsigned actor_params::mdbx_keylen_max() const {
  return unsigned(mdbx_limits_keysize_max(pagesize, table_flags));
}

unsigned actor_params::mdbx_datalen_min() const {
  return (table_flags & MDBX_INTEGERDUP) ? 4 : 0;
}

unsigned actor_params::mdbx_datalen_max() const {
  return std::min(unsigned(UINT16_MAX),
                  unsigned(mdbx_limits_valsize_max(pagesize, table_flags)));
}

bool actor_params::make_keygen_linear() {
  const auto base = serial_base();
  keygen.mesh = (table_flags & MDBX_DUPSORT) ? 0 : keygen.split;
  keygen.rotate = 0;
  keygen.offset = 0;
  const auto max_serial = serial_mask(keygen.width) + base;
  const auto max_key_serial = (keygen.split && (table_flags & MDBX_DUPSORT))
                                  ? max_serial >> keygen.split
                                  : max_serial;
  const auto max_value_serial = (keygen.split && (table_flags & MDBX_DUPSORT))
                                    ? serial_mask(keygen.split)
                                    : 0;

  while (keylen_min < 8 &&
         (keylen_min == 0 || serial_mask(keylen_min * 8) < max_key_serial)) {
    keylen_min += (table_flags & (MDBX_INTEGERKEY | MDBX_INTEGERDUP)) ? 4 : 1;
    if (keylen_max < keylen_min)
      keylen_max = keylen_min;
  }

  if (table_flags & MDBX_DUPSORT)
    while (
        datalen_min < 8 &&
        (datalen_min == 0 || serial_mask(datalen_min * 8) < max_value_serial)) {
      datalen_min +=
          (table_flags & (MDBX_INTEGERKEY | MDBX_INTEGERDUP)) ? 4 : 1;
      if (datalen_max < datalen_min)
        datalen_max = datalen_min;
    }

  return true;
}
