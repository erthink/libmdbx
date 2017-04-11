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

#define ACTOR_ID_MAX INT16_MAX

enum actor_testcase { ac_none, ac_hill, ac_deadread, ac_deadwrite, ac_jitter };

enum actor_status {
  as_unknown,
  as_debuging,
  as_running,
  as_successful,
  as_killed,
  as_failed
};

const char *testcase2str(const actor_testcase);
const char *status2str(actor_status status);

//-----------------------------------------------------------------------------

namespace config {

enum scale_mode { no_scale, decimal, binary, duration };

bool parse_option(int argc, char *const argv[], int &narg, const char *option,
                  const char **value, const char *default_value = nullptr);

bool parse_option(int argc, char *const argv[], int &narg, const char *option,
                  std::string &value, bool allow_empty = false);

bool parse_option(int argc, char *const argv[], int &narg, const char *option,
                  bool &value);

struct option_verb {
  const char *const verb;
  unsigned mask;
};

bool parse_option(int argc, char *const argv[], int &narg, const char *option,
                  size_t &mask, const option_verb *verbs);

bool parse_option(int argc, char *const argv[], int &narg, const char *option,
                  uint64_t &value, const scale_mode scale,
                  const uint64_t minval = 0, const uint64_t maxval = INT64_MAX);

bool parse_option(int argc, char *const argv[], int &narg, const char *option,
                  unsigned &value, const scale_mode scale,
                  const unsigned minval = 0, const unsigned maxval = INT32_MAX);

//-----------------------------------------------------------------------------

#pragma pack(push, 1)

struct actor_params_pod {
  unsigned loglevel;

  size_t mode_flags;
  size_t table_flags;
  uint64_t size;
  unsigned seed;

  unsigned test_duration;
  unsigned test_nrecords;
  unsigned nrepeat;
  unsigned nthreads;

  unsigned keylen_min, keylen_max;
  unsigned datalen_min, datalen_max;

  unsigned batch_read;
  unsigned batch_write;

  unsigned delaystart;
  unsigned waitfor_nops;

  bool drop_table;

  unsigned max_readers;
  unsigned max_tables;
};

struct actor_config_pod {
  unsigned id, order;
  actor_testcase testcase;
  unsigned wait4id;
  unsigned signal_nops;
};

#pragma pack(pop)

extern const struct option_verb mode_bits[];
extern const struct option_verb table_bits[];
void dump(const char *title = "config-dump: ");

} /* namespace config */

struct actor_params : public config::actor_params_pod {
  std::string pathname_log;
  std::string pathname_db;
  void set_defaults(void);
};

struct actor_config : public config::actor_config_pod {
  actor_params params;

  bool wanna_event4signalling() const { return true /* TODO ? */; }

  actor_config(actor_testcase testcase, const actor_params &params, unsigned id,
               unsigned wait4id);

  actor_config(const char *str) {
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
      if (!params.test_nrecords || params.test_nrecords >= nops)
        return true;
    default:
      return false;
    }
  }
};
