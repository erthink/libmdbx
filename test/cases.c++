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

registry *registry::instance() {
  static registry *singleton;
  if (!singleton)
    singleton = new registry();
  return singleton;
}

bool registry::add(const record *item) {
  auto const singleton = instance();
  assert(singleton->name2id.count(std::string(item->name)) == 0);
  assert(singleton->id2record.count(item->id) == 0);
  if (singleton->name2id.count(std::string(item->name)) +
          singleton->id2record.count(item->id) ==
      0) {
    singleton->name2id[std::string(item->name)] = item;
    singleton->id2record[item->id] = item;
    return true;
  }
  return false;
}

testcase *registry::create_actor(const actor_config &config,
                                 const mdbx_pid_t pid) {
  return instance()->id2record.at(config.testcase)->constructor(config, pid);
}

bool registry::review_actor_params(const actor_testcase id,
                                   actor_params &params) {
  return instance()->id2record.at(id)->review_params(params);
}

//-----------------------------------------------------------------------------

void configure_actor(unsigned &last_space_id, const actor_testcase testcase,
                     const char *space_id_cstr, actor_params params) {
  unsigned wait4id = 0;
  if (params.waitfor_nops) {
    for (auto i = global::actors.rbegin(); i != global::actors.rend(); ++i) {
      if (i->is_waitable(params.waitfor_nops)) {
        if (i->signal_nops && i->signal_nops != params.waitfor_nops)
          failure("Previous waitable actor (id=%u) already linked on %u-ops\n",
                  i->actor_id, i->signal_nops);
        wait4id = i->actor_id;
        i->signal_nops = params.waitfor_nops;
        break;
      }
    }
    if (!wait4id)
      failure("No previous waitable actor for %u-ops\n", params.waitfor_nops);
  }

  unsigned long space_id = 0;
  if (!space_id_cstr || strcmp(space_id_cstr, "auto") == 0)
    space_id = last_space_id + 1;
  else {
    char *end = nullptr;
    errno = 0;
    space_id = strtoul(space_id_cstr, &end, 0);
    if (errno)
      failure_perror("Expects an integer value for space-id\n", errno);
    if (end && *end)
      failure("The '%s' is unexpected for space-id\n", end);
  }

  if (!registry::review_actor_params(testcase, params))
    failure("Actor config-review failed for space-id %lu\n", space_id);

  if (space_id > ACTOR_ID_MAX)
    failure("Invalid space-id %lu\n", space_id);
  last_space_id = unsigned(space_id);

  log_trace("configure_actor: space %lu for %s", space_id,
            testcase2str(testcase));
  global::actors.emplace_back(
      actor_config(testcase, params, unsigned(space_id), wait4id));
  global::databases.insert(params.pathname_db);
}

void testcase_setup(const char *casename, const actor_params &params,
                    unsigned &last_space_id) {
  if (strcmp(casename, "basic") == 0) {
    log_notice(">>> testcase_setup(%s)", casename);
    configure_actor(last_space_id, ac_nested, nullptr, params);
    configure_actor(last_space_id, ac_hill, nullptr, params);
    configure_actor(last_space_id, ac_ttl, nullptr, params);
    configure_actor(last_space_id, ac_copy, nullptr, params);
    configure_actor(last_space_id, ac_append, nullptr, params);
    configure_actor(last_space_id, ac_jitter, nullptr, params);
    configure_actor(last_space_id, ac_try, nullptr, params);
    configure_actor(last_space_id, ac_jitter, nullptr, params);
    configure_actor(last_space_id, ac_try, nullptr, params);
    log_notice("<<< testcase_setup(%s): done", casename);
  } else {
    failure("unknown testcase `%s`", casename);
  }
}

void keycase_setup(const char *casename, actor_params &params) {
  if (strcmp(casename, "random") == 0 || strcmp(casename, "prng") == 0) {
    log_notice(">>> keycase_setup(%s)", casename);
    params.keygen.keycase = kc_random;
    // TODO
    log_notice("<<< keycase_setup(%s): done", casename);
  } else if (strcmp(casename, "dashes") == 0 ||
             strcmp(casename, "aside") == 0) {
    log_notice(">>> keycase_setup(%s)", casename);
    params.keygen.keycase = kc_dashes;
    // TODO
    log_notice("<<< keycase_setup(%s): done", casename);
  } else if (strcmp(casename, "custom") == 0) {
    log_notice("=== keycase_setup(%s): skip", casename);
    params.keygen.keycase = kc_custom;
  } else {
    failure("unknown keycase `%s`", casename);
  }
}

/* TODO */
