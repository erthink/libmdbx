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

void configure_actor(unsigned &lastid, const actor_testcase testcase,
                     const char *id_cstr, const actor_params &params) {
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

  unsigned id = 0;
  if (!id_cstr || strcmp(id_cstr, "auto") == 0)
    id = lastid + 1;
  else {
    char *end = nullptr;
    errno = 0;
    id = strtoul(id_cstr, &end, 0);
    if (errno)
      failure_perror("Expects an integer value for actor-id\n", errno);
    if (end && *end)
      failure("The '%s' is unexpected for actor-id\n", end);
  }

  if (id < 1 || id > ACTOR_ID_MAX)
    failure("Invalid actor-id %u\n", id);
  lastid = id;

  log_trace("configure_actor: %u for %s", id, testcase2str(testcase));
  global::actors.emplace_back(actor_config(testcase, params, id, wait4id));
  global::databases.insert(params.pathname_db);
}

void testcase_setup(const char *casename, actor_params &params,
                    unsigned &lastid) {
  if (strcmp(casename, "basic") == 0) {
    log_notice(">>> testcase_setup(%s)", casename);
    configure_actor(lastid, ac_hill, nullptr, params);
    configure_actor(lastid, ac_jitter, nullptr, params);
    configure_actor(lastid, ac_jitter, nullptr, params);
    configure_actor(lastid, ac_jitter, nullptr, params);
    log_notice("<<< testcase_setup(%s): done", casename);
  } else {
    failure("unknown testcase `%s`", casename);
  }
}

/* TODO */
