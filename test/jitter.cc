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

bool testcase_jitter::setup() {
  log_trace(">> setup");
  if (!inherited::setup())
    return false;

  /* TODO */

  log_trace("<< setup");
  return true;
}

bool testcase_jitter::run() { return true; }

bool testcase_jitter::teardown() {
  log_trace(">> teardown");
  return inherited::teardown();
}
