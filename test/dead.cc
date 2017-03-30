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

bool testcase_deadread::setup() {
  log_trace(">> setup");
  if (!inherited::setup())
    return false;

  log_trace("<< setup");
  return true;
}

bool testcase_deadread::run() {
  /* TODO */
  return true;
}

bool testcase_deadread::teardown() {
  log_trace(">> teardown");
  cursor_guard.release();
  txn_guard.release();
  db_guard.release();
  return true;
}

//-----------------------------------------------------------------------------

bool testcase_deadwrite::setup() {
  log_trace(">> setup");
  if (!inherited::setup())
    return false;

  log_trace("<< setup");
  return true;
}

bool testcase_deadwrite::run() {
  /* TODO */
  return true;
}

bool testcase_deadwrite::teardown() {
  log_trace(">> teardown");
  cursor_guard.release();
  txn_guard.release();
  db_guard.release();
  return true;
}
