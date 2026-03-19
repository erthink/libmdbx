/* This file is part of the libmdbx amalgamated source code (v0.14.1-473-g79d02de6 at 2026-03-19T02:14:19+03:00).
 *
 * libmdbx (aka MDBX) is an extremely fast, compact, powerful, embeddedable, transactional key-value storage engine with
 * open-source code. MDBX has a specific set of properties and capabilities, focused on creating unique lightweight
 * solutions.  Please visit https://libmdbx.dqdkfa.ru for more information, changelog, documentation, C++ API description
 * and links to the original git repo with the source code.  Questions, feedback and suggestions are welcome to the
 * Telegram' group https://t.me/libmdbx.
 *
 * The libmdbx code will forever remain open and with high-quality free support, as far as the life circumstances of the
 * project participants allow. Donations are welcome to ETH `0xD104d8f8B2dC312aaD74899F83EBf3EEBDC1EA3A`,
 * BTC `bc1qzvl9uegf2ea6cwlytnanrscyv8snwsvrc0xfsu`, SOL `FTCTgbHajoLVZGr8aEFWMzx3NDMyS5wXJgfeMTmJznRi`.
 * Всё будет хорошо!
 *
 * For ease of use and to eliminate potential limitations in both distribution and obstacles in technology development,
 * libmdbx is distributed as an amalgamated source code starting at the end of 2025.  The source code of the tests, as
 * well as the internal documentation, will be available only to the team directly involved in the development.
 *
 * Copyright 2015-2026 Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru>
 * SPDX-License-Identifier: Apache-2.0
 *
 * For notes about the license change, credits and acknowledgments, please refer to the COPYRIGHT file. */

/* Copied from mdbx_chk.c */

/* clang-format off */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <fuzz.h>

#define EXIT_FAILURE_MDBX (EXIT_FAILURE + 2)
MDBX_chk_context_t chk;
unsigned verbosity = 0xc;
static MDBX_chk_line_t line_struct;
bool quiet = 0;
static size_t line_count;
static FILE *line_output;

#define LINE_SEVERITY_NONE 255
static bool lf(void) {
  if (!line_struct.empty) {
    line_count += 1;
    line_struct.empty = true;
    line_struct.severity = LINE_SEVERITY_NONE;
    line_struct.scope_depth = 0;
    if (line_output) {
      fputc('\n', line_output);
      return true;
    }
  }
  return false;
}

static bool silently(enum MDBX_chk_severity severity) {
  int cutoff = chk.scope ? chk.scope->verbosity >> MDBX_chk_severity_prio_shift
                         : verbosity + (MDBX_chk_result >> MDBX_chk_severity_prio_shift);
  int prio = (severity >> MDBX_chk_severity_prio_shift);
  if (chk.scope && chk.scope->stage == MDBX_chk_tables && verbosity < 2)
    prio += 1;
  return quiet || cutoff < ((prio > 0) ? prio : 0);
}

static FILE *prefix(enum MDBX_chk_severity severity) {
  if (silently(severity))
    return NULL;

  static const char *const prefixes[16] = {
      "!!!fatal: ", // 0 fatal
      " ! ",        // 1 error
      " ~ ",        // 2 warning
      "   ",        // 3 notice
      "",           // 4 result
      " = ",        // 5 resolution
      " - ",        // 6 processing
      "   ",        // 7 info
      "   ",        // 8 verbose
      "   ",        // 9 details
      "   // ",     // A lib-verbose
      "   //// ",   // B lib-debug
      "   ////// ", // C lib-trace
      "   ////// ", // D lib-extra
      "   ////// ", // E +1
      "   ////// "  // F +2
  };

  const bool nl = line_struct.scope_depth != chk.scope_nesting ||
                  (line_struct.severity != severity && (line_struct.severity != MDBX_chk_processing ||
                                                        severity < MDBX_chk_result || severity > MDBX_chk_resolution));
  if (nl)
    lf();
  FILE *out = (severity > MDBX_chk_error) ? stdout : stderr;
  if (nl || line_struct.empty) {
    line_struct.severity = severity;
    line_struct.scope_depth = chk.scope_nesting;
    unsigned kind = line_struct.severity & MDBX_chk_severity_kind_mask;
    if (line_struct.scope_depth || *prefixes[kind]) {
      line_struct.empty = false;
      for (size_t i = 0; i < line_struct.scope_depth; ++i)
        fputs("   ", out);
      fputs(prefixes[kind], out);
    }
  }
  return line_output = out;
}

void logger(MDBX_log_level_t level, const char *function, int line, const char *fmt, va_list args) {
  if (level <= MDBX_LOG_ERROR)
    mdbx_env_chk_encount_problem(&chk);

  const unsigned kind =
      (level > MDBX_LOG_NOTICE) ? level - MDBX_LOG_NOTICE + (MDBX_chk_extra & MDBX_chk_severity_kind_mask) : level;
  const unsigned prio = kind << MDBX_chk_severity_prio_shift;
  enum MDBX_chk_severity severity = prio + kind;
  FILE *out = prefix(severity);
  if (out) {
    vfprintf(out, fmt, args);
    const bool have_lf = fmt[strlen(fmt) - 1] == '\n';
    if (level == MDBX_LOG_FATAL && function && line) {
      if (have_lf)
        for (size_t i = 0; i < line_struct.scope_depth; ++i)
          fputs("   ", out);
      fprintf(out, have_lf ? "          %s(), %u" : " (%s:%u)", function + (strncmp(function, "mdbx_", 5) ? 0 : 5),
              line);
      lf();
    } else if (have_lf) {
      line_struct.empty = true;
      line_struct.severity = LINE_SEVERITY_NONE;
      line_count += 1;
    } else
      lf();
  }
  if (level == MDBX_LOG_FATAL) {
#if !MDBX_DEBUG && !MDBX_FORCE_ASSERTIONS
    exit(EXIT_FAILURE_MDBX);
#endif
    abort();
  }
}
