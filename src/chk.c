/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2025

#include "internals.h"

typedef struct MDBX_chk_internal {
  MDBX_chk_context_t *usr;
  const struct MDBX_chk_callbacks *cb;
  uint64_t monotime_timeout;

  size_t *problem_counter;
  uint8_t flags;
  bool got_break;
  bool write_locked;
  uint8_t scope_depth;

  MDBX_chk_table_t table_gc, table_main;
  int16_t *pagemap;
  MDBX_chk_table_t *last_lookup;
  const void *last_nested;
  MDBX_chk_scope_t scope_stack[12];
  MDBX_chk_table_t *table[MDBX_MAX_DBI + CORE_DBS];

  MDBX_envinfo envinfo;
  troika_t troika;
  MDBX_val v2a_buf;
} MDBX_chk_internal_t;

__cold static int chk_check_break(MDBX_chk_scope_t *const scope) {
  MDBX_chk_internal_t *const chk = scope->internal;
  return (chk->got_break || (chk->cb->check_break && (chk->got_break = chk->cb->check_break(chk->usr))))
             ? MDBX_RESULT_TRUE
             : MDBX_RESULT_FALSE;
}

__cold static void chk_line_end(MDBX_chk_line_t *line) {
  if (likely(line)) {
    MDBX_chk_internal_t *chk = line->ctx->internal;
    assert(line->begin <= line->end && line->begin <= line->out && line->out <= line->end);
    if (likely(chk->cb->print_done))
      chk->cb->print_done(line);
  }
}

__cold __must_check_result static MDBX_chk_line_t *chk_line_begin(MDBX_chk_scope_t *const scope,
                                                                  enum MDBX_chk_severity severity) {
  MDBX_chk_internal_t *const chk = scope->internal;
  if (severity < MDBX_chk_warning)
    mdbx_env_chk_encount_problem(chk->usr);
  MDBX_chk_line_t *line = nullptr;
  if (likely(chk->cb->print_begin)) {
    line = chk->cb->print_begin(chk->usr, severity);
    if (likely(line)) {
      assert(line->ctx == nullptr || (line->ctx == chk->usr && line->empty));
      assert(line->begin <= line->end && line->begin <= line->out && line->out <= line->end);
      line->ctx = chk->usr;
    }
  }
  return line;
}

__cold static MDBX_chk_line_t *chk_line_feed(MDBX_chk_line_t *line) {
  if (likely(line)) {
    MDBX_chk_internal_t *chk = line->ctx->internal;
    enum MDBX_chk_severity severity = line->severity;
    chk_line_end(line);
    line = chk_line_begin(chk->usr->scope, severity);
  }
  return line;
}

__cold static MDBX_chk_line_t *chk_flush(MDBX_chk_line_t *line) {
  if (likely(line)) {
    MDBX_chk_internal_t *chk = line->ctx->internal;
    assert(line->begin <= line->end && line->begin <= line->out && line->out <= line->end);
    if (likely(chk->cb->print_flush)) {
      chk->cb->print_flush(line);
      assert(line->begin <= line->end && line->begin <= line->out && line->out <= line->end);
      line->out = line->begin;
    }
  }
  return line;
}

__cold static size_t chk_print_wanna(MDBX_chk_line_t *line, size_t need) {
  if (likely(line && need)) {
    size_t have = line->end - line->out;
    assert(line->begin <= line->end && line->begin <= line->out && line->out <= line->end);
    if (need > have) {
      line = chk_flush(line);
      have = line->end - line->out;
    }
    return (need < have) ? need : have;
  }
  return 0;
}

__cold static MDBX_chk_line_t *chk_puts(MDBX_chk_line_t *line, const char *str) {
  if (likely(line && str && *str)) {
    MDBX_chk_internal_t *chk = line->ctx->internal;
    size_t left = strlen(str);
    assert(line->begin <= line->end && line->begin <= line->out && line->out <= line->end);
    if (chk->cb->print_chars) {
      chk->cb->print_chars(line, str, left);
      assert(line->begin <= line->end && line->begin <= line->out && line->out <= line->end);
    } else
      do {
        size_t chunk = chk_print_wanna(line, left);
        assert(chunk <= left);
        if (unlikely(!chunk))
          break;
        memcpy(line->out, str, chunk);
        line->out += chunk;
        assert(line->begin <= line->end && line->begin <= line->out && line->out <= line->end);
        str += chunk;
        left -= chunk;
      } while (left);
    line->empty = false;
  }
  return line;
}

__cold static MDBX_chk_line_t *chk_print_va(MDBX_chk_line_t *line, const char *fmt, va_list args) {
  if (likely(line)) {
    MDBX_chk_internal_t *chk = line->ctx->internal;
    assert(line->begin <= line->end && line->begin <= line->out && line->out <= line->end);
    if (chk->cb->print_format) {
      chk->cb->print_format(line, fmt, args);
      assert(line->begin <= line->end && line->begin <= line->out && line->out <= line->end);
    } else {
      va_list ones;
      va_copy(ones, args);
      const int needed = vsnprintf(nullptr, 0, fmt, ones);
      va_end(ones);
      if (likely(needed > 0)) {
        const size_t have = chk_print_wanna(line, needed);
        if (likely(have > 0)) {
          int written = vsnprintf(line->out, have, fmt, args);
          if (likely(written > 0))
            line->out += written;
          assert(line->begin <= line->end && line->begin <= line->out && line->out <= line->end);
        }
      }
    }
    line->empty = false;
  }
  return line;
}

__cold static MDBX_chk_line_t *MDBX_PRINTF_ARGS(2, 3) chk_print(MDBX_chk_line_t *line, const char *fmt, ...) {
  if (likely(line)) {
    // MDBX_chk_internal_t *chk = line->ctx->internal;
    va_list args;
    va_start(args, fmt);
    line = chk_print_va(line, fmt, args);
    va_end(args);
    line->empty = false;
  }
  return line;
}

MDBX_MAYBE_UNUSED __cold static void chk_println_va(MDBX_chk_scope_t *const scope, enum MDBX_chk_severity severity,
                                                    const char *fmt, va_list args) {
  chk_line_end(chk_print_va(chk_line_begin(scope, severity), fmt, args));
}

MDBX_MAYBE_UNUSED __cold static void chk_println(MDBX_chk_scope_t *const scope, enum MDBX_chk_severity severity,
                                                 const char *fmt, ...) {
  va_list args;
  va_start(args, fmt);
  chk_println_va(scope, severity, fmt, args);
  va_end(args);
}

__cold static MDBX_chk_line_t *chk_print_size(MDBX_chk_line_t *line, const char *prefix, const uint64_t value,
                                              const char *suffix) {
  static const char sf[] = "KMGTPEZY"; /* LY: Kilo, Mega, Giga, Tera, Peta, Exa, Zetta, Yotta! */
  if (likely(line)) {
    MDBX_chk_internal_t *chk = line->ctx->internal;
    prefix = prefix ? prefix : "";
    suffix = suffix ? suffix : "";
    if (chk->cb->print_size)
      chk->cb->print_size(line, prefix, value, suffix);
    else
      for (unsigned i = 0;; ++i) {
        const unsigned scale = 10 + i * 10;
        const uint64_t rounded = value + (UINT64_C(5) << (scale - 10));
        const uint64_t integer = rounded >> scale;
        const uint64_t fractional = (rounded - (integer << scale)) * 100u >> scale;
        if ((rounded >> scale) <= 1000)
          return chk_print(line, "%s%" PRIu64 " (%u.%02u %ciB)%s", prefix, value, (unsigned)integer,
                           (unsigned)fractional, sf[i], suffix);
      }
    line->empty = false;
  }
  return line;
}

__cold static int chk_error_rc(MDBX_chk_scope_t *const scope, int err, const char *subj) {
  MDBX_chk_line_t *line = chk_line_begin(scope, MDBX_chk_error);
  if (line)
    chk_line_end(chk_flush(chk_print(line, "%s() failed, error %s (%d)", subj, mdbx_strerror(err), err)));
  else
    debug_log(MDBX_LOG_ERROR, "mdbx_env_chk", 0, "%s() failed, error %s (%d)", subj, mdbx_strerror(err), err);
  return err;
}

__cold static void MDBX_PRINTF_ARGS(5, 6)
    chk_object_issue(MDBX_chk_scope_t *const scope, const char *object, uint64_t entry_number, const char *caption,
                     const char *extra_fmt, ...) {
  MDBX_chk_internal_t *const chk = scope->internal;
  MDBX_chk_issue_t *issue = chk->usr->scope->issues;
  while (issue) {
    if (issue->caption == caption) {
      issue->count += 1;
      break;
    } else
      issue = issue->next;
  }
  const bool fresh = issue == nullptr;
  if (fresh) {
    issue = osal_malloc(sizeof(*issue));
    if (likely(issue)) {
      issue->caption = caption;
      issue->count = 1;
      issue->next = chk->usr->scope->issues;
      chk->usr->scope->issues = issue;
    } else
      chk_error_rc(scope, MDBX_ENOMEM, "adding issue");
  }

  va_list args;
  va_start(args, extra_fmt);
  if (chk->cb->issue) {
    mdbx_env_chk_encount_problem(chk->usr);
    chk->cb->issue(chk->usr, object, entry_number, caption, extra_fmt, args);
  } else {
    MDBX_chk_line_t *line = chk_line_begin(scope, MDBX_chk_error);
    if (entry_number != UINT64_MAX)
      chk_print(line, "%s #%" PRIu64 ": %s", object, entry_number, caption);
    else
      chk_print(line, "%s: %s", object, caption);
    if (extra_fmt)
      chk_puts(chk_print_va(chk_puts(line, " ("), extra_fmt, args), ")");
    chk_line_end(fresh ? chk_flush(line) : line);
  }
  va_end(args);
}

__cold static void MDBX_PRINTF_ARGS(2, 3) chk_scope_issue(MDBX_chk_scope_t *const scope, const char *fmt, ...) {
  MDBX_chk_internal_t *const chk = scope->internal;
  va_list args;
  va_start(args, fmt);
  if (likely(chk->cb->issue)) {
    mdbx_env_chk_encount_problem(chk->usr);
    chk->cb->issue(chk->usr, nullptr, 0, nullptr, fmt, args);
  } else
    chk_line_end(chk_print_va(chk_line_begin(scope, MDBX_chk_error), fmt, args));
  va_end(args);
}

__cold static int chk_scope_end(MDBX_chk_internal_t *chk, int err) {
  assert(chk->scope_depth > 0);
  MDBX_chk_scope_t *const inner = chk->scope_stack + chk->scope_depth;
  MDBX_chk_scope_t *const outer = chk->scope_depth ? inner - 1 : nullptr;
  if (!outer || outer->stage != inner->stage) {
    if (err == MDBX_SUCCESS && *chk->problem_counter)
      err = MDBX_PROBLEM;
    else if (*chk->problem_counter == 0 && MDBX_IS_ERROR(err))
      *chk->problem_counter = 1;
    if (chk->problem_counter != &chk->usr->result.total_problems) {
      chk->usr->result.total_problems += *chk->problem_counter;
      chk->problem_counter = &chk->usr->result.total_problems;
    }
    if (chk->cb->stage_end)
      err = chk->cb->stage_end(chk->usr, inner->stage, err);
  }
  if (chk->cb->scope_conclude)
    err = chk->cb->scope_conclude(chk->usr, outer, inner, err);
  chk->usr->scope = outer;
  chk->usr->scope_nesting = chk->scope_depth -= 1;
  if (outer)
    outer->subtotal_issues += inner->subtotal_issues;
  if (chk->cb->scope_pop)
    chk->cb->scope_pop(chk->usr, outer, inner);

  while (inner->issues) {
    MDBX_chk_issue_t *next = inner->issues->next;
    osal_free(inner->issues);
    inner->issues = next;
  }
  memset(inner, -1, sizeof(*inner));
  return err;
}

__cold static int chk_scope_begin_args(MDBX_chk_internal_t *chk, int verbosity_adjustment, enum MDBX_chk_stage stage,
                                       const void *object, size_t *problems, const char *fmt, va_list args) {
  if (unlikely(chk->scope_depth + 1u >= ARRAY_LENGTH(chk->scope_stack)))
    return MDBX_BACKLOG_DEPLETED;

  MDBX_chk_scope_t *const outer = chk->scope_stack + chk->scope_depth;
  const int verbosity = outer->verbosity + (verbosity_adjustment - 1) * (1 << MDBX_chk_severity_prio_shift);
  MDBX_chk_scope_t *const inner = outer + 1;
  memset(inner, 0, sizeof(*inner));
  inner->internal = outer->internal;
  inner->stage = stage ? stage : (stage = outer->stage);
  inner->object = object;
  inner->verbosity = (verbosity < MDBX_chk_warning) ? MDBX_chk_warning : (enum MDBX_chk_severity)verbosity;
  if (problems)
    chk->problem_counter = problems;
  else if (!chk->problem_counter || outer->stage != stage)
    chk->problem_counter = &chk->usr->result.total_problems;

  if (chk->cb->scope_push) {
    const int err = chk->cb->scope_push(chk->usr, outer, inner, fmt, args);
    if (unlikely(err != MDBX_SUCCESS))
      return err;
  }
  chk->usr->scope = inner;
  chk->usr->scope_nesting = chk->scope_depth += 1;

  if (stage != outer->stage && chk->cb->stage_begin) {
    int err = chk->cb->stage_begin(chk->usr, stage);
    if (unlikely(err != MDBX_SUCCESS)) {
      err = chk_scope_end(chk, err);
      assert(err != MDBX_SUCCESS);
      return err ? err : MDBX_RESULT_TRUE;
    }
  }
  return MDBX_SUCCESS;
}

__cold static int MDBX_PRINTF_ARGS(6, 7)
    chk_scope_begin(MDBX_chk_internal_t *chk, int verbosity_adjustment, enum MDBX_chk_stage stage, const void *object,
                    size_t *problems, const char *fmt, ...) {
  va_list args;
  va_start(args, fmt);
  int rc = chk_scope_begin_args(chk, verbosity_adjustment, stage, object, problems, fmt, args);
  va_end(args);
  return rc;
}

__cold static int chk_scope_restore(MDBX_chk_scope_t *const target, int err) {
  MDBX_chk_internal_t *const chk = target->internal;
  assert(target <= chk->usr->scope);
  while (chk->usr->scope > target)
    err = chk_scope_end(chk, err);
  return err;
}

__cold void chk_scope_pop(MDBX_chk_scope_t *const inner) {
  if (inner && inner > inner->internal->scope_stack)
    chk_scope_restore(inner - 1, MDBX_SUCCESS);
}

__cold static MDBX_chk_scope_t *MDBX_PRINTF_ARGS(3, 4)
    chk_scope_push(MDBX_chk_scope_t *const scope, int verbosity_adjustment, const char *fmt, ...) {
  chk_scope_restore(scope, MDBX_SUCCESS);
  va_list args;
  va_start(args, fmt);
  int err = chk_scope_begin_args(scope->internal, verbosity_adjustment, scope->stage, nullptr, nullptr, fmt, args);
  va_end(args);
  return err ? nullptr : scope + 1;
}

__cold static const char *chk_v2a(MDBX_chk_internal_t *chk, const MDBX_val *val) {
  if (val == MDBX_CHK_MAIN)
    return "@MAIN";
  if (val == MDBX_CHK_GC)
    return "@GC";
  if (val == MDBX_CHK_META)
    return "@META";

  const unsigned char *const data = val->iov_base;
  const size_t len = val->iov_len;
  if (data == MDBX_CHK_MAIN)
    return "@MAIN";
  if (data == MDBX_CHK_GC)
    return "@GC";
  if (data == MDBX_CHK_META)
    return "@META";

  if (!len)
    return "<zero-length>";
  if (!data)
    return "<nullptr>";
  if (len > 65536) {
    const size_t enough = 42;
    if (chk->v2a_buf.iov_len < enough) {
      void *ptr = osal_realloc(chk->v2a_buf.iov_base, enough);
      if (unlikely(!ptr))
        return "<out-of-memory>";
      chk->v2a_buf.iov_base = ptr;
      chk->v2a_buf.iov_len = enough;
    }
    snprintf(chk->v2a_buf.iov_base, chk->v2a_buf.iov_len, "<too-long.%" PRIuSIZE ">", len);
    return chk->v2a_buf.iov_base;
  }

  bool printable = true;
  bool quoting = false;
  size_t xchars = 0;
  for (size_t i = 0; i < len && printable; ++i) {
    quoting = quoting || !(data[i] == '_' || isalnum(data[i]));
    printable = isprint(data[i]) || (data[i] < ' ' && ++xchars < 4 && len > xchars * 4);
  }

  size_t need = len + 1;
  if (quoting || !printable)
    need += len + /* quotes */ 2 + 2 * /* max xchars */ 4;
  if (need > chk->v2a_buf.iov_len) {
    void *ptr = osal_realloc(chk->v2a_buf.iov_base, need);
    if (unlikely(!ptr))
      return "<out-of-memory>";
    chk->v2a_buf.iov_base = ptr;
    chk->v2a_buf.iov_len = need;
  }

  static const char hex[] = "0123456789abcdef";
  char *w = chk->v2a_buf.iov_base;
  if (!quoting) {
    memcpy(w, data, len);
    w += len;
  } else if (printable) {
    *w++ = '\'';
    for (size_t i = 0; i < len; ++i) {
      if (data[i] < ' ') {
        assert((char *)chk->v2a_buf.iov_base + chk->v2a_buf.iov_len > w + 4);
        w[0] = '\\';
        w[1] = 'x';
        w[2] = hex[data[i] >> 4];
        w[3] = hex[data[i] & 15];
        w += 4;
      } else if (strchr("\"'`\\", data[i])) {
        assert((char *)chk->v2a_buf.iov_base + chk->v2a_buf.iov_len > w + 2);
        w[0] = '\\';
        w[1] = data[i];
        w += 2;
      } else {
        assert((char *)chk->v2a_buf.iov_base + chk->v2a_buf.iov_len > w + 1);
        *w++ = data[i];
      }
    }
    *w++ = '\'';
  } else {
    *w++ = '\\';
    *w++ = 'x';
    for (size_t i = 0; i < len; ++i) {
      assert((char *)chk->v2a_buf.iov_base + chk->v2a_buf.iov_len > w + 2);
      w[0] = hex[data[i] >> 4];
      w[1] = hex[data[i] & 15];
      w += 2;
    }
  }
  assert((char *)chk->v2a_buf.iov_base + chk->v2a_buf.iov_len > w);
  *w = 0;
  return chk->v2a_buf.iov_base;
}

__cold static void chk_dispose(MDBX_chk_internal_t *chk) {
  assert(chk->table[FREE_DBI] == &chk->table_gc);
  assert(chk->table[MAIN_DBI] == &chk->table_main);
  for (size_t i = 0; i < ARRAY_LENGTH(chk->table); ++i) {
    MDBX_chk_table_t *const tbl = chk->table[i];
    if (tbl) {
      chk->table[i] = nullptr;
      if (chk->cb->table_dispose && tbl->cookie) {
        chk->cb->table_dispose(chk->usr, tbl);
        tbl->cookie = nullptr;
      }
      if (tbl != &chk->table_gc && tbl != &chk->table_main)
        osal_free(tbl);
    }
  }
  osal_free(chk->v2a_buf.iov_base);
  osal_free(chk->pagemap);
  chk->usr->internal = nullptr;
  chk->usr->scope = nullptr;
  chk->pagemap = nullptr;
  memset(chk, 0xDD, sizeof(*chk));
  osal_free(chk);
}

static size_t div_8s(size_t numerator, size_t divider) {
  assert(numerator <= (SIZE_MAX >> 8));
  return (numerator << 8) / divider;
}

static size_t mul_8s(size_t quotient, size_t multiplier) {
  size_t hi = multiplier * (quotient >> 8);
  size_t lo = multiplier * (quotient & 255) + 128;
  return hi + (lo >> 8);
}

static void histogram_reduce(struct MDBX_chk_histogram *p) {
  const size_t size = ARRAY_LENGTH(p->ranges), last = size - 1;
  // ищем пару для слияния с минимальной ошибкой
  size_t min_err = SIZE_MAX, min_i = last - 1;
  for (size_t i = 0; i < last; ++i) {
    const size_t b1 = p->ranges[i].begin, e1 = p->ranges[i].end, s1 = p->ranges[i].amount;
    const size_t b2 = p->ranges[i + 1].begin, e2 = p->ranges[i + 1].end, s2 = p->ranges[i + 1].amount;
    const size_t l1 = e1 - b1, l2 = e2 - b2, lx = e2 - b1, sx = s1 + s2;
    assert(s1 > 0 && b1 > 0 && b1 < e1);
    assert(s2 > 0 && b2 > 0 && b2 < e2);
    assert(e1 <= b2);
    // за ошибку принимаем площадь изменений на гистограмме при слиянии
    const size_t h1 = div_8s(s1, l1), h2 = div_8s(s2, l2), hx = div_8s(sx, lx);
    const size_t d1 = mul_8s((h1 > hx) ? h1 - hx : hx - h1, l1);
    const size_t d2 = mul_8s((h2 > hx) ? h2 - hx : hx - h2, l2);
    const size_t dx = mul_8s(hx, b2 - e1);
    const size_t err = d1 + d2 + dx;
    if (min_err >= err) {
      min_i = i;
      min_err = err;
    }
  }
  // объединяем
  p->ranges[min_i].end = p->ranges[min_i + 1].end;
  p->ranges[min_i].amount += p->ranges[min_i + 1].amount;
  p->ranges[min_i].count += p->ranges[min_i + 1].count;
  if (min_i < last)
    // перемещаем хвост
    memmove(p->ranges + min_i, p->ranges + min_i + 1, (last - min_i) * sizeof(p->ranges[0]));
  // обнуляем последний элемент и продолжаем
  p->ranges[last].count = 0;
}

static void histogram_acc(const size_t n, struct MDBX_chk_histogram *p) {
  STATIC_ASSERT(ARRAY_LENGTH(p->ranges) > 2);
  p->amount += n;
  p->count += 1;
  if (likely(n < 2)) {
    p->ones += n;
    p->pad += 1;
  } else
    for (;;) {
      const size_t size = ARRAY_LENGTH(p->ranges), last = size - 1;
      size_t i = 0;
      while (i < size && p->ranges[i].count && n >= p->ranges[i].begin) {
        if (n < p->ranges[i].end) {
          // значение попадает в существующий интервал
          p->ranges[i].amount += n;
          p->ranges[i].count += 1;
          return;
        }
        ++i;
      }
      if (p->ranges[last].count == 0) {
        // использованы еще не все слоты, добавляем интервал
        assert(i < size);
        if (p->ranges[i].count) {
          // раздвигаем
          assert(i < last);
#ifdef __COVERITY__
          if (i < last) /* avoid Coverity false-positive issue */
#endif                  /* __COVERITY__ */
            memmove(p->ranges + i + 1, p->ranges + i, (last - i) * sizeof(p->ranges[0]));
        }
        p->ranges[i].begin = n;
        p->ranges[i].end = n + 1;
        p->ranges[i].amount = n;
        p->ranges[i].count = 1;
        return;
      }
      histogram_reduce(p);
    }
}

__cold static MDBX_chk_line_t *histogram_dist(MDBX_chk_line_t *line, const struct MDBX_chk_histogram *histogram,
                                              const char *prefix, const char *first, bool amount) {
  line = chk_print(line, "%s:", prefix);
  const char *comma = "";
  const size_t first_val = amount ? histogram->ones : histogram->pad;
  if (first_val) {
    chk_print(line, " %s=%" PRIuSIZE, first, first_val);
    comma = ",";
  }
  for (size_t n = 0; n < ARRAY_LENGTH(histogram->ranges); ++n)
    if (histogram->ranges[n].count) {
      chk_print(line, "%s %" PRIuSIZE, comma, histogram->ranges[n].begin);
      if (histogram->ranges[n].begin != histogram->ranges[n].end - 1)
        chk_print(line, "-%" PRIuSIZE, histogram->ranges[n].end - 1);
      line = chk_print(line, "=%" PRIuSIZE, amount ? histogram->ranges[n].amount : histogram->ranges[n].count);
      comma = ",";
    }
  return line;
}

__cold static MDBX_chk_line_t *histogram_print(MDBX_chk_scope_t *scope, MDBX_chk_line_t *line,
                                               const struct MDBX_chk_histogram *histogram, const char *prefix,
                                               const char *first, bool amount) {
  if (histogram->count) {
    line = chk_print(line, "%s %" PRIuSIZE, prefix, amount ? histogram->amount : histogram->count);
    if (scope->verbosity > MDBX_chk_info)
      line = chk_puts(histogram_dist(line, histogram, " (distribution", first, amount), ")");
  }
  return line;
}

//-----------------------------------------------------------------------------

__cold static int chk_get_tbl(MDBX_chk_scope_t *const scope, const walk_tbl_t *in, MDBX_chk_table_t **out) {
  MDBX_chk_internal_t *const chk = scope->internal;
  if (chk->last_lookup && chk->last_lookup->name.iov_base == in->name.iov_base) {
    *out = chk->last_lookup;
    return MDBX_SUCCESS;
  }

  for (size_t i = 0; i < ARRAY_LENGTH(chk->table); ++i) {
    MDBX_chk_table_t *tbl = chk->table[i];
    if (!tbl) {
      tbl = osal_calloc(1, sizeof(MDBX_chk_table_t));
      if (unlikely(!tbl)) {
        *out = nullptr;
        return chk_error_rc(scope, MDBX_ENOMEM, "alloc_table");
      }
      chk->table[i] = tbl;
      tbl->flags = in->internal->flags;
      tbl->id = -1;
      tbl->name = in->name;
    }
    if (tbl->name.iov_base == in->name.iov_base) {
      if (tbl->id < 0) {
        tbl->id = (int)i;
        tbl->cookie =
            chk->cb->table_filter ? chk->cb->table_filter(chk->usr, &tbl->name, tbl->flags) : (void *)(intptr_t)-1;
      }
      *out = (chk->last_lookup = tbl);
      return MDBX_SUCCESS;
    }
  }
  chk_scope_issue(scope, "too many tables > %u", (unsigned)ARRAY_LENGTH(chk->table) - CORE_DBS - /* meta */ 1);
  *out = nullptr;
  return MDBX_PROBLEM;
}

//------------------------------------------------------------------------------

__cold static void chk_verbose_meta(MDBX_chk_scope_t *const scope, const unsigned num) {
  MDBX_chk_line_t *line = chk_line_begin(scope, MDBX_chk_verbose);
  MDBX_chk_internal_t *const chk = scope->internal;
  if (line) {
    MDBX_env *const env = chk->usr->env;
    const bool have_bootid = (chk->envinfo.mi_bootid.current.x | chk->envinfo.mi_bootid.current.y) != 0;
    const bool bootid_match = have_bootid && memcmp(&chk->envinfo.mi_bootid.meta[num], &chk->envinfo.mi_bootid.current,
                                                    sizeof(chk->envinfo.mi_bootid.current)) == 0;

    const char *status = "stay";
    if (num == chk->troika.recent)
      status = "head";
    else if (num == TROIKA_TAIL(&chk->troika))
      status = "tail";
    line = chk_print(line, "meta-%u: %s, ", num, status);

    switch (chk->envinfo.mi_meta_sign[num]) {
    case DATASIGN_NONE:
      line = chk_puts(line, "no-sync/legacy");
      break;
    case DATASIGN_WEAK:
      line = chk_print(line, "weak-%s",
                       have_bootid ? (bootid_match ? "intact (same boot-id)" : "dead") : "unknown (no boot-id)");
      break;
    default:
      line = chk_puts(line, "steady");
      break;
    }
    const txnid_t meta_txnid = chk->envinfo.mi_meta_txnid[num];
    line = chk_print(line, " txn#%" PRIaTXN ", ", meta_txnid);
    if (chk->envinfo.mi_bootid.meta[num].x | chk->envinfo.mi_bootid.meta[num].y)
      line = chk_print(line, "boot-id %" PRIx64 "-%" PRIx64 " (%s)", chk->envinfo.mi_bootid.meta[num].x,
                       chk->envinfo.mi_bootid.meta[num].y, bootid_match ? "live" : "not match");
    else
      line = chk_puts(line, "no boot-id");

    if (env->stuck_meta >= 0) {
      if (num == (unsigned)env->stuck_meta)
        line = chk_print(line, ", %s", "forced for checking");
    } else if (meta_txnid > chk->envinfo.mi_recent_txnid &&
               (env->flags & (MDBX_EXCLUSIVE | MDBX_RDONLY)) == MDBX_EXCLUSIVE)
      line = chk_print(line, ", rolled-back %" PRIu64 " commit(s) (%" PRIu64 " >>> %" PRIu64 ")",
                       meta_txnid - chk->envinfo.mi_recent_txnid, meta_txnid, chk->envinfo.mi_recent_txnid);
    chk_line_end(line);
  }
}

__cold static int chk_pgvisitor(const size_t pgno, const unsigned npages, void *const ctx, const int deep,
                                const walk_tbl_t *tbl_info, const size_t page_size, const page_type_t pagetype,
                                const MDBX_error_t page_err, const size_t nentries, const size_t payload_bytes,
                                const size_t header_bytes, const size_t unused_bytes, const size_t parent_pgno) {
  MDBX_chk_scope_t *const scope = ctx;
  MDBX_chk_internal_t *const chk = scope->internal;
  MDBX_chk_context_t *const usr = chk->usr;
  MDBX_env *const env = usr->env;

  MDBX_chk_table_t *tbl;
  int err = chk_get_tbl(scope, tbl_info, &tbl);
  if (unlikely(err))
    return err;

  if (deep > 42) {
    chk_scope_issue(scope, "too deeply %u, page %zu, parent %zu", deep, pgno, parent_pgno);
    return MDBX_CORRUPTED /* avoid infinite loop/recursion */;
  }
  histogram_acc(deep, &tbl->histogram.deep);
  usr->result.processed_pages += npages;
  const size_t page_bytes = payload_bytes + header_bytes + unused_bytes;

  int height = deep + 1;
  if (tbl->id >= CORE_DBS)
    height -= usr->txn->dbs[MAIN_DBI].height;
  const tree_t *nested = tbl_info->nested;
  if (nested) {
    if (tbl->flags & MDBX_DUPSORT)
      height -= tbl_info->internal->height;
    else {
      chk_object_issue(scope, "nested tree", pgno, "unexpected", "table %s flags 0x%x, deep %i",
                       chk_v2a(chk, &tbl->name), tbl->flags, deep);
      nested = nullptr;
    }
  } else
    chk->last_nested = nullptr;

  const char *pagetype_caption;
  bool branch = false;
  struct MDBX_chk_histogram *filling = nullptr;
  switch (pagetype) {
  default:
    chk_object_issue(scope, "page", pgno, "unknown page-type", "type %u, deep %i, parent %zu", (unsigned)pagetype, deep,
                     parent_pgno);
    pagetype_caption = "unknown";
    tbl->pages.other += npages;
    break;
  case page_broken:
    assert(page_err != MDBX_SUCCESS);
    pagetype_caption = "broken";
    tbl->pages.other += npages;
    break;
  case page_sub_broken:
    assert(page_err != MDBX_SUCCESS);
    pagetype_caption = "broken-subpage";
    tbl->pages.other += npages;
    break;
  case page_large:
    pagetype_caption = "large";
    histogram_acc(npages, &tbl->histogram.large_pages);
    if (tbl->flags & MDBX_DUPSORT)
      chk_object_issue(scope, "page", pgno, "unexpected", "type %u, table %s flags 0x%x, deep %i, parent %zu",
                       (unsigned)pagetype, chk_v2a(chk, &tbl->name), tbl->flags, deep, parent_pgno);
    break;
  case page_branch:
    branch = true;
    if (!nested) {
      pagetype_caption = "branch";
      tbl->pages.branch += 1;
      filling = &tbl->histogram.tree_filling;
    } else {
      pagetype_caption = "nested-branch";
      tbl->pages.nested_branch += 1;
      filling = &tbl->histogram.nested_tree_filling;
    }
    break;
  case page_dupfix_leaf:
    if (!nested)
      chk_object_issue(scope, "page", pgno, "unexpected", "type %u, table %s flags 0x%x, deep %i, parent %zu",
                       (unsigned)pagetype, chk_v2a(chk, &tbl->name), tbl->flags, deep, parent_pgno);
    /* fall through */
    __fallthrough;
  case page_leaf:
    if (!nested) {
      pagetype_caption = "leaf";
      tbl->pages.leaf += 1;
      filling = &tbl->histogram.tree_filling;
      if (height != tbl_info->internal->height)
        chk_object_issue(scope, "page", pgno, "wrong tree height", "actual %i != %i table %s, parent %zu", height,
                         tbl_info->internal->height, chk_v2a(chk, &tbl->name), parent_pgno);
    } else {
      pagetype_caption = (pagetype == page_leaf) ? "nested-leaf" : "nested-leaf-dupfix";
      tbl->pages.nested_leaf += 1;
      filling = &tbl->histogram.nested_tree_filling;
      if (chk->last_nested != nested) {
        histogram_acc(height, &tbl->histogram.nested_tree);
        chk->last_nested = nested;
      }
      if (height != nested->height)
        chk_object_issue(scope, "page", pgno, "wrong nested-tree height", "actual %i != %i dupsort-node %s, parent %zu",
                         height, nested->height, chk_v2a(chk, &tbl->name), parent_pgno);
    }
    break;
  case page_sub_dupfix_leaf:
  case page_sub_leaf:
    pagetype_caption = (pagetype == page_sub_leaf) ? "subleaf-dupsort" : "subleaf-dupfix";
    tbl->pages.nested_subleaf += 1;
    if ((tbl->flags & MDBX_DUPSORT) == 0 || nested)
      chk_object_issue(scope, "page", pgno, "unexpected", "type %u, table %s flags 0x%x, deep %i, parent %zu",
                       (unsigned)pagetype, chk_v2a(chk, &tbl->name), tbl->flags, deep, parent_pgno);
    else
      filling = &tbl->histogram.nested_tree_filling;
    break;
  }

  if (filling)
    histogram_acc((page_size - unused_bytes) * 100 / page_size, filling);

  if (npages) {
    if (tbl->cookie) {
      MDBX_chk_line_t *line = chk_line_begin(scope, MDBX_chk_extra);
      if (npages == 1)
        chk_print(line, "%s-page %" PRIuSIZE, pagetype_caption, pgno);
      else
        chk_print(line, "%s-span %" PRIuSIZE "[%u]", pagetype_caption, pgno, npages);
      chk_line_end(chk_print(
          line, " of %s: header %" PRIiPTR ", %s %" PRIiPTR ", payload %" PRIiPTR ", unused %" PRIiPTR ", deep %i",
          chk_v2a(chk, &tbl->name), header_bytes, (pagetype == page_branch) ? "keys" : "entries", nentries,
          payload_bytes, unused_bytes, deep));
    }

    bool already_used = false;
    for (unsigned n = 0; n < npages; ++n) {
      const size_t spanpgno = pgno + n;
      if (spanpgno >= usr->result.alloc_pages) {
        chk_object_issue(scope, "page", spanpgno, "wrong page-no", "%s-page: %" PRIuSIZE " > %" PRIuSIZE ", deep %i",
                         pagetype_caption, spanpgno, usr->result.alloc_pages, deep);
        tbl->pages.all += 1;
      } else if (chk->pagemap[spanpgno]) {
        const MDBX_chk_table_t *const rival = chk->table[chk->pagemap[spanpgno] - 1];
        chk_object_issue(scope, "page", spanpgno, (branch && rival == tbl) ? "loop" : "already used",
                         "%s-page: by %s, deep %i, parent %zu", pagetype_caption, chk_v2a(chk, &rival->name), deep,
                         parent_pgno);
        already_used = true;
      } else {
        chk->pagemap[spanpgno] = (int16_t)tbl->id + 1;
        tbl->pages.all += 1;
      }
    }

    if (already_used)
      return branch ? MDBX_RESULT_TRUE /* avoid infinite loop/recursion */
                    : MDBX_SUCCESS;
  }

  if (MDBX_IS_ERROR(page_err)) {
    chk_object_issue(scope, "page", pgno, "invalid/corrupted", "%s-page, parent %zu", pagetype_caption, parent_pgno);
  } else {
    if (unused_bytes > page_size)
      chk_object_issue(scope, "page", pgno, "illegal unused-bytes", "%s-page: %u < %" PRIuSIZE " < %u, parent %zu",
                       pagetype_caption, 0, unused_bytes, env->ps, parent_pgno);

    if (header_bytes < (int)sizeof(long) || (size_t)header_bytes >= env->ps - sizeof(long)) {
      chk_object_issue(scope, "page", pgno, "illegal header-length",
                       "%s-page: %" PRIuSIZE " < %" PRIuSIZE " < %" PRIuSIZE ", parent %zu", pagetype_caption,
                       sizeof(long), header_bytes, env->ps - sizeof(long), parent_pgno);
    }
    if (nentries < 1 || (pagetype == page_branch && nentries < 2)) {
      chk_object_issue(scope, "page", pgno, nentries ? "half-empty" : "empty",
                       "%s-page: payload %" PRIuSIZE " bytes, %" PRIuSIZE " entries, deep %i, parent %zu",
                       pagetype_caption, payload_bytes, nentries, deep, parent_pgno);
      tbl->pages.empty += 1;
    }

    if (npages) {
      if (page_bytes != page_size) {
        chk_object_issue(scope, "page", pgno, "misused",
                         "%s-page: %" PRIuPTR " != %" PRIuPTR " (%" PRIuPTR "h + %" PRIuPTR "p + %" PRIuPTR
                         "u), deep %i, parent %zu",
                         pagetype_caption, page_size, page_bytes, header_bytes, payload_bytes, unused_bytes, deep,
                         parent_pgno);
        if (page_size > page_bytes)
          tbl->lost_bytes += page_size - page_bytes;
      } else {
        tbl->payload_bytes += payload_bytes + header_bytes;
        usr->result.total_payload_bytes += payload_bytes + header_bytes;
      }
    }
  }
  return chk_check_break(scope);
}

__cold static int chk_tree(MDBX_chk_scope_t *const scope) {
  MDBX_chk_internal_t *const chk = scope->internal;
  MDBX_chk_context_t *const usr = chk->usr;
  MDBX_env *const env = usr->env;
  MDBX_txn *const txn = usr->txn;

#if defined(_WIN32) || defined(_WIN64)
  SetLastError(ERROR_SUCCESS);
#else
  errno = 0;
#endif /* Windows */
  chk->pagemap = osal_calloc(usr->result.alloc_pages, sizeof(*chk->pagemap));
  if (!chk->pagemap) {
    int err = osal_get_errno();
    return chk_error_rc(scope, err ? err : MDBX_ENOMEM, "calloc");
  }

  if (scope->verbosity > MDBX_chk_info)
    chk_scope_push(scope, 0, "Walking pages...");
  /* always skip key ordering checking
   * to avoid MDBX_CORRUPTED in case custom comparators were used */
  usr->result.processed_pages = NUM_METAS;
  int err = walk_pages(txn, chk_pgvisitor, scope, dont_check_keys_ordering);
  if (MDBX_IS_ERROR(err) && err != MDBX_EINTR)
    chk_error_rc(scope, err, "walk_pages");

  for (size_t n = NUM_METAS; n < usr->result.alloc_pages; ++n)
    if (!chk->pagemap[n])
      usr->result.unused_pages += 1;

  MDBX_chk_table_t total;
  memset(&total, 0, sizeof(total));
  total.pages.all = NUM_METAS;
  for (size_t i = 0; i < ARRAY_LENGTH(chk->table) && chk->table[i]; ++i) {
    MDBX_chk_table_t *const tbl = chk->table[i];
    total.payload_bytes += tbl->payload_bytes;
    total.lost_bytes += tbl->lost_bytes;
    total.pages.all += tbl->pages.all;
    total.pages.empty += tbl->pages.empty;
    total.pages.other += tbl->pages.other;
    total.pages.branch += tbl->pages.branch;
    total.pages.leaf += tbl->pages.leaf;
    total.pages.nested_branch += tbl->pages.nested_branch;
    total.pages.nested_leaf += tbl->pages.nested_leaf;
    total.pages.nested_subleaf += tbl->pages.nested_subleaf;
  }
  assert(total.pages.all == usr->result.processed_pages);

  const size_t total_page_bytes = pgno2bytes(env, total.pages.all);
  if (usr->scope->subtotal_issues || usr->scope->verbosity >= MDBX_chk_verbose)
    chk_line_end(chk_print(chk_line_begin(usr->scope, MDBX_chk_resolution),
                           "walked %zu pages, left/unused %zu"
                           ", %" PRIuSIZE " problem(s)",
                           usr->result.processed_pages, usr->result.unused_pages, usr->scope->subtotal_issues));

  err = chk_scope_restore(scope, err);
  if (scope->verbosity > MDBX_chk_info) {
    for (size_t i = 0; i < ARRAY_LENGTH(chk->table) && chk->table[i]; ++i) {
      MDBX_chk_table_t *const tbl = chk->table[i];
      MDBX_chk_scope_t *inner = chk_scope_push(scope, 0, "tree %s:", chk_v2a(chk, &tbl->name));
      if (tbl->pages.all == 0)
        chk_line_end(chk_print(chk_line_begin(inner, MDBX_chk_resolution), "empty"));
      else {
        MDBX_chk_line_t *line = chk_line_begin(inner, MDBX_chk_info);
        if (line) {
          line = chk_print(line, "page usage: subtotal %" PRIuSIZE, tbl->pages.all);
          const size_t branch_pages = tbl->pages.branch + tbl->pages.nested_branch;
          const size_t leaf_pages = tbl->pages.leaf + tbl->pages.nested_leaf + tbl->pages.nested_subleaf;
          if (tbl->pages.other)
            line = chk_print(line, ", other %" PRIuSIZE, tbl->pages.other);
          if (tbl->pages.other == 0 || (branch_pages | leaf_pages | tbl->histogram.large_pages.count) != 0) {
            line = chk_print(line, ", branch %" PRIuSIZE ", leaf %" PRIuSIZE, branch_pages, leaf_pages);
            if (tbl->histogram.large_pages.count || (tbl->flags & MDBX_DUPSORT) == 0) {
              line = chk_print(line, ", large %" PRIuSIZE, tbl->histogram.large_pages.count);
              if (tbl->histogram.large_pages.amount | tbl->histogram.large_pages.count)
                line = histogram_print(inner, line, &tbl->histogram.large_pages, " amount", "single", true);
            }
          }
          line = histogram_dist(chk_line_feed(line), &tbl->histogram.deep, "tree deep density", "1", false);
          if (tbl != &chk->table_gc && tbl->histogram.nested_tree.count) {
            line = chk_print(chk_line_feed(line), "nested tree(s) %" PRIuSIZE, tbl->histogram.nested_tree.count);
            line = histogram_dist(line, &tbl->histogram.nested_tree, " density", "1", false);
            line = chk_print(chk_line_feed(line),
                             "nested tree(s) pages %" PRIuSIZE ": branch %" PRIuSIZE ", leaf %" PRIuSIZE
                             ", subleaf %" PRIuSIZE,
                             tbl->pages.nested_branch + tbl->pages.nested_leaf, tbl->pages.nested_branch,
                             tbl->pages.nested_leaf, tbl->pages.nested_subleaf);
          }

          const size_t bytes = pgno2bytes(env, tbl->pages.all);
          line =
              chk_print(chk_line_feed(line),
                        "page filling: subtotal %" PRIuSIZE " bytes (%.1f%%), payload %" PRIuSIZE
                        " (%.1f%%), unused %" PRIuSIZE " (%.1f%%)",
                        bytes, bytes * 100.0 / total_page_bytes, tbl->payload_bytes, tbl->payload_bytes * 100.0 / bytes,
                        bytes - tbl->payload_bytes, (bytes - tbl->payload_bytes) * 100.0 / bytes);
          if (tbl->pages.empty)
            line = chk_print(line, ", %" PRIuSIZE " empty pages", tbl->pages.empty);
          if (tbl->lost_bytes)
            line = chk_print(line, ", %" PRIuSIZE " bytes lost", tbl->lost_bytes);

          line =
              histogram_dist(chk_line_feed(line), &tbl->histogram.tree_filling, "tree %-filling density", "1", false);
          if (tbl->histogram.nested_tree_filling.count)
            line = histogram_dist(chk_line_feed(line), &tbl->histogram.nested_tree_filling,
                                  "nested tree(s) %-filling density", "1", false);
          chk_line_end(line);
        }
      }
      chk_scope_restore(scope, 0);
    }
  }

  MDBX_chk_line_t *line = chk_line_begin(scope, MDBX_chk_resolution);
  line = chk_print(line,
                   "summary: total %" PRIuSIZE " bytes, payload %" PRIuSIZE " (%.1f%%), unused %" PRIuSIZE " (%.1f%%),"
                   " average fill %.1f%%",
                   total_page_bytes, usr->result.total_payload_bytes,
                   usr->result.total_payload_bytes * 100.0 / total_page_bytes,
                   total_page_bytes - usr->result.total_payload_bytes,
                   (total_page_bytes - usr->result.total_payload_bytes) * 100.0 / total_page_bytes,
                   usr->result.total_payload_bytes * 100.0 / total_page_bytes);
  if (total.pages.empty)
    line = chk_print(line, ", %" PRIuSIZE " empty pages", total.pages.empty);
  if (total.lost_bytes)
    line = chk_print(line, ", %" PRIuSIZE " bytes lost", total.lost_bytes);
  chk_line_end(line);
  return err;
}

typedef int(chk_kv_visitor)(MDBX_chk_scope_t *const scope, MDBX_chk_table_t *tbl, const size_t record_number,
                            const MDBX_val *key, const MDBX_val *data);

__cold static int chk_handle_kv(MDBX_chk_scope_t *const scope, MDBX_chk_table_t *tbl, const size_t record_number,
                                const MDBX_val *key, const MDBX_val *data) {
  MDBX_chk_internal_t *const chk = scope->internal;
  int err = MDBX_SUCCESS;
  assert(tbl->cookie);
  if (chk->cb->table_handle_kv)
    err = chk->cb->table_handle_kv(chk->usr, tbl, record_number, key, data);
  return err ? err : chk_check_break(scope);
}

__cold static int chk_db(MDBX_chk_scope_t *const scope, MDBX_dbi dbi, MDBX_chk_table_t *tbl, chk_kv_visitor *handler) {
  MDBX_chk_internal_t *const chk = scope->internal;
  MDBX_chk_context_t *const usr = chk->usr;
  MDBX_env *const env = usr->env;
  MDBX_txn *const txn = usr->txn;
  MDBX_cursor *cursor = nullptr;
  size_t record_count = 0, dups = 0, sub_databases = 0;
  int err;

  if ((MDBX_TXN_FINISHED | MDBX_TXN_ERROR) & txn->flags) {
    chk_line_end(chk_flush(chk_print(chk_line_begin(scope, MDBX_chk_error),
                                     "abort processing %s due to a previous error", chk_v2a(chk, &tbl->name))));
    err = MDBX_BAD_TXN;
    goto bailout;
  }

  if (0 > (int)dbi) {
    err = dbi_open(txn, &tbl->name, MDBX_DB_ACCEDE, &dbi,
                   (chk->flags & MDBX_CHK_IGNORE_ORDER) ? cmp_equal_or_greater : nullptr,
                   (chk->flags & MDBX_CHK_IGNORE_ORDER) ? cmp_equal_or_greater : nullptr);
    if (unlikely(err)) {
      tASSERT(txn, dbi >= txn->env->n_dbi || (txn->env->dbs_flags[dbi] & DB_VALID) == 0);
      chk_error_rc(scope, err, "mdbx_dbi_open");
      goto bailout;
    }
    tASSERT(txn, dbi < txn->env->n_dbi && (txn->env->dbs_flags[dbi] & DB_VALID) != 0);
  }

  const tree_t *const db = txn->dbs + dbi;
  if (handler) {
    const char *key_mode = nullptr;
    switch (tbl->flags & (MDBX_REVERSEKEY | MDBX_INTEGERKEY)) {
    case 0:
      key_mode = "usual";
      break;
    case MDBX_REVERSEKEY:
      key_mode = "reserve";
      break;
    case MDBX_INTEGERKEY:
      key_mode = "ordinal";
      break;
    case MDBX_REVERSEKEY | MDBX_INTEGERKEY:
      key_mode = "msgpack";
      break;
    default:
      key_mode = "inconsistent";
      chk_scope_issue(scope, "wrong key-mode (0x%x)", tbl->flags & (MDBX_REVERSEKEY | MDBX_INTEGERKEY));
    }

    const char *value_mode = nullptr;
    switch (tbl->flags & (MDBX_DUPSORT | MDBX_REVERSEDUP | MDBX_DUPFIXED | MDBX_INTEGERDUP)) {
    case 0:
      value_mode = "single";
      break;
    case MDBX_DUPSORT:
      value_mode = "multi";
      break;
    case MDBX_DUPSORT | MDBX_REVERSEDUP:
      value_mode = "multi-reverse";
      break;
    case MDBX_DUPSORT | MDBX_DUPFIXED:
      value_mode = "multi-samelength";
      break;
    case MDBX_DUPSORT | MDBX_DUPFIXED | MDBX_REVERSEDUP:
      value_mode = "multi-reverse-samelength";
      break;
    case MDBX_DUPSORT | MDBX_DUPFIXED | MDBX_INTEGERDUP:
      value_mode = "multi-ordinal";
      break;
    case MDBX_DUPSORT | MDBX_INTEGERDUP | MDBX_REVERSEDUP:
      value_mode = "multi-msgpack";
      break;
    case MDBX_DUPSORT | MDBX_DUPFIXED | MDBX_INTEGERDUP | MDBX_REVERSEDUP:
      value_mode = "reserved";
      break;
    default:
      value_mode = "inconsistent";
      chk_scope_issue(scope, "wrong value-mode (0x%x)",
                      tbl->flags & (MDBX_DUPSORT | MDBX_REVERSEDUP | MDBX_DUPFIXED | MDBX_INTEGERDUP));
    }

    MDBX_chk_line_t *line = chk_line_begin(scope, MDBX_chk_info);
    line = chk_print(line, "key-value kind: %s-key => %s-value", key_mode, value_mode);
    line = chk_print(line, ", flags:");
    if (!tbl->flags)
      line = chk_print(line, " none");
    else {
      const uint8_t f[] = {
          MDBX_DUPSORT, MDBX_INTEGERKEY, MDBX_REVERSEKEY, MDBX_DUPFIXED, MDBX_REVERSEDUP, MDBX_INTEGERDUP, 0};
      const char *const t[] = {"dupsort", "integerkey", "reversekey", "dupfix", "reversedup", "integerdup"};
      for (size_t i = 0; f[i]; i++)
        if (tbl->flags & f[i])
          line = chk_print(line, " %s", t[i]);
    }
    chk_line_end(chk_print(line, " (0x%02X)", tbl->flags));

    line = chk_print(chk_line_begin(scope, MDBX_chk_verbose), "entries %" PRIu64 ", sequence %" PRIu64, db->items,
                     db->sequence);
    if (db->mod_txnid)
      line = chk_print(line, ", last modification txn#%" PRIaTXN, db->mod_txnid);
    if (db->root != P_INVALID)
      line = chk_print(line, ", root #%" PRIaPGNO, db->root);
    chk_line_end(line);
    chk_line_end(chk_print(chk_line_begin(scope, MDBX_chk_verbose),
                           "b-tree depth %u, pages: branch %" PRIaPGNO ", leaf %" PRIaPGNO ", large %" PRIaPGNO,
                           db->height, db->branch_pages, db->leaf_pages, db->large_pages));

    if ((chk->flags & MDBX_CHK_SKIP_BTREE_TRAVERSAL) == 0) {
      const size_t branch_pages = tbl->pages.branch + tbl->pages.nested_branch;
      const size_t leaf_pages = tbl->pages.leaf + tbl->pages.nested_leaf;
      const size_t subtotal_pages = db->branch_pages + db->leaf_pages + db->large_pages;
      if (subtotal_pages != tbl->pages.all)
        chk_scope_issue(scope, "%s pages mismatch (%" PRIuSIZE " != walked %" PRIuSIZE ")", "subtotal", subtotal_pages,
                        tbl->pages.all);
      if (db->branch_pages != branch_pages)
        chk_scope_issue(scope, "%s pages mismatch (%" PRIaPGNO " != walked %" PRIuSIZE ")", "branch", db->branch_pages,
                        branch_pages);
      if (db->leaf_pages != leaf_pages)
        chk_scope_issue(scope, "%s pages mismatch (%" PRIaPGNO " != walked %" PRIuSIZE ")", "all-leaf", db->leaf_pages,
                        leaf_pages);
      if (db->large_pages != tbl->histogram.large_pages.amount)
        chk_scope_issue(scope, "%s pages mismatch (%" PRIaPGNO " != walked %" PRIuSIZE ")", "large/overlow",
                        db->large_pages, tbl->histogram.large_pages.amount);
    }
  }

  err = mdbx_cursor_open(txn, dbi, &cursor);
  if (unlikely(err)) {
    chk_error_rc(scope, err, "mdbx_cursor_open");
    goto bailout;
  }
  if (chk->flags & MDBX_CHK_IGNORE_ORDER) {
    cursor->checking |= z_ignord | z_pagecheck;
    if (cursor->subcur)
      cursor->subcur->cursor.checking |= z_ignord | z_pagecheck;
  }

  const size_t maxkeysize = mdbx_env_get_maxkeysize_ex(env, tbl->flags);
  MDBX_val prev_key = {nullptr, 0}, prev_data = {nullptr, 0};
  MDBX_val key, data;
  size_t dups_count = 0;
  err = mdbx_cursor_get(cursor, &key, &data, MDBX_FIRST);
  while (err == MDBX_SUCCESS) {
    err = chk_check_break(scope);
    if (unlikely(err))
      goto bailout;

    bool bad_key = false;
    if (key.iov_len > maxkeysize) {
      chk_object_issue(scope, "entry", record_count, "key length exceeds max-key-size", "%" PRIuPTR " > %" PRIuPTR,
                       key.iov_len, maxkeysize);
      bad_key = true;
    } else if ((tbl->flags & MDBX_INTEGERKEY) && key.iov_len != 8 && key.iov_len != 4) {
      chk_object_issue(scope, "entry", record_count, "wrong key length", "%" PRIuPTR " != 4or8", key.iov_len);
      bad_key = true;
    }

    bool bad_data = false;
    if ((tbl->flags & MDBX_INTEGERDUP) && data.iov_len != 8 && data.iov_len != 4) {
      chk_object_issue(scope, "entry", record_count, "wrong data length", "%" PRIuPTR " != 4or8", data.iov_len);
      bad_data = true;
    }

    if (prev_key.iov_base) {
      if (key.iov_base == prev_key.iov_base)
        dups_count += 1;
      else {
        histogram_acc(dups_count, &tbl->histogram.multival);
        dups_count = 0;
      }
      if (prev_data.iov_base && !bad_data && (tbl->flags & MDBX_DUPFIXED) && prev_data.iov_len != data.iov_len) {
        chk_object_issue(scope, "entry", record_count, "different data length", "%" PRIuPTR " != %" PRIuPTR,
                         prev_data.iov_len, data.iov_len);
        bad_data = true;
      }

      if (!bad_key) {
        int cmp = mdbx_cmp(txn, dbi, &key, &prev_key);
        if (cmp == 0) {
          ++dups;
          if ((tbl->flags & MDBX_DUPSORT) == 0) {
            chk_object_issue(scope, "entry", record_count, "duplicated entries", nullptr);
            if (prev_data.iov_base && data.iov_len == prev_data.iov_len &&
                memcmp(data.iov_base, prev_data.iov_base, data.iov_len) == 0)
              chk_object_issue(scope, "entry", record_count, "complete duplicate", nullptr);
          } else if (!bad_data && prev_data.iov_base) {
            cmp = mdbx_dcmp(txn, dbi, &data, &prev_data);
            if (cmp == 0)
              chk_object_issue(scope, "entry", record_count, "complete duplicate", nullptr);
            else if (cmp < 0 && !(chk->flags & MDBX_CHK_IGNORE_ORDER))
              chk_object_issue(scope, "entry", record_count, "wrong order of multi-values", nullptr);
          }
        } else if (cmp < 0 && !(chk->flags & MDBX_CHK_IGNORE_ORDER))
          chk_object_issue(scope, "entry", record_count, "wrong order of entries", nullptr);
      }
    }

    if (!bad_key) {
      if (!prev_key.iov_base && (tbl->flags & MDBX_INTEGERKEY))
        chk_line_end(chk_print(chk_line_begin(scope, MDBX_chk_info), "fixed key-size %" PRIuSIZE, key.iov_len));
      prev_key = key;
    }
    if (!bad_data) {
      if (!prev_data.iov_base && (tbl->flags & (MDBX_INTEGERDUP | MDBX_DUPFIXED)))
        chk_line_end(chk_print(chk_line_begin(scope, MDBX_chk_info), "fixed data-size %" PRIuSIZE, data.iov_len));
      prev_data = data;
    }

    record_count++;
    histogram_acc(key.iov_len, &tbl->histogram.key_len);
    histogram_acc(data.iov_len, &tbl->histogram.val_len);

    const node_t *const node = page_node(cursor->pg[cursor->top], cursor->ki[cursor->top]);
    if (node_flags(node) == N_TREE) {
      if (dbi != MAIN_DBI || (tbl->flags & (MDBX_DUPSORT | MDBX_DUPFIXED | MDBX_REVERSEDUP | MDBX_INTEGERDUP)))
        chk_object_issue(scope, "entry", record_count, "unexpected table", "node-flags 0x%x", node_flags(node));
      else if (data.iov_len != sizeof(tree_t))
        chk_object_issue(scope, "entry", record_count, "wrong table node size", "node-size %" PRIuSIZE " != %" PRIuSIZE,
                         data.iov_len, sizeof(tree_t));
      else if (scope->stage == MDBX_chk_maindb)
        /* подсчитываем table при первом проходе */
        sub_databases += 1;
      else {
        /* обработка table при втором проходе */
        tree_t aligned_db;
        memcpy(&aligned_db, data.iov_base, sizeof(aligned_db));
        walk_tbl_t tbl_info = {.name = key};
        tbl_info.internal = &aligned_db;
        MDBX_chk_table_t *table;
        err = chk_get_tbl(scope, &tbl_info, &table);
        if (unlikely(err))
          goto bailout;
        if (table->cookie) {
          err = chk_scope_begin(chk, 0, MDBX_chk_tables, table, &usr->result.problems_kv, "Processing table %s...",
                                chk_v2a(chk, &table->name));
          if (likely(!err)) {
            err = chk_db(usr->scope, (MDBX_dbi)-1, table, chk_handle_kv);
            if (err != MDBX_EINTR && err != MDBX_RESULT_TRUE)
              usr->result.table_processed += 1;
          }
          err = chk_scope_restore(scope, err);
          if (unlikely(err))
            goto bailout;
        } else
          chk_line_end(chk_flush(chk_print(chk_line_begin(scope, MDBX_chk_processing), "Skip processing %s...",
                                           chk_v2a(chk, &table->name))));
      }
    } else if (handler) {
      err = handler(scope, tbl, record_count, &key, &data);
      if (unlikely(err))
        goto bailout;
    }

    err = mdbx_cursor_get(cursor, &key, &data, MDBX_NEXT);
  }

  if (prev_key.iov_base)
    histogram_acc(dups_count, &tbl->histogram.multival);

  err = (err != MDBX_NOTFOUND) ? chk_error_rc(scope, err, "mdbx_cursor_get") : MDBX_SUCCESS;
  if (err == MDBX_SUCCESS && record_count != db->items)
    chk_scope_issue(scope, "different number of entries %" PRIuSIZE " != %" PRIu64, record_count, db->items);
bailout:
  if (cursor) {
    if (handler) {
      if (record_count) {
        MDBX_chk_line_t *line = chk_line_begin(scope, MDBX_chk_info);
        line = histogram_dist(line, &tbl->histogram.key_len, "key length density", "0/1", false);
        chk_line_feed(line);
        line = histogram_dist(line, &tbl->histogram.val_len, "value length density", "0/1", false);
        if (tbl->histogram.multival.amount) {
          chk_line_feed(line);
          line = histogram_dist(line, &tbl->histogram.multival, "number of multi-values density", "single", false);
          chk_line_feed(line);
          line = chk_print(line, "number of keys %" PRIuSIZE ", average values per key %.1f",
                           tbl->histogram.multival.count, record_count / (double)tbl->histogram.multival.count);
        }
        chk_line_end(line);
      }
      if (scope->stage == MDBX_chk_maindb)
        usr->result.table_total = sub_databases;
      if (chk->cb->table_conclude)
        err = chk->cb->table_conclude(usr, tbl, cursor, err);
      MDBX_chk_line_t *line = chk_line_begin(scope, MDBX_chk_resolution);
      line = chk_print(line, "summary: %" PRIuSIZE " records,", record_count);
      if (dups || (tbl->flags & (MDBX_DUPSORT | MDBX_DUPFIXED | MDBX_REVERSEDUP | MDBX_INTEGERDUP)))
        line = chk_print(line, " %" PRIuSIZE " dups,", dups);
      if (sub_databases || dbi == MAIN_DBI)
        line = chk_print(line, " %" PRIuSIZE " tables,", sub_databases);
      line = chk_print(line,
                       " %" PRIuSIZE " key's bytes,"
                       " %" PRIuSIZE " data's bytes,"
                       " %" PRIuSIZE " problem(s)",
                       tbl->histogram.key_len.amount, tbl->histogram.val_len.amount, scope->subtotal_issues);
      chk_line_end(chk_flush(line));
    }

    mdbx_cursor_close(cursor);
    if (!txn->cursors[dbi] && (txn->dbi_state[dbi] & DBI_FRESH))
      mdbx_dbi_close(env, dbi);
  }
  return err;
}

__cold static int chk_handle_gc(MDBX_chk_scope_t *const scope, MDBX_chk_table_t *tbl, const size_t record_number,
                                const MDBX_val *key, const MDBX_val *data) {
  MDBX_chk_internal_t *const chk = scope->internal;
  MDBX_chk_context_t *const usr = chk->usr;
  assert(tbl == &chk->table_gc);
  (void)tbl;
  const char *bad = "";
  pgno_t *iptr = data->iov_base;

  if (key->iov_len != sizeof(txnid_t))
    chk_object_issue(scope, "entry", record_number, "wrong txn-id size", "key-size %" PRIuSIZE, key->iov_len);
  else {
    txnid_t txnid;
    memcpy(&txnid, key->iov_base, sizeof(txnid));
    if (txnid < 1 || txnid > usr->txn->txnid)
      chk_object_issue(scope, "entry", record_number, "wrong txn-id", "%" PRIaTXN, txnid);
    else {
      if (data->iov_len < sizeof(pgno_t) || data->iov_len % sizeof(pgno_t))
        chk_object_issue(scope, "entry", txnid, "wrong idl size", "%" PRIuPTR, data->iov_len);
      size_t number = (data->iov_len >= sizeof(pgno_t)) ? *iptr++ : 0;
      if (number > PAGELIST_LIMIT)
        chk_object_issue(scope, "entry", txnid, "wrong idl length", "%" PRIuPTR, number);
      else if ((number + 1) * sizeof(pgno_t) > data->iov_len) {
        chk_object_issue(scope, "entry", txnid, "trimmed idl", "%" PRIuSIZE " > %" PRIuSIZE " (corruption)",
                         (number + 1) * sizeof(pgno_t), data->iov_len);
        number = data->iov_len / sizeof(pgno_t) - 1;
      } else if (data->iov_len - (number + 1) * sizeof(pgno_t) >=
                 /* LY: allow gap up to two page. it is ok
                  * and better than shrink-and-retry inside gc_update() */
                 usr->env->ps * 2)
        chk_object_issue(scope, "entry", txnid, "extra idl space",
                         "%" PRIuSIZE " < %" PRIuSIZE " (minor, not a trouble)", (number + 1) * sizeof(pgno_t),
                         data->iov_len);

      usr->result.gc_pages += number;
      if (chk->envinfo.mi_latter_reader_txnid > txnid)
        usr->result.reclaimable_pages += number;

      size_t prev = MDBX_PNL_ASCENDING ? NUM_METAS - 1 : usr->txn->geo.first_unallocated;
      size_t span = 1;
      for (size_t i = 0; i < number; ++i) {
        const size_t pgno = iptr[i];
        if (pgno < NUM_METAS)
          chk_object_issue(scope, "entry", txnid, "wrong idl entry", "pgno %" PRIuSIZE " < meta-pages %u", pgno,
                           NUM_METAS);
        else if (pgno >= usr->result.backed_pages)
          chk_object_issue(scope, "entry", txnid, "wrong idl entry", "pgno %" PRIuSIZE " > backed-pages %" PRIuSIZE,
                           pgno, usr->result.backed_pages);
        else if (pgno >= usr->result.alloc_pages)
          chk_object_issue(scope, "entry", txnid, "wrong idl entry", "pgno %" PRIuSIZE " > alloc-pages %" PRIuSIZE,
                           pgno, usr->result.alloc_pages - 1);
        else {
          if (MDBX_PNL_DISORDERED(prev, pgno)) {
            bad = " [bad sequence]";
            chk_object_issue(scope, "entry", txnid, "bad sequence", "%" PRIuSIZE " %c [%" PRIuSIZE "].%" PRIuSIZE, prev,
                             (prev == pgno) ? '=' : (MDBX_PNL_ASCENDING ? '>' : '<'), i, pgno);
          }
          if (chk->pagemap) {
            const intptr_t id = chk->pagemap[pgno];
            if (id == 0)
              chk->pagemap[pgno] = -1 /* mark the pgno listed in GC */;
            else if (id > 0) {
              assert(id - 1 <= (intptr_t)ARRAY_LENGTH(chk->table));
              chk_object_issue(scope, "page", pgno, "already used", "by %s", chk_v2a(chk, &chk->table[id - 1]->name));
            } else
              chk_object_issue(scope, "page", pgno, "already listed in GC", nullptr);
          }
        }
        prev = pgno;
        while (i + span < number &&
               iptr[i + span] == (MDBX_PNL_ASCENDING ? pgno_add(pgno, span) : pgno_sub(pgno, span)))
          ++span;
      }
      if (tbl->cookie) {
        chk_line_end(chk_print(chk_line_begin(scope, MDBX_chk_details),
                               "transaction %" PRIaTXN ", %" PRIuSIZE " pages, maxspan %" PRIuSIZE "%s", txnid, number,
                               span, bad));
        for (size_t i = 0; i < number; i += span) {
          const size_t pgno = iptr[i];
          for (span = 1; i + span < number &&
                         iptr[i + span] == (MDBX_PNL_ASCENDING ? pgno_add(pgno, span) : pgno_sub(pgno, span));
               ++span)
            ;
          histogram_acc(span, &tbl->histogram.nested_tree);
          MDBX_chk_line_t *line = chk_line_begin(scope, MDBX_chk_extra);
          if (line) {
            if (span > 1)
              line = chk_print(line, "%9" PRIuSIZE "[%" PRIuSIZE "]", pgno, span);
            else
              line = chk_print(line, "%9" PRIuSIZE, pgno);
            chk_line_end(line);
            int err = chk_check_break(scope);
            if (err)
              return err;
          }
        }
      }
    }
  }
  return chk_check_break(scope);
}

__cold static int env_chk(MDBX_chk_scope_t *const scope) {
  MDBX_chk_internal_t *const chk = scope->internal;
  MDBX_chk_context_t *const usr = chk->usr;
  MDBX_env *const env = usr->env;
  MDBX_txn *const txn = usr->txn;
  int err = env_info(env, txn, &chk->envinfo, sizeof(chk->envinfo), &chk->troika);
  if (unlikely(err))
    return chk_error_rc(scope, err, "env_info");

  MDBX_chk_line_t *line =
      chk_puts(chk_line_begin(scope, MDBX_chk_info - (1 << MDBX_chk_severity_prio_shift)), "dxb-id ");
  if (chk->envinfo.mi_dxbid.x | chk->envinfo.mi_dxbid.y)
    line = chk_print(line, "%016" PRIx64 "-%016" PRIx64, chk->envinfo.mi_dxbid.x, chk->envinfo.mi_dxbid.y);
  else
    line = chk_puts(line, "is absent");
  chk_line_end(line);

  line = chk_puts(chk_line_begin(scope, MDBX_chk_info), "current boot-id ");
  if (chk->envinfo.mi_bootid.current.x | chk->envinfo.mi_bootid.current.y)
    line = chk_print(line, "%016" PRIx64 "-%016" PRIx64, chk->envinfo.mi_bootid.current.x,
                     chk->envinfo.mi_bootid.current.y);
  else
    line = chk_puts(line, "is unavailable");
  chk_line_end(line);

  err = osal_filesize(env->lazy_fd, &env->dxb_mmap.filesize);
  if (unlikely(err))
    return chk_error_rc(scope, err, "osal_filesize");

  //--------------------------------------------------------------------------

  err = chk_scope_begin(chk, 1, MDBX_chk_meta, nullptr, &usr->result.problems_meta, "Peek the meta-pages...");
  if (likely(!err)) {
    MDBX_chk_scope_t *const inner = usr->scope;
    const uint64_t dxbfile_pages = env->dxb_mmap.filesize >> env->ps2ln;
    usr->result.alloc_pages = txn->geo.first_unallocated;
    usr->result.backed_pages = bytes2pgno(env, env->dxb_mmap.current);
    if (unlikely(usr->result.backed_pages > dxbfile_pages))
      chk_scope_issue(inner, "backed-pages %zu > file-pages %" PRIu64, usr->result.backed_pages, dxbfile_pages);
    if (unlikely(dxbfile_pages < NUM_METAS))
      chk_scope_issue(inner, "file-pages %" PRIu64 " < %u", dxbfile_pages, NUM_METAS);
    if (unlikely(usr->result.backed_pages < NUM_METAS))
      chk_scope_issue(inner, "backed-pages %zu < %u", usr->result.backed_pages, NUM_METAS);
    if (unlikely(usr->result.backed_pages < NUM_METAS)) {
      chk_scope_issue(inner, "backed-pages %zu < num-metas %u", usr->result.backed_pages, NUM_METAS);
      return MDBX_CORRUPTED;
    }
    if (unlikely(dxbfile_pages < NUM_METAS)) {
      chk_scope_issue(inner, "backed-pages %zu < num-metas %u", usr->result.backed_pages, NUM_METAS);
      return MDBX_CORRUPTED;
    }
    if (unlikely(usr->result.backed_pages > (size_t)MAX_PAGENO + 1)) {
      chk_scope_issue(inner, "backed-pages %zu > max-pages %zu", usr->result.backed_pages, (size_t)MAX_PAGENO + 1);
      usr->result.backed_pages = MAX_PAGENO + 1;
    }

    if ((env->flags & (MDBX_EXCLUSIVE | MDBX_RDONLY)) != MDBX_RDONLY) {
      if (unlikely(usr->result.backed_pages > dxbfile_pages)) {
        chk_scope_issue(inner, "backed-pages %zu > file-pages %" PRIu64, usr->result.backed_pages, dxbfile_pages);
        usr->result.backed_pages = (size_t)dxbfile_pages;
      }
      if (unlikely(usr->result.alloc_pages > usr->result.backed_pages)) {
        chk_scope_issue(scope, "alloc-pages %zu > backed-pages %zu", usr->result.alloc_pages, usr->result.backed_pages);
        usr->result.alloc_pages = usr->result.backed_pages;
      }
    } else {
      /* DB may be shrunk by writer down to the allocated (but unused) pages. */
      if (unlikely(usr->result.alloc_pages > usr->result.backed_pages)) {
        chk_scope_issue(inner, "alloc-pages %zu > backed-pages %zu", usr->result.alloc_pages, usr->result.backed_pages);
        usr->result.alloc_pages = usr->result.backed_pages;
      }
      if (unlikely(usr->result.alloc_pages > dxbfile_pages)) {
        chk_scope_issue(inner, "alloc-pages %zu > file-pages %" PRIu64, usr->result.alloc_pages, dxbfile_pages);
        usr->result.alloc_pages = (size_t)dxbfile_pages;
      }
      if (unlikely(usr->result.backed_pages > dxbfile_pages))
        usr->result.backed_pages = (size_t)dxbfile_pages;
    }

    line = chk_line_feed(chk_print(chk_line_begin(inner, MDBX_chk_info),
                                   "pagesize %u (%u system), max keysize %u..%u"
                                   ", max readers %u",
                                   env->ps, globals.sys_pagesize, mdbx_env_get_maxkeysize_ex(env, MDBX_DUPSORT),
                                   mdbx_env_get_maxkeysize_ex(env, MDBX_DB_DEFAULTS), env->max_readers));
    line = chk_line_feed(chk_print_size(line, "mapsize ", env->dxb_mmap.current, nullptr));
    if (txn->geo.lower == txn->geo.upper)
      line = chk_print_size(line, "fixed datafile: ", chk->envinfo.mi_geo.current, nullptr);
    else {
      line = chk_print_size(line, "dynamic datafile: ", chk->envinfo.mi_geo.lower, nullptr);
      line = chk_print_size(line, " .. ", chk->envinfo.mi_geo.upper, ", ");
      line = chk_print_size(line, "+", chk->envinfo.mi_geo.grow, ", ");

      line = chk_line_feed(chk_print_size(line, "-", chk->envinfo.mi_geo.shrink, nullptr));
      line = chk_print_size(line, "current datafile: ", chk->envinfo.mi_geo.current, nullptr);
    }
    tASSERT(txn, txn->geo.now == chk->envinfo.mi_geo.current / chk->envinfo.mi_dxb_pagesize);
    chk_line_end(chk_print(line, ", %u pages", txn->geo.now));
#if defined(_WIN32) || defined(_WIN64) || MDBX_DEBUG
    if (txn->geo.shrink_pv && txn->geo.now != txn->geo.upper && scope->verbosity >= MDBX_chk_verbose) {
      line = chk_line_begin(inner, MDBX_chk_notice);
      chk_line_feed(chk_print(line, " > WARNING: Due Windows system limitations a file couldn't"));
      chk_line_feed(chk_print(line, " > be truncated while the database is opened. So, the size"));
      chk_line_feed(chk_print(line, " > database file of may by large than the database itself,"));
      chk_line_end(chk_print(line, " > until it will be closed or reopened in read-write mode."));
    }
#endif /* Windows || Debug */
    chk_verbose_meta(inner, 0);
    chk_verbose_meta(inner, 1);
    chk_verbose_meta(inner, 2);

    if (env->stuck_meta >= 0) {
      chk_line_end(chk_print(chk_line_begin(inner, MDBX_chk_processing),
                             "skip checking meta-pages since the %u"
                             " is selected for verification",
                             env->stuck_meta));
      line = chk_line_feed(chk_print(chk_line_begin(inner, MDBX_chk_resolution),
                                     "transactions: recent %" PRIu64 ", "
                                     "selected for verification %" PRIu64 ", lag %" PRIi64,
                                     chk->envinfo.mi_recent_txnid, chk->envinfo.mi_meta_txnid[env->stuck_meta],
                                     chk->envinfo.mi_recent_txnid - chk->envinfo.mi_meta_txnid[env->stuck_meta]));
      chk_line_end(line);
    } else {
      chk_line_end(chk_puts(chk_line_begin(inner, MDBX_chk_verbose), "performs check for meta-pages clashes"));
      const unsigned meta_clash_mask = meta_eq_mask(&chk->troika);
      if (meta_clash_mask & 1)
        chk_scope_issue(inner, "meta-%d and meta-%d are clashed", 0, 1);
      if (meta_clash_mask & 2)
        chk_scope_issue(inner, "meta-%d and meta-%d are clashed", 1, 2);
      if (meta_clash_mask & 4)
        chk_scope_issue(inner, "meta-%d and meta-%d are clashed", 2, 0);

      const unsigned prefer_steady_metanum = chk->troika.prefer_steady;
      const uint64_t prefer_steady_txnid = chk->troika.txnid[prefer_steady_metanum];
      const unsigned recent_metanum = chk->troika.recent;
      const uint64_t recent_txnid = chk->troika.txnid[recent_metanum];
      if (env->flags & MDBX_EXCLUSIVE) {
        chk_line_end(
            chk_puts(chk_line_begin(inner, MDBX_chk_verbose), "performs full check recent-txn-id with meta-pages"));
        eASSERT(env, recent_txnid == chk->envinfo.mi_recent_txnid);
        if (prefer_steady_txnid != recent_txnid) {
          if ((chk->flags & MDBX_CHK_READWRITE) != 0 && (env->flags & MDBX_RDONLY) == 0 &&
              recent_txnid > prefer_steady_txnid &&
              (chk->envinfo.mi_bootid.current.x | chk->envinfo.mi_bootid.current.y) != 0 &&
              chk->envinfo.mi_bootid.current.x == chk->envinfo.mi_bootid.meta[recent_metanum].x &&
              chk->envinfo.mi_bootid.current.y == chk->envinfo.mi_bootid.meta[recent_metanum].y) {
            chk_line_end(chk_print(chk_line_begin(inner, MDBX_chk_verbose),
                                   "recent meta-%u is weak, but boot-id match current"
                                   " (will synced upon successful check)",
                                   recent_metanum));
          } else
            chk_scope_issue(inner, "steady meta-%d txn-id mismatch recent-txn-id (%" PRIi64 " != %" PRIi64 ")",
                            prefer_steady_metanum, prefer_steady_txnid, recent_txnid);
        }
      } else if (chk->write_locked) {
        chk_line_end(chk_puts(chk_line_begin(inner, MDBX_chk_verbose),
                              "performs lite check recent-txn-id with meta-pages (not a "
                              "monopolistic mode)"));
        if (recent_txnid != chk->envinfo.mi_recent_txnid) {
          chk_scope_issue(inner, "weak meta-%d txn-id mismatch recent-txn-id (%" PRIi64 " != %" PRIi64 ")",
                          recent_metanum, recent_txnid, chk->envinfo.mi_recent_txnid);
        }
      } else {
        chk_line_end(chk_puts(chk_line_begin(inner, MDBX_chk_verbose),
                              "skip check recent-txn-id with meta-pages (monopolistic or "
                              "read-write mode only)"));
      }

      chk_line_end(chk_print(chk_line_begin(inner, MDBX_chk_resolution),
                             "transactions: recent %" PRIu64 ", latter reader %" PRIu64 ", lag %" PRIi64,
                             chk->envinfo.mi_recent_txnid, chk->envinfo.mi_latter_reader_txnid,
                             chk->envinfo.mi_recent_txnid - chk->envinfo.mi_latter_reader_txnid));
    }
  }
  err = chk_scope_restore(scope, err);

  //--------------------------------------------------------------------------

  const char *const subj_tree = "B-Trees";
  if (chk->flags & MDBX_CHK_SKIP_BTREE_TRAVERSAL)
    chk_line_end(chk_print(chk_line_begin(scope, MDBX_chk_processing), "Skipping %s traversal...", subj_tree));
  else {
    err = chk_scope_begin(chk, -1, MDBX_chk_tree, nullptr, &usr->result.tree_problems,
                          "Traversal %s by txn#%" PRIaTXN "...", subj_tree, txn->txnid);
    if (likely(!err))
      err = chk_tree(usr->scope);
    if (usr->result.tree_problems && usr->result.gc_tree_problems == 0)
      usr->result.gc_tree_problems = usr->result.tree_problems;
    if (usr->result.tree_problems && usr->result.kv_tree_problems == 0)
      usr->result.kv_tree_problems = usr->result.tree_problems;
    chk_scope_restore(scope, err);
  }

  const char *const subj_gc = chk_v2a(chk, MDBX_CHK_GC);
  if (usr->result.gc_tree_problems > 0)
    chk_line_end(chk_print(chk_line_begin(scope, MDBX_chk_processing),
                           "Skip processing %s since %s is corrupted (%" PRIuSIZE " problem(s))", subj_gc, subj_tree,
                           usr->result.problems_gc = usr->result.gc_tree_problems));
  else {
    err = chk_scope_begin(chk, -1, MDBX_chk_gc, &chk->table_gc, &usr->result.problems_gc,
                          "Processing %s by txn#%" PRIaTXN "...", subj_gc, txn->txnid);
    if (likely(!err))
      err = chk_db(usr->scope, FREE_DBI, &chk->table_gc, chk_handle_gc);
    line = chk_line_begin(scope, MDBX_chk_info);
    if (line) {
      histogram_print(scope, line, &chk->table_gc.histogram.nested_tree, "span(s)", "single", false);
      chk_line_end(line);
    }
    if (usr->result.problems_gc == 0 && (chk->flags & MDBX_CHK_SKIP_BTREE_TRAVERSAL) == 0) {
      const size_t used_pages = usr->result.alloc_pages - usr->result.gc_pages;
      if (usr->result.processed_pages != used_pages)
        chk_scope_issue(usr->scope, "used pages mismatch (%" PRIuSIZE "(walked) != %" PRIuSIZE "(allocated - GC))",
                        usr->result.processed_pages, used_pages);
      if (usr->result.unused_pages != usr->result.gc_pages)
        chk_scope_issue(usr->scope, "GC pages mismatch (%" PRIuSIZE "(expected) != %" PRIuSIZE "(GC))",
                        usr->result.unused_pages, usr->result.gc_pages);
    }
  }
  chk_scope_restore(scope, err);

  //--------------------------------------------------------------------------

  err = chk_scope_begin(chk, 1, MDBX_chk_space, nullptr, nullptr, "Page allocation:");
  const double percent_boundary_reciprocal = 100.0 / txn->geo.upper;
  const double percent_backed_reciprocal = 100.0 / usr->result.backed_pages;
  const size_t detained = usr->result.gc_pages - usr->result.reclaimable_pages;
  const size_t available2boundary = txn->geo.upper - usr->result.alloc_pages + usr->result.reclaimable_pages;
  const size_t available2backed = usr->result.backed_pages - usr->result.alloc_pages + usr->result.reclaimable_pages;
  const size_t remained2boundary = txn->geo.upper - usr->result.alloc_pages;
  const size_t remained2backed = usr->result.backed_pages - usr->result.alloc_pages;

  const size_t used = (chk->flags & MDBX_CHK_SKIP_BTREE_TRAVERSAL) ? usr->result.alloc_pages - usr->result.gc_pages
                                                                   : usr->result.processed_pages;

  line = chk_line_begin(usr->scope, MDBX_chk_info);
  line = chk_print(line,
                   "backed by file: %" PRIuSIZE " pages (%.1f%%)"
                   ", %" PRIuSIZE " left to boundary (%.1f%%)",
                   usr->result.backed_pages, usr->result.backed_pages * percent_boundary_reciprocal,
                   txn->geo.upper - usr->result.backed_pages,
                   (txn->geo.upper - usr->result.backed_pages) * percent_boundary_reciprocal);
  line = chk_line_feed(line);

  line = chk_print(line, "%s: %" PRIuSIZE " page(s), %.1f%% of backed, %.1f%% of boundary", "used", used,
                   used * percent_backed_reciprocal, used * percent_boundary_reciprocal);
  line = chk_line_feed(line);

  line = chk_print(line, "%s: %" PRIuSIZE " page(s) (%.1f%%) of backed, %" PRIuSIZE " to boundary (%.1f%% of boundary)",
                   "remained", remained2backed, remained2backed * percent_backed_reciprocal, remained2boundary,
                   remained2boundary * percent_boundary_reciprocal);
  line = chk_line_feed(line);

  line =
      chk_print(line,
                "reclaimable: %" PRIuSIZE " (%.1f%% of backed, %.1f%% of boundary)"
                ", GC %" PRIuSIZE " (%.1f%% of backed, %.1f%% of boundary)",
                usr->result.reclaimable_pages, usr->result.reclaimable_pages * percent_backed_reciprocal,
                usr->result.reclaimable_pages * percent_boundary_reciprocal, usr->result.gc_pages,
                usr->result.gc_pages * percent_backed_reciprocal, usr->result.gc_pages * percent_boundary_reciprocal);
  line = chk_line_feed(line);

  line = chk_print(line,
                   "detained by reader(s): %" PRIuSIZE " (%.1f%% of backed, %.1f%% of boundary)"
                   ", %u reader(s), lag %" PRIi64,
                   detained, detained * percent_backed_reciprocal, detained * percent_boundary_reciprocal,
                   chk->envinfo.mi_numreaders, chk->envinfo.mi_recent_txnid - chk->envinfo.mi_latter_reader_txnid);
  line = chk_line_feed(line);

  line = chk_print(line, "%s: %" PRIuSIZE " page(s), %.1f%% of backed, %.1f%% of boundary", "allocated",
                   usr->result.alloc_pages, usr->result.alloc_pages * percent_backed_reciprocal,
                   usr->result.alloc_pages * percent_boundary_reciprocal);
  line = chk_line_feed(line);

  line = chk_print(line, "%s: %" PRIuSIZE " page(s) (%.1f%%) of backed, %" PRIuSIZE " to boundary (%.1f%% of boundary)",
                   "available", available2backed, available2backed * percent_backed_reciprocal, available2boundary,
                   available2boundary * percent_boundary_reciprocal);
  chk_line_end(line);

  line = chk_line_begin(usr->scope, MDBX_chk_resolution);
  line = chk_print(line, "%s %" PRIaPGNO " pages", (txn->geo.upper == txn->geo.now) ? "total" : "upto", txn->geo.upper);
  line = chk_print(line, ", backed %" PRIuSIZE " (%.1f%%)", usr->result.backed_pages,
                   usr->result.backed_pages * percent_boundary_reciprocal);
  line = chk_print(line, ", allocated %" PRIuSIZE " (%.1f%%)", usr->result.alloc_pages,
                   usr->result.alloc_pages * percent_boundary_reciprocal);
  line = chk_print(line, ", available %" PRIuSIZE " (%.1f%%)", available2boundary,
                   available2boundary * percent_boundary_reciprocal);
  chk_line_end(line);
  chk_scope_restore(scope, err);

  //--------------------------------------------------------------------------

  const char *const subj_main = chk_v2a(chk, MDBX_CHK_MAIN);
  if (chk->flags & MDBX_CHK_SKIP_KV_TRAVERSAL)
    chk_line_end(chk_print(chk_line_begin(scope, MDBX_chk_processing), "Skip processing %s...", subj_main));
  else if ((usr->result.problems_kv = usr->result.kv_tree_problems) > 0)
    chk_line_end(chk_print(chk_line_begin(scope, MDBX_chk_processing),
                           "Skip processing %s since %s is corrupted (%" PRIuSIZE " problem(s))", subj_main, subj_tree,
                           usr->result.problems_kv = usr->result.kv_tree_problems));
  else {
    err = chk_scope_begin(chk, 0, MDBX_chk_maindb, &chk->table_main, &usr->result.problems_kv, "Processing %s...",
                          subj_main);
    if (likely(!err))
      err = chk_db(usr->scope, MAIN_DBI, &chk->table_main, chk_handle_kv);
    chk_scope_restore(scope, err);

    const char *const subj_tables = "table(s)";
    if (usr->result.problems_kv && usr->result.table_total)
      chk_line_end(chk_print(chk_line_begin(scope, MDBX_chk_processing), "Skip processing %s", subj_tables));
    else if (usr->result.problems_kv == 0 && usr->result.table_total == 0)
      chk_line_end(chk_print(chk_line_begin(scope, MDBX_chk_info), "No %s", subj_tables));
    else if (usr->result.problems_kv == 0 && usr->result.table_total) {
      err = chk_scope_begin(chk, 1, MDBX_chk_tables, nullptr, &usr->result.problems_kv,
                            "Processing %s by txn#%" PRIaTXN "...", subj_tables, txn->txnid);
      if (!err)
        err = chk_db(usr->scope, MAIN_DBI, &chk->table_main, nullptr);
      if (usr->scope->subtotal_issues)
        chk_line_end(chk_print(chk_line_begin(usr->scope, MDBX_chk_resolution),
                               "processed %" PRIuSIZE " of %" PRIuSIZE " %s, %" PRIuSIZE " problems(s)",
                               usr->result.table_processed, usr->result.table_total, subj_tables,
                               usr->scope->subtotal_issues));
    }
    chk_scope_restore(scope, err);
  }

  return chk_scope_end(chk, chk_scope_begin(chk, 0, MDBX_chk_conclude, nullptr, nullptr, nullptr));
}

__cold int mdbx_env_chk_encount_problem(MDBX_chk_context_t *ctx) {
  if (likely(ctx && ctx->internal && ctx->internal->usr == ctx && ctx->internal->problem_counter && ctx->scope)) {
    *ctx->internal->problem_counter += 1;
    ctx->scope->subtotal_issues += 1;
    return MDBX_SUCCESS;
  }
  return MDBX_EINVAL;
}

__cold int mdbx_env_chk(MDBX_env *env, const struct MDBX_chk_callbacks *cb, MDBX_chk_context_t *ctx,
                        const MDBX_chk_flags_t flags, MDBX_chk_severity_t verbosity, unsigned timeout_seconds_16dot16) {
  int err, rc = check_env(env, false);
  if (unlikely(rc != MDBX_SUCCESS))
    return LOG_IFERR(rc);
  if (unlikely(!cb || !ctx || ctx->internal))
    return LOG_IFERR(MDBX_EINVAL);

  MDBX_chk_internal_t *const chk = osal_calloc(1, sizeof(MDBX_chk_internal_t));
  if (unlikely(!chk))
    return LOG_IFERR(MDBX_ENOMEM);

  chk->cb = cb;
  chk->usr = ctx;
  chk->usr->internal = chk;
  chk->usr->env = env;
  chk->flags = flags;

  chk->table_gc.id = -1;
  chk->table_gc.name.iov_base = MDBX_CHK_GC;
  chk->table[FREE_DBI] = &chk->table_gc;

  chk->table_main.id = -1;
  chk->table_main.name.iov_base = MDBX_CHK_MAIN;
  chk->table[MAIN_DBI] = &chk->table_main;

  chk->monotime_timeout =
      timeout_seconds_16dot16 ? osal_16dot16_to_monotime(timeout_seconds_16dot16) + osal_monotime() : 0;
  chk->usr->scope_nesting = 0;
  chk->usr->result.tables = (const void *)&chk->table;

  MDBX_chk_scope_t *const top = chk->scope_stack;
  top->verbosity = verbosity;
  top->internal = chk;

  // init
  rc = chk_scope_end(chk, chk_scope_begin(chk, 0, MDBX_chk_init, nullptr, nullptr, nullptr));

  // lock
  if (likely(!rc))
    rc = chk_scope_begin(chk, 0, MDBX_chk_lock, nullptr, nullptr, "Taking %slock...",
                         (env->flags & (MDBX_RDONLY | MDBX_EXCLUSIVE)) ? "" : "read ");
  if (likely(!rc) && (env->flags & (MDBX_RDONLY | MDBX_EXCLUSIVE)) == 0 && (flags & MDBX_CHK_READWRITE)) {
    rc = mdbx_txn_lock(env, false);
    if (unlikely(rc))
      chk_error_rc(ctx->scope, rc, "mdbx_txn_lock");
    else
      chk->write_locked = true;
  }
  if (likely(!rc)) {
    rc = mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &ctx->txn);
    if (unlikely(rc))
      chk_error_rc(ctx->scope, rc, "mdbx_txn_begin");
  }
  chk_scope_end(chk, rc);

  // doit
  if (likely(!rc)) {
    chk->table_gc.flags = ctx->txn->dbs[FREE_DBI].flags;
    chk->table_main.flags = ctx->txn->dbs[MAIN_DBI].flags;
    rc = env_chk(top);
  }

  // unlock
  if (ctx->txn || chk->write_locked) {
    chk_scope_begin(chk, 0, MDBX_chk_unlock, nullptr, nullptr, nullptr);
    if (ctx->txn) {
      err = mdbx_txn_abort(ctx->txn);
      if (err && !rc)
        rc = err;
      ctx->txn = nullptr;
    }
    if (chk->write_locked)
      mdbx_txn_unlock(env);
    rc = chk_scope_end(chk, rc);
  }

  // finalize
  err = chk_scope_begin(chk, 0, MDBX_chk_finalize, nullptr, nullptr, nullptr);
  rc = chk_scope_end(chk, err ? err : rc);
  chk_dispose(chk);
  return LOG_IFERR(rc);
}
