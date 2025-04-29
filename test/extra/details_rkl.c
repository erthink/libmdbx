/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2025

#define debug_log debug_log_sub

#include "../../src/rkl.c"
#include "../../src/txl.c"

MDBX_MAYBE_UNUSED __cold void debug_log_sub(int level, const char *function, int line, const char *fmt, ...) {
  (void)level;
  (void)function;
  (void)line;
  (void)fmt;
}

/*-----------------------------------------------------------------------------*/

static size_t tst_failed, tst_ok, tst_iterations, tst_cases, tst_cases_hole;
#ifndef NDEBUG
static size_t tst_target;
#endif

static bool check_bool(bool v, bool expect, const char *fn, unsigned line) {
  if (unlikely(v != expect)) {
    ++tst_failed;
    fflush(nullptr);
    fprintf(stderr, "iteration %zi: got %s, expected %s, at %s:%u\n", tst_iterations, v ? "true" : "false",
            expect ? "true" : "false", fn, line);
    fflush(nullptr);
    return false;
  }
  ++tst_ok;
  return true;
}

static bool check_eq(uint64_t v, uint64_t expect, const char *fn, unsigned line) {
  if (unlikely(v != expect)) {
    ++tst_failed;
    fflush(nullptr);
    fprintf(stderr, "iteration %zi: %" PRIu64 " (got) != %" PRIu64 " (expected), at %s:%u\n", tst_iterations, v, expect,
            fn, line);
    fflush(nullptr);
    return false;
  }
  ++tst_ok;
  return true;
}

#define CHECK_BOOL(T, EXPECT) check_bool((T), (EXPECT), __func__, __LINE__)
#define CHECK_TRUE(T) CHECK_BOOL(T, true)
#define CHECK_FALSE(T) CHECK_BOOL(T, false)
#define CHECK_EQ(T, EXPECT) check_eq((T), (EXPECT), __func__, __LINE__)

void trivia(void) {
  rkl_t x, y;

  rkl_init(&x);
  rkl_init(&y);
  CHECK_TRUE(rkl_check(&x));
  CHECK_TRUE(rkl_empty(&x));
  CHECK_EQ(rkl_len(&x), 0);

  rkl_iter_t f = rkl_iterator(&x, false);
  rkl_iter_t r = rkl_iterator(&x, true);
  CHECK_EQ(rkl_left(&f, false), 0);
  CHECK_EQ(rkl_left(&f, true), 0);
  CHECK_EQ(rkl_left(&r, false), 0);
  CHECK_EQ(rkl_left(&r, true), 0);
  CHECK_EQ(rkl_turn(&f, false), 0);
  CHECK_EQ(rkl_turn(&f, true), 0);
  CHECK_EQ(rkl_turn(&r, false), 0);
  CHECK_EQ(rkl_turn(&r, true), 0);
  CHECK_TRUE(rkl_check(&x));

  rkl_hole_t hole;
  hole = rkl_hole(&f, true);
  CHECK_EQ(hole.begin, 1);
  CHECK_EQ(hole.end, MAX_TXNID);
  hole = rkl_hole(&f, false);
  CHECK_EQ(hole.begin, 1);
  CHECK_EQ(hole.end, MAX_TXNID);
  hole = rkl_hole(&r, true);
  CHECK_EQ(hole.begin, 1);
  CHECK_EQ(hole.end, MAX_TXNID);
  hole = rkl_hole(&r, false);
  CHECK_EQ(hole.begin, 1);
  CHECK_EQ(hole.end, MAX_TXNID);

  CHECK_EQ(rkl_push(&x, 42, false), MDBX_SUCCESS);
  CHECK_TRUE(rkl_check(&x));
  CHECK_FALSE(rkl_empty(&x));
  CHECK_EQ(rkl_len(&x), 1);
  CHECK_EQ(rkl_push(&x, 42, true), MDBX_RESULT_TRUE);
  CHECK_TRUE(rkl_check(&x));

  f = rkl_iterator(&x, false);
  r = rkl_iterator(&x, true);
  CHECK_EQ(rkl_left(&f, false), 1);
  CHECK_EQ(rkl_left(&f, true), 0);
  CHECK_EQ(rkl_left(&r, false), 0);
  CHECK_EQ(rkl_left(&r, true), 1);

  CHECK_EQ(rkl_turn(&f, true), 0);
  CHECK_EQ(rkl_turn(&f, false), 42);
  CHECK_EQ(rkl_turn(&f, false), 0);
  CHECK_EQ(rkl_turn(&f, true), 42);
  CHECK_EQ(rkl_turn(&f, true), 0);

  CHECK_EQ(rkl_turn(&r, false), 0);
  CHECK_EQ(rkl_turn(&r, true), 42);
  CHECK_EQ(rkl_turn(&r, true), 0);
  CHECK_EQ(rkl_turn(&r, false), 42);
  CHECK_EQ(rkl_turn(&r, false), 0);

  f = rkl_iterator(&x, false);
  hole = rkl_hole(&f, false);
  CHECK_EQ(hole.begin, 43);
  CHECK_EQ(hole.end, MAX_TXNID);
  hole = rkl_hole(&f, false);
  CHECK_EQ(hole.begin, MAX_TXNID);
  CHECK_EQ(hole.end, MAX_TXNID);
  hole = rkl_hole(&f, true);
  CHECK_EQ(hole.begin, 43);
  CHECK_EQ(hole.end, MAX_TXNID);
  hole = rkl_hole(&f, true);
  CHECK_EQ(hole.begin, 1);
  CHECK_EQ(hole.end, 42);
  hole = rkl_hole(&f, true);
  CHECK_EQ(hole.begin, 1);
  CHECK_EQ(hole.end, 42);

  r = rkl_iterator(&x, true);
  hole = rkl_hole(&r, false);
  CHECK_EQ(hole.begin, MAX_TXNID);
  CHECK_EQ(hole.end, MAX_TXNID);
  hole = rkl_hole(&r, true);
  CHECK_EQ(hole.begin, 43);
  CHECK_EQ(hole.end, MAX_TXNID);
  hole = rkl_hole(&r, true);
  CHECK_EQ(hole.begin, 1);
  CHECK_EQ(hole.end, 42);
  hole = rkl_hole(&r, false);
  CHECK_EQ(hole.begin, 43);
  CHECK_EQ(hole.end, MAX_TXNID);
  hole = rkl_hole(&r, false);
  CHECK_EQ(hole.begin, MAX_TXNID);
  CHECK_EQ(hole.end, MAX_TXNID);

  rkl_resize(&x, 222);
  CHECK_FALSE(rkl_empty(&x));
  CHECK_TRUE(rkl_check(&x));

  rkl_destructive_move(&x, &y);
  CHECK_TRUE(rkl_check(&x));
  CHECK_TRUE(rkl_check(&y));
  rkl_destroy(&x);
  rkl_destroy(&y);
}

/*-----------------------------------------------------------------------------*/

uint64_t prng_state;

static uint64_t prng(void) {
  prng_state = prng_state * UINT64_C(6364136223846793005) + 1;
  return prng_state;
}

static bool flipcoin(void) { return (bool)prng() & 1; }

static bool stochastic_pass(const unsigned start, const unsigned width, const unsigned n) {
  rkl_t k, c;
  txl_t l = txl_alloc();
  if (!CHECK_TRUE(l))
    return false;

  rkl_init(&k);
  rkl_init(&c);
  const size_t errors = tst_failed;

  rkl_iter_t f = rkl_iterator(&k, false);
  rkl_iter_t r = rkl_iterator(&k, true);

  txnid_t lowest = UINT_MAX;
  txnid_t highest = 0;
  while (MDBX_PNL_GETSIZE(l) < n) {
    txnid_t id = (txnid_t)(prng() % width + start);
    if (id < MIN_TXNID || id >= INVALID_TXNID)
      continue;
    if (txl_contain(l, id)) {
      if (CHECK_TRUE(rkl_contain(&k, id)) && CHECK_EQ(rkl_push(&k, id, false), MDBX_RESULT_TRUE))
        continue;
      break;
    }
    if (!CHECK_FALSE(rkl_contain(&k, id)))
      break;

    if (tst_iterations % (1u << 24) == 0 && tst_iterations) {
      printf("done %.3fM iteration, %zu cases\n", tst_iterations / 1000000.0, tst_cases);
      fflush(nullptr);
    }
    tst_iterations += 1;

#ifndef NDEBUG
    if (tst_iterations == tst_target) {
      printf("reach %zu iteration\n", tst_iterations);
      fflush(nullptr);
    }
#endif

    if (!CHECK_EQ(rkl_push(&k, id, false), MDBX_SUCCESS))
      break;
    if (!CHECK_TRUE(rkl_check(&k)))
      break;
    if (!CHECK_EQ(txl_append(&l, id), MDBX_SUCCESS))
      break;
    if (!CHECK_TRUE(rkl_contain(&k, id)))
      break;

    lowest = (lowest < id) ? lowest : id;
    highest = (highest > id) ? highest : id;
    if (!CHECK_EQ(rkl_lowest(&k), lowest))
      break;
    if (!CHECK_EQ(rkl_highest(&k), highest))
      break;
  }

  txl_sort(l);
  CHECK_EQ(rkl_len(&k), n);
  CHECK_EQ(MDBX_PNL_GETSIZE(l), n);

  f = rkl_iterator(&k, false);
  r = rkl_iterator(&k, true);
  CHECK_EQ(rkl_left(&f, false), n);
  CHECK_EQ(rkl_left(&f, true), 0);
  CHECK_EQ(rkl_left(&r, false), 0);
  CHECK_EQ(rkl_left(&r, true), n);

  for (size_t i = 0; i < n; ++i) {
    CHECK_EQ(rkl_turn(&f, false), l[n - i]);
    CHECK_EQ(rkl_left(&f, false), n - i - 1);
    CHECK_EQ(rkl_left(&f, true), i + 1);

    CHECK_EQ(rkl_turn(&r, true), l[i + 1]);
    r.pos += 1;
    CHECK_EQ(rkl_turn(&r, true), l[i + 1]);
    CHECK_EQ(rkl_left(&r, true), n - i - 1);
    CHECK_EQ(rkl_left(&r, false), i + 1);
  }

  if (CHECK_EQ(rkl_copy(&k, &c), MDBX_SUCCESS)) {
    for (size_t i = 1; i <= n; ++i) {
      if (!CHECK_FALSE(rkl_empty(&k)))
        break;
      if (!CHECK_FALSE(rkl_empty(&c)))
        break;
      CHECK_EQ(rkl_pop(&k, true), l[i]);
      CHECK_EQ(rkl_pop(&c, false), l[1 + n - i]);
    }
  }

  CHECK_TRUE(rkl_empty(&k));
  CHECK_TRUE(rkl_empty(&c));

  rkl_destroy(&k);
  rkl_destroy(&c);
  txl_free(l);

  ++tst_cases;
  return errors == tst_failed;
}

static bool stochastic(const size_t limit_cases, const size_t limit_loops) {
  for (unsigned loop = 0; tst_cases < limit_cases || loop < limit_loops; ++loop)
    for (unsigned width = 2; width < 10; ++width)
      for (unsigned n = 1; n < width; ++n)
        for (unsigned prev = 1, start = 0, t; start < 4242; t = start + prev, prev = start, start = t)
          if (!stochastic_pass(start, 1u << width, 1u << n) || tst_failed > 42) {
            puts("bailout\n");
            return false;
          }
  return true;
}

/*-----------------------------------------------------------------------------*/

static bool bit(size_t set, size_t n) {
  assert(n < CHAR_BIT * sizeof(set));
  return (set >> n) & 1;
}

static size_t hamming_weight(size_t v) {
  const size_t m1 = (size_t)UINT64_C(0x5555555555555555);
  const size_t m2 = (size_t)UINT64_C(0x3333333333333333);
  const size_t m4 = (size_t)UINT64_C(0x0f0f0f0f0f0f0f0f);
  const size_t h01 = (size_t)UINT64_C(0x0101010101010101);
  v -= (v >> 1) & m1;
  v = (v & m2) + ((v >> 2) & m2);
  v = (v + (v >> 4)) & m4;
  return (v * h01) >> (sizeof(v) * 8 - 8);
}

static bool check_hole(const size_t set, const rkl_hole_t hole, size_t *acc) {
  const size_t errors = tst_failed;
  ++tst_iterations;

  if (hole.begin > 1)
    CHECK_EQ(bit(set, hole.begin - 1), 1);
  if (hole.end < CHAR_BIT * sizeof(set))
    CHECK_EQ(bit(set, hole.end), 1);

  for (size_t n = hole.begin; n < hole.end && n < CHAR_BIT * sizeof(set); n++) {
    CHECK_EQ(bit(set, n), 0);
    *acc += 1;
  }

  return errors == tst_failed;
}

static void debug_set(const size_t set, const char *str, int iter_offset) {
#if 1
  (void)set;
  (void)str;
  (void)iter_offset;
#else
  printf("\ncase %s+%d: count %zu, holes", str, iter_offset, hamming_weight(~set) - 1);
  for (size_t k, i = 1; i < CHAR_BIT * sizeof(set); ++i) {
    if (!bit(set, i)) {
      printf(" %zu", i);
      for (k = i; k < CHAR_BIT * sizeof(set) - 1 && !bit(set, k + 1); ++k)
        ;
      if (k > i) {
        printf("-%zu", k);
        i = k;
      }
    }
  }
  printf("\n");
  fflush(nullptr);
#endif
}

static bool check_holes_bothsides(const size_t set, rkl_iter_t const *i) {
  const size_t number_of_holes = hamming_weight(~set) - 1;
  size_t acc = 0;

  rkl_iter_t f = *i;
  for (;;) {
    rkl_hole_t hole = rkl_hole(&f, false);
    if (hole.begin == hole.end)
      break;
    if (!check_hole(set, hole, &acc))
      return false;
    if (hole.end >= CHAR_BIT * sizeof(set))
      break;
  }

  rkl_iter_t b = *i;
  for (;;) {
    rkl_hole_t hole = rkl_hole(&b, true);
    if (hole.begin == hole.end)
      break;
    if (!check_hole(set, hole, &acc))
      return false;
    if (hole.begin == 1)
      break;
  }

  if (!CHECK_EQ(acc, number_of_holes))
    return false;

  return true;
}

static bool check_holes_fourways(const size_t set, const rkl_t *rkl) {
  rkl_iter_t i = rkl_iterator(rkl, false);
  int o = 0;
  do {
    debug_set(set, "initial-forward", o++);
    if (!check_holes_bothsides(set, &i))
      return false;
  } while (rkl_turn(&i, false));

  do {
    debug_set(set, "recoil-reverse", --o);
    if (!check_holes_bothsides(set, &i))
      return false;
  } while (rkl_turn(&i, true));

  i = rkl_iterator(rkl, true);
  o = 0;
  do {
    debug_set(set, "initial-reverse", --o);
    if (!check_holes_bothsides(set, &i))
      return false;
  } while (rkl_turn(&i, false));

  do {
    debug_set(set, "recoil-forward", o++);
    if (!check_holes_bothsides(set, &i))
      return false;
  } while (rkl_turn(&i, true));

  return true;
}

static bool stochastic_pass_hole(size_t set, size_t trims) {
  const size_t one = 1;
  set &= ~one;
  if (!set)
    return true;

  ++tst_cases_hole;

  rkl_t rkl;
  rkl_init(&rkl);
  for (size_t n = 1; n < CHAR_BIT * sizeof(set); ++n)
    if (bit(set, n))
      CHECK_EQ(rkl_push(&rkl, n, false), MDBX_SUCCESS);

  if (!check_holes_fourways(set, &rkl))
    return false;

  while (rkl_len(&rkl) > 1 && trims-- > 0) {
    if (flipcoin()) {
      const size_t l = (size_t)rkl_pop(&rkl, false);
      if (l == 0)
        break;
      assert(bit(set, l));
      set -= one << l;
      if (!check_holes_fourways(set, &rkl))
        return false;
    } else {

      const size_t h = (size_t)rkl_pop(&rkl, true);
      if (h == 0)
        break;
      assert(bit(set, h));
      set -= one << h;
      if (!check_holes_fourways(set, &rkl))
        return false;
    }
  }

  return true;
}

static size_t prng_word(void) {
  size_t word = (size_t)(prng() >> 32);
  if (sizeof(word) > 4)
    word = (uint64_t)word << 32 | (size_t)(prng() >> 32);
  return word;
}

static bool stochastic_hole(size_t probes) {
  for (size_t n = 0; n < probes; ++n) {
    size_t set = prng_word();
    if (!stochastic_pass_hole(set, prng() % 11))
      return false;
    if (!stochastic_pass_hole(set & prng_word(), prng() % 11))
      return false;
    if (!stochastic_pass_hole(set | prng_word(), prng() % 11))
      return false;
  }
  return true;
}

/*-----------------------------------------------------------------------------*/

int main(int argc, const char *argv[]) {
  (void)argc;
  (void)argv;

#ifndef NDEBUG
  // tst_target = 281870;
#endif
  prng_state = (uint64_t)time(nullptr);
  printf("prng-seed %" PRIu64 "\n", prng_state);
  fflush(nullptr);

  trivia();
  stochastic(42 * 42 * 42, 42);
  stochastic_hole(24 * 24 * 24);
  printf("done: %zu+%zu cases, %zu iterations, %zu checks ok, %zu checks failed\n", tst_cases, tst_cases_hole,
         tst_iterations, tst_ok, tst_failed);
  fflush(nullptr);
  return tst_failed ? EXIT_FAILURE : EXIT_SUCCESS;
}
