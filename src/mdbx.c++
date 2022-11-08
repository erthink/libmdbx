//
// Copyright (c) 2020-2022, Leonid Yuriev <leo@yuriev.ru>.
// SPDX-License-Identifier: Apache-2.0
//
// Non-inline part of the libmdbx C++ API
//

#if defined(_MSC_VER) && !defined(_CRT_SECURE_NO_WARNINGS)
#define _CRT_SECURE_NO_WARNINGS
#endif /* _CRT_SECURE_NO_WARNINGS */

#if (defined(__MINGW__) || defined(__MINGW32__) || defined(__MINGW64__)) &&    \
    !defined(__USE_MINGW_ANSI_STDIO)
#define __USE_MINGW_ANSI_STDIO 1
#endif /* MinGW */

#include "../mdbx.h++"

#include "internals.h"

#include <array>
#include <atomic>
#include <cctype> // for isxdigit(), etc
#include <system_error>

namespace {

#if 0 /* Unused for now */

class trouble_location {

#ifndef TROUBLE_PROVIDE_LINENO
#define TROUBLE_PROVIDE_LINENO 1
#endif

#ifndef TROUBLE_PROVIDE_CONDITION
#define TROUBLE_PROVIDE_CONDITION 1
#endif

#ifndef TROUBLE_PROVIDE_FUNCTION
#define TROUBLE_PROVIDE_FUNCTION 1
#endif

#ifndef TROUBLE_PROVIDE_FILENAME
#define TROUBLE_PROVIDE_FILENAME 1
#endif

#if TROUBLE_PROVIDE_LINENO
  const unsigned line_;
#endif
#if TROUBLE_PROVIDE_CONDITION
  const char *const condition_;
#endif
#if TROUBLE_PROVIDE_FUNCTION
  const char *const function_;
#endif
#if TROUBLE_PROVIDE_FILENAME
  const char *const filename_;
#endif

public:
  MDBX_CXX11_CONSTEXPR trouble_location(unsigned line, const char *condition,
                                   const char *function, const char *filename)
      :
#if TROUBLE_PROVIDE_LINENO
        line_(line)
#endif
#if TROUBLE_PROVIDE_CONDITION
        ,
        condition_(condition)
#endif
#if TROUBLE_PROVIDE_FUNCTION
        ,
        function_(function)
#endif
#if TROUBLE_PROVIDE_FILENAME
        ,
        filename_(filename)
#endif
  {
#if !TROUBLE_PROVIDE_LINENO
    (void)line;
#endif
#if !TROUBLE_PROVIDE_CONDITION
    (void)condition;
#endif
#if !TROUBLE_PROVIDE_FUNCTION
    (void)function;
#endif
#if !TROUBLE_PROVIDE_FILENAME
    (void)filename;
#endif
  }

  trouble_location(const trouble_location &&) = delete;

  unsigned line() const {
#if TROUBLE_PROVIDE_LINENO
    return line_;
#else
    return 0;
#endif
  }

  const char *condition() const {
#if TROUBLE_PROVIDE_CONDITION
    return condition_;
#else
    return "";
#endif
  }

  const char *function() const {
#if TROUBLE_PROVIDE_FUNCTION
    return function_;
#else
    return "";
#endif
  }

  const char *filename() const {
#if TROUBLE_PROVIDE_FILENAME
    return filename_;
#else
    return "";
#endif
  }
};

//------------------------------------------------------------------------------

__cold  std::string format_va(const char *fmt, va_list ap) {
  va_list ones;
  va_copy(ones, ap);
#ifdef _MSC_VER
  int needed = _vscprintf(fmt, ap);
#else
  int needed = vsnprintf(nullptr, 0, fmt, ap);
#endif
  assert(needed >= 0);
  std::string result;
  result.reserve(size_t(needed + 1));
  result.resize(size_t(needed), '\0');
  assert(int(result.capacity()) > needed);
  int actual = vsnprintf(const_cast<char *>(result.data()), result.capacity(),
                         fmt, ones);
  assert(actual == needed);
  (void)actual;
  va_end(ones);
  return result;
}

__cold  std::string format(const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  std::string result = format_va(fmt, ap);
  va_end(ap);
  return result;
}

class bug : public std::runtime_error {
  const trouble_location &location_;

public:
  bug(const trouble_location &) noexcept;
  /* temporary workaround for "private field 'FOO' is not used" from CLANG
   * and for "function 'BAR' was declared but never referenced" from LCC. */
#ifndef __LCC__
  const trouble_location &location() const noexcept { return location_; }
#endif
  virtual ~bug() noexcept;
};

__cold  bug::bug(const trouble_location &location) noexcept
    : std::runtime_error(format("mdbx.bug: %s.%s at %s:%u", location.function(),
                                location.condition(), location.filename(),
                                location.line())),
      location_(location) {}

__cold  bug::~bug() noexcept {}

[[noreturn]] __cold  void raise_bug(const trouble_location &what_and_where) {
  throw bug(what_and_where);
}

#define RAISE_BUG(line, condition, function, file)                             \
  do {                                                                         \
    static MDBX_CXX11_CONSTEXPR_VAR trouble_location bug(line, condition,      \
                                                         function, file);      \
    raise_bug(bug);                                                            \
  } while (0)

#define ENSURE(condition)                                                      \
  do                                                                           \
    if (MDBX_UNLIKELY(!(condition)))                                           \
      MDBX_CXX20_UNLIKELY RAISE_BUG(__LINE__, #condition, __func__, __FILE__); \
  while (0)

#define NOT_IMPLEMENTED()                                                      \
  RAISE_BUG(__LINE__, "not_implemented", __func__, __FILE__);

#endif /* Unused*/

} // namespace

//------------------------------------------------------------------------------

namespace mdbx {

[[noreturn]] __cold void throw_max_length_exceeded() {
  throw std::length_error(
      "mdbx:: Exceeded the maximal length of data/slice/buffer.");
}

[[noreturn]] __cold void throw_too_small_target_buffer() {
  throw std::length_error("mdbx:: The target buffer is too small.");
}

[[noreturn]] __cold void throw_out_range() {
  throw std::out_of_range("mdbx:: Slice or buffer method was called with "
                          "an argument that exceeds the length.");
}

[[noreturn]] __cold void throw_allocators_mismatch() {
  throw std::logic_error(
      "mdbx:: An allocators mismatch, so an object could not be transferred "
      "into an incompatible memory allocation scheme.");
}

__cold exception::exception(const ::mdbx::error &error) noexcept
    : base(error.what()), error_(error) {}

__cold exception::~exception() noexcept {}

static std::atomic_int fatal_countdown;

__cold fatal::fatal(const ::mdbx::error &error) noexcept : base(error) {
  ++fatal_countdown;
}

__cold fatal::~fatal() noexcept {
  if (--fatal_countdown == 0)
    std::terminate();
}

#define DEFINE_EXCEPTION(NAME)                                                 \
  __cold NAME::NAME(const ::mdbx::error &rc) : exception(rc) {}                \
  __cold NAME::~NAME() noexcept {}

DEFINE_EXCEPTION(bad_map_id)
DEFINE_EXCEPTION(bad_transaction)
DEFINE_EXCEPTION(bad_value_size)
DEFINE_EXCEPTION(db_corrupted)
DEFINE_EXCEPTION(db_full)
DEFINE_EXCEPTION(db_invalid)
DEFINE_EXCEPTION(db_too_large)
DEFINE_EXCEPTION(db_unable_extend)
DEFINE_EXCEPTION(db_version_mismatch)
DEFINE_EXCEPTION(db_wanna_write_for_recovery)
DEFINE_EXCEPTION(incompatible_operation)
DEFINE_EXCEPTION(internal_page_full)
DEFINE_EXCEPTION(internal_problem)
DEFINE_EXCEPTION(key_exists)
DEFINE_EXCEPTION(key_mismatch)
DEFINE_EXCEPTION(max_maps_reached)
DEFINE_EXCEPTION(max_readers_reached)
DEFINE_EXCEPTION(multivalue)
DEFINE_EXCEPTION(no_data)
DEFINE_EXCEPTION(not_found)
DEFINE_EXCEPTION(operation_not_permitted)
DEFINE_EXCEPTION(permission_denied_or_not_writeable)
DEFINE_EXCEPTION(reader_slot_busy)
DEFINE_EXCEPTION(remote_media)
DEFINE_EXCEPTION(something_busy)
DEFINE_EXCEPTION(thread_mismatch)
DEFINE_EXCEPTION(transaction_full)
DEFINE_EXCEPTION(transaction_overlapping)

#undef DEFINE_EXCEPTION

__cold const char *error::what() const noexcept {
  if (is_mdbx_error())
    return mdbx_liberr2str(code());

  switch (code()) {
#define ERROR_CASE(CODE)                                                       \
  case CODE:                                                                   \
    return MDBX_STRINGIFY(CODE)
    ERROR_CASE(MDBX_ENODATA);
    ERROR_CASE(MDBX_EINVAL);
    ERROR_CASE(MDBX_EACCESS);
    ERROR_CASE(MDBX_ENOMEM);
    ERROR_CASE(MDBX_EROFS);
    ERROR_CASE(MDBX_ENOSYS);
    ERROR_CASE(MDBX_EIO);
    ERROR_CASE(MDBX_EPERM);
    ERROR_CASE(MDBX_EINTR);
    ERROR_CASE(MDBX_ENOFILE);
    ERROR_CASE(MDBX_EREMOTE);
#undef ERROR_CASE
  default:
    return "SYSTEM";
  }
}

__cold std::string error::message() const {
  char buf[1024];
  const char *msg = ::mdbx_strerror_r(code(), buf, sizeof(buf));
  return std::string(msg ? msg : "unknown");
}

[[noreturn]] __cold void error::panic(const char *context,
                                      const char *func) const noexcept {
  assert(code() != MDBX_SUCCESS);
  ::mdbx_panic("mdbx::%s.%s(): \"%s\" (%d)", context, func, what(), code());
  std::terminate();
}

__cold void error::throw_exception() const {
  switch (code()) {
  case MDBX_EINVAL:
    throw std::invalid_argument("mdbx");
  case MDBX_ENOMEM:
    throw std::bad_alloc();
  case MDBX_SUCCESS:
    static_assert(MDBX_SUCCESS == MDBX_RESULT_FALSE, "WTF?");
    throw std::logic_error("MDBX_SUCCESS (MDBX_RESULT_FALSE)");
  case MDBX_RESULT_TRUE:
    throw std::logic_error("MDBX_RESULT_TRUE");
#define CASE_EXCEPTION(NAME, CODE)                                             \
  case CODE:                                                                   \
    throw NAME(code())
    CASE_EXCEPTION(bad_map_id, MDBX_BAD_DBI);
    CASE_EXCEPTION(bad_transaction, MDBX_BAD_TXN);
    CASE_EXCEPTION(bad_value_size, MDBX_BAD_VALSIZE);
    CASE_EXCEPTION(db_corrupted, MDBX_CORRUPTED);
    CASE_EXCEPTION(db_corrupted, MDBX_CURSOR_FULL); /* branch-pages loop */
    CASE_EXCEPTION(db_corrupted, MDBX_PAGE_NOTFOUND);
    CASE_EXCEPTION(db_full, MDBX_MAP_FULL);
    CASE_EXCEPTION(db_invalid, MDBX_INVALID);
    CASE_EXCEPTION(db_too_large, MDBX_TOO_LARGE);
    CASE_EXCEPTION(db_unable_extend, MDBX_UNABLE_EXTEND_MAPSIZE);
    CASE_EXCEPTION(db_version_mismatch, MDBX_VERSION_MISMATCH);
    CASE_EXCEPTION(db_wanna_write_for_recovery, MDBX_WANNA_RECOVERY);
    CASE_EXCEPTION(fatal, MDBX_EBADSIGN);
    CASE_EXCEPTION(fatal, MDBX_PANIC);
    CASE_EXCEPTION(incompatible_operation, MDBX_INCOMPATIBLE);
    CASE_EXCEPTION(internal_page_full, MDBX_PAGE_FULL);
    CASE_EXCEPTION(internal_problem, MDBX_PROBLEM);
    CASE_EXCEPTION(key_mismatch, MDBX_EKEYMISMATCH);
    CASE_EXCEPTION(max_maps_reached, MDBX_DBS_FULL);
    CASE_EXCEPTION(max_readers_reached, MDBX_READERS_FULL);
    CASE_EXCEPTION(multivalue, MDBX_EMULTIVAL);
    CASE_EXCEPTION(no_data, MDBX_ENODATA);
    CASE_EXCEPTION(not_found, MDBX_NOTFOUND);
    CASE_EXCEPTION(operation_not_permitted, MDBX_EPERM);
    CASE_EXCEPTION(permission_denied_or_not_writeable, MDBX_EACCESS);
    CASE_EXCEPTION(reader_slot_busy, MDBX_BAD_RSLOT);
    CASE_EXCEPTION(remote_media, MDBX_EREMOTE);
    CASE_EXCEPTION(something_busy, MDBX_BUSY);
    CASE_EXCEPTION(thread_mismatch, MDBX_THREAD_MISMATCH);
    CASE_EXCEPTION(transaction_full, MDBX_TXN_FULL);
    CASE_EXCEPTION(transaction_overlapping, MDBX_TXN_OVERLAPPING);
#undef CASE_EXCEPTION
  default:
    if (is_mdbx_error())
      throw exception(*this);
    throw std::system_error(std::error_code(code(), std::system_category()));
  }
}

//------------------------------------------------------------------------------

bool slice::is_printable(bool disable_utf8) const noexcept {
  enum : byte {
    LS = 4,                     // shift for UTF8 sequence length
    P_ = 1 << LS,               // printable ASCII flag
    N_ = 0,                     // non-printable ASCII
    second_range_mask = P_ - 1, // mask for range flag
    r80_BF = 0,                 // flag for UTF8 2nd byte range
    rA0_BF = 1,                 // flag for UTF8 2nd byte range
    r80_9F = 2,                 // flag for UTF8 2nd byte range
    r90_BF = 3,                 // flag for UTF8 2nd byte range
    r80_8F = 4,                 // flag for UTF8 2nd byte range

    // valid utf-8 byte sequences
    // http://www.unicode.org/versions/Unicode6.0.0/ch03.pdf - page 94
    //                        Code               | Bytes  |        |        |
    //                        Points             | 1st    | 2nd    | 3rd    |4th
    //                       --------------------|--------|--------|--------|---
    C2 = 2 << LS | r80_BF, // U+000080..U+0007FF | C2..DF | 80..BF |        |
    E0 = 3 << LS | rA0_BF, // U+000800..U+000FFF | E0     | A0..BF | 80..BF |
    E1 = 3 << LS | r80_BF, // U+001000..U+00CFFF | E1..EC | 80..BF | 80..BF |
    ED = 3 << LS | r80_9F, // U+00D000..U+00D7FF | ED     | 80..9F | 80..BF |
    EE = 3 << LS | r80_BF, // U+00E000..U+00FFFF | EE..EF | 80..BF | 80..BF |
    F0 = 4 << LS | r90_BF, // U+010000..U+03FFFF | F0     | 90..BF | 80..BF |...
    F1 = 4 << LS | r80_BF, // U+040000..U+0FFFFF | F1..F3 | 80..BF | 80..BF |...
    F4 = 4 << LS | r80_BF, // U+100000..U+10FFFF | F4     | 80..8F | 80..BF |...
  };

  static const byte range_from[] = {0x80, 0xA0, 0x80, 0x90, 0x80};
  static const byte range_to[] = {0xBF, 0xBF, 0x9F, 0xBF, 0x8F};

  static const byte map[256] = {
      //  1   2   3   4   5   6   7   8   9   a   b   c   d   e   f
      N_, N_, N_, N_, N_, N_, N_, N_, N_, N_, N_, N_, N_, N_, N_, N_, // 00
      N_, N_, N_, N_, N_, N_, N_, N_, N_, N_, N_, N_, N_, N_, N_, N_, // 10
      P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, // 20
      P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, // 30
      P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, // 40
      P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, // 50
      P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, // 60
      P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, N_, // 70
      N_, N_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, N_, P_, N_, // 80
      N_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, N_, P_, P_, // 90
      P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, // a0
      P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, // b0
      P_, P_, C2, C2, C2, C2, C2, C2, C2, C2, C2, C2, C2, C2, C2, C2, // c0
      C2, C2, C2, C2, C2, C2, C2, C2, C2, C2, C2, C2, C2, C2, C2, C2, // df
      E0, E1, E1, E1, E1, E1, E1, E1, E1, E1, E1, E1, E1, ED, EE, EE, // e0
      F0, F1, F1, F1, F4, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_, P_  // f0
  };

  if (MDBX_UNLIKELY(length() < 1))
    MDBX_CXX20_UNLIKELY return false;

  auto src = byte_ptr();
  const auto end = src + length();
  if (MDBX_UNLIKELY(disable_utf8)) {
    do
      if (MDBX_UNLIKELY((P_ & map[*src]) == 0))
        MDBX_CXX20_UNLIKELY return false;
    while (++src < end);
    return true;
  }

  do {
    const auto bits = map[*src];
    const auto second_from = range_from[bits & second_range_mask];
    const auto second_to = range_to[bits & second_range_mask];
    switch (bits >> LS) {
    default:
      MDBX_CXX20_UNLIKELY return false;
    case 1:
      src += 1;
      continue;
    case 2:
      if (MDBX_UNLIKELY(src + 1 >= end))
        MDBX_CXX20_UNLIKELY return false;
      if (MDBX_UNLIKELY(src[1] < second_from || src[1] > second_to))
        MDBX_CXX20_UNLIKELY return false;
      src += 2;
      continue;
    case 3:
      if (MDBX_UNLIKELY(src + 3 >= end))
        MDBX_CXX20_UNLIKELY return false;
      if (MDBX_UNLIKELY(src[1] < second_from || src[1] > second_to))
        MDBX_CXX20_UNLIKELY return false;
      if (MDBX_UNLIKELY(src[2] < 0x80 || src[2] > 0xBF))
        MDBX_CXX20_UNLIKELY return false;
      src += 3;
      continue;
    case 4:
      if (MDBX_UNLIKELY(src + 4 >= end))
        MDBX_CXX20_UNLIKELY return false;
      if (MDBX_UNLIKELY(src[1] < second_from || src[1] > second_to))
        MDBX_CXX20_UNLIKELY return false;
      if (MDBX_UNLIKELY(src[2] < 0x80 || src[2] > 0xBF))
        MDBX_CXX20_UNLIKELY return false;
      if (MDBX_UNLIKELY(src[3] < 0x80 || src[3] > 0xBF))
        MDBX_CXX20_UNLIKELY return false;
      src += 4;
      continue;
    }
  } while (src < end);

  return true;
}

//------------------------------------------------------------------------------

char *to_hex::write_bytes(char *__restrict const dest, size_t dest_size) const {
  if (MDBX_UNLIKELY(envisage_result_length() > dest_size))
    MDBX_CXX20_UNLIKELY throw_too_small_target_buffer();

  auto ptr = dest;
  auto src = source.byte_ptr();
  const char alphabase = (uppercase ? 'A' : 'a') - 10;
  auto line = ptr;
  for (const auto end = source.end_byte_ptr(); src != end; ++src) {
    if (wrap_width && size_t(ptr - line) >= wrap_width) {
      *ptr = '\n';
      line = ++ptr;
    }
    const int8_t hi = *src >> 4;
    const int8_t lo = *src & 15;
    ptr[0] = char(alphabase + hi + (((hi - 10) >> 7) & -7));
    ptr[1] = char(alphabase + lo + (((lo - 10) >> 7) & -7));
    ptr += 2;
    assert(ptr <= dest + dest_size);
  }
  return ptr;
}

::std::ostream &to_hex::output(::std::ostream &out) const {
  if (MDBX_LIKELY(!is_empty()))
    MDBX_CXX20_LIKELY {
      ::std::ostream::sentry sentry(out);
      auto src = source.byte_ptr();
      const char alphabase = (uppercase ? 'A' : 'a') - 10;
      unsigned width = 0;
      for (const auto end = source.end_byte_ptr(); src != end; ++src) {
        if (wrap_width && width >= wrap_width) {
          out << ::std::endl;
          width = 0;
        }
        const int8_t hi = *src >> 4;
        const int8_t lo = *src & 15;
        out.put(char(alphabase + hi + (((hi - 10) >> 7) & -7)));
        out.put(char(alphabase + lo + (((lo - 10) >> 7) & -7)));
        width += 2;
      }
    }
  return out;
}

char *from_hex::write_bytes(char *__restrict const dest,
                            size_t dest_size) const {
  if (MDBX_UNLIKELY(source.length() % 2 && !ignore_spaces))
    MDBX_CXX20_UNLIKELY throw std::domain_error(
        "mdbx::from_hex:: odd length of hexadecimal string");
  if (MDBX_UNLIKELY(envisage_result_length() > dest_size))
    MDBX_CXX20_UNLIKELY throw_too_small_target_buffer();

  auto ptr = dest;
  auto src = source.byte_ptr();
  for (auto left = source.length(); left > 0;) {
    if (MDBX_UNLIKELY(*src <= ' ') &&
        MDBX_LIKELY(ignore_spaces && isspace(*src))) {
      ++src;
      --left;
      continue;
    }

    if (MDBX_UNLIKELY(left < 1 || !isxdigit(src[0]) || !isxdigit(src[1])))
      MDBX_CXX20_UNLIKELY throw std::domain_error(
          "mdbx::from_hex:: invalid hexadecimal string");

    int8_t hi = src[0];
    hi = (hi | 0x20) - 'a';
    hi += 10 + ((hi >> 7) & 7);

    int8_t lo = src[1];
    lo = (lo | 0x20) - 'a';
    lo += 10 + ((lo >> 7) & 7);

    *ptr++ = hi << 4 | lo;
    src += 2;
    left -= 2;
    assert(ptr <= dest + dest_size);
  }
  return ptr;
}

bool from_hex::is_erroneous() const noexcept {
  if (MDBX_UNLIKELY(source.length() % 2 && !ignore_spaces))
    MDBX_CXX20_UNLIKELY return true;

  bool got = false;
  auto src = source.byte_ptr();
  for (auto left = source.length(); left > 0;) {
    if (MDBX_UNLIKELY(*src <= ' ') &&
        MDBX_LIKELY(ignore_spaces && isspace(*src))) {
      ++src;
      --left;
      continue;
    }

    if (MDBX_UNLIKELY(left < 1 || !isxdigit(src[0]) || !isxdigit(src[1])))
      MDBX_CXX20_UNLIKELY return true;

    got = true;
    src += 2;
    left -= 2;
  }
  return !got;
}

//------------------------------------------------------------------------------

enum : signed char {
  OO /* ASCII NUL */ = -8,
  EQ /* BASE64 '=' pad */ = -4,
  SP /* SPACE */ = -2,
  IL /* invalid */ = -1
};

static const byte b58_alphabet[58] = {
    '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F',
    'G', 'H', 'J', 'K', 'L', 'M', 'N', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W',
    'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'm',
    'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'};

#ifndef bswap64
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
static inline uint64_t bswap64(uint64_t v) noexcept {
#if __GNUC_PREREQ(4, 4) || __CLANG_PREREQ(4, 0) ||                             \
    __has_builtin(__builtin_bswap64)
  return __builtin_bswap64(v);
#elif defined(_MSC_VER) && !defined(__clang__)
  return _byteswap_uint64(v);
#elif defined(__bswap_64)
  return __bswap_64(v);
#elif defined(bswap_64)
  return bswap_64(v);
#else
  return v << 56 | v >> 56 | ((v << 40) & UINT64_C(0x00ff000000000000)) |
         ((v << 24) & UINT64_C(0x0000ff0000000000)) |
         ((v << 8) & UINT64_C(0x000000ff00000000)) |
         ((v >> 8) & UINT64_C(0x00000000ff000000)) |
         ((v >> 24) & UINT64_C(0x0000000000ff0000)) |
         ((v >> 40) & UINT64_C(0x000000000000ff00));
#endif
}
#endif /* __BYTE_ORDER__ */
#endif /* ifndef bswap64 */

static inline char b58_8to11(uint64_t &v) noexcept {
  const unsigned i = unsigned(v % 58);
  v /= 58;
  return b58_alphabet[i];
}

char *to_base58::write_bytes(char *__restrict const dest,
                             size_t dest_size) const {
  if (MDBX_UNLIKELY(envisage_result_length() > dest_size))
    MDBX_CXX20_UNLIKELY throw_too_small_target_buffer();

  auto ptr = dest;
  auto src = source.byte_ptr();
  size_t left = source.length();
  auto line = ptr;
  while (MDBX_LIKELY(left > 7)) {
    uint64_t v;
    std::memcpy(&v, src, 8);
    src += 8;
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    v = bswap64(v);
#elif __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
#else
#error "FIXME: Unsupported byte order"
#endif /* __BYTE_ORDER__ */
    ptr[10] = b58_8to11(v);
    ptr[9] = b58_8to11(v);
    ptr[8] = b58_8to11(v);
    ptr[7] = b58_8to11(v);
    ptr[6] = b58_8to11(v);
    ptr[5] = b58_8to11(v);
    ptr[4] = b58_8to11(v);
    ptr[3] = b58_8to11(v);
    ptr[2] = b58_8to11(v);
    ptr[1] = b58_8to11(v);
    ptr[0] = b58_8to11(v);
    assert(v == 0);
    ptr += 11;
    left -= 8;
    if (wrap_width && size_t(ptr - line) >= wrap_width && left) {
      *ptr = '\n';
      line = ++ptr;
    }
    assert(ptr <= dest + dest_size);
  }

  if (left) {
    uint64_t v = 0;
    unsigned parrots = 31;
    do {
      v = (v << 8) + *src++;
      parrots += 43;
    } while (--left);

    auto tail = ptr += parrots >> 5;
    assert(ptr <= dest + dest_size);
    do {
      *--tail = b58_8to11(v);
      parrots -= 32;
    } while (parrots > 31);
    assert(v == 0);
  }

  return ptr;
}

::std::ostream &to_base58::output(::std::ostream &out) const {
  if (MDBX_LIKELY(!is_empty()))
    MDBX_CXX20_LIKELY {
      ::std::ostream::sentry sentry(out);
      auto src = source.byte_ptr();
      size_t left = source.length();
      unsigned width = 0;
      std::array<char, 11> buf;

      while (MDBX_LIKELY(left > 7)) {
        uint64_t v;
        std::memcpy(&v, src, 8);
        src += 8;
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
        v = bswap64(v);
#elif __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
#else
#error "FIXME: Unsupported byte order"
#endif /* __BYTE_ORDER__ */
        buf[10] = b58_8to11(v);
        buf[9] = b58_8to11(v);
        buf[8] = b58_8to11(v);
        buf[7] = b58_8to11(v);
        buf[6] = b58_8to11(v);
        buf[5] = b58_8to11(v);
        buf[4] = b58_8to11(v);
        buf[3] = b58_8to11(v);
        buf[2] = b58_8to11(v);
        buf[1] = b58_8to11(v);
        buf[0] = b58_8to11(v);
        assert(v == 0);
        out.write(&buf.front(), 11);
        left -= 8;
        if (wrap_width && (width += 11) >= wrap_width && left) {
          out << ::std::endl;
          width = 0;
        }
      }

      if (left) {
        uint64_t v = 0;
        unsigned parrots = 31;
        do {
          v = (v << 8) + *src++;
          parrots += 43;
        } while (--left);

        auto ptr = buf.end();
        do {
          *--ptr = b58_8to11(v);
          parrots -= 32;
        } while (parrots > 31);
        assert(v == 0);
        out.write(&*ptr, buf.end() - ptr);
      }
    }
  return out;
}

const signed char b58_map[256] = {
    //   1   2   3   4   5   6   7   8   9   a   b   c   d   e   f
    OO, IL, IL, IL, IL, IL, IL, IL, IL, SP, SP, SP, SP, SP, IL, IL, // 00
    IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, // 10
    SP, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, // 20
    IL, 0,  1,  2,  3,  4,  5,  6,  7,  8,  IL, IL, IL, IL, IL, IL, // 30
    IL, 9,  10, 11, 12, 13, 14, 15, 16, IL, 17, 18, 19, 20, 21, IL, // 40
    22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, IL, IL, IL, IL, IL, // 50
    IL, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, IL, 44, 45, 46, // 60
    47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, IL, IL, IL, IL, IL, // 70
    IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, // 80
    IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, // 90
    IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, // a0
    IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, // b0
    IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, // c0
    IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, // d0
    IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, // e0
    IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL  // f0
};

static inline signed char b58_11to8(uint64_t &v, const byte c) noexcept {
  const signed char m = b58_map[c];
  v = v * 58 + m;
  return m;
}

char *from_base58::write_bytes(char *__restrict const dest,
                               size_t dest_size) const {
  if (MDBX_UNLIKELY(envisage_result_length() > dest_size))
    MDBX_CXX20_UNLIKELY throw_too_small_target_buffer();

  auto ptr = dest;
  auto src = source.byte_ptr();
  for (auto left = source.length(); left > 0;) {
    if (MDBX_UNLIKELY(isspace(*src)) && ignore_spaces) {
      ++src;
      --left;
      continue;
    }

    if (MDBX_LIKELY(left > 10)) {
      uint64_t v = 0;
      if (MDBX_UNLIKELY((b58_11to8(v, src[0]) | b58_11to8(v, src[1]) |
                         b58_11to8(v, src[2]) | b58_11to8(v, src[3]) |
                         b58_11to8(v, src[4]) | b58_11to8(v, src[5]) |
                         b58_11to8(v, src[6]) | b58_11to8(v, src[7]) |
                         b58_11to8(v, src[8]) | b58_11to8(v, src[9]) |
                         b58_11to8(v, src[10])) < 0))
        MDBX_CXX20_UNLIKELY goto bailout;
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
      v = bswap64(v);
#elif __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
#else
#error "FIXME: Unsupported byte order"
#endif /* __BYTE_ORDER__ */
      std::memcpy(ptr, &v, 8);
      ptr += 8;
      src += 11;
      left -= 11;
      assert(ptr <= dest + dest_size);
      continue;
    }

    constexpr unsigned invalid_length_mask = 1 << 1 | 1 << 4 | 1 << 8;
    if (MDBX_UNLIKELY(invalid_length_mask & (1 << left)))
      MDBX_CXX20_UNLIKELY goto bailout;

    uint64_t v = 1;
    unsigned parrots = 0;
    do {
      if (MDBX_UNLIKELY(b58_11to8(v, *src++) < 0))
        MDBX_CXX20_UNLIKELY goto bailout;
      parrots += 32;
    } while (--left);

    auto tail = ptr += parrots / 43;
    assert(ptr <= dest + dest_size);
    do {
      *--tail = byte(v);
      v >>= 8;
    } while (v > 255);
    break;
  }
  return ptr;

bailout:
  throw std::domain_error("mdbx::from_base58:: invalid base58 string");
}

bool from_base58::is_erroneous() const noexcept {
  bool got = false;
  auto src = source.byte_ptr();
  for (auto left = source.length(); left > 0;) {
    if (MDBX_UNLIKELY(*src <= ' ') &&
        MDBX_LIKELY(ignore_spaces && isspace(*src))) {
      ++src;
      --left;
      continue;
    }

    if (MDBX_LIKELY(left > 10)) {
      if (MDBX_UNLIKELY((b58_map[src[0]] | b58_map[src[1]] | b58_map[src[2]] |
                         b58_map[src[3]] | b58_map[src[4]] | b58_map[src[5]] |
                         b58_map[src[6]] | b58_map[src[7]] | b58_map[src[8]] |
                         b58_map[src[9]] | b58_map[src[10]]) < 0))
        MDBX_CXX20_UNLIKELY return true;
      src += 11;
      left -= 11;
      got = true;
      continue;
    }

    constexpr unsigned invalid_length_mask = 1 << 1 | 1 << 4 | 1 << 8;
    if (invalid_length_mask & (1 << left))
      return false;

    do
      if (MDBX_UNLIKELY(b58_map[*src++] < 0))
        MDBX_CXX20_UNLIKELY return true;
    while (--left);
    got = true;
    break;
  }
  return !got;
}

//------------------------------------------------------------------------------

static inline void b64_3to4(const byte x, const byte y, const byte z,
                            char *__restrict dest) noexcept {
  static const byte alphabet[64] = {
      'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
      'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
      'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
      'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
      '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', '/'};
  dest[0] = alphabet[(x & 0xfc) >> 2];
  dest[1] = alphabet[((x & 0x03) << 4) + ((y & 0xf0) >> 4)];
  dest[2] = alphabet[((y & 0x0f) << 2) + ((z & 0xc0) >> 6)];
  dest[3] = alphabet[z & 0x3f];
}

char *to_base64::write_bytes(char *__restrict const dest,
                             size_t dest_size) const {
  if (MDBX_UNLIKELY(envisage_result_length() > dest_size))
    MDBX_CXX20_UNLIKELY throw_too_small_target_buffer();

  auto ptr = dest;
  auto src = source.byte_ptr();
  size_t left = source.length();
  auto line = ptr;
  while (true) {
    switch (left) {
    default:
      MDBX_CXX20_LIKELY left -= 3;
      b64_3to4(src[0], src[1], src[2], ptr);
      ptr += 4;
      src += 3;
      if (wrap_width && size_t(ptr - line) >= wrap_width && left) {
        *ptr = '\n';
        line = ++ptr;
      }
      assert(ptr <= dest + dest_size);
      continue;
    case 2:
      b64_3to4(src[0], src[1], 0, ptr);
      ptr[3] = '=';
      assert(ptr + 4 <= dest + dest_size);
      return ptr + 4;
    case 1:
      b64_3to4(src[0], 0, 0, ptr);
      ptr[2] = ptr[3] = '=';
      assert(ptr + 4 <= dest + dest_size);
      return ptr + 4;
    case 0:
      return ptr;
    }
  }
}

::std::ostream &to_base64::output(::std::ostream &out) const {
  if (MDBX_LIKELY(!is_empty()))
    MDBX_CXX20_LIKELY {
      ::std::ostream::sentry sentry(out);
      auto src = source.byte_ptr();
      size_t left = source.length();
      unsigned width = 0;
      std::array<char, 4> buf;

      while (true) {
        switch (left) {
        default:
          MDBX_CXX20_LIKELY left -= 3;
          b64_3to4(src[0], src[1], src[2], &buf.front());
          src += 3;
          out.write(&buf.front(), 4);
          if (wrap_width && (width += 4) >= wrap_width && left) {
            out << ::std::endl;
            width = 0;
          }
          continue;
        case 2:
          b64_3to4(src[0], src[1], 0, &buf.front());
          buf[3] = '=';
          return out.write(&buf.front(), 4);
        case 1:
          b64_3to4(src[0], 0, 0, &buf.front());
          buf[2] = buf[3] = '=';
          return out.write(&buf.front(), 4);
        case 0:
          return out;
        }
      }
    }
  return out;
}

static const signed char b64_map[256] = {
    //   1   2   3   4   5   6   7   8   9   a   b   c   d   e   f
    OO, IL, IL, IL, IL, IL, IL, IL, IL, SP, SP, SP, SP, SP, IL, IL, // 00
    IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, // 10
    SP, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, 62, IL, IL, IL, 63, // 20
    52, 53, 54, 55, 56, 57, 58, 59, 60, 61, IL, IL, IL, EQ, IL, IL, // 30
    IL, 0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, // 40
    15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, IL, IL, IL, IL, IL, // 50
    IL, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, // 60
    41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, IL, IL, IL, IL, IL, // 70
    IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, // 80
    IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, // 90
    IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, // a0
    IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, // b0
    IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, // c0
    IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, // d0
    IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, // e0
    IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL, IL  // f0
};

static inline signed char b64_4to3(signed char a, signed char b, signed char c,
                                   signed char d,
                                   char *__restrict dest) noexcept {
  dest[0] = byte((a << 2) + ((b & 0x30) >> 4));
  dest[1] = byte(((b & 0xf) << 4) + ((c & 0x3c) >> 2));
  dest[2] = byte(((c & 0x3) << 6) + d);
  return a | b | c | d;
}

char *from_base64::write_bytes(char *__restrict const dest,
                               size_t dest_size) const {
  if (MDBX_UNLIKELY(source.length() % 4 && !ignore_spaces))
    MDBX_CXX20_UNLIKELY throw std::domain_error(
        "mdbx::from_base64:: odd length of base64 string");
  if (MDBX_UNLIKELY(envisage_result_length() > dest_size))
    MDBX_CXX20_UNLIKELY throw_too_small_target_buffer();

  auto ptr = dest;
  auto src = source.byte_ptr();
  for (auto left = source.length(); left > 0;) {
    if (MDBX_UNLIKELY(*src <= ' ') &&
        MDBX_LIKELY(ignore_spaces && isspace(*src))) {
      ++src;
      --left;
      continue;
    }

    if (MDBX_UNLIKELY(left < 3))
      MDBX_CXX20_UNLIKELY {
      bailout:
        throw std::domain_error("mdbx::from_base64:: invalid base64 string");
      }
    const signed char a = b64_map[src[0]], b = b64_map[src[1]],
                      c = b64_map[src[2]], d = b64_map[src[3]];
    if (MDBX_UNLIKELY(b64_4to3(a, b, c, d, ptr) < 0)) {
      if (left == 4 && (a | b) >= 0 && d == EQ) {
        if (c >= 0) {
          assert(ptr + 2 <= dest + dest_size);
          return ptr + 2;
        }
        if (c == d) {
          assert(ptr + 1 <= dest + dest_size);
          return ptr + 1;
        }
      }
      MDBX_CXX20_UNLIKELY goto bailout;
    }
    src += 4;
    left -= 4;
    ptr += 3;
    assert(ptr <= dest + dest_size);
  }
  return ptr;
}

bool from_base64::is_erroneous() const noexcept {
  if (MDBX_UNLIKELY(source.length() % 4 && !ignore_spaces))
    MDBX_CXX20_UNLIKELY return true;

  bool got = false;
  auto src = source.byte_ptr();
  for (auto left = source.length(); left > 0;) {
    if (MDBX_UNLIKELY(*src <= ' ') &&
        MDBX_LIKELY(ignore_spaces && isspace(*src))) {
      ++src;
      --left;
      continue;
    }

    if (MDBX_UNLIKELY(left < 3))
      MDBX_CXX20_UNLIKELY return false;
    const signed char a = b64_map[src[0]], b = b64_map[src[1]],
                      c = b64_map[src[2]], d = b64_map[src[3]];
    if (MDBX_UNLIKELY((a | b | c | d) < 0))
      MDBX_CXX20_UNLIKELY {
        if (left == 4 && (a | b) >= 0 && d == EQ && (c >= 0 || c == d))
          return false;
        return true;
      }
    got = true;
    src += 4;
    left -= 4;
  }
  return !got;
}

//------------------------------------------------------------------------------

template class LIBMDBX_API_TYPE buffer<legacy_allocator>;

#if defined(__cpp_lib_memory_resource) &&                                      \
    __cpp_lib_memory_resource >= 201603L && _GLIBCXX_USE_CXX11_ABI
template class LIBMDBX_API_TYPE buffer<polymorphic_allocator>;
#endif /* __cpp_lib_memory_resource >= 201603L */

//------------------------------------------------------------------------------

static inline MDBX_env_flags_t mode2flags(env::mode mode) {
  switch (mode) {
  default:
    MDBX_CXX20_UNLIKELY throw std::invalid_argument("db::mode is invalid");
  case env::mode::readonly:
    return MDBX_RDONLY;
  case env::mode::write_file_io:
    return MDBX_ENV_DEFAULTS;
  case env::mode::write_mapped_io:
    return MDBX_WRITEMAP;
  }
}

__cold MDBX_env_flags_t
env::operate_parameters::make_flags(bool accede, bool use_subdirectory) const {
  MDBX_env_flags_t flags = mode2flags(mode);
  if (accede)
    flags |= MDBX_ACCEDE;
  if (!use_subdirectory)
    flags |= MDBX_NOSUBDIR;
  if (options.exclusive)
    flags |= MDBX_EXCLUSIVE;
  if (options.orphan_read_transactions)
    flags |= MDBX_NOTLS;
  if (options.disable_readahead)
    flags |= MDBX_NORDAHEAD;
  if (options.disable_clear_memory)
    flags |= MDBX_NOMEMINIT;

  if (mode != readonly) {
    if (options.nested_write_transactions)
      flags &= ~MDBX_WRITEMAP;
    if (reclaiming.coalesce)
      flags |= MDBX_COALESCE;
    if (reclaiming.lifo)
      flags |= MDBX_LIFORECLAIM;
    switch (durability) {
    default:
      MDBX_CXX20_UNLIKELY throw std::invalid_argument(
          "db::durability is invalid");
    case env::durability::robust_synchronous:
      break;
    case env::durability::half_synchronous_weak_last:
      flags |= MDBX_NOMETASYNC;
      break;
    case env::durability::lazy_weak_tail:
      static_assert(MDBX_MAPASYNC == MDBX_SAFE_NOSYNC, "WTF? Obsolete C API?");
      flags |= MDBX_SAFE_NOSYNC;
      break;
    case env::durability::whole_fragile:
      flags |= MDBX_UTTERLY_NOSYNC;
      break;
    }
  }
  return flags;
}

env::mode
env::operate_parameters::mode_from_flags(MDBX_env_flags_t flags) noexcept {
  if (flags & MDBX_RDONLY)
    return env::mode::readonly;
  return (flags & MDBX_WRITEMAP) ? env::mode::write_mapped_io
                                 : env::mode::write_file_io;
}

env::durability env::operate_parameters::durability_from_flags(
    MDBX_env_flags_t flags) noexcept {
  if ((flags & MDBX_UTTERLY_NOSYNC) == MDBX_UTTERLY_NOSYNC)
    return env::durability::whole_fragile;
  if (flags & MDBX_SAFE_NOSYNC)
    return env::durability::lazy_weak_tail;
  if (flags & MDBX_NOMETASYNC)
    return env::durability::half_synchronous_weak_last;
  return env::durability::robust_synchronous;
}

env::reclaiming_options::reclaiming_options(MDBX_env_flags_t flags) noexcept
    : lifo((flags & MDBX_LIFORECLAIM) ? true : false),
      coalesce((flags & MDBX_COALESCE) ? true : false) {}

env::operate_options::operate_options(MDBX_env_flags_t flags) noexcept
    : orphan_read_transactions(
          ((flags & (MDBX_NOTLS | MDBX_EXCLUSIVE)) == MDBX_NOTLS) ? true
                                                                  : false),
      nested_write_transactions((flags & (MDBX_WRITEMAP | MDBX_RDONLY)) ? false
                                                                        : true),
      exclusive((flags & MDBX_EXCLUSIVE) ? true : false),
      disable_readahead((flags & MDBX_NORDAHEAD) ? true : false),
      disable_clear_memory((flags & MDBX_NOMEMINIT) ? true : false) {}

bool env::is_pristine() const {
  return get_stat().ms_mod_txnid == 0 &&
         get_info().mi_recent_txnid == INITIAL_TXNID;
}

bool env::is_empty() const { return get_stat().ms_leaf_pages == 0; }

env &env::copy(filehandle fd, bool compactify, bool force_dynamic_size) {
  error::success_or_throw(
      ::mdbx_env_copy2fd(handle_, fd,
                         (compactify ? MDBX_CP_COMPACT : MDBX_CP_DEFAULTS) |
                             (force_dynamic_size ? MDBX_CP_FORCE_DYNAMIC_SIZE
                                                 : MDBX_CP_DEFAULTS)));
  return *this;
}

env &env::copy(const char *destination, bool compactify,
               bool force_dynamic_size) {
  error::success_or_throw(
      ::mdbx_env_copy(handle_, destination,
                      (compactify ? MDBX_CP_COMPACT : MDBX_CP_DEFAULTS) |
                          (force_dynamic_size ? MDBX_CP_FORCE_DYNAMIC_SIZE
                                              : MDBX_CP_DEFAULTS)));
  return *this;
}

env &env::copy(const ::std::string &destination, bool compactify,
               bool force_dynamic_size) {
  return copy(destination.c_str(), compactify, force_dynamic_size);
}

#if defined(_WIN32) || defined(_WIN64)
env &env::copy(const wchar_t *destination, bool compactify,
               bool force_dynamic_size) {
  error::success_or_throw(
      ::mdbx_env_copyW(handle_, destination,
                       (compactify ? MDBX_CP_COMPACT : MDBX_CP_DEFAULTS) |
                           (force_dynamic_size ? MDBX_CP_FORCE_DYNAMIC_SIZE
                                               : MDBX_CP_DEFAULTS)));
  return *this;
}

env &env::copy(const ::std::wstring &destination, bool compactify,
               bool force_dynamic_size) {
  return copy(destination.c_str(), compactify, force_dynamic_size);
}
#endif /* Windows */

#ifdef MDBX_STD_FILESYSTEM_PATH
env &env::copy(const MDBX_STD_FILESYSTEM_PATH &destination, bool compactify,
               bool force_dynamic_size) {
  return copy(destination.native(), compactify, force_dynamic_size);
}
#endif /* MDBX_STD_FILESYSTEM_PATH */

path env::get_path() const {
#if defined(_WIN32) || defined(_WIN64)
  const wchar_t *c_wstr;
  error::success_or_throw(::mdbx_env_get_pathW(handle_, &c_wstr));
  static_assert(sizeof(path::value_type) == sizeof(wchar_t), "Oops");
  return path(c_wstr);
#else
  const char *c_str;
  error::success_or_throw(::mdbx_env_get_path(handle_, &c_str));
  static_assert(sizeof(path::value_type) == sizeof(char), "Oops");
  return path(c_str);
#endif
}

bool env::remove(const char *pathname, const remove_mode mode) {
  return error::boolean_or_throw(
      ::mdbx_env_delete(pathname, MDBX_env_delete_mode_t(mode)));
}

bool env::remove(const ::std::string &pathname, const remove_mode mode) {
  return remove(pathname.c_str(), mode);
}

#if defined(_WIN32) || defined(_WIN64)
bool env::remove(const wchar_t *pathname, const remove_mode mode) {
  return error::boolean_or_throw(
      ::mdbx_env_deleteW(pathname, MDBX_env_delete_mode_t(mode)));
}

bool env::remove(const ::std::wstring &pathname, const remove_mode mode) {
  return remove(pathname.c_str(), mode);
}
#endif /* Windows */

#ifdef MDBX_STD_FILESYSTEM_PATH
bool env::remove(const MDBX_STD_FILESYSTEM_PATH &pathname,
                 const remove_mode mode) {
  return remove(pathname.native(), mode);
}
#endif /* MDBX_STD_FILESYSTEM_PATH */

//------------------------------------------------------------------------------

static inline MDBX_env *create_env() {
  MDBX_env *ptr;
  error::success_or_throw(::mdbx_env_create(&ptr));
  assert(ptr != nullptr);
  return ptr;
}

env_managed::~env_managed() noexcept {
  if (MDBX_UNLIKELY(handle_))
    MDBX_CXX20_UNLIKELY error::success_or_panic(
        ::mdbx_env_close(handle_), "mdbx::~env()", "mdbx_env_close");
}

void env_managed::close(bool dont_sync) {
  const error rc =
      static_cast<MDBX_error_t>(::mdbx_env_close_ex(handle_, dont_sync));
  switch (rc.code()) {
  case MDBX_EBADSIGN:
    MDBX_CXX20_UNLIKELY handle_ = nullptr;
    __fallthrough /* fall through */;
  default:
    MDBX_CXX20_UNLIKELY rc.throw_exception();
  case MDBX_SUCCESS:
    MDBX_CXX20_LIKELY handle_ = nullptr;
  }
}

__cold void env_managed::setup(unsigned max_maps, unsigned max_readers) {
  if (max_readers > 0)
    error::success_or_throw(::mdbx_env_set_maxreaders(handle_, max_readers));
  if (max_maps > 0)
    error::success_or_throw(::mdbx_env_set_maxdbs(handle_, max_maps));
}

__cold env_managed::env_managed(const char *pathname,
                                const operate_parameters &op, bool accede)
    : env_managed(create_env()) {
  setup(op.max_maps, op.max_readers);
  error::success_or_throw(
      ::mdbx_env_open(handle_, pathname, op.make_flags(accede), 0));

  if (op.options.nested_write_transactions &&
      !get_options().nested_write_transactions)
    MDBX_CXX20_UNLIKELY error::throw_exception(MDBX_INCOMPATIBLE);
}

__cold env_managed::env_managed(const char *pathname,
                                const env_managed::create_parameters &cp,
                                const env::operate_parameters &op, bool accede)
    : env_managed(create_env()) {
  setup(op.max_maps, op.max_readers);
  set_geometry(cp.geometry);
  error::success_or_throw(::mdbx_env_open(
      handle_, pathname, op.make_flags(accede, cp.use_subdirectory),
      cp.file_mode_bits));

  if (op.options.nested_write_transactions &&
      !get_options().nested_write_transactions)
    MDBX_CXX20_UNLIKELY error::throw_exception(MDBX_INCOMPATIBLE);
}

__cold env_managed::env_managed(const ::std::string &pathname,
                                const operate_parameters &op, bool accede)
    : env_managed(pathname.c_str(), op, accede) {}

__cold env_managed::env_managed(const ::std::string &pathname,
                                const env_managed::create_parameters &cp,
                                const env::operate_parameters &op, bool accede)
    : env_managed(pathname.c_str(), cp, op, accede) {}

#if defined(_WIN32) || defined(_WIN64)
__cold env_managed::env_managed(const wchar_t *pathname,
                                const operate_parameters &op, bool accede)
    : env_managed(create_env()) {
  setup(op.max_maps, op.max_readers);
  error::success_or_throw(
      ::mdbx_env_openW(handle_, pathname, op.make_flags(accede), 0));

  if (op.options.nested_write_transactions &&
      !get_options().nested_write_transactions)
    MDBX_CXX20_UNLIKELY error::throw_exception(MDBX_INCOMPATIBLE);
}

__cold env_managed::env_managed(const wchar_t *pathname,
                                const env_managed::create_parameters &cp,
                                const env::operate_parameters &op, bool accede)
    : env_managed(create_env()) {
  setup(op.max_maps, op.max_readers);
  set_geometry(cp.geometry);
  error::success_or_throw(::mdbx_env_openW(
      handle_, pathname, op.make_flags(accede, cp.use_subdirectory),
      cp.file_mode_bits));

  if (op.options.nested_write_transactions &&
      !get_options().nested_write_transactions)
    MDBX_CXX20_UNLIKELY error::throw_exception(MDBX_INCOMPATIBLE);
}

__cold env_managed::env_managed(const ::std::wstring &pathname,
                                const operate_parameters &op, bool accede)
    : env_managed(pathname.c_str(), op, accede) {}

__cold env_managed::env_managed(const ::std::wstring &pathname,
                                const env_managed::create_parameters &cp,
                                const env::operate_parameters &op, bool accede)
    : env_managed(pathname.c_str(), cp, op, accede) {}
#endif /* Windows */

#ifdef MDBX_STD_FILESYSTEM_PATH
__cold env_managed::env_managed(const MDBX_STD_FILESYSTEM_PATH &pathname,
                                const operate_parameters &op, bool accede)
    : env_managed(pathname.native(), op, accede) {}

__cold env_managed::env_managed(const MDBX_STD_FILESYSTEM_PATH &pathname,
                                const env_managed::create_parameters &cp,
                                const env::operate_parameters &op, bool accede)
    : env_managed(pathname.native(), cp, op, accede) {}
#endif /* MDBX_STD_FILESYSTEM_PATH */

//------------------------------------------------------------------------------

txn_managed txn::start_nested() {
  MDBX_txn *nested;
  error::throw_on_nullptr(handle_, MDBX_BAD_TXN);
  error::success_or_throw(::mdbx_txn_begin(mdbx_txn_env(handle_), handle_,
                                           MDBX_TXN_READWRITE, &nested));
  assert(nested != nullptr);
  return txn_managed(nested);
}

txn_managed::~txn_managed() noexcept {
  if (MDBX_UNLIKELY(handle_))
    MDBX_CXX20_UNLIKELY error::success_or_panic(::mdbx_txn_abort(handle_),
                                                "mdbx::~txn", "mdbx_txn_abort");
}

void txn_managed::abort() {
  const error err = static_cast<MDBX_error_t>(::mdbx_txn_abort(handle_));
  if (MDBX_LIKELY(err.code() != MDBX_THREAD_MISMATCH))
    MDBX_CXX20_LIKELY handle_ = nullptr;
  if (MDBX_UNLIKELY(err.code() != MDBX_SUCCESS))
    MDBX_CXX20_UNLIKELY err.throw_exception();
}

void txn_managed::commit() {
  const error err = static_cast<MDBX_error_t>(::mdbx_txn_commit(handle_));
  if (MDBX_LIKELY(err.code() != MDBX_THREAD_MISMATCH))
    MDBX_CXX20_LIKELY handle_ = nullptr;
  if (MDBX_UNLIKELY(err.code() != MDBX_SUCCESS))
    MDBX_CXX20_UNLIKELY err.throw_exception();
}

void txn_managed::commit(commit_latency *latency) {
  const error err =
      static_cast<MDBX_error_t>(::mdbx_txn_commit_ex(handle_, latency));
  if (MDBX_LIKELY(err.code() != MDBX_THREAD_MISMATCH))
    MDBX_CXX20_LIKELY handle_ = nullptr;
  if (MDBX_UNLIKELY(err.code() != MDBX_SUCCESS))
    MDBX_CXX20_UNLIKELY err.throw_exception();
}

//------------------------------------------------------------------------------

bool txn::drop_map(const char *name, bool throw_if_absent) {
  map_handle map;
  const int err = ::mdbx_dbi_open(handle_, name, MDBX_DB_ACCEDE, &map.dbi);
  switch (err) {
  case MDBX_SUCCESS:
    drop_map(map);
    return true;
  case MDBX_NOTFOUND:
  case MDBX_BAD_DBI:
    if (!throw_if_absent)
      return false;
    MDBX_CXX17_FALLTHROUGH /* fallthrough */;
  default:
    MDBX_CXX20_UNLIKELY error::throw_exception(err);
  }
}

bool txn::clear_map(const char *name, bool throw_if_absent) {
  map_handle map;
  const int err = ::mdbx_dbi_open(handle_, name, MDBX_DB_ACCEDE, &map.dbi);
  switch (err) {
  case MDBX_SUCCESS:
    clear_map(map);
    return true;
  case MDBX_NOTFOUND:
  case MDBX_BAD_DBI:
    if (!throw_if_absent)
      return false;
    MDBX_CXX17_FALLTHROUGH /* fallthrough */;
  default:
    MDBX_CXX20_UNLIKELY error::throw_exception(err);
  }
}

//------------------------------------------------------------------------------

void cursor_managed::close() {
  if (MDBX_UNLIKELY(!handle_))
    MDBX_CXX20_UNLIKELY error::throw_exception(MDBX_EINVAL);
  ::mdbx_cursor_close(handle_);
  handle_ = nullptr;
}

//------------------------------------------------------------------------------

__cold ::std::ostream &operator<<(::std::ostream &out, const slice &it) {
  out << "{";
  if (!it.is_valid())
    out << "INVALID." << it.length();
  else if (it.is_null())
    out << "NULL";
  else if (it.empty())
    out << "EMPTY->" << it.data();
  else {
    const slice root(it.head(std::min(it.length(), size_t(64))));
    out << it.length() << ".";
    if (root.is_printable())
      (out << "\"").write(root.char_ptr(), root.length()) << "\"";
    else
      out << root.encode_base58();
    if (root.length() < it.length())
      out << "...";
  }
  return out << "}";
}

__cold ::std::ostream &operator<<(::std::ostream &out, const pair &it) {
  return out << "{" << it.key << " => " << it.value << "}";
}

__cold ::std::ostream &operator<<(::std::ostream &out, const pair_result &it) {
  return out << "{" << (it.done ? "done: " : "non-done: ") << it.key << " => "
             << it.value << "}";
}

__cold ::std::ostream &operator<<(::std::ostream &out,
                                  const ::mdbx::env::geometry::size &it) {
  switch (it.bytes) {
  case ::mdbx::env::geometry::default_value:
    return out << "default";
  case ::mdbx::env::geometry::minimal_value:
    return out << "minimal";
  case ::mdbx::env::geometry::maximal_value:
    return out << "maximal";
  }

  const auto bytes = (it.bytes < 0) ? out << "-",
             size_t(-it.bytes)      : size_t(it.bytes);
  struct {
    size_t one;
    const char *suffix;
  } static const scales[] = {
#if MDBX_WORDBITS > 32
    {env_managed::geometry::EiB, "EiB"},
    {env_managed::geometry::EB, "EB"},
    {env_managed::geometry::PiB, "PiB"},
    {env_managed::geometry::PB, "PB"},
    {env_managed::geometry::TiB, "TiB"},
    {env_managed::geometry::TB, "TB"},
#endif
    {env_managed::geometry::GiB, "GiB"},
    {env_managed::geometry::GB, "GB"},
    {env_managed::geometry::MiB, "MiB"},
    {env_managed::geometry::MB, "MB"},
    {env_managed::geometry::KiB, "KiB"},
    {env_managed::geometry::kB, "kB"},
    {1, " bytes"}
  };

  for (const auto i : scales)
    if (bytes % i.one == 0)
      return out << bytes / i.one << i.suffix;

  assert(false);
  __unreachable();
  return out;
}

__cold ::std::ostream &operator<<(::std::ostream &out,
                                  const env::geometry &it) {
  return                                                                //
      out << "\tlower " << env::geometry::size(it.size_lower)           //
          << ",\n\tnow " << env::geometry::size(it.size_now)            //
          << ",\n\tupper " << env::geometry::size(it.size_upper)        //
          << ",\n\tgrowth " << env::geometry::size(it.growth_step)      //
          << ",\n\tshrink " << env::geometry::size(it.shrink_threshold) //
          << ",\n\tpagesize " << env::geometry::size(it.pagesize) << "\n";
}

__cold ::std::ostream &operator<<(::std::ostream &out,
                                  const env::operate_parameters &it) {
  return out << "{\n"                                 //
             << "\tmax_maps " << it.max_maps          //
             << ",\n\tmax_readers " << it.max_readers //
             << ",\n\tmode " << it.mode               //
             << ",\n\tdurability " << it.durability   //
             << ",\n\treclaiming " << it.reclaiming   //
             << ",\n\toptions " << it.options         //
             << "\n}";
}

__cold ::std::ostream &operator<<(::std::ostream &out, const env::mode &it) {
  switch (it) {
  case env::mode::readonly:
    return out << "readonly";
  case env::mode::write_file_io:
    return out << "write_file_io";
  case env::mode::write_mapped_io:
    return out << "write_mapped_io";
  default:
    return out << "mdbx::env::mode::invalid";
  }
}

__cold ::std::ostream &operator<<(::std::ostream &out,
                                  const env::durability &it) {
  switch (it) {
  case env::durability::robust_synchronous:
    return out << "robust_synchronous";
  case env::durability::half_synchronous_weak_last:
    return out << "half_synchronous_weak_last";
  case env::durability::lazy_weak_tail:
    return out << "lazy_weak_tail";
  case env::durability::whole_fragile:
    return out << "whole_fragile";
  default:
    return out << "mdbx::env::durability::invalid";
  }
}

__cold ::std::ostream &operator<<(::std::ostream &out,
                                  const env::reclaiming_options &it) {
  return out << "{"                                            //
             << "lifo: " << (it.lifo ? "yes" : "no")           //
             << ", coalesce: " << (it.coalesce ? "yes" : "no") //
             << "}";
}

__cold ::std::ostream &operator<<(::std::ostream &out,
                                  const env::operate_options &it) {
  static const char comma[] = ", ";
  const char *delimiter = "";
  out << "{";
  if (it.orphan_read_transactions) {
    out << delimiter << "orphan_read_transactions";
    delimiter = comma;
  }
  if (it.nested_write_transactions) {
    out << delimiter << "nested_write_transactions";
    delimiter = comma;
  }
  if (it.exclusive) {
    out << delimiter << "exclusive";
    delimiter = comma;
  }
  if (it.disable_readahead) {
    out << delimiter << "disable_readahead";
    delimiter = comma;
  }
  if (it.disable_clear_memory) {
    out << delimiter << "disable_clear_memory";
    delimiter = comma;
  }
  if (delimiter != comma)
    out << "default";
  return out << "}";
}

__cold ::std::ostream &operator<<(::std::ostream &out,
                                  const env_managed::create_parameters &it) {
  return out << "{\n"                                                        //
             << "\tfile_mode " << std::oct << it.file_mode_bits << std::dec  //
             << ",\n\tsubdirectory " << (it.use_subdirectory ? "yes" : "no") //
             << ",\n"
             << it.geometry << "}";
}

__cold ::std::ostream &operator<<(::std::ostream &out,
                                  const MDBX_log_level_t &it) {
  switch (it) {
  case MDBX_LOG_FATAL:
    return out << "LOG_FATAL";
  case MDBX_LOG_ERROR:
    return out << "LOG_ERROR";
  case MDBX_LOG_WARN:
    return out << "LOG_WARN";
  case MDBX_LOG_NOTICE:
    return out << "LOG_NOTICE";
  case MDBX_LOG_VERBOSE:
    return out << "LOG_VERBOSE";
  case MDBX_LOG_DEBUG:
    return out << "LOG_DEBUG";
  case MDBX_LOG_TRACE:
    return out << "LOG_TRACE";
  case MDBX_LOG_EXTRA:
    return out << "LOG_EXTRA";
  case MDBX_LOG_DONTCHANGE:
    return out << "LOG_DONTCHANGE";
  default:
    return out << "mdbx::log_level::invalid";
  }
}

__cold ::std::ostream &operator<<(::std::ostream &out,
                                  const MDBX_debug_flags_t &it) {
  if (it == MDBX_DBG_DONTCHANGE)
    return out << "DBG_DONTCHANGE";

  static const char comma[] = "|";
  const char *delimiter = "";
  out << "{";
  if (it & MDBX_DBG_ASSERT) {
    out << delimiter << "DBG_ASSERT";
    delimiter = comma;
  }
  if (it & MDBX_DBG_AUDIT) {
    out << delimiter << "DBG_AUDIT";
    delimiter = comma;
  }
  if (it & MDBX_DBG_JITTER) {
    out << delimiter << "DBG_JITTER";
    delimiter = comma;
  }
  if (it & MDBX_DBG_DUMP) {
    out << delimiter << "DBG_DUMP";
    delimiter = comma;
  }
  if (it & MDBX_DBG_LEGACY_MULTIOPEN) {
    out << delimiter << "DBG_LEGACY_MULTIOPEN";
    delimiter = comma;
  }
  if (it & MDBX_DBG_LEGACY_OVERLAP) {
    out << delimiter << "DBG_LEGACY_OVERLAP";
    delimiter = comma;
  }
  if (delimiter != comma)
    out << "DBG_NONE";
  return out << "}";
}

__cold ::std::ostream &operator<<(::std::ostream &out,
                                  const ::mdbx::error &err) {
  return out << err.what() << " (" << long(err.code()) << ")";
}

} // namespace mdbx
