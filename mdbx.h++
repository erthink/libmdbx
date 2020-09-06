//
// The libmdbx C++ API (preliminary)
//
// Reguires GNU C++ >= 5.1, clang >= 4.0, MSVC >= 19.0 (Visual Studio 2015).

/// \file mdbx.h++
/// \brief The libmdbx C++ API header file

#pragma once

#if (!defined(__cplusplus) || __cplusplus < 201103L) &&                        \
    !(defined(_MSC_VER) && _MSC_VER == 1900)
#error "C++11 or better is required"
#endif

#if (defined(_WIN32) || defined(_WIN64)) && MDBX_AVOID_CRT
#error "CRT is required for C++ API, the MDBX_AVOID_CRT option must be disabled"
#endif /* Windows */

#ifndef __has_include
#define __has_include(header) (0)
#endif /* __has_include */

#if defined(__has_include) && __has_include(<version>)
#include <version>
#endif /* <version> */

#ifndef NOMINMAX
#define NOMINMAX
#endif

#include <algorithm>   // for std::min/max
#include <cassert>     // for assert()
#include <cstring>     // for std::strlen, str:memcmp
#include <exception>   // for std::exception_ptr
#include <memory>      // for std::uniq_ptr
#include <ostream>     // for std::ostream
#include <sstream>     // for std::ostringstream
#include <stdexcept>   // for std::invalid_argument
#include <string>      // for std::string
#include <type_traits> // for std::is_pod<>, etc.
#include <vector>      // for std::vector<> as template args

#if defined(__cpp_lib_bit_cast) && __cpp_lib_bit_cast >= 201806L
#include <bit>
#endif

#if defined(__cpp_lib_memory_resource) && __cpp_lib_memory_resource >= 201603L
#include <memory_resource>
#endif

#if defined(__cpp_lib_string_view) && __cpp_lib_string_view >= 201606L
#include <string_view>
#endif

#if defined(__cpp_lib_filesystem) && __cpp_lib_filesystem >= 201703L
#include <filesystem>
#endif

#include "mdbx.h"

/* Workaround for old compilers without properly support for constexpr. */
#if !defined(DOXYGEN) && !defined(__cpp_constexpr) ||                          \
    __cpp_constexpr < 201304L ||                                               \
    (defined(__GNUC__) && __GNUC__ < 6 && !defined(__clang__)) ||              \
    (defined(_MSC_VER) && _MSC_VER < 1910) ||                                  \
    (defined(__clang__) && __clang_major__ < 4)
#define constexpr inline
#endif /* __cpp_constexpr < 201304 */

#if !defined(cxx17_constexpr)
#if defined(DOXYGEN) ||                                                        \
    defined(__cpp_constexpr) && __cpp_constexpr >= 201603L &&                  \
        ((defined(_MSC_VER) && _MSC_VER >= 1915) ||                            \
         (defined(__clang__) && __clang_major__ > 5) ||                        \
         (defined(__GNUC__) && __GNUC__ > 7) ||                                \
         (!defined(__GNUC__) && !defined(__clang__) && !defined(_MSC_VER)))
#define cxx17_constexpr constexpr
#else
#define cxx17_constexpr inline
#endif
#endif /* cxx17_constexpr */

#ifndef cxx20_constexpr
#if defined(DOXYGEN) || defined(__cpp_lib_is_constant_evaluated) &&            \
                            __cpp_lib_is_constant_evaluated >= 201811L &&      \
                            defined(__cpp_lib_constexpr_string) &&             \
                            __cpp_lib_constexpr_string >= 201907L
#define cxx20_constexpr constexpr
#else
#define cxx20_constexpr inline
#endif
#endif /* cxx20_constexpr */

#if !defined(CONSTEXPR_ASSERT)
#if defined NDEBUG
#define CONSTEXPR_ASSERT(expr) void(0)
#else
#define CONSTEXPR_ASSERT(expr) ((expr) ? void(0) : [] { assert(!#expr); }())
#endif
#endif /* constexpr_assert */

#ifndef mdbx_likely
#if defined(DOXYGEN) ||                                                        \
    (defined(__GNUC__) || __has_builtin(__builtin_expect)) &&                  \
        !defined(__COVERITY__)
#define mdbx_likely(cond) __builtin_expect(!!(cond), 1)
#else
#define mdbx_likely(x) (x)
#endif
#endif /* mdbx_likely */

#ifndef mdbx_unlikely
#if defined(DOXYGEN) ||                                                        \
    (defined(__GNUC__) || __has_builtin(__builtin_expect)) &&                  \
        !defined(__COVERITY__)
#define mdbx_unlikely(cond) __builtin_expect(!!(cond), 0)
#else
#define mdbx_unlikely(x) (x)
#endif
#endif /* mdbx_unlikely */

#ifndef cxx17_attribute_fallthrough
#if defined(DOXYGEN) ||                                                        \
    (__has_cpp_attribute(fallthrough) &&                                       \
     (!defined(__clang__) || __clang__ > 4)) ||                                \
    __cplusplus >= 201703L
#define cxx17_attribute_fallthrough [[fallthrough]]
#else
#define cxx17_attribute_fallthrough
#endif
#endif /* cxx17_attribute_fallthrough */

#ifndef cxx20_attribute_likely
#if defined(DOXYGEN) || __has_cpp_attribute(likely)
#define cxx20_attribute_likely [[likely]]
#else
#define cxx20_attribute_likely
#endif
#endif /* cxx20_attribute_likely */

#ifndef cxx20_attribute_unlikely
#if defined(DOXYGEN) || __has_cpp_attribute(unlikely)
#define cxx20_attribute_unlikely [[unlikely]]
#else
#define cxx20_attribute_unlikely
#endif
#endif /* cxx20_attribute_unlikely */

#ifdef _MSC_VER
#pragma warning(push, 4)
#pragma warning(disable : 4251) /* 'std::FOO' needs to have dll-interface to   \
                                   be used by clients of 'mdbx::BAR' */
#pragma warning(disable : 4275) /* non dll-interface 'std::FOO' used as        \
                                   base for dll-interface 'mdbx::BAR' */
/* MSVC is mad and can generate this warning for its own intermediate
 * automatically generated code, which becomes unreachable after some kinds of
 * optimization (copy ellision, etc). */
#pragma warning(disable : 4702) /* unreachable code */
#endif                          /* _MSC_VER (warnings) */

//------------------------------------------------------------------------------
/// \defgroup cxx_api C++ API
/// @{

namespace mdbx {

// Functions whose signature depends on the byte type
// must be strictly defined as inline!
#if defined(DOXYGEN) || (defined(__cpp_char8_t) && __cpp_char8_t >= 201811)
using byte = char8_t;
#else
using byte = unsigned char;
#endif /* __cpp_char8_t >= 201811*/

/// \copydoc MDBX_version_info
using version_info = ::MDBX_version_info;
/// \brief Returns libmdbx version information.
constexpr const version_info &get_version() noexcept;
/// \copydoc MDBX_build_info
using build_info = ::MDBX_build_info;
/// \brief Resutrns libmdbx build information.
constexpr const build_info &get_build() noexcept;

/// \brief Returns field offset in the container class.
template <class CONTAINER, class MEMBER>
static constexpr ptrdiff_t offset_of(const MEMBER CONTAINER::*const member) {
  return static_cast<const char *>(static_cast<const void *>(
             &(static_cast<const CONTAINER *>(nullptr)->*member))) -
         static_cast<const char *>(nullptr);
}

/// \brief Returns const pointer to container class instance
/// by const pointer to the member field.
template <class CONTAINER, class MEMBER>
static constexpr const CONTAINER *owner_of(const MEMBER *ptr,
                                           const MEMBER CONTAINER::*member) {
  return static_cast<const CONTAINER *>(static_cast<const void *>(
      static_cast<const char *>(static_cast<const void *>(ptr)) -
      offset_of(member)));
}

/// \brief Returns non-const pointer to container class instance
/// by non-const pointer to the member field.
template <class CONTAINER, class MEMBER>
static constexpr CONTAINER *owner_of(MEMBER *ptr,
                                     const MEMBER CONTAINER::*member) {
  return static_cast<CONTAINER *>(static_cast<void *>(
      static_cast<char *>(static_cast<void *>(ptr)) - offset_of(member)));
}

/// \brief constexpr-compatible strlen().
static cxx17_constexpr size_t strlen(const char *c_str) noexcept;

struct slice;
class env_ref;
class env;
class txn_ref;
class txn;
class cursor_ref;
class cursor;

/// \brief Legacy default allocator
/// but it is recommended to use \ref polymorphic_allocator.
using legacy_allocator = ::std::string::allocator_type;

#if defined(DOXYGEN) ||                                                        \
    defined(__cpp_lib_memory_resource) && __cpp_lib_memory_resource >= 201603L
/// \brief Default polymorphic allocator for modern code
using polymorphic_allocator = ::std::pmr::string::allocator_type;
#endif /* __cpp_lib_memory_resource >= 201603L */

/// \brief Default singe-byte string.
template <class ALLOCATOR = legacy_allocator>
using string = ::std::basic_string<char, ::std::char_traits<char>, ALLOCATOR>;

using filehandle = ::mdbx_filehandle_t;
#if defined(DOXYGEN) ||                                                        \
    (defined(__cpp_lib_filesystem) && __cpp_lib_filesystem >= 201703L &&       \
     (!defined(__MAC_OS_X_VERSION_MIN_REQUIRED) ||                             \
      __MAC_OS_X_VERSION_MIN_REQUIRED >= 101500))
using path = std::filesystem::path;
#elif defined(_WIN32) || defined(_WIN64)
using path = ::std::wstring;
#else
using path = ::std::string;
#endif

/// \brief Transfers C++ exceptions thru C callbacks.
/// \details Implements saving exceptions before returning
/// from an C++'s environment to the intermediate C code and re-throwing after
/// returning from C to the C++'s environment.
class LIBMDBX_API_TYPE exception_thunk {
  ::std::exception_ptr captured_;

public:
  exception_thunk() noexcept = default;
  exception_thunk(const exception_thunk &) = delete;
  exception_thunk(exception_thunk &&) = delete;
  exception_thunk &operator=(const exception_thunk &) = delete;
  exception_thunk &operator=(exception_thunk &&) = delete;
  inline bool is_clean() const noexcept;
  inline void capture() noexcept;
  inline void rethrow_captured() const;
};

/// \brief Implements error information and throwing corresponding exceptions.
class LIBMDBX_API_TYPE error {
  MDBX_error_t code_;
  inline error &operator=(MDBX_error_t error_code) noexcept;

public:
  constexpr error(MDBX_error_t error_code) noexcept;
  error(const error &) = default;
  error(error &&) = default;
  error &operator=(const error &) = default;
  error &operator=(error &&) = default;

  constexpr friend bool operator==(const error &a, const error &b) noexcept;
  constexpr friend bool operator!=(const error &a, const error &b) noexcept;

  constexpr bool is_success() const noexcept;
  constexpr bool is_result_true() const noexcept;
  constexpr bool is_result_false() const noexcept;
  constexpr bool is_failure() const noexcept;

  /// \brief Returns error code.
  constexpr MDBX_error_t code() const noexcept;

  /// \brief Returns message for MDBX's errors only and "SYSTEM" for others.
  const char *what() const noexcept;

  /// \brief Returns message for any errors.
  ::std::string message() const;

  /// \brief Returns true for MDBX's errors.
  constexpr bool is_mdbx_error() const noexcept;
  [[noreturn]] void panic(const char *context_where,
                          const char *func_who) const noexcept;
  [[noreturn]] void throw_exception() const;
  [[noreturn]] static inline void throw_exception(int error_code);
  inline void throw_on_failure() const;
  inline void success_or_throw() const;
  inline void success_or_throw(const exception_thunk &) const;
  inline void panic_on_failure(const char *context_where,
                               const char *func_who) const noexcept;
  inline void success_or_panic(const char *context_where,
                               const char *func_who) const noexcept;
  static inline void throw_on_nullptr(const void *ptr, MDBX_error_t error_code);
  static inline void success_or_throw(MDBX_error_t error_code);
  static void success_or_throw(int error_code) {
    success_or_throw(static_cast<MDBX_error_t>(error_code));
  }
  static inline void throw_on_failure(int error_code);
  static inline bool boolean_or_throw(int error_code);
  static inline void success_or_throw(int error_code, const exception_thunk &);
  static inline void panic_on_failure(int error_code, const char *context_where,
                                      const char *func_who) noexcept;
  static inline void success_or_panic(int error_code, const char *context_where,
                                      const char *func_who) noexcept;
};

/// \brief Base class for all libmdbx's exceptions that are corresponds
/// to libmdbx errors.
///
/// \see MDBX_error_t
class LIBMDBX_API_TYPE exception : public ::std::runtime_error {
  using base = ::std::runtime_error;
  ::mdbx::error error_;

public:
  exception(const ::mdbx::error &) noexcept;
  exception(const exception &) = default;
  exception(exception &&) = default;
  exception &operator=(const exception &) = default;
  exception &operator=(exception &&) = default;
  virtual ~exception() noexcept;
  const mdbx::error error() const noexcept { return error_; }
};

/// \brief Fatal exception that lead termination anyway
/// in dangerous unrecoverable cases.
class LIBMDBX_API_TYPE fatal : public exception {
  using base = exception;

public:
  fatal(const ::mdbx::error &) noexcept;
  fatal(const exception &src) noexcept : fatal(src.error()) {}
  fatal(exception &&src) noexcept : fatal(src.error()) {}
  fatal(const fatal &src) noexcept : fatal(src.error()) {}
  fatal(fatal &&src) noexcept : fatal(src.error()) {}
  fatal &operator=(fatal &&) = default;
  fatal &operator=(const fatal &) = default;
  virtual ~fatal() noexcept;
};

#define MDBX_DECLARE_EXCEPTION(NAME)                                           \
  struct LIBMDBX_API_TYPE NAME : public exception {                            \
    NAME(const ::mdbx::error &);                                               \
    virtual ~NAME() noexcept;                                                  \
  }
MDBX_DECLARE_EXCEPTION(bad_map_id);
MDBX_DECLARE_EXCEPTION(bad_transaction);
MDBX_DECLARE_EXCEPTION(bad_value_size);
MDBX_DECLARE_EXCEPTION(db_corrupted);
MDBX_DECLARE_EXCEPTION(db_full);
MDBX_DECLARE_EXCEPTION(db_invalid);
MDBX_DECLARE_EXCEPTION(db_too_large);
MDBX_DECLARE_EXCEPTION(db_unable_extend);
MDBX_DECLARE_EXCEPTION(db_version_mismatch);
MDBX_DECLARE_EXCEPTION(db_wanna_write_for_recovery);
MDBX_DECLARE_EXCEPTION(incompatible_operation);
MDBX_DECLARE_EXCEPTION(internal_page_full);
MDBX_DECLARE_EXCEPTION(internal_problem);
MDBX_DECLARE_EXCEPTION(key_exists);
MDBX_DECLARE_EXCEPTION(key_mismatch);
MDBX_DECLARE_EXCEPTION(max_maps_reached);
MDBX_DECLARE_EXCEPTION(max_readers_reached);
MDBX_DECLARE_EXCEPTION(multivalue);
MDBX_DECLARE_EXCEPTION(no_data);
MDBX_DECLARE_EXCEPTION(not_found);
MDBX_DECLARE_EXCEPTION(operation_not_permited);
MDBX_DECLARE_EXCEPTION(permission_denied_or_not_writeable);
MDBX_DECLARE_EXCEPTION(reader_slot_busy);
MDBX_DECLARE_EXCEPTION(remote_media);
MDBX_DECLARE_EXCEPTION(something_busy);
MDBX_DECLARE_EXCEPTION(thread_mismatch);
MDBX_DECLARE_EXCEPTION(transaction_full);
MDBX_DECLARE_EXCEPTION(transaction_overlapping);
#undef MDBX_DECLARE_EXCEPTION

[[noreturn]] LIBMDBX_API void throw_too_small_target_buffer();
[[noreturn]] LIBMDBX_API void throw_max_length_exceeded();
[[noreturn]] LIBMDBX_API void throw_out_range();
cxx14_constexpr size_t check_length(size_t bytes);

//------------------------------------------------------------------------------

/// \brief References a data located outside the slice.
///
/// The `slice` is similar in many ways to `std::string_view`, but it
/// implements specific capabilities and manipulates with bytes but not a
/// characters.
///
/// \copydetails MDBX_val
struct LIBMDBX_API_TYPE slice : public ::MDBX_val {
  /// \todo slice& operator<<(slice&, ...) for reading
  /// \todo key-to-value (parse/unpack) functions
  /// \todo template<class X> key(X); for decoding keys while reading

  enum { max_length = MDBX_MAXDATASIZE };

  /// \brief Create an empty slice.
  constexpr slice() noexcept;

  /// \brief Create a slice that refers to [0,bytes-1] of memory bytes pointed
  /// by ptr.
  cxx14_constexpr slice(const void *ptr, size_t bytes);

  /// \brief Create a slice that refers to [begin,end] of memory bytes.
  cxx14_constexpr slice(const void *begin, const void *end);

  /// \brief Create a slice that refers to text[0,strlen(text)-1].
  template <size_t SIZE>
  cxx14_constexpr slice(const char (&text)[SIZE]) noexcept
      : slice(text, SIZE - 1) {
    static_assert(SIZE > 0 && text[SIZE - 1] == '\0',
                  "Must be a null-terminated C-string");
  }
  /// \brief Create a slice that refers to c_str[0,strlen(c_str)-1].
  explicit cxx17_constexpr slice(const char *c_str);

  /// \brief Create a slice that refers to the contents of "str".
  /* 'explicit' to avoid reference to the temporary std::string instance */
  template <class C, class T, class A>
  explicit cxx20_constexpr slice(const ::std::basic_string<C, T, A> &str)
      : slice(str.data(), str.length() * sizeof(C)) {}

  cxx14_constexpr slice(const MDBX_val &src);
  constexpr slice(const slice &) noexcept = default;
#if defined(DOXYGEN) ||                                                        \
    (defined(__cpp_lib_string_view) && __cpp_lib_string_view >= 201606L)
  /// \brief Create a slice that refers to the same contents as "sv"
  template <class C, class T>
  explicit cxx14_constexpr slice(const ::std::basic_string_view<C, T> &sv)
      : slice(sv.data(), sv.data() + sv.length()) {}
#endif /* __cpp_lib_string_view >= 201606L */

  inline slice(MDBX_val &&src);
  inline slice(slice &&src) noexcept;
#if defined(DOXYGEN) ||                                                        \
    (defined(__cpp_lib_string_view) && __cpp_lib_string_view >= 201606L)
  template <class C, class T>
  slice(::std::basic_string_view<C, T> &&sv) : slice(sv) {
    sv = {};
  }
#endif /* __cpp_lib_string_view >= 201606L */

  template <size_t SIZE>
  static cxx14_constexpr slice wrap(const char (&text)[SIZE]) {
    return slice(text);
  }

  template <typename POD> cxx14_constexpr static slice wrap(const POD &pod) {
    static_assert(::std::is_standard_layout<POD>::value &&
                      !std::is_pointer<POD>::value,
                  "Must be a standard layout type!");
    return slice(&pod, sizeof(pod));
  }

  inline slice &assign(const void *ptr, size_t bytes);
  inline slice &assign(const slice &src) noexcept;
  inline slice &assign(const ::MDBX_val &src);
  inline slice &assign(slice &&src) noexcept;
  inline slice &assign(::MDBX_val &&src);
  inline slice &assign(const void *begin, const void *end);
  template <class C, class T, class A>
  slice &assign(const ::std::basic_string<C, T, A> &str) {
    return assign(str.data(), str.length() * sizeof(C));
  }
  inline slice &assign(const char *c_str);
#if defined(DOXYGEN) ||                                                        \
    (defined(__cpp_lib_string_view) && __cpp_lib_string_view >= 201606L)
  template <class C, class T>
  slice &assign(const ::std::basic_string_view<C, T> &view) {
    return assign(view.begin(), view.end());
  }
  template <class C, class T>
  slice &assign(::std::basic_string_view<C, T> &&view) {
    assign(view);
    view = {};
    return *this;
  }
#endif /* __cpp_lib_string_view >= 201606L */

  slice &operator=(const slice &) noexcept = default;
  inline slice &operator=(slice &&src) noexcept;
  inline slice &operator=(::MDBX_val &&src);
#if defined(DOXYGEN) ||                                                        \
    (defined(__cpp_lib_string_view) && __cpp_lib_string_view >= 201606L)
  template <class C, class T>
  slice &operator=(const ::std::basic_string_view<C, T> &view) {
    return assign(view);
  }
  template <class C, class T>
  slice &operator=(::std::basic_string_view<C, T> &&view) {
    return assign(view);
  }
#endif /* __cpp_lib_string_view >= 201606L */

  template <class C = char, class T = ::std::char_traits<C>,
            class A = legacy_allocator>
  cxx20_constexpr ::std::basic_string<C, T, A>
  string(const A &allocator = A()) const {
    static_assert(sizeof(C) == 1, "Must be single byte characters");
    return ::std::basic_string<C, T, A>(char_ptr(), length(), allocator);
  }

  template <class C, class T, class A>
  cxx20_constexpr operator ::std::basic_string<C, T, A>() const {
    return this->string<C, T, A>();
  }

  /// \brief Fills the buffer by hexadecimal data dump of slice content.
  /// \throws std::length_error if given buffer is too small.
  char *to_hex(char *dest, size_t dest_size, bool uppercase = false,
               unsigned wrap_width = 0) const;

  /// \brief Returns the buffer size in bytes needed for hexadecimal data dump
  /// of slice content.
  constexpr size_t to_hex_bytes(unsigned wrap_width = 0) const noexcept {
    const size_t bytes = length() << 1;
    return wrap_width ? bytes + bytes / wrap_width : bytes;
  }

  /// \brief Fills the buffer with data converted from hexadecimal dump
  /// from slice content.
  /// \throws std::length_error if given buffer is too small.
  byte *from_hex(byte *dest, size_t dest_size,
                 bool ignore_spaces = false) const;

  /// \brief Returns the buffer size in bytes needed for conversion
  /// hexadecimal dump from slice content to data.
  constexpr size_t from_hex_bytes() const noexcept { return length() >> 1; }

  /// \brief Fills the buffer by [Base58](https://en.wikipedia.org/wiki/Base58)
  /// data dump of slice content.
  /// \throws std::length_error if given buffer is too small.
  char *to_base58(char *dest, size_t dest_size, unsigned wrap_width = 0) const;

  /// \brief Returns the buffer size in bytes needed for
  /// [Base58](https://en.wikipedia.org/wiki/Base58) data dump of slice content.
  constexpr size_t to_base58_bytes(unsigned wrap_width = 0) const noexcept {
    const size_t bytes = length() / 8 * 11 + (length() % 8 * 43 + 31) / 32;
    return wrap_width ? bytes + bytes / wrap_width : bytes;
  }

  /// \brief Fills the buffer with data converted from
  /// [Base58](https://en.wikipedia.org/wiki/Base58) dump from slice content.
  /// \throws std::length_error if given buffer is too small.
  byte *from_base58(byte *dest, size_t dest_size,
                    bool ignore_spaces = false) const;

  /// vReturns the buffer size in bytes needed for conversion
  /// [Base58](https://en.wikipedia.org/wiki/Base58) dump to data.
  constexpr size_t from_base58_bytes() const noexcept {
    return length() / 11 * 8 + length() % 11 * 32 / 43;
  }

  /// \brief Fills the buffer by [Base64](https://en.wikipedia.org/wiki/Base64)
  /// data dump.
  /// \throws std::length_error if given buffer is too small.
  char *to_base64(char *dest, size_t dest_size, unsigned wrap_width = 0) const;

  /// \brief Returns the buffer size in bytes needed for
  /// [Base64](https://en.wikipedia.org/wiki/Base64) data dump.
  constexpr size_t to_base64_bytes(unsigned wrap_width = 0) const noexcept {
    const size_t bytes = (length() + 2) / 3 * 4;
    return wrap_width ? bytes + bytes / wrap_width : bytes;
  }

  /// \brief Fills the buffer with data converted from
  /// [Base64](https://en.wikipedia.org/wiki/Base64) dump.
  /// \throws std::length_error if given buffer is too small.
  byte *from_base64(byte *dest, size_t dest_size,
                    bool ignore_spaces = false) const;

  /// \brief Returns the buffer size in bytes needed for conversion
  /// [Base64](https://en.wikipedia.org/wiki/Base64) dump to data.
  constexpr size_t from_base64_bytes() const noexcept {
    return (length() + 3) / 4 * 3;
  }

  /// \brief Returns a string with a hexadecimal dump of the slice content.
  template <class ALLOCATOR = legacy_allocator>
  inline ::mdbx::string<ALLOCATOR>
  hex_encode(bool uppercase = false,
             const ALLOCATOR &allocator = ALLOCATOR()) const;

  /// \brief Decodes hexadecimal dump from the slice content into returned data
  /// string.
  template <class ALLOCATOR = legacy_allocator>
  inline ::mdbx::string<ALLOCATOR>
  hex_decode(const ALLOCATOR &allocator = ALLOCATOR()) const;

  /// \brief Returns a string with a
  /// [Base58](https://en.wikipedia.org/wiki/Base58) dump of the slice content.
  template <class ALLOCATOR = legacy_allocator>
  inline ::mdbx::string<ALLOCATOR>
  base58_encode(const ALLOCATOR &allocator = ALLOCATOR()) const;

  /// \brief Decodes [Base58](https://en.wikipedia.org/wiki/Base58) dump
  /// from the slice content into returned data string.
  template <class ALLOCATOR = legacy_allocator>
  inline ::mdbx::string<ALLOCATOR>
  base58_decode(const ALLOCATOR &allocator = ALLOCATOR()) const;

  /// \brief Returns a string with a
  /// [Base64](https://en.wikipedia.org/wiki/Base64) dump of the slice content.
  template <class ALLOCATOR = legacy_allocator>
  inline ::mdbx::string<ALLOCATOR>
  base64_encode(const ALLOCATOR &allocator = ALLOCATOR()) const;

  /// \brief Decodes [Base64](https://en.wikipedia.org/wiki/Base64) dump
  /// from the slice content into returned data string.
  template <class ALLOCATOR = legacy_allocator>
  inline ::mdbx::string<ALLOCATOR>
  base64_decode(const ALLOCATOR &allocator = ALLOCATOR()) const;

  /// \brief Checks whether the content of the slice is printable.
  /// \param [in] disable_utf8 By default if `disable_utf8` is `false` function
  /// checks that content bytes are printable ASCII-7 characters or a valid UTF8
  /// sequences. Otherwise, if if `disable_utf8` is `true` function checks that
  /// content bytes are printable extended 8-bit ASCII codes.
  __nothrow_pure_function bool
  is_printable(bool disable_utf8 = false) const noexcept;

  /// \brief Checks whether the content of the slice is a hexadecimal dump.
  /// \param [in] ignore_spaces If `true` function will skips spaces surrounding
  /// (before, between and after) a encoded bytes. However, spaces should not
  /// break a pair of characters encoding a single byte.
  __nothrow_pure_function bool
  is_hex(bool ignore_spaces = false) const noexcept;
  __nothrow_pure_function bool

  /// \brief Checks whether the content of the slice is a
  /// [Base58](https://en.wikipedia.org/wiki/Base58) dump.
  /// \param [in] ignore_spaces If `true` function will skips spaces surrounding
  /// (before, between and after) a encoded bytes. However, spaces should not
  /// break a code group of characters.
  is_base58(bool ignore_spaces = false) const noexcept;
  __nothrow_pure_function bool

  /// \brief Checks whether the content of the slice is a
  /// [Base64](https://en.wikipedia.org/wiki/Base64) dump.
  /// \param [in] ignore_spaces If `true` function will skips spaces surrounding
  /// (before, between and after) a encoded bytes. However, spaces should not
  /// break a code group of characters.
  is_base64(bool ignore_spaces = false) const noexcept;

#if defined(DOXYGEN) ||                                                        \
    (defined(__cpp_lib_string_view) && __cpp_lib_string_view >= 201606L)
  /// \brief Return a string_view that references the same data as this slice.
  template <class C, class T>
  constexpr explicit operator ::std::basic_string_view<C, T>() const noexcept {
    static_assert(sizeof(C) == 1, "Must be single byte characters");
    return ::std::basic_string_view<C, T>(char_ptr(), length());
  }

  /// \brief Return a string_view that references the same data as this slice.
  template <class C = char, class T = ::std::char_traits<C>>
  constexpr ::std::basic_string_view<C, T> string_view() const noexcept {
    static_assert(sizeof(C) == 1, "Must be single byte characters");
    return ::std::basic_string_view<C, T>(char_ptr(), length());
  }
#endif /* __cpp_lib_string_view >= 201606L */

  inline void swap(slice &other) noexcept;
#if defined(DOXYGEN) ||                                                        \
    (defined(__cpp_lib_string_view) && __cpp_lib_string_view >= 201606L)
  template <class C, class T>
  void swap(::std::basic_string_view<C, T> &view) noexcept {
    static_assert(sizeof(C) == 1, "Must be single byte characters");
    const auto temp = ::std::basic_string_view<C, T>(*this);
    *this = view;
    view = temp;
  }
#endif /* __cpp_lib_string_view >= 201606L */

  /// \brief Returns casted to pointer to byte an address of data.
  constexpr const byte *byte_ptr() const noexcept;

  /// \brief Returns casted to pointer to char an address of data.
  constexpr const char *char_ptr() const noexcept;

  /// \brief Return a pointer to the beginning of the referenced data.
  constexpr const void *data() const noexcept;

  /// \brief Returns the number of bytes.
  constexpr size_t length() const noexcept;

  /// \brief Checks whether the slice is empty.
  constexpr bool empty() const noexcept;

  /// \brief Checks whether the slice data pointer is nullptr.
  constexpr bool is_null() const noexcept;

  /// \brief Returns the number of bytes.
  constexpr size_t size() const noexcept;

  /// \brief Returns true if slice is not empty.
  constexpr operator bool() const noexcept;

  /// \brief Depletes content of slice and make it invalid.
  inline void invalidate() noexcept;

  /// \brief Makes the slice empty and referencing to nothing.
  inline void clear() noexcept;

  /// \brief Drops the first "n" bytes from this slice.
  /// \pre REQUIRES: `n <= size()`
  inline void remove_prefix(size_t n) noexcept;

  /// \brief Drops the last "n" bytes from this slice.
  /// \pre REQUIRES: `n <= size()`
  inline void remove_suffix(size_t n) noexcept;

  /// \brief Drops the first "n" bytes from this slice.
  /// \throws std::out_of_range if `n > size()`
  inline void safe_remove_prefix(size_t n);

  /// \brief Drops the last "n" bytes from this slice.
  /// \throws std::out_of_range if `n > size()`
  inline void safe_remove_suffix(size_t n);

  /// \brief Checks if the data starts with the given prefix.
  __nothrow_pure_function inline bool
  starts_with(const slice &prefix) const noexcept;

  /// \brief Checks if the data ends with the given suffix.
  __nothrow_pure_function inline bool
  ends_with(const slice &suffix) const noexcept;

  /// \brief Returns the nth byte in the referenced data.
  /// \pre REQUIRES: `n < size()`
  inline byte operator[](size_t n) const noexcept;

  /// \brief Returns the nth byte in the referenced data with bounds checking.
  /// \throws std::out_of_range if `n >= size()`
  inline byte at(size_t n) const;

  /// \brief Returns the first "n" bytes of the slice.
  /// \pre REQUIRES: `n <= size()`
  inline slice head(size_t n) const noexcept;

  /// \brief Returns the last "n" bytes of the slice.
  /// \pre REQUIRES: `n <= size()`
  inline slice tail(size_t n) const noexcept;

  /// \brief Returns the middle "n" bytes of the slice.
  /// \pre REQUIRES: `from + n <= size()`
  inline slice middle(size_t from, size_t n) const noexcept;

  /// \brief Returns the first "n" bytes of the slice.
  /// \throws std::out_of_range if `n >= size()`
  inline slice safe_head(size_t n) const;

  /// \brief Returns the last "n" bytes of the slice.
  /// \throws std::out_of_range if `n >= size()`
  inline slice safe_tail(size_t n) const;

  /// \brief Returns the middle "n" bytes of the slice.
  /// \throws std::out_of_range if `from + n >= size()`
  inline slice safe_middle(size_t from, size_t n) const;

  /// \brief Returns the hash value of referenced data.
  /// \attention Function implementation and returned hash values may changed
  /// version to version, and in future the t1ha3 will be used here. Therefore
  /// values obtained from this function shouldn't be persisted anywhere.
  __nothrow_pure_function cxx14_constexpr size_t hash_value() const noexcept;

  /// \brief Three-way fast non-lexicographically length-based comparison.
  /// \return value:
  ///   == 0 if "a" == "b",
  ///   <  0 if "a" shorter than "b",
  ///   >  0 if "a" longer than "b",
  ///   <  0 if "a" length-equal and lexicographically less than "b",
  ///   >  0 if "a" length-equal and lexicographically great than "b".
  __nothrow_pure_function static inline intptr_t
  compare_fast(const slice &a, const slice &b) noexcept;

  /// \brief Three-way lexicographically comparison.
  /// \return value:
  ///   <  0 if "a" <  "b",
  ///   == 0 if "a" == "b",
  ///   >  0 if "a" >  "b".
  __nothrow_pure_function static inline intptr_t
  compare_lexicographically(const slice &a, const slice &b) noexcept;
  friend inline bool operator==(const slice &a, const slice &b) noexcept;
  friend inline bool operator<(const slice &a, const slice &b) noexcept;
  friend inline bool operator>(const slice &a, const slice &b) noexcept;
  friend inline bool operator<=(const slice &a, const slice &b) noexcept;
  friend inline bool operator>=(const slice &a, const slice &b) noexcept;
  friend inline bool operator!=(const slice &a, const slice &b) noexcept;

  /// \brief Checks the slice is not refers to null address or has zero length.
  constexpr bool is_valid() const noexcept {
    return !(iov_base == nullptr && iov_len != 0);
  }
  /// \brief Build an invalid slice which non-zero length and refers to null
  /// address.
  constexpr static slice invalid() noexcept { return slice(size_t(-1)); }

protected:
  constexpr slice(size_t invalid_lenght) noexcept
      : ::MDBX_val({nullptr, invalid_lenght}) {}
};

//------------------------------------------------------------------------------

/// \brief The chunk of data stored inside the buffer or located outside it.
template <class ALLOCATOR = legacy_allocator> class buffer {
  friend class txn_ref;
  using silo = ::mdbx::string<ALLOCATOR>;
  silo silo_;
  ::mdbx::slice slice_;

  void insulate() {
    assert(is_reference());
    silo_.assign(slice_.char_ptr(), slice_.length());
    slice_.iov_base = const_cast<char *>(silo_.data());
  }

  __nothrow_pure_function cxx20_constexpr const byte *
  silo_begin() const noexcept {
    return static_cast<const byte *>(static_cast<const void *>(silo_.data()));
  }

  __nothrow_pure_function cxx20_constexpr const byte *
  silo_end() const noexcept {
    return silo_begin() + silo_.capacity();
  }

  struct data_preserver : public exception_thunk {
    static int callback(void *context, MDBX_val *target, const void *src,
                        size_t bytes) noexcept;
    constexpr operator MDBX_preserve_func() const noexcept { return callback; }
  };

public:
  /// \todo buffer& operator<<(buffer&, ...) for writing
  /// \todo buffer& operator>>(buffer&, ...) for reading (deletages to slice)
  /// \todo template<class X> key(X) for encoding keys while writing

  using allocator_type = ALLOCATOR;
  enum : size_t {
    max_length = MDBX_MAXDATASIZE,
    default_shrink_treshold = 1024
  };

  /// \brief Returns the associated allocator.
  cxx20_constexpr allocator_type get_allocator() const {
    return silo_.get_allocator();
  }

  /// \brief Checks whether data chunk stored inside the buffer, otherwise
  /// buffer just refers to data located outside the buffer.
  __nothrow_pure_function cxx20_constexpr bool
  is_freestanding() const noexcept {
    return size_t(byte_ptr() - silo_begin()) < silo_.capacity();
  }

  /// \brief Checks whether the buffer just refers to data located outside
  /// the buffer, rather than stores it.
  __nothrow_pure_function cxx20_constexpr bool is_reference() const noexcept {
    return !is_freestanding();
  }

  /// \brief Returns the number of bytes that can be held in currently allocated
  /// storage.
  __nothrow_pure_function cxx20_constexpr size_t capacity() const noexcept {
    return is_freestanding() ? silo_.capacity() : 0;
  }

  /// \brief Returns the number of bytes that available in currently allocated
  /// storage ahead of the currently beginning of data.
  __nothrow_pure_function cxx20_constexpr size_t headroom() const noexcept {
    return is_freestanding() ? slice_.byte_ptr() - silo_begin() : 0;
  }

  /// \brief Returns the number of bytes that available in currently allocated
  /// storage afther of the currently data end.
  __nothrow_pure_function cxx20_constexpr size_t tailroom() const noexcept {
    return is_freestanding() ? capacity() - headroom() - slice_.length() : 0;
  }

  /// \brief Returns casted to const pointer to byte an address of data.
  constexpr const byte *byte_ptr() const noexcept { return slice_.byte_ptr(); }

  /// \brief Returns casted to pointer to byte an address of data.
  /// \pre REQUIRES: The buffer should store data chunk, but not referenced to
  /// an external one.
  constexpr byte *byte_ptr() noexcept {
    assert(is_freestanding());
    return const_cast<byte *>(slice_.byte_ptr());
  }

  /// \brief Returns casted to const pointer to char an address of data.
  constexpr const char *char_ptr() const noexcept { return slice_.char_ptr(); }

  /// \brief Returns casted to pointer to char an address of data.
  /// \pre REQUIRES: The buffer should store data chunk, but not referenced to
  /// an external one.
  constexpr char *char_ptr() noexcept {
    assert(is_freestanding());
    return const_cast<char *>(slice_.char_ptr());
  }

  /// \brief Return a const pointer to the beginning of the referenced data.
  constexpr const void *data() const noexcept { return slice_.data(); }

  /// \brief Return a pointer to the beginning of the referenced data.
  /// \pre REQUIRES: The buffer should store data chunk, but not referenced to
  /// an external one.
  constexpr void *data() noexcept {
    assert(is_freestanding());
    return const_cast<void *>(slice_.data());
  }

  /// \brief Returns the number of bytes.
  __nothrow_pure_function cxx20_constexpr size_t length() const noexcept {
    return CONSTEXPR_ASSERT(is_reference() ||
                            slice_.length() + headroom() == silo_.length()),
           slice_.length();
  }

  void make_freestanding() {
    if (is_reference())
      insulate();
  }

  buffer(const ::mdbx::slice &src, bool make_reference,
         const allocator_type &allocator = allocator_type())
      : silo_(allocator), slice_(src) {
    if (!make_reference)
      insulate();
  }

  buffer(const buffer &src, bool make_reference,
         const allocator_type &allocator = allocator_type())
      : buffer(src.slice_, make_reference, allocator) {}

  buffer(const void *ptr, size_t bytes, bool make_reference,
         const allocator_type &allocator = allocator_type())
      : buffer(::mdbx::slice(ptr, bytes), make_reference, allocator) {}

  template <class C, class T, class A>
  buffer(const ::std::basic_string<C, T, A> &str, bool make_reference,
         const allocator_type &allocator = allocator_type())
      : buffer(::mdbx::slice(str), make_reference, allocator) {}

  buffer(const char *c_str, bool make_reference,
         const allocator_type &allocator = allocator_type())
      : buffer(::mdbx::slice(c_str), make_reference, allocator) {}

#if defined(DOXYGEN) ||                                                        \
    (defined(__cpp_lib_string_view) && __cpp_lib_string_view >= 201606L)
  template <class C, class T>
  buffer(const ::std::basic_string_view<C, T> &view, bool make_reference,
         const allocator_type &allocator = allocator_type())
      : buffer(::mdbx::slice(view), make_reference, allocator) {}
#endif /* __cpp_lib_string_view >= 201606L */

  cxx20_constexpr buffer(const ::mdbx::slice &src,
                         const allocator_type &allocator = allocator_type())
      : silo_(src.char_ptr(), src.length(), allocator), slice_(silo_) {}

  cxx20_constexpr buffer(const buffer &src,
                         const allocator_type &allocator = allocator_type())
      : buffer(src.slice_, allocator) {}

  cxx20_constexpr buffer(const void *ptr, size_t bytes,
                         const allocator_type &allocator = allocator_type())
      : buffer(::mdbx::slice(ptr, bytes), allocator) {}

  template <class C, class T, class A>
  cxx20_constexpr buffer(const ::std::basic_string<C, T, A> &str,
                         const allocator_type &allocator = allocator_type())
      : buffer(::mdbx::slice(str), allocator) {}

  cxx20_constexpr buffer(const char *c_str,
                         const allocator_type &allocator = allocator_type())
      : buffer(::mdbx::slice(c_str), allocator) {}

#if defined(DOXYGEN) ||                                                        \
    (defined(__cpp_lib_string_view) && __cpp_lib_string_view >= 201606L)
  template <class C, class T>
  cxx20_constexpr buffer(const ::std::basic_string_view<C, T> &view,
                         const allocator_type &allocator = allocator_type())
      : buffer(::mdbx::slice(view), allocator) {}
#endif /* __cpp_lib_string_view >= 201606L */

  inline buffer(size_t head_room, size_t tail_room,
                const allocator_type &allocator = allocator_type());

  inline buffer(size_t capacity,
                const allocator_type &allocator = allocator_type());

  inline buffer(size_t head_room, const ::mdbx::slice &src, size_t tail_room,
                const allocator_type &allocator = allocator_type());

  buffer(size_t head_room, const buffer &src, size_t tail_room,
         const allocator_type &allocator = allocator_type())
      : buffer(head_room, src.slice_, tail_room, allocator) {}

  cxx20_constexpr
  buffer(const allocator_type &allocator = allocator_type()) noexcept
      : silo_(allocator) {}

  inline buffer(const txn_ref &txn, const ::mdbx::slice &src,
                const allocator_type &allocator = allocator_type());

  buffer(buffer &&src) noexcept
      : silo_(::std::move(src.silo_)), slice_(::std::move(src.slice_)) {}

  buffer(silo &&str) noexcept : silo_(::std::move(str)), slice_(silo_) {}

  constexpr const ::mdbx::slice &slice() const noexcept { return slice_; }

  constexpr operator const ::mdbx::slice &() const noexcept { return slice_; }

  template <typename POD>
  static buffer wrap(const POD &pod, bool make_reference = false,
                     const allocator_type &allocator = allocator_type()) {
    return buffer(::mdbx::slice::wrap(pod), make_reference, allocator);
  }

  /// \brief Reserves storage.
  inline void reserve(size_t wanna_headroom, size_t wanna_tailroom,
                      size_t shrink_threshold = default_shrink_treshold);

  buffer &assign_reference(const void *ptr, size_t bytes) noexcept {
    silo_.clear();
    slice_.assign(ptr, bytes);
    return *this;
  }

  buffer &assign_freestanding(const void *ptr, size_t bytes) {
    silo_.assign(static_cast<const typename silo::value_type *>(ptr),
                 check_length(bytes));
    slice_.assign(silo_);
    return *this;
  }

  void swap(buffer &other)
#if defined(__cpp_noexcept_function_type) &&                                   \
    __cpp_noexcept_function_type >= 201510L
      noexcept(
          std::allocator_traits<ALLOCATOR>::propagate_on_container_swap::value
#if defined(__cpp_lib_allocator_traits_is_always_equal) &&                     \
    __cpp_lib_allocator_traits_is_always_equal >= 201411L
          || std::allocator_traits<ALLOCATOR>::is_always_equal::value
#endif /* __cpp_lib_allocator_traits_is_always_equal */
      )
#endif /* __cpp_noexcept_function_type */
          ;

  buffer &assign(buffer &&src)
#if defined(__cpp_noexcept_function_type) &&                                   \
    __cpp_noexcept_function_type >= 201510L
      noexcept(std::allocator_traits<
                   ALLOCATOR>::propagate_on_container_move_assignment::value
#if defined(__cpp_lib_allocator_traits_is_always_equal) &&                     \
    __cpp_lib_allocator_traits_is_always_equal >= 201411L
               || std::allocator_traits<ALLOCATOR>::is_always_equal::value
#endif /* __cpp_lib_allocator_traits_is_always_equal */
      )
#endif /* __cpp_noexcept_function_type */
  {
    silo_.assign(::std::move(src.silo_));
    slice_.assign(::std::move(src.slice_));
    return *this;
  }

  buffer &assign(silo &&src)
#if defined(__cpp_noexcept_function_type) &&                                   \
    __cpp_noexcept_function_type >= 201510L
      noexcept(std::allocator_traits<
                   ALLOCATOR>::propagate_on_container_move_assignment::value
#if defined(__cpp_lib_allocator_traits_is_always_equal) &&                     \
    __cpp_lib_allocator_traits_is_always_equal >= 201411L
               || std::allocator_traits<ALLOCATOR>::is_always_equal::value
#endif /* __cpp_lib_allocator_traits_is_always_equal */
      )
#endif /* __cpp_noexcept_function_type */
  {
    return assign(buffer(::std::move(src)));
  }

  static buffer clone(const buffer &src,
                      const allocator_type &allocator = allocator_type()) {
    return buffer(src.headroom(), src.slice_, src.tailroom(), allocator);
  }

  buffer &assign(const buffer &src, bool make_reference = false) {
    return assign(src.slice_, make_reference);
  }

  buffer &assign(const void *ptr, size_t bytes, bool make_reference = false) {
    return make_reference ? assign_reference(ptr, bytes)
                          : assign_freestanding(ptr, bytes);
  }

  buffer &assign(const ::mdbx::slice &src, bool make_reference = false) {
    return assign(src.data(), src.length(), make_reference);
  }

  buffer &assign(const ::MDBX_val &src, bool make_reference = false) {
    return assign(src.iov_base, src.iov_len, make_reference);
  }

  buffer &assign(::mdbx::slice &&src, bool make_reference = false) {
    assign(src.data(), src.length(), make_reference);
    src.invalidate();
    return *this;
  }

  buffer &assign(::MDBX_val &&src, bool make_reference = false) {
    assign(src.iov_base, src.iov_len, make_reference);
    src.iov_base = nullptr;
    return *this;
  }

  buffer &assign(const void *begin, const void *end,
                 bool make_reference = false) {
    return assign(begin,
                  static_cast<const byte *>(end) -
                      static_cast<const byte *>(begin),
                  make_reference);
  }

  template <class C, class T, class A>
  buffer &assign(const ::std::basic_string<C, T, A> &str,
                 bool make_reference = false) {
    return assign(str.data(), str.length(), make_reference);
  }

  buffer &assign(const char *c_str, bool make_reference = false) {
    return assign(c_str, ::mdbx::strlen(c_str), make_reference);
  }

#if defined(__cpp_lib_string_view) && __cpp_lib_string_view >= 201606L
  template <class C, class T>
  buffer &assign(const ::std::basic_string_view<C, T> &view,
                 bool make_reference = false) {
    return assign(view.data(), view.length(), make_reference);
  }

  template <class C, class T>
  buffer &assign(::std::basic_string_view<C, T> &&view,
                 bool make_reference = false) {
    assign(view.data(), view.length(), make_reference);
    view = {};
    return *this;
  }
#endif /* __cpp_lib_string_view >= 201606L */

  buffer &operator=(const buffer &src) { return assign(src); }

  buffer &operator=(buffer &&src) noexcept { return assign(::std::move(src)); }

  buffer &operator=(silo &&src) noexcept { return assign(::std::move(src)); }

  buffer &operator=(const ::mdbx::slice &src) { return assign(src); }

  buffer &operator=(::mdbx::slice &&src) { return assign(::std::move(src)); }

#if defined(DOXYGEN) ||                                                        \
    (defined(__cpp_lib_string_view) && __cpp_lib_string_view >= 201606L)
  template <class C, class T>
  buffer &operator=(const ::std::basic_string_view<C, T> &view) noexcept {
    return assign(view);
  }

  /// \brief Return a string_view that references the data of this buffer.
  template <class C = char, class T = ::std::char_traits<C>>
  ::std::basic_string_view<C, T> string_view() const noexcept {
    return slice_.string_view<C, T>();
  }

  /// \brief Return a string_view that references the data of this buffer.
  template <class C, class T>
  operator ::std::basic_string_view<C, T>() const noexcept {
    return string_view<C, T>();
  }
#endif /* __cpp_lib_string_view >= 201606L */

  /// \brief Decodes hexadecimal dump from the given slice to the returned
  /// buffer.
  static buffer decode_hex(const ::mdbx::slice &hex,
                           const allocator_type &allocator = allocator_type()) {
#if __cplusplus >= 201703L
    return buffer(hex.hex_decode(allocator));
#else
    silo data(hex.hex_decode(allocator));
    return buffer(::std::move(data));
#endif
  }

  /// \brief Returns a buffer with a hexadecimal dump of the given slice.
  static buffer encode_hex(const ::mdbx::slice &data, bool uppercase = false,
                           const allocator_type &allocator = allocator_type()) {
#if __cplusplus >= 201703L
    return buffer(data.hex_encode(uppercase, allocator));
#else
    silo hex(data.hex_encode(uppercase, allocator));
    return buffer(::std::move(hex));
#endif
  }

  /// \brief Decodes [Base58](https://en.wikipedia.org/wiki/Base58) dump from
  /// the given slice to the returned buffer.
  static buffer
  decode_base58(const ::mdbx::slice &base58,
                const allocator_type &allocator = allocator_type()) {
#if __cplusplus >= 201703L
    return buffer(base58.base58_decode(allocator));
#else
    silo data(base58.base58_decode(allocator));
    return buffer(::std::move(data));
#endif
  }

  /// \brief Returns a buffer with a
  /// [Base58](https://en.wikipedia.org/wiki/Base58) dump of the given slice.
  static buffer
  encode_base58(const ::mdbx::slice &data,
                const allocator_type &allocator = allocator_type()) {
#if __cplusplus >= 201703L
    return buffer(data.base58_encode(allocator));
#else
    silo base58(data.base58_encode(allocator));
    return buffer(::std::move(base58));
#endif
  }

  /// \brief Decodes [Base64](https://en.wikipedia.org/wiki/Base64) dump from
  /// the given slice to the returned buffer.
  static buffer
  decode_base64(const ::mdbx::slice &base64,
                const allocator_type &allocator = allocator_type()) {
#if __cplusplus >= 201703L
    return buffer(base64.base64_decode(allocator));
#else
    silo data(base64.base64_decode(allocator));
    return buffer(::std::move(data));
#endif
  }

  /// \brief Returns a buffer with a
  /// [Base64](https://en.wikipedia.org/wiki/Base64) dump of the given slice.
  static buffer
  encode_base64(const ::mdbx::slice &data,
                const allocator_type &allocator = allocator_type()) {
#if __cplusplus >= 201703L
    return buffer(data.base64_encode(allocator));
#else
    silo base64(data.base64_encode(allocator));
    return buffer(::std::move(base64));
#endif
  }

  /// \brief Checks whether the string is empty.
  __nothrow_pure_function cxx20_constexpr bool empty() const noexcept {
    return length() == 0;
  }

  /// \brief Checks whether the data pointer of the buffer is nullptr.
  constexpr bool is_null() const noexcept { return data() == nullptr; }

  /// \brief Returns the number of bytes.
  __nothrow_pure_function cxx20_constexpr size_t size() const noexcept {
    return length();
  }

  /// \brief Returns the hash value of the data.
  /// \attention Function implementation and returned hash values may changed
  /// version to version, and in future the t1ha3 will be used here. Therefore
  /// values obtained from this function shouldn't be persisted anywhere.
  __nothrow_pure_function cxx14_constexpr size_t hash_value() const noexcept {
    return slice_.hash_value();
  }

  template <class C = char, class T = ::std::char_traits<C>,
            class A = legacy_allocator>
  cxx20_constexpr ::std::basic_string<C, T, A>
  string(const A &allocator = A()) const {
    return slice_.string<C, T, A>(allocator);
  }

  template <class C, class T, class A>
  cxx20_constexpr operator ::std::basic_string<C, T, A>() const {
    return this->string<C, T, A>();
  }

  /// \brief Checks if the data starts with the given prefix.
  __nothrow_pure_function bool
  starts_with(const ::mdbx::slice &prefix) const noexcept {
    return slice_.starts_with(prefix);
  }

  /// \brief Checks if the data ends with the given suffix.
  __nothrow_pure_function bool
  ends_with(const ::mdbx::slice &suffix) const noexcept {
    return slice_.ends_with(suffix);
  }

  /// \brief Clears the contents and storage.
  void clear() noexcept {
    slice_.clear();
    silo_.clear();
  }

  /// \brief Reduces memory usage by freeing unused storage space.
  void shrink_to_fit(size_t threshold = 64) { reserve(0, 0, threshold); }

  /// \brief Drops the first "n" bytes from the data chunk.
  /// \pre REQUIRES: `n <= size()`
  void remove_prefix(size_t n) noexcept { slice_.remove_prefix(n); }

  /// \brief Drops the last "n" bytes from the data chunk.
  /// \pre REQUIRES: `n <= size()`
  void remove_suffix(size_t n) noexcept { slice_.remove_suffix(n); }

  /// \brief Drops the first "n" bytes from the data chunk.
  /// \throws std::out_of_range if `n > size()`
  void safe_remove_prefix(size_t n) { slice_.safe_remove_prefix(n); }

  /// \brief Drops the last "n" bytes from the data chunk.
  /// \throws std::out_of_range if `n > size()`
  void safe_remove_suffix(size_t n) { slice_.safe_remove_suffix(n); }

  /// \brief Accesses the specified byte of data chunk.
  /// \pre REQUIRES: `n < size()`
  byte operator[](size_t n) const noexcept { return slice_[n]; }

  /// \brief Accesses the specified byte of data chunk.
  /// \pre REQUIRES: `n < size()`
  byte &operator[](size_t n) noexcept {
    assert(n < size());
    return byte_ptr()[n];
  }

  /// \brief Accesses the specified byte of data chunk with bounds checking.
  /// \throws std::out_of_range if `n >= size()`
  byte at(size_t n) const { return slice_.at(n); }

  /// \brief Accesses the specified byte of data chunk with bounds checking.
  /// \throws std::out_of_range if `n >= size()`
  byte &at(size_t n) {
    if (mdbx_unlikely(n >= size()))
      cxx20_attribute_unlikely throw_out_range();
    return byte_ptr()[n];
  }

  /// \brief Returns the first "n" bytes of the data chunk.
  /// \pre REQUIRES: `n <= size()`
  ::mdbx::slice head(size_t n) const noexcept { return slice_.head(n); }

  /// \brief Returns the last "n" bytes of the data chunk.
  /// \pre REQUIRES: `n <= size()`
  ::mdbx::slice tail(size_t n) const noexcept { return slice_.tail(n); }

  /// \brief Returns the middle "n" bytes of the data chunk.
  /// \pre REQUIRES: `from + n <= size()`
  ::mdbx::slice middle(size_t from, size_t n) const noexcept {
    return slice_.middle(from, n);
  }

  /// \brief Returns the first "n" bytes of the data chunk.
  /// \throws std::out_of_range if `n >= size()`
  ::mdbx::slice safe_head(size_t n) const { return slice_.safe_head(n); }

  /// \brief Returns the last "n" bytes of the data chunk.
  /// \throws std::out_of_range if `n >= size()`
  ::mdbx::slice safe_tail(size_t n) const { return slice_.safe_tail(n); }

  /// \brief Returns the middle "n" bytes of the data chunk.
  /// \throws std::out_of_range if `from + n >= size()`
  ::mdbx::slice safe_middle(size_t from, size_t n) const {
    return slice_.safe_middle(from, n);
  }

  inline buffer &append(const void *src, size_t bytes);

  buffer &append(const ::mdbx::slice &chunk) {
    return append(chunk.data(), chunk.size());
  }

  inline buffer &add_header(const void *src, size_t bytes);

  buffer &add_header(const ::mdbx::slice &chunk) {
    return add_header(chunk.data(), chunk.size());
  }

  //----------------------------------------------------------------------------

  template <size_t SIZE>
  static buffer key_from(const char (&text)[SIZE], bool make_reference = true) {
    return buffer(::mdbx::slice(text), make_reference);
  }

#if defined(DOXYGEN) ||                                                        \
    (defined(__cpp_lib_string_view) && __cpp_lib_string_view >= 201606L)
  template <class C, class T>
  static buffer key_from(const ::std::basic_string_view<C, T> &src,
                         bool make_reference = false) {
    return buffer(src, make_reference);
  }
#endif /* __cpp_lib_string_view >= 201606L */

  static buffer key_from(const char *src, bool make_reference = false) {
    return buffer(src, make_reference);
  }

  template <class C, class T, class A>
  static buffer key_from(const ::std::basic_string<C, T, A> &src,
                         bool make_reference = false) {
    return buffer(src, make_reference);
  }

  static buffer key_from(const silo &&src) noexcept {
    return buffer(::std::move(src));
  }

  static buffer key_from(const double ieee754_64bit) {
    return wrap(::mdbx_key_from_double(ieee754_64bit));
  }

  static buffer key_from(const double *ieee754_64bit) {
    return wrap(::mdbx_key_from_ptrdouble(ieee754_64bit));
  }

  static buffer key_from(const uint64_t unsigned_int64) {
    return wrap(unsigned_int64);
  }

  static buffer key_from(const int64_t signed_int64) {
    return wrap(::mdbx_key_from_int64(signed_int64));
  }

  static buffer key_from_jsonInteger(const int64_t json_integer) {
    return wrap(::mdbx_key_from_jsonInteger(json_integer));
  }

  static buffer key_from(const float ieee754_32bit) {
    return wrap(::mdbx_key_from_float(ieee754_32bit));
  }

  static buffer key_from(const float *ieee754_32bit) {
    return wrap(::mdbx_key_from_ptrfloat(ieee754_32bit));
  }

  static buffer key_from(const uint32_t unsigned_int32) {
    return wrap(unsigned_int32);
  }

  static buffer key_from(const int32_t signed_int32) {
    return wrap(::mdbx_key_from_int32(signed_int32));
  }
};

/// \brief Combines data slice with boolean flag to represent result of certain
/// operations.
struct value_result {
  slice value;
  bool done;
  constexpr operator bool() const noexcept {
    assert(!done || bool(value));
    return done;
  }
};

/// \brief Combines pair of slices for key and value to represent result of
/// certain operations.
struct pair {
  slice key;
  slice value;
  constexpr operator bool() const noexcept {
    assert(bool(key) == bool(value));
    return key;
  }
};

/// \brief Combines pair of slices for key and value with boolean flag to
/// represent result of certain operations.
struct pair_result : public pair {
  bool done;
  constexpr operator bool() const noexcept {
    assert(!done || (bool(key) && bool(value)));
    return done;
  }
};

//------------------------------------------------------------------------------

/// Loop control constants for readers enumeration functor.
enum enumeration_loop_control { continue_loop = 0, exit_loop = INT32_MIN };

/// Kinds of the keys and corresponding modes of comparing it.
enum class key_mode {
  usual = MDBX_DB_DEFAULTS,  ///< Usual variable length keys with byte-by-byte
                             ///< lexicographic comparison like `std::memcmp()`.
  reverse = MDBX_REVERSEKEY, ///< Variable length keys with byte-by-byte
                             ///< lexicographic comparison in reverse order,
                             ///< from the end of the keys to the beginning.
  ordinal = MDBX_INTEGERKEY, ///< Keys are binary integers in native byte order,
                             ///< either `uint32_t` or `uint64_t`, and will be
                             ///< sorted as such. The keys must all be of the
                             ///< same size and must be aligned while passing
                             ///< as arguments.
  msgpack = -1 ///< Keys are in [MessagePack](https://msgpack.org/)
               ///< format with appropriate comparison.
               ///< \note Not yet implemented and PRs are welcome.
};

/// Kind of the values and sorted multi-values with corresponding comparison.
enum class value_mode {
  single = MDBX_DB_DEFAULTS, ///< Usual single value for each key. In terms of
                             ///< keys, they are unique.
  multi =
      MDBX_DUPSORT, ///< A more than one data value could be associated with
                    ///< each key. Internally each key is stored once, and the
                    ///< corresponding data values are sorted by byte-by-byte
                    ///< lexicographic comparison like `std::memcmp()`.
                    ///< In terms of keys, they are not unique, i.e. has
                    ///< duplicates which are sorted by associated data values.
#if defined(__cplusplus) && defined(_MSC_VER) && _MSC_VER < 1910
  multi_reverse = uint32_t(MDBX_DUPSORT) | uint32_t(MDBX_REVERSEDUP),
  multi_samelength = uint32_t(MDBX_DUPSORT) | uint32_t(MDBX_DUPFIXED),
  multi_ordinal = uint32_t(MDBX_DUPSORT) | uint32_t(MDBX_DUPFIXED) |
                  uint32_t(MDBX_INTEGERDUP),
  multi_reverse_samelength = uint32_t(MDBX_DUPSORT) |
                             uint32_t(MDBX_REVERSEDUP) | uint32_t(MDBX_DUPFIXED)
#else
  multi_reverse =
      MDBX_DUPSORT |
      MDBX_REVERSEDUP, ///< A more than one data value could be associated with
                       ///< each key. Internally each key is stored once, and
                       ///< the corresponding data values are sorted by
                       ///< byte-by-byte lexicographic comparison in reverse
                       ///< order, from the end of the keys to the beginning.
                       ///< In terms of keys, they are not unique, i.e. has
                       ///< duplicates which are sorted by associated data
                       ///< values.
  multi_samelength =
      MDBX_DUPSORT |
      MDBX_DUPFIXED, ///< A more than one data value could be associated with
                     ///< each key, and all data values must be same length.
                     ///< Internally each key is stored once, and the
                     ///< corresponding data values are sorted by byte-by-byte
                     ///< lexicographic comparison like `std::memcmp()`. In
                     ///< terms of keys, they are not unique, i.e. has
                     ///< duplicates which are sorted by associated data values.
  multi_ordinal =
      MDBX_DUPSORT | MDBX_DUPFIXED |
      MDBX_INTEGERDUP, ///< A more than one data value could be associated with
                       ///< each key, and all data values are binary integers in
                       ///< native byte order, either `uint32_t` or `uint64_t`,
                       ///< and will be sorted as such. Internally each key is
                       ///< stored once, and the corresponding data values are
                       ///< sorted. In terms of keys, they are not unique, i.e.
                       ///< has duplicates which are sorted by associated data
                       ///< values.
  multi_reverse_samelength =
      MDBX_DUPSORT | MDBX_REVERSEDUP |
      MDBX_DUPFIXED, ///< A more than one data value could be associated with
                     ///< each key, and all data values must be same length.
                     ///< Internally each key is stored once, and the
                     ///< corresponding data values are sorted by byte-by-byte
                     ///< lexicographic comparison in reverse order, from the
                     ///< end of the keys to the beginning. In terms of keys,
                     ///< they are not unique, i.e. has duplicates which are
                     ///< sorted by associated data values.
  msgpack = -1 ///< A more than one data value could be associated with each
               ///< key. Values are in [MessagePack](https://msgpack.org/)
               ///< format with appropriate comparison. Internally each key is
               ///< stored once, and the corresponding data values are sorted.
               ///< In terms of keys, they are not unique, i.e. has duplicates
               ///< which are sorted by associated data values.
               ///< \note Not yet implemented and PRs are welcome.
#endif
};

/// \brief A handle for an individual database (key-value spaces) in the
/// environment.
/// \see txn_ref::open_map() \see txn_ref::create_map()
/// \see txn_ref::clear_map() \see txn_ref::drop_map()
/// \see txn_ref::get_handle_info() \see txn_ref::get_map_stat()
/// \see env_ref::close_amp()
/// \see cursor_ref::map()
struct LIBMDBX_API_TYPE map_handle {
  MDBX_dbi dbi{0};
  constexpr map_handle() noexcept {}
  constexpr map_handle(MDBX_dbi dbi) noexcept : dbi(dbi) {}
  map_handle(const map_handle &) noexcept = default;
  map_handle &operator=(const map_handle &) noexcept = default;
  operator bool() const noexcept { return dbi != 0; }

  using flags = ::MDBX_db_flags_t;
  using state = ::MDBX_dbi_state_t;
  struct LIBMDBX_API_TYPE info {
    map_handle::flags flags;
    map_handle::state state;
    constexpr info(map_handle::flags flags, map_handle::state state) noexcept;
    info(const info &) noexcept = default;
    info &operator=(const info &) noexcept = default;
    constexpr ::mdbx::key_mode key_mode() const noexcept;
    constexpr ::mdbx::value_mode value_mode() const noexcept;
  };
};

enum put_mode {
  insert = MDBX_NOOVERWRITE,
  upsert = MDBX_UPSERT,
  update = MDBX_CURRENT,
};

/// \brief Unmanaged database environment.
///
/// Like other unmanaged classes, `env_ref` allows copying and assignment for
/// instances, but does not destroys the represented underlying object from the
/// own class destructor.
///
/// An environment supports multiple key-value databases (aka key-value spaces
/// or tables), all residing in the same shared-memory map.
class LIBMDBX_API_TYPE env_ref {
  friend class txn_ref;

protected:
  MDBX_env *handle_{nullptr};
  constexpr env_ref(MDBX_env *ptr) noexcept;

public:
  constexpr env_ref() noexcept = default;
  env_ref(const env_ref &) noexcept = default;
  inline env_ref &operator=(env_ref &&other) noexcept;
  inline env_ref(env_ref &&other) noexcept;
  inline ~env_ref() noexcept;

  constexpr operator bool() const noexcept;
  constexpr operator const MDBX_env *() const;
  inline operator MDBX_env *();
  friend constexpr bool operator==(const env_ref &a, const env_ref &b) noexcept;
  friend constexpr bool operator!=(const env_ref &a, const env_ref &b) noexcept;

  //----------------------------------------------------------------------------

  /// Database geometry for size management.
  struct LIBMDBX_API_TYPE geometry {
    enum : int64_t {
      default_value = -1,         ///< Means "keep current or use default"
      minimal_value = 0,          ///< Means "minimal acceptable"
      maximal_value = INTPTR_MAX, ///< Means "miximal acceptable"
      kB = 1000,                  ///< \f$10^{3}\f$ bytes
      MB = kB * 1000,             ///< \f$10^{6}\f$ bytes
      GB = MB * 1000,             ///< \f$10^{9}\f$ bytes
      TB = GB * 1000,             ///< \f$10^{12}\f$ bytes
      PB = TB * 1000,             ///< \f$10^{15}\f$ bytes
      EB = PB * 1000,             ///< \f$10^{18}\f$ bytes
      KiB = 1024,                 ///< \f$2^{10}\f$ bytes
      MiB = KiB << 10,            ///< \f$2^{20}\f$ bytes
      GiB = MiB << 10,            ///< \f$2^{30}\f$ bytes
      TiB = GiB << 10,            ///< \f$2^{40}\f$ bytes
      PiB = TiB << 10,            ///< \f$2^{50}\f$ bytes
      EiB = PiB << 10,            ///< \f$2^{60}\f$ bytes
    };

    /// \brief Tagged type for output to std::ostream
    struct size {
      intptr_t bytes;
      constexpr size(intptr_t bytes) noexcept : bytes(bytes) {}
      operator intptr_t() const noexcept { return bytes; }
    };

    /// \brief The lower bound of database size in bytes.
    intptr_t size_lower{minimal_value};

    /// \brief The size in bytes to setup the database size for now.
    /// \details It is recommended always pass \ref default_value in this
    /// argument except some special cases.
    intptr_t size_now{default_value};

    /// \brief The upper bound of database size in bytes.
    /// \details It is recommended to avoid change upper bound while database is
    /// used by other processes or threaded (i.e. just pass \ref default_value
    /// in this argument except absolutely necessary). Otherwise you must be
    /// ready for \ref MDBX_UNABLE_EXTEND_MAPSIZE error(s), unexpected pauses
    /// during remapping and/or system errors like "address busy", and so on. In
    /// other words, there is no way to handle a growth of the upper bound
    /// robustly because there may be a lack of appropriate system resources
    /// (which are extremely volatile in a multi-process multi-threaded
    /// environment).
    intptr_t size_upper{maximal_value};

    /// \brief The growth step in bytes, must be greater than zero to allow the
    /// database to grow.
    intptr_t growth_step{default_value};

    /// \brief The shrink threshold in bytes, must be greater than zero to allow
    /// the database to shrink.
    intptr_t shrink_threshold{default_value};

    /// \brief The database page size for new database creation
    /// or \ref default_value otherwise.
    /// \details Must be power of 2 in the range between \ref MDBX_MIN_PAGESIZE
    /// and \ref MDBX_MAX_PAGESIZE.
    intptr_t pagesize{default_value};

    inline geometry &make_fixed(intptr_t size) noexcept;
    inline geometry &make_dynamic(intptr_t lower = minimal_value,
                                  intptr_t upper = maximal_value) noexcept;
  };

  /// Operation mode.
  enum mode {
    readonly,       ///< \copydoc MDBX_RDONLY
    write_file_io,  // don't available on OpenBSD
    write_mapped_io ///< \copydoc MDBX_WRITEMAP
  };

  /// Durability level.
  enum durability {
    robust_synchronous,         ///< \copydoc MDBX_SYNC_DURABLE
    half_synchronous_weak_last, ///< \copydoc MDBX_NOMETASYNC
    lazy_weak_tail,             ///< \copydoc MDBX_SAFE_NOSYNC
    whole_fragile               ///< \copydoc MDBX_UTTERLY_NOSYNC
  };

  /// Garbage reclaiming options.
  struct LIBMDBX_API_TYPE reclaiming_options {
    /// \copydoc MDBX_LIFORECLAIM
    bool lifo{false};
    /// \copydoc MDBX_COALESCE
    bool coalesce{false};
    constexpr reclaiming_options() noexcept {}
    reclaiming_options(MDBX_env_flags_t) noexcept;
  };

  /// Operate options.
  struct LIBMDBX_API_TYPE operate_options {
    /// \copydoc MDBX_NOTLS
    bool orphan_read_transactions{false};
    bool nested_write_transactions{false};
    /// \copydoc MDBX_EXCLUSIVE
    bool exclusive{false};
    /// \copydoc MDBX_NORDAHEAD
    bool disable_readahead{false};
    /// \copydoc MDBX_NOMEMINIT
    bool disable_clear_memory{false};
    constexpr operate_options() noexcept {}
    operate_options(MDBX_env_flags_t) noexcept;
  };

  struct LIBMDBX_API_TYPE operate_parameters {
    unsigned max_maps{0};
    unsigned max_readers{0};
    env_ref::mode mode{write_mapped_io};
    env_ref::durability durability{robust_synchronous};
    env_ref::reclaiming_options reclaiming;
    env_ref::operate_options options;

    constexpr operate_parameters() noexcept {}
    MDBX_env_flags_t make_flags(bool accede = true, ///< \copydoc MDBX_ACCEDE
                                bool use_subdirectory = false) const;
    static env_ref::mode mode_from_flags(MDBX_env_flags_t) noexcept;
    static env_ref::durability durability_from_flags(MDBX_env_flags_t) noexcept;
    inline static env_ref::reclaiming_options
    reclaiming_from_flags(MDBX_env_flags_t flags) noexcept;
    inline static env_ref::operate_options
    options_from_flags(MDBX_env_flags_t flags) noexcept;
    operate_parameters(const env_ref &);
  };

  inline env_ref::operate_parameters get_operation_parameters() const;
  inline env_ref::mode get_mode() const;
  inline env_ref::durability get_durability() const;
  inline env_ref::reclaiming_options get_reclaiming() const;
  inline env_ref::operate_options get_options() const;

  /// \brief Returns true for a freshly created database,
  /// but false if at least one transaction was committed.
  bool is_pristine() const;

  /// \brief Returns true for an empty database.
  bool is_empty() const;

  static size_t default_pagesize() noexcept;
  struct limits {
    limits() = delete;
    static inline size_t pagesize_min() noexcept;
    static inline size_t pagesize_max() noexcept;
    static inline size_t dbsize_min(intptr_t pagesize);
    static inline size_t dbsize_max(intptr_t pagesize);
    static inline size_t key_min(MDBX_db_flags_t flags) noexcept;
    static inline size_t key_min(key_mode mode) noexcept;
    static inline size_t key_max(intptr_t pagesize, MDBX_db_flags_t flags);
    static inline size_t key_max(intptr_t pagesize, key_mode mode);
    static inline size_t key_max(const env_ref &, MDBX_db_flags_t flags);
    static inline size_t key_max(const env_ref &, key_mode mode);
    static inline size_t value_min(MDBX_db_flags_t flags) noexcept;
    static inline size_t value_min(key_mode) noexcept;
    static inline size_t value_max(intptr_t pagesize, MDBX_db_flags_t flags);
    static inline size_t value_max(intptr_t pagesize, key_mode);
    static inline size_t value_max(const env_ref &, MDBX_db_flags_t flags);
    static inline size_t value_max(const env_ref &, key_mode);
    static inline size_t transaction_size_max(intptr_t pagesize);
  };

  env_ref &copy(const path &destination, bool compactify,
                bool force_dynamic_size = false);
  env_ref &copy(filehandle fd, bool compactify,
                bool force_dynamic_size = false);

  using stat = ::MDBX_stat;
  using info = ::MDBX_envinfo;
  inline stat get_stat() const;
  inline info get_info() const;
  inline stat get_stat(const txn_ref &) const;
  inline info get_info(const txn_ref &) const;
  inline filehandle get_filehandle() const;
  path get_path() const;
  inline MDBX_env_flags_t get_flags() const;
  inline unsigned max_readers() const;
  inline unsigned max_maps() const;
  inline void *get_context() const noexcept;
  inline env_ref &set_context(void *);
  inline env_ref &set_sync_threshold(size_t bytes);
  inline env_ref &set_sync_period(unsigned seconds_16dot16);
  inline env_ref &set_sync_period(double seconds);
  inline env_ref &alter_flags(MDBX_env_flags_t flags, bool on_off);
  inline env_ref &set_geometry(const geometry &size);
  inline env_ref &set_max_maps(unsigned maps);
  inline env_ref &sync_to_disk();
  inline env_ref &sync_to_disk(bool force, bool nonblock);
  inline bool poll_sync_to_disk();
  inline void close_map(const map_handle &handle_);

  /// \brief Readed information
  struct reader_info {
    int slot;                ///< Reader Table Slot
    mdbx_pid_t pid;          ///< Reader Process ID
    mdbx_tid_t thread;       ///< Reader thread ID
    uint64_t transaction_id; ///<
    uint64_t transaction_lag;
    size_t bytes_used;
    size_t bytes_retained;
    constexpr reader_info(int slot, mdbx_pid_t pid, mdbx_tid_t thread,
                          uint64_t txnid, uint64_t lag, size_t used,
                          size_t retained) noexcept;
  };

  /// Enumerate readers
  /// through visitor.operator(const reader_info&, int number)
  template <typename VISITOR> inline int enumerate_readers(VISITOR &visitor);

  /// Checks for stale readers in the lock tablea and
  /// return number of cleared slots.
  inline unsigned check_readers();

  inline env_ref &set_OutOfSpace_callback(MDBX_oom_func *);
  inline MDBX_oom_func *get_OutOfSpace_callback() const noexcept;
  inline txn start_read() const;
  inline txn prepare_read() const;
  inline txn start_write(bool dont_wait = false);
  inline txn try_start_write();
};

/// \brief Managed database environment.
///
/// As other managed classes, `env` destroys the represented underlying object
/// from the own class destructor, but disallows copying and assignment for
/// instances.
///
/// An environment supports multiple key-value databases (aka key-value spaces
/// or tables), all residing in the same shared-memory map.
class LIBMDBX_API_TYPE env : public env_ref {
  using inherited = env_ref;
  /// delegated constructor for RAII
  constexpr env(MDBX_env *ptr) noexcept : inherited(ptr) {}
  void setup(unsigned max_maps, unsigned max_readers = 0);

public:
  constexpr env() noexcept = default;

  /// \brief Open existing database.
  env(const path &, const operate_parameters &, bool accede = true);

  /// Additional parameters for creating a new database.
  struct create_parameters {
    env_ref::geometry geometry;
    mdbx_mode_t file_mode_bits{0640};
    bool use_subdirectory{false};
  };

  /// \brief Create new or open existing database.
  env(const path &, const create_parameters &, const operate_parameters &,
      bool accede = true);

  /// \brief Closes environment explicitly.
  void close(bool dont_sync = false);

  env(env &&) = default;
  env &operator=(env &&) = default;
  env(const env &) = delete;
  env &operator=(const env &) = delete;
  virtual ~env() noexcept;
};

/// \brief Unmanaged database transaction.
///
/// Like other unmanaged classes, `txn_ref` allows copying and assignment for
/// instances, but does not destroys the represented underlying object from the
/// own class destructor.
///
/// All database operations require a transaction handle. Transactions may be
/// read-only or read-write.
class LIBMDBX_API_TYPE txn_ref {
protected:
  friend class cursor_ref;
  MDBX_txn *handle_{nullptr};
  constexpr txn_ref(MDBX_txn *ptr) noexcept;

public:
  constexpr txn_ref() noexcept = default;
  txn_ref(const txn_ref &) noexcept = default;
  inline txn_ref &operator=(txn_ref &&other) noexcept;
  inline txn_ref(txn_ref &&other) noexcept;
  inline ~txn_ref() noexcept;

  constexpr operator bool() const noexcept;
  constexpr operator const MDBX_txn *() const;
  inline operator MDBX_txn *();
  friend constexpr bool operator==(const txn_ref &a, const txn_ref &b) noexcept;
  friend constexpr bool operator!=(const txn_ref &a, const txn_ref &b) noexcept;

  inline ::mdbx::env_ref env() const noexcept;
  inline MDBX_txn_flags_t flags() const;
  inline uint64_t id() const;
  inline bool is_dirty(const void *ptr) const;

  using info = ::MDBX_txn_info;
  inline info get_info(bool scan_reader_lock_table = false) const;

  //----------------------------------------------------------------------------

  /// \brief Reset a read-only transaction.
  inline void reset_reading();

  /// \brief Renew a read-only transaction.
  inline void renew_reading();

  /// \brief Start nested write transaction.
  txn start_nested();

  inline cursor create_cursor(map_handle map);

  /// \brief Open existing key-value map.
  inline map_handle open_map(
      const char *name,
      const ::mdbx::key_mode key_mode = ::mdbx::key_mode::usual,
      const ::mdbx::value_mode value_mode = ::mdbx::value_mode::single) const;
  inline map_handle open_map(
      const ::std::string &name,
      const ::mdbx::key_mode key_mode = ::mdbx::key_mode::usual,
      const ::mdbx::value_mode value_mode = ::mdbx::value_mode::single) const;

  /// \brief Create new or open existing key-value map.
  inline map_handle
  create_map(const char *name,
             const ::mdbx::key_mode key_mode = ::mdbx::key_mode::usual,
             const ::mdbx::value_mode value_mode = ::mdbx::value_mode::single);
  inline map_handle
  create_map(const ::std::string &name,
             const ::mdbx::key_mode key_mode = ::mdbx::key_mode::usual,
             const ::mdbx::value_mode value_mode = ::mdbx::value_mode::single);

  /// \brief Drop key-value map.
  inline void drop_map(map_handle map);
  bool drop_map(const char *name, bool ignore_nonexists = true);
  inline bool drop_map(const ::std::string &name, bool ignore_nonexists = true);

  /// \brief Clear key-value map.
  inline void clear_map(map_handle map);
  bool clear_map(const char *name, bool ignore_nonexists = true);
  inline bool clear_map(const ::std::string &name,
                        bool ignore_nonexists = true);

  using map_stat = ::MDBX_stat;
  inline map_stat get_map_stat(map_handle map) const;
  inline uint32_t get_tree_deepmask(map_handle map) const;
  inline map_handle::info get_handle_info(map_handle map) const;

  using canary = ::MDBX_canary;
  inline txn_ref &put_canary(const canary &);
  inline canary get_canary() const;
  inline uint64_t sequence(map_handle map) const;
  inline uint64_t sequence(map_handle map, uint64_t increment);

  inline int compare_keys(map_handle map, const slice &a,
                          const slice &b) const noexcept;
  inline int compare_values(map_handle map, const slice &a,
                            const slice &b) const noexcept;
  inline int compare_keys(map_handle map, const pair &a,
                          const pair &b) const noexcept;
  inline int compare_values(map_handle map, const pair &a,
                            const pair &b) const noexcept;

  inline slice get(map_handle map, const slice &key) const;
  inline slice get(map_handle map, slice key, size_t &values_count) const;
  inline slice get(map_handle map, const slice &key,
                   const slice &if_not_exists) const;
  inline slice get(map_handle map, slice key, size_t &values_count,
                   const slice &if_not_exists) const;
  inline pair get_equal_or_great(map_handle map, const slice &key) const;
  inline pair get_equal_or_great(map_handle map, const slice &key,
                                 const slice &if_not_exists) const;

  inline MDBX_error_t put(map_handle map, const slice &key, slice *value,
                          MDBX_put_flags_t flags) noexcept;
  inline void insert(map_handle map, const slice &key, slice value);
  inline value_result try_insert(map_handle map, const slice &key, slice value);
  inline slice insert_reserve(map_handle map, const slice &key,
                              size_t value_length);
  inline value_result try_insert_reserve(map_handle map, const slice &key,
                                         size_t value_length);

  inline void upsert(map_handle map, const slice &key, const slice &value);
  inline slice upsert_reserve(map_handle map, const slice &key,
                              size_t value_length);

  inline void update(map_handle map, const slice &key, const slice &value);
  inline bool try_update(map_handle map, const slice &key, const slice &value);
  inline slice update_reserve(map_handle map, const slice &key,
                              size_t value_length);
  inline value_result try_update_reserve(map_handle map, const slice &key,
                                         size_t value_length);

  inline bool erase(map_handle map, const slice &key);

  /// \brief Removes the particular multi-value entry of the key.
  inline bool erase(map_handle map, const slice &key, const slice &value);

  /// \brief Replaces the particular multi-value of the key with a new value.
  inline void replace(map_handle map, const slice &key, slice old_value,
                      const slice &new_value);

  /// \brief Removes and return a value of the key.
  template <class ALLOCATOR>
  inline buffer<ALLOCATOR> extract(map_handle map, const slice &key,
                                   const ALLOCATOR &allocator = ALLOCATOR());

  /// \brief Replaces and returns a value of the key with new one.
  template <class ALLOCATOR>
  inline buffer<ALLOCATOR> replace(map_handle map, const slice &key,
                                   const slice &new_value,
                                   const ALLOCATOR &allocator = ALLOCATOR());

  template <class ALLOCATOR>
  inline buffer<ALLOCATOR>
  replace_reserve(map_handle map, const slice &key, slice &new_value,
                  const ALLOCATOR &allocator = ALLOCATOR());

  /// \brief Adding a key-value pair, provided that ascending order of the keys
  /// and (optionally) values are preserved.
  ///
  /// Instead of splitting the full b+tree pages, the data will be placed on new
  /// ones. Thus appending is about two times faster than insertion, and the
  /// pages will be filled in completely mostly but not half as after splitting
  /// ones. On the other hand, any subsequent insertion or update with an
  /// increase in the length of the value will be twice as slow, since it will
  /// require splitting already filled pages.
  ///
  /// \param [in] multivalue_order_preserved
  /// If `multivalue_order_preserved == true` then the same rules applied for
  /// to pages of nested b+tree of multimap's values.
  inline void append(map_handle map, const slice &key, const slice &value,
                     bool multivalue_order_preserved = true);

  size_t put_multiple(map_handle map, const slice &key,
                      const size_t value_length, const void *values_array,
                      size_t values_count, put_mode mode,
                      bool allow_partial = false);
  template <typename VALUE>
  void put_multiple(map_handle map, const slice &key,
                    const std::vector<VALUE> &vector, put_mode mode) {
    put_multiple(map, key, sizeof(VALUE), vector.data(), vector.size(), mode,
                 false);
  }

  inline ptrdiff_t estimate(map_handle map, pair from, pair to) const;
  inline ptrdiff_t estimate(map_handle map, slice from, slice to) const;
  inline ptrdiff_t estimate_from_first(map_handle map, slice to) const;
  inline ptrdiff_t estimate_to_last(map_handle map, slice from) const;
};

/// \brief Managed database transaction.
///
/// As other managed classes, `txn` destroys the represented underlying object
/// from the own class destructor, but disallows copying and assignment for
/// instances.
///
/// All database operations require a transaction handle. Transactions may be
/// read-only or read-write.
class LIBMDBX_API_TYPE txn : public txn_ref {
  using inherited = txn_ref;
  friend class env_ref;
  friend class txn_ref;
  /// delegated constructor for RAII
  constexpr txn(MDBX_txn *ptr) noexcept : inherited(ptr) {}

public:
  constexpr txn() noexcept = default;
  txn(txn &&) = default;
  txn &operator=(txn &&) = default;
  txn(const txn &) = delete;
  txn &operator=(const txn &) = delete;
  virtual ~txn() noexcept;

  //----------------------------------------------------------------------------

  /// \brief Abort write transaction or read-only transaction.
  void abort();

  /// \brief Commit write transaction.
  void commit();
};

/// \brief Unmanaged cursor.
///
/// Like other unmanaged classes, `cursor_ref` allows copying and assignment for
/// instances, but does not destroys the represented underlying object from the
/// own class destructor.
///
/// \copydetails MDBX_cursor
class LIBMDBX_API_TYPE cursor_ref {
protected:
  MDBX_cursor *handle_{nullptr};
  constexpr cursor_ref(MDBX_cursor *ptr) noexcept;

public:
  constexpr cursor_ref() noexcept = default;
  cursor_ref(const cursor_ref &) noexcept = default;
  inline cursor_ref &operator=(cursor_ref &&other) noexcept;
  inline cursor_ref(cursor_ref &&other) noexcept;
  inline ~cursor_ref() noexcept;
  constexpr operator bool() const noexcept;
  constexpr operator const MDBX_cursor *() const;
  inline operator MDBX_cursor *();
  friend constexpr bool operator==(const cursor_ref &a,
                                   const cursor_ref &b) noexcept;
  friend constexpr bool operator!=(const cursor_ref &a,
                                   const cursor_ref &b) noexcept;

  enum move_operation {
    first = MDBX_FIRST,
    last = MDBX_LAST,
    next = MDBX_NEXT,
    previous = MDBX_PREV,
    get_current = MDBX_GET_CURRENT,

    multi_prevkey_lastvalue = MDBX_PREV_NODUP,
    multi_currentkey_firstvalue = MDBX_FIRST_DUP,
    multi_currentkey_prevvalue = MDBX_PREV_DUP,
    multi_currentkey_nextvalue = MDBX_NEXT_DUP,
    multi_currentkey_lastvalue = MDBX_LAST_DUP,
    multi_nextkey_firstvalue = MDBX_NEXT_NODUP,

    multi_find_pair = MDBX_GET_BOTH,
    multi_exactkey_lowerboundvalue = MDBX_GET_BOTH_RANGE,

    find_key = MDBX_SET,
    key_exact = MDBX_SET_KEY,
    key_lowerbound = MDBX_SET_RANGE
  };

  struct move_result : public pair_result {
    inline move_result(const cursor_ref &cursor, bool throw_notfound);
    inline move_result(cursor_ref &cursor, move_operation operation,
                       bool throw_notfound);
    inline move_result(cursor_ref &cursor, move_operation operation,
                       const slice &key, bool throw_notfound);
    inline move_result(cursor_ref &cursor, move_operation operation,
                       const slice &key, const slice &value,
                       bool throw_notfound);
    move_result(const move_result &) noexcept = default;
  };

protected:
  inline bool move(move_operation operation, MDBX_val *key, MDBX_val *value,
                   bool throw_notfound) const
      /* fake const, i.e. for some operations */;
  inline ptrdiff_t estimate(move_operation operation, MDBX_val *key,
                            MDBX_val *value) const;

public:
  inline move_result move(move_operation operation, bool throw_notfound);
  inline move_result to_first(bool throw_notfound = true);
  inline move_result to_previous(bool throw_notfound = true);
  inline move_result to_previous_last_multi(bool throw_notfound = true);
  inline move_result to_current_first_multi(bool throw_notfound = true);
  inline move_result to_current_prev_multi(bool throw_notfound = true);
  inline move_result current(bool throw_notfound = true) const;
  inline move_result to_current_next_multi(bool throw_notfound = true);
  inline move_result to_current_last_multi(bool throw_notfound = true);
  inline move_result to_next_first_multi(bool throw_notfound = true);
  inline move_result to_next(bool throw_notfound = true);
  inline move_result to_last(bool throw_notfound = true);

  inline move_result move(move_operation operation, const slice &key,
                          bool throw_notfound);
  inline move_result find(const slice &key, bool throw_notfound = true);
  inline move_result lower_bound(const slice &key, bool throw_notfound = true);

  inline move_result move(move_operation operation, const slice &key,
                          const slice &value, bool throw_notfound);
  inline move_result find_multivalue(const slice &key, const slice &value,
                                     bool throw_notfound = true);
  inline move_result lower_bound_multivalue(const slice &key,
                                            const slice &value,
                                            bool throw_notfound = false);

  inline bool seek(const slice &key);
  inline bool move(move_operation operation, slice &key, slice &value,
                   bool throw_notfound);
  /// \brief Return count of duplicates for current key.
  inline size_t count_multivalue() const;

  inline bool eof() const;
  inline bool on_first() const;
  inline bool on_last() const;
  inline ptrdiff_t estimate(slice key, slice value) const;
  inline ptrdiff_t estimate(slice key) const;
  inline ptrdiff_t estimate(move_operation operation) const;

  //----------------------------------------------------------------------------

  inline void renew(::mdbx::txn_ref &txn);
  inline ::mdbx::txn_ref txn() const;
  inline map_handle map() const;

  inline operator ::mdbx::txn_ref() const { return txn(); }
  inline operator ::mdbx::map_handle() const { return map(); }

  inline MDBX_error_t put(const slice &key, slice *value,
                          MDBX_put_flags_t flags) noexcept;
  inline void insert(const slice &key, slice value);
  inline value_result try_insert(const slice &key, slice value);
  inline slice insert_reserve(const slice &key, size_t value_length);
  inline value_result try_insert_reserve(const slice &key, size_t value_length);

  inline void upsert(const slice &key, const slice &value);
  inline slice upsert_reserve(const slice &key, size_t value_length);

  inline void update(const slice &key, const slice &value);
  inline bool try_update(const slice &key, const slice &value);
  inline slice update_reserve(const slice &key, size_t value_length);
  inline value_result try_update_reserve(const slice &key, size_t value_length);

  inline bool erase(bool whole_multivalue = false);
};

/// \brief Managed cursor.
///
/// As other managed classes, `cursor` destroys the represented underlying
/// object from the own class destructor, but disallows copying and assignment
/// for instances.
///
/// \copydetails MDBX_cursor
class LIBMDBX_API_TYPE cursor : public cursor_ref {
  using inherited = cursor_ref;
  friend class txn_ref;
  /// delegated constructor for RAII
  constexpr cursor(MDBX_cursor *ptr) noexcept : inherited(ptr) {}

public:
  constexpr cursor() noexcept = default;
  void close();

  cursor(cursor &&) = default;
  cursor &operator=(cursor &&) = default;
  cursor(const cursor &) = delete;
  cursor &operator=(const cursor &) = delete;
  virtual ~cursor() noexcept;
};

//------------------------------------------------------------------------------

LIBMDBX_API ::std::ostream &operator<<(::std::ostream &, const ::mdbx::slice &);
LIBMDBX_API ::std::ostream &operator<<(::std::ostream &, const ::mdbx::pair &);
template <class ALLOCATOR>
inline ::std::ostream &operator<<(::std::ostream &out,
                                  const ::mdbx::buffer<ALLOCATOR> &it) {
  return (it.is_freestanding()
              ? out << "buf-" << it.headroom() << "." << it.tailroom()
              : out << "ref-")
         << it.slice();
}
LIBMDBX_API ::std::ostream &operator<<(::std::ostream &,
                                       const ::mdbx::env_ref::geometry::size &);
LIBMDBX_API ::std::ostream &operator<<(::std::ostream &,
                                       const ::mdbx::env_ref::geometry &);
LIBMDBX_API ::std::ostream &
operator<<(::std::ostream &, const ::mdbx::env_ref::operate_parameters &);
LIBMDBX_API ::std::ostream &operator<<(::std::ostream &,
                                       const ::mdbx::env_ref::mode &);
LIBMDBX_API ::std::ostream &operator<<(::std::ostream &,
                                       const ::mdbx::env_ref::durability &);
LIBMDBX_API ::std::ostream &
operator<<(::std::ostream &, const ::mdbx::env_ref::reclaiming_options &);
LIBMDBX_API ::std::ostream &
operator<<(::std::ostream &, const ::mdbx::env_ref::operate_options &);
LIBMDBX_API ::std::ostream &operator<<(::std::ostream &,
                                       const ::mdbx::env::create_parameters &);

LIBMDBX_API ::std::ostream &operator<<(::std::ostream &,
                                       const MDBX_log_level_t &);
LIBMDBX_API ::std::ostream &operator<<(::std::ostream &,
                                       const MDBX_debug_flags_t &);
LIBMDBX_API ::std::ostream &operator<<(::std::ostream &, const ::mdbx::error &);
inline ::std::ostream &operator<<(::std::ostream &out,
                                  const MDBX_error_t &errcode) {
  return out << ::mdbx::error(errcode);
}

} // namespace mdbx

//------------------------------------------------------------------------------

namespace std {

#if defined(__cpp_lib_string_view) && __cpp_lib_string_view >= 201606L
template <> struct is_convertible<::mdbx::slice, string_view> : true_type {};
#if __cplusplus >= 202002L
template <>
struct is_nothrow_convertible<::mdbx::slice, string_view> : true_type {};
#endif /* C++20 */
#endif /* std::string_view */

LIBMDBX_API string to_string(const ::mdbx::slice &);
LIBMDBX_API string to_string(const ::mdbx::pair &);

template <class ALLOCATOR>
inline string to_string(const ::mdbx::buffer<ALLOCATOR> &buffer) {
  ostringstream out;
  out << buffer;
  return out.str();
}

LIBMDBX_API string to_string(const ::mdbx::env_ref::geometry &);
LIBMDBX_API string to_string(const ::mdbx::env_ref::operate_parameters &);
LIBMDBX_API string to_string(const ::mdbx::env_ref::mode &);
LIBMDBX_API string to_string(const ::mdbx::env_ref::durability &);
LIBMDBX_API string to_string(const ::mdbx::env_ref::reclaiming_options &);
LIBMDBX_API string to_string(const ::mdbx::env_ref::operate_options &);
LIBMDBX_API string to_string(const ::mdbx::env::create_parameters &);

LIBMDBX_API string to_string(const ::MDBX_log_level_t &);
LIBMDBX_API string to_string(const ::MDBX_debug_flags_t &);
LIBMDBX_API string to_string(const ::mdbx::error &);
inline string to_string(const ::MDBX_error_t &errcode) {
  return to_string(::mdbx::error(errcode));
}

} // namespace std

//==============================================================================
//
// Inline body of the libmdbx C++ API (preliminary draft)
//

namespace mdbx {

constexpr const version_info &get_version() noexcept { return ::mdbx_version; }
constexpr const build_info &get_build() noexcept { return ::mdbx_build; }

static cxx17_constexpr size_t strlen(const char *c_str) noexcept {
#if defined(__cpp_lib_is_constant_evaluated) &&                                \
    __cpp_lib_is_constant_evaluated >= 201811L
  if (::std::is_constant_evaluated()) {
    for (size_t i = 0; c_str; ++i)
      if (!c_str[i])
        return i;
    return 0;
  }
#endif /* __cpp_lib_is_constant_evaluated >= 201811 */
#if defined(__cpp_lib_string_view) && __cpp_lib_string_view >= 201606L
  return c_str ? ::std::string_view(c_str).length() : 0;
#else
  return c_str ? ::std::strlen(c_str) : 0;
#endif
}

cxx14_constexpr size_t check_length(size_t bytes) {
  if (mdbx_unlikely(bytes > size_t(MDBX_MAXDATASIZE)))
    cxx20_attribute_unlikely throw_max_length_exceeded();
  return bytes;
}

inline bool exception_thunk::is_clean() const noexcept { return !captured_; }

inline void exception_thunk::capture() noexcept {
  assert(is_clean());
  captured_ = ::std::current_exception();
}

inline void exception_thunk::rethrow_captured() const {
  if (captured_)
    cxx20_attribute_unlikely ::std::rethrow_exception(captured_);
}

//------------------------------------------------------------------------------

constexpr error::error(MDBX_error_t error_code) noexcept : code_(error_code) {}

inline error &error::operator=(MDBX_error_t error_code) noexcept {
  code_ = error_code;
  return *this;
}

constexpr bool operator==(const error &a, const error &b) noexcept {
  return a.code_ == b.code_;
}

constexpr bool operator!=(const error &a, const error &b) noexcept {
  return !(a == b);
}

constexpr bool error::is_success() const noexcept {
  return code_ == MDBX_SUCCESS;
}

constexpr bool error::is_result_true() const noexcept {
  return code_ == MDBX_RESULT_FALSE;
}

constexpr bool error::is_result_false() const noexcept {
  return code_ == MDBX_RESULT_TRUE;
}

constexpr bool error::is_failure() const noexcept {
  return code_ != MDBX_SUCCESS && code_ != MDBX_RESULT_TRUE;
}

constexpr MDBX_error_t error::code() const noexcept { return code_; }

constexpr bool error::is_mdbx_error() const noexcept {
  return (code() >= MDBX_FIRST_LMDB_ERRCODE &&
          code() <= MDBX_LAST_LMDB_ERRCODE) ||
         (code() >= MDBX_FIRST_ADDED_ERRCODE &&
          code() <= MDBX_LAST_ADDED_ERRCODEE);
}

inline void error::throw_exception(int error_code) {
  const error trouble(static_cast<MDBX_error_t>(error_code));
  trouble.throw_exception();
}

inline void error::throw_on_failure() const {
  if (mdbx_unlikely(is_failure()))
    cxx20_attribute_unlikely throw_exception();
}

inline void error::success_or_throw() const {
  if (mdbx_unlikely(!is_success()))
    cxx20_attribute_unlikely throw_exception();
}

inline void error::success_or_throw(const exception_thunk &thunk) const {
  assert(thunk.is_clean() || code() != MDBX_SUCCESS);
  if (mdbx_unlikely(!is_success())) {
    cxx20_attribute_unlikely if (!thunk.is_clean()) thunk.rethrow_captured();
    else throw_exception();
  }
}

inline void error::panic_on_failure(const char *context_where,
                                    const char *func_who) const noexcept {
  if (mdbx_unlikely(is_failure()))
    cxx20_attribute_unlikely panic(context_where, func_who);
}

inline void error::success_or_panic(const char *context_where,
                                    const char *func_who) const noexcept {
  if (mdbx_unlikely(!is_success()))
    cxx20_attribute_unlikely panic(context_where, func_who);
}

inline void error::throw_on_nullptr(const void *ptr, MDBX_error_t error_code) {
  if (mdbx_unlikely(ptr == nullptr))
    cxx20_attribute_unlikely error(error_code).throw_exception();
}

inline void error::throw_on_failure(int error_code) {
  error rc(static_cast<MDBX_error_t>(error_code));
  rc.throw_on_failure();
}

inline void error::success_or_throw(MDBX_error_t error_code) {
  error rc(error_code);
  rc.success_or_throw();
}

inline bool error::boolean_or_throw(int error_code) {
  switch (error_code) {
  case MDBX_RESULT_FALSE:
    return false;
  case MDBX_RESULT_TRUE:
    return true;
  default:
    cxx20_attribute_unlikely throw_exception(error_code);
  }
}

inline void error::success_or_throw(int error_code,
                                    const exception_thunk &thunk) {
  error rc(static_cast<MDBX_error_t>(error_code));
  rc.success_or_throw(thunk);
}

inline void error::panic_on_failure(int error_code, const char *context_where,
                                    const char *func_who) noexcept {
  error rc(static_cast<MDBX_error_t>(error_code));
  rc.panic_on_failure(context_where, func_who);
}

inline void error::success_or_panic(int error_code, const char *context_where,
                                    const char *func_who) noexcept {
  error rc(static_cast<MDBX_error_t>(error_code));
  rc.success_or_panic(context_where, func_who);
}

//------------------------------------------------------------------------------

constexpr slice::slice() noexcept : ::MDBX_val({nullptr, 0}) {}

cxx14_constexpr slice::slice(const void *ptr, size_t bytes)
    : ::MDBX_val({const_cast<void *>(ptr), check_length(bytes)}) {}

cxx14_constexpr slice::slice(const void *begin, const void *end)
    : slice(begin, static_cast<const byte *>(end) -
                       static_cast<const byte *>(begin)) {}

cxx17_constexpr slice::slice(const char *c_str)
    : slice(c_str, ::mdbx::strlen(c_str)) {}

cxx14_constexpr slice::slice(const MDBX_val &src)
    : slice(src.iov_base, src.iov_len) {}

inline slice::slice(MDBX_val &&src) : slice(src) { src.iov_base = nullptr; }

inline slice::slice(slice &&src) noexcept : slice(src) { src.invalidate(); }

inline slice &slice::assign(const void *ptr, size_t bytes) {
  iov_base = const_cast<void *>(ptr);
  iov_len = check_length(bytes);
  return *this;
}

inline slice &slice::assign(const slice &src) noexcept {
  iov_base = src.iov_base;
  iov_len = src.iov_len;
  return *this;
}

inline slice &slice::assign(const ::MDBX_val &src) {
  return assign(src.iov_base, src.iov_len);
}

slice &slice::assign(slice &&src) noexcept {
  assign(src);
  src.invalidate();
  return *this;
}

inline slice &slice::assign(::MDBX_val &&src) {
  assign(src.iov_base, src.iov_len);
  src.iov_base = nullptr;
  return *this;
}

inline slice &slice::assign(const void *begin, const void *end) {
  return assign(begin, static_cast<const byte *>(end) -
                           static_cast<const byte *>(begin));
}

inline slice &slice::assign(const char *c_str) {
  return assign(c_str, ::mdbx::strlen(c_str));
}

inline slice &slice::operator=(slice &&src) noexcept {
  return assign(::std::move(src));
}

inline slice &slice::operator=(::MDBX_val &&src) {
  return assign(::std::move(src));
}

inline void slice::swap(slice &other) noexcept {
  const auto temp = *this;
  *this = other;
  other = temp;
}

constexpr const mdbx::byte *slice::byte_ptr() const noexcept {
  return static_cast<const byte *>(iov_base);
}

constexpr const char *slice::char_ptr() const noexcept {
  return static_cast<const char *>(iov_base);
}

constexpr const void *slice::data() const noexcept { return iov_base; }

constexpr size_t slice::length() const noexcept { return iov_len; }

constexpr bool slice::empty() const noexcept { return length() == 0; }

constexpr bool slice::is_null() const noexcept { return data() == nullptr; }

constexpr size_t slice::size() const noexcept { return length(); }

constexpr slice::operator bool() const noexcept { return !is_null(); }

inline void slice::invalidate() noexcept { iov_base = nullptr; }

inline void slice::clear() noexcept {
  iov_base = nullptr;
  iov_len = 0;
}

inline void slice::remove_prefix(size_t n) noexcept {
  assert(n <= size());
  iov_base = static_cast<byte *>(iov_base) + n;
  iov_len -= n;
}

inline void slice::safe_remove_prefix(size_t n) {
  if (mdbx_unlikely(n > size()))
    cxx20_attribute_unlikely throw_out_range();
  remove_prefix(n);
}

inline void slice::remove_suffix(size_t n) noexcept {
  assert(n <= size());
  iov_len -= n;
}

inline void slice::safe_remove_suffix(size_t n) {
  if (mdbx_unlikely(n > size()))
    cxx20_attribute_unlikely throw_out_range();
  remove_suffix(n);
}

inline bool slice::starts_with(const slice &prefix) const noexcept {
  return length() >= prefix.length() &&
         ::std::memcmp(data(), prefix.data(), prefix.length()) == 0;
}

inline bool slice::ends_with(const slice &suffix) const noexcept {
  return length() >= suffix.length() &&
         ::std::memcmp(byte_ptr() + length() - suffix.length(), suffix.data(),
                       suffix.length()) == 0;
}

__nothrow_pure_function cxx14_constexpr size_t
slice::hash_value() const noexcept {
  size_t h = length() * 3977471;
  for (size_t i = 0; i < length(); ++i)
    h = (h ^ static_cast<const uint8_t *>(data())[i]) * 1664525 + 1013904223;
  return h ^ 3863194411 * (h >> 11);
}

inline byte slice::operator[](size_t n) const noexcept {
  assert(n < size());
  return byte_ptr()[n];
}

inline byte slice::at(size_t n) const {
  if (mdbx_unlikely(n >= size()))
    cxx20_attribute_unlikely throw_out_range();
  return byte_ptr()[n];
}

inline slice slice::head(size_t n) const noexcept {
  assert(n <= size());
  return slice(data(), n);
}

inline slice slice::tail(size_t n) const noexcept {
  assert(n <= size());
  return slice(char_ptr() + size() - n, n);
}

inline slice slice::middle(size_t from, size_t n) const noexcept {
  assert(from + n <= size());
  return slice(char_ptr() + from, n);
}

inline slice slice::safe_head(size_t n) const {
  if (mdbx_unlikely(n > size()))
    cxx20_attribute_unlikely throw_out_range();
  return head(n);
}

inline slice slice::safe_tail(size_t n) const {
  if (mdbx_unlikely(n > size()))
    cxx20_attribute_unlikely throw_out_range();
  return tail(n);
}

inline slice slice::safe_middle(size_t from, size_t n) const {
  if (mdbx_unlikely(n > max_length))
    cxx20_attribute_unlikely throw_max_length_exceeded();
  if (mdbx_unlikely(from + n > size()))
    cxx20_attribute_unlikely throw_out_range();
  return middle(from, n);
}

inline intptr_t slice::compare_fast(const slice &a, const slice &b) noexcept {
  const intptr_t diff = a.length() - b.length();
  return diff ? diff
              : (a.data() == b.data())
                    ? 0
                    : ::std::memcmp(a.data(), b.data(), a.length());
}

inline intptr_t slice::compare_lexicographically(const slice &a,
                                                 const slice &b) noexcept {
  const intptr_t diff =
      ::std::memcmp(a.data(), b.data(), ::std::min(a.length(), b.length()));
  return diff ? diff : intptr_t(a.length() - b.length());
}

__nothrow_pure_function inline bool operator==(const slice &a,
                                               const slice &b) noexcept {
  return slice::compare_fast(a, b) == 0;
}

__nothrow_pure_function inline bool operator<(const slice &a,
                                              const slice &b) noexcept {
  return slice::compare_lexicographically(a, b) < 0;
}

__nothrow_pure_function inline bool operator>(const slice &a,
                                              const slice &b) noexcept {
  return slice::compare_lexicographically(a, b) > 0;
}

__nothrow_pure_function inline bool operator<=(const slice &a,
                                               const slice &b) noexcept {
  return slice::compare_lexicographically(a, b) <= 0;
}

__nothrow_pure_function inline bool operator>=(const slice &a,
                                               const slice &b) noexcept {
  return slice::compare_lexicographically(a, b) >= 0;
}

__nothrow_pure_function inline bool operator!=(const slice &a,
                                               const slice &b) noexcept {
  return slice::compare_fast(a, b) != 0;
}

template <class ALLOCATOR>
inline ::mdbx::string<ALLOCATOR>
slice::hex_encode(bool uppercase, const ALLOCATOR &allocator) const {
  ::mdbx::string<ALLOCATOR> result(allocator);
  if (mdbx_likely(length() > 0)) {
    result.reserve(to_hex_bytes());
    result.resize(to_hex(const_cast<char *>(result.data()), result.capacity()) -
                      result.data(),
                  uppercase);
  }
  return result;
}

template <class ALLOCATOR>
inline ::mdbx::string<ALLOCATOR>
slice::hex_decode(const ALLOCATOR &allocator) const {
  ::mdbx::string<ALLOCATOR> result(allocator);
  if (mdbx_likely(length() > 0)) {
    result.reserve(from_hex_bytes());
    result.resize(
        from_hex(static_cast<byte *>(
                     static_cast<void *>(const_cast<char *>(result.data()))),
                 result.capacity()) -
        static_cast<const byte *>(static_cast<const void *>(result.data())));
  }
  return result;
}

template <class ALLOCATOR>
inline ::mdbx::string<ALLOCATOR>
slice::base58_encode(const ALLOCATOR &allocator) const {
  ::mdbx::string<ALLOCATOR> result(allocator);
  if (mdbx_likely(length() > 0)) {
    result.reserve(to_base58_bytes());
    result.resize(
        to_base58(const_cast<char *>(result.data()), result.capacity()) -
        result.data());
  }
  return result;
}

template <class ALLOCATOR>
inline ::mdbx::string<ALLOCATOR>
slice::base58_decode(const ALLOCATOR &allocator) const {
  ::mdbx::string<ALLOCATOR> result(allocator);
  if (mdbx_likely(length() > 0)) {
    result.reserve(from_base58_bytes());
    result.resize(
        from_base58(static_cast<byte *>(
                        static_cast<void *>(const_cast<char *>(result.data()))),
                    result.capacity()) -
        static_cast<const byte *>(static_cast<const void *>(result.data())));
  }
  return result;
}

template <class ALLOCATOR>
inline ::mdbx::string<ALLOCATOR>
slice::base64_encode(const ALLOCATOR &allocator) const {
  ::mdbx::string<ALLOCATOR> result(allocator);
  if (mdbx_likely(length() > 0)) {
    result.reserve(to_base64_bytes());
    result.resize(
        to_base64(const_cast<char *>(result.data()), result.capacity()) -
        result.data());
  }
  return result;
}

template <class ALLOCATOR>
inline ::mdbx::string<ALLOCATOR>
slice::base64_decode(const ALLOCATOR &allocator) const {
  ::mdbx::string<ALLOCATOR> result(allocator);
  if (mdbx_likely(length() > 0)) {
    result.reserve(from_base64_bytes());
    result.resize(
        from_base64(static_cast<byte *>(
                        static_cast<void *>(const_cast<char *>(result.data()))),
                    result.capacity()) -
        static_cast<const byte *>(static_cast<const void *>(result.data())));
  }
  return result;
}

//------------------------------------------------------------------------------

constexpr map_handle::info::info(map_handle::flags flags,
                                 map_handle::state state) noexcept
    : flags(flags), state(state) {}

constexpr ::mdbx::key_mode map_handle::info::key_mode() const noexcept {
  return ::mdbx::key_mode(flags & (MDBX_REVERSEKEY | MDBX_INTEGERKEY));
}

constexpr ::mdbx::value_mode map_handle::info::value_mode() const noexcept {
  return ::mdbx::value_mode(flags & (MDBX_DUPSORT | MDBX_REVERSEDUP |
                                     MDBX_DUPFIXED | MDBX_INTEGERDUP));
}

//------------------------------------------------------------------------------

constexpr env_ref::env_ref(MDBX_env *ptr) noexcept : handle_(ptr) {}

inline env_ref &env_ref::operator=(env_ref &&other) noexcept {
  handle_ = other.handle_;
  other.handle_ = nullptr;
  return *this;
}

inline env_ref::env_ref(env_ref &&other) noexcept : handle_(other.handle_) {
  other.handle_ = nullptr;
}

inline env_ref::~env_ref() noexcept {
#ifndef NDEBUG
  handle_ = reinterpret_cast<MDBX_env *>(uintptr_t(0xDeadBeef));
#endif
}

constexpr env_ref::operator bool() const noexcept { return handle_ != nullptr; }

constexpr env_ref::operator const MDBX_env *() const { return handle_; }

inline env_ref::operator MDBX_env *() { return handle_; }

constexpr bool operator==(const env_ref &a, const env_ref &b) noexcept {
  return a.handle_ == b.handle_;
}

constexpr bool operator!=(const env_ref &a, const env_ref &b) noexcept {
  return a.handle_ != b.handle_;
}

inline env_ref::geometry &
env_ref::geometry::make_fixed(intptr_t size) noexcept {
  size_lower = size_now = size_upper = size;
  growth_step = shrink_threshold = 0;
  return *this;
}

inline env_ref::geometry &
env_ref::geometry::make_dynamic(intptr_t lower, intptr_t upper) noexcept {
  size_now = size_lower = lower;
  size_upper = upper;
  growth_step = shrink_threshold = default_value;
  return *this;
}

inline env_ref::reclaiming_options
env_ref::operate_parameters::reclaiming_from_flags(
    MDBX_env_flags_t flags) noexcept {
  return reclaiming_options(flags);
}

inline env_ref::operate_options env_ref::operate_parameters::options_from_flags(
    MDBX_env_flags_t flags) noexcept {
  return operate_options(flags);
}

inline size_t env_ref::limits::pagesize_min() noexcept {
  return MDBX_MIN_PAGESIZE;
}

inline size_t env_ref::limits::pagesize_max() noexcept {
  return MDBX_MAX_PAGESIZE;
}

inline size_t env_ref::limits::dbsize_min(intptr_t pagesize) {
  const intptr_t result = mdbx_limits_dbsize_min(pagesize);
  if (result < 0)
    cxx20_attribute_unlikely error::throw_exception(MDBX_EINVAL);
  return static_cast<size_t>(result);
}

inline size_t env_ref::limits::dbsize_max(intptr_t pagesize) {
  const intptr_t result = mdbx_limits_dbsize_max(pagesize);
  if (result < 0)
    cxx20_attribute_unlikely error::throw_exception(MDBX_EINVAL);
  return static_cast<size_t>(result);
}

inline size_t env_ref::limits::key_min(MDBX_db_flags_t flags) noexcept {
  return (flags & MDBX_INTEGERKEY) ? 4 : 0;
}

inline size_t env_ref::limits::key_min(key_mode mode) noexcept {
  return key_min(MDBX_db_flags_t(mode));
}

inline size_t env_ref::limits::key_max(intptr_t pagesize,
                                       MDBX_db_flags_t flags) {
  const intptr_t result = mdbx_limits_keysize_max(pagesize, flags);
  if (result < 0)
    cxx20_attribute_unlikely error::throw_exception(MDBX_EINVAL);
  return static_cast<size_t>(result);
}

inline size_t env_ref::limits::key_max(intptr_t pagesize, key_mode mode) {
  return key_max(pagesize, MDBX_db_flags_t(mode));
}

inline size_t env_ref::limits::key_max(const env_ref &env,
                                       MDBX_db_flags_t flags) {
  const intptr_t result = mdbx_env_get_maxkeysize_ex(env, flags);
  if (result < 0)
    cxx20_attribute_unlikely error::throw_exception(MDBX_EINVAL);
  return static_cast<size_t>(result);
}

inline size_t env_ref::limits::key_max(const env_ref &env, key_mode mode) {
  return key_max(env, MDBX_db_flags_t(mode));
}

inline size_t env_ref::limits::value_min(MDBX_db_flags_t flags) noexcept {
  return (flags & MDBX_INTEGERDUP) ? 4 : 0;
}

inline size_t env_ref::limits::value_min(key_mode mode) noexcept {
  return value_min(MDBX_db_flags_t(mode));
}

inline size_t env_ref::limits::value_max(intptr_t pagesize,
                                         MDBX_db_flags_t flags) {
  const intptr_t result = mdbx_limits_valsize_max(pagesize, flags);
  if (result < 0)
    cxx20_attribute_unlikely error::throw_exception(MDBX_EINVAL);
  return static_cast<size_t>(result);
}

inline size_t env_ref::limits::value_max(intptr_t pagesize, key_mode mode) {
  return value_max(pagesize, MDBX_db_flags_t(mode));
}

inline size_t env_ref::limits::value_max(const env_ref &env,
                                         MDBX_db_flags_t flags) {
  const intptr_t result = mdbx_env_get_maxvalsize_ex(env, flags);
  if (result < 0)
    cxx20_attribute_unlikely error::throw_exception(MDBX_EINVAL);
  return static_cast<size_t>(result);
}

inline size_t env_ref::limits::value_max(const env_ref &env, key_mode mode) {
  return value_max(env, MDBX_db_flags_t(mode));
}

inline size_t env_ref::limits::transaction_size_max(intptr_t pagesize) {
  const intptr_t result = mdbx_limits_txnsize_max(pagesize);
  if (result < 0)
    cxx20_attribute_unlikely error::throw_exception(MDBX_EINVAL);
  return static_cast<size_t>(result);
}

inline env_ref::operate_parameters env_ref::get_operation_parameters() const {
  return env_ref::operate_parameters(*this);
}

inline env_ref::mode env_ref::get_mode() const {
  return operate_parameters::mode_from_flags(get_flags());
}

inline env_ref::durability env_ref::get_durability() const {
  return env_ref::operate_parameters::durability_from_flags(get_flags());
}

inline env_ref::reclaiming_options env_ref::get_reclaiming() const {
  return env_ref::operate_parameters::reclaiming_from_flags(get_flags());
}

inline env_ref::operate_options env_ref::get_options() const {
  return env_ref::operate_parameters::options_from_flags(get_flags());
}

inline env_ref::stat env_ref::get_stat() const {
  env_ref::stat r;
  error::success_or_throw(::mdbx_env_stat_ex(handle_, nullptr, &r, sizeof(r)));
  return r;
}

inline env_ref::stat env_ref::get_stat(const txn_ref &txn) const {
  env_ref::stat r;
  error::success_or_throw(::mdbx_env_stat_ex(handle_, txn, &r, sizeof(r)));
  return r;
}

inline env_ref::info env_ref::get_info() const {
  env_ref::info r;
  error::success_or_throw(::mdbx_env_info_ex(handle_, nullptr, &r, sizeof(r)));
  return r;
}

inline env_ref::info env_ref::get_info(const txn_ref &txn) const {
  env_ref::info r;
  error::success_or_throw(::mdbx_env_info_ex(handle_, txn, &r, sizeof(r)));
  return r;
}

inline filehandle env_ref::get_filehandle() const {
  filehandle fd;
  error::success_or_throw(::mdbx_env_get_fd(handle_, &fd));
  return fd;
}

inline MDBX_env_flags_t env_ref::get_flags() const {
  unsigned bits;
  error::success_or_throw(::mdbx_env_get_flags(handle_, &bits));
  return MDBX_env_flags_t(bits);
}

inline unsigned env_ref::max_readers() const {
  unsigned r;
  error::success_or_throw(::mdbx_env_get_maxreaders(handle_, &r));
  return r;
}

inline unsigned env_ref::max_maps() const {
  unsigned r;
  error::success_or_throw(::mdbx_env_get_maxdbs(handle_, &r));
  return r;
}

inline void *env_ref::get_context() const noexcept {
  return mdbx_env_get_userctx(handle_);
}

inline env_ref &env_ref::set_context(void *ptr) {
  error::success_or_throw(::mdbx_env_set_userctx(handle_, ptr));
  return *this;
}

inline env_ref &env_ref::set_sync_threshold(size_t bytes) {
  error::success_or_throw(::mdbx_env_set_syncbytes(handle_, bytes));
  return *this;
}

inline env_ref &env_ref::set_sync_period(unsigned seconds_16dot16) {
  error::success_or_throw(::mdbx_env_set_syncperiod(handle_, seconds_16dot16));
  return *this;
}

inline env_ref &env_ref::set_sync_period(double seconds) {
  return set_sync_period(unsigned(seconds * 65536));
}

inline env_ref &env_ref::alter_flags(MDBX_env_flags_t flags, bool on_off) {
  error::success_or_throw(::mdbx_env_set_flags(handle_, flags, on_off));
  return *this;
}

inline env_ref &env_ref::set_geometry(const geometry &geo) {
  error::success_or_throw(::mdbx_env_set_geometry(
      handle_, geo.size_lower, geo.size_now, geo.size_upper, geo.growth_step,
      geo.shrink_threshold, geo.pagesize));
  return *this;
}

inline env_ref &env_ref::set_max_maps(unsigned maps) {
  error::success_or_throw(::mdbx_env_set_maxdbs(handle_, maps));
  return *this;
}

inline env_ref &env_ref::sync_to_disk() {
  error::success_or_throw(::mdbx_env_sync(handle_));
  return *this;
}

inline env_ref &env_ref::sync_to_disk(bool force, bool nonblock) {
  error::success_or_throw(::mdbx_env_sync_ex(handle_, force, nonblock));
  return *this;
}

inline bool env_ref::poll_sync_to_disk() {
  return error::boolean_or_throw(::mdbx_env_sync_poll(handle_));
}

inline void env_ref::close_map(const map_handle &handle) {
  error::success_or_throw(::mdbx_dbi_close(handle_, handle.dbi));
}

constexpr env_ref::reader_info::reader_info(int slot, mdbx_pid_t pid,
                                            mdbx_tid_t thread, uint64_t txnid,
                                            uint64_t lag, size_t used,
                                            size_t retained) noexcept
    : slot(slot), pid(pid), thread(thread), transaction_id(txnid),
      transaction_lag(lag), bytes_used(used), bytes_retained(retained) {}

template <typename VISITOR>
inline int env_ref::enumerate_readers(VISITOR &visitor) {
  struct reader_visitor_thunk : public exception_thunk {
    VISITOR &visitor_;
    static int cb(void *ctx, int number, int slot, mdbx_pid_t pid,
                  mdbx_tid_t thread, uint64_t txnid, uint64_t lag, size_t used,
                  size_t retained) noexcept {
      reader_visitor_thunk *thunk = static_cast<reader_visitor_thunk *>(ctx);
      assert(thunk->is_clean());
      try {
        const reader_info info(slot, pid, thread, txnid, lag, used, retained);
        return enumeration_loop_control(thunk->visitor_(info, number));
      } catch (... /* capture any exception to rethrow it over C code */) {
        thunk->capture();
        return enumeration_loop_control::exit_loop;
      }
    }
    constexpr reader_visitor_thunk(VISITOR &visitor) noexcept
        : visitor_(visitor) {}
  };
  reader_visitor_thunk thunk(visitor);
  const auto rc = ::mdbx_reader_list(*this, thunk.cb, &thunk);
  thunk.rethow_catched();
  return rc;
}

inline unsigned env_ref::check_readers() {
  int dead_count;
  error::throw_on_failure(::mdbx_reader_check(*this, &dead_count));
  assert(dead_count >= 0);
  return static_cast<unsigned>(dead_count);
}

inline env_ref &env_ref::set_OutOfSpace_callback(MDBX_oom_func *cb) {
  error::success_or_throw(::mdbx_env_set_oomfunc(handle_, cb));
  return *this;
}

inline MDBX_oom_func *env_ref::get_OutOfSpace_callback() const noexcept {
  return ::mdbx_env_get_oomfunc(handle_);
}

inline txn env_ref::start_read() const {
  ::MDBX_txn *ptr;
  error::success_or_throw(
      ::mdbx_txn_begin(handle_, nullptr, MDBX_TXN_RDONLY, &ptr));
  assert(ptr != nullptr);
  return txn(ptr);
}

inline txn env_ref::prepare_read() const {
  ::MDBX_txn *ptr;
  error::success_or_throw(
      ::mdbx_txn_begin(handle_, nullptr, MDBX_TXN_RDONLY_PREPARE, &ptr));
  assert(ptr != nullptr);
  return txn(ptr);
}

inline txn env_ref::start_write(bool dont_wait) {
  ::MDBX_txn *ptr;
  error::success_or_throw(::mdbx_txn_begin(
      handle_, nullptr, dont_wait ? MDBX_TXN_TRY : MDBX_TXN_READWRITE, &ptr));
  assert(ptr != nullptr);
  return txn(ptr);
}

inline txn env_ref::try_start_write() { return start_write(true); }

//------------------------------------------------------------------------------

constexpr txn_ref::txn_ref(MDBX_txn *ptr) noexcept : handle_(ptr) {}

inline txn_ref &txn_ref::operator=(txn_ref &&other) noexcept {
  handle_ = other.handle_;
  other.handle_ = nullptr;
  return *this;
}

inline txn_ref::txn_ref(txn_ref &&other) noexcept : handle_(other.handle_) {
  other.handle_ = nullptr;
}

inline txn_ref::~txn_ref() noexcept {
#ifndef NDEBUG
  handle_ = reinterpret_cast<MDBX_txn *>(uintptr_t(0xDeadBeef));
#endif
}

constexpr txn_ref::operator bool() const noexcept { return handle_ != nullptr; }

constexpr txn_ref::operator const MDBX_txn *() const { return handle_; }

inline txn_ref::operator MDBX_txn *() { return handle_; }

constexpr bool operator==(const txn_ref &a, const txn_ref &b) noexcept {
  return a.handle_ == b.handle_;
}

constexpr bool operator!=(const txn_ref &a, const txn_ref &b) noexcept {
  return a.handle_ != b.handle_;
}

inline bool txn_ref::is_dirty(const void *ptr) const {
  int err = ::mdbx_is_dirty(handle_, ptr);
  switch (err) {
  default:
    cxx20_attribute_unlikely error::throw_exception(err);
  case MDBX_RESULT_TRUE:
    return true;
  case MDBX_RESULT_FALSE:
    return false;
  }
}

inline ::mdbx::env_ref txn_ref::env() const noexcept {
  return ::mdbx_txn_env(handle_);
}

inline MDBX_txn_flags_t txn_ref::flags() const {
  const int bits = mdbx_txn_flags(handle_);
  error::throw_on_failure((bits != -1) ? MDBX_SUCCESS : MDBX_BAD_TXN);
  return static_cast<MDBX_txn_flags_t>(bits);
}

inline uint64_t txn_ref::id() const {
  const uint64_t txnid = mdbx_txn_id(handle_);
  error::throw_on_failure(txnid ? MDBX_SUCCESS : MDBX_BAD_TXN);
  return txnid;
}

inline void txn_ref::reset_reading() {
  error::success_or_throw(::mdbx_txn_reset(handle_));
}

inline void txn_ref::renew_reading() {
  error::success_or_throw(::mdbx_txn_renew(handle_));
}

inline txn_ref::info txn_ref::get_info(bool scan_reader_lock_table) const {
  txn_ref::info r;
  error::success_or_throw(::mdbx_txn_info(handle_, &r, scan_reader_lock_table));
  return r;
}

inline cursor txn_ref::create_cursor(map_handle map) {
  MDBX_cursor *ptr;
  error::success_or_throw(::mdbx_cursor_open(handle_, map.dbi, &ptr));
  return cursor(ptr);
}

inline ::mdbx::map_handle
txn_ref::open_map(const char *name, const ::mdbx::key_mode key_mode,
                  const ::mdbx::value_mode value_mode) const {
  ::mdbx::map_handle map;
  error::success_or_throw(::mdbx_dbi_open(
      handle_, name, MDBX_db_flags_t(key_mode) | MDBX_db_flags_t(value_mode),
      &map.dbi));
  assert(map.dbi != 0);
  return map;
}

inline ::mdbx::map_handle
txn_ref::open_map(const ::std::string &name, const ::mdbx::key_mode key_mode,
                  const ::mdbx::value_mode value_mode) const {
  return open_map(name.c_str(), key_mode, value_mode);
}

inline ::mdbx::map_handle
txn_ref::create_map(const char *name, const ::mdbx::key_mode key_mode,
                    const ::mdbx::value_mode value_mode) {
  ::mdbx::map_handle map;
  error::success_or_throw(::mdbx_dbi_open(
      handle_, name,
      MDBX_CREATE | MDBX_db_flags_t(key_mode) | MDBX_db_flags_t(value_mode),
      &map.dbi));
  assert(map.dbi != 0);
  return map;
}

inline ::mdbx::map_handle
txn_ref::create_map(const ::std::string &name, const ::mdbx::key_mode key_mode,
                    const ::mdbx::value_mode value_mode) {
  return create_map(name.c_str(), key_mode, value_mode);
}

inline void txn_ref::drop_map(map_handle map) {
  error::success_or_throw(::mdbx_drop(handle_, map.dbi, true));
}

inline bool txn_ref::drop_map(const ::std::string &name,
                              bool ignore_nonexists) {
  return drop_map(name.c_str(), ignore_nonexists);
}

inline void txn_ref::clear_map(map_handle map) {
  error::success_or_throw(::mdbx_drop(handle_, map.dbi, false));
}

inline bool txn_ref::clear_map(const ::std::string &name,
                               bool ignore_nonexists) {
  return clear_map(name.c_str(), ignore_nonexists);
}

inline txn_ref::map_stat txn_ref::get_map_stat(map_handle map) const {
  txn_ref::map_stat r;
  error::success_or_throw(::mdbx_dbi_stat(handle_, map.dbi, &r, sizeof(r)));
  return r;
}

inline uint32_t txn_ref::get_tree_deepmask(map_handle map) const {
  uint32_t r;
  error::success_or_throw(::mdbx_dbi_dupsort_depthmask(handle_, map.dbi, &r));
  return r;
}

inline map_handle::info txn_ref::get_handle_info(map_handle map) const {
  unsigned flags, state;
  error::success_or_throw(
      ::mdbx_dbi_flags_ex(handle_, map.dbi, &flags, &state));
  return map_handle::info(MDBX_db_flags_t(flags), MDBX_dbi_state_t(state));
}

inline txn_ref &txn_ref::put_canary(const txn_ref::canary &canary) {
  error::success_or_throw(::mdbx_canary_put(handle_, &canary));
  return *this;
}

inline txn_ref::canary txn_ref::get_canary() const {
  txn_ref::canary r;
  error::success_or_throw(::mdbx_canary_get(handle_, &r));
  return r;
}

inline uint64_t txn_ref::sequence(map_handle map) const {
  uint64_t result;
  error::success_or_throw(::mdbx_dbi_sequence(handle_, map.dbi, &result, 0));
  return result;
}

inline uint64_t txn_ref::sequence(map_handle map, uint64_t increment) {
  uint64_t result;
  error::success_or_throw(
      ::mdbx_dbi_sequence(handle_, map.dbi, &result, increment));
  return result;
}

inline int txn_ref::compare_keys(map_handle map, const slice &a,
                                 const slice &b) const noexcept {
  return ::mdbx_cmp(handle_, map.dbi, &a, &b);
}

inline int txn_ref::compare_values(map_handle map, const slice &a,
                                   const slice &b) const noexcept {
  return ::mdbx_dcmp(handle_, map.dbi, &a, &b);
}

inline int txn_ref::compare_keys(map_handle map, const pair &a,
                                 const pair &b) const noexcept {
  return compare_keys(map, a.key, b.key);
}

inline int txn_ref::compare_values(map_handle map, const pair &a,
                                   const pair &b) const noexcept {
  return compare_values(map, a.value, b.value);
}

inline slice txn_ref::get(map_handle map, const slice &key) const {
  slice result;
  error::success_or_throw(::mdbx_get(handle_, map.dbi, &key, &result));
  return result;
}

inline slice txn_ref::get(map_handle map, slice key,
                          size_t &values_count) const {
  slice result;
  error::success_or_throw(
      ::mdbx_get_ex(handle_, map.dbi, &key, &result, &values_count));
  return result;
}

inline slice txn_ref::get(map_handle map, const slice &key,
                          const slice &if_not_exists) const {
  slice result;
  const int err = ::mdbx_get(handle_, map.dbi, &key, &result);
  switch (err) {
  case MDBX_SUCCESS:
    return result;
  case MDBX_NOTFOUND:
    return if_not_exists;
  default:
    cxx20_attribute_unlikely error::throw_exception(err);
  }
}

inline slice txn_ref::get(map_handle map, slice key, size_t &values_count,
                          const slice &if_not_exists) const {
  slice result;
  const int err = ::mdbx_get_ex(handle_, map.dbi, &key, &result, &values_count);
  switch (err) {
  case MDBX_SUCCESS:
    return result;
  case MDBX_NOTFOUND:
    return if_not_exists;
  default:
    cxx20_attribute_unlikely error::throw_exception(err);
  }
}

inline pair txn_ref::get_equal_or_great(map_handle map,
                                        const slice &key) const {
  pair result{key, slice()};
  error::success_or_throw(
      ::mdbx_get_equal_or_great(handle_, map.dbi, &result.key, &result.value));
  return result;
}

inline pair txn_ref::get_equal_or_great(map_handle map, const slice &key,
                                        const slice &if_not_exists) const {
  pair result{key, slice()};
  const int err =
      ::mdbx_get_equal_or_great(handle_, map.dbi, &result.key, &result.value);
  switch (err) {
  case MDBX_SUCCESS:
    return result;
  case MDBX_NOTFOUND:
    return pair{key, if_not_exists};
  default:
    cxx20_attribute_unlikely error::throw_exception(err);
  }
}

inline MDBX_error_t txn_ref::put(map_handle map, const slice &key, slice *value,
                                 MDBX_put_flags_t flags) noexcept {
  return MDBX_error_t(::mdbx_put(handle_, map.dbi, &key, value, flags));
}

inline void txn_ref::insert(map_handle map, const slice &key, slice value) {
  error::success_or_throw(
      put(map, key, &value /* takes the present value in case MDBX_KEYEXIST */,
          MDBX_put_flags_t(put_mode::insert)));
}

inline value_result txn_ref::try_insert(map_handle map, const slice &key,
                                        slice value) {
  const int err =
      put(map, key, &value /* takes the present value in case MDBX_KEYEXIST */,
          MDBX_put_flags_t(put_mode::insert));
  switch (err) {
  case MDBX_SUCCESS:
    return value_result{slice(), true};
  case MDBX_KEYEXIST:
    return value_result{value, false};
  default:
    cxx20_attribute_unlikely error::throw_exception(err);
  }
}

inline slice txn_ref::insert_reserve(map_handle map, const slice &key,
                                     size_t value_length) {
  slice result(nullptr, value_length);
  error::success_or_throw(
      put(map, key, &result /* takes the present value in case MDBX_KEYEXIST */,
          MDBX_put_flags_t(put_mode::insert) | MDBX_RESERVE));
  return result;
}

inline value_result txn_ref::try_insert_reserve(map_handle map,
                                                const slice &key,
                                                size_t value_length) {
  slice result(nullptr, value_length);
  const int err =
      put(map, key, &result /* takes the present value in case MDBX_KEYEXIST */,
          MDBX_put_flags_t(put_mode::insert) | MDBX_RESERVE);
  switch (err) {
  case MDBX_SUCCESS:
    return value_result{result, true};
  case MDBX_KEYEXIST:
    return value_result{result, false};
  default:
    cxx20_attribute_unlikely error::throw_exception(err);
  }
}

inline void txn_ref::upsert(map_handle map, const slice &key,
                            const slice &value) {
  error::success_or_throw(put(map, key, const_cast<slice *>(&value),
                              MDBX_put_flags_t(put_mode::upsert)));
}

inline slice txn_ref::upsert_reserve(map_handle map, const slice &key,
                                     size_t value_length) {
  slice result(nullptr, value_length);
  error::success_or_throw(put(
      map, key, &result, MDBX_put_flags_t(put_mode::upsert) | MDBX_RESERVE));
  return result;
}

inline void txn_ref::update(map_handle map, const slice &key,
                            const slice &value) {
  error::success_or_throw(put(map, key, const_cast<slice *>(&value),
                              MDBX_put_flags_t(put_mode::update)));
}

inline bool txn_ref::try_update(map_handle map, const slice &key,
                                const slice &value) {
  const int err = put(map, key, const_cast<slice *>(&value),
                      MDBX_put_flags_t(put_mode::update));
  switch (err) {
  case MDBX_SUCCESS:
    return true;
  case MDBX_NOTFOUND:
    return false;
  default:
    cxx20_attribute_unlikely error::throw_exception(err);
  }
}

inline slice txn_ref::update_reserve(map_handle map, const slice &key,
                                     size_t value_length) {
  slice result(nullptr, value_length);
  error::success_or_throw(put(
      map, key, &result, MDBX_put_flags_t(put_mode::update) | MDBX_RESERVE));
  return result;
}

inline value_result txn_ref::try_update_reserve(map_handle map,
                                                const slice &key,
                                                size_t value_length) {
  slice result(nullptr, value_length);
  const int err =
      put(map, key, &result, MDBX_put_flags_t(put_mode::update) | MDBX_RESERVE);
  switch (err) {
  case MDBX_SUCCESS:
    return value_result{result, true};
  case MDBX_NOTFOUND:
    return value_result{slice(), false};
  default:
    cxx20_attribute_unlikely error::throw_exception(err);
  }
}

inline bool txn_ref::erase(map_handle map, const slice &key) {
  const int err = ::mdbx_del(handle_, map.dbi, &key, nullptr);
  switch (err) {
  case MDBX_SUCCESS:
    return true;
  case MDBX_NOTFOUND:
    return false;
  default:
    cxx20_attribute_unlikely error::throw_exception(err);
  }
}

inline bool txn_ref::erase(map_handle map, const slice &key,
                           const slice &value) {
  const int err = ::mdbx_del(handle_, map.dbi, &key, &value);
  switch (err) {
  case MDBX_SUCCESS:
    return true;
  case MDBX_NOTFOUND:
    return false;
  default:
    cxx20_attribute_unlikely error::throw_exception(err);
  }
}

inline void txn_ref::replace(map_handle map, const slice &key, slice old_value,
                             const slice &new_value) {
  error::success_or_throw(::mdbx_replace_ex(
      handle_, map.dbi, &key, const_cast<slice *>(&new_value), &old_value,
      MDBX_CURRENT | MDBX_NOOVERWRITE, nullptr, nullptr));
}

template <class ALLOCATOR>
inline buffer<ALLOCATOR> txn_ref::extract(map_handle map, const slice &key,
                                          const ALLOCATOR &allocator) {
  buffer<ALLOCATOR> result(allocator);
  typename buffer<ALLOCATOR>::data_preserver thunk;
  error::success_or_throw(::mdbx_replace_ex(handle_, map.dbi, &key, nullptr,
                                            &result.slice_, MDBX_CURRENT, thunk,
                                            &thunk),
                          thunk);
  return result;
}

template <class ALLOCATOR>
inline buffer<ALLOCATOR> txn_ref::replace(map_handle map, const slice &key,
                                          const slice &new_value,
                                          const ALLOCATOR &allocator) {
  buffer<ALLOCATOR> result(allocator);
  typename buffer<ALLOCATOR>::data_preserver thunk;
  error::success_or_throw(
      ::mdbx_replace_ex(handle_, map.dbi, &key, const_cast<slice *>(&new_value),
                        &result.slice_, MDBX_CURRENT, thunk, &thunk),
      thunk);
  return result;
}

template <class ALLOCATOR>
inline buffer<ALLOCATOR>
txn_ref::replace_reserve(map_handle map, const slice &key, slice &new_value,
                         const ALLOCATOR &allocator) {
  buffer<ALLOCATOR> result(allocator);
  typename buffer<ALLOCATOR>::data_preserver thunk;
  error::success_or_throw(
      ::mdbx_replace_ex(handle_, map.dbi, &key, &new_value, &result.slice_,
                        MDBX_CURRENT | MDBX_RESERVE, thunk, &thunk),
      thunk);
  return result;
}

inline void txn_ref::append(map_handle map, const slice &key,
                            const slice &value,
                            bool multivalue_order_preserved) {
  error::success_or_throw(::mdbx_put(
      handle_, map.dbi, const_cast<slice *>(&key), const_cast<slice *>(&value),
      multivalue_order_preserved ? (MDBX_APPEND | MDBX_APPENDDUP)
                                 : MDBX_APPEND));
}

inline size_t txn_ref::put_multiple(map_handle map, const slice &key,
                                    const size_t value_length,
                                    const void *values_array,
                                    size_t values_count, put_mode mode,
                                    bool allow_partial) {
  MDBX_val args[2] = {{const_cast<void *>(values_array), value_length},
                      {nullptr, values_count}};
  const int err = ::mdbx_put(handle_, map.dbi, const_cast<slice *>(&key), args,
                             MDBX_put_flags_t(mode) | MDBX_MULTIPLE);
  switch (err) {
  case MDBX_SUCCESS:
    cxx20_attribute_likely break;
  case MDBX_KEYEXIST:
    if (allow_partial)
      break;
    mdbx_txn_break(handle_);
  default:
    cxx20_attribute_unlikely error::throw_exception(err);
  }
  return args[1].iov_len /* done item count */;
}

inline ptrdiff_t txn_ref::estimate(map_handle map, pair from, pair to) const {
  ptrdiff_t result;
  error::success_or_throw(mdbx_estimate_range(
      handle_, map.dbi, &from.key, &from.value, &to.key, &to.value, &result));
  return result;
}

inline ptrdiff_t txn_ref::estimate(map_handle map, slice from, slice to) const {
  ptrdiff_t result;
  error::success_or_throw(mdbx_estimate_range(handle_, map.dbi, &from, nullptr,
                                              &to, nullptr, &result));
  return result;
}

inline ptrdiff_t txn_ref::estimate_from_first(map_handle map, slice to) const {
  ptrdiff_t result;
  error::success_or_throw(mdbx_estimate_range(handle_, map.dbi, nullptr,
                                              nullptr, &to, nullptr, &result));
  return result;
}

inline ptrdiff_t txn_ref::estimate_to_last(map_handle map, slice from) const {
  ptrdiff_t result;
  error::success_or_throw(mdbx_estimate_range(handle_, map.dbi, &from, nullptr,
                                              nullptr, nullptr, &result));
  return result;
}

//------------------------------------------------------------------------------

constexpr cursor_ref::cursor_ref(MDBX_cursor *ptr) noexcept : handle_(ptr) {}

inline cursor_ref &cursor_ref::operator=(cursor_ref &&other) noexcept {
  handle_ = other.handle_;
  other.handle_ = nullptr;
  return *this;
}

inline cursor_ref::cursor_ref(cursor_ref &&other) noexcept
    : handle_(other.handle_) {
  other.handle_ = nullptr;
}

inline cursor_ref::~cursor_ref() noexcept {
#ifndef NDEBUG
  handle_ = reinterpret_cast<MDBX_cursor *>(uintptr_t(0xDeadBeef));
#endif
}

constexpr cursor_ref::operator bool() const noexcept {
  return handle_ != nullptr;
}

constexpr cursor_ref::operator const MDBX_cursor *() const { return handle_; }

inline cursor_ref::operator MDBX_cursor *() { return handle_; }

constexpr bool operator==(const cursor_ref &a, const cursor_ref &b) noexcept {
  return a.handle_ == b.handle_;
}

constexpr bool operator!=(const cursor_ref &a, const cursor_ref &b) noexcept {
  return a.handle_ != b.handle_;
}

inline cursor_ref::move_result::move_result(const cursor_ref &cursor,
                                            bool throw_notfound) {
  done = cursor.move(get_current, &key, &value, throw_notfound);
}

inline cursor_ref::move_result::move_result(cursor_ref &cursor,
                                            move_operation operation,
                                            bool throw_notfound) {
  done = cursor.move(operation, &key, &value, throw_notfound);
}

inline cursor_ref::move_result::move_result(cursor_ref &cursor,
                                            move_operation operation,
                                            const slice &key,
                                            bool throw_notfound) {
  this->key = key;
  this->done = cursor.move(operation, &this->key, &this->value, throw_notfound);
}

inline cursor_ref::move_result::move_result(cursor_ref &cursor,
                                            move_operation operation,
                                            const slice &key,
                                            const slice &value,
                                            bool throw_notfound) {
  this->key = key;
  this->value = value;
  this->done = cursor.move(operation, &this->key, &this->value, throw_notfound);
}

inline bool cursor_ref::move(move_operation operation, MDBX_val *key,
                             MDBX_val *value, bool throw_notfound) const {
  const int err =
      ::mdbx_cursor_get(handle_, key, value, MDBX_cursor_op(operation));
  switch (err) {
  case MDBX_SUCCESS:
    cxx20_attribute_likely return true;
  case MDBX_NOTFOUND:
    if (!throw_notfound)
      return false;
    cxx17_attribute_fallthrough /* fallthrough */;
  default:
    cxx20_attribute_unlikely error::throw_exception(err);
  }
}

inline ptrdiff_t cursor_ref::estimate(move_operation operation, MDBX_val *key,
                                      MDBX_val *value) const {
  ptrdiff_t result;
  error::success_or_throw(::mdbx_estimate_move(
      *this, key, value, MDBX_cursor_op(operation), &result));
  return result;
}

inline ptrdiff_t estimate(const cursor_ref &from, const cursor_ref &to) {
  ptrdiff_t result;
  error::success_or_throw(mdbx_estimate_distance(from, to, &result));
  return result;
}

inline cursor_ref::move_result cursor_ref::move(move_operation operation,
                                                bool throw_notfound) {
  return move_result(*this, operation, throw_notfound);
}

inline cursor_ref::move_result cursor_ref::to_first(bool throw_notfound) {
  return move(first, throw_notfound);
}

inline cursor_ref::move_result cursor_ref::to_previous(bool throw_notfound) {
  return move(previous, throw_notfound);
}

inline cursor_ref::move_result
cursor_ref::to_previous_last_multi(bool throw_notfound) {
  return move(multi_prevkey_lastvalue, throw_notfound);
}

inline cursor_ref::move_result
cursor_ref::to_current_first_multi(bool throw_notfound) {
  return move(multi_currentkey_firstvalue, throw_notfound);
}

inline cursor_ref::move_result
cursor_ref::to_current_prev_multi(bool throw_notfound) {
  return move(multi_currentkey_prevvalue, throw_notfound);
}

inline cursor_ref::move_result cursor_ref::current(bool throw_notfound) const {
  return move_result(*this, throw_notfound);
}

inline cursor_ref::move_result
cursor_ref::to_current_next_multi(bool throw_notfound) {
  return move(multi_currentkey_nextvalue, throw_notfound);
}

inline cursor_ref::move_result
cursor_ref::to_current_last_multi(bool throw_notfound) {
  return move(multi_currentkey_lastvalue, throw_notfound);
}

inline cursor_ref::move_result
cursor_ref::to_next_first_multi(bool throw_notfound) {
  return move(multi_nextkey_firstvalue, throw_notfound);
}

inline cursor_ref::move_result cursor_ref::to_next(bool throw_notfound) {
  return move(next, throw_notfound);
}

inline cursor_ref::move_result cursor_ref::to_last(bool throw_notfound) {
  return move(last, throw_notfound);
}

inline cursor_ref::move_result cursor_ref::move(move_operation operation,
                                                const slice &key,
                                                bool throw_notfound) {
  return move_result(*this, operation, key, throw_notfound);
}

inline cursor_ref::move_result cursor_ref::find(const slice &key,
                                                bool throw_notfound) {
  return move(key_exact, key, throw_notfound);
}

inline cursor_ref::move_result cursor_ref::lower_bound(const slice &key,
                                                       bool throw_notfound) {
  return move(key_lowerbound, key, throw_notfound);
}

inline cursor_ref::move_result cursor_ref::move(move_operation operation,
                                                const slice &key,
                                                const slice &value,
                                                bool throw_notfound) {
  return move_result(*this, operation, key, value, throw_notfound);
}

inline cursor_ref::move_result
cursor_ref::find_multivalue(const slice &key, const slice &value,
                            bool throw_notfound) {
  return move(key_exact, key, value, throw_notfound);
}

inline cursor_ref::move_result
cursor_ref::lower_bound_multivalue(const slice &key, const slice &value,
                                   bool throw_notfound) {
  return move(multi_exactkey_lowerboundvalue, key, value, throw_notfound);
}

inline bool cursor_ref::seek(const slice &key) {
  return move(find_key, const_cast<slice *>(&key), nullptr, false);
}

inline bool cursor_ref::move(move_operation operation, slice &key, slice &value,
                             bool throw_notfound) {
  return move(operation, &key, &value, throw_notfound);
}

inline size_t cursor_ref::count_multivalue() const {
  size_t result;
  error::success_or_throw(::mdbx_cursor_count(*this, &result));
  return result;
}

inline bool cursor_ref::eof() const {
  return error::boolean_or_throw(::mdbx_cursor_eof(*this));
}

inline bool cursor_ref::on_first() const {
  return error::boolean_or_throw(::mdbx_cursor_on_first(*this));
}

inline bool cursor_ref::on_last() const {
  return error::boolean_or_throw(::mdbx_cursor_on_last(*this));
}

inline ptrdiff_t cursor_ref::estimate(slice key, slice value) const {
  return estimate(multi_exactkey_lowerboundvalue, &key, &value);
}

inline ptrdiff_t cursor_ref::estimate(slice key) const {
  return estimate(key_lowerbound, &key, nullptr);
}

inline ptrdiff_t cursor_ref::estimate(move_operation operation) const {
  slice unused_key;
  return estimate(operation, &unused_key, nullptr);
}

inline void cursor_ref::renew(::mdbx::txn_ref &txn) {
  error::success_or_throw(::mdbx_cursor_renew(txn, handle_));
}

inline txn_ref cursor_ref::txn() const {
  MDBX_txn *txn = ::mdbx_cursor_txn(handle_);
  error::throw_on_nullptr(txn, MDBX_EINVAL);
  return ::mdbx::txn_ref(txn);
}

inline map_handle cursor_ref::map() const {
  const MDBX_dbi dbi = ::mdbx_cursor_dbi(handle_);
  if (mdbx_unlikely(dbi > MDBX_MAX_DBI))
    error::throw_exception(MDBX_EINVAL);
  return map_handle(dbi);
}

inline MDBX_error_t cursor_ref::put(const slice &key, slice *value,
                                    MDBX_put_flags_t flags) noexcept {
  return MDBX_error_t(::mdbx_cursor_put(handle_, &key, value, flags));
}

inline void cursor_ref::insert(const slice &key, slice value) {
  error::success_or_throw(
      put(key, &value /* takes the present value in case MDBX_KEYEXIST */,
          MDBX_put_flags_t(put_mode::insert)));
}

inline value_result cursor_ref::try_insert(const slice &key, slice value) {
  const int err =
      put(key, &value /* takes the present value in case MDBX_KEYEXIST */,
          MDBX_put_flags_t(put_mode::insert));
  switch (err) {
  case MDBX_SUCCESS:
    return value_result{slice(), true};
  case MDBX_KEYEXIST:
    return value_result{value, false};
  default:
    cxx20_attribute_unlikely error::throw_exception(err);
  }
}

inline slice cursor_ref::insert_reserve(const slice &key, size_t value_length) {
  slice result(nullptr, value_length);
  error::success_or_throw(
      put(key, &result /* takes the present value in case MDBX_KEYEXIST */,
          MDBX_put_flags_t(put_mode::insert) | MDBX_RESERVE));
  return result;
}

inline value_result cursor_ref::try_insert_reserve(const slice &key,
                                                   size_t value_length) {
  slice result(nullptr, value_length);
  const int err =
      put(key, &result /* takes the present value in case MDBX_KEYEXIST */,
          MDBX_put_flags_t(put_mode::insert) | MDBX_RESERVE);
  switch (err) {
  case MDBX_SUCCESS:
    return value_result{result, true};
  case MDBX_KEYEXIST:
    return value_result{result, false};
  default:
    cxx20_attribute_unlikely error::throw_exception(err);
  }
}

inline void cursor_ref::upsert(const slice &key, const slice &value) {
  error::success_or_throw(put(key, const_cast<slice *>(&value),
                              MDBX_put_flags_t(put_mode::upsert)));
}

inline slice cursor_ref::upsert_reserve(const slice &key, size_t value_length) {
  slice result(nullptr, value_length);
  error::success_or_throw(
      put(key, &result, MDBX_put_flags_t(put_mode::upsert) | MDBX_RESERVE));
  return result;
}

inline void cursor_ref::update(const slice &key, const slice &value) {
  error::success_or_throw(put(key, const_cast<slice *>(&value),
                              MDBX_put_flags_t(put_mode::update)));
}

inline bool cursor_ref::try_update(const slice &key, const slice &value) {
  const int err =
      put(key, const_cast<slice *>(&value), MDBX_put_flags_t(put_mode::update));
  switch (err) {
  case MDBX_SUCCESS:
    return true;
  case MDBX_NOTFOUND:
    return false;
  default:
    cxx20_attribute_unlikely error::throw_exception(err);
  }
}

inline slice cursor_ref::update_reserve(const slice &key, size_t value_length) {
  slice result(nullptr, value_length);
  error::success_or_throw(
      put(key, &result, MDBX_put_flags_t(put_mode::update) | MDBX_RESERVE));
  return result;
}

inline value_result cursor_ref::try_update_reserve(const slice &key,
                                                   size_t value_length) {
  slice result(nullptr, value_length);
  const int err =
      put(key, &result, MDBX_put_flags_t(put_mode::update) | MDBX_RESERVE);
  switch (err) {
  case MDBX_SUCCESS:
    return value_result{result, true};
  case MDBX_NOTFOUND:
    return value_result{slice(), false};
  default:
    cxx20_attribute_unlikely error::throw_exception(err);
  }
}

inline bool cursor_ref::erase(bool whole_multivalue) {
  const int err = ::mdbx_cursor_del(handle_, whole_multivalue ? MDBX_ALLDUPS
                                                              : MDBX_CURRENT);
  switch (err) {
  case MDBX_SUCCESS:
    cxx20_attribute_likely return true;
  case MDBX_NOTFOUND:
    return false;
  default:
    cxx20_attribute_unlikely error::throw_exception(err);
  }
}

//------------------------------------------------------------------------------

template <class ALLOCATOR>
inline buffer<ALLOCATOR>::buffer(const txn_ref &txn, const ::mdbx::slice &src,
                                 const ALLOCATOR &allocator)
    : buffer(src, !txn.is_dirty(src.data()), allocator) {}

template <class ALLOCATOR>
inline buffer<ALLOCATOR>::buffer(size_t head_room, size_t tail_room,
                                 const ALLOCATOR &allocator)
    : silo_(allocator) {
  if (mdbx_unlikely(head_room > max_length || tail_room > max_length ||
                    head_room + tail_room > max_length))
    throw_max_length_exceeded();
  silo_.reserve(head_room + tail_room);
  silo_.append(head_room, '\0');
  slice_.iov_base = const_cast<char *>(silo_.data());
  assert(slice_.iov_len == 0);
}

template <class ALLOCATOR>
inline buffer<ALLOCATOR>::buffer(size_t capacity, const ALLOCATOR &allocator)
    : silo_(allocator) {
  silo_.reserve(check_length(capacity));
  slice_.iov_base = const_cast<char *>(silo_.data());
  assert(slice_.iov_len == 0);
}

template <class ALLOCATOR>
inline buffer<ALLOCATOR>::buffer(size_t head_room, const ::mdbx::slice &src,
                                 size_t tail_room, const ALLOCATOR &allocator)
    : silo_(allocator) {
  if (mdbx_unlikely(head_room > max_length || tail_room > max_length ||
                    head_room + tail_room > max_length - slice_.length()))
    throw_max_length_exceeded();
  silo_.reserve(head_room + src.length() + tail_room);
  silo_.append(head_room, '\0');
  silo_.append(src.char_ptr(), src.length());
  slice_.iov_base = const_cast<char *>(silo_.data());
  slice_.iov_len = src.length();
}

template <class ALLOCATOR>
inline void buffer<ALLOCATOR>::reserve(size_t wanna_headroom,
                                       size_t wanna_tailroom,
                                       size_t shrink_threshold) {
  if (mdbx_unlikely(
          wanna_headroom > max_length || wanna_tailroom > max_length ||
          wanna_headroom + wanna_tailroom > max_length - slice_.length()))
    throw_max_length_exceeded();

  wanna_headroom = std::min(std::max(headroom(), wanna_headroom),
                            wanna_headroom + shrink_threshold);
  wanna_tailroom = std::min(std::max(tailroom(), wanna_tailroom),
                            wanna_tailroom + shrink_threshold);
  const auto wanna_capacity = wanna_headroom + slice_.length() + wanna_tailroom;
  if (is_reference() || slice_.empty()) {
    silo_.reserve(wanna_capacity);
    silo_.resize(wanna_headroom);
    silo_.append(slice_.char_ptr(), slice_.length());
  } else {
    const auto was_headroom = headroom();
    if (was_headroom > wanna_headroom)
      silo_.erase(wanna_headroom, was_headroom - wanna_headroom);
    silo_.reserve(wanna_capacity);
    if (was_headroom < wanna_headroom)
      silo_.insert(was_headroom, wanna_headroom - was_headroom, '\0');
  }
  slice_.iov_base = const_cast<byte *>(silo_begin()) + wanna_headroom;
  assert(headroom() >= wanna_headroom &&
         headroom() <= wanna_headroom + shrink_threshold);
  assert(tailroom() >= wanna_tailroom &&
         tailroom() <= wanna_tailroom + shrink_threshold);
}

template <class ALLOCATOR>
inline buffer<ALLOCATOR> &buffer<ALLOCATOR>::append(const void *src,
                                                    size_t bytes) {
  if (mdbx_unlikely(tailroom() < check_length(bytes)))
    reserve(0, bytes);
  std::memcpy(static_cast<char *>(slice_.iov_base) + size(), src, bytes);
  slice_.iov_len += bytes;
  return *this;
}

template <class ALLOCATOR>
inline buffer<ALLOCATOR> &buffer<ALLOCATOR>::add_header(const void *src,
                                                        size_t bytes) {
  if (mdbx_unlikely(headroom() < check_length(bytes)))
    reserve(bytes, 0);
  slice_.iov_base =
      std::memcpy(static_cast<char *>(slice_.iov_base) - bytes, src, bytes);
  slice_.iov_len += bytes;
  return *this;
}

template <class ALLOCATOR>
inline void buffer<ALLOCATOR>::swap(buffer &other)
#if defined(__cpp_noexcept_function_type) &&                                   \
    __cpp_noexcept_function_type >= 201510L
    noexcept(
        std::allocator_traits<ALLOCATOR>::propagate_on_container_swap::value
#if defined(__cpp_lib_allocator_traits_is_always_equal) &&                     \
    __cpp_lib_allocator_traits_is_always_equal >= 201411L
        || std::allocator_traits<ALLOCATOR>::is_always_equal::value
#endif /* __cpp_lib_allocator_traits_is_always_equal */
    )
#endif /* __cpp_noexcept_function_type */
{
  if /* checking the equality of allocators to avoid UB */
#if defined(__cpp_if_constexpr) && __cpp_if_constexpr >= 201606L
      constexpr
#endif
      (!std::allocator_traits<ALLOCATOR>::propagate_on_container_swap::value
#if defined(__cpp_lib_allocator_traits_is_always_equal) &&                     \
    __cpp_lib_allocator_traits_is_always_equal >= 201411L
       && !std::allocator_traits<ALLOCATOR>::is_always_equal::value
#endif /* __cpp_lib_allocator_traits_is_always_equal */
      ) {
    if (mdbx_unlikely(silo_.get_allocator() != other.silo_.get_allocator()))
      throw std::bad_alloc();
  }
  silo_.swap(other.silo_);
  slice_.swap(other.slice_);
}

template <class ALLOCATOR>
inline int buffer<ALLOCATOR>::data_preserver::callback(void *context,
                                                       MDBX_val *target,
                                                       const void *src,
                                                       size_t bytes) noexcept {
  const auto self = static_cast<data_preserver *>(context);
  assert(self->is_clean());
  try {
    owner_of(static_cast<::mdbx::slice *>(target), &buffer::slice_)
        ->assign(src, bytes, false);
    return MDBX_RESULT_FALSE;
  } catch (... /* capture any exception to rethrow it over C code */) {
    self->capture();
    return MDBX_RESULT_TRUE;
  }
}

} // namespace mdbx

/* Undo workaround for old compilers without properly support for constexpr. */
#ifdef constexpr
#undef constexpr
#endif

#ifdef _MSC_VER
#pragma warning(pop)
#endif

/// @} end of C++ API
