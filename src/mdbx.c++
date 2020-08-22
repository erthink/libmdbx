/*
 * Copyright 2020 Leonid Yuriev <leo@yuriev.ru>
 * and other libmdbx authors: please see AUTHORS file.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in the file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>. */

//
// Non-inline part of the libmdbx C++ API (preliminary draft)
//

#ifdef _MSC_VER
#define _CRT_SECURE_NO_WARNINGS
#endif

#include "../mdbx.h++"

#include "defs.h"
#include "internals.h"

#include <atomic>
#include <sstream>
#include <system_error>

#if defined(__has_include) && __has_include(<version>)
#include <version>
#endif

#if defined(__cpp_lib_filesystem) && __cpp_lib_filesystem >= 201703L
#include <filesystem>
#endif /* __cpp_lib_filesystem >= 201703L */

namespace {

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
  cxx11_constexpr trouble_location(unsigned line, const char *condition,
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

__cold std::string format_va(const char *fmt, va_list ap) {
  va_list ones;
  va_copy(ones, ap);
#ifdef _MSC_VER
  int needed = _vscprintf(fmt, ap);
#else
  int needed = vsnprintf(nullptr, 0, fmt, ap);
#endif
  assert(needed >= 0);
  std::string result;
  result.reserve((size_t)needed + 1);
  result.resize((size_t)needed, '\0');
  assert((int)result.capacity() > needed);
  int actual = vsnprintf((char *)result.data(), result.capacity(), fmt, ones);
  assert(actual == needed);
  (void)actual;
  va_end(ones);
  return result;
}

__cold std::string format(const char *fmt, ...) {
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
   * and for "function 'BAR' was declared but never referenced" fomr LCC. */
#ifndef __LCC__
  const trouble_location &location() const noexcept { return location_; }
#endif
  virtual ~bug() noexcept;
};

__cold bug::bug(const trouble_location &location) noexcept
    : std::runtime_error(format("mdbx.bug: %s.%s at %s:%u", location.function(),
                                location.condition(), location.filename(),
                                location.line())),
      location_(location) {}

__cold bug::~bug() noexcept {}

[[noreturn]] __cold void raise_bug(const trouble_location &what_and_where) {
  throw bug(what_and_where);
}

#define RAISE_BUG(line, condition, function, file)                             \
  do {                                                                         \
    static cxx11_constexpr_var trouble_location bug(line, condition, function, \
                                                    file);                     \
    raise_bug(bug);                                                            \
  } while (0)

#define ENSURE(condition)                                                      \
  do                                                                           \
    if (unlikely(!(condition)))                                                \
      RAISE_BUG(__LINE__, #condition, __func__, __FILE__);                     \
  while (0)

#define NOT_IMPLEMENTED()                                                      \
  RAISE_BUG(__LINE__, "not_implemented", __func__, __FILE__);

//------------------------------------------------------------------------------

template <typename PATH> struct path_to_pchar {
  const std::string str;
  path_to_pchar(const PATH &path) : str(path.generic_string()) {}
  operator const char *() const { return str.c_str(); }
};

template <typename PATH> PATH pchar_to_path(const char *c_str) {
  return PATH(c_str);
}

template <> struct path_to_pchar<std::string> {
  const char *const ptr;
  path_to_pchar(const std::string &path) : ptr(path.c_str()) {}
  operator const char *() const { return ptr; }
};

#if defined(_WIN32) || defined(_WIN64)

template <> struct path_to_pchar<std::wstring> {
  std::string str;
  path_to_pchar(const std::wstring &path) {
    if (!path.empty()) {
      const int chars =
          WideCharToMultiByte(CP_UTF8, WC_ERR_INVALID_CHARS, path.data(),
                              int(path.size()), nullptr, 0, nullptr, nullptr);
      if (chars == 0)
        mdbx::error::throw_exception(GetLastError());
      str.append(chars, '\0');
      WideCharToMultiByte(CP_UTF8, WC_ERR_INVALID_CHARS, path.data(),
                          int(path.size()), const_cast<char *>(str.data()),
                          chars, nullptr, nullptr);
    }
  }
  operator const char *() const { return str.c_str(); }
};

template <> std::wstring pchar_to_path<std::wstring>(const char *c_str) {
  std::wstring wstr;
  if (c_str && *c_str) {
    const int chars = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, c_str,
                                          int(strlen(c_str)), nullptr, 0);
    if (chars == 0)
      mdbx::error::throw_exception(GetLastError());
    wstr.append(chars, '\0');
    MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, c_str,
                        int(strlen(c_str)), const_cast<wchar_t *>(wstr.data()),
                        chars);
  }
  return wstr;
}

#endif /* Windows */

} // namespace

//------------------------------------------------------------------------------

namespace mdbx {

[[noreturn]] __cold void throw_max_length_exceeded() {
  throw std::length_error(
      "mdbx:: exceeded the maximal length of data/slice/buffer");
}

__cold exception::exception(const error &error) noexcept
    : base(error.what()), error_(error) {}

__cold exception::~exception() noexcept {}

static std::atomic_int fatal_countdown;

__cold fatal::fatal(const error &error_) noexcept : error_(error_) {
  ++fatal_countdown;
}

__cold fatal::fatal(const fatal &src) noexcept : error_(src.error_) {
  ++fatal_countdown;
}

__cold fatal::fatal(fatal &&src) noexcept : error_(src.error_) {
  ++fatal_countdown;
}

__cold fatal::~fatal() noexcept {
  if (--fatal_countdown == 0)
    std::terminate();
}

__cold const char *fatal::what() const noexcept { return error_.what(); }

#define DEFINE_EXCEPTION(NAME)                                                 \
  __cold NAME::NAME(const error &rc) : exception(rc) {}                        \
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
DEFINE_EXCEPTION(operation_not_permited)
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
    return STRINGIFY(CODE)
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
  ::mdbx_panic("mdbx::%s.%s() failed: \"%s\" (%d)", context, func, what(),
               code());
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
    CASE_EXCEPTION(operation_not_permited, MDBX_EPERM);
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

bool slice::is_base64() const noexcept {
  NOT_IMPLEMENTED();
  return true;
}

bool slice::is_hex() const noexcept {
  NOT_IMPLEMENTED();
  return true;
}

bool slice::is_printable(bool allow_utf8) const noexcept {
  NOT_IMPLEMENTED();
  return allow_utf8;
}

std::string slice::hex_string(bool uppercase) const {
  std::string result;
  if (length() > 0) {
    result.reserve(length() * 2);
    const uint8_t *ptr = static_cast<const uint8_t *>(data());
    const uint8_t *const end = ptr + length();
    const char x0A = (uppercase ? 'A' : 'a') - 10;
    do {
      char high = *ptr >> 4;
      char low = *ptr & 15;
      result.push_back((high < 10) ? high + '0' : high + x0A);
      result.push_back((low < 10) ? low + '0' : low + x0A);
    } while (++ptr < end);
  }
  return result;
}

std::string slice::base64_string() const {
  std::string result;
  NOT_IMPLEMENTED();
  return result;
}

//------------------------------------------------------------------------------

void buffer::reserve(size_t head_room, size_t tail_room) {
  if (unlikely(head_room > max_length || tail_room > max_length ||
               head_room + tail_room > max_length - slice_.length()))
    throw_max_length_exceeded();

  const size_t whole = head_room + slice_.length() + tail_room;
  if (whole == 0)
    silo_.clear();
  else if (is_reference() || slice_.empty()) {
    silo_.reserve(whole);
    silo_.append(head_room, '\0');
    silo_.append(slice_.char_ptr(), slice_.length());
  } else {
    std::string buffer;
    buffer.reserve(whole);
    buffer.append(head_room, '\0');
    buffer.append(slice_.char_ptr(), slice_.length());
    silo_.assign(std::move(buffer));
  }
  slice_.iov_base = const_cast<char *>(silo_.data());
}

void buffer::insulate() {
  assert(is_reference());
  silo_.assign(slice_.char_ptr(), slice_.length());
  slice_.iov_base = const_cast<char *>(silo_.data());
}

buffer &buffer::assign_reference(const void *ptr, size_t bytes) noexcept {
  silo_.clear();
  slice_.assign(ptr, bytes);
  return *this;
}

buffer &buffer::assign_freestanding(const void *ptr, size_t bytes) {
  silo_.assign(static_cast<const char *>(ptr), check_length(bytes));
  slice_.assign(silo_);
  return *this;
}

void buffer::clear() noexcept {
  slice_.reset();
  silo_.clear();
}

void buffer::shrink_to_fit() {
  if (silo_.capacity() != length()) {
    if (silo_.length() != length())
      silo_.assign(char_ptr(), length());
    silo_.shrink_to_fit();
    slice_.assign(silo_);
  }
}

void buffer::shrink() {
  if (silo_.length() != length()) {
    silo_.assign(char_ptr(), length());
    slice_.assign(silo_);
  }
}

int buffer::thunk::cb_copy(void *context, MDBX_val *target, const void *src,
                           size_t bytes) noexcept {
  thunk *self = static_cast<thunk *>(context);
  assert(self->is_clean());
  try {
    owner_of(static_cast<slice *>(target), &buffer::slice_)
        ->assign(src, bytes, false);
    return MDBX_RESULT_FALSE;
  } catch (... /* capture any exception to rethrow it over C code */) {
    self->capture();
    return MDBX_RESULT_TRUE;
  }
}

buffer::buffer(size_t head_room, size_t tail_room) {
  if (unlikely(head_room > max_length || tail_room > max_length ||
               head_room + tail_room > max_length))
    throw_max_length_exceeded();
  silo_.reserve(head_room + tail_room);
  silo_.append(head_room, '\0');
  slice_.iov_base = const_cast<char *>(silo_.data());
  assert(slice_.iov_len == 0);
}

buffer::buffer(size_t head_room, const slice &src, size_t tail_room) {
  if (unlikely(head_room > max_length || tail_room > max_length ||
               head_room + tail_room > max_length - slice_.length()))
    throw_max_length_exceeded();
  silo_.reserve(head_room + src.length() + tail_room);
  silo_.append(head_room, '\0');
  silo_.append(src.char_ptr(), src.length());
  slice_.iov_base = const_cast<char *>(silo_.data());
  slice_.iov_len = src.length();
}

buffer::buffer(size_t capacity) {
  silo_.reserve(check_length(capacity));
  slice_.iov_base = const_cast<char *>(silo_.data());
  assert(slice_.iov_len == 0);
}

buffer::buffer(const txn_ref &txn, const slice &src)
    : buffer(src, !txn.is_dirty(src.data())) {}

buffer buffer::decode_hex(const slice &hex) {
  if (hex.length() % 2)
    throw std::invalid_argument("odd length of hexadecimal string");
  buffer result(hex.length() / 2);
  NOT_IMPLEMENTED();
  return result;
}

buffer buffer::decode_base64(const slice &base64) {
  buffer result(base64.length() * 4 / 3);
  NOT_IMPLEMENTED();
  return result;
}

//------------------------------------------------------------------------------

size_t env_ref::default_pagesize() noexcept { return ::mdbx_syspagesize(); }

static inline MDBX_env_flags_t mode2flags(env_ref::mode mode) {
  switch (mode) {
  default:
    cxx20_attribute_unlikely throw std::invalid_argument("db::mode is invalid");
  case env_ref::mode::readonly:
    return MDBX_RDONLY;
  case env_ref::mode::write_file_io:
    return MDBX_ENV_DEFAULTS;
  case env_ref::mode::write_mapped_io:
    return MDBX_WRITEMAP;
  }
}

__cold MDBX_env_flags_t env_ref::operate_parameters::make_flags(
    bool accede, bool use_subdirectory) const {
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
      cxx20_attribute_unlikely throw std::invalid_argument(
          "db::durability is invalid");
    case env_ref::durability::robust_synchronous:
      break;
    case env_ref::durability::half_synchronous_weak_last:
      flags |= MDBX_NOMETASYNC;
      break;
    case env_ref::durability::lazy_weak_tail:
      flags |= (flags & MDBX_WRITEMAP) ? MDBX_MAPASYNC : MDBX_SAFE_NOSYNC;
      break;
    case env_ref::durability::whole_fragile:
      flags |= MDBX_UTTERLY_NOSYNC;
      break;
    }
  }
  return flags;
}

env_ref::mode
env_ref::operate_parameters::mode_from_flags(MDBX_env_flags_t) noexcept {
  NOT_IMPLEMENTED();
}

env_ref::durability
env_ref::operate_parameters::durability_from_flags(MDBX_env_flags_t) noexcept {
  NOT_IMPLEMENTED();
}

env_ref::reclaiming_options::reclaiming_options(MDBX_env_flags_t) noexcept {
  NOT_IMPLEMENTED();
}

env_ref::operate_options::operate_options(MDBX_env_flags_t) noexcept {
  NOT_IMPLEMENTED();
}

env_ref::operate_parameters::operate_parameters(const env_ref &) {
  NOT_IMPLEMENTED();
}

bool env_ref::is_pristine() const {
  return get_stat().ms_mod_txnid == 0 &&
         get_info().mi_recent_txnid == INITIAL_TXNID;
}

bool env_ref::is_empty() const { return get_stat().ms_branch_pages == 0; }

env_ref &env_ref::copy(const path &destination, bool compactify,
                       bool force_dynamic_size) {
  const path_to_pchar<path> utf8(destination);
  error::success_or_throw(
      ::mdbx_env_copy(handle_, utf8,
                      (compactify ? MDBX_CP_COMPACT : MDBX_CP_DEFAULTS) |
                          (force_dynamic_size ? MDBX_CP_FORCE_DYNAMIC_SIZE
                                              : MDBX_CP_DEFAULTS)));
  return *this;
}

env_ref &env_ref::copy(filehandle fd, bool compactify,
                       bool force_dynamic_size) {
  error::success_or_throw(
      ::mdbx_env_copy2fd(handle_, fd,
                         (compactify ? MDBX_CP_COMPACT : MDBX_CP_DEFAULTS) |
                             (force_dynamic_size ? MDBX_CP_FORCE_DYNAMIC_SIZE
                                                 : MDBX_CP_DEFAULTS)));
  return *this;
}

path env_ref::get_path() const {
  const char *c_str;
  error::success_or_throw(::mdbx_env_get_path(handle_, &c_str));
  return pchar_to_path<path>(c_str);
}

//------------------------------------------------------------------------------

static inline MDBX_env *create_env() {
  MDBX_env *ptr;
  error::success_or_throw(::mdbx_env_create(&ptr));
  assert(ptr != nullptr);
  return ptr;
}

env::~env() noexcept {
  if (handle_)
    error::success_or_panic(::mdbx_env_close(handle_), "mdbx::~env()",
                            "mdbx_env_close");
}

void env::close(bool dont_sync) {
  const error rc =
      static_cast<MDBX_error_t>(::mdbx_env_close_ex(handle_, dont_sync));
  switch (rc.code()) {
  case MDBX_EBADSIGN:
    handle_ = nullptr;
    __fallthrough /* fall through */;
  default:
    rc.throw_exception();
  case MDBX_SUCCESS:
    handle_ = nullptr;
  }
}

__cold void env::setup(unsigned max_maps, unsigned max_readers) {
  if (max_readers > 0)
    error::success_or_throw(::mdbx_env_set_maxreaders(handle_, max_readers));
  if (max_readers > 0)
    error::success_or_throw(::mdbx_env_set_maxdbs(handle_, max_maps));
}

__cold env::env(const path &pathname, const operate_parameters &op, bool accede)
    : env(create_env()) {
  setup(op.max_maps, op.max_readers);
  const path_to_pchar<path> utf8(pathname);
  error::success_or_throw(
      ::mdbx_env_open(handle_, utf8, op.make_flags(accede), 0));

  //  if (po.options.nested_write_transactions && (flags() & MDBX_WRITEMAP))
  //    error::throw_exception(MDBX_INCOMPATIBLE);
}

__cold env::env(const path &pathname, const env_ref::create_parameters &cp,
                const env_ref::operate_parameters &op, bool accede)
    : env(create_env()) {
  setup(op.max_maps, op.max_readers);
  const path_to_pchar<path> utf8(pathname);
  set_geometry(cp.geometry);
  error::success_or_throw(
      ::mdbx_env_open(handle_, utf8, op.make_flags(accede, cp.use_subdirectory),
                      cp.file_mode_bits));

  //  if (po.options.nested_write_transactions && (flags() & MDBX_WRITEMAP))
  //    error::throw_exception(MDBX_INCOMPATIBLE);
}

//------------------------------------------------------------------------------

txn txn_ref::start_nested() {
  MDBX_txn *nested;
  error::throw_on_nullptr(handle_, MDBX_BAD_TXN);
  error::success_or_throw(::mdbx_txn_begin(mdbx_txn_env(handle_), handle_,
                                           MDBX_TXN_READWRITE, &nested));
  assert(nested != nullptr);
  return txn(nested);
}

txn::~txn() noexcept {
  if (handle_)
    error::success_or_panic(::mdbx_txn_abort(handle_), "mdbx::~txn",
                            "mdbx_txn_abort");
}

void txn::abort() {
  const error err = static_cast<MDBX_error_t>(::mdbx_txn_abort(handle_));
  if (unlikely(err.code() != MDBX_SUCCESS)) {
    if (err.code() != MDBX_THREAD_MISMATCH)
      handle_ = nullptr;
    err.throw_exception();
  }
}

void txn::commit() {
  const error err = static_cast<MDBX_error_t>(::mdbx_txn_commit(handle_));
  if (unlikely(err.code() != MDBX_SUCCESS)) {
    if (err.code() != MDBX_THREAD_MISMATCH)
      handle_ = nullptr;
    err.throw_exception();
  }
}

//------------------------------------------------------------------------------

bool txn_ref::drop_map(const char *name, bool ignore_nonexists) {
  map_handle map;
  const int err = ::mdbx_dbi_open(handle_, name, MDBX_DB_ACCEDE, &map.dbi);
  switch (err) {
  case MDBX_SUCCESS:
    drop_map(map);
    return true;
  case MDBX_NOTFOUND:
  case MDBX_BAD_DBI:
    if (ignore_nonexists)
      return false;
    cxx17_attribute_fallthrough /* fallthrough */;
  default:
    cxx20_attribute_unlikely error::throw_exception(err);
  }
}

bool txn_ref::clear_map(const char *name, bool ignore_nonexists) {
  map_handle map;
  const int err = ::mdbx_dbi_open(handle_, name, MDBX_DB_ACCEDE, &map.dbi);
  switch (err) {
  case MDBX_SUCCESS:
    clear_map(map);
    return true;
  case MDBX_NOTFOUND:
  case MDBX_BAD_DBI:
    if (ignore_nonexists)
      return false;
    cxx17_attribute_fallthrough /* fallthrough */;
  default:
    cxx20_attribute_unlikely error::throw_exception(err);
  }
}

//------------------------------------------------------------------------------

void cursor::close() {
  if (mdbx_unlikely(!handle_))
    error::throw_exception(MDBX_EINVAL);
  ::mdbx_cursor_close(handle_);
  handle_ = nullptr;
}

cursor::~cursor() noexcept { ::mdbx_cursor_close(handle_); }

//------------------------------------------------------------------------------

__cold ::std::ostream &operator<<(::std::ostream &, const slice &) {
  NOT_IMPLEMENTED();
}

__cold ::std::ostream &operator<<(::std::ostream &, const pair &) {
  NOT_IMPLEMENTED();
}

__cold ::std::ostream &operator<<(::std::ostream &, const buffer &) {
  NOT_IMPLEMENTED();
}

__cold ::std::ostream &operator<<(::std::ostream &, const env_ref::geometry &) {
  NOT_IMPLEMENTED();
}

__cold ::std::ostream &operator<<(::std::ostream &,
                                  const env_ref::operate_parameters &) {
  NOT_IMPLEMENTED();
}

__cold ::std::ostream &operator<<(::std::ostream &, const env_ref::mode &) {
  NOT_IMPLEMENTED();
}

__cold ::std::ostream &operator<<(::std::ostream &,
                                  const env_ref::durability &) {
  NOT_IMPLEMENTED();
}

__cold ::std::ostream &operator<<(::std::ostream &,
                                  const env_ref::reclaiming_options &) {
  NOT_IMPLEMENTED();
}

__cold ::std::ostream &operator<<(::std::ostream &,
                                  const env_ref::operate_options &) {
  NOT_IMPLEMENTED();
}

__cold ::std::ostream &operator<<(::std::ostream &,
                                  const env_ref::create_parameters &) {
  NOT_IMPLEMENTED();
}

__cold ::std::ostream &operator<<(::std::ostream &, const MDBX_log_level_t &) {
  NOT_IMPLEMENTED();
}

__cold ::std::ostream &operator<<(::std::ostream &,
                                  const MDBX_debug_flags_t &) {
  NOT_IMPLEMENTED();
}

__cold ::std::ostream &operator<<(::std::ostream &, const MDBX_error_t &) {
  NOT_IMPLEMENTED();
}

__cold ::std::ostream &operator<<(::std::ostream &, const MDBX_env_flags_t &) {
  NOT_IMPLEMENTED();
}

__cold ::std::ostream &operator<<(::std::ostream &, const MDBX_txn_flags_t &) {
  NOT_IMPLEMENTED();
}

__cold ::std::ostream &operator<<(::std::ostream &, const MDBX_db_flags_t &) {
  NOT_IMPLEMENTED();
}

__cold ::std::ostream &operator<<(::std::ostream &, const MDBX_put_flags_t &) {
  NOT_IMPLEMENTED();
}

__cold ::std::ostream &operator<<(::std::ostream &, const MDBX_copy_flags_t &) {
  NOT_IMPLEMENTED();
}

__cold ::std::ostream &operator<<(::std::ostream &, const MDBX_cursor_op &) {
  NOT_IMPLEMENTED();
}

__cold ::std::ostream &operator<<(::std::ostream &, const MDBX_dbi_state_t &) {
  NOT_IMPLEMENTED();
}

} // namespace mdbx

//------------------------------------------------------------------------------

namespace std {

__cold string to_string(const ::mdbx::slice &value) {
  ostringstream out;
  out << value;
  return out.str();
}

__cold string to_string(const ::mdbx::pair &value) {
  ostringstream out;
  out << value;
  return out.str();
}

__cold string to_string(const ::mdbx::buffer &value) {
  ostringstream out;
  out << value;
  return out.str();
}

__cold string to_string(const ::mdbx::env_ref::geometry &value) {
  ostringstream out;
  out << value;
  return out.str();
}

__cold string to_string(const ::mdbx::env_ref::operate_parameters &value) {
  ostringstream out;
  out << value;
  return out.str();
}

__cold string to_string(const ::mdbx::env_ref::mode &value) {
  ostringstream out;
  out << value;
  return out.str();
}

__cold string to_string(const ::mdbx::env_ref::durability &value) {
  ostringstream out;
  out << value;
  return out.str();
}

__cold string to_string(const ::mdbx::env_ref::reclaiming_options &value) {
  ostringstream out;
  out << value;
  return out.str();
}

__cold string to_string(const ::mdbx::env_ref::operate_options &value) {
  ostringstream out;
  out << value;
  return out.str();
}

__cold string to_string(const ::mdbx::env_ref::create_parameters &value) {
  ostringstream out;
  out << value;
  return out.str();
}

__cold string to_string(const MDBX_log_level_t &value) {
  ostringstream out;
  out << value;
  return out.str();
}

__cold string to_string(const MDBX_debug_flags_t &value) {
  ostringstream out;
  out << value;
  return out.str();
}

__cold string to_string(const MDBX_error_t &value) {
  ostringstream out;
  out << value;
  return out.str();
}

__cold string to_string(const MDBX_env_flags_t &value) {
  ostringstream out;
  out << value;
  return out.str();
}

__cold string to_string(const MDBX_txn_flags_t &value) {
  ostringstream out;
  out << value;
  return out.str();
}

__cold string to_string(const MDBX_db_flags_t &value) {
  ostringstream out;
  out << value;
  return out.str();
}

__cold string to_string(const MDBX_put_flags_t &value) {
  ostringstream out;
  out << value;
  return out.str();
}

__cold string to_string(const MDBX_copy_flags_t &value) {
  ostringstream out;
  out << value;
  return out.str();
}

__cold string to_string(const MDBX_cursor_op &value) {
  ostringstream out;
  out << value;
  return out.str();
}

__cold string to_string(const MDBX_dbi_state_t &value) {
  ostringstream out;
  out << value;
  return out.str();
}

} // namespace std
