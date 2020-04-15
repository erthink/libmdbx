#pragma push_macro("mdbx_trace")
#pragma push_macro("mdbx_debug")
#pragma push_macro("mdbx_verbose")
#pragma push_macro("mdbx_notice")
#pragma push_macro("mdbx_warning")
#pragma push_macro("mdbx_error")
#pragma push_macro("mdbx_assert")

#undef mdbx_trace
#define mdbx_trace(fmt, ...)                                                   \
  mdbx_debug_log(MDBX_LOG_TRACE, __func__, __LINE__, fmt "\n", __VA_ARGS__)

#undef mdbx_debug
#define mdbx_debug(fmt, ...)                                                   \
  mdbx_debug_log(MDBX_LOG_DEBUG, __func__, __LINE__, fmt "\n", __VA_ARGS__)

#undef mdbx_verbose
#define mdbx_verbose(fmt, ...)                                                 \
  mdbx_debug_log(MDBX_LOG_VERBOSE, __func__, __LINE__, fmt "\n", __VA_ARGS__)

#undef mdbx_notice
#define mdbx_notice(fmt, ...)                                                  \
  mdbx_debug_log(MDBX_LOG_NOTICE, __func__, __LINE__, fmt "\n", __VA_ARGS__)

#undef mdbx_warning
#define mdbx_warning(fmt, ...)                                                 \
  mdbx_debug_log(MDBX_LOG_WARN, __func__, __LINE__, fmt "\n", __VA_ARGS__)

#undef mdbx_error
#define mdbx_error(fmt, ...)                                                   \
  mdbx_debug_log(MDBX_LOG_ERROR, __func__, __LINE__, fmt "\n", __VA_ARGS__)

#undef mdbx_assert
#define mdbx_assert(env, expr) mdbx_ensure(env, expr)
