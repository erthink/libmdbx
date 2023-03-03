#if defined(__GNUC__) && !defined(__LCC__)

#pragma push_macro("TRACE")
#pragma push_macro("DEBUG")
#pragma push_macro("VERBOSE")
#pragma push_macro("NOTICE")
#pragma push_macro("WARNING")
#pragma push_macro("ERROR")
#pragma push_macro("eASSERT")

#undef TRACE
#define TRACE(fmt, ...)                                                        \
  debug_log(MDBX_LOG_TRACE, __func__, __LINE__, fmt "\n", __VA_ARGS__)

#undef DEBUG
#define DEBUG(fmt, ...)                                                        \
  debug_log(MDBX_LOG_DEBUG, __func__, __LINE__, fmt "\n", __VA_ARGS__)

#undef VERBOSE
#define VERBOSE(fmt, ...)                                                      \
  debug_log(MDBX_LOG_VERBOSE, __func__, __LINE__, fmt "\n", __VA_ARGS__)

#undef NOTICE
#define NOTICE(fmt, ...)                                                       \
  debug_log(MDBX_LOG_NOTICE, __func__, __LINE__, fmt "\n", __VA_ARGS__)

#undef WARNING
#define WARNING(fmt, ...)                                                      \
  debug_log(MDBX_LOG_WARN, __func__, __LINE__, fmt "\n", __VA_ARGS__)

#undef ERROR
#define ERROR(fmt, ...)                                                        \
  debug_log(MDBX_LOG_ERROR, __func__, __LINE__, fmt "\n", __VA_ARGS__)

#undef eASSERT
#define eASSERT(env, expr) ENSURE(env, expr)

#if !defined(__clang__)
#pragma GCC optimize("-Og")
#endif

#endif /* GCC only */
