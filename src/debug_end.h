#if defined(__GNUC__) && !defined(__LCC__)

#pragma pop_macro("mdbx_trace")
#pragma pop_macro("mdbx_debug")
#pragma pop_macro("mdbx_verbose")
#pragma pop_macro("mdbx_notice")
#pragma pop_macro("mdbx_warning")
#pragma pop_macro("mdbx_error")
#pragma pop_macro("mdbx_assert")

#if !defined(__clang__)
#pragma GCC reset_options
#endif

#endif /* GCC only */
