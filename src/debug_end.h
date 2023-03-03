#if defined(__GNUC__) && !defined(__LCC__)

#pragma pop_macro("TRACE")
#pragma pop_macro("DEBUG")
#pragma pop_macro("VERBOSE")
#pragma pop_macro("NOTICE")
#pragma pop_macro("WARNING")
#pragma pop_macro("ERROR")
#pragma pop_macro("eASSERT")

#if !defined(__clang__)
#pragma GCC reset_options
#endif

#endif /* GCC only */
