#ifndef _LOGGER_H
# define _LOGGER_H

#include <mdbx.h>

struct              dbi_mode_desc
{
    const char      *seed_name;
    const char      *dbi_name;
    MDBX_db_flags_t flags;
};

void logger(MDBX_log_level_t level, const char *function, int line, const char *fmt, va_list args);

#endif
