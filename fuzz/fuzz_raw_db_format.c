#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <limits.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>

#include <fuzz.h>

static const struct dbi_mode_desc raw_db_modes[] = {
    { "mode0_default",  NULL,       MDBX_DB_DEFAULTS },
    { "mode1_named",    "named",    MDBX_DB_DEFAULTS },
    { "mode2_rev",      "rev",      MDBX_REVERSEKEY },
    { "mode3_dup",      "dup",      MDBX_DUPSORT },
    { "mode4_dupfixed", "dupfixed", MDBX_DUPSORT | MDBX_DUPFIXED },
    { "mode5_intkey",   "intkey",   MDBX_INTEGERKEY },
    { "mode6_intdup",   "intdup",   MDBX_DUPSORT | MDBX_DUPFIXED | MDBX_INTEGERDUP },
    { "mode7_revdup",   "revdup",   MDBX_DUPSORT | MDBX_REVERSEDUP },
};

static void cleanup_dir(const char *dir, const char *db_path, const char *lock_path)
{
    if (db_path)
			unlink(db_path);
    if (lock_path)
			unlink(lock_path);
    if (dir)
			rmdir(dir);
}

static void   try_dbi(MDBX_txn *txn, const char *name, MDBX_db_flags_t flags)
{
	MDBX_dbi    dbi;
	MDBX_val    key, data;
	MDBX_cursor *cursor = NULL;

	if (mdbx_dbi_open(txn, name, flags, &dbi) == MDBX_SUCCESS)
	{
		if (mdbx_cursor_open(txn, dbi, &cursor) == MDBX_SUCCESS)
		{
      (void)mdbx_cursor_get(cursor, &key, &data, MDBX_FIRST);
      for (int i = 0; i < 8; i++)
        (void)mdbx_cursor_get(cursor, &key, &data, MDBX_NEXT);
      (void)mdbx_cursor_get(cursor, &key, &data, MDBX_LAST);
  		mdbx_cursor_close(cursor);
		}
	}
}

static void       do_fuzz_open(const char *db_path, const uint8_t dbi_mode)
{
  MDBX_env        *env = NULL;
	MDBX_txn        *txn = NULL;
  MDBX_db_flags_t dbi_flags;

	if (mdbx_env_create(&env) == MDBX_SUCCESS)
	{
	  mdbx_env_set_maxdbs(env, 4);
	  if (mdbx_env_open(env, db_path, MDBX_NOSUBDIR, 0664) == MDBX_SUCCESS)
	  {
			if (mdbx_txn_begin(env, NULL, MDBX_TXN_RDONLY, &txn) == MDBX_SUCCESS)
			{
        dbi_flags = raw_db_modes[dbi_mode % 7].flags;
        const char *dbi_name = raw_db_modes[dbi_mode % 7].dbi_name;
        try_dbi(txn, dbi_name, dbi_flags);
	    	mdbx_txn_abort(txn);
			}
	  }
	  mdbx_env_close(env);
	}
}

int         LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
	char      workdir[] = "/tmp/libmdbx-seed-XXXXXX";
  int       fd = -1;
	char      db_path[PATH_MAX] = {};
	char      lock_path[PATH_MAX] = {};
  uint8_t   dbi_mode;
  
  if (size < 1)
    return (0);
#ifdef MDBX_DEBUG  
  mdbx_setup_debug(
      MDBX_LOG_TRACE, MDBX_DBG_DUMP | MDBX_DBG_ASSERT | MDBX_DBG_AUDIT
    | MDBX_DBG_LEGACY_OVERLAP | MDBX_DBG_DONT_UPGRADE, logger);
#endif
	if (mkdtemp(workdir))
	{
		if (snprintf(db_path, sizeof(db_path), "%s/mdbx.dat", workdir) < (int)sizeof(db_path))
		{
			if (snprintf(lock_path, sizeof(lock_path), "%s/mdbx.lck", workdir) < (int)sizeof(lock_path))
			{
  			if ((fd = open(db_path, O_CREAT | O_TRUNC | O_WRONLY, 0644)) > -1)
				{
          dbi_mode = data[0];
					write(fd, data + 1, size - 1);
					close(fd);
          do_fuzz_open(db_path, dbi_mode);
				}
			}
		}
		cleanup_dir(workdir, db_path, lock_path);
  }

  return (0);
}
