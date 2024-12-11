/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2024

#include "internals.h"

__cold int mdbx_is_readahead_reasonable(size_t volume, intptr_t redundancy) {
  if (volume <= 1024 * 1024 * 4ul)
    return MDBX_RESULT_TRUE;

  intptr_t pagesize, total_ram_pages;
  int err = mdbx_get_sysraminfo(&pagesize, &total_ram_pages, nullptr);
  if (unlikely(err != MDBX_SUCCESS))
    return LOG_IFERR(err);

  const int log2page = log2n_powerof2(pagesize);
  const intptr_t volume_pages = (volume + pagesize - 1) >> log2page;
  const intptr_t redundancy_pages = (redundancy < 0) ? -(intptr_t)((-redundancy + pagesize - 1) >> log2page)
                                                     : (intptr_t)(redundancy + pagesize - 1) >> log2page;
  if (volume_pages >= total_ram_pages || volume_pages + redundancy_pages >= total_ram_pages)
    return MDBX_RESULT_FALSE;

  intptr_t avail_ram_pages;
  err = mdbx_get_sysraminfo(nullptr, nullptr, &avail_ram_pages);
  if (unlikely(err != MDBX_SUCCESS))
    return LOG_IFERR(err);

  return (volume_pages + redundancy_pages >= avail_ram_pages) ? MDBX_RESULT_FALSE : MDBX_RESULT_TRUE;
}

int mdbx_dbi_sequence(MDBX_txn *txn, MDBX_dbi dbi, uint64_t *result, uint64_t increment) {
  int rc = check_txn(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS)) {
  bailout:
    if (likely(result))
      *result = ~UINT64_C(0);
    return LOG_IFERR(rc);
  }

  rc = dbi_check(txn, dbi);
  if (unlikely(rc != MDBX_SUCCESS))
    goto bailout;

  if (unlikely(txn->dbi_state[dbi] & DBI_STALE)) {
    rc = tbl_fetch(txn, dbi);
    if (unlikely(rc != MDBX_SUCCESS))
      goto bailout;
  }

  tree_t *dbs = &txn->dbs[dbi];
  if (likely(result))
    *result = dbs->sequence;

  if (likely(increment > 0)) {
    if (unlikely(dbi == FREE_DBI || (txn->flags & MDBX_TXN_RDONLY) != 0))
      return MDBX_EACCESS;

    uint64_t new = dbs->sequence + increment;
    if (unlikely(new < increment))
      return MDBX_RESULT_TRUE;

    tASSERT(txn, new > dbs->sequence);
    if ((txn->dbi_state[dbi] & DBI_DIRTY) == 0) {
      txn->flags |= MDBX_TXN_DIRTY;
      txn->dbi_state[dbi] |= DBI_DIRTY;
      if (unlikely(dbi == MAIN_DBI) && txn->dbs[MAIN_DBI].root != P_INVALID) {
        /* LY: Временная подпорка для coherency_check(), которую в перспективе
         * следует заменить вместе с переделкой установки mod_txnid.
         *
         * Суть проблемы:
         *  - coherency_check() в качестве одного из критериев "когерентности"
         *    проверяет условие meta.maindb.mod_txnid == maindb.root->txnid;
         *  - при обновлении maindb.sequence высталяется DBI_DIRTY, что приведет
         *    к обновлению meta.maindb.mod_txnid = current_txnid;
         *  - однако, если в само дерево maindb обновление не вносились и оно
         *    не пустое, то корневая страницы останеться с прежним txnid и из-за
         *    этого ложно сработает coherency_check().
         *
         * Временное (текущее) решение: Принудительно обновляем корневую
         * страницу в описанной выше ситуации. Это устраняет проблему, но и
         * не создает рисков регресса.
         *
         * FIXME: Итоговое решение, которое предстоит реализовать:
         *  - изменить семантику установки/обновления mod_txnid, привязав его
         *    строго к изменению b-tree, но не атрибутов;
         *  - обновлять mod_txnid при фиксации вложенных транзакций;
         *  - для dbi-хендлов пользовательских table (видимо) можно оставить
         *    DBI_DIRTY в качестве признака необходимости обновления записи
         *    table в MainDB, при этом взводить DBI_DIRTY вместе с обновлением
         *    mod_txnid, в том числе при обновлении sequence.
         *  - для MAIN_DBI при обновлении sequence не следует взводить DBI_DIRTY
         *    и/или обновлять mod_txnid, а только взводить MDBX_TXN_DIRTY.
         *  - альтернативно, можно перераспределить флажки-признаки dbi_state,
         *    чтобы различать состояние dirty-tree и dirty-attributes. */
        cursor_couple_t cx;
        rc = cursor_init(&cx.outer, txn, MAIN_DBI);
        if (unlikely(rc != MDBX_SUCCESS))
          return LOG_IFERR(rc);
        rc = tree_search(&cx.outer, nullptr, Z_MODIFY | Z_ROOTONLY);
        if (unlikely(rc != MDBX_SUCCESS))
          return LOG_IFERR(rc);
      }
    }
    dbs->sequence = new;
  }

  return MDBX_SUCCESS;
}

int mdbx_cmp(const MDBX_txn *txn, MDBX_dbi dbi, const MDBX_val *a, const MDBX_val *b) {
  eASSERT(nullptr, txn->signature == txn_signature);
  tASSERT(txn, (dbi_state(txn, dbi) & DBI_VALID) && !dbi_changed(txn, dbi));
  tASSERT(txn, dbi < txn->env->n_dbi && (txn->env->dbs_flags[dbi] & DB_VALID) != 0);
  return txn->env->kvs[dbi].clc.k.cmp(a, b);
}

int mdbx_dcmp(const MDBX_txn *txn, MDBX_dbi dbi, const MDBX_val *a, const MDBX_val *b) {
  eASSERT(nullptr, txn->signature == txn_signature);
  tASSERT(txn, (dbi_state(txn, dbi) & DBI_VALID) && !dbi_changed(txn, dbi));
  tASSERT(txn, dbi < txn->env->n_dbi && (txn->env->dbs_flags[dbi] & DB_VALID));
  return txn->env->kvs[dbi].clc.v.cmp(a, b);
}

__cold MDBX_cmp_func *mdbx_get_keycmp(MDBX_db_flags_t flags) { return builtin_keycmp(flags); }

__cold MDBX_cmp_func *mdbx_get_datacmp(MDBX_db_flags_t flags) { return builtin_datacmp(flags); }

/*----------------------------------------------------------------------------*/

__cold const char *mdbx_liberr2str(int errnum) {
  /* Table of descriptions for MDBX errors */
  static const char *const tbl[] = {
      "MDBX_KEYEXIST: Key/data pair already exists",
      "MDBX_NOTFOUND: No matching key/data pair found",
      "MDBX_PAGE_NOTFOUND: Requested page not found",
      "MDBX_CORRUPTED: Database is corrupted",
      "MDBX_PANIC: Environment had fatal error",
      "MDBX_VERSION_MISMATCH: DB version mismatch libmdbx",
      "MDBX_INVALID: File is not an MDBX file",
      "MDBX_MAP_FULL: Environment mapsize limit reached",
      "MDBX_DBS_FULL: Too many DBI-handles (maxdbs reached)",
      "MDBX_READERS_FULL: Too many readers (maxreaders reached)",
      nullptr /* MDBX_TLS_FULL (-30789): unused in MDBX */,
      "MDBX_TXN_FULL: Transaction has too many dirty pages,"
      " i.e transaction is too big",
      "MDBX_CURSOR_FULL: Cursor stack limit reachedn - this usually indicates"
      " corruption, i.e branch-pages loop",
      "MDBX_PAGE_FULL: Internal error - Page has no more space",
      "MDBX_UNABLE_EXTEND_MAPSIZE: Database engine was unable to extend"
      " mapping, e.g. since address space is unavailable or busy,"
      " or Operation system not supported such operations",
      "MDBX_INCOMPATIBLE: Environment or database is not compatible"
      " with the requested operation or the specified flags",
      "MDBX_BAD_RSLOT: Invalid reuse of reader locktable slot,"
      " e.g. read-transaction already run for current thread",
      "MDBX_BAD_TXN: Transaction is not valid for requested operation,"
      " e.g. had errored and be must aborted, has a child, or is invalid",
      "MDBX_BAD_VALSIZE: Invalid size or alignment of key or data"
      " for target database, either invalid table name",
      "MDBX_BAD_DBI: The specified DBI-handle is invalid"
      " or changed by another thread/transaction",
      "MDBX_PROBLEM: Unexpected internal error, transaction should be aborted",
      "MDBX_BUSY: Another write transaction is running,"
      " or environment is already used while opening with MDBX_EXCLUSIVE flag",
  };

  if (errnum >= MDBX_KEYEXIST && errnum <= MDBX_BUSY) {
    int i = errnum - MDBX_KEYEXIST;
    return tbl[i];
  }

  switch (errnum) {
  case MDBX_SUCCESS:
    return "MDBX_SUCCESS: Successful";
  case MDBX_EMULTIVAL:
    return "MDBX_EMULTIVAL: The specified key has"
           " more than one associated value";
  case MDBX_EBADSIGN:
    return "MDBX_EBADSIGN: Wrong signature of a runtime object(s),"
           " e.g. memory corruption or double-free";
  case MDBX_WANNA_RECOVERY:
    return "MDBX_WANNA_RECOVERY: Database should be recovered,"
           " but this could NOT be done automatically for now"
           " since it opened in read-only mode";
  case MDBX_EKEYMISMATCH:
    return "MDBX_EKEYMISMATCH: The given key value is mismatched to the"
           " current cursor position";
  case MDBX_TOO_LARGE:
    return "MDBX_TOO_LARGE: Database is too large for current system,"
           " e.g. could NOT be mapped into RAM";
  case MDBX_THREAD_MISMATCH:
    return "MDBX_THREAD_MISMATCH: A thread has attempted to use a not"
           " owned object, e.g. a transaction that started by another thread";
  case MDBX_TXN_OVERLAPPING:
    return "MDBX_TXN_OVERLAPPING: Overlapping read and write transactions for"
           " the current thread";
  case MDBX_DUPLICATED_CLK:
    return "MDBX_DUPLICATED_CLK: Alternative/Duplicate LCK-file is exists,"
           " please keep one and remove unused other";
  case MDBX_DANGLING_DBI:
    return "MDBX_DANGLING_DBI: Some cursors and/or other resources should be"
           " closed before table or corresponding DBI-handle could be (re)used";
  case MDBX_OUSTED:
    return "MDBX_OUSTED: The parked read transaction was outed for the sake"
           " of recycling old MVCC snapshots";
  case MDBX_MVCC_RETARDED:
    return "MDBX_MVCC_RETARDED: MVCC snapshot used by read transaction"
           " is outdated and could not be copied"
           " since corresponding meta-pages was overwritten";
  default:
    return nullptr;
  }
}

__cold const char *mdbx_strerror_r(int errnum, char *buf, size_t buflen) {
  const char *msg = mdbx_liberr2str(errnum);
  if (!msg && buflen > 0 && buflen < INT_MAX) {
#if defined(_WIN32) || defined(_WIN64)
    DWORD size = FormatMessageA(FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS, nullptr, errnum,
                                MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), buf, (DWORD)buflen, nullptr);
    while (size && buf[size - 1] <= ' ')
      --size;
    buf[size] = 0;
    return size ? buf : "FormatMessageA(FORMAT_MESSAGE_FROM_SYSTEM) failed";
#elif defined(_GNU_SOURCE) && defined(__GLIBC__)
    /* GNU-specific */
    if (errnum > 0)
      msg = strerror_r(errnum, buf, buflen);
#elif (_POSIX_C_SOURCE >= 200112L || _XOPEN_SOURCE >= 600)
    /* XSI-compliant */
    if (errnum > 0 && strerror_r(errnum, buf, buflen) == 0)
      msg = buf;
#else
    if (errnum > 0) {
      msg = strerror(errnum);
      if (msg) {
        strncpy(buf, msg, buflen);
        msg = buf;
      }
    }
#endif
    if (!msg) {
      (void)snprintf(buf, buflen, "error %d", errnum);
      msg = buf;
    }
    buf[buflen - 1] = '\0';
  }
  return msg;
}

__cold const char *mdbx_strerror(int errnum) {
#if defined(_WIN32) || defined(_WIN64)
  static char buf[1024];
  return mdbx_strerror_r(errnum, buf, sizeof(buf));
#else
  const char *msg = mdbx_liberr2str(errnum);
  if (!msg) {
    if (errnum > 0)
      msg = strerror(errnum);
    if (!msg) {
      static char buf[32];
      (void)snprintf(buf, sizeof(buf) - 1, "error %d", errnum);
      msg = buf;
    }
  }
  return msg;
#endif
}

#if defined(_WIN32) || defined(_WIN64) /* Bit of madness for Windows */
const char *mdbx_strerror_r_ANSI2OEM(int errnum, char *buf, size_t buflen) {
  const char *msg = mdbx_liberr2str(errnum);
  if (!msg && buflen > 0 && buflen < INT_MAX) {
    DWORD size = FormatMessageA(FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS, nullptr, errnum,
                                MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), buf, (DWORD)buflen, nullptr);
    while (size && buf[size - 1] <= ' ')
      --size;
    buf[size] = 0;
    if (!size)
      msg = "FormatMessageA(FORMAT_MESSAGE_FROM_SYSTEM) failed";
    else if (!CharToOemBuffA(buf, buf, size))
      msg = "CharToOemBuffA() failed";
    else
      msg = buf;
  }
  return msg;
}

const char *mdbx_strerror_ANSI2OEM(int errnum) {
  static char buf[1024];
  return mdbx_strerror_r_ANSI2OEM(errnum, buf, sizeof(buf));
}
#endif /* Bit of madness for Windows */
