/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2024

#include "internals.h"

__cold void debug_log_va(int level, const char *function, int line,
                         const char *fmt, va_list args) {
  ENSURE(nullptr, osal_fastmutex_acquire(&globals.debug_lock) == 0);
  if (globals.logger.ptr) {
    if (globals.logger_buffer == nullptr)
      globals.logger.fmt(level, function, line, fmt, args);
    else {
      const int len = vsnprintf(globals.logger_buffer,
                                globals.logger_buffer_size, fmt, args);
      if (len > 0)
        globals.logger.nofmt(level, function, line, globals.logger_buffer, len);
    }
  } else {
#if defined(_WIN32) || defined(_WIN64)
    if (IsDebuggerPresent()) {
      int prefix_len = 0;
      char *prefix = nullptr;
      if (function && line > 0)
        prefix_len = osal_asprintf(&prefix, "%s:%d ", function, line);
      else if (function)
        prefix_len = osal_asprintf(&prefix, "%s: ", function);
      else if (line > 0)
        prefix_len = osal_asprintf(&prefix, "%d: ", line);
      if (prefix_len > 0 && prefix) {
        OutputDebugStringA(prefix);
        osal_free(prefix);
      }
      char *msg = nullptr;
      int msg_len = osal_vasprintf(&msg, fmt, args);
      if (msg_len > 0 && msg) {
        OutputDebugStringA(msg);
        osal_free(msg);
      }
    }
#else
    if (function && line > 0)
      fprintf(stderr, "%s:%d ", function, line);
    else if (function)
      fprintf(stderr, "%s: ", function);
    else if (line > 0)
      fprintf(stderr, "%d: ", line);
    vfprintf(stderr, fmt, args);
    fflush(stderr);
#endif
  }
  ENSURE(nullptr, osal_fastmutex_release(&globals.debug_lock) == 0);
}

__cold void debug_log(int level, const char *function, int line,
                      const char *fmt, ...) {
  va_list args;
  va_start(args, fmt);
  debug_log_va(level, function, line, fmt, args);
  va_end(args);
}

/* Dump a val in ascii or hexadecimal. */
__cold const char *mdbx_dump_val(const MDBX_val *val, char *const buf,
                                 const size_t bufsize) {
  if (!val)
    return "<null>";
  if (!val->iov_len)
    return "<empty>";
  if (!buf || bufsize < 4)
    return nullptr;

  if (!val->iov_base) {
    int len = snprintf(buf, bufsize, "<nullptr.%zu>", val->iov_len);
    assert(len > 0 && (size_t)len < bufsize);
    (void)len;
    return buf;
  }

  bool is_ascii = true;
  const uint8_t *const data = val->iov_base;
  for (size_t i = 0; i < val->iov_len; i++)
    if (data[i] < ' ' || data[i] > '~') {
      is_ascii = false;
      break;
    }

  if (is_ascii) {
    int len =
        snprintf(buf, bufsize, "%.*s",
                 (val->iov_len > INT_MAX) ? INT_MAX : (int)val->iov_len, data);
    assert(len > 0 && (size_t)len < bufsize);
    (void)len;
  } else {
    char *const detent = buf + bufsize - 2;
    char *ptr = buf;
    *ptr++ = '<';
    for (size_t i = 0; i < val->iov_len && ptr < detent; i++) {
      const char hex[16] = {'0', '1', '2', '3', '4', '5', '6', '7',
                            '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
      *ptr++ = hex[data[i] >> 4];
      *ptr++ = hex[data[i] & 15];
    }
    if (ptr < detent)
      *ptr++ = '>';
    *ptr = '\0';
  }
  return buf;
}

/*------------------------------------------------------------------------------
 LY: debug stuff */

__cold const char *pagetype_caption(const uint8_t type, char buf4unknown[16]) {
  switch (type) {
  case P_BRANCH:
    return "branch";
  case P_LEAF:
    return "leaf";
  case P_LEAF | P_SUBP:
    return "subleaf";
  case P_LEAF | P_DUPFIX:
    return "dupfix-leaf";
  case P_LEAF | P_DUPFIX | P_SUBP:
    return "dupfix-subleaf";
  case P_LEAF | P_DUPFIX | P_SUBP | P_LEGACY_DIRTY:
    return "dupfix-subleaf.legacy-dirty";
  case P_LARGE:
    return "large";
  default:
    snprintf(buf4unknown, 16, "unknown_0x%x", type);
    return buf4unknown;
  }
}

__cold static const char *leafnode_type(node_t *n) {
  static const char *const tp[2][2] = {{"", ": DB"},
                                       {": sub-page", ": sub-DB"}};
  return (node_flags(n) & N_BIG)
             ? ": large page"
             : tp[!!(node_flags(n) & N_DUP)][!!(node_flags(n) & N_TREE)];
}

/* Display all the keys in the page. */
__cold void page_list(page_t *mp) {
  pgno_t pgno = mp->pgno;
  const char *type;
  node_t *node;
  size_t i, nkeys, nsize, total = 0;
  MDBX_val key;
  DKBUF;

  switch (page_type(mp)) {
  case P_BRANCH:
    type = "Branch page";
    break;
  case P_LEAF:
    type = "Leaf page";
    break;
  case P_LEAF | P_SUBP:
    type = "Leaf sub-page";
    break;
  case P_LEAF | P_DUPFIX:
    type = "Leaf2 page";
    break;
  case P_LEAF | P_DUPFIX | P_SUBP:
    type = "Leaf2 sub-page";
    break;
  case P_LARGE:
    VERBOSE("Overflow page %" PRIaPGNO " pages %u\n", pgno, mp->pages);
    return;
  case P_META:
    VERBOSE("Meta-page %" PRIaPGNO " txnid %" PRIu64 "\n", pgno,
            unaligned_peek_u64(4, page_meta(mp)->txnid_a));
    return;
  default:
    VERBOSE("Bad page %" PRIaPGNO " flags 0x%X\n", pgno, mp->flags);
    return;
  }

  nkeys = page_numkeys(mp);
  VERBOSE("%s %" PRIaPGNO " numkeys %zu\n", type, pgno, nkeys);

  for (i = 0; i < nkeys; i++) {
    if (is_dupfix_leaf(
            mp)) { /* DUPFIX pages have no entries[] or node headers */
      key = page_dupfix_key(mp, i, nsize = mp->dupfix_ksize);
      total += nsize;
      VERBOSE("key %zu: nsize %zu, %s\n", i, nsize, DKEY(&key));
      continue;
    }
    node = page_node(mp, i);
    key.iov_len = node_ks(node);
    key.iov_base = node->payload;
    nsize = NODESIZE + key.iov_len;
    if (is_branch(mp)) {
      VERBOSE("key %zu: page %" PRIaPGNO ", %s\n", i, node_pgno(node),
              DKEY(&key));
      total += nsize;
    } else {
      if (node_flags(node) & N_BIG)
        nsize += sizeof(pgno_t);
      else
        nsize += node_ds(node);
      total += nsize;
      nsize += sizeof(indx_t);
      VERBOSE("key %zu: nsize %zu, %s%s\n", i, nsize, DKEY(&key),
              leafnode_type(node));
    }
    total = EVEN_CEIL(total);
  }
  VERBOSE("Total: header %u + contents %zu + unused %zu\n",
          is_dupfix_leaf(mp) ? PAGEHDRSZ : PAGEHDRSZ + mp->lower, total,
          page_room(mp));
}

__cold static int setup_debug(MDBX_log_level_t level, MDBX_debug_flags_t flags,
                              union logger_union logger, char *buffer,
                              size_t buffer_size) {
  ENSURE(nullptr, osal_fastmutex_acquire(&globals.debug_lock) == 0);

  const int rc = globals.runtime_flags | (globals.loglevel << 16);
  if (level != MDBX_LOG_DONTCHANGE)
    globals.loglevel = (uint8_t)level;

  if (flags != MDBX_DBG_DONTCHANGE) {
    flags &=
#if MDBX_DEBUG
        MDBX_DBG_ASSERT | MDBX_DBG_AUDIT | MDBX_DBG_JITTER |
#endif
        MDBX_DBG_DUMP | MDBX_DBG_LEGACY_MULTIOPEN | MDBX_DBG_LEGACY_OVERLAP |
        MDBX_DBG_DONT_UPGRADE;
    globals.runtime_flags = (uint8_t)flags;
  }

  assert(MDBX_LOGGER_DONTCHANGE == ((MDBX_debug_func *)(intptr_t)-1));
  if (logger.ptr != (void *)((intptr_t)-1)) {
    globals.logger.ptr = logger.ptr;
    globals.logger_buffer = buffer;
    globals.logger_buffer_size = buffer_size;
  }

  ENSURE(nullptr, osal_fastmutex_release(&globals.debug_lock) == 0);
  return rc;
}

__cold int mdbx_setup_debug_nofmt(MDBX_log_level_t level,
                                  MDBX_debug_flags_t flags,
                                  MDBX_debug_func_nofmt *logger, char *buffer,
                                  size_t buffer_size) {
  union logger_union thunk;
  thunk.nofmt =
      (logger && buffer && buffer_size) ? logger : MDBX_LOGGER_NOFMT_DONTCHANGE;
  return setup_debug(level, flags, thunk, buffer, buffer_size);
}

__cold int mdbx_setup_debug(MDBX_log_level_t level, MDBX_debug_flags_t flags,
                            MDBX_debug_func *logger) {
  union logger_union thunk;
  thunk.fmt = logger;
  return setup_debug(level, flags, thunk, nullptr, 0);
}
