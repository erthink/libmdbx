/// \copyright SPDX-License-Identifier: Apache-2.0
/// \note Please refer to the COPYRIGHT file for explanations license change,
/// credits and acknowledgments.
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2024

#include "internals.h"

typedef struct compacting_context {
  MDBX_env *env;
  MDBX_txn *txn;
  MDBX_copy_flags_t flags;
  pgno_t first_unallocated;
  osal_condpair_t condpair;
  volatile unsigned head;
  volatile unsigned tail;
  uint8_t *write_buf[2];
  size_t write_len[2];
  /* Error code.  Never cleared if set.  Both threads can set nonzero
   * to fail the copy.  Not mutex-protected, expects atomic int. */
  volatile int error;
  mdbx_filehandle_t fd;
} ctx_t;

__cold static int compacting_walk_tree(ctx_t *ctx, tree_t *tree);

/* Dedicated writer thread for compacting copy. */
__cold static THREAD_RESULT THREAD_CALL compacting_write_thread(void *arg) {
  ctx_t *const ctx = arg;

#if defined(EPIPE) && !(defined(_WIN32) || defined(_WIN64))
  sigset_t sigset;
  sigemptyset(&sigset);
  sigaddset(&sigset, SIGPIPE);
  ctx->error = pthread_sigmask(SIG_BLOCK, &sigset, nullptr);
#endif /* EPIPE */

  osal_condpair_lock(&ctx->condpair);
  while (!ctx->error) {
    while (ctx->tail == ctx->head && !ctx->error) {
      int err = osal_condpair_wait(&ctx->condpair, true);
      if (err != MDBX_SUCCESS) {
        ctx->error = err;
        goto bailout;
      }
    }
    const unsigned toggle = ctx->tail & 1;
    size_t wsize = ctx->write_len[toggle];
    if (wsize == 0) {
      ctx->tail += 1;
      break /* EOF */;
    }
    ctx->write_len[toggle] = 0;
    uint8_t *ptr = ctx->write_buf[toggle];
    if (!ctx->error) {
      int err = osal_write(ctx->fd, ptr, wsize);
      if (err != MDBX_SUCCESS) {
#if defined(EPIPE) && !(defined(_WIN32) || defined(_WIN64))
        if (err == EPIPE) {
          /* Collect the pending SIGPIPE,
           * otherwise at least OS X gives it to the process on thread-exit. */
          int unused;
          sigwait(&sigset, &unused);
        }
#endif /* EPIPE */
        ctx->error = err;
        goto bailout;
      }
    }
    ctx->tail += 1;
    osal_condpair_signal(&ctx->condpair, false);
  }
bailout:
  osal_condpair_unlock(&ctx->condpair);
  return (THREAD_RESULT)0;
}

/* Give buffer and/or MDBX_EOF to writer thread, await unused buffer. */
__cold static int compacting_toggle_write_buffers(ctx_t *ctx) {
  osal_condpair_lock(&ctx->condpair);
  eASSERT(ctx->env, ctx->head - ctx->tail < 2 || ctx->error);
  ctx->head += 1;
  osal_condpair_signal(&ctx->condpair, true);
  while (!ctx->error && ctx->head - ctx->tail == 2 /* both buffers in use */) {
    if (ctx->flags & MDBX_CP_THROTTLE_MVCC)
      mdbx_txn_park(ctx->txn, false);
    int err = osal_condpair_wait(&ctx->condpair, false);
    if (err == MDBX_SUCCESS && (ctx->flags & MDBX_CP_THROTTLE_MVCC) != 0)
      err = mdbx_txn_unpark(ctx->txn, false);
    if (err != MDBX_SUCCESS)
      ctx->error = err;
  }
  osal_condpair_unlock(&ctx->condpair);
  return ctx->error;
}

static int compacting_put_bytes(ctx_t *ctx, const void *src, size_t bytes,
                                pgno_t pgno, pgno_t npages) {
  assert(pgno == 0 || bytes > PAGEHDRSZ);
  while (bytes > 0) {
    const size_t side = ctx->head & 1;
    const size_t left = MDBX_ENVCOPY_WRITEBUF - ctx->write_len[side];
    if (left < (pgno ? PAGEHDRSZ : 1)) {
      int err = compacting_toggle_write_buffers(ctx);
      if (unlikely(err != MDBX_SUCCESS))
        return err;
      continue;
    }
    const size_t chunk = (bytes < left) ? bytes : left;
    void *const dst = ctx->write_buf[side] + ctx->write_len[side];
    if (src) {
      memcpy(dst, src, chunk);
      if (pgno) {
        assert(chunk > PAGEHDRSZ);
        page_t *mp = dst;
        mp->pgno = pgno;
        if (mp->txnid == 0)
          mp->txnid = ctx->txn->txnid;
        if (mp->flags == P_LARGE) {
          assert(bytes <= pgno2bytes(ctx->env, npages));
          mp->pages = npages;
        }
        pgno = 0;
      }
      src = ptr_disp(src, chunk);
    } else
      memset(dst, 0, chunk);
    bytes -= chunk;
    ctx->write_len[side] += chunk;
  }
  return MDBX_SUCCESS;
}

static int compacting_put_page(ctx_t *ctx, const page_t *mp,
                               const size_t head_bytes, const size_t tail_bytes,
                               const pgno_t npages) {
  if (tail_bytes) {
    assert(head_bytes + tail_bytes <= ctx->env->ps);
    assert(npages == 1 &&
           (page_type(mp) == P_BRANCH || page_type(mp) == P_LEAF));
  } else {
    assert(head_bytes <= pgno2bytes(ctx->env, npages));
    assert((npages == 1 && page_type(mp) == (P_LEAF | P_DUPFIX)) ||
           page_type(mp) == P_LARGE);
  }

  const pgno_t pgno = ctx->first_unallocated;
  ctx->first_unallocated += npages;
  int err = compacting_put_bytes(ctx, mp, head_bytes, pgno, npages);
  if (unlikely(err != MDBX_SUCCESS))
    return err;
  err = compacting_put_bytes(
      ctx, nullptr, pgno2bytes(ctx->env, npages) - (head_bytes + tail_bytes), 0,
      0);
  if (unlikely(err != MDBX_SUCCESS))
    return err;
  return compacting_put_bytes(ctx, ptr_disp(mp, ctx->env->ps - tail_bytes),
                              tail_bytes, 0, 0);
}

__cold static int compacting_walk(ctx_t *ctx, MDBX_cursor *mc,
                                  pgno_t *const parent_pgno,
                                  txnid_t parent_txnid) {
  mc->top = 0;
  mc->ki[0] = 0;
  int rc = page_get(mc, *parent_pgno, &mc->pg[0], parent_txnid);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  rc = tree_search_finalize(mc, nullptr, Z_FIRST);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  /* Make cursor pages writable */
  const intptr_t deep_limit = mc->top + 1;
  void *const buf = osal_malloc(pgno2bytes(ctx->env, deep_limit + 1));
  if (buf == nullptr)
    return MDBX_ENOMEM;

  void *ptr = buf;
  for (intptr_t i = 0; i <= mc->top; i++) {
    page_copy(ptr, mc->pg[i], ctx->env->ps);
    mc->pg[i] = ptr;
    ptr = ptr_disp(ptr, ctx->env->ps);
  }
  /* This is writable space for a leaf page. Usually not needed. */
  page_t *const leaf = ptr;

  while (mc->top >= 0) {
    page_t *mp = mc->pg[mc->top];
    const size_t nkeys = page_numkeys(mp);
    if (is_leaf(mp)) {
      if (!(mc->flags & z_inner) /* may have nested N_TREE or N_BIG nodes */) {
        for (size_t i = 0; i < nkeys; i++) {
          node_t *node = page_node(mp, i);
          if (node_flags(node) == N_BIG) {
            /* Need writable leaf */
            if (mp != leaf) {
              mc->pg[mc->top] = leaf;
              page_copy(leaf, mp, ctx->env->ps);
              mp = leaf;
              node = page_node(mp, i);
            }

            const pgr_t lp =
                page_get_large(mc, node_largedata_pgno(node), mp->txnid);
            if (unlikely((rc = lp.err) != MDBX_SUCCESS))
              goto bailout;
            const size_t datasize = node_ds(node);
            const pgno_t npages = largechunk_npages(ctx->env, datasize);
            poke_pgno(node_data(node), ctx->first_unallocated);
            rc = compacting_put_page(ctx, lp.page, PAGEHDRSZ + datasize, 0,
                                     npages);
            if (unlikely(rc != MDBX_SUCCESS))
              goto bailout;
          } else if (node_flags(node) & N_TREE) {
            if (!MDBX_DISABLE_VALIDATION &&
                unlikely(node_ds(node) != sizeof(tree_t))) {
              ERROR("%s/%d: %s %u", "MDBX_CORRUPTED", MDBX_CORRUPTED,
                    "invalid dupsort sub-tree node size",
                    (unsigned)node_ds(node));
              rc = MDBX_CORRUPTED;
              goto bailout;
            }

            /* Need writable leaf */
            if (mp != leaf) {
              mc->pg[mc->top] = leaf;
              page_copy(leaf, mp, ctx->env->ps);
              mp = leaf;
              node = page_node(mp, i);
            }

            tree_t *nested = nullptr;
            if (node_flags(node) & N_DUP) {
              rc = cursor_dupsort_setup(mc, node, mp);
              if (likely(rc == MDBX_SUCCESS)) {
                nested = &mc->subcur->nested_tree;
                rc = compacting_walk(ctx, &mc->subcur->cursor, &nested->root,
                                     mp->txnid);
              }
            } else {
              cASSERT(mc, (mc->flags & z_inner) == 0 && mc->subcur == 0);
              cursor_couple_t *couple =
                  container_of(mc, cursor_couple_t, outer);
              nested = &couple->inner.nested_tree;
              memcpy(nested, node_data(node), sizeof(tree_t));
              rc = compacting_walk_tree(ctx, nested);
            }
            if (unlikely(rc != MDBX_SUCCESS))
              goto bailout;
            memcpy(node_data(node), nested, sizeof(tree_t));
          }
        }
      }
    } else {
      mc->ki[mc->top]++;
      if (mc->ki[mc->top] < nkeys) {
        for (;;) {
          const node_t *node = page_node(mp, mc->ki[mc->top]);
          rc = page_get(mc, node_pgno(node), &mp, mp->txnid);
          if (unlikely(rc != MDBX_SUCCESS))
            goto bailout;
          mc->top += 1;
          if (unlikely(mc->top >= deep_limit)) {
            rc = MDBX_CURSOR_FULL;
            goto bailout;
          }
          mc->ki[mc->top] = 0;
          if (!is_branch(mp)) {
            mc->pg[mc->top] = mp;
            break;
          }
          /* Whenever we advance to a sibling branch page,
           * we must proceed all the way down to its first leaf. */
          page_copy(mc->pg[mc->top], mp, ctx->env->ps);
        }
        continue;
      }
    }

    const pgno_t pgno = ctx->first_unallocated;
    if (likely(!is_dupfix_leaf(mp))) {
      rc = compacting_put_page(ctx, mp, PAGEHDRSZ + mp->lower,
                               ctx->env->ps - (PAGEHDRSZ + mp->upper), 1);
    } else {
      rc = compacting_put_page(
          ctx, mp, PAGEHDRSZ + page_numkeys(mp) * mp->dupfix_ksize, 0, 1);
    }
    if (unlikely(rc != MDBX_SUCCESS))
      goto bailout;

    if (mc->top) {
      /* Update parent if there is one */
      node_set_pgno(page_node(mc->pg[mc->top - 1], mc->ki[mc->top - 1]), pgno);
      cursor_pop(mc);
    } else {
      /* Otherwise we're done */
      *parent_pgno = pgno;
      break;
    }
  }

bailout:
  osal_free(buf);
  return rc;
}

__cold static int compacting_walk_tree(ctx_t *ctx, tree_t *tree) {
  if (unlikely(tree->root == P_INVALID))
    return MDBX_SUCCESS; /* empty db */

  cursor_couple_t couple;
  memset(&couple, 0, sizeof(couple));
  couple.inner.cursor.signature = ~cur_signature_live;
  kvx_t kvx = {.clc = {.k = {.lmin = INT_MAX}, .v = {.lmin = INT_MAX}}};
  int rc = cursor_init4walk(&couple, ctx->txn, tree, &kvx);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  couple.outer.checking |= z_ignord | z_pagecheck;
  couple.inner.cursor.checking |= z_ignord | z_pagecheck;
  if (!tree->mod_txnid)
    tree->mod_txnid = ctx->txn->txnid;
  return compacting_walk(ctx, &couple.outer, &tree->root, tree->mod_txnid);
}

__cold static void compacting_fixup_meta(MDBX_env *env, meta_t *meta) {
  eASSERT(env, meta->trees.gc.mod_txnid || meta->trees.gc.root == P_INVALID);
  eASSERT(env,
          meta->trees.main.mod_txnid || meta->trees.main.root == P_INVALID);

  /* Calculate filesize taking in account shrink/growing thresholds */
  if (meta->geometry.first_unallocated != meta->geometry.now) {
    meta->geometry.now = meta->geometry.first_unallocated;
    const size_t aligner =
        pv2pages(meta->geometry.grow_pv ? meta->geometry.grow_pv
                                        : meta->geometry.shrink_pv);
    if (aligner) {
      const pgno_t aligned = pgno_align2os_pgno(
          env, meta->geometry.first_unallocated + aligner -
                   meta->geometry.first_unallocated % aligner);
      meta->geometry.now = aligned;
    }
  }

  if (meta->geometry.now < meta->geometry.lower)
    meta->geometry.now = meta->geometry.lower;
  if (meta->geometry.now > meta->geometry.upper)
    meta->geometry.now = meta->geometry.upper;

  /* Update signature */
  assert(meta->geometry.now >= meta->geometry.first_unallocated);
  meta_sign_as_steady(meta);
}

/* Make resizable */
__cold static void meta_make_sizeable(meta_t *meta) {
  meta->geometry.lower = MIN_PAGENO;
  if (meta->geometry.grow_pv == 0) {
    const pgno_t step = 1 + (meta->geometry.upper - meta->geometry.lower) / 42;
    meta->geometry.grow_pv = pages2pv(step);
  }
  if (meta->geometry.shrink_pv == 0) {
    const pgno_t step = pv2pages(meta->geometry.grow_pv) << 1;
    meta->geometry.shrink_pv = pages2pv(step);
  }
}

__cold static int copy_with_compacting(MDBX_env *env, MDBX_txn *txn,
                                       mdbx_filehandle_t fd, uint8_t *buffer,
                                       const bool dest_is_pipe,
                                       const MDBX_copy_flags_t flags) {
  const size_t meta_bytes = pgno2bytes(env, NUM_METAS);
  uint8_t *const data_buffer =
      buffer + ceil_powerof2(meta_bytes, globals.sys_pagesize);
  meta_t *const meta = meta_init_triplet(env, buffer);
  meta_set_txnid(env, meta, txn->txnid);

  if (flags & MDBX_CP_FORCE_DYNAMIC_SIZE)
    meta_make_sizeable(meta);

  /* copy canary sequences if present */
  if (txn->canary.v) {
    meta->canary = txn->canary;
    meta->canary.v = constmeta_txnid(meta);
  }

  if (txn->dbs[MAIN_DBI].root == P_INVALID) {
    /* When the DB is empty, handle it specially to
     * fix any breakage like page leaks from ITS#8174. */
    meta->trees.main.flags = txn->dbs[MAIN_DBI].flags;
    compacting_fixup_meta(env, meta);
    if (dest_is_pipe) {
      if (flags & MDBX_CP_THROTTLE_MVCC)
        mdbx_txn_park(txn, false);
      int rc = osal_write(fd, buffer, meta_bytes);
      if (likely(rc == MDBX_SUCCESS) && (flags & MDBX_CP_THROTTLE_MVCC) != 0)
        rc = mdbx_txn_unpark(txn, false);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
    }
  } else {
    /* Count free pages + GC pages. */
    cursor_couple_t couple;
    int rc = cursor_init(&couple.outer, txn, FREE_DBI);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    pgno_t gc_npages = txn->dbs[FREE_DBI].branch_pages +
                       txn->dbs[FREE_DBI].leaf_pages +
                       txn->dbs[FREE_DBI].large_pages;
    MDBX_val key, data;
    rc = outer_first(&couple.outer, &key, &data);
    while (rc == MDBX_SUCCESS) {
      const pnl_t pnl = data.iov_base;
      if (unlikely(data.iov_len % sizeof(pgno_t) ||
                   data.iov_len < MDBX_PNL_SIZEOF(pnl))) {
        ERROR("%s/%d: %s %zu", "MDBX_CORRUPTED", MDBX_CORRUPTED,
              "invalid GC-record length", data.iov_len);
        return MDBX_CORRUPTED;
      }
      if (unlikely(!pnl_check(pnl, txn->geo.first_unallocated))) {
        ERROR("%s/%d: %s", "MDBX_CORRUPTED", MDBX_CORRUPTED,
              "invalid GC-record content");
        return MDBX_CORRUPTED;
      }
      gc_npages += MDBX_PNL_GETSIZE(pnl);
      rc = outer_next(&couple.outer, &key, &data, MDBX_NEXT);
    }
    if (unlikely(rc != MDBX_NOTFOUND))
      return rc;

    meta->geometry.first_unallocated = txn->geo.first_unallocated - gc_npages;
    meta->trees.main = txn->dbs[MAIN_DBI];

    ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    rc = osal_condpair_init(&ctx.condpair);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;

    memset(data_buffer, 0, 2 * (size_t)MDBX_ENVCOPY_WRITEBUF);
    ctx.write_buf[0] = data_buffer;
    ctx.write_buf[1] = data_buffer + (size_t)MDBX_ENVCOPY_WRITEBUF;
    ctx.first_unallocated = NUM_METAS;
    ctx.env = env;
    ctx.fd = fd;
    ctx.txn = txn;
    ctx.flags = flags;

    osal_thread_t thread;
    int thread_err = osal_thread_create(&thread, compacting_write_thread, &ctx);
    if (likely(thread_err == MDBX_SUCCESS)) {
      if (dest_is_pipe) {
        if (!meta->trees.main.mod_txnid)
          meta->trees.main.mod_txnid = txn->txnid;
        compacting_fixup_meta(env, meta);
        if (flags & MDBX_CP_THROTTLE_MVCC)
          mdbx_txn_park(txn, false);
        rc = osal_write(fd, buffer, meta_bytes);
        if (likely(rc == MDBX_SUCCESS) && (flags & MDBX_CP_THROTTLE_MVCC) != 0)
          rc = mdbx_txn_unpark(txn, false);
      }
      if (likely(rc == MDBX_SUCCESS))
        rc = compacting_walk_tree(&ctx, &meta->trees.main);
      if (ctx.write_len[ctx.head & 1])
        /* toggle to flush non-empty buffers */
        compacting_toggle_write_buffers(&ctx);

      if (likely(rc == MDBX_SUCCESS) &&
          unlikely(meta->geometry.first_unallocated != ctx.first_unallocated)) {
        if (ctx.first_unallocated > meta->geometry.first_unallocated) {
          ERROR("the source DB %s: post-compactification used pages %" PRIaPGNO
                " %c expected %" PRIaPGNO,
                "has double-used pages or other corruption",
                ctx.first_unallocated, '>', meta->geometry.first_unallocated);
          rc = MDBX_CORRUPTED; /* corrupted DB */
        }
        if (ctx.first_unallocated < meta->geometry.first_unallocated) {
          WARNING(
              "the source DB %s: post-compactification used pages %" PRIaPGNO
              " %c expected %" PRIaPGNO,
              "has page leak(s)", ctx.first_unallocated, '<',
              meta->geometry.first_unallocated);
          if (dest_is_pipe)
            /* the root within already written meta-pages is wrong */
            rc = MDBX_CORRUPTED;
        }
        /* fixup meta */
        meta->geometry.first_unallocated = ctx.first_unallocated;
      }

      /* toggle with empty buffers to exit thread's loop */
      eASSERT(env, (ctx.write_len[ctx.head & 1]) == 0);
      compacting_toggle_write_buffers(&ctx);
      thread_err = osal_thread_join(thread);
      eASSERT(env, (ctx.tail == ctx.head && ctx.write_len[ctx.head & 1] == 0) ||
                       ctx.error);
      osal_condpair_destroy(&ctx.condpair);
    }
    if (unlikely(thread_err != MDBX_SUCCESS))
      return thread_err;
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
    if (unlikely(ctx.error != MDBX_SUCCESS))
      return ctx.error;
    if (!dest_is_pipe)
      compacting_fixup_meta(env, meta);
  }

  if (flags & MDBX_CP_THROTTLE_MVCC)
    mdbx_txn_park(txn, false);

  /* Extend file if required */
  if (meta->geometry.now != meta->geometry.first_unallocated) {
    const size_t whole_size = pgno2bytes(env, meta->geometry.now);
    if (!dest_is_pipe)
      return osal_ftruncate(fd, whole_size);

    const size_t used_size = pgno2bytes(env, meta->geometry.first_unallocated);
    memset(data_buffer, 0, (size_t)MDBX_ENVCOPY_WRITEBUF);
    for (size_t offset = used_size; offset < whole_size;) {
      const size_t chunk = ((size_t)MDBX_ENVCOPY_WRITEBUF < whole_size - offset)
                               ? (size_t)MDBX_ENVCOPY_WRITEBUF
                               : whole_size - offset;
      int rc = osal_write(fd, data_buffer, chunk);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
      offset += chunk;
    }
  }
  return MDBX_SUCCESS;
}

//----------------------------------------------------------------------------

__cold static int copy_asis(MDBX_env *env, MDBX_txn *txn, mdbx_filehandle_t fd,
                            uint8_t *buffer, const bool dest_is_pipe,
                            const MDBX_copy_flags_t flags) {
  bool should_unlock = false;
  if ((txn->flags & MDBX_TXN_RDONLY) != 0 && (flags & MDBX_CP_RENEW_TXN) != 0) {
    /* Try temporarily block writers until we snapshot the meta pages */
    int err = lck_txn_lock(env, true);
    if (likely(err == MDBX_SUCCESS))
      should_unlock = true;
    else if (unlikely(err != MDBX_BUSY))
      return err;
  }

  jitter4testing(false);
  int rc = MDBX_SUCCESS;
  const size_t meta_bytes = pgno2bytes(env, NUM_METAS);
  troika_t troika = meta_tap(env);
  /* Make a snapshot of meta-pages,
   * but writing ones after the data was flushed */
retry_snap_meta:
  memcpy(buffer, env->dxb_mmap.base, meta_bytes);
  const meta_ptr_t recent = meta_recent(env, &troika);
  meta_t *headcopy = /* LY: get pointer to the snapshot copy */
      ptr_disp(buffer, ptr_dist(recent.ptr_c, env->dxb_mmap.base));
  jitter4testing(false);
  if (txn->flags & MDBX_TXN_RDONLY) {
    if (recent.txnid != txn->txnid) {
      if (flags & MDBX_CP_RENEW_TXN)
        rc = mdbx_txn_renew(txn);
      else {
        rc = MDBX_MVCC_RETARDED;
        for (size_t n = 0; n < NUM_METAS; ++n) {
          meta_t *const meta = page_meta(ptr_disp(buffer, pgno2bytes(env, n)));
          if (troika.txnid[n] == txn->txnid &&
              ((/* is_steady */ (troika.fsm >> n) & 1) || rc != MDBX_SUCCESS)) {
            rc = MDBX_SUCCESS;
            headcopy = meta;
          } else if (troika.txnid[n] > txn->txnid)
            meta_set_txnid(env, meta, 0);
        }
      }
    }
    if (should_unlock)
      lck_txn_unlock(env);
    else {
      troika_t snap = meta_tap(env);
      if (memcmp(&troika, &snap, sizeof(troika_t)) && rc == MDBX_SUCCESS) {
        troika = snap;
        goto retry_snap_meta;
      }
    }
  }
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (txn->flags & MDBX_TXN_RDONLY)
    eASSERT(env, meta_txnid(headcopy) == txn->txnid);
  if (flags & MDBX_CP_FORCE_DYNAMIC_SIZE)
    meta_make_sizeable(headcopy);
  /* Update signature to steady */
  meta_sign_as_steady(headcopy);

  /* Copy the data */
  const size_t whole_size = pgno_align2os_bytes(env, txn->geo.end_pgno);
  const size_t used_size = pgno2bytes(env, txn->geo.first_unallocated);
  jitter4testing(false);

  if (flags & MDBX_CP_THROTTLE_MVCC)
    mdbx_txn_park(txn, false);

  if (dest_is_pipe)
    rc = osal_write(fd, buffer, meta_bytes);

  uint8_t *const data_buffer =
      buffer + ceil_powerof2(meta_bytes, globals.sys_pagesize);
#if MDBX_USE_COPYFILERANGE
  static bool copyfilerange_unavailable;
  bool not_the_same_filesystem = false;
  struct statfs statfs_info;
  if (fstatfs(fd, &statfs_info) ||
      statfs_info.f_type == /* ECRYPTFS_SUPER_MAGIC */ 0xf15f)
    /* avoid use copyfilerange_unavailable() to ecryptfs due bugs */
    not_the_same_filesystem = true;
#endif /* MDBX_USE_COPYFILERANGE */

  for (size_t offset = meta_bytes; rc == MDBX_SUCCESS && offset < used_size;) {
    if (flags & MDBX_CP_THROTTLE_MVCC) {
      rc = mdbx_txn_unpark(txn, false);
      if (unlikely(rc != MDBX_SUCCESS))
        break;
    }

#if MDBX_USE_SENDFILE
    static bool sendfile_unavailable;
    if (dest_is_pipe && likely(!sendfile_unavailable)) {
      off_t in_offset = offset;
      const ssize_t written =
          sendfile(fd, env->lazy_fd, &in_offset, used_size - offset);
      if (likely(written > 0)) {
        offset = in_offset;
        if (flags & MDBX_CP_THROTTLE_MVCC)
          rc = mdbx_txn_park(txn, false);
        continue;
      }
      rc = MDBX_ENODATA;
      if (written == 0 || ignore_enosys(rc = errno) != MDBX_RESULT_TRUE)
        break;
      sendfile_unavailable = true;
    }
#endif /* MDBX_USE_SENDFILE */

#if MDBX_USE_COPYFILERANGE
    if (!dest_is_pipe && !not_the_same_filesystem &&
        likely(!copyfilerange_unavailable)) {
      off_t in_offset = offset, out_offset = offset;
      ssize_t bytes_copied = copy_file_range(
          env->lazy_fd, &in_offset, fd, &out_offset, used_size - offset, 0);
      if (likely(bytes_copied > 0)) {
        offset = in_offset;
        if (flags & MDBX_CP_THROTTLE_MVCC)
          rc = mdbx_txn_park(txn, false);
        continue;
      }
      rc = MDBX_ENODATA;
      if (bytes_copied == 0)
        break;
      rc = errno;
      if (rc == EXDEV || rc == /* workaround for ecryptfs bug(s),
                                  maybe useful for others FS */
                             EINVAL)
        not_the_same_filesystem = true;
      else if (ignore_enosys(rc) == MDBX_RESULT_TRUE)
        copyfilerange_unavailable = true;
      else
        break;
    }
#endif /* MDBX_USE_COPYFILERANGE */

    /* fallback to portable */
    const size_t chunk = ((size_t)MDBX_ENVCOPY_WRITEBUF < used_size - offset)
                             ? (size_t)MDBX_ENVCOPY_WRITEBUF
                             : used_size - offset;
    /* copy to avoid EFAULT in case swapped-out */
    memcpy(data_buffer, ptr_disp(env->dxb_mmap.base, offset), chunk);
    if (flags & MDBX_CP_THROTTLE_MVCC)
      mdbx_txn_park(txn, false);
    rc = osal_write(fd, data_buffer, chunk);
    offset += chunk;
  }

  /* Extend file if required */
  if (likely(rc == MDBX_SUCCESS) && whole_size != used_size) {
    if (!dest_is_pipe)
      rc = osal_ftruncate(fd, whole_size);
    else {
      memset(data_buffer, 0, (size_t)MDBX_ENVCOPY_WRITEBUF);
      for (size_t offset = used_size;
           rc == MDBX_SUCCESS && offset < whole_size;) {
        const size_t chunk =
            ((size_t)MDBX_ENVCOPY_WRITEBUF < whole_size - offset)
                ? (size_t)MDBX_ENVCOPY_WRITEBUF
                : whole_size - offset;
        rc = osal_write(fd, data_buffer, chunk);
        offset += chunk;
      }
    }
  }

  return rc;
}

//----------------------------------------------------------------------------

__cold static int copy2fd(MDBX_txn *txn, mdbx_filehandle_t fd,
                          MDBX_copy_flags_t flags) {
  if (unlikely(txn->flags & MDBX_TXN_DIRTY))
    return MDBX_BAD_TXN;

  int rc = MDBX_SUCCESS;
  if (txn->flags & MDBX_TXN_RDONLY) {
    if (flags & MDBX_CP_THROTTLE_MVCC) {
      rc = mdbx_txn_park(txn, true);
      if (unlikely(rc != MDBX_SUCCESS))
        return rc;
    }
  } else if (unlikely(flags & (MDBX_CP_THROTTLE_MVCC | MDBX_CP_RENEW_TXN)))
    return MDBX_EINVAL;

  const int dest_is_pipe = osal_is_pipe(fd);
  if (MDBX_IS_ERROR(dest_is_pipe))
    return dest_is_pipe;

  if (!dest_is_pipe) {
    rc = osal_fseek(fd, 0);
    if (unlikely(rc != MDBX_SUCCESS))
      return rc;
  }

  MDBX_env *const env = txn->env;
  const size_t buffer_size =
      pgno_align2os_bytes(env, NUM_METAS) +
      ceil_powerof2(((flags & MDBX_CP_COMPACT)
                         ? 2 * (size_t)MDBX_ENVCOPY_WRITEBUF
                         : (size_t)MDBX_ENVCOPY_WRITEBUF),
                    globals.sys_pagesize);

  uint8_t *buffer = nullptr;
  rc = osal_memalign_alloc(globals.sys_pagesize, buffer_size, (void **)&buffer);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  if (!dest_is_pipe) {
    /* Firstly write a stub to meta-pages.
     * Now we sure to incomplete copy will not be used. */
    memset(buffer, -1, pgno2bytes(env, NUM_METAS));
    rc = osal_write(fd, buffer, pgno2bytes(env, NUM_METAS));
  }

  if (likely(rc == MDBX_SUCCESS))
    rc = mdbx_txn_unpark(txn, false);
  if (likely(rc == MDBX_SUCCESS)) {
    memset(buffer, 0, pgno2bytes(env, NUM_METAS));
    rc = ((flags & MDBX_CP_COMPACT) ? copy_with_compacting : copy_asis)(
        env, txn, fd, buffer, dest_is_pipe, flags);

    if (likely(rc == MDBX_SUCCESS))
      rc = mdbx_txn_unpark(txn, false);
  }

  if (flags & MDBX_CP_THROTTLE_MVCC)
    mdbx_txn_park(txn, true);
  else if (flags & MDBX_CP_DISPOSE_TXN)
    mdbx_txn_reset(txn);

  if (!dest_is_pipe) {
    if (likely(rc == MDBX_SUCCESS) && (flags & MDBX_CP_DONT_FLUSH) == 0)
      rc = osal_fsync(fd, MDBX_SYNC_DATA | MDBX_SYNC_SIZE);

    /* Write actual meta */
    if (likely(rc == MDBX_SUCCESS))
      rc = osal_pwrite(fd, buffer, pgno2bytes(env, NUM_METAS), 0);

    if (likely(rc == MDBX_SUCCESS) && (flags & MDBX_CP_DONT_FLUSH) == 0)
      rc = osal_fsync(fd, MDBX_SYNC_DATA | MDBX_SYNC_IODQ);
  }

  osal_memalign_free(buffer);
  return rc;
}

__cold static int copy2pathname(MDBX_txn *txn, const pathchar_t *dest_path,
                                MDBX_copy_flags_t flags) {
  if (unlikely(!dest_path || *dest_path == '\0'))
    return MDBX_EINVAL;

  /* The destination path must exist, but the destination file must not.
   * We don't want the OS to cache the writes, since the source data is
   * already in the OS cache. */
  mdbx_filehandle_t newfd = INVALID_HANDLE_VALUE;
  int rc = osal_openfile(MDBX_OPEN_COPY, txn->env, dest_path, &newfd,
#if defined(_WIN32) || defined(_WIN64)
                         (mdbx_mode_t)-1
#else
                         S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP
#endif
  );

#if defined(_WIN32) || defined(_WIN64)
  /* no locking required since the file opened with ShareMode == 0 */
#else
  if (rc == MDBX_SUCCESS) {
    MDBX_STRUCT_FLOCK lock_op;
    memset(&lock_op, 0, sizeof(lock_op));
    lock_op.l_type = F_WRLCK;
    lock_op.l_whence = SEEK_SET;
    lock_op.l_start = 0;
    lock_op.l_len = OFF_T_MAX;
    if (MDBX_FCNTL(newfd, MDBX_F_SETLK, &lock_op)
#if (defined(__linux__) || defined(__gnu_linux__)) && defined(LOCK_EX) &&      \
    (!defined(__ANDROID_API__) || __ANDROID_API__ >= 24)
        || flock(newfd, LOCK_EX | LOCK_NB)
#endif /* Linux */
    )
      rc = errno;
  }
#endif /* Windows / POSIX */

  if (rc == MDBX_SUCCESS)
    rc = copy2fd(txn, newfd, flags);

  if (newfd != INVALID_HANDLE_VALUE) {
    int err = osal_closefile(newfd);
    if (rc == MDBX_SUCCESS && err != rc)
      rc = err;
    if (rc != MDBX_SUCCESS)
      (void)osal_removefile(dest_path);
  }
  return rc;
}

//----------------------------------------------------------------------------

__cold int mdbx_txn_copy2fd(MDBX_txn *txn, mdbx_filehandle_t fd,
                            MDBX_copy_flags_t flags) {
  int rc = check_txn(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  rc = copy2fd(txn, fd, flags);
  if (flags & MDBX_CP_DISPOSE_TXN)
    mdbx_txn_abort(txn);
  return rc;
}

__cold int mdbx_env_copy2fd(MDBX_env *env, mdbx_filehandle_t fd,
                            MDBX_copy_flags_t flags) {
  if (unlikely(flags & (MDBX_CP_DISPOSE_TXN | MDBX_CP_RENEW_TXN)))
    return MDBX_EINVAL;

  int rc = check_env(env, true);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  MDBX_txn *txn = nullptr;
  rc = mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &txn);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  rc = copy2fd(txn, fd, flags | MDBX_CP_DISPOSE_TXN | MDBX_CP_RENEW_TXN);
  mdbx_txn_abort(txn);
  return rc;
}

__cold int mdbx_txn_copy2pathname(MDBX_txn *txn, const char *dest_path,
                                  MDBX_copy_flags_t flags) {
#if defined(_WIN32) || defined(_WIN64)
  wchar_t *dest_pathW = nullptr;
  int rc = osal_mb2w(dest_path, &dest_pathW);
  if (likely(rc == MDBX_SUCCESS)) {
    rc = mdbx_txn_copy2pathnameW(txn, dest_pathW, flags);
    osal_free(dest_pathW);
  }
  return rc;
}

__cold int mdbx_txn_copy2pathnameW(MDBX_txn *txn, const wchar_t *dest_path,
                                   MDBX_copy_flags_t flags) {
#endif /* Windows */
  int rc = check_txn(txn, MDBX_TXN_BLOCKED);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  rc = copy2pathname(txn, dest_path, flags);
  if (flags & MDBX_CP_DISPOSE_TXN)
    mdbx_txn_abort(txn);
  return rc;
}

__cold int mdbx_env_copy(MDBX_env *env, const char *dest_path,
                         MDBX_copy_flags_t flags) {
#if defined(_WIN32) || defined(_WIN64)
  wchar_t *dest_pathW = nullptr;
  int rc = osal_mb2w(dest_path, &dest_pathW);
  if (likely(rc == MDBX_SUCCESS)) {
    rc = mdbx_env_copyW(env, dest_pathW, flags);
    osal_free(dest_pathW);
  }
  return rc;
}

__cold int mdbx_env_copyW(MDBX_env *env, const wchar_t *dest_path,
                          MDBX_copy_flags_t flags) {
#endif /* Windows */
  if (unlikely(flags & (MDBX_CP_DISPOSE_TXN | MDBX_CP_RENEW_TXN)))
    return MDBX_EINVAL;

  int rc = check_env(env, true);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  MDBX_txn *txn = nullptr;
  rc = mdbx_txn_begin(env, nullptr, MDBX_TXN_RDONLY, &txn);
  if (unlikely(rc != MDBX_SUCCESS))
    return rc;

  rc = copy2pathname(txn, dest_path,
                     flags | MDBX_CP_DISPOSE_TXN | MDBX_CP_RENEW_TXN);
  mdbx_txn_abort(txn);
  return rc;
}
