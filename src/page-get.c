/// \copyright SPDX-License-Identifier: Apache-2.0
/// \author Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru> \date 2015-2024

#include "internals.h"

__cold int MDBX_PRINTF_ARGS(2, 3) bad_page(const page_t *mp, const char *fmt, ...) {
  if (LOG_ENABLED(MDBX_LOG_ERROR)) {
    static const page_t *prev;
    if (prev != mp) {
      char buf4unknown[16];
      prev = mp;
      debug_log(MDBX_LOG_ERROR, "badpage", 0, "corrupted %s-page #%u, mod-txnid %" PRIaTXN "\n",
                pagetype_caption(page_type(mp), buf4unknown), mp->pgno, mp->txnid);
    }

    va_list args;
    va_start(args, fmt);
    debug_log_va(MDBX_LOG_ERROR, "badpage", 0, fmt, args);
    va_end(args);
  }
  return MDBX_CORRUPTED;
}

__cold void MDBX_PRINTF_ARGS(2, 3) poor_page(const page_t *mp, const char *fmt, ...) {
  if (LOG_ENABLED(MDBX_LOG_NOTICE)) {
    static const page_t *prev;
    if (prev != mp) {
      char buf4unknown[16];
      prev = mp;
      debug_log(MDBX_LOG_NOTICE, "poorpage", 0, "suboptimal %s-page #%u, mod-txnid %" PRIaTXN "\n",
                pagetype_caption(page_type(mp), buf4unknown), mp->pgno, mp->txnid);
    }

    va_list args;
    va_start(args, fmt);
    debug_log_va(MDBX_LOG_NOTICE, "poorpage", 0, fmt, args);
    va_end(args);
  }
}

MDBX_CONST_FUNCTION static clc_t value_clc(const MDBX_cursor *mc) {
  if (likely((mc->flags & z_inner) == 0))
    return mc->clc->v;
  else {
    clc_t stub = {.cmp = cmp_equal_or_wrong, .lmin = 0, .lmax = 0};
    return stub;
  }
}

__cold int page_check(const MDBX_cursor *const mc, const page_t *const mp) {
  DKBUF;
  int rc = MDBX_SUCCESS;
  if (unlikely(mp->pgno < MIN_PAGENO || mp->pgno > MAX_PAGENO))
    rc = bad_page(mp, "invalid pgno (%u)\n", mp->pgno);

  MDBX_env *const env = mc->txn->env;
  const ptrdiff_t offset = ptr_dist(mp, env->dxb_mmap.base);
  unsigned flags_mask = P_ILL_BITS;
  unsigned flags_expected = 0;
  if (offset < 0 || offset > (ptrdiff_t)(pgno2bytes(env, mc->txn->geo.first_unallocated) -
                                         ((mp->flags & P_SUBP) ? PAGEHDRSZ + 1 : env->ps))) {
    /* should be dirty page without MDBX_WRITEMAP, or a subpage of. */
    flags_mask -= P_SUBP;
    if ((env->flags & MDBX_WRITEMAP) != 0 || (!is_shadowed(mc->txn, mp) && !(mp->flags & P_SUBP)))
      rc = bad_page(mp, "invalid page-address %p, offset %zi\n", __Wpedantic_format_voidptr(mp), offset);
  } else if (offset & (env->ps - 1))
    flags_expected = P_SUBP;

  if (unlikely((mp->flags & flags_mask) != flags_expected))
    rc = bad_page(mp, "unknown/extra page-flags (have 0x%x, expect 0x%x)\n", mp->flags & flags_mask, flags_expected);

  cASSERT(mc, (mc->checking & z_dupfix) == 0 || (mc->flags & z_inner) != 0);
  const uint8_t type = page_type(mp);
  switch (type) {
  default:
    return bad_page(mp, "invalid type (%u)\n", type);
  case P_LARGE:
    if (unlikely(mc->flags & z_inner))
      rc = bad_page(mp, "unexpected %s-page for %s (db-flags 0x%x)\n", "large", "nested dupsort tree", mc->tree->flags);
    const pgno_t npages = mp->pages;
    if (unlikely(npages < 1 || npages >= MAX_PAGENO / 2))
      rc = bad_page(mp, "invalid n-pages (%u) for large-page\n", npages);
    if (unlikely(mp->pgno + npages > mc->txn->geo.first_unallocated))
      rc = bad_page(mp, "end of large-page beyond (%u) allocated space (%u next-pgno)\n", mp->pgno + npages,
                    mc->txn->geo.first_unallocated);
    return rc; //-------------------------- end of large/overflow page handling
  case P_LEAF | P_SUBP:
    if (unlikely(mc->tree->height != 1))
      rc =
          bad_page(mp, "unexpected %s-page for %s (db-flags 0x%x)\n", "leaf-sub", "nested dupsort db", mc->tree->flags);
    /* fall through */
    __fallthrough;
  case P_LEAF:
    if (unlikely((mc->checking & z_dupfix) != 0))
      rc = bad_page(mp, "unexpected leaf-page for dupfix subtree (db-lags 0x%x)\n", mc->tree->flags);
    break;
  case P_LEAF | P_DUPFIX | P_SUBP:
    if (unlikely(mc->tree->height != 1))
      rc = bad_page(mp, "unexpected %s-page for %s (db-flags 0x%x)\n", "leaf2-sub", "nested dupsort db",
                    mc->tree->flags);
    /* fall through */
    __fallthrough;
  case P_LEAF | P_DUPFIX:
    if (unlikely((mc->checking & z_dupfix) == 0))
      rc = bad_page(mp, "unexpected leaf2-page for non-dupfix (sub)tree (db-flags 0x%x)\n", mc->tree->flags);
    break;
  case P_BRANCH:
    break;
  }

  if (unlikely(mp->upper < mp->lower || (mp->lower & 1) || PAGEHDRSZ + mp->upper > env->ps))
    rc = bad_page(mp, "invalid page lower(%u)/upper(%u) with limit %zu\n", mp->lower, mp->upper, page_space(env));

  const char *const end_of_page = ptr_disp(mp, env->ps);
  const size_t nkeys = page_numkeys(mp);
  STATIC_ASSERT(P_BRANCH == 1);
  if (unlikely(nkeys <= (uint8_t)(mp->flags & P_BRANCH))) {
    if ((!(mc->flags & z_inner) || mc->tree->items) &&
        (!(mc->checking & z_updating) || !(is_modifable(mc->txn, mp) || (mp->flags & P_SUBP))))
      rc = bad_page(mp, "%s-page nkeys (%zu) < %u\n", is_branch(mp) ? "branch" : "leaf", nkeys, 1 + is_branch(mp));
  }

  const size_t ksize_max = keysize_max(env->ps, 0);
  const size_t leaf2_ksize = mp->dupfix_ksize;
  if (is_dupfix_leaf(mp)) {
    if (unlikely((mc->flags & z_inner) == 0 || (mc->tree->flags & MDBX_DUPFIXED) == 0))
      rc = bad_page(mp, "unexpected leaf2-page (db-flags 0x%x)\n", mc->tree->flags);
    else if (unlikely(leaf2_ksize != mc->tree->dupfix_size))
      rc = bad_page(mp, "invalid leaf2_ksize %zu\n", leaf2_ksize);
    else if (unlikely(((leaf2_ksize & nkeys) ^ mp->upper) & 1))
      rc = bad_page(mp, "invalid page upper (%u) for nkeys %zu with leaf2-length %zu\n", mp->upper, nkeys, leaf2_ksize);
  } else {
    if (unlikely((mp->upper & 1) || PAGEHDRSZ + mp->upper + nkeys * sizeof(node_t) + nkeys - 1 > env->ps))
      rc = bad_page(mp, "invalid page upper (%u) for nkeys %zu with limit %zu\n", mp->upper, nkeys, page_space(env));
  }

  MDBX_val here, prev = {0, 0};
  clc_t v_clc = value_clc(mc);
  for (size_t i = 0; i < nkeys; ++i) {
    if (is_dupfix_leaf(mp)) {
      const char *const key = page_dupfix_ptr(mp, i, mc->tree->dupfix_size);
      if (unlikely(end_of_page < key + leaf2_ksize)) {
        rc = bad_page(mp, "leaf2-item beyond (%zu) page-end\n", key + leaf2_ksize - end_of_page);
        continue;
      }

      if (unlikely(leaf2_ksize != mc->clc->k.lmin)) {
        if (unlikely(leaf2_ksize < mc->clc->k.lmin || leaf2_ksize > mc->clc->k.lmax))
          rc = bad_page(mp, "leaf2-item size (%zu) <> min/max length (%zu/%zu)\n", leaf2_ksize, mc->clc->k.lmin,
                        mc->clc->k.lmax);
        else
          mc->clc->k.lmin = mc->clc->k.lmax = leaf2_ksize;
      }
      if ((mc->checking & z_ignord) == 0) {
        here.iov_base = (void *)key;
        here.iov_len = leaf2_ksize;
        if (prev.iov_base && unlikely(mc->clc->k.cmp(&prev, &here) >= 0))
          rc = bad_page(mp, "leaf2-item #%zu wrong order (%s >= %s)\n", i, DKEY(&prev), DVAL(&here));
        prev = here;
      }
    } else {
      const node_t *const node = page_node(mp, i);
      const char *const node_end = ptr_disp(node, NODESIZE);
      if (unlikely(node_end > end_of_page)) {
        rc = bad_page(mp, "node[%zu] (%zu) beyond page-end\n", i, node_end - end_of_page);
        continue;
      }
      const size_t ksize = node_ks(node);
      if (unlikely(ksize > ksize_max))
        rc = bad_page(mp, "node[%zu] too long key (%zu)\n", i, ksize);
      const char *const key = node_key(node);
      if (unlikely(end_of_page < key + ksize)) {
        rc = bad_page(mp, "node[%zu] key (%zu) beyond page-end\n", i, key + ksize - end_of_page);
        continue;
      }
      if ((is_leaf(mp) || i > 0)) {
        if (unlikely(ksize < mc->clc->k.lmin || ksize > mc->clc->k.lmax))
          rc = bad_page(mp, "node[%zu] key size (%zu) <> min/max key-length (%zu/%zu)\n", i, ksize, mc->clc->k.lmin,
                        mc->clc->k.lmax);
        if ((mc->checking & z_ignord) == 0) {
          here.iov_base = (void *)key;
          here.iov_len = ksize;
          if (prev.iov_base && unlikely(mc->clc->k.cmp(&prev, &here) >= 0))
            rc = bad_page(mp, "node[%zu] key wrong order (%s >= %s)\n", i, DKEY(&prev), DVAL(&here));
          prev = here;
        }
      }
      if (is_branch(mp)) {
        if ((mc->checking & z_updating) == 0 && i == 0 && unlikely(ksize != 0))
          rc = bad_page(mp, "branch-node[%zu] wrong 0-node key-length (%zu)\n", i, ksize);
        const pgno_t ref = node_pgno(node);
        if (unlikely(ref < MIN_PAGENO) || (unlikely(ref >= mc->txn->geo.first_unallocated) &&
                                           (unlikely(ref >= mc->txn->geo.now) || !(mc->checking & z_retiring))))
          rc = bad_page(mp, "branch-node[%zu] wrong pgno (%u)\n", i, ref);
        if (unlikely(node_flags(node)))
          rc = bad_page(mp, "branch-node[%zu] wrong flags (%u)\n", i, node_flags(node));
        continue;
      }

      switch (node_flags(node)) {
      default:
        rc = bad_page(mp, "invalid node[%zu] flags (%u)\n", i, node_flags(node));
        break;
      case N_BIG /* data on large-page */:
      case 0 /* usual */:
      case N_TREE /* sub-db */:
      case N_TREE | N_DUP /* dupsorted sub-tree */:
      case N_DUP /* short sub-page */:
        break;
      }

      const size_t dsize = node_ds(node);
      const char *const data = node_data(node);
      if (node_flags(node) & N_BIG) {
        if (unlikely(end_of_page < data + sizeof(pgno_t))) {
          rc = bad_page(mp, "node-%s(%zu of %zu, %zu bytes) beyond (%zu) page-end\n", "bigdata-pgno", i, nkeys, dsize,
                        data + dsize - end_of_page);
          continue;
        }
        if (unlikely(dsize <= v_clc.lmin || dsize > v_clc.lmax))
          rc = bad_page(mp, "big-node data size (%zu) <> min/max value-length (%zu/%zu)\n", dsize, v_clc.lmin,
                        v_clc.lmax);
        if (unlikely(node_size_len(node_ks(node), dsize) <= mc->txn->env->leaf_nodemax) &&
            mc->tree != &mc->txn->dbs[FREE_DBI])
          poor_page(mp, "too small data (%zu bytes) for bigdata-node", dsize);

        if ((mc->checking & z_retiring) == 0) {
          const pgr_t lp = page_get_large(mc, node_largedata_pgno(node), mp->txnid);
          if (unlikely(lp.err != MDBX_SUCCESS))
            return lp.err;
          cASSERT(mc, page_type(lp.page) == P_LARGE);
          const unsigned npages = largechunk_npages(env, dsize);
          if (unlikely(lp.page->pages != npages)) {
            if (lp.page->pages < npages)
              rc = bad_page(lp.page, "too less n-pages %u for bigdata-node (%zu bytes)", lp.page->pages, dsize);
            else if (mc->tree != &mc->txn->dbs[FREE_DBI])
              poor_page(lp.page, "extra n-pages %u for bigdata-node (%zu bytes)", lp.page->pages, dsize);
          }
        }
        continue;
      }

      if (unlikely(end_of_page < data + dsize)) {
        rc = bad_page(mp, "node-%s(%zu of %zu, %zu bytes) beyond (%zu) page-end\n", "data", i, nkeys, dsize,
                      data + dsize - end_of_page);
        continue;
      }

      switch (node_flags(node)) {
      default:
        /* wrong, but already handled */
        continue;
      case 0 /* usual */:
        if (unlikely(dsize < v_clc.lmin || dsize > v_clc.lmax)) {
          rc = bad_page(mp, "node-data size (%zu) <> min/max value-length (%zu/%zu)\n", dsize, v_clc.lmin, v_clc.lmax);
          continue;
        }
        break;
      case N_TREE /* sub-db */:
        if (unlikely(dsize != sizeof(tree_t))) {
          rc = bad_page(mp, "invalid sub-db record size (%zu)\n", dsize);
          continue;
        }
        break;
      case N_TREE | N_DUP /* dupsorted sub-tree */:
        if (unlikely(dsize != sizeof(tree_t))) {
          rc = bad_page(mp, "invalid nested-db record size (%zu, expect %zu)\n", dsize, sizeof(tree_t));
          continue;
        }
        break;
      case N_DUP /* short sub-page */:
        if (unlikely(dsize <= PAGEHDRSZ)) {
          rc = bad_page(mp, "invalid nested/sub-page record size (%zu)\n", dsize);
          continue;
        } else {
          const page_t *const sp = (page_t *)data;
          switch (sp->flags &
                  /* ignore legacy P_DIRTY flag */ ~P_LEGACY_DIRTY) {
          case P_LEAF | P_SUBP:
          case P_LEAF | P_DUPFIX | P_SUBP:
            break;
          default:
            rc = bad_page(mp, "invalid nested/sub-page flags (0x%02x)\n", sp->flags);
            continue;
          }

          const char *const end_of_subpage = data + dsize;
          const intptr_t nsubkeys = page_numkeys(sp);
          if (unlikely(nsubkeys == 0) && !(mc->checking & z_updating) && mc->tree->items)
            rc = bad_page(mp, "no keys on a %s-page\n", is_dupfix_leaf(sp) ? "leaf2-sub" : "leaf-sub");

          MDBX_val sub_here, sub_prev = {0, 0};
          for (int ii = 0; ii < nsubkeys; ii++) {
            if (is_dupfix_leaf(sp)) {
              /* DUPFIX pages have no entries[] or node headers */
              const size_t sub_ksize = sp->dupfix_ksize;
              const char *const sub_key = page_dupfix_ptr(sp, ii, mc->tree->dupfix_size);
              if (unlikely(end_of_subpage < sub_key + sub_ksize)) {
                rc = bad_page(mp, "nested-leaf2-key beyond (%zu) nested-page\n", sub_key + sub_ksize - end_of_subpage);
                continue;
              }

              if (unlikely(sub_ksize != v_clc.lmin)) {
                if (unlikely(sub_ksize < v_clc.lmin || sub_ksize > v_clc.lmax))
                  rc = bad_page(mp,
                                "nested-leaf2-key size (%zu) <> min/max "
                                "value-length (%zu/%zu)\n",
                                sub_ksize, v_clc.lmin, v_clc.lmax);
                else
                  v_clc.lmin = v_clc.lmax = sub_ksize;
              }
              if ((mc->checking & z_ignord) == 0) {
                sub_here.iov_base = (void *)sub_key;
                sub_here.iov_len = sub_ksize;
                if (sub_prev.iov_base && unlikely(v_clc.cmp(&sub_prev, &sub_here) >= 0))
                  rc = bad_page(mp, "nested-leaf2-key #%u wrong order (%s >= %s)\n", ii, DKEY(&sub_prev),
                                DVAL(&sub_here));
                sub_prev = sub_here;
              }
            } else {
              const node_t *const sub_node = page_node(sp, ii);
              const char *const sub_node_end = ptr_disp(sub_node, NODESIZE);
              if (unlikely(sub_node_end > end_of_subpage)) {
                rc = bad_page(mp, "nested-node beyond (%zu) nested-page\n", end_of_subpage - sub_node_end);
                continue;
              }
              if (unlikely(node_flags(sub_node) != 0))
                rc = bad_page(mp, "nested-node invalid flags (%u)\n", node_flags(sub_node));

              const size_t sub_ksize = node_ks(sub_node);
              const char *const sub_key = node_key(sub_node);
              const size_t sub_dsize = node_ds(sub_node);
              /* char *sub_data = node_data(sub_node); */

              if (unlikely(sub_ksize < v_clc.lmin || sub_ksize > v_clc.lmax))
                rc = bad_page(mp,
                              "nested-node-key size (%zu) <> min/max "
                              "value-length (%zu/%zu)\n",
                              sub_ksize, v_clc.lmin, v_clc.lmax);
              if ((mc->checking & z_ignord) == 0) {
                sub_here.iov_base = (void *)sub_key;
                sub_here.iov_len = sub_ksize;
                if (sub_prev.iov_base && unlikely(v_clc.cmp(&sub_prev, &sub_here) >= 0))
                  rc = bad_page(mp, "nested-node-key #%u wrong order (%s >= %s)\n", ii, DKEY(&sub_prev),
                                DVAL(&sub_here));
                sub_prev = sub_here;
              }
              if (unlikely(sub_dsize != 0))
                rc = bad_page(mp, "nested-node non-empty data size (%zu)\n", sub_dsize);
              if (unlikely(end_of_subpage < sub_key + sub_ksize))
                rc = bad_page(mp, "nested-node-key beyond (%zu) nested-page\n", sub_key + sub_ksize - end_of_subpage);
            }
          }
        }
        break;
      }
    }
  }
  return rc;
}

static __always_inline int check_page_header(const uint16_t ILL, const page_t *page, MDBX_txn *const txn,
                                             const txnid_t front) {
  if (unlikely(page->flags & ILL)) {
    if (ILL == P_ILL_BITS || (page->flags & P_ILL_BITS))
      return bad_page(page, "invalid page's flags (%u)\n", page->flags);
    else if (ILL & P_LARGE) {
      assert((ILL & (P_BRANCH | P_LEAF | P_DUPFIX)) == 0);
      assert(page->flags & (P_BRANCH | P_LEAF | P_DUPFIX));
      return bad_page(page, "unexpected %s instead of %s (%u)\n", "large/overflow", "branch/leaf/leaf2", page->flags);
    } else if (ILL & (P_BRANCH | P_LEAF | P_DUPFIX)) {
      assert((ILL & P_BRANCH) && (ILL & P_LEAF) && (ILL & P_DUPFIX));
      assert(page->flags & (P_BRANCH | P_LEAF | P_DUPFIX));
      return bad_page(page, "unexpected %s instead of %s (%u)\n", "branch/leaf/leaf2", "large/overflow", page->flags);
    } else {
      assert(false);
    }
  }

  if (unlikely(page->txnid > front) && unlikely(page->txnid > txn->front_txnid || front < txn->txnid))
    return bad_page(page, "invalid page' txnid (%" PRIaTXN ") for %s' txnid (%" PRIaTXN ")\n", page->txnid,
                    (front == txn->front_txnid && front != txn->txnid) ? "front-txn" : "parent-page", front);

  if (((ILL & P_LARGE) || !is_largepage(page)) && (ILL & (P_BRANCH | P_LEAF | P_DUPFIX)) == 0) {
    /* Контроль четности page->upper тут либо приводит к ложным ошибкам,
     * либо слишком дорог по количеству операций. Заковырка в том, что upper
     * может быть нечетным на DUPFIX-страницах, при нечетном количестве
     * элементов нечетной длины. Поэтому четность page->upper здесь не
     * проверяется, но соответствующие полные проверки есть в page_check(). */
    if (unlikely(page->upper < page->lower || (page->lower & 1) || PAGEHDRSZ + page->upper > txn->env->ps))
      return bad_page(page, "invalid page' lower(%u)/upper(%u) with limit %zu\n", page->lower, page->upper,
                      page_space(txn->env));

  } else if ((ILL & P_LARGE) == 0) {
    const pgno_t npages = page->pages;
    if (unlikely(npages < 1) || unlikely(npages >= MAX_PAGENO / 2))
      return bad_page(page, "invalid n-pages (%u) for large-page\n", npages);
    if (unlikely(page->pgno + npages > txn->geo.first_unallocated))
      return bad_page(page, "end of large-page beyond (%u) allocated space (%u next-pgno)\n", page->pgno + npages,
                      txn->geo.first_unallocated);
  } else {
    assert(false);
  }
  return MDBX_SUCCESS;
}

__cold static __noinline pgr_t check_page_complete(const uint16_t ILL, page_t *page, const MDBX_cursor *const mc,
                                                   const txnid_t front) {
  pgr_t r = {page, check_page_header(ILL, page, mc->txn, front)};
  if (likely(r.err == MDBX_SUCCESS))
    r.err = page_check(mc, page);
  if (unlikely(r.err != MDBX_SUCCESS))
    mc->txn->flags |= MDBX_TXN_ERROR;
  return r;
}

static __always_inline pgr_t page_get_inline(const uint16_t ILL, const MDBX_cursor *const mc, const pgno_t pgno,
                                             const txnid_t front) {
  MDBX_txn *const txn = mc->txn;
  tASSERT(txn, front <= txn->front_txnid);

  pgr_t r;
  if (unlikely(pgno >= txn->geo.first_unallocated)) {
    ERROR("page #%" PRIaPGNO " beyond next-pgno", pgno);
    r.page = nullptr;
    r.err = MDBX_PAGE_NOTFOUND;
  bailout:
    txn->flags |= MDBX_TXN_ERROR;
    return r;
  }

  eASSERT(txn->env, ((txn->flags ^ txn->env->flags) & MDBX_WRITEMAP) == 0);
  r.page = pgno2page(txn->env, pgno);
  if ((txn->flags & (MDBX_TXN_RDONLY | MDBX_WRITEMAP)) == 0) {
    const MDBX_txn *spiller = txn;
    do {
      /* Spilled pages were dirtied in this txn and flushed
       * because the dirty list got full. Bring this page
       * back in from the map (but don't unspill it here,
       * leave that unless page_touch happens again). */
      if (unlikely(spiller->flags & MDBX_TXN_SPILLS) && spill_search(spiller, pgno))
        break;

      const size_t i = dpl_search(spiller, pgno);
      tASSERT(txn, (intptr_t)i > 0);
      if (spiller->tw.dirtylist->items[i].pgno == pgno) {
        r.page = spiller->tw.dirtylist->items[i].ptr;
        break;
      }

      spiller = spiller->parent;
    } while (unlikely(spiller));
  }

  if (unlikely(r.page->pgno != pgno)) {
    r.err = bad_page(r.page, "pgno mismatch (%" PRIaPGNO ") != expected (%" PRIaPGNO ")\n", r.page->pgno, pgno);
    goto bailout;
  }

  if (unlikely(mc->checking & z_pagecheck))
    return check_page_complete(ILL, r.page, mc, front);

#if MDBX_DISABLE_VALIDATION
  r.err = MDBX_SUCCESS;
#else
  r.err = check_page_header(ILL, r.page, txn, front);
  if (unlikely(r.err != MDBX_SUCCESS))
    goto bailout;
#endif /* MDBX_DISABLE_VALIDATION */
  return r;
}

pgr_t page_get_any(const MDBX_cursor *const mc, const pgno_t pgno, const txnid_t front) {
  return page_get_inline(P_ILL_BITS, mc, pgno, front);
}

__hot pgr_t page_get_three(const MDBX_cursor *const mc, const pgno_t pgno, const txnid_t front) {
  return page_get_inline(P_ILL_BITS | P_LARGE, mc, pgno, front);
}

pgr_t page_get_large(const MDBX_cursor *const mc, const pgno_t pgno, const txnid_t front) {
  return page_get_inline(P_ILL_BITS | P_BRANCH | P_LEAF | P_DUPFIX, mc, pgno, front);
}
