/* This file is part of the libmdbx amalgamated source code (v0.14.1-473-g79d02de6 at 2026-03-19T02:14:19+03:00).
 *
 * libmdbx (aka MDBX) is an extremely fast, compact, powerful, embeddedable, transactional key-value storage engine with
 * open-source code. MDBX has a specific set of properties and capabilities, focused on creating unique lightweight
 * solutions.  Please visit https://libmdbx.dqdkfa.ru for more information, changelog, documentation, C++ API description
 * and links to the original git repo with the source code.  Questions, feedback and suggestions are welcome to the
 * Telegram' group https://t.me/libmdbx.
 *
 * The libmdbx code will forever remain open and with high-quality free support, as far as the life circumstances of the
 * project participants allow. Donations are welcome to ETH `0xD104d8f8B2dC312aaD74899F83EBf3EEBDC1EA3A`,
 * BTC `bc1qzvl9uegf2ea6cwlytnanrscyv8snwsvrc0xfsu`, SOL `FTCTgbHajoLVZGr8aEFWMzx3NDMyS5wXJgfeMTmJznRi`.
 * Всё будет хорошо!
 *
 * For ease of use and to eliminate potential limitations in both distribution and obstacles in technology development,
 * libmdbx is distributed as an amalgamated source code starting at the end of 2025.  The source code of the tests, as
 * well as the internal documentation, will be available only to the team directly involved in the development.
 *
 * Copyright 2015-2026 Леонид Юрьев aka Leonid Yuriev <leo@yuriev.ru>
 * SPDX-License-Identifier: Apache-2.0
 *
 * For notes about the license change, credits and acknowledgments, please refer to the COPYRIGHT file. */

/* clang-format off */


#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <mdbx.h>
#include <fuzz.h>

static int      mkdir_p(const char *path)
{
    int         err = 0;
    struct stat st;

    if (stat(path, &st) == 0) {
        if (!S_ISDIR(st.st_mode)) {
            fprintf(stderr, "mkdir_p: %s exists but is not a directory\n", path);
            err = -1;
        }
    } else {
        if (mkdir(path, 0755) != 0) {
            fprintf(stderr, "mkdir_p(mkdir): %s: %m\n", path);
            err = -1;
        }
    }

    return (err);
}

static int      remove_if_exists(const char *path)
{
    int         err = 0;

    if (unlink(path) != 0) {
        if (errno != ENOENT) {
            fprintf(stderr, "unlink: %s: %m\n", path);
            err = -1;
        }
    }

    return (err);
}

static int          write_file(const char *path, const void *buf, size_t len)
{
    int             fd = -1;
    int             err = -1;
    ssize_t         n = 0;
    size_t          off = 0;
    const uint8_t   *p = (const uint8_t *)buf;

    if ((fd = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0644)) >= 0) {
        while (off < len) {
            if ((n = write(fd, p + off, len - off)) < 0) {
                fprintf(stderr, "write_file(write): %s: %m\n", path);
            } else {
                off += (size_t)n;
            }
            if (n < 0)
                break;
        }
        if (off == len)
            err = 0;
        if (close(fd) != 0)
            fprintf(stderr, "write_file(close): %s: %m\n", path);
    } else {
        fprintf(stderr, "write_file(open): %s: %m\n", path);
    }

    return (err);
}

static int      read_file_open_and_stat(const char *path, int *fd, size_t *len)
{
    int         err = -1;
    struct stat st = {};

    *fd = -1;
    *len = 0;
    if ((*fd = open(path, O_RDONLY)) > -1) {
        if (fstat(*fd, &st) == 0) {
            *len = (size_t)st.st_size;
            err = 0;
        } else {
            fprintf(stderr, "read_file(stat): %s: %m\n", path);
            if (close(*fd) != 0)
                fprintf(stderr, "read_file(close): %s: %m\n", path);
            *fd = -1;
        }
    } else {
        fprintf(stderr, "read_file(open): %s: %m\n", path);
    }

    return (err);
}

static int      read_file_alloc_buffer(size_t len, uint8_t **buf)
{
    int         err = -1;

    *buf = NULL;
    if (len > 0) {
        if ((*buf = (uint8_t *)malloc(len)) != NULL) {
            err = 0;
        } else {
            perror("malloc");
        }
    } else {
        err = 0;
    }

    return (err);
}

static int      read_file_fill_buffer(const char *path, int fd, uint8_t *buf,
                                      size_t len, size_t *out_len)
{
    int         err = -1;
    size_t      off = 0;
    ssize_t     n = 0;

    *out_len = 0;
    if (len == 0) {
        err = 0;
    } else {
        while (off < len) {
            if ((n = read(fd, buf + off, len - off)) < 0) {
                fprintf(stderr, "read_file(read): %s: %m\n", path);
            } else {
                if (n == 0) {
                    break;
                } else {
                    off += (size_t)n;
                }
            }

            if (n <= 0)
                break;
        }
        if (off == len) {
            *out_len = len;
            err = 0;
        }
    }

    return (err);
}

static uint8_t  *read_file(const char *path, size_t *out_len)
{
    uint8_t     *buf = NULL;
    int         fd = -1;
    size_t      len = 0;
    size_t      got = 0;

    *out_len = 0;
    if (read_file_open_and_stat(path, &fd, &len) == 0) {
        if (read_file_alloc_buffer(len, &buf) == 0) {
            if (len == 0) {
                *out_len = 0;
            } else {
                if (read_file_fill_buffer(path, fd, buf, len, &got) == 0) {
                    *out_len = got;
                } else {
                    free(buf);
                    buf = NULL;
                }
            }
        }
        if (fd > -1) {
            if (close(fd) != 0)
                fprintf(stderr, "read_file(close): %s: %m\n", path);
        }
    }

    return (buf);
}

static int      write_wrapped_seed(const char *dst, uint8_t dbi_mode,
                                   const uint8_t *db_bytes, size_t db_len)
{
    int         err = -1;
    uint8_t     *buf = NULL;

    if ((buf = (uint8_t *)malloc(db_len + 1)) != NULL) {
        buf[0] = dbi_mode;
        if (db_len > 0)
            memcpy(buf + 1, db_bytes, db_len);
        err = write_file(dst, buf, db_len + 1);
        free(buf);
    } else {
        perror("malloc");
    }

    return (err);
}

static int      put_kv(MDBX_txn *txn, MDBX_dbi dbi, const void *k, size_t klen,
                       const void *v, size_t vlen, MDBX_put_flags_t put_flags)
{
    int         err = 0;
    MDBX_val    key = { .iov_base = (void *)k, .iov_len = klen };
    MDBX_val    val = { .iov_base = (void *)v, .iov_len = vlen };

    if ((err = mdbx_put(txn, dbi, &key, &val, put_flags)) != MDBX_SUCCESS) {
        fprintf(stderr, "put_kv(mdbx_put): %s\n", mdbx_strerror(err));
        err = -1;
    } else {
        err = 0;
    }

    return (err);
}

static int      populate_mode(MDBX_txn *txn, MDBX_dbi dbi, uint8_t mode)
{
    int         err = 0;

    switch (mode)
    {
        case 0:
        {
            for (int i = 0; i < 16; ++i) {
                char k[32], v[32];

                snprintf(k, sizeof(k), "k%04d", i);
                snprintf(v, sizeof(v), "v%04d", i);
                if (put_kv(txn, dbi, k, strlen(k), v, strlen(v), 0) != 0)
                    err = -1;
            }
            break;
        }
        case 1:
        {
            for (int i = 0; i < 8; ++i) {
                char k[32], v[32];

                snprintf(k, sizeof(k), "named-key-%02d", i);
                snprintf(v, sizeof(v), "named-val-%02d", i);
                if (put_kv(txn, dbi, k, strlen(k), v, strlen(v), 0) != 0)
                    err = -1;
            }
            break;
        }
        case 2:
        {
            const char *keys[] = { "abc", "abcd", "za", "zz", "zzz", "yx" };

            for (size_t i = 0; i < sizeof(keys) / sizeof(keys[0]); ++i) {
                char v[32];

                snprintf(v, sizeof(v), "rev-val-%zu", i);
                if (put_kv(txn, dbi, keys[i], strlen(keys[i]), v, strlen(v), 0) != 0)
                    err = -1;
            }
            break;
        }
        case 3:
        {
            if (put_kv(txn, dbi, "dupkey", 6, "one", 3, 0) != 0)
                err = -1;
            if (put_kv(txn, dbi, "dupkey", 6, "two", 3, 0) != 0)
                err = -1;
            if (put_kv(txn, dbi, "dupkey", 6, "three", 5, 0) != 0)
                err = -1;
            if (put_kv(txn, dbi, "other", 5, "x", 1, 0) != 0)
                err = -1;
            break;
        }
        case 4:
        {
            if (put_kv(txn, dbi, "dupkey", 6, "AAAA", 4, 0) != 0)
                err = -1;
            if (put_kv(txn, dbi, "dupkey", 6, "BBBB", 4, 0) != 0)
                err = -1;
            if (put_kv(txn, dbi, "dupkey", 6, "CCCC", 4, 0) != 0)
                err = -1;
            if (put_kv(txn, dbi, "other", 5, "DDDD", 4, 0) != 0)
                err = -1;
            break;
        }
        case 5:
        {
            for (uint32_t i = 1; i <= 8; ++i) {
                char v[32];

                snprintf(v, sizeof(v), "ival-%u", i);
                if (put_kv(txn, dbi, &i, sizeof(i), v, strlen(v), 0) != 0)
                    err = -1;
            }
            break;
        }
        case 6:
        {
            const char *k = "dupint";
            uint32_t vals[] = { 10, 20, 30, 40 };

            for (size_t i = 0; i < sizeof(vals) / sizeof(vals[0]); ++i) {
                if (put_kv(txn, dbi, k, strlen(k), &vals[i], sizeof(vals[i]), 0) != 0)
                    err = -1;
            }
            break;
        }
        case 7:
        {
            const char *k = "duprev";

            if (put_kv(txn, dbi, k, strlen(k), "abc", 3, 0) != 0)
                err = -1;
            if (put_kv(txn, dbi, k, strlen(k), "bcd", 3, 0) != 0)
                err = -1;
            if (put_kv(txn, dbi, k, strlen(k), "zzz", 3, 0) != 0)
                err = -1;
            break;
        }
        default:
        {
            fprintf(stderr, "unknown mode %u\n", mode);
            err = -1;
            break;
        }
    }

    return (err);
}

static int      build_seed_db_open_env(const char *dbfile, MDBX_env **env)
{
    int         rc = MDBX_SUCCESS;
    int         err = -1;

    if ((rc = mdbx_env_create(env)) == MDBX_SUCCESS) {
        (void)mdbx_env_set_maxdbs(*env, 16);
        if ((rc = mdbx_env_open(*env, dbfile, MDBX_NOSUBDIR, 0644))
            == MDBX_SUCCESS)
            err = 0;
        else
            fprintf(stderr, "mdbx_env_open: %s\n", mdbx_strerror(rc));
    } else {
        fprintf(stderr, "mdbx_env_create: %s\n", mdbx_strerror(rc));
    }

    return (err);
}

static int          build_seed_db_fill(const struct dbi_mode_desc *m,
                                       MDBX_env *env, uint8_t mode)
{
    MDBX_txn        *txn = NULL;
    MDBX_dbi        dbi = 0;
    MDBX_db_flags_t create_flags = 0;
    int             rc = MDBX_SUCCESS;
    int             err = -1;

    if ((rc = mdbx_txn_begin(env, NULL, 0, &txn)) == MDBX_SUCCESS) {
        create_flags = m->flags;
        if (m->dbi_name != NULL) {
            printf("%s\n", m->dbi_name);
            create_flags |= MDBX_CREATE;
        }
        if ((rc = mdbx_dbi_open(txn, m->dbi_name, create_flags, &dbi))
            == MDBX_SUCCESS) {
            if (populate_mode(txn, dbi, mode) == 0) {
                if ((rc = mdbx_txn_commit(txn)) == MDBX_SUCCESS) {
                    txn = NULL;
                    err = 0;
                } else {
                    fprintf(stderr, "mdbx_txn_commit: %s\n", mdbx_strerror(rc));
                }
            } else {
                fprintf(stderr, "populate_mode: %u\n", mode);
            }
        } else {
            fprintf(stderr, "mdbx_dbi_open: %s\n", mdbx_strerror(rc));
        }
        mdbx_txn_abort(txn);
    } else {
        fprintf(stderr, "mdbx_txn_begin: %s\n", mdbx_strerror(rc));
    }

    return (err);
}

static int      build_seed_db_wrap_output(const char *out_path,
                                          const char *dbfile, uint8_t mode)
{
    int         err = -1;
    size_t      db_len = 0;
    uint8_t     *db_buf = NULL;

    if ((db_buf = read_file(dbfile, &db_len)) != NULL || db_len == 0) {
        if (write_wrapped_seed(out_path, mode, db_buf, db_len) == 0)
            err = 0;
        else
            fprintf(stderr, "write_wrapped_seed: %s\n", out_path);
        free(db_buf);
    } else {
        fprintf(stderr, "read_file: %s\n", dbfile);
    }

    return (err);
}

static int      build_seed_db_cleanup(const char *lockfile, const char *dbfile,
                                      const char *workdir)
{
    int         err = 0;

    if (remove_if_exists(lockfile) != 0)
        err = -1;
    if (remove_if_exists(dbfile) != 0)
        err = -1;
    if (rmdir(workdir) != 0) {
        fprintf(stderr, "rmdir(workdir): %m\n");
        err = -1;
    }

    return (err);
}

static int      build_seed_db(const char *out_path, uint8_t mode)
{
    char        workdir[] = "/tmp/libmdbx-seed-XXXXXX";
    char        dbfile[PATH_MAX];
    char        lockfile[PATH_MAX];
    MDBX_env    *env = NULL;
    int         err = -1;
    const dbi_mode_desc_t   *m = NULL;

    if (mkdtemp(workdir) != NULL) {
        snprintf(dbfile, sizeof(dbfile), "%s/mdbx.dat", workdir);
        snprintf(lockfile, sizeof(lockfile), "%s/mdbx.lck", workdir);

        if (build_seed_db_open_env(dbfile, &env) == 0) {
            m = &g_modes[mode];
            if (build_seed_db_fill(m, env, mode) == 0) {
                if (build_seed_db_wrap_output(out_path, dbfile, mode) == 0)
                    err = 0;
            }
            mdbx_env_close(env);
        }
        if (build_seed_db_cleanup(lockfile, dbfile, workdir) != 0)
            err = -1;
    } else {
        fprintf(stderr, "mkdtemp: %m\n");
    }

    return (err);
}

static int      write_truncated_wrapped_copy(const char *src, const char *dst,
                                             size_t body_n)
{
    int         err = -1;
    size_t      len = 0;
    uint8_t     *buf = NULL;

    if ((buf = read_file(src, &len)) != NULL) {
        if (len > 0) {
            size_t body_len = len - 1;

            if (body_n > body_len)
                body_n = body_len;
            if (write_file(dst, buf, body_n + 1) == 0)
                err = 0;
        } else {
            if (write_file(dst, "", 0) == 0)
                err = 0;
        }
        free(buf);
    } else {
        if (len == 0) {
            if (write_file(dst, "", 0) == 0)
                err = 0;
        }
    }

    return (err);
}

static int      write_bitflip_wrapped_copy(const char *src, const char *dst,
                                           size_t body_offset, uint8_t mask)
{
    int         err = -1;
    size_t      len = 0;
    uint8_t     *buf = NULL;

    if ((buf = read_file(src, &len)) != NULL) {
        if (len > 1) {
            size_t body_len = len - 1;

            if (body_offset >= body_len)
                body_offset = body_len / 2;
            buf[1 + body_offset] ^= mask;
        }
        if (write_file(dst, buf, len) == 0)
            err = 0;
        free(buf);
    } else {
        if (write_file(dst, buf, len) == 0)
            err = 0;
    }

    return (err);
}

int             main(int argc, char **argv)
{
    int         err = 0;
    const char  *outdir = (argc >= 2) ? argv[1] : "corpus";
    char        src[PATH_MAX] = {};
    char        dst[PATH_MAX] = {};
    uint8_t     zero_seed[1 + 4096] = {};
    uint8_t     empty_seed[1] = {};

    if (mkdir_p(outdir) == 0) {
        for (uint8_t mode = 0; mode < 8; ++mode) {
            char path[PATH_MAX];

            snprintf(path, sizeof(path), "%s/%s", outdir, g_modes[mode].seed_name);
            if (build_seed_db(path, mode) != 0)
                err = 1;
        }

        snprintf(src, sizeof(src), "%s/mode0_default", outdir);

        snprintf(dst, sizeof(dst), "%s/mode0_trunc_64", outdir);
        if (write_truncated_wrapped_copy(src, dst, 64) != 0)
            err = 1;

        snprintf(dst, sizeof(dst), "%s/mode0_trunc_512", outdir);
        if (write_truncated_wrapped_copy(src, dst, 512) != 0)
            err = 1;

        snprintf(dst, sizeof(dst), "%s/mode0_bitflip_start", outdir);
        if (write_bitflip_wrapped_copy(src, dst, 8, 0x01) != 0)
            err = 1;

        snprintf(src, sizeof(src), "%s/mode3_dup", outdir);
        snprintf(dst, sizeof(dst), "%s/mode3_bitflip", outdir);
        if (write_bitflip_wrapped_copy(src, dst, 32, 0x80) != 0)
            err = 1;

        snprintf(src, sizeof(src), "%s/mode5_intkey", outdir);
        snprintf(dst, sizeof(dst), "%s/mode5_bitflip", outdir);
        if (write_bitflip_wrapped_copy(src, dst, 32, 0x04) != 0)
            err = 1;

        snprintf(src, sizeof(src), "%s/mode6_intdup", outdir);
        snprintf(dst, sizeof(dst), "%s/mode6_trunc_128", outdir);
        if (write_truncated_wrapped_copy(src, dst, 128) != 0)
            err = 1;
            
        snprintf(dst, sizeof(dst), "%s/mode0_zeros_4096", outdir);
        if (write_file(dst, zero_seed, sizeof(zero_seed)) != 0)
            err = 1;

        snprintf(dst, sizeof(dst), "%s/mode0_empty_body", outdir);
        if (write_file(dst, empty_seed, sizeof(empty_seed)) != 0)
            err = 1;

        if (err == 0)
            printf("Seed corpus written to %s\n", outdir);
    } else {
        err = 1;
    }

    return (err);
}
