/**** BRIEFLY ******************************************************************
 *
 * libmdbx is superior to LMDB (https://bit.ly/26ts7tL) in terms of features
 * and reliability, not inferior in performance. In comparison to LMDB, libmdbx
 * makes many things just work perfectly, not silently and catastrophically
 * break down. libmdbx supports Linux, Windows, MacOS, FreeBSD, DragonFly,
 * Solaris, OpenSolaris, OpenIndiana, NetBSD, OpenBSD and other systems
 * compliant with POSIX.1-2008.
 *
 * Look below for API description, for other information (build, embedding and
 * amalgamation, improvements over LMDB, benchmarking, etc) please refer to
 * README.md at https://abf.io/erthink/libmdbx.
 *
 *  ---
 *
 * The next version is under active non-public development and will be released
 * as MithrilDB and libmithrildb for libraries & packages. Admittedly mythical
 * Mithril is resembling silver but being stronger and lighter than steel.
 * Therefore MithrilDB is rightly relevant name.
 *
 * MithrilDB will be radically different from libmdbx by the new database format
 * and API based on C++17, as well as the Apache 2.0 License. The goal of this
 * revolution is to provide a clearer and robust API, add more features and new
 * valuable properties of database.
 *
 * The Future will (be) Positive. Всё будет хорошо.
 *
 *
 **** INTRODUCTION *************************************************************
 *
 *   // For the most part, this section is a copy of the corresponding text
 *   // from LMDB description, but with some edits reflecting the improvements
 *   // and enhancements were made in MDBX.
 *
 * MDBX is a Btree-based database management library modeled loosely on the
 * BerkeleyDB API, but much simplified. The entire database (aka "environment")
 * is exposed in a memory map, and all data fetches return data directly from
 * the mapped memory, so no malloc's or memcpy's occur during data fetches.
 * As such, the library is extremely simple because it requires no page caching
 * layer of its own, and it is extremely high performance and memory-efficient.
 * It is also fully transactional with full ACID semantics, and when the memory
 * map is read-only, the database integrity cannot be corrupted by stray pointer
 * writes from application code.
 *
 * The library is fully thread-aware and supports concurrent read/write access
 * from multiple processes and threads. Data pages use a copy-on-write strategy
 * so no active data pages are ever overwritten, which also provides resistance
 * to corruption and eliminates the need of any special recovery procedures
 * after a system crash. Writes are fully serialized; only one write transaction
 * may be active at a time, which guarantees that writers can never deadlock.
 * The database structure is multi-versioned so readers run with no locks;
 * writers cannot block readers, and readers don't block writers.
 *
 * Unlike other well-known database mechanisms which use either write-ahead
 * transaction logs or append-only data writes, MDBX requires no maintenance
 * during operation. Both write-ahead loggers and append-only databases require
 * periodic checkpointing and/or compaction of their log or database files
 * otherwise they grow without bound. MDBX tracks retired/freed pages within the
 * database and re-uses them for new write operations, so the database size does
 * not grow without bound in normal use. It is worth noting that the "next"
 * version libmdbx (MithrilDB) will solve this problem.
 *
 * The memory map can be used as a read-only or read-write map. It is read-only
 * by default as this provides total immunity to corruption. Using read-write
 * mode offers much higher write performance, but adds the possibility for stray
 * application writes thru pointers to silently corrupt the database.
 * Of course if your application code is known to be bug-free (...) then this is
 * not an issue.
 *
 * If this is your first time using a transactional embedded key-value store,
 * you may find the "GETTING STARTED" section below to be helpful.
 *
 *
 **** GETTING STARTED **********************************************************
 *
 *   // This section is based on Bert Hubert's intro "LMDB Semantics", with
 *   // edits reflecting the improvements and enhancements were made in MDBX.
 *   // See https://bit.ly/2maejGY for Bert Hubert's original.
 *
 * Everything starts with an environment, created by mdbx_env_create().
 * Once created, this environment must also be opened with mdbx_env_open(),
 * and after use be closed by mdbx_env_close(). At that a non-zero value of the
 * last argument "mode" supposes MDBX will create database and directory if ones
 * does not exist. In this case the non-zero "mode" argument specifies the file
 * mode bits be applied when a new files are created by open() function.
 *
 * Within that directory, a lock file (aka LCK-file) and a storage file (aka
 * DXB-file) will be generated. If you don't want to use a directory, you can
 * pass the MDBX_NOSUBDIR option, in which case the path you provided is used
 * directly as the DXB-file, and another file with a "-lck" suffix added
 * will be used for the LCK-file.
 *
 * Once the environment is open, a transaction can be created within it using
 * mdbx_txn_begin(). Transactions may be read-write or read-only, and read-write
 * transactions may be nested. A transaction must only be used by one thread at
 * a time. Transactions are always required, even for read-only access. The
 * transaction provides a consistent view of the data.
 *
 * Once a transaction has been created, a database (i.e. key-value space inside
 * the environment) can be opened within it using mdbx_dbi_open(). If only one
 * database will ever be used in the environment, a NULL can be passed as the
 * database name. For named databases, the MDBX_CREATE flag must be used to
 * create the database if it doesn't already exist. Also, mdbx_env_set_maxdbs()
 * must be called after mdbx_env_create() and before mdbx_env_open() to set the
 * maximum number of named databases you want to support.
 *
 * NOTE: a single transaction can open multiple databases. Generally databases
 *       should only be opened once, by the first transaction in the process.
 *
 * Within a transaction, mdbx_get() and mdbx_put() can store single key-value
 * pairs if that is all you need to do (but see CURSORS below if you want to do
 * more).
 *
 * A key-value pair is expressed as two MDBX_val structures. This struct that is
 * exactly similar to POSIX's struct iovec and has two fields, iov_len and
 * iov_base. The data is a void pointer to an array of iov_len bytes.
 * (!) The notable difference between MDBX and LMDB is that MDBX support zero
 * length keys.
 *
 * Because MDBX is very efficient (and usually zero-copy), the data returned in
 * an MDBX_val structure may be memory-mapped straight from disk. In other words
 * look but do not touch (or free() for that matter). Once a transaction is
 * closed, the values can no longer be used, so make a copy if you need to keep
 * them after that.
 *
 *
 * CURSORS -- To do more powerful things, we must use a cursor.
 *
 * Within the transaction, a cursor can be created with mdbx_cursor_open().
 * With this cursor we can store/retrieve/delete (multiple) values using
 * mdbx_cursor_get(), mdbx_cursor_put(), and mdbx_cursor_del().
 *
 * mdbx_cursor_get() positions itself depending on the cursor operation
 * requested, and for some operations, on the supplied key. For example, to list
 * all key-value pairs in a database, use operation MDBX_FIRST for the first
 * call to mdbx_cursor_get(), and MDBX_NEXT on subsequent calls, until the end
 * is hit.
 *
 * To retrieve all keys starting from a specified key value, use MDBX_SET. For
 * more cursor operations, see the API description below.
 *
 * When using mdbx_cursor_put(), either the function will position the cursor
 * for you based on the key, or you can use operation MDBX_CURRENT to use the
 * current position of the cursor. NOTE that key must then match the current
 * position's key.
 *
 *
 * SUMMARIZING THE OPENING
 *
 * So we have a cursor in a transaction which opened a database in an
 * environment which is opened from a filesystem after it was separately
 * created.
 *
 * Or, we create an environment, open it from a filesystem, create a transaction
 * within it, open a database within that transaction, and create a cursor
 * within all of the above.
 *
 * Got it?
 *
 *
 * THREADS AND PROCESSES
 *
 * Do not have open an database twice in the same process at the same time, MDBX
 * will track and prevent this. Instead, share the MDBX environment that has
 * opened the file across all threads. The reason for this is:
 *  - When the "Open file description" locks (aka OFD-locks) are not available,
 *    MDBX uses POSIX locks on files, and these locks have issues if one process
 *    opens a file multiple times.
 *  - If a single process opens the same environment multiple times, closing it
 *    once will remove all the locks held on it, and the other instances will be
 *    vulnerable to corruption from other processes.
 *  + For compatibility with LMDB which allows multi-opening, MDBX can be
 *    configured at runtime by mdbx_setup_debug(MDBX_DBG_LEGACY_MULTIOPEN, ...)
 *    prior to calling other MDBX funcitons. In this way MDBX will track
 *    databases opening, detect multi-opening cases and then recover POSIX file
 *    locks as necessary. However, lock recovery can cause unexpected pauses,
 *    such as when another process opened the database in exclusive mode before
 *    the lock was restored - we have to wait until such a process releases the
 *    database, and so on.
 *
 * Do not use opened MDBX environment(s) after fork() in a child process(es),
 * MDBX will check and prevent this at critical points. Instead, ensure there is
 * no open MDBX-instance(s) during fork(), or atleast close it immediately after
 * fork() in the child process and reopen if required - for instance by using
 * pthread_atfork(). The reason for this is:
 *  - For competitive consistent reading, MDBX assigns a slot in the shared
 *    table for each process that interacts with the database. This slot is
 *    populated with process attributes, including the PID.
 *  - After fork(), in order to remain connected to a database, the child
 *    process must have its own such "slot", which can't be assigned in any
 *    simple and robust way another than the regular.
 *  - A write transaction from a parent process cannot continue in a child
 *    process for obvious reasons.
 *  - Moreover, in a multithreaded process at the fork() moment any number of
 *    threads could run in critical and/or intermediate sections of MDBX code
 *    with interaction and/or racing conditions with threads from other
 *    process(es). For instance: shrinking a database or copying it to a pipe,
 *    opening or closing environment, begining or finishing a transaction,
 *    and so on.
 *  = Therefore, any solution other than simply close database (and reopen if
 *    necessary) in a child process would be both extreme complicated and so
 *    fragile.
 *
 * Do not start more than one transaction for a one thread. If you think about
 * this, it's really strange to do something with two data snapshots at once,
 * which may be different. MDBX checks and preventing this by returning
 * corresponding error code (MDBX_TXN_OVERLAPPING, MDBX_BAD_RSLOT, MDBX_BUSY)
 * unless you using MDBX_NOTLS option on the environment. Nonetheless, with the
 * MDBX_NOTLS option, you must know exactly what you are doing, otherwise you
 * will get deadlocks or reading an alien data.
 *
 * Also note that a transaction is tied to one thread by default using Thread
 * Local Storage. If you want to pass read-only transactions across threads,
 * you can use the MDBX_NOTLS option on the environment. Nevertheless, a write
 * transaction entirely should only be used in one thread from start to finish.
 * MDBX checks this in a reasonable manner and return the MDBX_THREAD_MISMATCH
 * error in rules violation.
 *
 *
 * TRANSACTIONS, ROLLBACKS, etc.
 *
 * To actually get anything done, a transaction must be committed using
 * mdbx_txn_commit(). Alternatively, all of a transaction's operations
 * can be discarded using mdbx_txn_abort().
 *
 * (!) An important difference between MDBX and LMDB is that MDBX required that
 * any opened cursors can be reused and must be freed explicitly, regardless
 * ones was opened in a read-only or write transaction. The REASON for this is
 * eliminates ambiguity which helps to avoid errors such as: use-after-free,
 * double-free, i.e. memory corruption and segfaults.
 *
 * For read-only transactions, obviously there is nothing to commit to storage.
 * (!) An another notable difference between MDBX and LMDB is that MDBX make
 * handles opened for existing databases immediately available for other
 * transactions, regardless this transaction will be aborted or reset. The
 * REASON for this is to avoiding the requirement for multiple opening a same
 * handles in concurrent read transactions, and tracking of such open but hidden
 * handles until the completion of read transactions which opened them.
 *
 * In addition, as long as a transaction is open, a consistent view of the
 * database is kept alive, which requires storage. A read-only transaction that
 * no longer requires this consistent view should be terminated (committed or
 * aborted) when the view is no longer needed (but see below for an
 * optimization).
 *
 * There can be multiple simultaneously active read-only transactions but only
 * one that can write. Once a single read-write transaction is opened, all
 * further attempts to begin one will block until the first one is committed or
 * aborted. This has no effect on read-only transactions, however, and they may
 * continue to be opened at any time.
 *
 *
 * DUPLICATE KEYS
 *
 * mdbx_get() and mdbx_put() respectively have no and only some support or
 * multiple key-value pairs with identical keys. If there are multiple values
 * for a key, mdbx_get() will only return the first value.
 *
 * When multiple values for one key are required, pass the MDBX_DUPSORT flag to
 * mdbx_dbi_open(). In an MDBX_DUPSORT database, by default mdbx_put() will not
 * replace the value for a key if the key existed already. Instead it will add
 * the new value to the key. In addition, mdbx_del() will pay attention to the
 * value field too, allowing for specific values of a key to be deleted.
 *
 * Finally, additional cursor operations become available for traversing through
 * and retrieving duplicate values.
 *
 *
 * SOME OPTIMIZATION
 *
 * If you frequently begin and abort read-only transactions, as an optimization,
 * it is possible to only reset and renew a transaction.
 *
 * mdbx_txn_reset() releases any old copies of data kept around for a read-only
 * transaction. To reuse this reset transaction, call mdbx_txn_renew() on it.
 * Any cursors in this transaction can also be renewed using mdbx_cursor_renew()
 * or freed by mdbx_cursor_close().
 *
 * To permanently free a transaction, reset or not, use mdbx_txn_abort().
 *
 *
 * CLEANING UP
 *
 * Any created cursors must be closed using mdbx_cursor_close(). It is advisable
 * to repeat:
 * (!) An important difference between MDBX and LMDB is that MDBX required that
 * any opened cursors can be reused and must be freed explicitly, regardless
 * ones was opened in a read-only or write transaction. The REASON for this is
 * eliminates ambiguity which helps to avoid errors such as: use-after-free,
 * double-free, i.e. memory corruption and segfaults.
 *
 * It is very rarely necessary to close a database handle, and in general they
 * should just be left open. When you close a handle, it immediately becomes
 * unavailable for all transactions in the environment. Therefore, you should
 * avoid closing the handle while at least one transaction is using it.
 *
 *
 * THE FULL API
 *
 *   The full MDBX documentation lists further details below,
 *   like how to:
 *
 *     - configure database size and automatic size management
 *     - drop and clean a database
 *     - detect and report errors
 *     - optimize (bulk) loading speed
 *     - (temporarily) reduce robustness to gain even more speed
 *     - gather statistics about the database
 *     - estimate size of range query result
 *     - double perfomance by LIFO reclaiming on storages with write-back
 *     - use sequences and canary markers
 *     - use lack-of-space callback (aka OOM-KICK)
 *     - use exclusive mode
 *     - define custom sort orders (but this is recommended to be avoided)
 *
 *
 **** RESTRICTIONS & CAVEATS ***************************************************
 *    in addition to those listed for some functions.
 *
 *  - Troubleshooting the LCK-file.
 *      1. A broken LCK-file can cause sync issues, including appearance of
 *         wrong/inconsistent data for readers. When database opened in the
 *         cooperative read-write mode the LCK-file requires to be mapped to
 *         memory in read-write access. In this case it is always possible for
 *         stray/malfunctioned application could writes thru pointers to
 *         silently corrupt the LCK-file.
 *
 *         Unfortunately, there is no any portable way to prevent such
 *         corruption, since the LCK-file is updated concurrently by
 *         multiple processes in a lock-free manner and any locking is
 *         unwise due to a large overhead.
 *
 *         The "next" version of libmdbx (MithrilDB) will solve this issue.
 *
 *         Workaround: Just make all programs using the database close it;
 *                     the LCK-file is always reset on first open.
 *
 *      2. Stale reader transactions left behind by an aborted program cause
 *         further writes to grow the database quickly, and stale locks can
 *         block further operation.
 *         MDBX checks for stale readers while opening environment and before
 *         growth the database. But in some cases, this may not be enough.
 *
 *         Workaround: Check for stale readers periodically, using the
 *                     mdbx_reader_check() function or the mdbx_stat tool.
 *
 *      3. Stale writers will be cleared automatically by MDBX on supprted
 *         platforms. But this is platform-specific, especially of
 *         implementation of shared POSIX-mutexes and support for robust
 *         mutexes. For instance there are no known issues on Linux, OSX,
 *         Windows and FreeBSD.
 *
 *         Workaround: Otherwise just make all programs using the database
 *                     close it; the LCK-file is always reset on first open
 *                     of the environment.
 *
 *  - Do not use MDBX databases on remote filesystems, even between processes
 *    on the same host. This breaks file locks on some platforms, possibly
 *    memory map sync, and certainly sync between programs on different hosts.
 *
 *    On the other hand, MDBX support the exclusive database operation over
 *    a network, and cooperative read-only access to the database placed on
 *    a read-only network shares.
 *
 *  - Do not use opened MDBX_env instance(s) in a child processes after fork().
 *    It would be insane to call fork() and any MDBX-functions simultaneously
 *    from multiple threads. The best way is to prevent the presence of open
 *    MDBX-instances during fork().
 *
 *    The MDBX_TXN_CHECKPID build-time option, which is ON by default on
 *    non-Windows platforms (i.e. where fork() is available), enables PID
 *    checking at a few critical points. But this does not give any guarantees,
 *    but only allows you to detect such errors a little sooner. Depending on
 *    the platform, you should expect an application crash and/or database
 *    corruption in such cases.
 *
 *    On the other hand, MDBX allow calling mdbx_close_env() in such cases to
 *    release resources, but no more and in general this is a wrong way.
 *
 *  - There is no pure read-only mode in a normal explicitly way, since
 *    readers need write access to LCK-file to be ones visible for writer.
 *    MDBX always tries to open/create LCK-file for read-write, but switches
 *    to without-LCK mode on appropriate errors (EROFS, EACCESS, EPERM)
 *    if the read-only mode was requested by the MDBX_RDONLY flag which is
 *    described below.
 *
 *    The "next" version of libmdbx (MithrilDB) will solve this issue.
 *
 *  - A thread can only use one transaction at a time, plus any nested
 *    read-write transactions in the non-writemap mode. Each transaction
 *    belongs to one thread. The MDBX_NOTLS flag changes this for read-only
 *    transactions. See below.
 *
 *    Do not start more than one transaction for a one thread. If you think
 *    about this, it's really strange to do something with two data snapshots
 *    at once, which may be different. MDBX checks and preventing this by
 *    returning corresponding error code (MDBX_TXN_OVERLAPPING, MDBX_BAD_RSLOT,
 *    MDBX_BUSY) unless you using MDBX_NOTLS option on the environment.
 *    Nonetheless, with the MDBX_NOTLS option, you must know exactly what you
 *    are doing, otherwise you will get deadlocks or reading an alien data.
 *
 *  - Do not have open an MDBX database twice in the same process at the same
 *    time. By default MDBX prevent this in most cases by tracking databases
 *    opening and return MDBX_BUSY if anyone LCK-file is already open.
 *
 *    The reason for this is that when the "Open file description" locks (aka
 *    OFD-locks) are not available, MDBX uses POSIX locks on files, and these
 *    locks have issues if one process opens a file multiple times. If a single
 *    process opens the same environment multiple times, closing it once will
 *    remove all the locks held on it, and the other instances will be
 *    vulnerable to corruption from other processes.
 *
 *    For compatibility with LMDB which allows multi-opening, MDBX can be
 *    configured at runtime by mdbx_setup_debug(MDBX_DBG_LEGACY_MULTIOPEN, ...)
 *    prior to calling other MDBX funcitons. In this way MDBX will track
 *    databases opening, detect multi-opening cases and then recover POSIX file
 *    locks as necessary. However, lock recovery can cause unexpected pauses,
 *    such as when another process opened the database in exclusive mode before
 *    the lock was restored - we have to wait until such a process releases the
 *    database, and so on.
 *
 *  - Avoid long-lived read transactions, especially in the scenarios with a
 *    high rate of write transactions. Long-lived read transactions prevents
 *    recycling pages retired/freed by newer write transactions, thus the
 *    database can grow quickly.
 *
 *    Understanding the problem of long-lived read transactions requires some
 *    explanation, but can be difficult for quick perception. So is is
 *    reasonable to simplify this as follows:
 *      1. Garbage collection problem exists in all databases one way or
 *         another, e.g. VACUUM in PostgreSQL. But in MDBX it's even more
 *         discernible because of high transaction rate and intentional
 *         internals simplification in favor of performance.
 *
 *      2. MDBX employs Multiversion concurrency control on the Copy-on-Write
 *         basis, that allows multiple readers runs in parallel with a write
 *         transaction without blocking. An each write transaction needs free
 *         pages to put the changed data, that pages will be placed in the new
 *         b-tree snapshot at commit. MDBX efficiently recycling pages from
 *         previous created unused snapshots, BUT this is impossible if anyone
 *         a read transaction use such snapshot.
 *
 *      3. Thus massive altering of data during a parallel long read operation
 *         will increase the process's work set and may exhaust entire free
 *         database space.
 *
 *    A good example of long readers is a hot backup to the slow destination
 *    or debugging of a client application while retaining an active read
 *    transaction. LMDB this results in MAP_FULL error and subsequent write
 *    performance degradation.
 *
 *    MDBX mostly solve "long-lived" readers issue by the lack-of-space callback
 *    which allow to aborts long readers, and by the MDBX_LIFORECLAIM mode which
 *    addresses subsequent performance degradation.
 *    The "next" version of libmdbx (MithrilDB) will completely solve this.
 *
 *  - Avoid suspending a process with active transactions. These would then be
 *    "long-lived" as above.
 *
 *    The "next" version of libmdbx (MithrilDB) will solve this issue.
 *
 *  - Avoid aborting a process with an active read-only transaction in scenaries
 *    with high rate of write transactions. The transaction becomes "long-lived"
 *    as above until a check for stale readers is performed or the LCK-file is
 *    reset, since the process may not remove it from the lockfile. This does
 *    not apply to write transactions if the system clears stale writers, see
 *    above.
 *
 *  - An MDBX database configuration will often reserve considerable unused
 *    memory address space and maybe file size for future growth. This does
 *    not use actual memory or disk space, but users may need to understand
 *    the difference so they won't be scared off.
 *
 *  - The Write Amplification Factor.
 *    TBD.
 *
 **** LICENSE AND COPYRUSTING **************************************************
 *
 * Copyright 2015-2020 Leonid Yuriev <leo@yuriev.ru>
 * and other libmdbx authors: please see AUTHORS file.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in the file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 *
 *  ---
 *
 * This code is derived from "LMDB engine" written by
 * Howard Chu (Symas Corporation), which itself derived from btree.c
 * written by Martin Hedenfalk.
 *
 *  ---
 *
 * Portions Copyright 2011-2015 Howard Chu, Symas Corp. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in the file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 *
 *  ---
 *
 * Portions Copyright (c) 2009, 2010 Martin Hedenfalk <martin@bzero.se>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *
 **** ACKNOWLEDGEMENTS *********************************************************
 *
 * Howard Chu (Symas Corporation) - the author of LMDB,
 * from which originated the MDBX in 2015.
 *
 * Martin Hedenfalk <martin@bzero.se> - the author of `btree.c` code,
 * which was used for begin development of LMDB.
 *
 ******************************************************************************/

#pragma once
#ifndef LIBMDBX_H
#define LIBMDBX_H

#ifdef _MSC_VER
#pragma warning(push, 1)
#pragma warning(disable : 4548) /* expression before comma has no effect;      \
                                   expected expression with side - effect */
#pragma warning(disable : 4530) /* C++ exception handler used, but unwind      \
                                 * semantics are not enabled. Specify /EHsc */
#pragma warning(disable : 4577) /* 'noexcept' used with no exception handling  \
                                 * mode specified; termination on exception is \
                                 * not guaranteed. Specify /EHsc */
#endif                          /* _MSC_VER (warnings) */

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>

#if defined(_WIN32) || defined(_WIN64)

#include <windows.h>
#include <winnt.h>
#ifndef __mode_t_defined
typedef unsigned short mode_t;
#endif
typedef HANDLE mdbx_filehandle_t;
typedef DWORD mdbx_pid_t;
typedef DWORD mdbx_tid_t;
#define MDBX_ENODATA ERROR_HANDLE_EOF
#define MDBX_EINVAL ERROR_INVALID_PARAMETER
#define MDBX_EACCESS ERROR_ACCESS_DENIED
#define MDBX_ENOMEM ERROR_OUTOFMEMORY
#define MDBX_EROFS ERROR_FILE_READ_ONLY
#define MDBX_ENOSYS ERROR_NOT_SUPPORTED
#define MDBX_EIO ERROR_WRITE_FAULT
#define MDBX_EPERM ERROR_INVALID_FUNCTION
#define MDBX_EINTR ERROR_CANCELLED
#define MDBX_ENOFILE ERROR_FILE_NOT_FOUND
#define MDBX_EREMOTE ERROR_REMOTE_STORAGE_MEDIA_ERROR

#else

#include <errno.h>     /* for error codes */
#include <pthread.h>   /* for pthread_t */
#include <sys/types.h> /* for pid_t */
#include <sys/uio.h>   /* for truct iovec */
#define HAVE_STRUCT_IOVEC 1
typedef int mdbx_filehandle_t;
typedef pid_t mdbx_pid_t;
typedef pthread_t mdbx_tid_t;
#ifdef ENODATA
#define MDBX_ENODATA ENODATA
#else
#define MDBX_ENODATA -1
#endif
#define MDBX_EINVAL EINVAL
#define MDBX_EACCESS EACCES
#define MDBX_ENOMEM ENOMEM
#define MDBX_EROFS EROFS
#define MDBX_ENOSYS ENOSYS
#define MDBX_EIO EIO
#define MDBX_EPERM EPERM
#define MDBX_EINTR EINTR
#define MDBX_ENOFILE ENOENT
#define MDBX_EREMOTE ENOTBLK

#endif

#ifdef _MSC_VER
#pragma warning(pop)
#endif

/*----------------------------------------------------------------------------*/

#ifndef __has_attribute
#define __has_attribute(x) (0)
#endif /* __has_attribute */

#ifndef __deprecated
#if defined(__GNUC__) || __has_attribute(__deprecated__)
#define __deprecated __attribute__((__deprecated__))
#elif defined(_MSC_VER)
#define __deprecated __declspec(deprecated)
#else
#define __deprecated
#endif
#endif /* __deprecated */

#ifndef __dll_export
#if defined(_WIN32) || defined(__CYGWIN__)
#if defined(__GNUC__) || __has_attribute(__dllexport__)
#define __dll_export __attribute__((__dllexport__))
#elif defined(_MSC_VER)
#define __dll_export __declspec(dllexport)
#else
#define __dll_export
#endif
#elif defined(__GNUC__) || __has_attribute(__visibility__)
#define __dll_export __attribute__((__visibility__("default")))
#else
#define __dll_export
#endif
#endif /* __dll_export */

#ifndef __dll_import
#if defined(_WIN32) || defined(__CYGWIN__)
#if defined(__GNUC__) || __has_attribute(__dllimport__)
#define __dll_import __attribute__((__dllimport__))
#elif defined(_MSC_VER)
#define __dll_import __declspec(dllimport)
#else
#define __dll_import
#endif
#else
#define __dll_import
#endif
#endif /* __dll_import */

/*----------------------------------------------------------------------------*/

/* MDBX version 0.7.0, released 2020-03-18 */
#define MDBX_VERSION_MAJOR 0
#define MDBX_VERSION_MINOR 7

#ifndef LIBMDBX_API
#if defined(LIBMDBX_EXPORTS)
#define LIBMDBX_API __dll_export
#elif defined(LIBMDBX_IMPORTS)
#define LIBMDBX_API __dll_import
#else
#define LIBMDBX_API
#endif
#endif /* LIBMDBX_API */

#ifdef __cplusplus
extern "C" {
#endif

/**** MDBX version information ************************************************/

#if defined(LIBMDBX_IMPORTS)
#define LIBMDBX_VERINFO_API __dll_import
#else
#define LIBMDBX_VERINFO_API __dll_export
#endif /* LIBMDBX_VERINFO_API */

typedef struct mdbx_version_info {
  uint8_t major;
  uint8_t minor;
  uint16_t release;
  uint32_t revision;
  struct /* source info from git */ {
    const char *datetime /* committer date, strict ISO-8601 format */;
    const char *tree /* commit hash (hexadecimal digits) */;
    const char *commit /* tree hash, i.e. digest of the source code */;
    const char *describe /* git-describe string */;
  } git;
  const char *sourcery /* sourcery anchor for pinning */;
} mdbx_version_info;
extern LIBMDBX_VERINFO_API const mdbx_version_info mdbx_version;

/* MDBX build information.
 * WARNING: Some strings could be NULL in case no corresponding information was
 *          provided at build time (i.e. flags). */
typedef struct mdbx_build_info {
  const char *datetime /* build timestamp (ISO-8601 or __DATE__ __TIME__) */;
  const char *target /* cpu/arch-system-config triplet */;
  const char *options /* mdbx-related options */;
  const char *compiler /* compiler */;
  const char *flags /* CFLAGS */;
} mdbx_build_info;
extern LIBMDBX_VERINFO_API const mdbx_build_info mdbx_build;

#if defined(_WIN32) || defined(_WIN64)
#if !MDBX_BUILD_SHARED_LIBRARY

/* MDBX internally uses global and thread local storage destructors to
 * automatically (de)initialization, releasing reader lock table slots
 * and so on.
 *
 * If MDBX builded as a DLL this is done out-of-the-box by DllEntry() function,
 * which called automatically by Windows core with passing corresponding reason
 * argument.
 *
 * Otherwise, if MDBX was builded not as a DLL, some black magic
 * may be required depending of Windows version:
 *  - Modern Windows versions, including Windows Vista and later, provides
 *    support for "TLS Directory" (e.g .CRT$XL[A-Z] sections in executable
 *    or dll file). In this case, MDBX capable of doing all automatically,
 *    and you do not need to call mdbx_dll_handler().
 *  - Obsolete versions of Windows, prior to Windows Vista, REQUIRES calling
 *    mdbx_dll_handler() manually from corresponding DllMain() or WinMain()
 *    of your DLL or application.
 *  - This behavior is under control of the MODX_CONFIG_MANUAL_TLS_CALLBACK
 *    option, which is determined by default according to the target version
 *    of Windows at build time.
 *    But you may override MODX_CONFIG_MANUAL_TLS_CALLBACK in special cases.
 *
 * Therefore, building MDBX as a DLL is recommended for all version of Windows.
 * So, if you doubt, just build MDBX as the separate DLL and don't worry. */

#ifndef MDBX_CONFIG_MANUAL_TLS_CALLBACK
#if defined(_WIN32_WINNT_VISTA) && WINVER >= _WIN32_WINNT_VISTA
/* As described above mdbx_dll_handler() is NOT needed forWindows Vista
 * and later. */
#define MDBX_CONFIG_MANUAL_TLS_CALLBACK 0
#else
/* As described above mdbx_dll_handler() IS REQUIRED for Windows versions
 * prior to Windows Vista. */
#define MDBX_CONFIG_MANUAL_TLS_CALLBACK 1
#endif
#endif /* MDBX_CONFIG_MANUAL_TLS_CALLBACK */

#if MDBX_CONFIG_MANUAL_TLS_CALLBACK
void LIBMDBX_API NTAPI mdbx_dll_handler(PVOID module, DWORD reason,
                                        PVOID reserved);
#endif /* MDBX_CONFIG_MANUAL_TLS_CALLBACK */
#endif /* !MDBX_BUILD_SHARED_LIBRARY */
#endif /* Windows */

/**** OPACITY STRUCTURES ******************************************************/

/* Opaque structure for a database environment.
 *
 * An environment supports multiple key-value databases (aka key-value spaces
 * or tables), all residing in the same shared-memory map. */
typedef struct MDBX_env MDBX_env;

/* Opaque structure for a transaction handle.
 *
 * All database operations require a transaction handle. Transactions may be
 * read-only or read-write. */
typedef struct MDBX_txn MDBX_txn;

/* A handle for an individual database (key-value spaces) in the environment.
 * Zero handle is used internally (hidden Garbage Collection DB).
 * So, any valid DBI-handle great than 0 and less than or equal MDBX_MAX_DBI. */
typedef uint32_t MDBX_dbi;
#define MDBX_MAX_DBI UINT32_C(32765)

/* Opaque structure for navigating through a database */
typedef struct MDBX_cursor MDBX_cursor;

/* Generic structure used for passing keys and data in and out of the database.
 *
 * Values returned from the database are valid only until a subsequent
 * update operation, or the end of the transaction. Do not modify or
 * free them, they commonly point into the database itself.
 *
 * Key sizes must be between 0 and mdbx_env_get_maxkeysize() inclusive.
 * The same applies to data sizes in databases with the MDBX_DUPSORT flag.
 * Other data items can in theory be from 0 to 0x7fffffff bytes long.
 *
 * (!) The notable difference between MDBX and LMDB is that MDBX support zero
 * length keys. */
#ifndef HAVE_STRUCT_IOVEC
struct iovec {
  void *iov_base /* pointer to some data */;
  size_t iov_len /* the length of data in bytes */;
};
#define HAVE_STRUCT_IOVEC
#endif /* HAVE_STRUCT_IOVEC */

#if defined(__sun) || defined(__SVR4) || defined(__svr4__)
/* The `iov_len` is signed on Sun/Solaris.
 * So define custom MDBX_val to avoid a lot of warings. */
typedef struct MDBX_val {
  void *iov_base /* pointer to some data */;
  size_t iov_len /* the length of data in bytes */;
} MDBX_val;
#else
typedef struct iovec MDBX_val;
#endif

/* The maximum size of a data item.
 * MDBX only store a 32 bit value for node sizes. */
#define MDBX_MAXDATASIZE INT32_MAX

/**** DEBUG & LOGGING **********************************************************
 * Logging and runtime debug flags.
 *
 * NOTE: Most of debug feature enabled only when libmdbx builded with
 *       MDBX_DEBUG options.
 */

/* Log level (requires build libmdbx with MDBX_DEBUG) */
#define MDBX_LOG_FATAL 0   /* critical conditions, i.e. assertion failures */
#define MDBX_LOG_ERROR 1   /* error conditions */
#define MDBX_LOG_WARN 2    /* warning conditions */
#define MDBX_LOG_NOTICE 3  /* normal but significant condition */
#define MDBX_LOG_VERBOSE 4 /* verbose informational */
#define MDBX_LOG_DEBUG 5   /* debug-level messages */
#define MDBX_LOG_TRACE 6   /* trace debug-level messages */
#define MDBX_LOG_EXTRA 7   /* extra debug-level messages (dump pgno lists) */

/* Runtime debug flags.
 *
 * MDBX_DBG_DUMP and MDBX_DBG_LEGACY_MULTIOPEN always have an effect,
 * but MDBX_DBG_ASSERT, MDBX_DBG_AUDIT and MDBX_DBG_JITTER only if libmdbx
 * builded with MDBX_DEBUG. */

/* Enable assertion checks */
#define MDBX_DBG_ASSERT 1

/* Enable pages usage audit at commit transactions */
#define MDBX_DBG_AUDIT 2

/* Enable small random delays in critical points */
#define MDBX_DBG_JITTER 4

/* Include or not meta-pages in coredump files,
 * MAY affect performance in MDBX_WRITEMAP mode */
#define MDBX_DBG_DUMP 8

/* Allow multi-opening environment(s) */
#define MDBX_DBG_LEGACY_MULTIOPEN 16

/* Allow read and write transactions overlapping for the same thread */
#define MDBX_DBG_LEGACY_OVERLAP 32

/* A debug-logger callback function,
 * called before printing the message and aborting.
 *
 * [in] env  An environment handle returned by mdbx_env_create().
 * [in] msg  The assertion message, not including newline. */
typedef void MDBX_debug_func(int loglevel, const char *function, int line,
                             const char *msg, va_list args);

/* Don't change current settings */
#define MDBX_LOG_DONTCHANGE (-1)
#define MDBX_DBG_DONTCHANGE (-1)
#define MDBX_LOGGER_DONTCHANGE ((MDBX_debug_func *)(intptr_t)-1)

/* Setup global log-level, debug options and debug logger. */
LIBMDBX_API int mdbx_setup_debug(int loglevel, int flags,
                                 MDBX_debug_func *logger);

/* A callback function for most MDBX assert() failures,
 * called before printing the message and aborting.
 *
 * [in] env  An environment handle returned by mdbx_env_create().
 * [in] msg  The assertion message, not including newline. */
typedef void MDBX_assert_func(const MDBX_env *env, const char *msg,
                              const char *function, unsigned line);

/* Set or reset the assert() callback of the environment.
 *
 * Does nothing if libmdbx was built with MDBX_DEBUG=0 or with NDEBUG,
 * and will return MDBX_ENOSYS in such case.
 *
 * [in] env   An environment handle returned by mdbx_env_create().
 * [in] func  An MDBX_assert_func function, or 0.
 *
 * Returns A non-zero error value on failure and 0 on success. */
LIBMDBX_API int mdbx_env_set_assert(MDBX_env *env, MDBX_assert_func *func);

/* FIXME: Complete description */
LIBMDBX_API const char *mdbx_dump_val(const MDBX_val *key, char *const buf,
                                      const size_t bufsize);

/**** THE FILES ****************************************************************
 * At the file system level, the environment corresponds to a pair of files. */

/* The name of the lock file in the environment */
#define MDBX_LOCKNAME "/mdbx.lck"
/* The name of the data file in the environment */
#define MDBX_DATANAME "/mdbx.dat"

/* The suffix of the lock file when MDBX_NOSUBDIR is used */
#define MDBX_LOCK_SUFFIX "-lck"

/**** ENVIRONMENT FLAGS *******************************************************/

/* MDBX_NOSUBDIR = no environment directory.
 *
 * By default, MDBX creates its environment in a directory whose pathname is
 * given in path, and creates its data and lock files under that directory.
 * With this option, path is used as-is for the database main data file.
 * The database lock file is the path with "-lck" appended.
 *
 *  - with MDBX_NOSUBDIR = in a filesystem we have the pair of MDBX-files which
 *    names derived from given pathname by appending predefined suffixes.
 *
 *  - without MDBX_NOSUBDIR = in a filesystem we have the MDBX-directory with
 *    given pathname, within that a pair of MDBX-files with predefined names.
 *
 * This flag affects only at environment opening and can't be changed after. */
#define MDBX_NOSUBDIR 0x4000u

/* MDBX_RDONLY = read only mode.
 *
 * Open the environment in read-only mode. No write operations will be allowed.
 * MDBX will still modify the lock file - except on read-only filesystems,
 * where MDBX does not use locks.
 *
 *  - with MDBX_RDONLY = open environment in read-only mode.
 *    MDBX supports pure read-only mode (i.e. without opening LCK-file) only
 *    when environment directory and/or both files are not writable (and the
 *    LCK-file may be missing). In such case allowing file(s) to be placed
 *    on a network read-only share.
 *
 *  - without MDBX_RDONLY = open environment in read-write mode.
 *
 * This flag affects only at environment opening but can't be changed after. */
#define MDBX_RDONLY 0x20000u

/* MDBX_EXCLUSIVE = open environment in exclusive/monopolistic mode.
 *
 * MDBX_EXCLUSIVE flag can be used as a replacement for MDB_NOLOCK, which don't
 * supported by MDBX. In this way, you can get the minimal overhead, but with
 * the correct multi-process and mutli-thread locking.
 *
 *  - with MDBX_EXCLUSIVE = open environment in exclusive/monopolistic mode
 *    or return MDBX_BUSY if environment already used by other process.
 *    The main feature of the exclusive mode is the ability to open the
 *    environment placed on a network share.
 *
 *  - without MDBX_EXCLUSIVE = open environment in cooperative mode,
 *    i.e. for multi-process access/interaction/cooperation.
 *    The main requirements of the cooperative mode are:
 *      1. data files MUST be placed in the LOCAL file system,
 *         but NOT on a network share.
 *      2. environment MUST be opened only by LOCAL processes,
 *         but NOT over a network.
 *      3. OS kernel (i.e. file system and memory mapping implementation) and
 *         all processes that open the given environment MUST be running
 *         in the physically single RAM with cache-coherency. The only
 *         exception for cache-consistency requirement is Linux on MIPS
 *         architecture, but this case has not been tested for a long time).

 * This flag affects only at environment opening but can't be changed after. */
#define MDBX_EXCLUSIVE 0x400000u

/* MDBX_ACCEDE = using database which already opened by another process(es).
 *
 * The MDBX_ACCEDE flag avoid MDBX_INCOMPATIBLE error while opening If the
 * database is already used by another process(es) and environment mode/flags
 * isn't compatible. In such cases, when using the MDBX_ACCEDE flag, instead of
 * the specified incompatible options, the mode in which the database is already
 * opened by other processes will be used, including MDBX_LIFORECLAIM,
 * MDBX_COALESCE and MDBX_NORDAHEAD. The MDBX_ACCEDE flag is useful to open a
 * database that already used by another process(es) and used mode/flags isn't
 * known.
 *
 * MDBX_ACCEDE has no effect if the current process is the only one either
 * opening the DB in read-only mode or other process(es) uses the DB in
 * read-only mode. */
#define MDBX_ACCEDE 0x40000000u

/* MDBX_WRITEMAP = map data into memory with write permission.
 *
 * Use a writeable memory map unless MDBX_RDONLY is set. This uses fewer mallocs
 * and requires much less work for tracking database pages, but loses protection
 * from application bugs like wild pointer writes and other bad updates into the
 * database. This may be slightly faster for DBs that fit entirely in RAM, but
 * is slower for DBs larger than RAM. Also adds the possibility for stray
 * application writes thru pointers to silently corrupt the database.
 * Incompatible with nested transactions.
 *
 * NOTE: The MDBX_WRITEMAP mode is incompatible with nested transactions, since
 *       this is unreasonable. I.e. nested transactions requires mallocation of
 *       database pages and more work for tracking ones, which neuters a
 *       performance boost caused by the MDBX_WRITEMAP mode.
 *
 * NOTE: MDBX don't allow to mix processes with and without MDBX_WRITEMAP on
 *       the same environment. In such case MDBX_INCOMPATIBLE will be generated.
 *
 *  - with MDBX_WRITEMAP = all data will be mapped into memory in the read-write
 *    mode. This offers a significant performance benefit, since the data will
 *    be modified directly in mapped memory and then flushed to disk by
 *    single system call, without any memory management nor copying.
 *    (!) On the other hand, MDBX_WRITEMAP adds the possibility for stray
 *    application writes thru pointers to silently corrupt the database.
 *    Moreover, MDBX_WRITEMAP disallows nested write transactions.
 *
 *  - without MDBX_WRITEMAP = data will be mapped into memory in the read-only
 *    mode. This requires stocking all modified database pages in memory and
 *    then writing them to disk through file operations.
 *
 * This flag affects only at environment opening but can't be changed after. */
#define MDBX_WRITEMAP 0x80000u

/* MDBX_NOTLS = tie reader locktable slots to read-only transactions instead
 * of to threads.
 *
 * Don't use Thread-Local Storage, instead tie reader locktable slots to
 * MDBX_txn objects instead of to threads. So, mdbx_txn_reset() keeps the slot
 * reserved for the MDBX_txn object. A thread may use parallel read-only
 * transactions. And a read-only transaction may span threads if you
 * synchronizes its use.
 *
 * Applications that multiplex many user threads over individual OS threads need
 * this option. Such an application must also serialize the write transactions
 * in an OS thread, since MDBX's write locking is unaware of the user threads.
 *
 * NOTE: Regardless to MDBX_NOTLS flag a write transaction entirely should
 * always be used in one thread from start to finish. MDBX checks this in a
 * reasonable manner and return the MDBX_THREAD_MISMATCH error in rules
 * violation.
 *
 * This flag affects only at environment opening but can't be changed after. */
#define MDBX_NOTLS 0x200000u

/* MDBX_NORDAHEAD = don't do readahead.
 *
 * Turn off readahead. Most operating systems perform readahead on read requests
 * by default. This option turns it off if the OS supports it. Turning it off
 * may help random read performance when the DB is larger than RAM and system
 * RAM is full.
 *
 * By default libmdbx dynamically enables/disables readahead depending on the
 * actual database size and currently available memory. On the other hand, such
 * automation has some limitation, i.e. could be performed only when DB size
 * changing but can't tracks and reacts changing a free RAM availability, since
 * it changes independently and asynchronously.
 *
 * NOTE: The mdbx_is_readahead_reasonable() function allows to quickly find out
 *       whether to use readahead or not based on the size of the data and the
 *       amount of available memory.
 *
 * This flag affects only at environment opening and can't be changed after. */
#define MDBX_NORDAHEAD 0x800000u

/* MDBX_NOMEMINIT = don't initialize malloc'd memory before writing to datafile.
 *
 * Don't initialize malloc'd memory before writing to unused spaces in the data
 * file. By default, memory for pages written to the data file is obtained using
 * malloc. While these pages may be reused in subsequent transactions, freshly
 * malloc'd pages will be initialized to zeroes before use. This avoids
 * persisting leftover data from other code (that used the heap and subsequently
 * freed the memory) into the data file.
 *
 * Note that many other system libraries may allocate and free memory from the
 * heap for arbitrary uses. E.g., stdio may use the heap for file I/O buffers.
 * This initialization step has a modest performance cost so some applications
 * may want to disable it using this flag. This option can be a problem for
 * applications which handle sensitive data like passwords, and it makes memory
 * checkers like Valgrind noisy. This flag is not needed with MDBX_WRITEMAP,
 * which writes directly to the mmap instead of using malloc for pages. The
 * initialization is also skipped if MDBX_RESERVE is used; the caller is
 * expected to overwrite all of the memory that was reserved in that case.
 *
 * This flag may be changed at any time using mdbx_env_set_flags(). */
#define MDBX_NOMEMINIT 0x1000000u

/* MDBX_COALESCE = aims to coalesce a Garbage Collection items.
 *
 * With MDBX_COALESCE flag MDBX will aims to coalesce items while recycling
 * a Garbage Collection. Technically, when possible short lists of pages will
 * be combined into longer ones, but to fit on one database page. As a result,
 * there will be fewer items in Garbage Collection and a page lists are longer,
 * which slightly increases the likelihood of returning pages to Unallocated
 * space and reducing the database file.
 *
 * This flag may be changed at any time using mdbx_env_set_flags(). */
#define MDBX_COALESCE 0x2000000u

/* MDBX_LIFORECLAIM = LIFO policy for recycling a Garbage Collection items.
 *
 * MDBX_LIFORECLAIM flag turns on LIFO policy for recycling a Garbage
 * Collection items, instead of FIFO by default. On systems with a disk
 * write-back cache, this can significantly increase write performance, up to
 * several times in a best case scenario.
 *
 * LIFO recycling policy means that for reuse pages will be taken which became
 * unused the lastest (i.e. just now or most recently). Therefore the loop of
 * database pages circulation becomes as short as possible. In other words, the
 * number of pages, that are overwritten in memory and on disk during a series
 * of write transactions, will be as small as possible. Thus creates ideal
 * conditions for the efficient operation of the disk write-back cache.
 *
 * MDBX_LIFORECLAIM is compatible with all no-sync flags (i.e. MDBX_NOMETASYNC,
 * MDBX_SAFE_NOSYNC, MDBX_UTTERLY_NOSYNC, MDBX_MAPASYNC), but gives no
 * noticeable impact in combination with MDBX_SAFE_NOSYNC. Because MDBX will
 * reused pages only before the last "steady" MVCC-snapshot, i.e. the loop
 * length of database pages circulation will be mostly defined by frequency of
 * calling mdbx_env_sync() rather than LIFO and FIFO difference.
 *
 * This flag may be changed at any time using mdbx_env_set_flags(). */
#define MDBX_LIFORECLAIM 0x4000000u

/* Debugging option, fill/perturb released pages. */
#define MDBX_PAGEPERTURB 0x8000000u

/**** SYNC MODES ***************************************************************
 * (!!!) Using any combination of MDBX_SAFE_NOSYNC, MDBX_NOMETASYNC,
 * MDBX_MAPASYNC and especially MDBX_UTTERLY_NOSYNC is always a deal to reduce
 * durability for gain write performance. You must know exactly what you are
 * doing and what risks you are taking!
 *
 * NOTE for LMDB users: MDBX_SAFE_NOSYNC is NOT similar to LMDB_NOSYNC, but
 *                      MDBX_UTTERLY_NOSYNC is exactly match LMDB_NOSYNC.
 *                      See details below.
 *
 * THE SCENE:
 *   - The DAT-file contains several MVCC-snapshots of B-tree at same time,
 *     each of those B-tree has its own root page.
 *   - Each of meta pages at the beginning of the DAT file contains a pointer
 *     to the root page of B-tree which is the result of the particular
 *     transaction, and a number of this transaction.
 *   - For data durability, MDBX must first write all MVCC-snapshot data pages
 *     and ensure that are written to the disk, then update a meta page with
 *     the new transaction number and a pointer to the corresponding new root
 *     page, and flush any buffers yet again.
 *   - Thus during commit a I/O buffers should be flushed to the disk twice;
 *     i.e. fdatasync(), FlushFileBuffers() or similar syscall should be called
 *     twice for each commit. This is very expensive for performance, but
 *     guaranteed durability even on unexpected system failure or power outage.
 *     Of course, provided that the operating system and the underlying hardware
 *     (e.g. disk) work correctly.
 *
 * TRADE-OFF: By skipping some stages described above, you can significantly
 * benefit in speed, while partially or completely losing in the guarantee of
 * data durability and/or consistency in the event of system or power failure.
 * Moreover, if for any reason disk write order is not preserved, then at moment
 * of a system crash, a meta-page with a pointer to the new B-tree may be
 * written to disk, while the itself B-tree not yet. In that case, the database
 * will be corrupted!
 *
 *
 * MDBX_NOMETASYNC = don't sync the meta-page after commit.
 *
 *      Flush system buffers to disk only once per transaction, omit the
 *      metadata flush. Defer that until the system flushes files to disk,
 *      or next non-MDBX_RDONLY commit or mdbx_env_sync(). Depending on the
 *      platform and hardware, with MDBX_NOMETASYNC you may get a doubling of
 *      write performance.
 *
 *      This trade-off maintains database integrity, but a system crash may
 *      undo the last committed transaction. I.e. it preserves the ACI
 *      (atomicity, consistency, isolation) but not D (durability) database
 *      property.
 *
 *      MDBX_NOMETASYNC flag may be changed at any time using
 *      mdbx_env_set_flags() or by passing to mdbx_txn_begin() for particular
 *      write transaction.
 *
 *
 * MDBX_UTTERLY_NOSYNC = don't sync anything and wipe previous steady commits.
 *
 *      Don't flush system buffers to disk when committing a transaction. This
 *      optimization means a system crash can corrupt the database, if buffers
 *      are not yet flushed to disk. Depending on the platform and hardware,
 *      with MDBX_UTTERLY_NOSYNC you may get a multiple increase of write
 *      performance, even 100 times or more.
 *
 *      If the filesystem preserves write order (which is rare and never
 *      provided unless explicitly noted) and the MDBX_WRITEMAP and
 *      MDBX_LIFORECLAIM flags are not used, then a system crash can't corrupt
 *      the database, but you can lose the last transactions, if at least one
 *      buffer is not yet flushed to disk. The risk is governed by how often the
 *      system flushes dirty buffers to disk and how often mdbx_env_sync() is
 *      called. So, transactions exhibit ACI (atomicity, consistency, isolation)
 *      properties and only lose D (durability). I.e. database integrity is
 *      maintained, but a system crash may undo the final transactions.
 *
 *      Otherwise, if the filesystem not preserves write order (which is
 *      typically) or MDBX_WRITEMAP or MDBX_LIFORECLAIM flags are used, you
 *      should expect the corrupted database after a system crash.
 *
 *      So, most important thing about MDBX_UTTERLY_NOSYNC:
 *       - a system crash immediately after commit the write transaction
 *         high likely lead to database corruption.
 *       - successful completion of mdbx_env_sync(force = true) after one or
 *         more commited transactions guarantees consystency and durability.
 *       - BUT by committing two or more transactions you back database into a
 *         weak state, in which a system crash may lead to database corruption!
 *         In case single transaction after mdbx_env_sync, you may lose
 *         transaction itself, but not a whole database.
 *
 *      Nevertheless, MDBX_UTTERLY_NOSYNC provides "weak" durability in case of
 *      an application crash (but no durability on system failure), and
 *      therefore may be very useful in scenarios where data durability is not
 *      required over a system failure (e.g for short-lived data), or if you can
 *      take such risk.
 *
 *      MDBX_UTTERLY_NOSYNC flag may be changed at any time using
 *      mdbx_env_set_flags(), but don't has effect if passed to mdbx_txn_begin()
 *      for particular write transaction.
 *
 *
 * MDBX_SAFE_NOSYNC = don't sync anything but keep previous steady commits.
 *
 *      Like MDBX_UTTERLY_NOSYNC the MDBX_SAFE_NOSYNC flag similarly disable
 *      flush system buffers to disk when committing a transaction. But there
 *      is a huge difference in how are recycled the MVCC snapshots
 *      corresponding to previous "steady" transactions (see below).
 *
 *      Depending on the platform and hardware, with MDBX_SAFE_NOSYNC you may
 *      get a multiple increase of write performance, even 10 times or more.
 *      NOTE that (MDBX_SAFE_NOSYNC | MDBX_WRITEMAP) leaves the system with no
 *      hint for when to write transactions to disk. Therefore the
 *      (MDBX_MAPASYNC | MDBX_WRITEMAP) may be preferable, but without
 *      MDBX_SAFE_NOSYNC because the (MDBX_MAPASYNC | MDBX_SAFE_NOSYNC) actually
 *      gives MDBX_UTTERLY_NOSYNC.
 *
 *      In contrast to MDBX_UTTERLY_NOSYNC mode, with MDBX_SAFE_NOSYNC flag MDBX
 *      will keeps untouched pages within B-tree of the last transaction
 *      "steady" which was synced to disk completely. This has big implications
 *      for both data durability and (unfortunately) performance:
 *       - a system crash can't corrupt the database, but you will lose the
 *         last transactions; because MDBX will rollback to last steady commit
 *         since it kept explicitly.
 *       - the last steady transaction makes an effect similar to "long-lived"
 *         read transaction (see above in the "RESTRICTIONS & CAVEATS" section)
 *         since prevents reuse of pages freed by newer write transactions,
 *         thus the any data changes will be placed in newly allocated pages.
 *       - to avoid rapid database growth, the system will sync data and issue
 *         a steady commit-point to resume reuse pages, each time there is
 *         insufficient space and before increasing the size of the file on
 *         disk.
 *
 *      In other words, with MDBX_SAFE_NOSYNC flag MDBX insures you from the
 *      whole database corruption, at the cost increasing database size and/or
 *      number of disk IOPS. So, MDBX_SAFE_NOSYNC flag could be used with
 *      mdbx_env_synv() as alternatively for batch committing or nested
 *      transaction (in some cases). As well, auto-sync feature exposed by
 *      mdbx_env_set_syncbytes() and mdbx_env_set_syncperiod() functions could
 *      be very usefull with MDBX_SAFE_NOSYNC flag.
 *
 *      The number and volume of of disk IOPS with MDBX_SAFE_NOSYNC flag will
 *      exactly the as without any no-sync flags. However, you should expect a
 *      larger process's work set (https://bit.ly/2kA2tFX) and significantly
 *      worse a locality of reference (https://bit.ly/2mbYq2J), due to the more
 *      intensive allocation of previously unused pages and increase the size of
 *      the database.
 *
 *      MDBX_SAFE_NOSYNC flag may be changed at any time using
 *      mdbx_env_set_flags() or by passing to mdbx_txn_begin() for particular
 *      write transaction.
 *
 *
 * MDBX_MAPASYNC = use asynchronous msync when MDBX_WRITEMAP is used.
 *
 *      MDBX_MAPASYNC meaningful and give effect only in conjunction
 *      with MDBX_WRITEMAP or MDBX_SAFE_NOSYNC:
 *       - with MDBX_SAFE_NOSYNC actually gives MDBX_UTTERLY_NOSYNC, which
 *         wipe previous steady commits for reuse pages as described above.
 *       - with MDBX_WRITEMAP but without MDBX_SAFE_NOSYNC instructs MDBX to use
 *         asynchronous mmap-flushes to disk as described below.
 *       - with both MDBX_WRITEMAP and MDBX_SAFE_NOSYNC you get the both
 *         effects.
 *
 *      Asynchronous mmap-flushes means that actually all writes will scheduled
 *      and performed by operation system on it own manner, i.e. unordered.
 *      MDBX itself just notify operating system that it would be nice to write
 *      data to disk, but no more.
 *
 *      With MDBX_MAPASYNC flag, but without MDBX_UTTERLY_NOSYNC (i.e. without
 *      OR'ing with MDBX_SAFE_NOSYNC) MDBX will keeps untouched pages within
 *      B-tree of the last transaction "steady" which was synced to disk
 *      completely. So, this makes exactly the same "long-lived" impact and the
 *      same consequences as described above for MDBX_SAFE_NOSYNC flag.
 *
 *      Depending on the platform and hardware, with combination of
 *      MDBX_WRITEMAP and MDBX_MAPASYNC you may get a multiple increase of write
 *      performance, even 25 times or more. MDBX_MAPASYNC flag may be changed at
 *      any time using mdbx_env_set_flags() or by passing to mdbx_txn_begin()
 *      for particular write transaction.
 */

/* Don't sync meta-page after commit,
 * see description in the "SYNC MODES" section above. */
#define MDBX_NOMETASYNC 0x40000u

/* Don't sync anything but keep previous steady commits,
 * see description in the "SYNC MODES" section above.
 *
 * (!) don't combine this flag with MDBX_MAPASYNC
 *     since you will got MDBX_UTTERLY_NOSYNC in that way (see below) */
#define MDBX_SAFE_NOSYNC 0x10000u

/* Use asynchronous msync when MDBX_WRITEMAP is used,
 * see description in the "SYNC MODES" section above.
 *
 * (!) don't combine this flag with MDBX_SAFE_NOSYNC
 *     since you will got MDBX_UTTERLY_NOSYNC in that way (see below) */
#define MDBX_MAPASYNC 0x100000u

/* Don't sync anything and wipe previous steady commits,
 * see description in the "SYNC MODES" section above. */
#define MDBX_UTTERLY_NOSYNC (MDBX_SAFE_NOSYNC | MDBX_MAPASYNC)

/**** DATABASE FLAGS **********************************************************/
/* Use reverse string keys */
#define MDBX_REVERSEKEY 0x02u

/* Use sorted duplicates */
#define MDBX_DUPSORT 0x04u

/* Numeric keys in native byte order, either uint32_t or uint64_t.
 * The keys must all be of the same size and must be aligned while passing as
 * arguments. */
#define MDBX_INTEGERKEY 0x08u

/* With MDBX_DUPSORT, sorted dup items have fixed size */
#define MDBX_DUPFIXED 0x10u

/* With MDBX_DUPSORT, dups are MDBX_INTEGERKEY-style integers.
 * The data values must all be of the same size and must be aligned while
 * passing as arguments. */
#define MDBX_INTEGERDUP 0x20u

/* With MDBX_DUPSORT, use reverse string dups */
#define MDBX_REVERSEDUP 0x40u

/* Create DB if not already existing */
#define MDBX_CREATE 0x40000u

/**** DATA UPDATE FLAGS *******************************************************/
/* For put: Don't write if the key already exists. */
#define MDBX_NOOVERWRITE 0x10u
/* Only for MDBX_DUPSORT
 * For put: don't write if the key and data pair already exist.
 * For mdbx_cursor_del: remove all duplicate data items. */
#define MDBX_NODUPDATA 0x20u
/* For mdbx_cursor_put: overwrite the current key/data pair
 * MDBX allows this flag for mdbx_put() for explicit overwrite/update without
 * insertion. */
#define MDBX_CURRENT 0x40u
/* For put: Just reserve space for data, don't copy it. Return a
 * pointer to the reserved space. */
#define MDBX_RESERVE 0x10000u
/* Data is being appended, don't split full pages. */
#define MDBX_APPEND 0x20000u
/* Duplicate data is being appended, don't split full pages. */
#define MDBX_APPENDDUP 0x40000u
/* Store multiple data items in one call. Only for MDBX_DUPFIXED. */
#define MDBX_MULTIPLE 0x80000u

/**** TRANSACTION FLAGS *******************************************************/
/* Do not block when starting a write transaction */
#define MDBX_TRYTXN 0x10000000u

/**** ENVIRONMENT COPY FLAGS **************************************************/
/* Compacting: Omit free space from copy, and renumber all pages sequentially */
#define MDBX_CP_COMPACT 1u

/**** CURSOR OPERATIONS ********************************************************
 *
 * This is the set of all operations for retrieving data
 * using a cursor. */
typedef enum MDBX_cursor_op {
  MDBX_FIRST,          /* Position at first key/data item */
  MDBX_FIRST_DUP,      /* MDBX_DUPSORT-only: Position at first data item
                        * of current key. */
  MDBX_GET_BOTH,       /* MDBX_DUPSORT-only: Position at key/data pair. */
  MDBX_GET_BOTH_RANGE, /* MDBX_DUPSORT-only: position at key, nearest data. */
  MDBX_GET_CURRENT,    /* Return key/data at current cursor position */
  MDBX_GET_MULTIPLE,   /* MDBX_DUPFIXED-only: Return up to a page of duplicate
                        * data items from current cursor position.
                        * Move cursor to prepare for MDBX_NEXT_MULTIPLE. */
  MDBX_LAST,           /* Position at last key/data item */
  MDBX_LAST_DUP,       /* MDBX_DUPSORT-only: Position at last data item
                        * of current key. */
  MDBX_NEXT,           /* Position at next data item */
  MDBX_NEXT_DUP,       /* MDBX_DUPSORT-only: Position at next data item
                        * of current key. */
  MDBX_NEXT_MULTIPLE,  /* MDBX_DUPFIXED-only: Return up to a page of
                        * duplicate data items from next cursor position.
                        * Move cursor to prepare for MDBX_NEXT_MULTIPLE. */
  MDBX_NEXT_NODUP,     /* Position at first data item of next key */
  MDBX_PREV,           /* Position at previous data item */
  MDBX_PREV_DUP,       /* MDBX_DUPSORT-only: Position at previous data item
                        * of current key. */
  MDBX_PREV_NODUP,     /* Position at last data item of previous key */
  MDBX_SET,            /* Position at specified key */
  MDBX_SET_KEY,        /* Position at specified key, return both key and data */
  MDBX_SET_RANGE,      /* Position at first key greater than or equal to
                        * specified key. */
  MDBX_PREV_MULTIPLE   /* MDBX_DUPFIXED-only: Position at previous page and
                        * return up to a page of duplicate data items. */
} MDBX_cursor_op;

/**** ERRORS & RETURN CODES ****************************************************
 * BerkeleyDB uses -30800 to -30999, we'll go under them */

/* Successful result */
#define MDBX_SUCCESS 0
#define MDBX_RESULT_FALSE MDBX_SUCCESS
/* Successful result with special meaning or a flag */
#define MDBX_RESULT_TRUE (-1)

/* key/data pair already exists */
#define MDBX_KEYEXIST (-30799)

/* key/data pair not found (EOF) */
#define MDBX_NOTFOUND (-30798)

/* Requested page not found - this usually indicates corruption */
#define MDBX_PAGE_NOTFOUND (-30797)

/* Database is corrupted (page was wrong type and so on) */
#define MDBX_CORRUPTED (-30796)

/* Environment had fatal error (i.e. update of meta page failed and so on) */
#define MDBX_PANIC (-30795)

/* DB file version mismatch with libmdbx */
#define MDBX_VERSION_MISMATCH (-30794)

/* File is not a valid MDBX file */
#define MDBX_INVALID (-30793)

/* Environment mapsize reached */
#define MDBX_MAP_FULL (-30792)

/* Environment maxdbs reached */
#define MDBX_DBS_FULL (-30791)

/* Environment maxreaders reached */
#define MDBX_READERS_FULL (-30790)

/* Transaction has too many dirty pages, i.e transaction too big */
#define MDBX_TXN_FULL (-30788)

/* Cursor stack too deep - internal error */
#define MDBX_CURSOR_FULL (-30787)

/* Page has not enough space - internal error */
#define MDBX_PAGE_FULL (-30786)

/* Database engine was unable to extend mapping, e.g. since address space
 * is unavailable or busy. This can mean:
 *  - Database size extended by other process beyond to environment mapsize
 *    and engine was unable to extend mapping while starting read transaction.
 *    Environment should be reopened to continue.
 *  - Engine was unable to extend mapping during write transaction
 *    or explicit call of mdbx_env_set_geometry(). */
#define MDBX_UNABLE_EXTEND_MAPSIZE (-30785)

/* Environment or database is not compatible with the requested operation
 * or the specified flags. This can mean:
 *  - The operation expects an MDBX_DUPSORT / MDBX_DUPFIXED database.
 *  - Opening a named DB when the unnamed DB has MDBX_DUPSORT/MDBX_INTEGERKEY.
 *  - Accessing a data record as a database, or vice versa.
 *  - The database was dropped and recreated with different flags. */
#define MDBX_INCOMPATIBLE (-30784)

/* Invalid reuse of reader locktable slot,
 * e.g. read-transaction already run for current thread */
#define MDBX_BAD_RSLOT (-30783)

/* Transaction is not valid for requested operation,
 * e.g. had errored and be must aborted, has a child, or is invalid */
#define MDBX_BAD_TXN (-30782)

/* Invalid size or alignment of key or data for target database,
 * either invalid subDB name */
#define MDBX_BAD_VALSIZE (-30781)

/* The specified DBI-handle is invalid
 * or changed by another thread/transaction */
#define MDBX_BAD_DBI (-30780)

/* Unexpected internal error, transaction should be aborted */
#define MDBX_PROBLEM (-30779)

/* The last LMDB-compatible defined error code */
#define MDBX_LAST_LMDB_ERRCODE MDBX_PROBLEM

/* Another write transaction is running or environment is already used while
 * opening with MDBX_EXCLUSIVE flag */
#define MDBX_BUSY (-30778)

/* The specified key has more than one associated value */
#define MDBX_EMULTIVAL (-30421)

/* Bad signature of a runtime object(s), this can mean:
 *  - memory corruption or double-free;
 *  - ABI version mismatch (rare case); */
#define MDBX_EBADSIGN (-30420)

/* Database should be recovered, but this could NOT be done for now
 * since it opened in read-only mode */
#define MDBX_WANNA_RECOVERY (-30419)

/* The given key value is mismatched to the current cursor position */
#define MDBX_EKEYMISMATCH (-30418)

/* Database is too large for current system,
 * e.g. could NOT be mapped into RAM. */
#define MDBX_TOO_LARGE (-30417)

/* A thread has attempted to use a not owned object,
 * e.g. a transaction that started by another thread. */
#define MDBX_THREAD_MISMATCH (-30416)

/* Overlapping read and write transactions for the current thread */
#define MDBX_TXN_OVERLAPPING (-30415)

/**** FUNCTIONS & RELATED STRUCTURES ******************************************/

/* Return a string describing a given error code.
 *
 * This function is a superset of the ANSI C X3.159-1989 (ANSI C) strerror(3)
 * function. If the error code is greater than or equal to 0, then the string
 * returned by the system function strerror(3) is returned. If the error code
 * is less than 0, an error string corresponding to the MDBX library error is
 * returned. See errors for a list of MDBX-specific error codes.
 *
 * mdbx_strerror()    - is NOT thread-safe because may share common internal
 *                      buffer for system maessages. The returned string must
 *                      NOT be modified by the application, but MAY be modified
 *                      by a subsequent call to mdbx_strerror(), strerror() and
 *                      other related functions.
 *
 * mdbx_strerror_r()  - is thread-safe since uses user-supplied buffer where
 *                      appropriate. The returned string must NOT be modified
 *                      by the application, since it may be pointer to internal
 *                      constatn string. However, there is no restriction if the
 *                      returned string points to the supplied buffer.
 *
 * [in] err  The error code.
 *
 * Returns "error message" The description of the error. */
LIBMDBX_API const char *mdbx_strerror(int errnum);
LIBMDBX_API const char *mdbx_strerror_r(int errnum, char *buf, size_t buflen);

#if defined(_WIN32) || defined(_WIN64)
/* Bit of Windows' madness. The similar functions but returns Windows
 * error-messages in the OEM-encoding for console utilities. */
LIBMDBX_API const char *mdbx_strerror_ANSI2OEM(int errnum);
LIBMDBX_API const char *mdbx_strerror_r_ANSI2OEM(int errnum, char *buf,
                                                 size_t buflen);
#endif /* Bit of Windows' madness */

/* Create an MDBX environment instance.
 *
 * This function allocates memory for a MDBX_env structure. To release
 * the allocated memory and discard the handle, call mdbx_env_close().
 * Before the handle may be used, it must be opened using mdbx_env_open().
 *
 * Various other options may also need to be set before opening the handle,
 * e.g. mdbx_env_set_geometry(), mdbx_env_set_maxreaders(),
 * mdbx_env_set_maxdbs(), depending on usage requirements.
 *
 * [out] env  The address where the new handle will be stored.
 *
 * Returns a non-zero error value on failure and 0 on success. */
LIBMDBX_API int mdbx_env_create(MDBX_env **penv);

/* Open an environment instance.
 *
 * Indifferently this function will fails or not, the mdbx_env_close() must be
 * called later to discard the MDBX_env handle and release associated resources.
 *
 * [in] env       An environment handle returned by mdbx_env_create()
 * [in] pathname  The directory in which the database files reside.
 *                This directory must already exist and be writable.
 * [in] flags     Special options for this environment. This parameter
 *                must be set to 0 or by bitwise OR'ing together one
 *                or more of the values described above in the
 *                "ENVIRONMENT FLAGS" and "SYNC MODES" sections.
 *
 * Flags set by mdbx_env_set_flags() are also used:
 *  - MDBX_NOSUBDIR, MDBX_RDONLY, MDBX_EXCLUSIVE, MDBX_WRITEMAP, MDBX_NOTLS,
 *    MDBX_NORDAHEAD, MDBX_NOMEMINIT, MDBX_COALESCE, MDBX_LIFORECLAIM.
 *    See "ENVIRONMENT FLAGS" section above.
 *
 *  - MDBX_NOMETASYNC, MDBX_SAFE_NOSYNC, MDBX_UTTERLY_NOSYNC, MDBX_MAPASYNC.
 *    See "SYNC MODES" section above.
 *
 * NOTE: MDB_NOLOCK flag don't supported by MDBX,
 *       try use MDBX_EXCLUSIVE as a replacement.
 *
 * NOTE: MDBX don't allow to mix processes with different MDBX_WRITEMAP,
 *       MDBX_SAFE_NOSYNC, MDBX_NOMETASYNC, MDBX_MAPASYNC flags on the same
 *       environment. In such case MDBX_INCOMPATIBLE will be returned.
 *
 * If the database is already exist and parameters specified early by
 * mdbx_env_set_geometry() are incompatible (i.e. for instance, different page
 * size) then mdbx_env_open() will return MDBX_INCOMPATIBLE error.
 *
 * [in] mode   The UNIX permissions to set on created files. Zero value means
 *             to open existing, but do not create.
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *   - MDBX_VERSION_MISMATCH = the version of the MDBX library doesn't match the
 *                             version that created the database environment.
 *   - MDBX_INVALID          = the environment file headers are corrupted.
 *   - MDBX_ENOENT           = the directory specified by the path parameter
 *                             doesn't exist.
 *   - MDBX_EACCES           = the user didn't have permission to access
 *                             the environment files.
 *   - MDBX_EAGAIN           = the environment was locked by another process.
 *   - MDBX_BUSY             = MDBX_EXCLUSIVE flag was specified and the
 *                             environment is in use by another process,
 *                             or the current process tries to open environment
 *                             more than once.
 *   - MDBX_INCOMPATIBLE     = Environment is already opened by another process,
 *                             but with different set of MDBX_WRITEMAP,
 *                             MDBX_SAFE_NOSYNC, MDBX_NOMETASYNC, MDBX_MAPASYNC
 *                             flags.
 *                             Or if the database is already exist and
 *                             parameters specified early by
 *                             mdbx_env_set_geometry() are incompatible (i.e.
 *                             for instance, different page size).
 *   - MDBX_WANNA_RECOVERY   = MDBX_RDONLY flag was specified but read-write
 *                             access is required to rollback inconsistent state
 *                             after a system crash.
 *   - MDBX_TOO_LARGE        = Database is too large for this process, i.e.
 *                             32-bit process tries to open >4Gb database. */
LIBMDBX_API int mdbx_env_open(MDBX_env *env, const char *pathname,
                              unsigned flags, mode_t mode);

/* Copy an MDBX environment to the specified path, with options.
 *
 * This function may be used to make a backup of an existing environment.
 * No lockfile is created, since it gets recreated at need.
 * NOTE: This call can trigger significant file size growth if run in
 * parallel with write transactions, because it employs a read-only
 * transaction. See long-lived transactions under "Caveats" section.
 *
 * [in] env    An environment handle returned by mdbx_env_create(). It must
 *             have already been opened successfully.
 * [in] dest   The pathname of a file in which the copy will reside. This file
 *             must not be already exist, but parent directory must be writable.
 * [in] flags  Special options for this operation. This parameter must be set
 *             to 0 or by bitwise OR'ing together one or more of the values
 *             described here:
 *
 *  - MDBX_CP_COMPACT
 *      Perform compaction while copying: omit free pages and sequentially
 *      renumber all pages in output. This option consumes little bit more
 *      CPU for processing, but may running quickly than the default, on
 *      account skipping free pages.
 *
 * Returns A non-zero error value on failure and 0 on success. */
LIBMDBX_API int mdbx_env_copy(MDBX_env *env, const char *dest, unsigned flags);

/* Copy an MDBX environment to the specified file descriptor,
 * with options.
 *
 * This function may be used to make a backup of an existing environment.
 * No lockfile is created, since it gets recreated at need. See
 * mdbx_env_copy() for further details.
 *
 * NOTE: This call can trigger significant file size growth if run in
 *       parallel with write transactions, because it employs a read-only
 *       transaction. See long-lived transactions under "Caveats" section.
 *
 * NOTE: Fails if the environment has suffered a page leak and the destination
 *       file descriptor is associated with a pipe, socket, or FIFO.
 *
 * [in] env     An environment handle returned by mdbx_env_create(). It must
 *              have already been opened successfully.
 * [in] fd      The filedescriptor to write the copy to. It must have already
 *              been opened for Write access.
 * [in] flags   Special options for this operation. See mdbx_env_copy() for
 *              options.
 *
 * Returns A non-zero error value on failure and 0 on success. */
LIBMDBX_API int mdbx_env_copy2fd(MDBX_env *env, mdbx_filehandle_t fd,
                                 unsigned flags);

/* Statistics for a database in the environment */
typedef struct MDBX_stat {
  uint32_t ms_psize;          /* Size of a database page.
                               * This is the same for all databases. */
  uint32_t ms_depth;          /* Depth (height) of the B-tree */
  uint64_t ms_branch_pages;   /* Number of internal (non-leaf) pages */
  uint64_t ms_leaf_pages;     /* Number of leaf pages */
  uint64_t ms_overflow_pages; /* Number of overflow pages */
  uint64_t ms_entries;        /* Number of data items */
  uint64_t ms_mod_txnid;      /* Transaction ID of commited last modification */
} MDBX_stat;

/* Return statistics about the MDBX environment.
 *
 * At least one of env or txn argument must be non-null. If txn is passed
 * non-null then stat will be filled accordingly to the given transaction.
 * Otherwise, if txn is null, then stat will be populated by a snapshot from the
 * last committed write transaction, and at next time, other information can be
 * returned.
 *
 * Legacy mdbx_env_stat() correspond to calling mdbx_env_stat_ex() with the null
 * txn argument.
 *
 * [in] env     An environment handle returned by mdbx_env_create()
 * [in] txn     A transaction handle returned by mdbx_txn_begin()
 * [out] stat   The address of an MDBX_stat structure where the statistics
 *              will be copied
 *
 * Returns A non-zero error value on failure and 0 on success. */
LIBMDBX_API int mdbx_env_stat_ex(const MDBX_env *env, const MDBX_txn *txn,
                                 MDBX_stat *stat, size_t bytes);
__deprecated LIBMDBX_API int mdbx_env_stat(MDBX_env *env, MDBX_stat *stat,
                                           size_t bytes);

/* Information about the environment */
typedef struct MDBX_envinfo {
  struct {
    uint64_t lower;   /* lower limit for datafile size */
    uint64_t upper;   /* upper limit for datafile size */
    uint64_t current; /* current datafile size */
    uint64_t shrink;  /* shrink threshold for datafile */
    uint64_t grow;    /* growth step for datafile */
  } mi_geo;
  uint64_t mi_mapsize;             /* Size of the data memory map */
  uint64_t mi_last_pgno;           /* ID of the last used page */
  uint64_t mi_recent_txnid;        /* ID of the last committed transaction */
  uint64_t mi_latter_reader_txnid; /* ID of the last reader transaction */
  uint64_t mi_self_latter_reader_txnid; /* ID of the last reader transaction of
                                           caller process */
  uint64_t mi_meta0_txnid, mi_meta0_sign;
  uint64_t mi_meta1_txnid, mi_meta1_sign;
  uint64_t mi_meta2_txnid, mi_meta2_sign;
  uint32_t mi_maxreaders;   /* max reader slots in the environment */
  uint32_t mi_numreaders;   /* max reader slots used in the environment */
  uint32_t mi_dxb_pagesize; /* database pagesize */
  uint32_t mi_sys_pagesize; /* system pagesize */

  struct {
    /* A mostly unique ID that is regenerated on each boot. As such it can be
       used to identify the local machine's current boot. MDBX uses such when
       open the database to determine whether rollback required to the last
       steady sync point or not. I.e. if current bootid is differ from the value
       within a database then the system was rebooted and all changes since last
       steady sync must be reverted for data integrity. Zeros mean that no
       relevant information is available from the system. */
    struct {
      uint64_t l, h;
    } current, meta0, meta1, meta2;
  } mi_bootid;

  uint64_t mi_unsync_volume; /* bytes not explicitly synchronized to disk */
  uint64_t mi_autosync_threshold;        /* current auto-sync threshold, see
                                            mdbx_env_set_syncbytes(). */
  uint32_t mi_since_sync_seconds16dot16; /* time since the last steady sync in
                                            1/65536 of second */
  uint32_t mi_autosync_period_seconds16dot16 /* current auto-sync period in
                                                1/65536 of second, see
                                                mdbx_env_set_syncperiod(). */
      ;
  uint32_t mi_since_reader_check_seconds16dot16; /* time since the last readers
                                                    check in 1/65536 of second,
                                                    see mdbx_reader_check(). */
  uint32_t mi_mode; /* current environment mode, the same as
                       mdbx_env_get_flags() returns. */
} MDBX_envinfo;

/* Return information about the MDBX environment.
 *
 * At least one of env or txn argument must be non-null. If txn is passed
 * non-null then stat will be filled accordingly to the given transaction.
 * Otherwise, if txn is null, then stat will be populated by a snapshot from the
 * last committed write transaction, and at next time, other information can be
 * returned.
 *
 * Legacy mdbx_env_info() correspond to calling mdbx_env_info_ex() with the null
 * txn argument.

 * [in] env     An environment handle returned by mdbx_env_create()
 * [in] txn     A transaction handle returned by mdbx_txn_begin()
 * [out] stat   The address of an MDBX_envinfo structure
 *              where the information will be copied
 *
 * Returns A non-zero error value on failure and 0 on success. */
LIBMDBX_API int mdbx_env_info_ex(const MDBX_env *env, const MDBX_txn *txn,
                                 MDBX_envinfo *info, size_t bytes);
__deprecated LIBMDBX_API int mdbx_env_info(MDBX_env *env, MDBX_envinfo *info,
                                           size_t bytes);

/* Flush the environment data buffers to disk.
 *
 * Unless the environment was opened with no-sync flags (MDBX_NOMETASYNC,
 * MDBX_SAFE_NOSYNC, MDBX_UTTERLY_NOSYNC and MDBX_MAPASYNC), then data is always
 * written an flushed to disk when mdbx_txn_commit() is called. Otherwise
 * mdbx_env_sync() may be called to manually write and flush unsynced data to
 * disk.
 *
 * Besides, mdbx_env_sync_ex() with argument force=false may be used to
 * provide polling mode for lazy/asynchronous sync in conjunction with
 * mdbx_env_set_syncbytes() and/or mdbx_env_set_syncperiod().
 *
 * The mdbx_env_sync() is shortcut to calling mdbx_env_sync_ex() with
 * try force=true and nonblock=false arguments.
 *
 * The mdbx_env_sync_poll() is shortcut to calling mdbx_env_sync_ex() with
 * the force=false and nonblock=true arguments.
 *
 * NOTE: This call is not valid if the environment was opened with MDBX_RDONLY.
 *
 * [in] env       An environment handle returned by mdbx_env_create().
 * [in] force     If non-zero, force a flush. Otherwise, if force is zero, then
 *                will run in polling mode, i.e. it will check the thresholds
 *                that were set mdbx_env_set_syncbytes() and/or
 *                mdbx_env_set_syncperiod() and perform flush If at least one
 *                of the thresholds is reached.
 * [in] nonblock  Don't wait if write transaction is running by other thread.
 *
 * Returns A non-zero error value on failure and MDBX_RESULT_TRUE or 0 on
 * success. The MDBX_RESULT_TRUE means no data pending for flush to disk,
 * and 0 otherwise. Some possible errors are:
 *  - MDBX_EACCES   = the environment is read-only.
 *  - MDBX_BUSY     = the environment is used by other thread and nonblock=true.
 *  - MDBX_EINVAL   = an invalid parameter was specified.
 *  - MDBX_EIO      = an error occurred during synchronization. */
LIBMDBX_API int mdbx_env_sync_ex(MDBX_env *env, int force, int nonblock);
LIBMDBX_API int mdbx_env_sync(MDBX_env *env);
LIBMDBX_API int mdbx_env_sync_poll(MDBX_env *env);

/* Sets threshold to force flush the data buffers to disk, even of
 * MDBX_SAFE_NOSYNC, MDBX_NOMETASYNC and MDBX_MAPASYNC flags in the environment.
 * The threshold value affects all processes which operates with given
 * environment until the last process close environment or a new value will be
 * settled.
 *
 * Data is always written to disk when mdbx_txn_commit() is called,  but the
 * operating system may keep it buffered. MDBX always flushes the OS buffers
 * upon commit as well, unless the environment was opened with MDBX_SAFE_NOSYNC,
 * MDBX_MAPASYNC or in part MDBX_NOMETASYNC.
 *
 * The default is 0, than mean no any threshold checked, and no additional
 * flush will be made.
 *
 * [in] env         An environment handle returned by mdbx_env_create().
 * [in] threshold   The size in bytes of summary changes when a synchronous
 *                  flush would be made.
 *
 * Returns A non-zero error value on failure and 0 on success. */
LIBMDBX_API int mdbx_env_set_syncbytes(MDBX_env *env, size_t threshold);

/* Sets relative period since the last unsteay commit to force flush the data
 * buffers to disk, even of MDBX_SAFE_NOSYNC, MDBX_NOMETASYNC and MDBX_MAPASYNC
 * flags in the environment. The relative period value affects all processes
 * which operates with given environment until the last process close
 * environment or a new value will be settled.
 *
 * Data is always written to disk when mdbx_txn_commit() is called, but the
 * operating system may keep it buffered. MDBX always flushes the OS buffers
 * upon commit as well, unless the environment was opened with MDBX_SAFE_NOSYNC,
 * MDBX_MAPASYNC or in part MDBX_NOMETASYNC.
 *
 * Settled period don't checked asynchronously, but only by the
 * mdbx_txn_commit() and mdbx_env_sync() functions. Therefore, in cases where
 * transactions are committed infrequently and/or irregularly, polling by
 * mdbx_env_sync() may be a reasonable solution to timeout enforcement.
 *
 * The default is 0, than mean no any timeout checked, and no additional
 * flush will be made.
 *
 * [in] env               An environment handle returned by mdbx_env_create().
 * [in] seconds_16dot16   The period in 1/65536 of second when a synchronous
 *                        flush would be made since the last unsteay commit.
 *
 * Returns A non-zero error value on failure and 0 on success. */
LIBMDBX_API int mdbx_env_set_syncperiod(MDBX_env *env,
                                        unsigned seconds_16dot16);

/* Close the environment and release the memory map.
 *
 * Only a single thread may call this function. All transactions, databases,
 * and cursors must already be closed before calling this function. Attempts
 * to use any such handles after calling this function will cause a SIGSEGV.
 * The environment handle will be freed and must not be used again after this
 * call.
 *
 * Legacy mdbx_env_close() correspond to calling mdbx_env_close_ex() with the
 * argument dont_sync=false.
 *
 * [in] env        An environment handle returned by mdbx_env_create().
 * [in] dont_sync  A dont'sync flag, if non-zero the last checkpoint (meta-page
 *                 update) will be kept "as is" and may be still "weak" in the
 *                 NOSYNC/MAPASYNC modes. Such "weak" checkpoint will be
 *                 ignored on opening next time, and transactions since the
 *                 last non-weak checkpoint (meta-page update) will rolledback
 *                 for consistency guarantee.
 *
 * Returns A non-zero error value on failure and 0 on success.
 * Some possible errors are:
 *  - MDBX_BUSY    = The write transaction is running by other thread, in such
 *                   case MDBX_env instance has NOT be destroyed not released!
 *                   NOTE: if any OTHER error code was returned then given
 *                         MDBX_env instance has been destroyed and released.
 *  - MDBX_PANIC   = If mdbx_env_close_ex() was called in the child process
 *                   after fork(). In this case MDBX_PANIC is a expecte,
 *                   i.e. MDBX_env instance was freed in proper manner.
 *  - MDBX_EIO     = an error occurred during synchronization. */
LIBMDBX_API int mdbx_env_close_ex(MDBX_env *env, int dont_sync);
LIBMDBX_API int mdbx_env_close(MDBX_env *env);

/* Set environment flags.
 *
 * This may be used to set some flags in addition to those from
 * mdbx_env_open(), or to unset these flags.
 *
 * NOTE: In contrast to LMDB, the MDBX serialize threads via mutex while
 * changing the flags. Therefore this function will be blocked while a write
 * transaction running by other thread, or MDBX_BUSY will be returned if
 * function called within a write transaction.
 *
 * [in] env     An environment handle returned by mdbx_env_create().
 * [in] flags   The flags to change, bitwise OR'ed together.
 * [in] onoff   A non-zero value sets the flags, zero clears them.
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_EINVAL   = an invalid parameter was specified. */
LIBMDBX_API int mdbx_env_set_flags(MDBX_env *env, unsigned flags, int onoff);

/* Get environment flags.
 *
 * [in] env     An environment handle returned by mdbx_env_create().
 * [out] flags  The address of an integer to store the flags.
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_EINVAL   = an invalid parameter was specified. */
LIBMDBX_API int mdbx_env_get_flags(MDBX_env *env, unsigned *flags);

/* Return the path that was used in mdbx_env_open().
 *
 * [in] env     An environment handle returned by mdbx_env_create()
 * [out] dest   Address of a string pointer to contain the path.
 *              This is the actual string in the environment, not a copy.
 *              It should not be altered in any way.
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_EINVAL   = an invalid parameter was specified. */
LIBMDBX_API int mdbx_env_get_path(MDBX_env *env, const char **dest);

/* Return the file descriptor for the given environment.
 *
 * NOTE: All MDBX file descriptors have FD_CLOEXEC and
 * could't be used after exec() and or fork().
 *
 * [in] env   An environment handle returned by mdbx_env_create().
 * [out] fd   Address of a int to contain the descriptor.
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_EINVAL   = an invalid parameter was specified. */
LIBMDBX_API int mdbx_env_get_fd(MDBX_env *env, mdbx_filehandle_t *fd);

/* Set all size-related parameters of environment, including page size and the
 * min/max size of the memory map.
 *
 * In contrast to LMDB, the MDBX provide automatic size management of an
 * database according the given parameters, including shrinking and resizing
 * on the fly. From user point of view all of these just working. Nevertheless,
 * it is reasonable to know some details in order to make optimal decisions when
 * choosing parameters.
 *
 * Both mdbx_env_info_ex() and legacy mdbx_env_info() are inapplicable to
 * read-only opened environment.
 *
 * Both mdbx_env_info_ex() and legacy mdbx_env_info() could be called either
 * before or after mdbx_env_open(), either within the write transaction running
 * by current thread or not:
 *
 *  - In case mdbx_env_info_ex() or legacy mdbx_env_info() was called BEFORE
 *    mdbx_env_open(), i.e. for closed environment, then the specified
 *    parameters will be used for new database creation, or will be appliend
 *    during openeing if database exists and no other process using it.
 *
 *    If the database is already exist, opened with MDBX_EXCLUSIVE or not used
 *    by any other process, and parameters specified by mdbx_env_set_geometry()
 *    are incompatible (i.e. for instance, different page size) then
 *    mdbx_env_open() will return MDBX_INCOMPATIBLE error.
 *
 *    In another way, if database will opened read-only or will used by other
 *    process during calling mdbx_env_open() that specified parameters will
 *    silently discarded (open the database with MDBX_EXCLUSIVE flag to avoid
 *    this).
 *
 *  - In case mdbx_env_info_ex() or legacy mdbx_env_info() was called after
 *    mdbx_env_open() WITHIN the write transaction running by current thread,
 *    then specified parameters will be appliad as a part of write transaction,
 *    i.e. will not be visible to any others processes until the current write
 *    transaction has been committed by the current process. However, if
 *    transaction will be aborted, then the database file will be reverted to
 *    the previous size not immediately, but when a next transaction will be
 *    committed or when the database will be opened next time.
 *
 *  - In case mdbx_env_info_ex() or legacy mdbx_env_info() was called after
 *    mdbx_env_open() but OUTSIDE a write transaction, then MDBX will execute
 *    internal pseudo-transaction to apply new parameters (but only if anything
 *    has been changed), and changes be visible to any others processes
 *    immediatelly after succesfull competeion of function.
 *
 * Essentially a concept of "automatic size management" is simple and useful:
 *  - There are the lower and upper bound of the database file size;
 *  - There is the growth step by which the database file will be increased,
 *    in case of lack of space.
 *  - There is the threshold for unused space, beyond which the database file
 *    will be shrunk.
 *  - The size of the memory map is also the maximum size of the database.
 *  - MDBX will automatically manage both the size of the database and the size
 *    of memory map, according to the given parameters.
 *
 * So, there some considerations about choosing these parameters:
 *  - The lower bound allows you to prevent database shrinking below some
 *    rational size to avoid unnecessary resizing costs.
 *  - The upper bound allows you to prevent database growth above some rational
 *    size. Besides, the upper bound defines the linear address space
 *    reservation in each process that opens the database. Therefore changing
 *    the upper bound is costly and may be required reopening environment in
 *    case of MDBX_UNABLE_EXTEND_MAPSIZE errors, and so on. Therefore, this
 *    value should be chosen reasonable as large as possible, to accommodate
 *    future growth of the database.
 *  - The growth step must be greater than zero to allow the database to grow,
 *    but also reasonable not too small, since increasing the size by little
 *    steps will result a large overhead.
 *  - The shrink threshold must be greater than zero to allow the database
 *    to shrink but also reasonable not too small (to avoid extra overhead) and
 *    not less than growth step to avoid up-and-down flouncing.
 *  - The current size (i.e. size_now argument) is an auxiliary parameter for
 *    simulation legacy mdbx_env_set_mapsize() and as workaround Windows issues
 *    (see below).
 *
 * Unfortunately, Windows has is a several issues
 * with resizing of memory-mapped file:
 *  - Windows unable shrinking a memory-mapped file (i.e memory-mapped section)
 *    in any way except unmapping file entirely and then map again. Moreover,
 *    it is impossible in any way if a memory-mapped file is used more than
 *    one process.
 *  - Windows does not provide the usual API to augment a memory-mapped file
 *    (that is, a memory-mapped partition), but only by using "Native API"
 *    in an undocumented way.
 *
 * MDBX bypasses all Windows issues, but at a cost:
 *  - Ability to resize database on the fly requires an additional lock
 *    and release SlimReadWriteLock during each read-only transaction.
 *  - During resize all in-process threads should be paused and then resumed.
 *  - Shrinking of database file is performed only when it used by single
 *    process, i.e. when a database closes by the last process or opened
 *    by the first.
 *  = Therefore, the size_now argument may be useful to set database size
 *    by the first process which open a database, and thus avoid expensive
 *    remapping further.
 *
 * For create a new database with particular parameters, including the page
 * size, mdbx_env_set_geometry() should be called after mdbx_env_create() and
 * before mdbx_env_open(). Once the database is created, the page size cannot be
 * changed. If you do not specify all or some of the parameters, the
 * corresponding default values will be used. For instance, the default for
 * database size is 10485760 bytes.
 *
 * If the mapsize is increased by another process, MDBX silently and
 * transparently adopt these changes at next transaction start. However,
 * mdbx_txn_begin() will return MDBX_UNABLE_EXTEND_MAPSIZE if new mapping size
 * could not be applied for current process (for instance if address space
 * is busy).  Therefore, in the case of MDBX_UNABLE_EXTEND_MAPSIZE error you
 * need close and reopen the environment to resolve error.
 *
 * NOTE: Actual values may be different than your have specified because of
 * rounding to specified database page size, the system page size and/or the
 * size of the system virtual memory management unit. You can get actual values
 * by mdbx_env_sync_ex() or see by using the tool "mdbx_chk" with the "-v"
 * option.
 *
 * Legacy mdbx_env_set_mapsize() correspond to calling mdbx_env_set_geometry()
 * with the arguments size_lower, size_now, size_upper equal to the size
 * and -1 (i.e. default) for all other parameters.
 *
 * [in] env               An environment handle returned by mdbx_env_create()
 *
 * [in] size_lower        The lower bound of database sive in bytes.
 *                        Zero value means "minimal acceptable",
 *                        and negative means "keep current or use default".
 *
 * [in] size_now          The size in bytes to setup the database size for now.
 *                        Zero value means "minimal acceptable",
 *                        and negative means "keep current or use default".
 *                        So, it is recommended always pass -1 in this argument
 *                        except some special cases.
 *
 * [in] size_upper        The upper bound of database sive in bytes.
 *                        Zero value means "minimal acceptable",
 *                        and negative means "keep current or use default".
 *                        It is recommended to avoid change upper bound while
 *                        database is used by other processes or threaded
 *                        (i.e. just pass -1 in this argument except absolutely
 *                        necessity). Otherwise you must be ready for
 *                        MDBX_UNABLE_EXTEND_MAPSIZE error(s), unexpected pauses
 *                        during remapping and/or system errors like "addtress
 *                        busy", and so on. In other words, there is no way to
 *                        handle a growth of the upper bound robustly because
 *                        there may be a lack of appropriate system resources
 *                        (which are extremely volatile in a multi-process
 *                        multi-threaded environment).
 *
 * [in] growth_step       The growth step in bytes, must be greater than zero
 *                        to allow the database to grow.
 *                        Negative value means "keep current or use default".
 *
 * [in] shrink_threshold  The shrink threshold in bytes, must be greater than
 *                        zero to allow the database to shrink.
 *                        Negative value means "keep current or use default".
 *
 * [in] pagesize          The database page size for new database creation
 *                        or -1 otherwise. Must be power of 2 in the range
 *                        between MDBX_MIN_PAGESIZE and MDBX_MAX_PAGESIZE.
 *                        Zero value means "minimal acceptable",
 *                        and negative means "keep current or use default".
 *
 * Returns A non-zero error value on failure and 0 on success,
 * some possible errors are:
 *  - MDBX_EINVAL    = An invalid parameter was specified,
 *                     or the environment has an active write transaction.
 *  - MDBX_EPERM     = specific for Windows: Shrinking was disabled before and
 *                     now it wanna be enabled, but there are reading threads
 *                     that don't use the additional SRWL (that is required to
 *                     avoid Windows issues).
 *  - MDBX_EACCESS   = The environment opened in read-only.
 *  - MDBX_MAP_FULL  = Specified size smaller than the space already
 *                     consumed by the environment.
 *  - MDBX_TOO_LARGE = Specified size is too large, i.e. too many pages for
 *                     given size, or a 32-bit process requests too much bytes
 *                     for the 32-bit address space. */
LIBMDBX_API int mdbx_env_set_geometry(MDBX_env *env, intptr_t size_lower,
                                      intptr_t size_now, intptr_t size_upper,
                                      intptr_t growth_step,
                                      intptr_t shrink_threshold,
                                      intptr_t pagesize);
__deprecated LIBMDBX_API int mdbx_env_set_mapsize(MDBX_env *env, size_t size);

/* Find out whether to use readahead or not, based on the given database size
 * and the amount of available memory.
 *
 * [in] volume      The expected database size in bytes.
 * [in] redundancy  Additional reserve or overload in case of negative value.
 *
 * Returns:
 *  - MDBX_RESULT_TRUE    = readahead is reasonable.
 *  - MDBX_RESULT_FALSE   = readahead is NOT reasonable, i.e. MDBX_NORDAHEAD
 *                          is useful to open environment by mdbx_env_open().
 *  - Otherwise the error code. */
LIBMDBX_API int mdbx_is_readahead_reasonable(size_t volume,
                                             intptr_t redundancy);

/* The minimal database page size in bytes. */
#define MDBX_MIN_PAGESIZE 256
__inline intptr_t mdbx_limits_pgsize_min(void) { return MDBX_MIN_PAGESIZE; }

/* The maximal database page size in bytes. */
#define MDBX_MAX_PAGESIZE 65536
__inline intptr_t mdbx_limits_pgsize_max(void) { return MDBX_MAX_PAGESIZE; }

/* Returns minimal database size in bytes for given page size,
 * or -1 if pagesize is invalid. */
LIBMDBX_API intptr_t mdbx_limits_dbsize_min(intptr_t pagesize);

/* Returns maximal database size in bytes for given page size,
 * or -1 if pagesize is invalid. */
LIBMDBX_API intptr_t mdbx_limits_dbsize_max(intptr_t pagesize);

/* Returns maximal key and data size in bytes for given page size
 * and database flags (see mdbx_dbi_open_ex() description),
 * or -1 if pagesize is invalid. */
LIBMDBX_API intptr_t mdbx_limits_keysize_max(intptr_t pagesize, unsigned flags);
LIBMDBX_API intptr_t mdbx_limits_valsize_max(intptr_t pagesize, unsigned flags);

/* Returns maximal write transaction size (i.e. limit for summary volume of
 * dirty pages) in bytes for given page size, or -1 if pagesize is invalid. */
LIBMDBX_API intptr_t mdbx_limits_txnsize_max(intptr_t pagesize);

/* Set the maximum number of threads/reader slots for the environment.
 *
 * This defines the number of slots in the lock table that is used to track
 * readers in the the environment. The default is 119 for 4K system page size.
 * Starting a read-only transaction normally ties a lock table slot to the
 * current thread until the environment closes or the thread exits. If
 * MDBX_NOTLS is in use, mdbx_txn_begin() instead ties the slot to the
 * MDBX_txn object until it or the MDBX_env object is destroyed.
 * This function may only be called after mdbx_env_create() and before
 * mdbx_env_open().
 *
 * [in] env       An environment handle returned by mdbx_env_create().
 * [in] readers   The maximum number of reader lock table slots.
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_EINVAL   = an invalid parameter was specified.
 *  - MDBX_EPERM    = the environment is already open. */
LIBMDBX_API int mdbx_env_set_maxreaders(MDBX_env *env, unsigned readers);

/* Get the maximum number of threads/reader slots for the environment.
 *
 * [in] env An environment handle returned by mdbx_env_create().
 * [out] readers Address of an integer to store the number of readers.
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_EINVAL   = an invalid parameter was specified. */
LIBMDBX_API int mdbx_env_get_maxreaders(MDBX_env *env, unsigned *readers);

/* Set the maximum number of named databases for the environment.
 *
 * This function is only needed if multiple databases will be used in the
 * environment. Simpler applications that use the environment as a single
 * unnamed database can ignore this option.
 * This function may only be called after mdbx_env_create() and before
 * mdbx_env_open().
 *
 * Currently a moderate number of slots are cheap but a huge number gets
 * expensive: 7-120 words per transaction, and every mdbx_dbi_open()
 * does a linear search of the opened slots.
 *
 * [in] env   An environment handle returned by mdbx_env_create().
 * [in] dbs   The maximum number of databases.
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_EINVAL   = an invalid parameter was specified.
 *  - MDBX_EPERM    = the environment is already open. */
LIBMDBX_API int mdbx_env_set_maxdbs(MDBX_env *env, MDBX_dbi dbs);

/* Get the maximum size of keys and data we can write.
 *
 * [in] env    An environment handle returned by mdbx_env_create().
 * [in] flags  Database options (MDBX_DUPSORT, MDBX_INTEGERKEY ans so on),
 *             see mdbx_dbi_open_ex() description.
 *
 * Returns The maximum size of a key we can write,
 * or -1 if something is wrong. */
LIBMDBX_API int mdbx_env_get_maxkeysize_ex(MDBX_env *env, unsigned flags);
LIBMDBX_API int mdbx_env_get_maxvalsize_ex(MDBX_env *env, unsigned flags);
__deprecated LIBMDBX_API int mdbx_env_get_maxkeysize(MDBX_env *env);

/* Set application information associated with the MDBX_env.
 *
 * [in] env  An environment handle returned by mdbx_env_create().
 * [in] ctx  An arbitrary pointer for whatever the application needs.
 *
 * Returns A non-zero error value on failure and 0 on success. */
LIBMDBX_API int mdbx_env_set_userctx(MDBX_env *env, void *ctx);

/* Get the application information associated with the MDBX_env.
 *
 * [in] env An environment handle returned by mdbx_env_create()
 * Returns The pointer set by mdbx_env_set_userctx(). */
LIBMDBX_API void *mdbx_env_get_userctx(MDBX_env *env);

/* Create a transaction for use with the environment.
 *
 * The transaction handle may be discarded using mdbx_txn_abort()
 * or mdbx_txn_commit().
 *
 * NOTE: A transaction and its cursors must only be used by a single thread,
 * and a thread may only have a single transaction at a time. If MDBX_NOTLS is
 * in use, this does not apply to read-only transactions.
 *
 * NOTE: Cursors may not span transactions.
 *
 * [in] env     An environment handle returned by mdbx_env_create()
 * [in] parent  If this parameter is non-NULL, the new transaction will be
 *              a nested transaction, with the transaction indicated by parent
 *              as its parent. Transactions may be nested to any level.
 *              A parent transaction and its cursors may not issue any other
 *              operations than mdbx_txn_commit and mdbx_txn_abort while it
 *              has active child transactions.
 * [in] flags   Special options for this transaction. This parameter
 *              must be set to 0 or by bitwise OR'ing together one or more
 *              of the values described here.
 *
 *  - MDBX_RDONLY
 *      This transaction will not perform any write operations.
 *
 *  - MDBX_TRYTXN
 *      Do not block when starting a write transaction.
 *
 *  - MDBX_SAFE_NOSYNC, MDBX_NOMETASYNC or MDBX_MAPASYNC
 *      Do not sync data to disk corresponding to MDBX_NOMETASYNC
 *      or MDBX_SAFE_NOSYNC description (see abobe).
 *
 * [out] txn Address where the new MDBX_txn handle will be stored
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_PANIC         = a fatal error occurred earlier and the environment
 *                         must be shut down.
 *  - MDBX_UNABLE_EXTEND_MAPSIZE
 *                       = another process wrote data beyond this MDBX_env's
 *                         mapsize and this environment's map must be resized
 *                         as well. See mdbx_env_set_mapsize().
 *  - MDBX_READERS_FULL  = a read-only transaction was requested and the reader
 *                         lock table is full. See mdbx_env_set_maxreaders().
 *  - MDBX_ENOMEM        = out of memory.
 *  - MDBX_BUSY          = the write transaction is already started by the
 *                         current thread. */
LIBMDBX_API int mdbx_txn_begin(MDBX_env *env, MDBX_txn *parent, unsigned flags,
                               MDBX_txn **txn);

/* Information about the transaction */
typedef struct MDBX_txn_info {
  uint64_t txn_id; /* The ID of the transaction. For a READ-ONLY transaction,
                      this corresponds to the snapshot being read. */

  uint64_t
      txn_reader_lag; /* For READ-ONLY transaction: the lag from a recent
                         MVCC-snapshot, i.e. the number of committed
                         transaction since read transaction started.
                         For WRITE transaction (provided if scan_rlt=true): the
                         lag of the oldest reader from current transaction (i.e.
                         atleast 1 if any reader running). */

  uint64_t txn_space_used; /* Used space by this transaction, i.e. corresponding
                              to the last used database page. */

  uint64_t txn_space_limit_soft; /* Current size of database file. */

  uint64_t
      txn_space_limit_hard; /* Upper bound for size the database file,
                               i.e. the value "size_upper" argument of the
                               approriate call of mdbx_env_set_geometry(). */

  uint64_t txn_space_retired; /* For READ-ONLY transaction: The total size of
                                 the database pages that were retired by
                                 committed write transactions after the reader's
                                 MVCC-snapshot, i.e. the space which would be
                                 freed after the Reader releases the
                                 MVCC-snapshot for reuse by completion read
                                 transaction.
                                 For WRITE transaction: The summarized size of
                                 the database pages that were retired for now
                                 due Copy-On-Write during this transaction. */

  uint64_t
      txn_space_leftover; /* For READ-ONLY transaction: the space available for
                             writer(s) and that must be exhausted for reason to
                             call the OOM-killer for this read transaction.
                             For WRITE transaction: the space inside transaction
                             that left to MDBX_TXN_FULL error. */

  uint64_t txn_space_dirty; /* For READ-ONLY transaction (provided if
                               scan_rlt=true): The space that actually become
                               available for reuse when only this transaction
                               will be finished.
                               For WRITE transaction: The summarized size of the
                               dirty database pages that generated during this
                               transaction. */
} MDBX_txn_info;

/* Return information about the MDBX transaction.
 *
 * [in] txn        A transaction handle returned by mdbx_txn_begin().
 * [out] stat      The address of an MDBX_txn_info structure
 *                 where the information will be copied.
 * [in] scan_rlt   The boolean flag controls the scan of the read lock table to
 *                 provide complete information. Such scan is relatively
 *                 expensive and you can avoid it if corresponding fields are
 *                 not needed (see description of MDBX_txn_info above).
 *
 * Returns A non-zero error value on failure and 0 on success. */
LIBMDBX_API int mdbx_txn_info(MDBX_txn *txn, MDBX_txn_info *info, int scan_rlt);

/* Returns the transaction's MDBX_env.
 *
 * [in] txn  A transaction handle returned by mdbx_txn_begin() */
LIBMDBX_API MDBX_env *mdbx_txn_env(MDBX_txn *txn);

/* Return the transaction's flags.
 *
 * This returns the flags associated with this transaction.
 *
 * [in] txn A transaction handle returned by mdbx_txn_begin().
 *
 * Returns A transaction flags, valid if input is an valid transaction,
 * otherwise -1. */
LIBMDBX_API int mdbx_txn_flags(MDBX_txn *txn);

/* Return the transaction's ID.
 *
 * This returns the identifier associated with this transaction. For a read-only
 * transaction, this corresponds to the snapshot being read; concurrent readers
 * will frequently have the same transaction ID.
 *
 * [in] txn A transaction handle returned by mdbx_txn_begin().
 *
 * Returns A transaction ID, valid if input is an active transaction,
 * otherwise 0. */
LIBMDBX_API uint64_t mdbx_txn_id(MDBX_txn *txn);

/* Commit all the operations of a transaction into the database.
 *
 * The transaction handle is freed. It and its cursors must not be used again
 * after this call, except with mdbx_cursor_renew() and mdbx_cursor_close().
 *
 * A cursor must be closed explicitly always, before or after its transaction
 * ends. It can be reused with mdbx_cursor_renew() before finally closing it.
 *
 * [in] txn  A transaction handle returned by mdbx_txn_begin().
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_EINVAL   = an invalid parameter was specified.
 *  - MDBX_ENOSPC   = no more disk space.
 *  - MDBX_EIO      = a low-level I/O error occurred while writing.
 *  - MDBX_ENOMEM   = out of memory. */
LIBMDBX_API int mdbx_txn_commit(MDBX_txn *txn);

/* Abandon all the operations of the transaction instead of saving them.
 *
 * The transaction handle is freed. It and its cursors must not be used again
 * after this call, except with mdbx_cursor_renew() and mdbx_cursor_close().
 *
 * A cursor must be closed explicitly always, before or after its transaction
 * ends. It can be reused with mdbx_cursor_renew() before finally closing it.
 *
 * [in] txn  A transaction handle returned by mdbx_txn_begin().
 *
 * Returns A non-zero error value on failure and 0 on success. */
LIBMDBX_API int mdbx_txn_abort(MDBX_txn *txn);

/* Reset a read-only transaction.
 *
 * Abort the read-only transaction like mdbx_txn_abort(), but keep the
 * transaction handle. Therefore mdbx_txn_renew() may reuse the handle. This
 * saves allocation overhead if the process will start a new read-only
 * transaction soon, and also locking overhead if MDBX_NOTLS is in use. The
 * reader table lock is released, but the table slot stays tied to its thread or
 * MDBX_txn. Use mdbx_txn_abort() to discard a reset handle, and to free its
 * lock table slot if MDBX_NOTLS is in use.
 *
 * Cursors opened within the transaction must not be used again after this call,
 * except with mdbx_cursor_renew() and mdbx_cursor_close().
 *
 * Reader locks generally don't interfere with writers, but they keep old
 * versions of database pages allocated. Thus they prevent the old pages from
 * being reused when writers commit new data, and so under heavy load the
 * database size may grow much more rapidly than otherwise.
 *
 * [in] txn  A transaction handle returned by mdbx_txn_begin().
 *
 * Returns A non-zero error value on failure and 0 on success. */
LIBMDBX_API int mdbx_txn_reset(MDBX_txn *txn);

/* Renew a read-only transaction.
 *
 * This acquires a new reader lock for a transaction handle that had been
 * released by mdbx_txn_reset(). It must be called before a reset transaction
 * may be used again.
 *
 * [in] txn  A transaction handle returned by mdbx_txn_begin().
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_PANIC     = a fatal error occurred earlier and the environment
 *                     must be shut down.
 *  - MDBX_EINVAL    = an invalid parameter was specified. */
LIBMDBX_API int mdbx_txn_renew(MDBX_txn *txn);

/* The fours integers markers (aka "canary") associated with the environment.
 *
 * The `x`, `y` and `z` values could be set by mdbx_canary_put(), while the 'v'
 * will be always set to the transaction number. Updated values becomes visible
 * outside the current transaction only after it was committed. Current values
 * could be retrieved by mdbx_canary_get(). */
typedef struct mdbx_canary {
  uint64_t x, y, z, v;
} mdbx_canary;

/* Set integers markers (aka "canary") associated with the environment.
 *
 * [in] txn     A transaction handle returned by mdbx_txn_begin()
 * [in] canary  A optional pointer to mdbx_canary structure for `x`, `y`
 *              and `z` values from.
 *            - If canary is NOT NULL then the `x`, `y` and `z` values will be
 *              updated from given canary argument, but the 'v' be always set
 *              to the current transaction number if at least one `x`, `y` or
 *              `z` values have changed (i.e. if `x`, `y` and `z` have the same
 *              values as currently present then nothing will be changes or
 *              updated).
 *            - if canary is NULL then the `v` value will be explicitly update
 *              to the current transaction number without changes `x`, `y` nor
 *              `z`.
 *
 * Returns A non-zero error value on failure and 0 on success. */
LIBMDBX_API int mdbx_canary_put(MDBX_txn *txn, const mdbx_canary *canary);

/* Returns fours integers markers (aka "canary") associated with the
 * environment.
 *
 * [in] txn     A transaction handle returned by mdbx_txn_begin().
 * [in] canary  The address of an mdbx_canary structure where the information
 *              will be copied.
 *
 * Returns A non-zero error value on failure and 0 on success. */
LIBMDBX_API int mdbx_canary_get(MDBX_txn *txn, mdbx_canary *canary);

/* A callback function used to compare two keys in a database */
typedef int(MDBX_cmp_func)(const MDBX_val *a, const MDBX_val *b);

/* Open a database in the environment.
 *
 * A database handle denotes the name and parameters of a database,
 * independently of whether such a database exists. The database handle may be
 * discarded by calling mdbx_dbi_close(). The old database handle is returned if
 * the database was already open. The handle may only be closed once.
 *
 * (!) A notable difference between MDBX and LMDB is that MDBX make handles
 * opened for existing databases immediately available for other transactions,
 * regardless this transaction will be aborted or reset. The REASON for this is
 * to avoiding the requirement for multiple opening a same handles in concurrent
 * read transactions, and tracking of such open but hidden handles until the
 * completion of read transactions which opened them.
 *
 * Nevertheless, the handle for the NEWLY CREATED database will be invisible for
 * other transactions until the this write transaction is successfully
 * committed. If the write transaction is aborted the handle will be closed
 * automatically. After a successful commit the such handle will reside in the
 * shared environment, and may be used by other transactions.
 *
 * In contrast to LMDB, the MDBX allow this function to be called from multiple
 * concurrent transactions or threads in the same process.
 *
 * To use named database (with name != NULL), mdbx_env_set_maxdbs()
 * must be called before opening the environment. Table names are
 * keys in the internal unnamed database, and may be read but not written.
 *
 * [in] txn    transaction handle returned by mdbx_txn_begin().
 * [in] name   The name of the database to open. If only a single
 *             database is needed in the environment, this value may be NULL.
 * [in] flags  Special options for this database. This parameter must be set
 *             to 0 or by bitwise OR'ing together one or more of the values
 *             described here:
 *  - MDBX_REVERSEKEY
 *      Keys are strings to be compared in reverse order, from the end
 *      of the strings to the beginning. By default, Keys are treated as
 *      strings and compared from beginning to end.
 *  - MDBX_DUPSORT
 *      Duplicate keys may be used in the database. Or, from another point of
 *      view, keys may have multiple data items, stored in sorted order. By
 *      default keys must be unique and may have only a single data item.
 *  - MDBX_INTEGERKEY
 *      Keys are binary integers in native byte order, either uin32_t or
 *      uint64_t, and will be sorted as such. The keys must all be of the
 *      same size and must be aligned while passing as arguments.
 *  - MDBX_DUPFIXED
 *      This flag may only be used in combination with MDBX_DUPSORT. This
 *      option tells the library that the data items for this database are
 *      all the same size, which allows further optimizations in storage and
 *      retrieval. When all data items are the same size, the MDBX_GET_MULTIPLE,
 *      MDBX_NEXT_MULTIPLE and MDBX_PREV_MULTIPLE cursor operations may be used
 *      to retrieve multiple items at once.
 *  - MDBX_INTEGERDUP
 *      This option specifies that duplicate data items are binary integers,
 *      similar to MDBX_INTEGERKEY keys. The data values must all be of the
 *      same size and must be aligned while passing as arguments.
 *  - MDBX_REVERSEDUP
 *      This option specifies that duplicate data items should be compared as
 *      strings in reverse order (the comparison is performed in the direction
 *      from the last byte to the first).
 *  - MDBX_CREATE
 *      Create the named database if it doesn't exist. This option is not
 *      allowed in a read-only transaction or a read-only environment.
 *
 * [out] dbi     Address where the new MDBX_dbi handle will be stored.
 *
 * For mdbx_dbi_open_ex() additional arguments allow you to set custom
 * comparison functions for keys and values (for multimaps).
 * However, I recommend not using custom comparison functions, but instead
 * converting the keys to one of the forms that are suitable for built-in
 * comparators (for instance take look to the mdbx_key_from_xxx()
 * functions). The reasons to not using custom comparators are:
 *   - The order of records could not be validated without your code.
 *     So mdbx_chk utility will reports "wrong order" errors
 *     and the '-i' option is required to ignore ones.
 *   - A records could not be ordered or sorted without your code.
 *     So mdbx_load utility should be used with '-a' option to preserve
 *     input data order.
 *
 * [in] keycmp   Optional custom key comparison function for a database.
 * [in] datacmp  Optional custom data comparison function for a database, takes
 *               effect only if database was opened with the MDB_DUPSORT flag.
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_NOTFOUND      = the specified database doesn't exist in the
 *                         environment and MDBX_CREATE was not specified.
 *  - MDBX_DBS_FULL      = too many databases have been opened.
 *                         See mdbx_env_set_maxdbs().
 *  - MDBX_INCOMPATIBLE  = Database is incompatible with given flags,
 *                         i.e. the passed flags is different with which the
 *                         database was created, or the database was already
 *                         opened with a different comparison function(s). */
LIBMDBX_API int mdbx_dbi_open_ex(MDBX_txn *txn, const char *name,
                                 unsigned flags, MDBX_dbi *dbi,
                                 MDBX_cmp_func *keycmp, MDBX_cmp_func *datacmp);
LIBMDBX_API int mdbx_dbi_open(MDBX_txn *txn, const char *name, unsigned flags,
                              MDBX_dbi *dbi);

/* Key-making functions to avoid custom comparators.
 *
 * The mdbx_key_from_jsonInteger() build key which are comparable with
 * keys created by mdbx_key_from_double(). So this allow mix int64 and IEEE754
 * double values in one index for JSON-numbers with restriction for integer
 * numbers range corresponding to RFC-7159 (i.e. [-(2**53)+1, (2**53)-1].
 * See bottom of page 6 at https://tools.ietf.org/html/rfc7159 */
LIBMDBX_API uint64_t mdbx_key_from_jsonInteger(const int64_t json_integer);
LIBMDBX_API uint64_t mdbx_key_from_double(const double ieee754_64bit);
LIBMDBX_API uint64_t mdbx_key_from_ptrdouble(const double *const ieee754_64bit);
LIBMDBX_API uint32_t mdbx_key_from_float(const float ieee754_32bit);
LIBMDBX_API uint32_t mdbx_key_from_ptrfloat(const float *const ieee754_32bit);
__inline uint64_t mdbx_key_from_int64(const int64_t i64) {
  return UINT64_C(0x8000000000000000) + i64;
}
__inline uint32_t mdbx_key_from_int32(const int32_t i32) {
  return UINT32_C(0x80000000) + i32;
}

/* Retrieve statistics for a database.
 *
 * [in] txn     A transaction handle returned by mdbx_txn_begin().
 * [in] dbi     A database handle returned by mdbx_dbi_open().
 * [out] stat   The address of an MDBX_stat structure where the statistics
 *              will be copied.
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_EINVAL   = an invalid parameter was specified. */
LIBMDBX_API int mdbx_dbi_stat(MDBX_txn *txn, MDBX_dbi dbi, MDBX_stat *stat,
                              size_t bytes);

/* Retrieve the DB flags and status for a database handle.
 *
 * [in] txn     A transaction handle returned by mdbx_txn_begin().
 * [in] dbi     A database handle returned by mdbx_dbi_open().
 * [out] flags  Address where the flags will be returned.
 * [out] state  Address where the state will be returned.
 *
 * Legacy mdbx_dbi_flags() correspond to calling mdbx_dbi_flags_ex() with
 * discarding result from the last argument.
 *
 * Returns A non-zero error value on failure and 0 on success. */
#define MDBX_TBL_DIRTY 0x01 /* DB was written in this txn */
#define MDBX_TBL_STALE 0x02 /* Named-DB record is older than txnID */
#define MDBX_TBL_FRESH 0x04 /* Named-DB handle opened in this txn */
#define MDBX_TBL_CREAT 0x08 /* Named-DB handle created in this txn */
LIBMDBX_API int mdbx_dbi_flags_ex(MDBX_txn *txn, MDBX_dbi dbi, unsigned *flags,
                                  unsigned *state);
LIBMDBX_API int mdbx_dbi_flags(MDBX_txn *txn, MDBX_dbi dbi, unsigned *flags);

/* Close a database handle. Normally unnecessary.
 *
 * NOTE: Use with care.
 * This call is synchronized via mutex with mdbx_dbi_close(), but NOT with
 * other transactions running by other threads. The "next" version of libmdbx
 * (MithrilDB) will solve this issue.
 *
 * Handles should only be closed if no other threads are going to reference
 * the database handle or one of its cursors any further. Do not close a handle
 * if an existing transaction has modified its database. Doing so can cause
 * misbehavior from database corruption to errors like MDBX_BAD_VALSIZE (since
 * the DB name is gone).
 *
 * Closing a database handle is not necessary, but lets mdbx_dbi_open() reuse
 * the handle value. Usually it's better to set a bigger mdbx_env_set_maxdbs(),
 * unless that value would be large.
 *
 * [in] env  An environment handle returned by mdbx_env_create().
 * [in] dbi  A database handle returned by mdbx_dbi_open().
 *
 * Returns A non-zero error value on failure and 0 on success. */
LIBMDBX_API int mdbx_dbi_close(MDBX_env *env, MDBX_dbi dbi);

/* Empty or delete and close a database.
 *
 * See mdbx_dbi_close() for restrictions about closing the DB handle.
 *
 * [in] txn  A transaction handle returned by mdbx_txn_begin().
 * [in] dbi  A database handle returned by mdbx_dbi_open().
 * [in] del  0 to empty the DB, 1 to delete it from the environment
 *           and close the DB handle.
 *
 * Returns A non-zero error value on failure and 0 on success. */
LIBMDBX_API int mdbx_drop(MDBX_txn *txn, MDBX_dbi dbi, int del);

/* Get items from a database.
 *
 * This function retrieves key/data pairs from the database. The address
 * and length of the data associated with the specified key are returned
 * in the structure to which data refers.
 * If the database supports duplicate keys (MDBX_DUPSORT) then the
 * first data item for the key will be returned. Retrieval of other
 * items requires the use of mdbx_cursor_get().
 *
 * NOTE: The memory pointed to by the returned values is owned by the
 * database. The caller need not dispose of the memory, and may not
 * modify it in any way. For values returned in a read-only transaction
 * any modification attempts will cause a SIGSEGV.
 *
 * NOTE: Values returned from the database are valid only until a
 * subsequent update operation, or the end of the transaction.
 *
 * [in] txn       A transaction handle returned by mdbx_txn_begin().
 * [in] dbi       A database handle returned by mdbx_dbi_open().
 * [in] key       The key to search for in the database.
 * [in,out] data  The data corresponding to the key.
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_NOTFOUND  = the key was not in the database.
 *  - MDBX_EINVAL    = an invalid parameter was specified. */
LIBMDBX_API int mdbx_get(MDBX_txn *txn, MDBX_dbi dbi, MDBX_val *key,
                         MDBX_val *data);

/* Get items from a database and optionaly number of data items for a given key.
 *
 * Briefly this function does the same as mdbx_get() with a few differences:
 *  1. If values_count is NOT NULL, then returns the count
 *     of multi-values/duplicates for a given key.
 *  2. Updates BOTH the key and the data for pointing to the actual key-value
 *     pair inside the database.
 *
 * [in] txn            A transaction handle returned by mdbx_txn_begin().
 * [in] dbi            A database handle returned by mdbx_dbi_open().
 * [in,out] key        The key to search for in the database.
 * [in,out] data       The data corresponding to the key.
 * [out] values_count  The optional address to return number of values
 *                     associated with given key, i.e.
 *                        = 0 - in case MDBX_NOTFOUND error;
 *                        = 1 - exactly for databases WITHOUT MDBX_DUPSORT;
 *                       >= 1 for databases WITH MDBX_DUPSORT.
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_NOTFOUND  = the key was not in the database.
 *  - MDBX_EINVAL    = an invalid parameter was specified. */
LIBMDBX_API int mdbx_get_ex(MDBX_txn *txn, MDBX_dbi dbi, MDBX_val *key,
                            MDBX_val *data, size_t *values_count);

/* Get nearest items from a database.
 *
 * Briefly this function does the same as mdbx_get() with a few differences:
 * 1. Return nearest (i.e. equal or great due comparison function) key-value
 *    pair, but not only exactly matching with the key.
 * 2. On success return MDBX_SUCCESS if key found exactly,
 *    and MDBX_RESULT_TRUE otherwise. Moreover, for databases with MDBX_DUPSORT
 *    flag the data argument also will be used to match over
 *    multi-value/duplicates, and MDBX_SUCCESS will be returned only when BOTH
 *    the key and the data match exactly.
 * 3. Updates BOTH the key and the data for pointing to the actual key-value
 *    pair inside the database.
 *
 * [in] txn            A transaction handle returned by mdbx_txn_begin().
 * [in] dbi            A database handle returned by mdbx_dbi_open().
 * [in,out] key        The key to search for in the database.
 * [in,out] data       The data corresponding to the key.
 *
 * Returns A non-zero error value on failure and MDBX_RESULT_TRUE (0) or
 * MDBX_RESULT_TRUE on success (as described above).
 * Some possible errors are:
 *  - MDBX_NOTFOUND  = the key was not in the database.
 *  - MDBX_EINVAL    = an invalid parameter was specified. */
LIBMDBX_API int mdbx_get_nearest(MDBX_txn *txn, MDBX_dbi dbi, MDBX_val *key,
                                 MDBX_val *data);

/* Store items into a database.
 *
 * This function stores key/data pairs in the database. The default behavior
 * is to enter the new key/data pair, replacing any previously existing key
 * if duplicates are disallowed, or adding a duplicate data item if
 * duplicates are allowed (MDBX_DUPSORT).
 *
 * [in] txn        A transaction handle returned by mdbx_txn_begin().
 * [in] dbi        A database handle returned by mdbx_dbi_open().
 * [in] key        The key to store in the database.
 * [in,out] data   The data to store.
 * [in] flags      Special options for this operation. This parameter must be
 *                 set to 0 or by bitwise OR'ing together one or more of the
 *                 values described here.
 *
 *  - MDBX_NODUPDATA
 *      Enter the new key/data pair only if it does not already appear
 *      in the database. This flag may only be specified if the database
 *      was opened with MDBX_DUPSORT. The function will return MDBX_KEYEXIST
 *      if the key/data pair already appears in the database.
 *
 *  - MDBX_NOOVERWRITE
 *      Enter the new key/data pair only if the key does not already appear
 *      in the database. The function will return MDBX_KEYEXIST if the key
 *      already appears in the database, even if the database supports
 *      duplicates (MDBX_DUPSORT). The data parameter will be set to point
 *      to the existing item.
 *
 *  - MDBX_CURRENT
 *      Update an single existing entry, but not add new ones. The function
 *      will return MDBX_NOTFOUND if the given key not exist in the database.
 *      Or the MDBX_EMULTIVAL in case duplicates for the given key.
 *
 *  - MDBX_RESERVE
 *      Reserve space for data of the given size, but don't copy the given
 *      data. Instead, return a pointer to the reserved space, which the
 *      caller can fill in later - before the next update operation or the
 *      transaction ends. This saves an extra memcpy if the data is being
 *      generated later. MDBX does nothing else with this memory, the caller
 *      is expected to modify all of the space requested. This flag must not
 *      be specified if the database was opened with MDBX_DUPSORT.
 *
 *  - MDBX_APPEND
 *      Append the given key/data pair to the end of the database. This option
 *      allows fast bulk loading when keys are already known to be in the
 *      correct order. Loading unsorted keys with this flag will cause
 *      a MDBX_EKEYMISMATCH error.
 *
 *  - MDBX_APPENDDUP
 *      As above, but for sorted dup data.
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_KEYEXIST
 *  - MDBX_MAP_FULL  = the database is full, see mdbx_env_set_mapsize().
 *  - MDBX_TXN_FULL  = the transaction has too many dirty pages.
 *  - MDBX_EACCES    = an attempt was made to write in a read-only transaction.
 *  - MDBX_EINVAL    = an invalid parameter was specified. */
LIBMDBX_API int mdbx_put(MDBX_txn *txn, MDBX_dbi dbi, MDBX_val *key,
                         MDBX_val *data, unsigned flags);

/* Replace items in a database.
 *
 * This function allows to update or delete an existing value at the same time
 * as the previous value is retrieved. If the argument new_data equal is NULL
 * zero, the removal is performed, otherwise the update/insert.
 *
 * The current value may be in an already changed (aka dirty) page. In this
 * case, the page will be overwritten during the update, and the old value will
 * be lost. Therefore, an additional buffer must be passed via old_data argument
 * initially to copy the old value. If the buffer passed in is too small, the
 * function will return MDBX_RESULT_TRUE (-1) by setting iov_len field pointed
 * by old_data argument to the appropriate value, without performing any
 * changes.
 *
 * For databases with non-unique keys (i.e. with MDBX_DUPSORT flag), another use
 * case is also possible, when by old_data argument selects a specific item from
 * multi-value/duplicates with the same key for deletion or update. To select
 * this scenario in flags should simultaneously specify MDBX_CURRENT and
 * MDBX_NOOVERWRITE. This combination is chosen because it makes no sense, and
 * thus allows you to identify the request of such a scenario.
 *
 * [in] txn            A transaction handle returned by mdbx_txn_begin().
 * [in] dbi            A database handle returned by mdbx_dbi_open().
 * [in] key            The key to store in the database.
 * [in,out] new_data   The data to store, if NULL then deletion will be
 *                     performed.
 * [in,out] old_data   The buffer for retrieve previous value as describe
 *                     above.
 * [in] flags          Special options for this operation. This parameter must
 *                     be set to 0 or by bitwise OR'ing together one or more of
 *                     the values described in mdbx_put() description above,
 *                     and additionally (MDBX_CURRENT | MDBX_NOOVERWRITE)
 *                     combination for selection particular item from
 *                     multi-value/duplicates.
 *
 * Returns A non-zero error value on failure and 0 on success. */
LIBMDBX_API int mdbx_replace(MDBX_txn *txn, MDBX_dbi dbi, MDBX_val *key,
                             MDBX_val *new_data, MDBX_val *old_data,
                             unsigned flags);

/* Delete items from a database.
 *
 * This function removes key/data pairs from the database.
 *
 * NOTE: The data parameter is NOT ignored regardless the database does
 * support sorted duplicate data items or not. If the data parameter
 * is non-NULL only the matching data item will be deleted.
 *
 * This function will return MDBX_NOTFOUND if the specified key/data
 * pair is not in the database.
 *
 * [in] txn   A transaction handle returned by mdbx_txn_begin().
 * [in] dbi   A database handle returned by mdbx_dbi_open().
 * [in] key   The key to delete from the database.
 * [in] data  The data to delete.
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_EACCES   = an attempt was made to write in a read-only transaction.
 *  - MDBX_EINVAL   = an invalid parameter was specified. */
LIBMDBX_API int mdbx_del(MDBX_txn *txn, MDBX_dbi dbi, MDBX_val *key,
                         MDBX_val *data);

/* Create a cursor handle.
 *
 * A cursor is associated with a specific transaction and database. A cursor
 * cannot be used when its database handle is closed. Nor when its transaction
 * has ended, except with mdbx_cursor_renew(). Also it can be discarded with
 * mdbx_cursor_close().
 *
 * A cursor must be closed explicitly always, before or after its transaction
 * ends. It can be reused with mdbx_cursor_renew() before finally closing it.
 *
 * NOTE: In contrast to LMDB, the MDBX required that any opened cursors can be
 * reused and must be freed explicitly, regardless ones was opened in a
 * read-only or write transaction. The REASON for this is eliminates ambiguity
 * which helps to avoid errors such as: use-after-free, double-free, i.e. memory
 * corruption and segfaults.
 *
 * [in] txn      A transaction handle returned by mdbx_txn_begin().
 * [in] dbi      A database handle returned by mdbx_dbi_open().
 * [out] cursor  Address where the new MDBX_cursor handle will be stored.
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_EINVAL   = an invalid parameter was specified. */
LIBMDBX_API int mdbx_cursor_open(MDBX_txn *txn, MDBX_dbi dbi,
                                 MDBX_cursor **cursor);

/* Close a cursor handle.
 *
 * The cursor handle will be freed and must not be used again after this call,
 * but its transaction may still be live.
 *
 * NOTE: In contrast to LMDB, the MDBX required that any opened cursors can be
 * reused and must be freed explicitly, regardless ones was opened in a
 * read-only or write transaction. The REASON for this is eliminates ambiguity
 * which helps to avoid errors such as: use-after-free, double-free, i.e. memory
 * corruption and segfaults.
 *
 * [in] cursor  A cursor handle returned by mdbx_cursor_open(). */
LIBMDBX_API void mdbx_cursor_close(MDBX_cursor *cursor);

/* Renew a cursor handle.
 *
 * A cursor is associated with a specific transaction and database. The cursor
 * may be associated with a new transaction, and referencing the same database
 * handle as it was created with. This may be done whether the previous
 * transaction is live or dead.
 *
 * NOTE: In contrast to LMDB, the MDBX allow any cursor to be re-used by using
 * mdbx_cursor_renew(), to avoid unnecessary malloc/free overhead until it freed
 * by mdbx_cursor_close().
 *
 * [in] txn     A transaction handle returned by mdbx_txn_begin().
 * [in] cursor  A cursor handle returned by mdbx_cursor_open().
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_EINVAL   = an invalid parameter was specified. */
LIBMDBX_API int mdbx_cursor_renew(MDBX_txn *txn, MDBX_cursor *cursor);

/* Return the cursor's transaction handle.
 *
 * [in] cursor A cursor handle returned by mdbx_cursor_open(). */
LIBMDBX_API MDBX_txn *mdbx_cursor_txn(MDBX_cursor *cursor);

/* Return the cursor's database handle.
 *
 * [in] cursor  A cursor handle returned by mdbx_cursor_open(). */
LIBMDBX_API MDBX_dbi mdbx_cursor_dbi(MDBX_cursor *cursor);

/* Retrieve by cursor.
 *
 * This function retrieves key/data pairs from the database. The address and
 * length of the key are returned in the object to which key refers (except
 * for the case of the MDBX_SET option, in which the key object is unchanged),
 * and the address and length of the data are returned in the object to which
 * data refers. See mdbx_get() for restrictions on using the output values.
 *
 * [in] cursor    A cursor handle returned by mdbx_cursor_open().
 * [in,out] key   The key for a retrieved item.
 * [in,out] data  The data of a retrieved item.
 * [in] op        A cursor operation MDBX_cursor_op.
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_NOTFOUND  = no matching key found.
 *  - MDBX_EINVAL    = an invalid parameter was specified. */
LIBMDBX_API int mdbx_cursor_get(MDBX_cursor *cursor, MDBX_val *key,
                                MDBX_val *data, MDBX_cursor_op op);

/* Store by cursor.
 *
 * This function stores key/data pairs into the database. The cursor is
 * positioned at the new item, or on failure usually near it.
 *
 * [in] cursor  A cursor handle returned by mdbx_cursor_open().
 * [in] key     The key operated on.
 * [in] data    The data operated on.
 * [in] flags   Options for this operation. This parameter
 *              must be set to 0 or one of the values described here:
 *
 *  - MDBX_CURRENT
 *      Replace the item at the current cursor position. The key parameter
 *      must still be provided, and must match it, otherwise the function
 *      return MDBX_EKEYMISMATCH.
 *
 *      NOTE: MDBX unlike LMDB allows you to change the size of the data and
 *      automatically handles reordering for sorted duplicates (MDBX_DUPSORT).
 *
 *  - MDBX_NODUPDATA
 *      Enter the new key/data pair only if it does not already appear in the
 *      database. This flag may only be specified if the database was opened
 *      with MDBX_DUPSORT. The function will return MDBX_KEYEXIST if the
 *      key/data pair already appears in the database.
 *
 *  - MDBX_NOOVERWRITE
 *      Enter the new key/data pair only if the key does not already appear
 *      in the database. The function will return MDBX_KEYEXIST if the key
 *      already appears in the database, even if the database supports
 *      duplicates (MDBX_DUPSORT).
 *
 *  - MDBX_RESERVE
 *      Reserve space for data of the given size, but don't copy the given
 *      data. Instead, return a pointer to the reserved space, which the
 *      caller can fill in later - before the next update operation or the
 *      transaction ends. This saves an extra memcpy if the data is being
 *      generated later. This flag must not be specified if the database
 *      was opened with MDBX_DUPSORT.
 *
 *  - MDBX_APPEND
 *      Append the given key/data pair to the end of the database. No key
 *      comparisons are performed. This option allows fast bulk loading when
 *      keys are already known to be in the correct order. Loading unsorted
 *      keys with this flag will cause a MDBX_KEYEXIST error.
 *
 *  - MDBX_APPENDDUP
 *      As above, but for sorted dup data.
 *
 *  - MDBX_MULTIPLE
 *      Store multiple contiguous data elements in a single request. This flag
 *      may only be specified if the database was opened with MDBX_DUPFIXED.
 *      The data argument must be an array of two MDBX_vals. The iov_len of the
 *      first MDBX_val must be the size of a single data element. The iov_base
 *      of the first MDBX_val must point to the beginning of the array of
 *      contiguous data elements. The iov_len of the second MDBX_val must be
 *      the count of the number of data elements to store. On return this
 *      field will be set to the count of the number of elements actually
 *      written. The iov_base of the second MDBX_val is unused.
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_EKEYMISMATCH
 *  - MDBX_MAP_FULL  = the database is full, see mdbx_env_set_mapsize().
 *  - MDBX_TXN_FULL  = the transaction has too many dirty pages.
 *  - MDBX_EACCES    = an attempt was made to write in a read-only transaction.
 *  - MDBX_EINVAL    = an invalid parameter was specified. */
LIBMDBX_API int mdbx_cursor_put(MDBX_cursor *cursor, MDBX_val *key,
                                MDBX_val *data, unsigned flags);

/* Delete current key/data pair.
 *
 * This function deletes the key/data pair to which the cursor refers. This does
 * not invalidate the cursor, so operations such as MDBX_NEXT can still be used
 * on it. Both MDBX_NEXT and MDBX_GET_CURRENT will return the same record after
 * this operation.
 *
 * [in] cursor  A cursor handle returned by mdbx_cursor_open().
 * [in] flags   Options for this operation. This parameter must be set to 0
 *              or one of the values described here.
 *
 *  - MDBX_NODUPDATA
 *      Delete all of the data items for the current key. This flag may only
 *      be specified if the database was opened with MDBX_DUPSORT.
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_EACCES  = an attempt was made to write in a read-only transaction.
 *  - MDBX_EINVAL  = an invalid parameter was specified. */
LIBMDBX_API int mdbx_cursor_del(MDBX_cursor *cursor, unsigned flags);

/* Return count of duplicates for current key.
 *
 * This call is valid for all databases, but reasonable only for that support
 * sorted duplicate data items MDBX_DUPSORT.
 *
 * [in] cursor    A cursor handle returned by mdbx_cursor_open().
 * [out] countp   Address where the count will be stored.
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_EINVAL   = cursor is not initialized, or an invalid parameter
 *                    was specified. */
LIBMDBX_API int mdbx_cursor_count(MDBX_cursor *cursor, size_t *countp);

/* Determines whether the cursor is pointed to a key-value pair or not,
 * i.e. was not positioned or points to the end of data.
 *
 * [in] cursor    A cursor handle returned by mdbx_cursor_open().
 *
 * Returns:
 *  - MDBX_RESULT_TRUE    = no more data available or cursor not positioned;
 *  - MDBX_RESULT_FALSE   = data available;
 *  - Otherwise the error code. */
LIBMDBX_API int mdbx_cursor_eof(MDBX_cursor *mc);

/* Determines whether the cursor is pointed to the first key-value pair or not.
 *
 * [in] cursor    A cursor handle returned by mdbx_cursor_open().
 *
 * Returns:
 *  - MDBX_RESULT_TRUE    = cursor positioned to the first key-value pair.
 *  - MDBX_RESULT_FALSE   = cursor NOT positioned to the first key-value pair.
 *  - Otherwise the error code. */
LIBMDBX_API int mdbx_cursor_on_first(MDBX_cursor *mc);

/* Determines whether the cursor is pointed to the last key-value pair or not.
 *
 * [in] cursor    A cursor handle returned by mdbx_cursor_open().
 *
 * Returns:
 *  - MDBX_RESULT_TRUE    = cursor positioned to the last key-value pair.
 *  - MDBX_RESULT_FALSE   = cursor NOT positioned to the last key-value pair.
 *  - Otherwise the error code. */
LIBMDBX_API int mdbx_cursor_on_last(MDBX_cursor *mc);

/* Estimates the distance between cursors as a number of elements. The results
 * of such estimation can be used to build and/or optimize query execution
 * plans.
 *
 * This function performs a rough estimate based only on b-tree pages that are
 * common for the both cursor's stacks.
 *
 * NOTE: The result varies greatly depending on the filling of specific pages
 * and the overall balance of the b-tree:
 *
 * 1. The number of items is estimated by analyzing the height and fullness of
 * the b-tree. The accuracy of the result directly depends on the balance of the
 * b-tree, which in turn is determined by the history of previous insert/delete
 * operations and the nature of the data (i.e. variability of keys length and so
 * on). Therefore, the accuracy of the estimation can vary greatly in a
 * particular situation.
 *
 * 2. To understand the potential spread of results, you should consider a
 * possible situations basing on the general criteria for splitting and merging
 * b-tree pages:
 *  - the page is split into two when there is no space for added data;
 *  - two pages merge if the result fits in half a page;
 *  - thus, the b-tree can consist of an arbitrary combination of pages filled
 *    both completely and only 1/4. Therefore, in the worst case, the result
 *    can diverge 4 times for each level of the b-tree excepting the first and
 *    the last.
 *
 * 3. In practice, the probability of extreme cases of the above situation is
 * close to zero and in most cases the error does not exceed a few percent. On
 * the other hand, it's just a chance you shouldn't overestimate.
 *
 * Both cursors must be initialized for the same database and the same
 * transaction.
 *
 * [in] first             The first cursor for estimation.
 * [in] last              The second cursor for estimation.
 * [out] distance_items   A pointer to store estimated distance value,
 *                        i.e. *distance_items = distance(first, last).
 *
 * Returns A non-zero error value on failure and 0 on success. */
LIBMDBX_API int mdbx_estimate_distance(const MDBX_cursor *first,
                                       const MDBX_cursor *last,
                                       ptrdiff_t *distance_items);

/* Estimates the move distance, i.e. between the current cursor position and
 * next position after the specified move-operation with given key and data.
 * The results of such estimation can be used to build and/or optimize query
 * execution plans. Current cursor position and state are preserved.
 *
 * Please see notes on accuracy of the result in mdbx_estimate_distance()
 * description above.
 *
 * [in] cursor            Cursor for estimation.
 * [in,out] key           The key for a retrieved item.
 * [in,out] data          The data of a retrieved item.
 * [in] op                A cursor operation MDBX_cursor_op.
 * [out] distance_items   A pointer to store estimated move distance
 *                        as the number of elements.
 *
 * Returns A non-zero error value on failure and 0 on success. */
LIBMDBX_API int mdbx_estimate_move(const MDBX_cursor *cursor, MDBX_val *key,
                                   MDBX_val *data, MDBX_cursor_op move_op,
                                   ptrdiff_t *distance_items);

/* Estimates the size of a range as a number of elements. The results
 * of such estimation can be used to build and/or optimize query execution
 * plans.
 *
 * Please see notes on accuracy of the result in mdbx_estimate_distance()
 * description above.
 *
 * [in] txn               A transaction handle returned by mdbx_txn_begin().
 * [in] dbi               A database handle returned by mdbx_dbi_open().
 * [in] begin_key         The key of range beginning or NULL for explicit FIRST.
 * [in] begin_data        Optional additional data to seeking among sorted
 *                        duplicates. Only for MDBX_DUPSORT, NULL otherwise.
 * [in] end_key           The key of range ending or NULL for explicit LAST.
 * [in] end_data          Optional additional data to seeking among sorted
 *                        duplicates. Only for MDBX_DUPSORT, NULL otherwise.
 * [out] distance_items   A pointer to store range estimation result.
 *
 * Returns A non-zero error value on failure and 0 on success. */
#define MDBX_EPSILON ((MDBX_val *)((ptrdiff_t)-1))
LIBMDBX_API int mdbx_estimate_range(MDBX_txn *txn, MDBX_dbi dbi,
                                    MDBX_val *begin_key, MDBX_val *begin_data,
                                    MDBX_val *end_key, MDBX_val *end_data,
                                    ptrdiff_t *size_items);

/* Determines whether the given address is on a dirty database page of the
 * transaction or not. Ultimately, this allows to avoid copy data from non-dirty
 * pages.
 *
 * "Dirty" pages are those that have already been changed during a write
 * transaction. Accordingly, any further changes may result in such pages being
 * overwritten. Therefore, all functions libmdbx performing changes inside the
 * database as arguments should NOT get pointers to data in those pages. In
 * turn, "not dirty" pages before modification will be copied.
 *
 * In other words, data from dirty pages must either be copied before being
 * passed as arguments for further processing or rejected at the argument
 * validation stage. Thus, mdbx_is_dirty() allows you to get rid of unnecessary
 * copying, and perform a more complete check of the arguments.
 *
 * NOTE: The address passed must point to the beginning of the data. This is the
 * only way to ensure that the actual page header is physically located in the
 * same memory page, including for multi-pages with long data.
 *
 * NOTE: In rare cases the function may return a false positive answer
 * (DBX_RESULT_TRUE when data is NOT on a dirty page), but never a false
 * negative if the arguments are correct.
 *
 * [in] txn      A transaction handle returned by mdbx_txn_begin().
 * [in] ptr      The address of data to check.
 *
 * Returns:
 *  - MDBX_RESULT_TRUE    = given address is on the dirty page.
 *  - MDBX_RESULT_FALSE   = given address is NOT on the dirty page.
 *  - Otherwise the error code. */
LIBMDBX_API int mdbx_is_dirty(const MDBX_txn *txn, const void *ptr);

/* Sequence generation for a database.
 *
 * The function allows to create a linear sequence of unique positive integers
 * for each database. The function can be called for a read transaction to
 * retrieve the current sequence value, and the increment must be zero.
 * Sequence changes become visible outside the current write transaction after
 * it is committed, and discarded on abort.
 *
 * [in] txn        A transaction handle returned by mdbx_txn_begin().
 * [in] dbi        A database handle returned by mdbx_dbi_open().
 * [out] result    The optional address where the value of sequence before the
 *                 change will be stored.
 * [in] increment  Value to increase the sequence,
 *                 must be 0 for read-only transactions.
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_RESULT_TRUE   = Increasing the sequence has resulted in an overflow
 *                         and therefore cannot be executed. */
LIBMDBX_API int mdbx_dbi_sequence(MDBX_txn *txn, MDBX_dbi dbi, uint64_t *result,
                                  uint64_t increment);

/* Compare two data items according to a particular database.
 *
 * This returns a comparison as if the two data items were keys in the
 * specified database.
 *
 * [in] txn   A transaction handle returned by mdbx_txn_begin().
 * [in] dbi   A database handle returned by mdbx_dbi_open().
 * [in] a     The first item to compare.
 * [in] b     The second item to compare.
 *
 * Returns < 0 if a < b, 0 if a == b, > 0 if a > b */
LIBMDBX_API int mdbx_cmp(MDBX_txn *txn, MDBX_dbi dbi, const MDBX_val *a,
                         const MDBX_val *b);

/* Compare two data items according to a particular database.
 *
 * This returns a comparison as if the two items were data items of the
 * specified database. The database must have the MDBX_DUPSORT flag.
 *
 * [in] txn   A transaction handle returned by mdbx_txn_begin().
 * [in] dbi   A database handle returned by mdbx_dbi_open().
 * [in] a     The first item to compare.
 * [in] b     The second item to compare.
 *
 * Returns < 0 if a < b, 0 if a == b, > 0 if a > b */
LIBMDBX_API int mdbx_dcmp(MDBX_txn *txn, MDBX_dbi dbi, const MDBX_val *a,
                          const MDBX_val *b);

/* A callback function used to enumerate the reader lock table.
 *
 * [in] ctx           An arbitrary context pointer for the callback.
 * [in] num           The serial number during enumeration, starting from 1.
 * [in] slot          The reader lock table slot number.
 * [in] txnid         The ID of the transaction being read,
 *                    i.e. the MVCC-snaphot number.
 * [in] lag           The lag from a recent MVCC-snapshot, i.e. the number of
 *                    committed transaction since read transaction started.
 * [in] pid           The reader process ID.
 * [in] thread        The reader thread ID.
 * [in] bytes_used    The number of last used page in the MVCC-snapshot which
 *                    being read, i.e. database file can't shrinked beyond this.
 * [in] bytes_retired The total size of the database pages that were retired by
 *                    committed write transactions after the reader's
 *                    MVCC-snapshot, i.e. the space which would be freed after
 *                    the Reader releases the MVCC-snapshot for reuse by
 *                    completion read transaction.
 *
 * Returns < 0 on failure, >= 0 on success. */
typedef int(MDBX_reader_list_func)(void *ctx, int num, int slot, mdbx_pid_t pid,
                                   mdbx_tid_t thread, uint64_t txnid,
                                   uint64_t lag, size_t bytes_used,
                                   size_t bytes_retained);

/* Enumarete the entries in the reader lock table.
 *
 * [in] env     An environment handle returned by mdbx_env_create().
 * [in] func    A MDBX_reader_list_func function.
 * [in] ctx     An arbitrary context pointer for the enumeration function.
 *
 * Returns A non-zero error value on failure and 0 on success,
 * or MDBX_RESULT_TRUE (-1) if the reader lock table is empty. */
LIBMDBX_API int mdbx_reader_list(MDBX_env *env, MDBX_reader_list_func *func,
                                 void *ctx);

/* Check for stale entries in the reader lock table.
 *
 * [in] env     An environment handle returned by mdbx_env_create().
 * [out] dead   Number of stale slots that were cleared.
 *
 * Returns A non-zero error value on failure and 0 on success,
 * or MDBX_RESULT_TRUE (-1) if a dead reader(s) found or mutex was recovered. */
LIBMDBX_API int mdbx_reader_check(MDBX_env *env, int *dead);

/* Returns a lag of the reading for the given transaction.
 *
 * Returns an information for estimate how much given read-only
 * transaction is lagging relative the to actual head.
 * This is deprecated function, use mdbx_txn_info() instead.
 *
 * [in] txn       A transaction handle returned by mdbx_txn_begin().
 * [out] percent  Percentage of page allocation in the database.
 *
 * Returns Number of transactions committed after the given was started for
 * read, or negative value on failure. */
__deprecated LIBMDBX_API int mdbx_txn_straggler(MDBX_txn *txn, int *percent);

/* A lack-of-space callback function to resolve issues with a laggard readers.
 *
 * Read transactions prevent reuse of pages freed by newer write transactions,
 * thus the database can grow quickly. This callback will be called when there
 * is not enough space in the database (ie. before increasing the database size
 * or before MDBX_MAP_FULL error) and thus can be used to resolve issues with
 * a "long-lived" read transactions.
 *
 * Depending on the arguments and needs, your implementation may wait, terminate
 * a process or thread that is performing a long read, or perform some other
 * action. In doing so it is important that the returned code always corresponds
 * to the performed action.
 *
 * [in] env     An environment handle returned by mdbx_env_create().
 * [in] pid     A pid of the reader process.
 * [in] tid     A thread_id of the reader thread.
 * [in] txn     A transaction number on which stalled.
 * [in] gap     A lag from the last commited txn.
 * [in] space   A space that actually become available for reuse after this
 *              reader finished. The callback function can take this value into
 *              account to evaluate the impact that a long-running transaction
 *              has.
 * [in] retry   A retry number starting from 0. if callback has returned 0
 *              at least once, then at end of current OOM-handler loop callback
 *              will be called additionally with negative value to notify about
 *              the end of loop. The callback function can use this value to
 *              implement timeout logic while waiting for readers.
 *
 * The RETURN CODE determines the further actions libmdbx and must match the
 * action which was executed by the callback:
 *
 *   -2 or less      = An error condition and the reader was not killed.
 *
 *   -1              = The callback was unable to solve the problem and agreed
 *                     on MDBX_MAP_FULL error, libmdbx should increase the
 *                     database size or return MDBX_MAP_FULL error.
 *
 *    0 (zero)       = The callback solved the problem or just waited for
 *                     a while, libmdbx should rescan the reader lock table and
 *                     retry. This also includes a situation when corresponding
 *                     transaction terminated in normal way by mdbx_txn_abort()
 *                     or mdbx_txn_reset(), and my be restarted. I.e. reader
 *                     slot don't needed to be cleaned from transaction.
 *
 *    1              = Transaction aborted asynchronous and reader slot should
 *                     be cleared immediately, i.e. read transaction will not
 *                     continue but mdbx_txn_abort() or mdbx_txn_reset() will
 *                     be called later.
 *
 *    2 or great     = The reader process was terminated or killed, and libmdbx
 *                     should entirely reset reader registration. */
typedef int(MDBX_oom_func)(MDBX_env *env, mdbx_pid_t pid, mdbx_tid_t tid,
                           uint64_t txn, unsigned gap, size_t space, int retry);

/* Set the OOM callback.
 *
 * The callback will only be triggered on lack of space to resolve issues with
 * lagging reader(s) (i.e. to kill it) for resume reuse pages from the garbage
 * collector.
 *
 * [in] env        An environment handle returned by mdbx_env_create().
 * [in] oom_func   A MDBX_oom_func function or NULL to disable.
 *
 * Returns A non-zero error value on failure and 0 on success. */
LIBMDBX_API int mdbx_env_set_oomfunc(MDBX_env *env, MDBX_oom_func *oom_func);

/* Get the current oom_func callback.
 *
 * [in] env   An environment handle returned by mdbx_env_create().
 *
 * Returns A MDBX_oom_func function or NULL if disabled. */
LIBMDBX_API MDBX_oom_func *mdbx_env_get_oomfunc(MDBX_env *env);

/**** B-tree Traversal *********************************************************
 * This is internal API for mdbx_chk tool. You should avoid to use it, except
 * some extremal special cases. */

/* Page types for traverse the b-tree. */
typedef enum {
  MDBX_page_void,
  MDBX_page_meta,
  MDBX_page_large,
  MDBX_page_branch,
  MDBX_page_leaf,
  MDBX_page_dupfixed_leaf,
  MDBX_subpage_leaf,
  MDBX_subpage_dupfixed_leaf
} MDBX_page_type_t;

#define MDBX_PGWALK_MAIN ((const char *)((ptrdiff_t)0))
#define MDBX_PGWALK_GC ((const char *)((ptrdiff_t)-1))
#define MDBX_PGWALK_META ((const char *)((ptrdiff_t)-2))

/* Callback function for traverse the b-tree. */
typedef int
MDBX_pgvisitor_func(const uint64_t pgno, const unsigned number, void *const ctx,
                    const int deep, const char *const dbi,
                    const size_t page_size, const MDBX_page_type_t type,
                    const size_t nentries, const size_t payload_bytes,
                    const size_t header_bytes, const size_t unused_bytes);

/* B-tree traversal function. */
LIBMDBX_API int mdbx_env_pgwalk(MDBX_txn *txn, MDBX_pgvisitor_func *visitor,
                                void *ctx);

/**** Attribute support functions for Nexenta *********************************/
#ifdef MDBX_NEXENTA_ATTRS
typedef uint_fast64_t mdbx_attr_t;

/* Store by cursor with attribute.
 *
 * This function stores key/data pairs into the database. The cursor is
 * positioned at the new item, or on failure usually near it.
 *
 * NOTE: Internally based on MDBX_RESERVE feature,
 *       therefore doesn't support MDBX_DUPSORT.
 *
 * [in] cursor  A cursor handle returned by mdbx_cursor_open()
 * [in] key     The key operated on.
 * [in] data    The data operated on.
 * [in] attr    The attribute.
 * [in] flags   Options for this operation. This parameter must be set to 0
 *              or one of the values described here:
 *
 *  - MDBX_CURRENT
 *      Replace the item at the current cursor position. The key parameter
 *      must still be provided, and must match it, otherwise the function
 *      return MDBX_EKEYMISMATCH.
 *
 *  - MDBX_APPEND
 *      Append the given key/data pair to the end of the database. No key
 *      comparisons are performed. This option allows fast bulk loading when
 *      keys are already known to be in the correct order. Loading unsorted
 *      keys with this flag will cause a MDBX_KEYEXIST error.
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_EKEYMISMATCH
 *  - MDBX_MAP_FULL  = the database is full, see mdbx_env_set_mapsize().
 *  - MDBX_TXN_FULL  = the transaction has too many dirty pages.
 *  - MDBX_EACCES    = an attempt was made to write in a read-only transaction.
 *  - MDBX_EINVAL    = an invalid parameter was specified. */
LIBMDBX_API int mdbx_cursor_put_attr(MDBX_cursor *cursor, MDBX_val *key,
                                     MDBX_val *data, mdbx_attr_t attr,
                                     unsigned flags);

/* Store items and attributes into a database.
 *
 * This function stores key/data pairs in the database. The default behavior
 * is to enter the new key/data pair, replacing any previously existing key
 * if duplicates are disallowed.
 *
 * NOTE: Internally based on MDBX_RESERVE feature,
 *       therefore doesn't support MDBX_DUPSORT.
 *
 * [in] txn       A transaction handle returned by mdbx_txn_begin().
 * [in] dbi       A database handle returned by mdbx_dbi_open().
 * [in] key       The key to store in the database.
 * [in] attr      The attribute to store in the database.
 * [in,out] data  The data to store.
 * [in] flags     Special options for this operation. This parameter must be
 *                set to 0 or by bitwise OR'ing together one or more of the
 *                values described here:
 *
 *  - MDBX_NOOVERWRITE
 *      Enter the new key/data pair only if the key does not already appear
 *      in the database. The function will return MDBX_KEYEXIST if the key
 *      already appears in the database. The data parameter will be set to
 *      point to the existing item.
 *
 *  - MDBX_CURRENT
 *      Update an single existing entry, but not add new ones. The function
 *      will return MDBX_NOTFOUND if the given key not exist in the database.
 *      Or the MDBX_EMULTIVAL in case duplicates for the given key.
 *
 *  - MDBX_APPEND
 *      Append the given key/data pair to the end of the database. This option
 *      allows fast bulk loading when keys are already known to be in the
 *      correct order. Loading unsorted keys with this flag will cause
 *      a MDBX_EKEYMISMATCH error.
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_KEYEXIST
 *  - MDBX_MAP_FULL  = the database is full, see mdbx_env_set_mapsize().
 *  - MDBX_TXN_FULL  = the transaction has too many dirty pages.
 *  - MDBX_EACCES    = an attempt was made to write in a read-only transaction.
 *  - MDBX_EINVAL    = an invalid parameter was specified. */
LIBMDBX_API int mdbx_put_attr(MDBX_txn *txn, MDBX_dbi dbi, MDBX_val *key,
                              MDBX_val *data, mdbx_attr_t attr, unsigned flags);

/* Set items attribute from a database.
 *
 * This function stores key/data pairs attribute to the database.
 *
 * NOTE: Internally based on MDBX_RESERVE feature,
 *       therefore doesn't support MDBX_DUPSORT.
 *
 * [in] txn   A transaction handle returned by mdbx_txn_begin().
 * [in] dbi   A database handle returned by mdbx_dbi_open().
 * [in] key   The key to search for in the database.
 * [in] data  The data to be stored or NULL to save previous value.
 * [in] attr  The attribute to be stored.
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_NOTFOUND   = the key-value pair was not in the database.
 *  - MDBX_EINVAL     = an invalid parameter was specified. */
LIBMDBX_API int mdbx_set_attr(MDBX_txn *txn, MDBX_dbi dbi, MDBX_val *key,
                              MDBX_val *data, mdbx_attr_t attr);

/* Get items attribute from a database cursor.
 *
 * This function retrieves key/data pairs from the database. The address and
 * length of the key are returned in the object to which key refers (except
 * for the case of the MDBX_SET option, in which the key object is unchanged),
 * and the address and length of the data are returned in the object to which
 * data refers. See mdbx_get() for restrictions on using the output values.
 *
 * [in] cursor    A cursor handle returned by mdbx_cursor_open().
 * [in,out] key   The key for a retrieved item.
 * [in,out] data  The data of a retrieved item.
 * [in] op        A cursor operation MDBX_cursor_op.
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_NOTFOUND  = no matching key found.
 *  - MDBX_EINVAL    = an invalid parameter was specified. */
LIBMDBX_API int mdbx_cursor_get_attr(MDBX_cursor *mc, MDBX_val *key,
                                     MDBX_val *data, mdbx_attr_t *attrptr,
                                     MDBX_cursor_op op);

/* Get items attribute from a database.
 *
 * This function retrieves key/data pairs from the database. The address
 * and length of the data associated with the specified key are returned
 * in the structure to which data refers.
 * If the database supports duplicate keys (MDBX_DUPSORT) then the
 * first data item for the key will be returned. Retrieval of other
 * items requires the use of mdbx_cursor_get().
 *
 * NOTE: The memory pointed to by the returned values is owned by the
 * database. The caller need not dispose of the memory, and may not
 * modify it in any way. For values returned in a read-only transaction
 * any modification attempts will cause a SIGSEGV.
 *
 * NOTE: Values returned from the database are valid only until a
 * subsequent update operation, or the end of the transaction.
 *
 * [in] txn       A transaction handle returned by mdbx_txn_begin().
 * [in] dbi       A database handle returned by mdbx_dbi_open().
 * [in] key       The key to search for in the database.
 * [in,out] data  The data corresponding to the key.
 *
 * Returns A non-zero error value on failure and 0 on success, some
 * possible errors are:
 *  - MDBX_NOTFOUND  = the key was not in the database.
 *  - MDBX_EINVAL    = an invalid parameter was specified. */
LIBMDBX_API int mdbx_get_attr(MDBX_txn *txn, MDBX_dbi dbi, MDBX_val *key,
                              MDBX_val *data, mdbx_attr_t *attrptr);
#endif /* MDBX_NEXENTA_ATTRS */

/*******************************************************************************
 * Workaround for mmaped-lookahead-cross-page-boundary bug
 * in an obsolete versions of Elbrus's libc and kernels. */
#if defined(__e2k__) && defined(MDBX_E2K_MLHCPB_WORKAROUND) &&                 \
    MDBX_E2K_MLHCPB_WORKAROUND
LIBMDBX_API int mdbx_e2k_memcmp_bug_workaround(const void *s1, const void *s2,
                                               size_t n);
LIBMDBX_API int mdbx_e2k_strcmp_bug_workaround(const char *s1, const char *s2);
LIBMDBX_API int mdbx_e2k_strncmp_bug_workaround(const char *s1, const char *s2,
                                                size_t n);
LIBMDBX_API size_t mdbx_e2k_strlen_bug_workaround(const char *s);
LIBMDBX_API size_t mdbx_e2k_strnlen_bug_workaround(const char *s,
                                                   size_t maxlen);
#include <string.h>
#include <strings.h>
#undef memcmp
#define memcmp mdbx_e2k_memcmp_bug_workaround
#undef bcmp
#define bcmp mdbx_e2k_memcmp_bug_workaround
#undef strcmp
#define strcmp mdbx_e2k_strcmp_bug_workaround
#undef strncmp
#define strncmp mdbx_e2k_strncmp_bug_workaround
#undef strlen
#define strlen mdbx_e2k_strlen_bug_workaround
#undef strnlen
#define strnlen mdbx_e2k_strnlen_bug_workaround
#endif /* MDBX_E2K_MLHCPB_WORKAROUND */

#ifdef __cplusplus
}
#endif

#endif /* LIBMDBX_H */
