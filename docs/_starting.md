Getting started {#starting}
===============

> This section is based on Bert Hubert's intro "LMDB Semantics", with
> edits reflecting the improvements and enhancements were made in MDBX.
> See Bert Hubert's [original](https://github.com/ahupowerdns/ahutils/blob/master/lmdb-semantics.md).

Everything starts with an environment, created by \ref mdbx_env_create().
Once created, this environment must also be opened with \ref mdbx_env_open(),
and after use be closed by \ref mdbx_env_close(). At that a non-zero value
of the last argument "mode" supposes MDBX will create database and directory
if ones does not exist. In this case the non-zero "mode" argument specifies
the file mode bits be applied when a new files are created by `open()` function.

Within that directory, a lock file (aka LCK-file) and a storage file (aka
DXB-file) will be generated. If you don't want to use a directory, you can
pass the \ref MDBX_NOSUBDIR option, in which case the path you provided is used
directly as the DXB-file, and another file with a "-lck" suffix added
will be used for the LCK-file.

Once the environment is open, a transaction can be created within it using
\ref mdbx_txn_begin(). Transactions may be read-write or read-only, and read-write
transactions may be nested. A transaction must only be used by one thread at
a time. Transactions are always required, even for read-only access. The
transaction provides a consistent view of the data.

Once a transaction has been created, a database (i.e. key-value space inside
the environment) can be opened within it using \ref mdbx_dbi_open(). If only one
database will ever be used in the environment, a `NULL` can be passed as the
database name. For named databases, the \ref MDBX_CREATE flag must be used to
create the database if it doesn't already exist. Also, \ref mdbx_env_set_maxdbs()
must be called after \ref mdbx_env_create() and before \ref mdbx_env_open() to set
the maximum number of named databases you want to support.

\note A single transaction can open multiple databases. Generally databases
should only be opened once, by the first transaction in the process.

Within a transaction, \ref mdbx_get() and \ref mdbx_put() can store single key-value
pairs if that is all you need to do (but see \ref Cursors below if you want to do
more).

A key-value pair is expressed as two \ref MDBX_val structures. This struct that is
exactly similar to POSIX's `struct iovec` and has two fields, `iov_len` and
`iov_base`. The data is a `void` pointer to an array of `iov_len` bytes.
\note The notable difference between MDBX and LMDB is that MDBX support zero
length keys.

Because MDBX is very efficient (and usually zero-copy), the data returned in
an \ref MDBX_val structure may be memory-mapped straight from disk. In other words
look but do not touch (or `free()` for that matter). Once a transaction is
closed, the values can no longer be used, so make a copy if you need to keep
them after that.

## Cursors {#Cursors}
To do more powerful things, we must use a cursor.

Within the transaction, a cursor can be created with \ref mdbx_cursor_open().
With this cursor we can store/retrieve/delete (multiple) values using
\ref mdbx_cursor_get(), \ref mdbx_cursor_put() and \ref mdbx_cursor_del().

The \ref mdbx_cursor_get() positions itself depending on the cursor operation
requested, and for some operations, on the supplied key. For example, to list
all key-value pairs in a database, use operation \ref MDBX_FIRST for the first
call to \ref mdbx_cursor_get(), and \ref MDBX_NEXT on subsequent calls, until
the end is hit.

To retrieve all keys starting from a specified key value, use \ref MDBX_SET. For
more cursor operations, see the \ref c_api reference.

When using \ref mdbx_cursor_put()\ref , either the function will position the cursor
for you based on the key, or you can use operation \ref MDBX_CURRENT to use the
current position of the cursor. \note Note that key must then match the current
position's key.


## Summarizing the opening

So we have a cursor in a transaction which opened a database in an
environment which is opened from a filesystem after it was separately
created.

Or, we create an environment, open it from a filesystem, create a transaction
within it, open a database within that transaction, and create a cursor
within all of the above.

Got it?


## Threads and processes

Do not have open an database twice in the same process at the same time, MDBX
will track and prevent this. Instead, share the MDBX environment that has
opened the file across all threads. The reason for this is:
 - When the "Open file description" locks (aka OFD-locks) are not available,
   MDBX uses POSIX locks on files, and these locks have issues if one process
   opens a file multiple times.
 - If a single process opens the same environment multiple times, closing it
   once will remove all the locks held on it, and the other instances will be
   vulnerable to corruption from other processes.
 + For compatibility with LMDB which allows multi-opening, MDBX can be
   configured at runtime by \ref mdbx_setup_debug() with \ref MDBX_DBG_LEGACY_MULTIOPEN` option
   prior to calling other MDBX functions. In this way MDBX will track
   databases opening, detect multi-opening cases and then recover POSIX file
   locks as necessary. However, lock recovery can cause unexpected pauses,
   such as when another process opened the database in exclusive mode before
   the lock was restored - we have to wait until such a process releases the
   database, and so on.

Do not use opened MDBX environment(s) after `fork()` in a child process(es),
MDBX will check and prevent this at critical points. Instead, ensure there is
no open MDBX-instance(s) during fork(), or at least close it immediately after
`fork()` in the child process and reopen if required - for instance by using
`pthread_atfork()`. The reason for this is:
 - For competitive consistent reading, MDBX assigns a slot in the shared
   table for each process that interacts with the database. This slot is
   populated with process attributes, including the PID.
 - After `fork()`, in order to remain connected to a database, the child
   process must have its own such "slot", which can't be assigned in any
   simple and robust way another than the regular.
 - A write transaction from a parent process cannot continue in a child
   process for obvious reasons.
 - Moreover, in a multithreaded process at the fork() moment any number of
   threads could run in critical and/or intermediate sections of MDBX code
   with interaction and/or racing conditions with threads from other
   process(es). For instance: shrinking a database or copying it to a pipe,
   opening or closing environment, beginning or finishing a transaction,
   and so on.
 = Therefore, any solution other than simply close database (and reopen if
   necessary) in a child process would be both extreme complicated and so
   fragile.

Do not start more than one transaction for a one thread. If you think about
this, it's really strange to do something with two data snapshots at once,
which may be different. MDBX checks and preventing this by returning
corresponding error code (\ref MDBX_TXN_OVERLAPPING, \ref MDBX_BAD_RSLOT, \ref MDBX_BUSY)
unless you using \ref MDBX_NOTLS option on the environment. Nonetheless, with the
\ref MDBX_NOTLS option, you must know exactly what you are doing, otherwise you
will get deadlocks or reading an alien data.

Also note that a transaction is tied to one thread by default using Thread
Local Storage. If you want to pass read-only transactions across threads,
you can use the \ref MDBX_NOTLS option on the environment. Nevertheless, a write
transaction entirely should only be used in one thread from start to finish.
MDBX checks this in a reasonable manner and return the \ref MDBX_THREAD_MISMATCH
error in rules violation.


## Transactions, rollbacks etc

To actually get anything done, a transaction must be committed using
\ref mdbx_txn_commit(). Alternatively, all of a transaction's operations
can be discarded using \ref mdbx_txn_abort().

\attention An important difference between MDBX and LMDB is that MDBX required
that any opened cursors can be reused and must be freed explicitly, regardless
ones was opened in a read-only or write transaction. The REASON for this is
eliminates ambiguity which helps to avoid errors such as: use-after-free,
double-free, i.e. memory corruption and segfaults.

For read-only transactions, obviously there is nothing to commit to storage.
\attention An another notable difference between MDBX and LMDB is that MDBX make
handles opened for existing databases immediately available for other
transactions, regardless this transaction will be aborted or reset. The
REASON for this is to avoiding the requirement for multiple opening a same
handles in concurrent read transactions, and tracking of such open but hidden
handles until the completion of read transactions which opened them.

In addition, as long as a transaction is open, a consistent view of the
database is kept alive, which requires storage. A read-only transaction that
no longer requires this consistent view should be terminated (committed or
aborted) when the view is no longer needed (but see below for an
optimization).

There can be multiple simultaneously active read-only transactions but only
one that can write. Once a single read-write transaction is opened, all
further attempts to begin one will block until the first one is committed or
aborted. This has no effect on read-only transactions, however, and they may
continue to be opened at any time.


## Duplicate keys aka Multi-values

\ref mdbx_get() and \ref mdbx_put() respectively have no and only some support or
multiple key-value pairs with identical keys. If there are multiple values
for a key, \ref mdbx_get() will only return the first value.

When multiple values for one key are required, pass the \ref MDBX_DUPSORT flag to
\ref mdbx_dbi_open(). In an \ref MDBX_DUPSORT database, by default \ref mdbx_put() will
not replace the value for a key if the key existed already. Instead it will add
the new value to the key. In addition, \ref mdbx_del() will pay attention to the
value field too, allowing for specific values of a key to be deleted.

Finally, additional cursor operations become available for traversing through
and retrieving duplicate values.


## Some optimization

If you frequently begin and abort read-only transactions, as an optimization,
it is possible to only reset and renew a transaction.

\ref mdbx_txn_reset() releases any old copies of data kept around for a read-only
transaction. To reuse this reset transaction, call \ref mdbx_txn_renew() on it.
Any cursors in this transaction can also be renewed using \ref mdbx_cursor_renew()
or freed by \ref mdbx_cursor_close().

To permanently free a transaction, reset or not, use \ref mdbx_txn_abort().


## Cleaning up

Any created cursors must be closed using \ref mdbx_cursor_close(). It is advisable
to repeat:
\note An important difference between MDBX and LMDB is that MDBX required that
any opened cursors can be reused and must be freed explicitly, regardless
ones was opened in a read-only or write transaction. The REASON for this is
eliminates ambiguity which helps to avoid errors such as: use-after-free,
double-free, i.e. memory corruption and segfaults.

It is very rarely necessary to close a database handle, and in general they
should just be left open. When you close a handle, it immediately becomes
unavailable for all transactions in the environment. Therefore, you should
avoid closing the handle while at least one transaction is using it.


## Now read up on the full API!

The full \ref c_api documentation lists further details below, like how to:

- Configure database size and automatic size management: \ref mdbx_env_set_geometry().
- Drop and clean a database: \ref mdbx_drop().
- Detect and report errors: \ref c_err.
- Optimize (bulk) loading speed: \ref MDBX_MULTIPLE, \ref MDBX_APPEND.
- Reduce (temporarily) robustness to gain even more speed: \ref sync_modes.
- Gather statistics about the database: \ref c_statinfo.
- Sstimate size of range query result: \ref c_rqest.
- Double performance by LIFO reclaiming on storages with write-back: \ref MDBX_LIFORECLAIM.
- Use sequences and canary markers: \ref mdbx_dbi_sequence(), \ref MDBX_canary.
- Use Handle-Slow-Readers callback to resolve a database full/overflow issues
  due to long-lived read transactions: \ref mdbx_env_set_hsr().
- Use exclusive mode: \ref MDBX_EXCLUSIVE.
- Define custom sort orders (but this is recommended to be avoided).
