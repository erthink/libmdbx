### The [repository now only mirrored on the Github](https://abf.io/erthink/libmdbx) due to illegal discriminatory restrictions for Russian Crimea and for sovereign crimeans.
<!-- Required extensions: pymdownx.betterem, pymdownx.tilde, pymdownx.emoji, pymdownx.tasklist, pymdownx.superfences -->
-----

libmdbx
======================================

_libmdbx_ is an extremely fast, compact, powerful, embedded
transactional [key-value
store](https://en.wikipedia.org/wiki/Key-value_database)
database, with permissive [OpenLDAP Public License](LICENSE).
_libmdbx_ has a specific set of properties and capabilities,
focused on creating unique lightweight solutions with
extraordinary performance.

The next version is under active non-public development and will be
released as **_MithrilDB_** and `libmithrildb` for libraries & packages.
Admittedly mythical [Mithril](https://en.wikipedia.org/wiki/Mithril) is
resembling silver but being stronger and lighter than steel. Therefore
_MithrilDB_ is rightly relevant name.
> _MithrilDB_ will be radically different from _libmdbx_ by the new
> database format and API based on C++17, as well as the [Apache 2.0
> License](https://www.apache.org/licenses/LICENSE-2.0). The goal of this
> revolution is to provide a clearer and robust API, add more features and
> new valuable properties of database.

*The Future will (be) [Positive](https://www.ptsecurity.com). Всё будет хорошо.*

[![Build Status](https://travis-ci.org/leo-yuriev/libmdbx.svg?branch=master)](https://travis-ci.org/leo-yuriev/libmdbx)
[![Build status](https://ci.appveyor.com/api/projects/status/ue94mlopn50dqiqg/branch/master?svg=true)](https://ci.appveyor.com/project/leo-yuriev/libmdbx/branch/master)
[![Coverity Scan Status](https://scan.coverity.com/projects/12915/badge.svg)](https://scan.coverity.com/projects/reopen-libmdbx)

## Table of Contents
- [Overview](#overview)
    - [Comparison with other databases](#comparison-with-other-databases)
    - [History & Acknowledgments](#history)
- [Description](#description)
    - [Key features](#key-features)
    - [Improvements over LMDB](#improvements-over-lmdb)
    - [Gotchas](#gotchas)
- [Usage](#usage)
    - [Building](#building)
    - [Bindings](#bindings)
- [Performance comparison](#performance-comparison)
    - [Integral performance](#integral-performance)
    - [Read scalability](#read-scalability)
    - [Sync-write mode](#sync-write-mode)
    - [Lazy-write mode](#lazy-write-mode)
    - [Async-write mode](#async-write-mode)
    - [Cost comparison](#cost-comparison)

-----

## Overview

_libmdbx_ is revised and extended descendant of amazing [Lightning
Memory-Mapped
Database](https://en.wikipedia.org/wiki/Lightning_Memory-Mapped_Database).
_libmdbx_ inherits all features and characteristics from
[LMDB](https://en.wikipedia.org/wiki/Lightning_Memory-Mapped_Database),
but resolves some issues and adds several features.

 - _libmdbx_ guarantee data integrity after crash unless this was explicitly
neglected in favour of write performance.

 - _libmdbx_ allows multiple processes to read and update several key-value
tables concurrently, while being
[ACID](https://en.wikipedia.org/wiki/ACID)-compliant, with minimal
overhead and Olog(N) operation cost.

 - _libmdbx_ enforce
[serializability](https://en.wikipedia.org/wiki/Serializability) for
writers by single
[mutex](https://en.wikipedia.org/wiki/Mutual_exclusion) and affords
[wait-free](https://en.wikipedia.org/wiki/Non-blocking_algorithm#Wait-freedom)
for parallel readers without atomic/interlocked operations, while
writing and reading transactions do not block each other.

 - _libmdbx_ uses [B+Trees](https://en.wikipedia.org/wiki/B%2B_tree) and
[Memory-Mapping](https://en.wikipedia.org/wiki/Memory-mapped_file),
doesn't use [WAL](https://en.wikipedia.org/wiki/Write-ahead_logging)
which might be a caveat for some workloads.

 - _libmdbx_ implements a simplified variant of the [Berkeley
DB](https://en.wikipedia.org/wiki/Berkeley_DB) and/or
[dbm](https://en.wikipedia.org/wiki/DBM_(computing)) API.

 - _libmdbx_ supports Linux, Windows, MacOS, FreeBSD and other systems
compliant with POSIX.1-2008.

### Comparison with other databases
For now please refer to [chapter of "BoltDB comparison with other
databases"](https://github.com/coreos/bbolt#comparison-with-other-databases)
which is also (mostly) applicable to _libmdbx_.

### History
At first the development was carried out within the
[ReOpenLDAP](https://github.com/leo-yuriev/ReOpenLDAP) project. About a
year later _libmdbx_ was separated into standalone project, which was
[presented at Highload++ 2015
conference](http://www.highload.ru/2015/abstracts/1831.html).

Since 2017 _libmdbx_ is used in [Fast Positive Tables](https://github.com/leo-yuriev/libfpta),
and development is funded by [Positive Technologies](https://www.ptsecurity.com).

### Acknowledgments
Howard Chu <hyc@openldap.org> is the author of LMDB, from which
originated the MDBX in 2015.

Martin Hedenfalk <martin@bzero.se> is the author of `btree.c` code, which
was used for begin development of LMDB.

-----

Description
===========

## Key features

1. Key-value pairs are stored in ordered map(s), keys are always sorted,
range lookups are supported.

2. Data is [memory-mapped](https://en.wikipedia.org/wiki/Memory-mapped_file)
into each worker DB process, and could be accessed zero-copy from transactions.

3. Transactions are
[ACID](https://en.wikipedia.org/wiki/ACID)-compliant, through to
[MVCC](https://en.wikipedia.org/wiki/Multiversion_concurrency_control)
and [CoW](https://en.wikipedia.org/wiki/Copy-on-write). Writes are
strongly serialized and aren't blocked by reads, transactions can't
conflict with each other. Reads are guaranteed to get only commited data
([relaxing serializability](https://en.wikipedia.org/wiki/Serializability#Relaxing_serializability)).

4. Read transactions are
[non-blocking](https://en.wikipedia.org/wiki/Non-blocking_algorithm),
don't use [atomic operations](https://en.wikipedia.org/wiki/Linearizability#High-level_atomic_operations).
Readers don't block each other and aren't blocked by writers. Read
performance scales linearly with CPU core count.
  > Nonetheless, "connect to DB" (starting the first read transaction in a thread) and
  > "disconnect from DB" (closing DB or thread termination) requires a lock
  > acquisition to register/unregister at the "readers table".

5. Keys with multiple values are stored efficiently without key
duplication, sorted by value, including integers (valuable for
secondary indexes).

6. Efficient operation on short fixed length keys,
including 32/64-bit integer types.

7. [WAF](https://en.wikipedia.org/wiki/Write_amplification) (Write
Amplification Factor) и RAF (Read Amplification Factor) are Olog(N).

8. No [WAL](https://en.wikipedia.org/wiki/Write-ahead_logging) and
transaction journal. In case of a crash no recovery needed. No need for
regular maintenance. Backups can be made on the fly on working DB
without freezing writers.

9. No additional memory management, all done by basic OS services.


## Improvements over LMDB

_libmdbx_ is superior to _legendary [LMDB](https://symas.com/lmdb/)_ in
terms of features and reliability, not inferior in performance. In
comparison to LMDB, _libmdbx_ make things "just work" perfectly and
out-of-the-box, not silently and catastrophically break down. The list
below is pruned down to the improvements most notable and obvious from
the user's point of view.

1. Automatic on-the-fly database size control by preset parameters, both
reduction and increment.
  > _libmdbx_ manage the database size according to parameters specified
  > by `mdbx_env_set_geometry()` function,
  > ones include the growth step and the truncation threshold.

2. Automatic continuous zero-overhead database compactification.
  > _libmdbx_ logically move as possible a freed pages
  > at end of allocation area into unallocated space,
  > and then release such space if a lot of.

3. LIFO policy for recycling a Garbage Collection items. On systems with a disk
write-back cache, this can significantly increase write performance, up to
several times in a best case scenario.
  > LIFO means that for reuse pages will be taken which became unused the lastest.
  > Therefore the loop of database pages circulation becomes as short as possible.
  > In other words, the number of pages, that are overwritten in memory
  > and on disk during a series of write transactions, will be as small as possible.
  > Thus creates ideal conditions for the efficient operation of the disk write-back cache.

4. Fast estimation of range query result volume, i.e. how many items can
be found between a `KEY1` and a `KEY2`. This is prerequisite for build
and/or optimize query execution plans.
  > _libmdbx_ performs a rough estimate based only on b-tree pages that
  > are common for the both stacks of cursors that were set to corresponing
  > keys.

5. `mdbx_chk` tool for database integrity check.

6. Guarantee of database integrity even in asynchronous unordered write-to-disk mode.
  > _libmdbx_ propose additional trade-off by implementing append-like manner for updates
  > in `NOSYNC` and `MAPASYNC` modes, that avoid database corruption after a system crash
  > contrary to LMDB. Nevertheless, the `MDBX_UTTERLY_NOSYNC` mode available to match LMDB behaviour,
  > and for a special use-cases.

7. Automated steady flush to disk upon volume of changes and/or by
timeout via cheap polling.

8. Sequence generation and three cheap persistent 64-bit markers with ACID.

9. Support for keys and values of zero length, including multi-values
(aka sorted duplicates).

10. The handler of lack-of-space condition with a callback,
that allow you to control and resolve such situations.

11. Support for opening a database in the exclusive mode, including on a network share.

12. Extended transaction info, including dirty and leftover space info
for a write transaction, reading lag and hold over space for read
transactions.

13. Extended whole-database info (aka environment) and reader enumeration.

14. Extended update or delete, _at once_ with getting previous value
and addressing the particular item from multi-value with the same key.

15. Support for explicitly updating the existing record, not insertion a new one.

16. All cursors are uniformly, can be reused and should be closed explicitly,
regardless ones were opened within write or read transaction.

17. Correct update of current record with `MDBX_CURRENT` flag when size
of key or data was changed, including sorted duplicated.

18. Opening database handles is spared from race conditions and
pre-opening is not needed.

19. Ability to determine whether the particular data is on a dirty page
or not, that allows to avoid copy-out before updates.

20. Ability to determine whether the cursor is pointed to a key-value
pair, to the first, to the last, or not set to anything.

21. Returning `MDBX_EMULTIVAL` error in case of ambiguous update or delete.

22. On **MacOS** the `fcntl(F_FULLFSYNC)` syscall is used _by
default_ to synchronize data with the disk, as this is [the only way to
guarantee data
durability](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/fsync.2.html)
in case of power failure. Unfortunately, in scenarios with high write
intensity, the use of `F_FULLFSYNC` significant degrades performance
compared to LMDB, where the `fsync()` syscall is used. Therefore,
_libmdbx_ allows you to override this behavior by defining the
`MDBX_OSX_SPEED_INSTEADOF_DURABILITY=1` option while build the library.

23. On **Windows** the `LockFileEx()` syscall is used for locking, since
it allows place the database on network drives, and provides protection
against incompetent user actions (aka
[poka-yoke](https://en.wikipedia.org/wiki/Poka-yoke)). Therefore
_libmdbx_ may be a little lag in performance tests from LMDB where a
named mutexes are used.


## Gotchas

1. There cannot be more than one writer at a time.
  > On the other hand, this allows serialize an updates and eliminate any
  > possibility of conflicts, deadlocks or logical errors.

2. No [WAL](https://en.wikipedia.org/wiki/Write-ahead_logging) means
relatively big [WAF](https://en.wikipedia.org/wiki/Write_amplification)
(Write Amplification Factor). Because of this syncing data to disk might
be quite resource intensive and be main performance bottleneck during
intensive write workload.
  > As compromise _libmdbx_ allows several modes of lazy and/or periodic
  > syncing, including `MAPASYNC` mode, which modificate data in memory and
  > asynchronously syncs data to disk, moment to sync is picked by OS.
  >
  > Although this should be used with care, synchronous transactions in a DB
  > with transaction journal will require 2 IOPS minimum (probably 3-4 in
  > practice) because of filesystem overhead, overhead depends on
  > filesystem, not on record count or record size. In _libmdbx_ IOPS count
  > will grow logarithmically depending on record count in DB (height of B+
  > tree) and will require at least 2 IOPS per transaction too.

3. [CoW](https://en.wikipedia.org/wiki/Copy-on-write) for
[MVCC](https://en.wikipedia.org/wiki/Multiversion_concurrency_control)
is done on memory page level with
[B+trees](https://ru.wikipedia.org/wiki/B-%D0%B4%D0%B5%D1%80%D0%B5%D0%B2%D0%BE).
Therefore altering data requires to copy about Olog(N) memory pages,
which uses [memory bandwidth](https://en.wikipedia.org/wiki/Memory_bandwidth) and is main
performance bottleneck in `MDBX_MAPASYNC` mode.
  > This is unavoidable, but isn't that bad. Syncing data to disk requires
  > much more similar operations which will be done by OS, therefore this is
  > noticeable only if data sync to persistent storage is fully disabled.
  > _libmdbx_ allows to safely save data to persistent storage with minimal
  > performance overhead. If there is no need to save data to persistent
  > storage then it's much more preferable to use `std::map`.

4. Massive altering of data during a parallel long read operation will
increase the process work set, may exhaust entire free database space and
result in subsequent write performance degradation.
  > _libmdbx_ mostly solve this issue by lack-of-space callback and `MDBX_LIFORECLAIM` mode.
  > See [`mdbx.h`](mdbx.h) with API description for details.
  > The "next" version of libmdbx (MithrilDB) will completely solve this.

5. There are no built-in checksums or digests to verify database integrity.
  > The "next" version of _libmdbx_ (MithrilDB) will solve this issue employing [Merkle Tree](https://en.wikipedia.org/wiki/Merkle_tree).

--------------------------------------------------------------------------------

Usage
=====

## Source code embedding

_libmdbx_ provides two official ways for integration in source code form:

1. Using the amalgamated source code.
  > The amalgamated source code includes all files requires to build and
  > use _libmdbx_, but not for testing _libmdbx_ itself.

2. Adding the complete original source code as a `git submodule`.
  > This allows you to build as _libmdbx_ and testing tool.
  >  On the other hand, this way requires you to pull git tags, and use C++11 compiler for test tool.

**_Please, avoid using any other techniques._** Otherwise, at least
don't ask for support and don't name such chimeras `libmdbx`.

The amalgamated source code could be created from original clone of git
repository on Linux by executing `make dist`. As a result, the desired
set of files will be formed in the `dist` subdirectory.

## Building

Both amalgamated and original source code provides build through the use
[CMake](https://cmake.org/) or [GNU
Make](https://www.gnu.org/software/make/) with
[bash](https://en.wikipedia.org/wiki/Bash_(Unix_shell)). All build ways
are completely traditional and have minimal prerequirements like
`build-essential`, i.e. the non-obsolete C/C++ compiler and a
[SDK](https://en.wikipedia.org/wiki/Software_development_kit) for the
target platform. Obviously you need building tools itself, i.e. `git`,
`cmake` or GNU `make` with `bash`.

So just use CMake or GNU Make in your habitual manner and feel free to
fill an issue or make pull request in the case something will be
unexpected or broken down.

#### DSO/DLL unloading and destructors of Thread-Local-Storage objects
When building _libmdbx_ as a shared library or use static _libmdbx_ as a
part of another dynamic library, it is advisable to make sure that your
system ensures the correctness of the call destructors of
Thread-Local-Storage objects when unloading dynamic libraries.

If this is not the case, then unloading a dynamic-link library with
_libmdbx_ code inside, can result in either a resource leak or a crash
due to calling destructors from an already unloaded DSO/DLL object. The
problem can only manifest in a multithreaded application, which makes
the unloading of shared dynamic libraries with _libmdbx_ code inside,
after using _libmdbx_. It is known that TLS-destructors are properly
maintained in the following cases:

- On all modern versions of Windows (Windows 7 and later).

- On systems with the
[`__cxa_thread_atexit_impl()`](https://sourceware.org/glibc/wiki/Destructor%20support%20for%20thread_local%20variables)
function in the standard C library, including systems with GNU libc
version 2.18 and later.

- On systems with libpthread/ntpl from GNU libc with bug fixes
[#21031](https://sourceware.org/bugzilla/show_bug.cgi?id=21031) and
[#21032](https://sourceware.org/bugzilla/show_bug.cgi?id=21032), or
where there are no similar bugs in the pthreads implementation.

### Linux and other platforms with GNU Make
To build the library it is enough to execute `make all` in the directory
of source code, and `make check` for execute the basic tests.

If the `make` installed on the system is not GNU Make, there will be a
lot of errors from make when trying to build. In this case, perhaps you
should use `gmake` instead of `make`, or even `gnu-make`, etc.

### FreeBSD and related platforms
As a rule, in such systems, the default is to use Berkeley Make. And GNU
Make is called by the gmake command or may be missing. In addition,
[bash](https://en.wikipedia.org/wiki/Bash_(Unix_shell)) may be absent.

You need to install the required components: GNU Make, bash, C and C++
compilers compatible with GCC or CLANG. After that, to build the
library, it is enough execute `gmake all` (or `make all`) in the
directory with source code, and `gmake check` (or `make check`) to run
the basic tests.

### Windows
For build _libmdbx_ on Windows the _original_ CMake and [Microsoft Visual
Studio](https://en.wikipedia.org/wiki/Microsoft_Visual_Studio) are
recommended.

Building by MinGW, MSYS or Cygwin is potentially possible. However,
these scripts are not tested and will probably require you to modify the
CMakeLists.txt or Makefile respectively.

It should be noted that in _libmdbx_ was efforts to resolve
runtime dependencies from CRT and other libraries Visual Studio.
For this is enough define the `MDBX_AVOID_CRT` during build.

An example of running a basic test script can be found in the
[CI-script](appveyor.yml) for [AppVeyor](https://www.appveyor.com/). To
run the [long stochastic test scenario](test/long_stochastic.sh),
[bash](https://en.wikipedia.org/wiki/Bash_(Unix_shell)) is required, and
the such testing is recommended with place the test data on the
[RAM-disk](https://en.wikipedia.org/wiki/RAM_drive).

### MacOS
Current [native build tools](https://en.wikipedia.org/wiki/Xcode) for
MacOS include GNU Make, CLANG and an outdated version of bash.
Therefore, to build the library, it is enough to run `make all` in the
directory with source code, and run `make check` to execute the base
tests. If something goes wrong, it is recommended to install
[Homebrew](https://brew.sh/) and try again.

To run the [long stochastic test scenario](test/long_stochastic.sh), you
will need to install the current (not outdated) version of
[bash](https://en.wikipedia.org/wiki/Bash_(Unix_shell)). To do this, we
recommend that you install [Homebrew](https://brew.sh/) and then execute
`brew install bash`.

## Bindings

  | Runtime  | GitHub | Author |
  | -------- | ------ | ------ |
  | Java     | [mdbxjni](https://github.com/castortech/mdbxjni)   | [Castor Technologies](https://castortech.com/) |
  | .NET     | [mdbx.NET](https://github.com/wangjia184/mdbx.NET) | [Jerry Wang](https://github.com/wangjia184) |


--------------------------------------------------------------------------------

Performance comparison
======================

All benchmarks were done by [IOArena](https://github.com/pmwkaa/ioarena)
and multiple [scripts](https://github.com/pmwkaa/ioarena/tree/HL%2B%2B2015)
runs on Lenovo Carbon-2 laptop, i7-4600U 2.1 GHz, 8 Gb RAM,
SSD SAMSUNG MZNTD512HAGL-000L1 (DXT23L0Q) 512 Gb.

## Integral performance

Here showed sum of performance metrics in 3 benchmarks:

 - Read/Search on 4 CPU cores machine;

 - Transactions with [CRUD](https://en.wikipedia.org/wiki/CRUD)
 operations in sync-write mode (fdatasync is called after each
 transaction);

 - Transactions with [CRUD](https://en.wikipedia.org/wiki/CRUD)
 operations in lazy-write mode (moment to sync data to persistent storage
 is decided by OS).

*Reasons why asynchronous mode isn't benchmarked here:*

  1. It doesn't make sense as it has to be done with DB engines, oriented
  for keeping data in memory e.g. [Tarantool](https://tarantool.io/),
  [Redis](https://redis.io/)), etc.

  2. Performance gap is too high to compare in any meaningful way.

![Comparison #1: Integral Performance](https://raw.githubusercontent.com/wiki/leo-yuriev/libmdbx/img/perf-slide-1.png)

--------------------------------------------------------------------------------

## Read Scalability

Summary performance with concurrent read/search queries in 1-2-4-8
threads on 4 CPU cores machine.

![Comparison #2: Read Scalability](https://raw.githubusercontent.com/wiki/leo-yuriev/libmdbx/img/perf-slide-2.png)

--------------------------------------------------------------------------------

## Sync-write mode

 - Linear scale on left and dark rectangles mean arithmetic mean
 transactions per second;

 - Logarithmic scale on right is in seconds and yellow intervals mean
 execution time of transactions. Each interval shows minimal and maximum
 execution time, cross marks standard deviation.

**10,000 transactions in sync-write mode**. In case of a crash all data
is consistent and state is right after last successful transaction.
[fdatasync](https://linux.die.net/man/2/fdatasync) syscall is used after
each write transaction in this mode.

In the benchmark each transaction contains combined CRUD operations (2
inserts, 1 read, 1 update, 1 delete). Benchmark starts on empty database
and after full run the database contains 10,000 small key-value records.

![Comparison #3: Sync-write mode](https://raw.githubusercontent.com/wiki/leo-yuriev/libmdbx/img/perf-slide-3.png)

--------------------------------------------------------------------------------

## Lazy-write mode

 - Linear scale on left and dark rectangles mean arithmetic mean of
 thousands transactions per second;

 - Logarithmic scale on right in seconds and yellow intervals mean
 execution time of transactions. Each interval shows minimal and maximum
 execution time, cross marks standard deviation.

**100,000 transactions in lazy-write mode**. In case of a crash all data
is consistent and state is right after one of last transactions, but
transactions after it will be lost. Other DB engines use
[WAL](https://en.wikipedia.org/wiki/Write-ahead_logging) or transaction
journal for that, which in turn depends on order of operations in
journaled filesystem. _libmdbx_ doesn't use WAL and hands I/O operations
to filesystem and OS kernel (mmap).

In the benchmark each transaction contains combined CRUD operations (2
inserts, 1 read, 1 update, 1 delete). Benchmark starts on empty database
and after full run the database contains 100,000 small key-value
records.


![Comparison #4: Lazy-write mode](https://raw.githubusercontent.com/wiki/leo-yuriev/libmdbx/img/perf-slide-4.png)

--------------------------------------------------------------------------------

## Async-write mode

 - Linear scale on left and dark rectangles mean arithmetic mean of
 thousands transactions per second;

 - Logarithmic scale on right in seconds and yellow intervals mean
 execution time of transactions. Each interval shows minimal and maximum
 execution time, cross marks standard deviation.

**1,000,000 transactions in async-write mode**. In case of a crash all
data will be consistent and state will be right after one of last
transactions, but lost transaction count is much higher than in
lazy-write mode. All DB engines in this mode do as little writes as
possible on persistent storage. _libmdbx_ uses
[msync(MS_ASYNC)](https://linux.die.net/man/2/msync) in this mode.

In the benchmark each transaction contains combined CRUD operations (2
inserts, 1 read, 1 update, 1 delete). Benchmark starts on empty database
and after full run the database contains 10,000 small key-value records.

![Comparison #5: Async-write mode](https://raw.githubusercontent.com/wiki/leo-yuriev/libmdbx/img/perf-slide-5.png)

--------------------------------------------------------------------------------

## Cost comparison

Summary of used resources during lazy-write mode benchmarks:

 - Read and write IOPS;

 - Sum of user CPU time and sys CPU time;

 - Used space on persistent storage after the test and closed DB, but not
 waiting for the end of all internal housekeeping operations (LSM
 compactification, etc).

_ForestDB_ is excluded because benchmark showed it's resource
consumption for each resource (CPU, IOPS) much higher than other engines
which prevents to meaningfully compare it with them.

All benchmark data is gathered by
[getrusage()](http://man7.org/linux/man-pages/man2/getrusage.2.html)
syscall and by scanning data directory.

![Comparison #6: Cost comparison](https://raw.githubusercontent.com/wiki/leo-yuriev/libmdbx/img/perf-slide-6.png)

--------------------------------------------------------------------------------

```
$ objdump -f -h -j .text libmdbx.so

libmdbx.so:     file format elf64-x86-64
architecture: i386:x86-64, flags 0x00000150:
HAS_SYMS, DYNAMIC, D_PAGED
start address 0x0000000000003710

Sections:
Idx Name          Size      VMA               LMA               File off  Algn
 11 .text         00015eff  0000000000003710  0000000000003710  00003710  2**4
                  CONTENTS, ALLOC, LOAD, READONLY, CODE
```
