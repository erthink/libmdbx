ChangeLog
---------

## v0.9.x (in the development):
- Since v0.9 usage of custom comparators and the `mdbx_dbi_open_ex()` are deprecated.
- TODO: API for explicit threads (de)registration.
- TODO: Native bindings for C++.
- TODO: Packages for AltLinux, Fedora/RHEL, Debian/Ubuntu.
- TODO: support for Doxygen & online API reference.

## v0.8.2 2020-07-06:
- Added support multi-opening the same DB in a process with SysV locking (BSD).
- Fixed warnings & minors for LCC compiler (E2K).
- Enabled to simultaneously open the same database from processes with and without the `MDBX_WRITEMAP` option.
- Added key-to-value, `mdbx_get_keycmp()` and `mdbx_get_datacmp()` functions (helpful to avoid using custom comparators).
- Added `ENABLE_UBSAN` CMake option to enabling the UndefinedBehaviorSanitizer from GCC/CLANG.
- Workaround for [CLANG bug](https://bugs.llvm.org/show_bug.cgi?id=43275).
- Returning `MDBX_CORRUPTED` in case all meta-pages are weak and no other error.
- Refined mode bits while auto-creating LCK-file.
- Avoids unnecessary database file re-mapping in case geometry changed by another process(es).
  From the user's point of view, the `MDBX_UNABLE_EXTEND_MAPSIZE` error will now be returned less frequently and only when using the DB in the current process really requires it to be reopened.
- Remapping on-the-fly and of the database file was implemented.
  Now remapping with a change of address is performed automatically if there are no dependent readers in the current process.

## v0.8.1 2020-06-12:
- Minor change versioning. The last number in the version now means the number of commits since last release/tag.
- Provide ChangeLog file.
- Fix for using libmdbx as a C-only sub-project with CMake.
- Fix `mdbx_env_set_geometry()` for case it is called from an opened environment outside of a write transaction.
- Add support for huge transactions and `MDBX_HUGE_TRANSACTIONS` build-option (default `OFF`).
- Refine LTO (link time optimization) for clang.
- Force enabling exceptions handling for MSVC (`/EHsc` option).

## v0.8.0 2020-06-05:
- Support for Android/Bionic.
- Support for iOS.
- Auto-handling `MDBX_NOSUBDIR` while opening for any existing database.
- Engage github-actions to make release-assets.
- Clarify API description.
- Extended keygen-cases in stochastic test.
- Fix fetching of first/lower key from LEAF2-page during page merge.
- Fix missing comma in array of error messages.
- Fix div-by-zero while copy-with-compaction for non-resizable environments.
- Fixes & enhancements for custom-comparators.
- Fix `MDBX_AVOID_CRT` option and missing `ntdll.def`.
- Fix `mdbx_env_close()` to work correctly called concurrently from several threads.
- Fix null-deref in an ASAN-enabled builds while opening the environment with error and/or read-only.
- Fix AddressSanitizer errors after closing the environment.
- Fix/workaround to avoid GCC 10.x pedantic warnings.
- Fix using `ENODATA` for FreeBSD.
- Avoid invalidation of DBI-handle(s) when it just closes.
- Avoid using `pwritev()` for single-writes (up to 10% speedup for some kernels & scenarios).
- Avoiding `MDBX_UTTERLY_NOSYNC` as result of flags merge.
- Add `mdbx_dbi_dupsort_depthmask()` function.
- Add `MDBX_CP_FORCE_RESIZEABLE` option.
- Add deprecated `MDBX_MAP_RESIZED` for compatibility.
- Add `MDBX_BUILD_TOOLS` option (default `ON`).
- Refine `mdbx_dbi_open_ex()` to safe concurrently opening the same handle from different threads.
- Truncate clk-file during environment closing. So a zero-length lck-file indicates that the environment was closed properly.
- Refine `mdbx_update_gc()` for huge transactions with small sizes of database page.
- Extends dump/load to support all MDBX attributes.
- Avoid upsertion the same key-value data, fix related assertions.
- Rework min/max length checking for keys & values.
- Checking the order of keys on all pages during checking.
- Support `CFLAGS_EXTRA` make-option for convenience.
- Preserve the last txnid while copying with compactification.
- Auto-reset running transaction in mdbx_txn_renew().
- Automatically abort errored transaction in mdbx_txn_commit().
- Auto-choose page size for large databases.
- Rearrange source files, rework build, options-support by CMake.
- Crutch for WSL1 (Windows subsystem for Linux).
- Refine install/uninstall targets.
- Support for Valgrind 3.14 and later.
- Add check-analyzer check-ubsan check-asan check-leak targets to Makefile.
- Minor fix/workaround to avoid UBSAN traps for `memcpy(ptr, NULL, 0)`.
- Avoid some GCC-analyzer false-positive warnings.

## v0.7.0 2020-03-18:
- Workarounds for Wine (Windows compatibility layer for Linux).
- `MDBX_MAP_RESIZED` renamed to `MDBX_UNABLE_EXTEND_MAPSIZE`.
- Clarify API description, fix typos.
- Speedup runtime checks in debug/checked builds.
- Added checking for read/write transactions overlapping for the same thread, added `MDBX_TXN_OVERLAPPING` error and `MDBX_DBG_LEGACY_OVERLAP` option.
- Added `mdbx_key_from_jsonInteger()`, `mdbx_key_from_double()`, `mdbx_key_from_float()`, `mdbx_key_from_int64()` and `mdbx_key_from_int32()` functions. See `mdbx.h` for description.
- Fix compatibility (use zero for invalid DBI).
- Refine/clarify error messages.
- Avoids extra error messages "bad txn" from mdbx_chk when DB is corrupted.

## v0.6.0 2020-01-21:
- Fix `mdbx_load` utility for custom comparators.
- Fix checks related to `MDBX_APPEND` flag inside `mdbx_cursor_put()`.
- Refine/fix dbi_bind() internals.
- Refine/fix handling `STATUS_CONFLICTING_ADDRESSES`.
- Rework `MDBX_DBG_DUMP` option to avoid disk I/O performance degradation.
- Add built-in help to test tool.
- Fix `mdbx_env_set_geometry()` for large page size.
- Fix env_set_geometry() for large pagesize.
- Clarify API description & comments, fix typos.

## v0.5.0 2019-12-31:
- Fix returning MDBX_RESULT_TRUE from page_alloc().
- Fix false-positive ASAN issue.
- Fix assertion for `MDBX_NOTLS` option.
- Rework `MADV_DONTNEED` threshold.
- Fix `mdbx_chk` utility for don't checking some numbers if walking on the B-tree was disabled.
- Use page's mp_txnid for basic integrity checking.
- Add `MDBX_FORCE_ASSERTIONS` built-time option.
- Rework `MDBX_DBG_DUMP` to avoid performance degradation.
- Rename `MDBX_NOSYNC` to `MDBX_SAFE_NOSYNC` for clarity.
- Interpret `ERROR_ACCESS_DENIED` from `OpenProcess()` as 'process exists'.
- Avoid using `FILE_FLAG_NO_BUFFERING` for compatibility with small database pages.
- Added install section for CMake.

## v0.4.0 2019-12-02:
- Support for Mac OSX, FreeBSD, NetBSD, OpenBSD, DragonFly BSD, OpenSolaris, OpenIndiana (AIX and HP-UX pending).
- Use bootid for decisions of rollback.
- Counting retired pages and extended transaction info.
- Add `MDBX_ACCEDE` flag for database opening.
- Using OFD-locks and tracking for in-process multi-opening.
- Hot backup into pipe.
- Support for cmake & amalgamated sources.
- Fastest internal sort implementation.
- New internal dirty-list implementation with lazy sorting.
- Support for lazy-sync-to-disk with polling.
- Extended key length.
- Last update transaction number for each sub-database.
- Automatic read ahead enabling/disabling.
- More auto-compactification.
- Using -fsanitize=undefined and -Wpedantic options.
- Rework page merging.
- Nested transactions.
- API description.
- Checking for non-local filesystems to avoid DB corruption.
