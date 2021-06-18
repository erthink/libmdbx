Incomplete TODO list to merge `python-bindings` (status: draft) branch to the `master` (status: ready for use):

0.

 - [ ] Q: does the `python/libmdbx/` subdirectory is required instead of just `python/` ?
 - [ ] Q: does names like  `MDBXFoo` is reasonable instead of using namespace(s) or `MDBX_Foo` ?
 - [ ] Q: could we use some automation to update, synchronize and/or check definitions and doxygen-comments ?

1.

 - [ ] cmake: enable python binding only for cmake >= 3.14 and warn otherwise (but we unable change the requirement for minimal cmake version).
 - [ ] cmake: use `FindPython()` instead of calling `python` directly.
 - [ ] cmake: always use `python3 -m pytest` instead of finding `pytest`, since such way is fragile and unportable.
 - [ ] cmake: replace using `sed` by CMake's `configure_file()`.
 - [ ] cmake: seems CMake have some useful features for python than we should use instead of custom targets.

2.

 - [ ] integration: provide & fix building from both amalgamated and non-amalgamated sources.
 - [ ] integration: manually check & fix all building variants with all libmdbx cases (static, solib/dll, windows-without-crt, with/without LTO).
 - [ ] integration: manually check & fix building by: clang 4.0..latest, gcc 4.8..latest.
 - [ ] ci: provide scripts for checking builds on Windows and FreeBSD.
 - [ ] integration: manually check & fix for Buildroot (i.e. cross-compilation) with all python-related options.
 - [ ] integration: manually check & fix for Solaris/OpenIndiana, OpenBSD, NetBSD, DragonflyBSD, Android, iOS.

3.

 - [ ] code: fix a hardcoded platform-depended error codes (MDBX_ENODATA, MDBX_EINVAL, MDBX_EACCESS, MDBX_ENOMEM, MDBX_EROFS, MDBX_ENOSYS, MDBX_EIO, MDBX_EPERM, MDBX_EINTR, MDBX_ENOFILE, MDBX_EREMOTE, etc ?).
 - [ ] feedback: take into account the wishes and recommendations of users.
 - [ ] coverity: check bindings with CoverityScan.
 - [ ] ci: add checking within CentOS 6.x .. latest (this is also applicable for mainstream).