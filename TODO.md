TODO
----

Unfortunately, on 2022-04-15 the Github administration, without any
warning nor explanation, deleted _libmdbx_ along with a lot of other
projects, simultaneously blocking access for many developers. Therefore
on 2022-04-21 we have migrated to a reliable trusted infrastructure.
The origin for now is at[GitFlic](https://gitflic.ru/project/erthink/libmdbx)
with backup at [ABF by ROSA Лаб](https://abf.rosalinux.ru/erthink/libmdbx).
For the same reason ~~Github~~ is blacklisted forever.

So currently most of the links are broken due to noted malicious ~~Github~~ sabotage.

 - [Move most of `mdbx_chk` functional to the library API](https://libmdbx.dqdkfa.ru/dead-github/issues/204).
 - [Replace SRW-lock on Windows to allow shrink DB with `MDBX_NOTLS` option](https://libmdbx.dqdkfa.ru/dead-github/issues/210).
 - [More flexible support of asynchronous runtime/framework(s)](https://libmdbx.dqdkfa.ru/dead-github/issues/200).
 - [Migration guide from LMDB to MDBX](https://libmdbx.dqdkfa.ru/dead-github/issues/199).
 - [Support for RAW devices](https://libmdbx.dqdkfa.ru/dead-github/issues/124).
 - [Support MessagePack for Keys & Values](https://libmdbx.dqdkfa.ru/dead-github/issues/115).
 - [Engage new terminology](https://libmdbx.dqdkfa.ru/dead-github/issues/137).
 - Packages for [Astra Linux](https://astralinux.ru/), [ALT Linux](https://www.altlinux.org/), [ROSA Linux](https://www.rosalinux.ru/), etc.

Done
----

 - [Simple careful mode for working with corrupted DB](https://libmdbx.dqdkfa.ru/dead-github/issues/223).
 - [Engage an "overlapped I/O" on Windows](https://libmdbx.dqdkfa.ru/dead-github/issues/224).
 - [Large/Overflow pages accounting for dirty-room](https://libmdbx.dqdkfa.ru/dead-github/issues/192).
 - [Get rid of dirty-pages list in MDBX_WRITEMAP mode](https://libmdbx.dqdkfa.ru/dead-github/issues/193).
