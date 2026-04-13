
TODO
----

 - preserve/restore cursors state during `mdbx_txn_checkpoint()`, `mdbx_txn_amend()` and `mdbx_txn_commit_embark_read()`.
 - man-page for `mdbx_defrag`.
 - Предоставление информации о размере рагруженной/кешируемой в ОЗУ части БД и её использование для управления упреждающим чтением.
 - [SWIG](https://www.swig.org/).
 - Интеграция c DTrace и аналогами.
 - LTTng for tracing.
 - Новый стиль обработки ошибок с записью "трассы" и причин.
 - Формирование отладочной информации посредством gdb.
 - Поддержка WASM.
 - Автоматическая уплотнение/дефрагментация.
 - Нелинейная обработка GC.
 - Перевести курсоры на двусвязный список вместо односвязного.
 - [Migration guide from LMDB to MDBX](https://libmdbx.dqdkfa.ru/dead-github/issues/199).
 - [Support for RAW devices](https://libmdbx.dqdkfa.ru/dead-github/issues/124).
 - [Support MessagePack for Keys & Values](https://libmdbx.dqdkfa.ru/dead-github/issues/115).
 - Packages for [Astra Linux](https://astralinux.ru/), [ALT Linux](https://www.altlinux.org/), [ROSA Linux](https://www.rosalinux.ru/), etc.
 - Extended example of using the C++ API, which can also be used as a simple smoke-test.

In development
--------------
 - v0.14.2 miletone

Done
----
 - Ultra-fast deletion of consecutive elements by cutting off b-tree branches.
 - split ASSERT() to CHECK{0,1,2} and basal `assert()`.
 - Refine/simplify assertion-like errors handling.
 - CLI-options to mdbx_defrag and mdbx_load.
 - Optional page-get operation statistics for transactions.
 - digging/refactoring/optimizing page splitting and tree rebalance.
 - Явная уплотнение/дефрагментация и mdbx_defrag.
 - get-cached API.
 - Cloning read transactions.
 - Параллельная lto-сборка с устранением предупреждений.
 - HarmonyOS support.
 - Ранняя/не-отложенная очистка GC.
 - Рефакторинг gc-get/gc-put c переходом на "интервальные" списки.
 - [Engage new terminology](https://libmdbx.dqdkfa.ru/dead-github/issues/137).
 - [More flexible support of asynchronous runtime/framework(s)](https://libmdbx.dqdkfa.ru/dead-github/issues/200).
 - [Move most of `mdbx_chk` functional to the library API](https://libmdbx.dqdkfa.ru/dead-github/issues/204).
 - [Simple careful mode for working with corrupted DB](https://libmdbx.dqdkfa.ru/dead-github/issues/223).
 - [Engage an "overlapped I/O" on Windows](https://libmdbx.dqdkfa.ru/dead-github/issues/224).
 - [Large/Overflow pages accounting for dirty-room](https://libmdbx.dqdkfa.ru/dead-github/issues/192).
 - [Get rid of dirty-pages list in MDBX_WRITEMAP mode](https://libmdbx.dqdkfa.ru/dead-github/issues/193).

Cancelled
--------

 - Внутри `txn_renew()` вынести проверку когерентности mmap за/после изменение размера.
   Потеряло смысл в результате рефакторинга.

 - [Replace SRW-lock on Windows to allow shrink DB with `MDBX_NOSTICKYTHREADS` option](https://libmdbx.dqdkfa.ru/dead-github/issues/210).
   Доработка не может быть реализована, так как замена SRW-блокировки
   лишает лишь предварительную проблему, но не главную. На Windows
   уменьшение размера отображенного в память файла не поддерживается ядром
   ОС. Для этого необходимо снять отображение, изменить размер файла и
   затем отобразить обратно. В свою очередь, для это необходимо
   приостановить работающие с БД потоки выполняющие транзакции чтения, либо
   готовые к такому выполнению. Но в режиме MDBX_NOSTICKYTHREADS нет
   возможности отслеживать работающие с БД потоки, а приостановка всех
   потоков неприемлема для большинства приложений.
