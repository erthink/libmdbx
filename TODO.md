TODO
----

 - [SWIG](https://www.swig.org/).
 - Параллельная lto-сборка с устранением предупреждений.
 - Интеграция c DTrace и аналогами.
 - Новый стиль обработки ошибок с записью "трассы" и причин.
 - Формирование отладочной информации посредством gdb.
 - Поддержка WASM.
 - Ранняя/не-отложенная очистка GC.
 - Явная и автоматические уплотнение/дефрагментация.
 - Нелинейная обработка GC.
 - Перевести курсоры на двусвязный список вместо односвязного.
 - Внутри `txn_renew()` вынести проверку когерентности mmap за/после изменение размера.
 - [Migration guide from LMDB to MDBX](https://libmdbx.dqdkfa.ru/dead-github/issues/199).
 - [Support for RAW devices](https://libmdbx.dqdkfa.ru/dead-github/issues/124).
 - [Support MessagePack for Keys & Values](https://libmdbx.dqdkfa.ru/dead-github/issues/115).
 - Packages for [Astra Linux](https://astralinux.ru/), [ALT Linux](https://www.altlinux.org/), [ROSA Linux](https://www.rosalinux.ru/), etc.

Done
----

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
