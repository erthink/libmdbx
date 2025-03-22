#include "mdbx.h++"

#include <chrono>
#include <deque>
#include <iostream>
#include <vector>
#if defined(__cpp_lib_latch) && __cpp_lib_latch >= 201907L
#include <latch>
#include <thread>
#endif

#if defined(ENABLE_MEMCHECK) || defined(MDBX_CI)
#if MDBX_DEBUG || !defined(NDEBUG)
#define RELIEF_FACTOR 16
#else
#define RELIEF_FACTOR 8
#endif
#elif MDBX_DEBUG || !defined(NDEBUG) || defined(__APPLE__) || defined(_WIN32)
#define RELIEF_FACTOR 4
#elif UINTPTR_MAX > 0xffffFFFFul || ULONG_MAX > 0xffffFFFFul
#define RELIEF_FACTOR 2
#else
#define RELIEF_FACTOR 1
#endif

#define NN (1000 / RELIEF_FACTOR)

static void logger_nofmt(MDBX_log_level_t loglevel, const char *function, int line, const char *msg,
                         unsigned length) noexcept {
  (void)length;
  (void)loglevel;
  std::cout << function << ":" << line << " " << msg;
}

static char log_buffer[1024];

//--------------------------------------------------------------------------------------------

bool case0(mdbx::env env) {
  auto txn = env.start_write();
  auto table = txn.create_map("case0", mdbx::key_mode::usual, mdbx::value_mode::single);
  auto cursor_1 = txn.open_cursor(table);
  auto cursor_2 = cursor_1.clone();

  auto nested = env.start_write(txn);
  auto nested_cursor_1 = nested.open_cursor(table);
  auto nested_cursor_2 = nested_cursor_1.clone();
  auto nested_cursor_3 = cursor_1.clone();

  auto deep = env.start_write(nested);
  auto deep_cursor_1 = deep.open_cursor(table);
  auto deep_cursor_2 = nested_cursor_1.clone();
  auto deep_cursor_3 = cursor_1.clone();
  deep_cursor_1.close();
  deep.commit();
  deep_cursor_2.close();

  nested_cursor_1.close();
  nested.abort();
  nested_cursor_2.close();

  cursor_1.close();
  txn.commit();
  cursor_2.close();
  return true;
}

//--------------------------------------------------------------------------------------------

/* Сценарий:
 *
 * 0. Создаём N таблиц, курсор для каждой таблицы и заполняем (1000 ключей, от 1 до 1000 значений в каждом ключе).
 * 1. Запускаем N-1 фоновых потоков и используем текущий/основной.
 * 2. В каждом потоке 100500 раз повторяем последовательность действий:
 *   - 100500 раз запускаем читающую транзакцию и выполняем "читающий цикл":
 *     - в читающей транзакции создаем 0..3 курсоров, потом подключаем заранее созданный курсор,
 *       потом еще 0..3 курсоров;
 *     - выполняем по паре поисков через каждый курсор;
 *     - отключаем заранее созданный курсор;
 *     - снова выполняем несколько поисков по каждому курсору;
 *     - псевдослучайно закрываем один из курсоров и один отключаем;
 *     - псевдослучайно выполняем один из путей:
 *       - закрываем все курсоры посредством mdbx_txn_release_all_cursors();
 *       - отсоединяем все курсоры посредством mdbx_txn_release_all_cursors();
 *       - псевдослучайно закрываем один из курсоров и один отключаем;
 *       - ничего не делаем;
 *     - завершаем читающую транзакцию псевдослучайно выбирая между commit и abort;
 *     - закрываем оставшиеся курсоры.
 * 3. Выполняем "пишущий цикл":
 *   - запускаем пишущую или вложенную транзакцию;
 *   - из оставшихся с предыдущих итераций курсоров половину закрываем,
 *     половину подключаем к транзакции;
 *   - для каждой таблицы с вероятностью 1/2 выполняем "читающий цикл";
 *   - для каждой таблицы с вероятностью 1/2 выполняем "модифицирующий" цикл:
 *     - подключаем курсор, либо создаем при отсутствии подходящих;
 *     - 100 раз выполняем поиск случайных пар ключ/значение;
 *     - при успешном поиске удаляем значение, иначе вставляем;
 *     - с вероятностью 1/2 повторяем "читающий цикл";
 *   - с вероятностью 7/16 запускаем вложенную транзакцию:
 *     - действуем рекурсивно как с пишущей транзакцией;
 *     - в "читающих циклах" немного меняем поведение:
 *       - игнорируем ожидаемые ошибки mdbx_cursor_unbind();
 *       - в 2-3 раза уменьшаем вероятность использования mdbx_txn_release_all_cursors();
 *     - завершаем вложенную транзакцию псевдослучайно выбирая между commit и abort;
 *   - для каждой таблицы с вероятностью 1/2 выполняем "читающий цикл";
 *   - завершаем транзакцию псевдослучайно выбирая между commit и abort;
 * 4. Ждем завершения фоновых потоков.
 * 5. Закрываем оставшиеся курсоры и закрываем БД. */

thread_local size_t salt;

static size_t prng() {
  salt = salt * 134775813 + 1;
  return salt ^ ((salt >> 11) * 1822226723);
}

static inline bool flipcoin() { return prng() & 1; }

static inline size_t prng(size_t range) { return prng() % range; }

void case1_shuffle_pool(std::vector<MDBX_cursor *> &pool) {
  for (size_t n = 1; n < pool.size(); ++n) {
    const auto i = prng(n);
    if (i != n)
      std::swap(pool[n], pool[i]);
  }
}

void case1_read_pool(std::vector<MDBX_cursor *> &pool) {
  for (auto c : pool)
    if (flipcoin())
      mdbx::cursor(c).find_multivalue(mdbx::slice::wrap(prng(NN)), mdbx::slice::wrap(prng(NN)), false);
  for (auto c : pool)
    if (flipcoin())
      mdbx::cursor(c).find_multivalue(mdbx::slice::wrap(prng(NN)), mdbx::slice::wrap(prng(NN)), false);
}

MDBX_cursor *case1_try_unbind(MDBX_cursor *cursor) {
  if (cursor) {
    auto err = mdbx::error(static_cast<MDBX_error_t>(mdbx_cursor_unbind(cursor)));
    if (err.code() != MDBX_EINVAL)
      err.success_or_throw();
  }
  return cursor;
}

MDBX_cursor *case1_pool_remove(std::vector<MDBX_cursor *> &pool) {
  switch (pool.size()) {
  case 0:
    return nullptr;
  case 1:
    if (flipcoin()) {
      const auto c = pool[0];
      pool.pop_back();
      return c;
    }
    return nullptr;
  default:
    const auto i = prng(pool.size());
    const auto c = pool[i];
    pool.erase(pool.begin() + i);
    return c;
  }
}

mdbx::map_handle case1_cycle_dbi(std::deque<mdbx::map_handle> &dbi) {
  const auto h = dbi.front();
  dbi.pop_front();
  dbi.push_back(h);
  return h;
}

void case1_read_cycle(mdbx::txn txn, std::deque<mdbx::map_handle> &dbi, std::vector<MDBX_cursor *> &pool,
                      mdbx::cursor pre, bool nested = false) {
  for (auto c : pool)
    mdbx::cursor(c).bind(txn, case1_cycle_dbi(dbi));
  pre.bind(txn, case1_cycle_dbi(dbi));

  for (auto n = prng(3 + dbi.size()); n > 0; --n) {
    auto c = txn.open_cursor(dbi[prng(dbi.size())]);
    pool.push_back(c.withdraw_handle());
  }
  case1_shuffle_pool(pool);
  case1_read_pool(pool);

  pool.push_back(pre);
  case1_read_pool(pool);
  pool.pop_back();

  for (auto n = prng(3 + dbi.size()); n > 0; --n) {
    auto c = txn.open_cursor(dbi[prng(dbi.size())]);
    pool.push_back(c.withdraw_handle());
  }
  pool.push_back(pre);
  case1_read_pool(pool);
  pool.pop_back();

  case1_try_unbind(pre);
  case1_shuffle_pool(pool);
  case1_read_pool(pool);

  if (flipcoin()) {
    mdbx_cursor_close(case1_pool_remove(pool));
    auto u = case1_try_unbind(case1_pool_remove(pool));
    case1_read_pool(pool);
    if (u)
      pool.push_back(u);
  } else {
    auto u = case1_try_unbind(case1_pool_remove(pool));
    mdbx_cursor_close(case1_pool_remove(pool));
    case1_read_pool(pool);
    if (u)
      pool.push_back(u);
  }

  switch (prng(nested ? 7 : 3)) {
  case 0:
    for (auto i = pool.begin(); i != pool.end();)
      if (mdbx_cursor_txn(*i))
        i = pool.erase(i);
      else
        ++i;
    txn.close_all_cursors();
    break;
  case 1:
    txn.unbind_all_cursors();
    break;
  }
}

void case1_write_cycle(mdbx::txn_managed txn, std::deque<mdbx::map_handle> &dbi, std::vector<MDBX_cursor *> &pool,
                       mdbx::cursor pre, bool nested = false) {
  if (flipcoin())
    case1_cycle_dbi(dbi);
  if (flipcoin())
    case1_shuffle_pool(pool);

  for (auto n = prng(dbi.size() + 1); n > 1; n -= 2) {
    if (!nested)
      pre.unbind();
    if (!pre.txn())
      pre.bind(txn, dbi[prng(dbi.size())]);
    for (auto i = 0; i < NN; ++i) {
      auto k = mdbx::default_buffer::wrap(prng(NN));
      auto v = mdbx::default_buffer::wrap(prng(NN));
      if (pre.find_multivalue(k, v, false))
        pre.erase();
      else
        pre.upsert(k, v);
    }
  }

  if (prng(16) > 8)
    case1_write_cycle(txn.start_nested(), dbi, pool, pre, true);

  if (flipcoin())
    txn.commit();
  else
    txn.abort();
}

bool case1_thread(mdbx::env env, std::deque<mdbx::map_handle> dbi, mdbx::cursor pre) {
  salt = size_t(std::chrono::high_resolution_clock::now().time_since_epoch().count());
  std::vector<MDBX_cursor *> pool;
  for (auto loop = 0; loop < 333 / RELIEF_FACTOR; ++loop) {
    for (auto read = 0; read < 333 / RELIEF_FACTOR; ++read) {
      auto txn = env.start_read();
      case1_read_cycle(txn, dbi, pool, pre);
      if (flipcoin())
        txn.commit();
      else
        txn.abort();
    }

    case1_write_cycle(env.start_write(), dbi, pool, pre);

    for (auto c : pool)
      mdbx_cursor_close(c);
    pool.clear();
  }

  pre.unbind();
  return true;
}

bool case1(mdbx::env env) {
  bool ok = true;
  std::deque<mdbx::map_handle> dbi;
  std::vector<mdbx::cursor_managed> cursors;
#if defined(__cpp_lib_latch) && __cpp_lib_latch >= 201907L
  static const auto N = 10;
#else
  static const auto N = 3;
#endif
  for (auto t = 0; t < N; ++t) {
    auto txn = env.start_write();
    auto table = txn.create_map(std::to_string(t), mdbx::key_mode::ordinal, mdbx::value_mode::multi_samelength);
    auto cursor = txn.open_cursor(table);
    for (size_t i = 0; i < NN * 11; ++i)
      cursor.upsert(mdbx::default_buffer::wrap(prng(NN)), mdbx::default_buffer::wrap(prng(NN)));
    txn.commit();

    cursors.push_back(std::move(cursor));
    dbi.push_back(table);
  }

#if defined(__cpp_lib_latch) && __cpp_lib_latch >= 201907L
  std::latch s(1);
  std::vector<std::thread> threads;
  for (auto t = 1; t < N; ++t) {
    case1_cycle_dbi(dbi);
    threads.push_back(std::thread([&, t]() {
      s.wait();
      if (!case1_thread(env, dbi, cursors[t]))
        ok = false;
    }));
  }
  case1_cycle_dbi(dbi);
  s.count_down();
#endif

  if (!case1_thread(env, dbi, cursors[0]))
    ok = false;

#if defined(__cpp_lib_latch) && __cpp_lib_latch >= 201907L
  for (auto &t : threads)
    t.join();
#endif

  return ok;
}

//--------------------------------------------------------------------------------------------

bool case2(mdbx::env env) {
  bool ok = true;

  auto txn = env.start_write();
  auto dbi = txn.create_map("case2", mdbx::key_mode::usual, mdbx::value_mode::single);
  txn.commit_embark_read();
  auto cursor1 = txn.open_cursor(dbi);
  auto cursor2 = txn.open_cursor(0);
  cursor1.move(mdbx::cursor::next, false);
  cursor2.move(mdbx::cursor::next, false);
  txn.commit_embark_read();
  cursor2.bind(txn, dbi);
  cursor1.bind(txn, 0);
  cursor1.move(mdbx::cursor::last, false);
  cursor2.move(mdbx::cursor::last, false);

  return ok;
}

//--------------------------------------------------------------------------------------------

int doit() {
  mdbx::path db_filename = "test-cursor-closing";
  mdbx::env::remove(db_filename);

  mdbx::env_managed env(db_filename, mdbx::env_managed::create_parameters(),
                        mdbx::env::operate_parameters(42, 0, mdbx::env::nested_transactions));

  bool ok = case0(env);
  ok = case1(env) && ok;
  ok = case2(env) && ok;

  if (ok) {
    std::cout << "OK\n";
    return EXIT_SUCCESS;
  } else {
    std::cout << "FAIL!\n";
    return EXIT_FAILURE;
  }
}

int main(int argc, char *argv[]) {
  (void)argc;
  (void)argv;
  mdbx_setup_debug_nofmt(MDBX_LOG_NOTICE, MDBX_DBG_ASSERT, logger_nofmt, log_buffer, sizeof(log_buffer));
  try {
    return doit();
  } catch (const std::exception &ex) {
    std::cerr << "Exception: " << ex.what() << "\n";
    return EXIT_FAILURE;
  }
}
