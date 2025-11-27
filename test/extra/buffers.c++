#include "mdbx.h++"

#include <iostream>
#include <random>
#include <set>
#include <unordered_set>
#include <vector>

template <typename buffer> bool case1() {
  try {
    bool ok = true;
    std::vector<buffer> vector;
    vector.push_back(buffer("reference=false", false));
    vector.push_back(buffer("reference=true", true));
    vector.push_back(buffer(42, mdbx::slice("allocate"), 42 * 42));
    vector.emplace_back("emplace, reference=false", false);
    vector.emplace_back("emplace, reference=true", true);
    vector.emplace_back(42, mdbx::slice("emplace, allocate", 42 * 42));
    {
      auto copy = vector;
    }
    {
      auto copy = vector;
      vector.clear();
      vector = copy;
    }
    {
      auto copy = vector;
      vector.clear();
      vector = std::move(copy);
    }

    {
      std::shuffle(vector.begin(), vector.end(), std::default_random_engine());
      auto copy = vector;
      std::shuffle(copy.begin(), copy.end(), std::default_random_engine());
#if defined(__cpp_lib_containers_ranges) && __cpp_lib_containers_ranges > 202202L
      vector.append_range(std::move(copy));
#else
      while (!copy.empty()) {
        vector.push_back(copy.back());
        copy.pop_back();
      }
#endif
    }

    {
      std::set<buffer> set;
#if defined(__cpp_lib_containers_ranges) && __cpp_lib_containers_ranges > 202202L
      set.insert_range(vector);
#else
      for (auto &item : vector)
        set.insert(item);
#endif
      std::shuffle(vector.begin(), vector.end(), std::default_random_engine());
#if defined(__cpp_lib_containers_ranges) && __cpp_lib_containers_ranges > 202202L
      set.insert_range(vector);
#else
      for (auto &item : vector)
        set.insert(item);
#endif
      ok = set.size() == vector.size() / 2 && ok;
    }

    {
      std::unordered_set<buffer> set;
#if defined(__cpp_lib_containers_ranges) && __cpp_lib_containers_ranges > 202202L
      vector.append_range(vector);
      set.insert_range(vector);
#else
      for (size_t i = vector.size(); i-- > 0;)
        vector.push_back(vector[i]);
      for (auto &item : vector)
        set.insert(item);
#endif
      std::shuffle(vector.begin(), vector.end(), std::default_random_engine());
#if defined(__cpp_lib_containers_ranges) && __cpp_lib_containers_ranges > 202202L
      set.insert_range(vector);
#else
      for (auto &item : vector)
        set.insert(item);
#endif
      ok = set.size() == vector.size() / 4 && ok;
    }

    std::shuffle(vector.begin(), vector.end(), std::default_random_engine());
    std::shuffle(vector.begin(), vector.end(), std::default_random_engine());
    std::shuffle(vector.begin(), vector.end(), std::default_random_engine());
    std::shuffle(vector.begin(), vector.end(), std::default_random_engine());
    std::shuffle(vector.begin(), vector.end(), std::default_random_engine());
    std::shuffle(vector.begin(), vector.end(), std::default_random_engine());
    return ok;
  } catch (const std::exception &ex) {
    std::cerr << "Exception: " << ex.what() << "\n";
    return false;
  }
}

int doit() {

  bool ok = true;
  ok = case1<mdbx::default_buffer>() && ok;
  ok = case1<mdbx::buffer<mdbx::legacy_allocator, mdbx::default_capacity_policy>>() && ok;

  if (ok) {
    std::cout << "OK\n";
    return EXIT_SUCCESS;
  } else {
    std::cerr << "FAIL\n";
    return EXIT_FAILURE;
  }
}

int main(int argc, char *argv[]) {
  (void)argc;
  (void)argv;
  try {
    return doit();
  } catch (const std::exception &ex) {
    std::cerr << "Exception: " << ex.what() << "\n";
    return EXIT_FAILURE;
  }
}
