// Copyright 2021, Trail of Bits. All rights reserved.

#include <gtest/gtest.h>
#include <glog/logging.h>

#include <sys/time.h>

#include <algorithm>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <set>
#include <cstdio>
#include <cinttypes>

#include "FactPaths.h"

#include <drlojekyll/Runtime/StdRuntime.h>
#include "points_to.db.h"  // Auto-generated.

using DatabaseStorage = hyde::rt::StdStorage;
using DatabaseFunctors = points_to::DatabaseFunctors<DatabaseStorage>;
using DatabaseLog = points_to::DatabaseLog<DatabaseStorage>;
using Database = points_to::Database<DatabaseStorage, DatabaseLog, DatabaseFunctors>;

template <typename... Args>
using Vector = hyde::rt::Vector<DatabaseStorage, Args...>;

class Timed {
 private:
  const char * const timer;
  int64_t start;
 public:
  Timed(const char *timer_)
      : timer(timer_) {
    struct timeval x2_t;
    gettimeofday(&x2_t, NULL);
    start = x2_t.tv_sec * 1000000L + x2_t.tv_usec;
  }

  ~Timed(void) {
    struct timeval x2_t;
    gettimeofday(&x2_t, NULL);
    auto now = (x2_t.tv_sec * 1000000L) + x2_t.tv_usec;

    std::cerr << timer << ": " << (now - start) << "\n";
  }
};

TEST(PointsTo, RunOnFacts) {

  DatabaseFunctors functors;
  DatabaseLog log;
  DatabaseStorage storage;
  Database db(storage, log, functors);

  Vector<uint32_t, uint32_t> assign_alloc_facts(storage, 0);
  {
    Timed timer("Time to load AssignAlloc.facts");
    std::ifstream fs(kAssignAllocPath);
    for (std::string line; std::getline(fs, line);) {
      uint32_t var;
      uint32_t heap;
      if (2 == sscanf(line.c_str(), "%u\t%u", &var, &heap)) {
        assign_alloc_facts.Add(var, heap);
      }
    }
  }

  Vector<uint32_t, uint32_t, uint32_t> load_facts(storage, 1);
  {
    Timed timer("Time to load Load.facts");
    std::ifstream fs(kLoadPath);
    for (std::string line; std::getline(fs, line);) {
      uint32_t base;
      uint32_t dest;
      uint32_t field;
      if (3 == sscanf(line.c_str(), "%u\t%u\t%u", &base, &dest, &field)) {
        load_facts.Add(base, dest, field);
      }
    }
  }

  Vector<uint32_t, uint32_t> primitive_assign_facts(storage, 2);
  {
    Timed timer("Time to load PrimitiveAssign.facts");
    std::ifstream fs(kPrimitiveAssignPath);
    for (std::string line; std::getline(fs, line);) {
      uint32_t source;
      uint32_t dest;
      if (2 == sscanf(line.c_str(), "%u\t%u", &source, &dest)) {
        primitive_assign_facts.Add(source, dest);
      }
    }
  }

  Vector<uint32_t, uint32_t, uint32_t> store_facts(storage, 3);
  {
    Timed timer("Time to load Store.facts");
    std::ifstream fs(kStorePath);
    for (std::string line; std::getline(fs, line);) {
      uint32_t source;
      uint32_t base;
      uint32_t field;
      if (3 == sscanf(line.c_str(), "%u\t%u\t%u", &source, &base, &field)) {
        store_facts.Add(source, base, field);
      }
    }
  }

  {
    Timed timer("Time to apply all inputs");
    db.assign_alloc_2(std::move(assign_alloc_facts));
    db.load_3(std::move(load_facts));
    db.primitive_assign_2(std::move(primitive_assign_facts));
    db.store_3(std::move(store_facts));
//    db.proc_41_(std::move(primitive_assign_facts), std::move(store_facts),
//                std::move(load_facts), std::move(assign_alloc_facts));
  }

  {
    Timed timer("Time to write Alias.tsv");
    std::ofstream fs(kAliasPath);
    db.alias_ff([&fs] (uint32_t x, uint32_t y) {
      fs << x << '\t' << y << '\n';
      return true;
    });
  }

  {
    Timed timer("Time to write Assign.tsv");
    std::ofstream fs(kAssignPath);
    db.assign_ff([&fs] (uint32_t source, uint32_t dest) {
      fs << source << '\t' << dest << '\n';
      return true;
    });
  }

  {
    Timed timer("Time to write VarPointsTo.tsv");
    std::ofstream fs(kVarPointsToPath);
    db.var_points_to_ff([&fs] (uint32_t var, uint32_t heap) {
      fs << var << '\t' << heap << '\n';
      return true;
    });
  }
}

