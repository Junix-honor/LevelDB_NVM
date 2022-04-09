#pragma once
#include <sys/time.h>

#include <cstdint>
#include <cstdio>
#include <unordered_map>

#include "histogram.h"

namespace leveldb {
namespace benchmark {
enum Type {
  FOREGROUND_COMPACTION,
  LOG,
  INSERT,
  GET_MEMTABLE,
  GET_VERSION,
};
static const Type AllTypes[] = {
    FOREGROUND_COMPACTION, LOG, INSERT, GET_MEMTABLE, GET_VERSION
};
class PerfLog {
 public:
  PerfLog() {
    names_.insert({Type::FOREGROUND_COMPACTION, "Foreground compaction"});
    names_.insert({Type::LOG, "log"});
    names_.insert({Type::INSERT, "Insert"});
    names_.insert({Type::GET_MEMTABLE, "Get from memtable"});
    names_.insert({Type::GET_VERSION, "Get from version"});
    for (auto type : AllTypes) {
      histograms_.insert({type, Histogram()});
    }
    Clear();
  }

  ~PerfLog() = default;

  void Clear() {
    for (auto type : AllTypes) {
      histograms_.at(type).Clear();
    }
  }

  void LogMicro(Type type, uint64_t micros) {
    histograms_.at(type).Add(micros);
  }

  std::string GetHistogram() {
    std::string r;
    for (auto type : AllTypes) {
      r.append("\n【");
      r.append(names_.at(type));
      r.append("】\n");
      r.append(histograms_.at(type).ToString());
    }
    return r;
  }

 private:
  std::unordered_map<Type, Histogram> histograms_;
  std::unordered_map<Type, std::string> names_;
};
extern void CreatePerfLog();
extern void ClearPerfLog();
extern uint64_t NowMicros();
extern void LogMicros(Type, uint64_t);
extern std::string GetHistogram();
extern void ClosePerfLog();

extern uint64_t bench_start_time;
}  // namespace benchmark
}  // namespace leveldb