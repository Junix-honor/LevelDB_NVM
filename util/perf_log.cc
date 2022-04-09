#include "perf_log.h"

#include <sys/time.h>

#include <cstdarg>
#include <cstdint>
#include <cstdio>

namespace leveldb {

namespace benchmark {

static PerfLog* log;

void CreatePerfLog() { log = new PerfLog; }

void ClearPerfLog() {
  if (log == nullptr) return;
  log->Clear();
};

uint64_t NowMicros() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

void LogMicros(Type type, uint64_t micros) {
  if (log == nullptr) return;
  log->LogMicro(type, micros);
}

std::string GetHistogram() {
  if (log == nullptr) return std::string();
  return log->GetHistogram();
}

void ClosePerfLog() {
  delete log;
  log = nullptr;
}

uint64_t bench_start_time;

}  // namespace benchmark

}  // namespace leveldb
