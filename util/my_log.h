#pragma once


#include <string>
#include <stdarg.h>

namespace leveldb {

#define LZW_INFO
#ifdef LZW_INFO
#define RECORD_INFO(file_num,format,...)   LZW_LOG(file_num,format,##__VA_ARGS__)

#else
#define RECORD_INFO(file_num,format,...)
#endif

#define PRI_DEBUG
#ifdef PRI_DEBUG
#define DBG_PRINT(format, a...) printf(" DEBUG:%4d %-20s : " format, __LINE__, __FUNCTION__,  ##a)
#else
#define DBG_PRINT(format, a...)
#endif

#define LZW_DEBUG

#ifdef LZW_DEBUG
#define RECORD_LOG(format,...)   LZW_LOG(0,format,##__VA_ARGS__)

#else
#define RECORD_LOG(format,...)
#endif


const std::string log_file0(".//perf_log//RUN_LOG");
const std::string log_file1(".//perf_log//OP_TIME.csv");
const std::string log_file2(".//perf_log//OP_DATA");
const std::string log_file3(".//perf_log//STALL_SLEEP.csv");
const std::string log_file4(".//perf_log//STALL_MINOR_COMPACTION.csv");
const std::string log_file5(".//perf_log//STALL_MAJOR_COMPACTION.csv");
const std::string log_file6(".//perf_log//BACKGROUND_MINOR_COMPACTION.csv");
const std::string log_file7(".//perf_log//BACKGROUND_MAJOR_COMPACTION.csv");
const std::string log_file8(".//perf_log//BACKGROUND_COMPACTION.csv");
const std::string log_file9(".//perf_log//SWITCH_MEMTABLE.csv");
const std::string log_file10(".//perf_log//PERF_LOG");
const std::string log_file11(".//perf_log//BACKGROUND_MAJOR_INNER_MINOR_COMPACTION.csv");
// const std::string log_file4("Latency.csv");
// const std::string log_file5("PerSecondLatency.csv");


extern void init_log_file();

extern void LZW_LOG(int file_num,const char* format, ...);


}


