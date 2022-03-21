#pragma once

#include <string>

namespace leveldb {

struct NVMOption {
  bool use_nvm_mem_module = false;
  std::string pmem_path = "/mnt/hjxPMem/db_test";
  size_t write_buffer_size = 2ul * 1024 * 1024 * 1024;
};

}  // namespace leveldb