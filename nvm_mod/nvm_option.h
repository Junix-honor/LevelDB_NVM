#pragma once

#include <string>

namespace leveldb {

struct NVMOption {
  bool use_nvm_mem_module = false;
  std::string pmem_path;
  uint64_t write_buffer_size = 4 * 1024 * 1024;
};

}  // namespace leveldb