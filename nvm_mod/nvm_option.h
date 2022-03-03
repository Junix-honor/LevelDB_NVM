#pragma once

#include <string>

namespace leveldb {

struct NVMOption {
  bool use_nvm_module;
  std::string pmem_path;
  uint64_t write_buffer_size;
};

}  // namespace leveldb