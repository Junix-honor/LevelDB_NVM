#pragma once
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <libpmem.h>
#include <string.h>

#include "util/allocator.h"

#include "nvm_mod/nvm_option.h"
namespace leveldb {
class PmemManager : public Allocator {
 public:
  PmemManager(NVMOption* nvm_option, std::string filename);

  PmemManager(const PmemManager&) = delete;
  PmemManager& operator=(const PmemManager&) = delete;

  ~PmemManager();

  // Return a pointer to a newly allocated memory block of "bytes" bytes.
  char* Allocate(size_t bytes);

  // Allocate memory with the normal alignment guarantees provided by malloc.
  char* AllocateAligned(size_t bytes);

  // Returns an estimate of the total memory usage of data allocated
  // by the arena.
  size_t MemoryUsage() const { return (size_t)*memory_usage_; }

  void Clear();
  void Sync();

 public:
  //偏移量
  static const int MEMORY_USAGE_OFFSET = 0;             // MEMORY_USAGE偏移量
  static const int MEMORY_USAGE_SIZE = 4;  // MEMORY_USAGE大小

  static const int DATA_OFFSET =
      MEMORY_USAGE_OFFSET + MEMORY_USAGE_SIZE;  // 数据偏移量

  char* GetDataStart() { return pmem_addr + DATA_OFFSET; }
  char* GetMemoryUsage() { return pmem_addr + MEMORY_USAGE_OFFSET; }

 private:
  void OpenNVMFile();

  // Allocation state
  char* alloc_ptr_;
  size_t alloc_bytes_remaining_;

  // Total memory usage of the arena.
  uint32_t* memory_usage_;

  std::string pmem_path;
  size_t write_buffer_size;
  std::string pmem_file_name;

  char* pmem_addr = nullptr;
  size_t mapped_len;
  int is_pmem;
};
inline char* PmemManager::Allocate(size_t bytes) {
  assert(bytes > 0);
  if (bytes <= alloc_bytes_remaining_) {
    char* result = alloc_ptr_;
    alloc_ptr_ += bytes;
    alloc_bytes_remaining_ -= bytes;
    // TODO:?+sizeof(char*)
    *memory_usage_ = *memory_usage_ + bytes;
    return result;
  }
  return nullptr;
}

}  // namespace leveldb
