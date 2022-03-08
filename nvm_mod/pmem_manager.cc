#include "pmem_manager.h"

namespace leveldb {
PmemManager::PmemManager(const NVMOption* nvm_option, std::string filename) {
  write_buffer_size = nvm_option->write_buffer_size;
  pmem_path = nvm_option->pmem_path;
  pmem_file_name = filename;
  OpenNVMFile();
  memory_usage_ = (uint32_t*)GetMemoryUsage();
  alloc_bytes_remaining_ = write_buffer_size - *memory_usage_;
  alloc_ptr_ = GetDataStart() + *memory_usage_;
}
PmemManager::~PmemManager() { pmem_unmap(pmem_addr, mapped_len); }

void PmemManager::OpenNVMFile() {
  char buf[100];
  snprintf(buf, sizeof(buf), "%s/%s", pmem_path.c_str(),
           pmem_file_name.c_str());
  std::string pmem_file_path(buf, strlen(buf));
  pmem_addr = (char*)(pmem_map_file(
      pmem_file_path.c_str(), write_buffer_size + DATA_OFFSET, PMEM_FILE_CREATE,
      0666, &mapped_len, &is_pmem));
  assert(pmem_addr != nullptr);
}

char* PmemManager::AllocateAligned(size_t bytes) {
  const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;
  static_assert((align & (align - 1)) == 0,
                "Pointer size should be a power of 2");
  size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align - 1);
  size_t slop = (current_mod == 0 ? 0 : align - current_mod);
  size_t needed = bytes + slop;
  char* result;
  // TODO:如果空间不够了是否还要分配
  if (needed <= alloc_bytes_remaining_) {
    result = alloc_ptr_ + slop;
    alloc_ptr_ += needed;
    alloc_bytes_remaining_ -= needed;
    *memory_usage_ = *memory_usage_ + bytes;
  } else {
    result = nullptr;
  }
  assert((reinterpret_cast<uintptr_t>(result) & (align - 1)) == 0);
  return result;
}
void PmemManager::Sync() {
  if (is_pmem)
    pmem_persist(pmem_addr, mapped_len);
  else
    pmem_msync(pmem_addr, mapped_len);
}
void PmemManager::Clear() {
  *memory_usage_ = 0;
  alloc_bytes_remaining_ = write_buffer_size;
  alloc_ptr_ = GetDataStart();
}

}  // namespace leveldb
