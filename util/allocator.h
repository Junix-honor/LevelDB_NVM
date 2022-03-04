#pragma once

namespace leveldb {
class Allocator {
 public:
  virtual ~Allocator() {}

  virtual char* Allocate(size_t bytes) = 0;
  virtual char* AllocateAligned(size_t bytes) = 0;
  virtual size_t MemoryUsage() const = 0;
  virtual void clear()=0;
  virtual void Sync()=0;
  virtual char* GetDataStart()=0;
};
}  // namespace leveldb