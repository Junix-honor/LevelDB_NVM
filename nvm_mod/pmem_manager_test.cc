#include <iostream>

#include "util/random.h"

#include "gtest/gtest.h"
#include "nvm_mod/nvm_option.h"
#include "nvm_mod/pmem_manager.h"

namespace leveldb {

TEST(SkipListPmemManagerTest, Sample) {
  NVMOption nvm_option;
  nvm_option.write_buffer_size = 4 * 1024 * 1024;
  nvm_option.pmem_path = "/mnt/hjxPMem";
  std::string filename = "test.pool";

  const int N = 100000;
  size_t bytes = 0;
  Random rnd(301);
  //创建
  {
    std::vector<std::pair<size_t, char*>> allocated;
    PmemManager allocator(&nvm_option, filename);
    allocator.clear();

    for (int i = 0; i < N; i++) {
      size_t s;
      if (i % (N / 10) == 0) {
        s = i;
      } else {
        s = rnd.OneIn(4000)
                ? rnd.Uniform(6000)
                : (rnd.OneIn(10) ? rnd.Uniform(100) : rnd.Uniform(20));
      }
      if (s == 0) {
        // Our arena disallows size 0 allocations.
        s = 1;
      }
      char* r;
      //    if (rnd.OneIn(10)) {
      //      r = allocator.AllocateAligned(s);
      //    } else {
      //      r = allocator.Allocate(s);
      //    }
      r = allocator.Allocate(s);

      for (size_t b = 0; b < s; b++) {
        // Fill the "i"th allocation with a known bit pattern
        r[b] = i % 256;
      }
      bytes += s;
      allocated.push_back(std::make_pair(s, r));
      ASSERT_EQ(allocator.MemoryUsage(), bytes);
    }
    for (size_t i = 0; i < allocated.size(); i++) {
      size_t num_bytes = allocated[i].first;
      const char* p = allocated[i].second;
      for (size_t b = 0; b < num_bytes; b++) {
        // Check the "i"th allocation for the known bit pattern
        ASSERT_EQ(int(p[b]) & 0xff, i % 256);
      }
    }
    allocator.Sync();
    std::cout << "++++++++++create++++++++++" << std::endl;
    std::cout << "bytes:" << bytes << std::endl;
    std::cout << "mem usages:" << allocator.MemoryUsage() << std::endl;
  }

  //恢复
  {
    PmemManager allocator(&nvm_option, filename);
    ASSERT_EQ(allocator.MemoryUsage(), bytes);
  }
}

}  // namespace leveldb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
