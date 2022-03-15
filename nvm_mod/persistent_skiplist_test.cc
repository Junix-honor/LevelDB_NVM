
#include "nvm_mod/persistent_skiplist.h"

#include <atomic>
#include <iostream>
#include <random>
#include <set>
#include <string>

#include "leveldb/env.h"
#include "leveldb/slice.h"

#include "port/port.h"
#include "port/thread_annotations.h"
#include "util/hash.h"
#include "util/testutil.h"

#include "gtest/gtest.h"
#include "nvm_mod/nvm_option.h"
#include "nvm_mod/pmem_manager.h"

namespace leveldb {

struct MyComparator {
  int operator()(const Slice& a, const Slice& b) const { return a.compare(b); }
};

std::string strRand(int length) {  // length: 产生字符串的长度
  char tmp;                        // tmp: 暂存一个随机数
  std::string buffer;              // buffer: 保存返回值

  // 下面这两行比较重要:
  std::random_device rd;  // 产生一个 std::random_device 对象 rd
  std::default_random_engine random(
      rd());  // 用 rd 初始化一个随机数发生器 random

  for (int i = 0; i < length; i++) {
    tmp = random() % 36;  // 随机一个小于 36 的整数，0-9、A-Z 共 36 种字符
    if (tmp < 10) {  // 如果随机数小于 10，变换成一个阿拉伯数字的 ASCII
      tmp += '0';
    } else {  // 否则，变换成一个大写字母的 ASCII
      tmp -= 10;
      tmp += 'A';
    }
    buffer += tmp;
  }
  return buffer;
}

TEST(SkipTest, Empty) {
  NVMOption nvm_option;
  nvm_option.write_buffer_size = 4 * 1024 * 1024;
  //  nvm_option.pmem_path = "/mnt/hjxPMem";
  nvm_option.pmem_path = "/mnt/d";
  std::string filename = "/mnt/d/test.pool";
  PmemManager allocator(&nvm_option, filename);
  MyComparator cmp;
  allocator.Clear();
  allocator.Allocate(8);
  PersistentSkipList<MyComparator> list(cmp, &allocator, 8);
  list.Clear();

  std::cout << "memusage:" << allocator.MemoryUsage() << std::endl;
  std::cout << "maxheight:" << list.GetMaxHeight() << std::endl;
  ASSERT_TRUE(!list.Contains((char*)100));

  PersistentSkipList<MyComparator>::Iterator iter(&list);
  ASSERT_TRUE(!iter.Valid());
  iter.SeekToFirst();
  ASSERT_TRUE(!iter.Valid());
  iter.Seek((char*)300);
  ASSERT_TRUE(!iter.Valid());
  iter.SeekToLast();
  ASSERT_TRUE(!iter.Valid());
}

TEST(SkipTest, InsertAndLookup) {
  const int N = 20;
  std::set<std::string> keys;

  NVMOption nvm_option;
  nvm_option.write_buffer_size = 4 * 1024 * 1024;
  //  nvm_option.pmem_path = "hjxPMem";
  nvm_option.pmem_path = "/mnt/d";
  std::string filename = "/mnt/d/test.pool";
  MyComparator cmp;

  //创建
  {
    PmemManager allocator(&nvm_option, filename);
    std::cout << "memusage:" << allocator.MemoryUsage() << std::endl;
    PersistentSkipList<MyComparator> list(cmp, &allocator, 8);
    std::cout << "maxheight:" << list.GetMaxHeight() << std::endl;
    for (int i = 0; i < N; i++) {
      std::string key = strRand(10);
      char* buf = allocator.Allocate(key.size());
      std::strcpy(buf, key.c_str());
      if (keys.insert(key).second) {
        list.Insert(buf);
      }
    }

    // Simple iterator tests
    {
      PersistentSkipList<MyComparator>::Iterator iter(&list);
      ASSERT_TRUE(!iter.Valid());

      iter.SeekToFirst();
      ASSERT_TRUE(iter.Valid());
      ASSERT_EQ(*(keys.begin()), iter.key());

      iter.SeekToLast();
      ASSERT_TRUE(iter.Valid());
      ASSERT_EQ(*(keys.rbegin()), iter.key());
    }

    // Forward iteration test
    {
      PersistentSkipList<MyComparator>::Iterator iter(&list);
      iter.SeekToFirst();

      std::cout << "===Forward iteration test===" << std::endl;

      // Compare against model iterator
      for (auto model_iter = keys.begin(); model_iter != keys.end();
           ++model_iter) {
        ASSERT_TRUE(iter.Valid());
        ASSERT_EQ(*model_iter, iter.key());
        std::cout << iter.key() << std::endl;
        iter.Next();
      }
      ASSERT_TRUE(!iter.Valid());
    }

    // Backward iteration test
    {
      PersistentSkipList<MyComparator>::Iterator iter(&list);
      iter.SeekToLast();

      std::cout << "===Backward iteration test===" << std::endl;

      // Compare against model iterator
      for (auto model_iter = keys.rbegin(); model_iter != keys.rend();
           ++model_iter) {
        ASSERT_TRUE(iter.Valid());
        ASSERT_EQ(*model_iter, iter.key());
        std::cout << iter.key() << std::endl;
        iter.Prev();
      }
      ASSERT_TRUE(!iter.Valid());
    }
  }
  //恢复
  {
    PmemManager allocator(&nvm_option, filename);
    PersistentSkipList<MyComparator> list(cmp, &allocator, 8);
    // Forward iteration test
    {
      PersistentSkipList<MyComparator>::Iterator iter(&list);
      iter.SeekToFirst();

      std::cout << "===Forward iteration test===" << std::endl;

      // Compare against model iterator
      for (auto model_iter = keys.begin(); model_iter != keys.end();
           ++model_iter) {
        ASSERT_TRUE(iter.Valid());
        ASSERT_EQ(*model_iter, iter.key());
        std::cout << iter.key() << std::endl;
        iter.Next();
      }
      ASSERT_TRUE(!iter.Valid());
    }

    // Backward iteration test
    {
      PersistentSkipList<MyComparator>::Iterator iter(&list);
      iter.SeekToLast();

      std::cout << "===Backward iteration test===" << std::endl;

      // Compare against model iterator
      for (auto model_iter = keys.rbegin(); model_iter != keys.rend();
           ++model_iter) {
        ASSERT_TRUE(iter.Valid());
        ASSERT_EQ(*model_iter, iter.key());
        std::cout << iter.key() << std::endl;
        iter.Prev();
      }
      ASSERT_TRUE(!iter.Valid());
    }
  }
}
}  // namespace leveldb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}