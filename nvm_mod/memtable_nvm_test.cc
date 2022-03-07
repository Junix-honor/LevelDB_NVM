
#include "nvm_mod/memtable_nvm.h"

#include "db/dbformat.h"
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
#include "util/random.h"
#include "util/testutil.h"

#include "gtest/gtest.h"
#include "nvm_mod/nvm_option.h"
#include "nvm_mod/persistent_skiplist.h"
#include "nvm_mod/pmem_manager.h"

namespace leveldb {

typedef const char* Key;
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

struct Data {
  Data(SequenceNumber iseq, ValueType itype, std::string ikey,
       std::string ivalue)
      : seq(iseq), type(itype), key(ikey), value(ivalue) {}
  SequenceNumber seq;
  ValueType type;
  std::string key;
  std::string value;
};

struct DataCmp {
  bool operator()(const Data& a, const Data& b) {
    if (a.key != b.key) return a.key < b.key;
    if (a.seq != b.seq) return a.seq > b.seq;
    if (a.type != b.type) return a.type > b.type;
    return false;
  }
};

TEST(MemTableNVMTest, InsertAndLookup) {
  NVMOption nvm_option;
  nvm_option.write_buffer_size = 4 * 1024 * 1024;
  nvm_option.pmem_path = "/mnt/hjxPMem";
  std::string filename = "test.pool";
  const Comparator* cmp1 = BytewiseComparator();
  const InternalKeyComparator cmp2(cmp1);

  const int N = 10;
  std::set<Data, DataCmp> data_set;
  Random rnd(301);

  //创建
  {
    PmemManager allocator(&nvm_option, filename);
    MemTableNVM memtable(cmp2, &allocator);
    memtable.Clear();

    for (int i = 0; i < N; i++) {
      std::string key = strRand(5);
      std::string value = strRand(10);
      SequenceNumber seq = rnd.Uniform(100);
      ValueType type = kTypeValue;
      memtable.Add(seq, type, key, value);
      data_set.insert(Data(seq, type, key, value));
      std::cout << "seq:" << seq << " type:" << type << " key:" << key
                << " value:" << value << std::endl;
      Status s;
      std::string get_value;
      LookupKey lookupkey(key, seq);
      ASSERT_TRUE(memtable.Get(lookupkey, &get_value, &s));
      std::cout << "look up key:" << key << " value:" << get_value << std::endl;
      ASSERT_EQ(value, get_value);
    }

    // Forward iteration test
    {
      Iterator* iter = memtable.NewIterator();
      iter->SeekToFirst();

      std::cout << "===Forward iteration test===" << std::endl;

      // Compare against model iterator
      for (auto model_iter = data_set.begin(); model_iter != data_set.end();
           ++model_iter) {
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(model_iter->value, iter->value().ToString());
        std::cout << "val:" << iter->value().ToString() << std::endl;
        iter->Next();
      }
      ASSERT_TRUE(!iter->Valid());
      delete iter;
    }

    //     Backward iteration test
    {
      Iterator* iter = memtable.NewIterator();
      iter->SeekToLast();

      std::cout << "===Backward iteration test===" << std::endl;

      // Compare against model iterator
      for (auto model_iter = data_set.rbegin(); model_iter != data_set.rend();
           ++model_iter) {
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(model_iter->value, iter->value().ToString());
        std::cout << "val:" << iter->value().ToString() << std::endl;
        iter->Prev();
      }
      ASSERT_TRUE(!iter->Valid());
      delete iter;
    }
  }
  //恢复
  {
    PmemManager allocator(&nvm_option, filename);
    MemTableNVM memtable(cmp2, &allocator);

    // Forward iteration test
    {
      Iterator* iter = memtable.NewIterator();
      iter->SeekToFirst();

      std::cout << "===Forward iteration test===" << std::endl;

      // Compare against model iterator
      for (auto model_iter = data_set.begin(); model_iter != data_set.end();
           ++model_iter) {
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(model_iter->value, iter->value().ToString());
        std::cout << "val:" << iter->value().ToString() << std::endl;
        iter->Next();
      }
      ASSERT_TRUE(!iter->Valid());
      delete iter;
    }

    // Backward iteration test
    {
      Iterator* iter = memtable.NewIterator();
      iter->SeekToLast();

      std::cout << "===Backward iteration test===" << std::endl;

      // Compare against model iterator
      for (auto model_iter = data_set.rbegin(); model_iter != data_set.rend();
           ++model_iter) {
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(model_iter->value, iter->value().ToString());
        std::cout << "val:" << iter->value().ToString() << std::endl;
        iter->Prev();
      }
      ASSERT_TRUE(!iter->Valid());
      delete iter;
    }
  }
}
}  // namespace leveldb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}