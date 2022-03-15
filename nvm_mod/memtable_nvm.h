#pragma once

#include "db/dbformat.h"
#include "db/memtablerep.h"
#include <string>

#include "leveldb/db.h"

#include "libpmem.h"
#include "nvm_mod/persistent_skiplist.h"
#include "nvm_mod/pmem_manager.h"
namespace leveldb {
class InternalKeyComparator;
class MemTableNVM : public MemTableRep {
 public:
  static const int MAX_SEQUENCE_OFFSET = 0;  // MAX_SEQUENCE偏移量
  static const int MAX_SEQUENCE_SIZE = 8;    // MAX_SEQUENCE大小

  static const int MIN_SEQUENCE_OFFSET =
      MAX_SEQUENCE_OFFSET + MAX_SEQUENCE_SIZE;  // MIN_SEQUENCE偏移量
  static const int MIN_SEQUENCE_SIZE = 8;       // MIN_SEQUENCE大小

  static const int MEM_TABLE_DATA_OFFSET =
      MIN_SEQUENCE_OFFSET + MIN_SEQUENCE_SIZE;  // 数据偏移量

 private:
  inline char* GetPmemMinSequence() {
    return allocator_.GetDataStart() + MIN_SEQUENCE_OFFSET;
  }
  inline char* GetPmemMaxSequence() {
    return allocator_.GetDataStart() + MAX_SEQUENCE_OFFSET;
  }

 public:
  // MemTables are reference counted.  The initial reference count
  // is zero and the caller must call Ref() at least once.
  explicit MemTableNVM(const InternalKeyComparator& comparator,
                       const NVMOption* nvm_option, std::string filename);

  MemTableNVM(const MemTableNVM&) = delete;
  MemTableNVM& operator=(const MemTableNVM&) = delete;

  // Increase reference count.
  void Ref() override { ++refs_; }

  // Drop reference count.  Delete if no more references exist.
  void Unref() override {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      delete this;
    }
  }

  // Returns an estimate of the number of bytes of data in use by this
  // data structure. It is safe to call when MemTableNVM is being modified.
  size_t ApproximateMemoryUsage() override;

  // Return an iterator that yields the contents of the memtable.
  //
  // The caller must ensure that the underlying MemTableNVM remains live
  // while the returned iterator is live.  The keys returned by this
  // iterator are internal keys encoded by AppendInternalKey in the
  // db/format.{h,cc} module.
  Iterator* NewIterator() override;

  // Add an entry into memtable that maps key to value at the
  // specified sequence number and with the specified type.
  // Typically value will be empty if type==kTypeDeletion.
  void Add(SequenceNumber seq, ValueType type, const Slice& key,
           const Slice& value) override;

  // If memtable contains a value for key, stre it in *value and return true.
  // If memtable contains a deletion for key, store a NotFound() error
  // in *status and return true.
  // Else, return false.
  bool Get(const LookupKey& key, std::string* value, SequenceNumber* seq,
           Status* s) override;

  void Clear(uint64_t earliest_seq) override;
  bool IsPersistent() override { return true; }

  // recovery needs
  SequenceNumber GetMaxSequenceNumber() override { return *max_sequence; }

  // transaction needs
  SequenceNumber GetEarliestSequenceNumber() override {
    return *earliest_sequence;
  }

  ~MemTableNVM() override;

  class MemTableIterator;

 private:
  struct KeyComparator {
    const InternalKeyComparator comparator;
    explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) {}
    int operator()(const char* a, const char* b) const;
  };

  typedef PersistentSkipList<KeyComparator> Table;

  uint64_t* earliest_sequence;
  uint64_t* max_sequence;

  KeyComparator comparator_;
  int refs_;
  PmemManager allocator_;
  Table table_;
};
}  // namespace leveldb