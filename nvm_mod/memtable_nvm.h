#pragma once

#include "db/dbformat.h"
#include <string>

#include "leveldb/db.h"

#include "util/allocator.h"

#include "libpmem.h"
#include "nvm_mod/persistent_skiplist.h"
namespace leveldb {
class InternalKeyComparator;
class MemTableNVM {
 public:
  // MemTables are reference counted.  The initial reference count
  // is zero and the caller must call Ref() at least once.
  explicit MemTableNVM(const InternalKeyComparator& comparator,
                       Allocator* allocator);

  MemTableNVM(const MemTableNVM&) = delete;
  MemTableNVM& operator=(const MemTableNVM&) = delete;

  // Increase reference count.
  void Ref() { ++refs_; }

  // Drop reference count.  Delete if no more references exist.
  void Unref() {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      delete this;
    }
  }

  // Returns an estimate of the number of bytes of data in use by this
  // data structure. It is safe to call when MemTableNVM is being modified.
  size_t ApproximateMemoryUsage();

  // Return an iterator that yields the contents of the memtable.
  //
  // The caller must ensure that the underlying MemTableNVM remains live
  // while the returned iterator is live.  The keys returned by this
  // iterator are internal keys encoded by AppendInternalKey in the
  // db/format.{h,cc} module.
  Iterator* NewIterator();

  // Add an entry into memtable that maps key to value at the
  // specified sequence number and with the specified type.
  // Typically value will be empty if type==kTypeDeletion.
  void Add(SequenceNumber seq, ValueType type, const Slice& key,
           const Slice& value);

  // If memtable contains a value for key, store it in *value and return true.
  // If memtable contains a deletion for key, store a NotFound() error
  // in *status and return true.
  // Else, return false.
  bool Get(const LookupKey& key, std::string* value, Status* s);

  class MemTableIterator;

  ~MemTableNVM();

  void Clear();

 private:
  struct KeyComparator {
    const InternalKeyComparator comparator;
    explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) {}
    int operator()(const char* a, const char* b) const;
  };

  typedef PersistentSkipList<KeyComparator> Table;

  // Private since only Unref() should be used to delete it

  KeyComparator comparator_;
  int refs_;
  Allocator* allocator_;
  Table table_;
};
}  // namespace leveldb