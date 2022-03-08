#pragma once

#include "db/dbformat.h"
namespace leveldb {
class MemTableRep {
 public:
  explicit MemTableRep() {}

  virtual void Ref() = 0;

  virtual void Unref() = 0;

  virtual size_t ApproximateMemoryUsage() = 0;

  virtual Iterator* NewIterator() = 0;

  virtual void Add(SequenceNumber seq, ValueType type, const Slice& key,
                   const Slice& value) = 0;

  virtual bool Get(const LookupKey& key, std::string* value, Status* s) = 0;

  virtual void Clear() = 0;

  virtual bool IsPersistent() = 0;

  virtual SequenceNumber GetMaxSequenceNumber() = 0;

  virtual ~MemTableRep() {}

  //  class MemTableIterator {
  //    virtual ~MemTableIterator() {}
  //
  //    virtual bool Valid() const = 0;
  //    virtual void Seek(const Slice& k) = 0;
  //    virtual void SeekToFirst() = 0;
  //    virtual void SeekToLast() = 0;
  //    virtual void Next() = 0;
  //    virtual void Prev() = 0;
  //    virtual Slice key() const = 0;
  //    virtual Slice value() const = 0;
  //    virtual Status status() = 0;
  //  };

  // protected:
  //  struct KeyComparator {
  //    const InternalKeyComparator comparator;
  //    explicit KeyComparator(const InternalKeyComparator& c) : comparator(c)
  //    {} virtual int operator()(const char* a, const char* b) const = 0;
  //  };
};
}  // namespace leveldb