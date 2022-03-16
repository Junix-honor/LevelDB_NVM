#pragma once

#include "leveldb/iterator.h"
#include "leveldb/options.h"
#include "leveldb/status.h"

namespace leveldb {
struct Options;
struct ReadOptions;
struct WriteOptions;
class WriteBatch;
class OptimisticTransaction;

class OptimisticTransactionDB : public DB {
 public:
  static Status Open(const Options& options, const std::string& name,
                     OptimisticTransactionDB** dbptr);

  OptimisticTransactionDB(const OptimisticTransactionDB&) = delete;
  OptimisticTransactionDB& operator=(const OptimisticTransactionDB&) = delete;

  virtual OptimisticTransaction* BeginTransaction(
      const WriteOptions& write_options,
      OptimisticTransaction* old_txn = nullptr) = 0;

  ~OptimisticTransactionDB() override { delete db_; };

  DB* GetBaseDB() { return db_; }

  // DB接口
  Status Put(const WriteOptions& options, const Slice& key,
             const Slice& value) override {
    return db_->Put(options, key, value);
  }
  Status Delete(const WriteOptions& options, const Slice& key) override {
    return db_->Delete(options, key);
  }

  Status Write(const WriteOptions& options, WriteBatch* updates,
               WriteCallback* callback = nullptr) override {
    return db_->Write(options, updates);
  }

  Status Get(const ReadOptions& options, const Slice& key,
             std::string* value) override {
    return db_->Get(options, key, value);
  }
  Iterator* NewIterator(const ReadOptions& options) override {
    return db_->NewIterator(options);
  }
  const Snapshot* GetSnapshot() override { return db_->GetSnapshot(); }

  void ReleaseSnapshot(const Snapshot* snapshot) override {
    return db_->ReleaseSnapshot(snapshot);
  }
  bool GetProperty(const Slice& property, std::string* value) override {
    return db_->GetProperty(property, value);
  }
  void GetApproximateSizes(const Range* range, int n,
                           uint64_t* sizes) override {
    db_->GetApproximateSizes(range, n, sizes);
  }
  void CompactRange(const Slice* begin, const Slice* end) override {
    db_->CompactRange(begin, end);
  }

 protected:
  DB* db_;
  explicit OptimisticTransactionDB(DB* db) : db_(db) {}
};
}  // namespace leveldb
