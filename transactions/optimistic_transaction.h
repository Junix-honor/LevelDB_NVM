#pragma once
#include "db/db_impl.h"
#include "db/dbformat.h"

#include "leveldb/db.h"
#include "leveldb/options.h"
#include "leveldb/write_batch.h"
#include "leveldb/write_callback.h"

#include "transactions/lock_tracker.h"
#include "transactions/optimistic_transaction_db.h"
namespace leveldb {
class OptimisticTransaction {
 public:
  OptimisticTransaction(OptimisticTransactionDB* txn_db,
                        const WriteOptions& write_options);

  void Reinitialize(OptimisticTransactionDB* txn_db,
                    const WriteOptions& write_options);

  Status Put(const Slice& key, const Slice& value,
             const bool assume_tracked = false);
  Status Delete(const Slice& key, const bool assume_tracked = false);

  Status Commit();

  Status Rollback();

  void Clear();

  Status CheckTransactionForConflicts();

 private:
  Status TryLock(const Slice& key, bool read_only, bool exclusive,
                 const bool do_validate = true);

  void TrackKey(const std::string& key, SequenceNumber seq, bool readonly,
                bool exclusive);

  Status CheckKeysForConflicts(bool cache_only);
  Status CheckKey(SequenceNumber earliest_seq, SequenceNumber snap_seq,
                  const std::string& key, bool cache_only);

 private:
  DB* db_;
  DBImpl* dbimpl_;
  PointLockTracker tracked_locks_;

  WriteOptions write_options_;
  OptimisticTransactionDB* txn_db_;
  WriteBatch write_batch_;
  // Stores that time the txn was constructed, in microseconds.
  uint64_t start_time_;
  // Count of various operations pending in this transaction
  uint64_t num_puts_ = 0;
  uint64_t num_deletes_ = 0;
};

class OptimisticTransactionCallback : public WriteCallback {
 public:
  explicit OptimisticTransactionCallback(OptimisticTransaction* txn)
      : txn_(txn) {}

  Status Callback(DB* db) override {
    return txn_->CheckTransactionForConflicts();
  }

  bool AllowWriteBatching() override { return false; }

 private:
  OptimisticTransaction* txn_;
};
}  // namespace leveldb
