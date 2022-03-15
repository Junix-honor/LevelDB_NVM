#pragma once

#include "leveldb/db.h"

#include "transactions/optimistic_transaction_db.h"

namespace leveldb {
class OptimisticTransactionDBImpl : public OptimisticTransactionDB {
 public:
  explicit OptimisticTransactionDBImpl(DB* db) : OptimisticTransactionDB(db) {}
  ~OptimisticTransactionDBImpl() override = default;

  OptimisticTransaction* BeginTransaction(
      const WriteOptions& write_options,
      OptimisticTransaction* old_txn) override;

 private:
  void ReinitializeTransaction(OptimisticTransaction* txn,
                               const WriteOptions& write_options);
};
}  // namespace leveldb
