#include "optimistic_transaction_db_impl.h"

#include "leveldb/db.h"

#include "optimistic_transaction.h"
#include "optimistic_transaction_db.h"
namespace leveldb {
OptimisticTransaction* OptimisticTransactionDBImpl::BeginTransaction(
    const WriteOptions& write_options, OptimisticTransaction* old_txn) {
  if (old_txn != nullptr) {
    ReinitializeTransaction(old_txn, write_options);
    return old_txn;
  } else {
    return new OptimisticTransaction(this, write_options);
  }
}
void OptimisticTransactionDBImpl::ReinitializeTransaction(
    OptimisticTransaction* txn, const WriteOptions& write_options) {
  assert(txn != nullptr);
  txn->Reinitialize(this, write_options);
}
Status OptimisticTransactionDB::Open(const Options& options,
                                     const std::string& name,
                                     OptimisticTransactionDB** dbptr) {
  Status s;
  DB* db;
  s = DB::Open(options, name, &db);
  if (s.ok()) {
    *dbptr = new OptimisticTransactionDBImpl(db);
  }
  return s;
}
}  // namespace leveldb
