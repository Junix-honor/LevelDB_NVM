#include "optimistic_transaction.h"

#include <functional>
#include <inttypes.h>

namespace leveldb {
OptimisticTransaction::OptimisticTransaction(OptimisticTransactionDB* txn_db,
                                             const WriteOptions& write_options)
    : txn_db_(txn_db),
      db_(txn_db->GetBaseDB()),
      dbimpl_(static_cast<DBImpl*>(db_)),
      write_options_(write_options) {}
void OptimisticTransaction::Reinitialize(OptimisticTransactionDB* txn_db,
                                         const WriteOptions& write_options) {
  txn_db_ = txn_db;
  db_ = txn_db->GetBaseDB();
  dbimpl_ = static_cast<DBImpl*>(db_);
  write_options_ = write_options;
  Clear();
  ClearSnapshot();
}
Status OptimisticTransaction::Put(const Slice& key, const Slice& value,
                                  const bool assume_tracked) {
  const bool do_validate = !assume_tracked;
  Status s =
      TryLock(key, false /* read_only */, true /* exclusive */, do_validate);
  if (s.ok()) {
    write_batch_.Put(key, value);
    num_puts_++;
  }
  return s;
}
Status OptimisticTransaction::Delete(const Slice& key,
                                     const bool assume_tracked) {
  const bool do_validate = !assume_tracked;
  Status s =
      TryLock(key, false /* read_only */, true /* exclusive */, do_validate);
  if (s.ok()) {
    write_batch_.Delete(key);
    num_deletes_++;
  }
  return s;
}
Status OptimisticTransaction::TryLock(const Slice& key, bool read_only,
                                      bool exclusive, const bool do_validate) {
  if (!do_validate) {
    return Status::OK();
  }
  SequenceNumber seq;
  if (snapshot_) {
    seq = snapshot_->sequence_number();
  } else {
    seq = dbimpl_->GetLatestSequenceNumber();
  }
  std::string key_str = key.ToString();
  TrackKey(key_str, seq, read_only, exclusive);

  return Status::OK();
}
void OptimisticTransaction::TrackKey(const std::string& key, SequenceNumber seq,
                                     bool readonly, bool exclusive) {
  PointLockRequest r;
  r.key = key;
  r.seq = seq;
  r.read_only = readonly;
  r.exclusive = exclusive;

  tracked_locks_.Track(r);
}
Status OptimisticTransaction::Commit() {
  OptimisticTransactionCallback callback(this);
  Status s = dbimpl_->Write(write_options_, &write_batch_, &callback);
  if (s.ok()) {
    Clear();
  }
  return s;
}

Status OptimisticTransaction::Rollback() {
  Clear();
  return Status::OK();
}

void OptimisticTransaction::Clear() {
  write_batch_.Clear();
  tracked_locks_.Clear();
  num_puts_ = 0;
  num_deletes_ = 0;
}

Status OptimisticTransaction::CheckTransactionForConflicts() {
  return CheckKeysForConflicts(true /* cache_only */);
}
Status OptimisticTransaction::CheckKeysForConflicts(bool cache_only) {
  Status result;
  SequenceNumber earliest_seq = dbimpl_->GetEarliestMemTableSequenceNumber();
  std::unique_ptr<PointLockTracker::TrackedKeysIterator> key_it(
      tracked_locks_.GetKeyIterator());
  assert(key_it != nullptr);

  while (key_it->HasNext()) {
    const std::string& key = key_it->Next();
    PointLockStatus status = tracked_locks_.GetPointLockStatus(key);
    const SequenceNumber key_seq = status.seq;

    result = CheckKey(earliest_seq, key_seq, key, cache_only);
    if (!result.ok()) {
      break;
    }
  }
  return result;
}
Status OptimisticTransaction::CheckKey(SequenceNumber earliest_seq,
                                       SequenceNumber snap_seq,
                                       const std::string& key,
                                       bool cache_only) {
  Status result;
  bool need_to_read_sst = false;

  // Since it would be too slow to check the SST files, we will only use
  // the memtables to check whether there have been any recent writes
  // to this key after it was accessed in this transaction.  But if the
  // Memtables do not contain a long enough history, we must fail the
  // transaction.
  if (earliest_seq == kMaxSequenceNumber) {
    // The age of this memtable is unknown.  Cannot rely on it to check
    // for recent writes.  This error shouldn't happen often in practice as
    // the Memtable should have a valid earliest sequence number except in some
    // corner cases (such as error cases during recovery).
    need_to_read_sst = true;

    if (cache_only) {
      result = Status::TryAgain(
          "Transaction could not check for conflicts as the MemTable does not "
          "contain a long enough history to check write at SequenceNumber: ",
          std::to_string(snap_seq));
    }
  } else if (snap_seq < earliest_seq) {
    // Use <= for min_uncommitted since earliest_seq is actually the largest sec
    // before this memtable was created
    need_to_read_sst = true;

    if (cache_only) {
      // The age of this memtable is too new to use to check for recent
      // writes.
      char msg[300];
      snprintf(msg, sizeof(msg),
               "Transaction could not check for conflicts for operation at "
               "SequenceNumber %" PRIu64
               " as the MemTable only contains changes newer than "
               "SequenceNumber %" PRIu64
               ".  Increasing the value of the "
               "max_write_buffer_size_to_maintain option could reduce the "
               "frequency "
               "of this error.",
               snap_seq, earliest_seq);
      result = Status::TryAgain(msg);
    }
  }
  if (result.ok()) {
    SequenceNumber seq = kMaxSequenceNumber;
    bool found_record_for_key = false;

    SequenceNumber lower_bound_seq = snap_seq;
    Status s = dbimpl_->GetLatestSequenceForKey(
        key, !need_to_read_sst, lower_bound_seq, &seq, &found_record_for_key);
    if (!s.ok())
      result = s;
    else if (found_record_for_key && snap_seq < seq) {
      result = Status::Busy("write_conflict");
    }
  }

  return result;
}
Status OptimisticTransaction::Get(const ReadOptions& options, const Slice& key,
                                  std::string* value) {
  Status s;
  if (write_batch_.Get(key, value, &s)) return s;
  return dbimpl_->Get(options, key, value);
}
Status OptimisticTransaction::GetForUpdate(const ReadOptions& options,
                                           const Slice& key, std::string* value,
                                           bool exclusive,
                                           const bool do_validate) {
  Status s = TryLock(key, true /* read_only */, exclusive /* exclusive */,
                     do_validate);
  if (s.ok()) {
    s = Get(options, key, value);
  }
  return s;
}
void OptimisticTransaction::SetSnapshot() {
  const Snapshot* snapshot = dbimpl_->GetSnapshot();
  snapshot_.reset(snapshot, std::bind(&OptimisticTransaction::ReleaseSnapshot,
                                      this, std::placeholders::_1, db_));
}
void OptimisticTransaction::ReleaseSnapshot(const Snapshot* snapshot, DB* db) {
  if (snapshot != nullptr) {
    db->ReleaseSnapshot(snapshot);
  }
}

};  // namespace leveldb
