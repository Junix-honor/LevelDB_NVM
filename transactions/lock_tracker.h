#pragma once
#include "db/dbformat.h"
#include <unordered_map>

namespace leveldb {

struct PointLockRequest {
  // The key to lock.
  std::string key;
  // The sequence number from which there is no concurrent update to key.
  SequenceNumber seq = 0;
  // Whether the lock is acquired only for read.
  bool read_only = false;
  // Whether the lock is in exclusive mode.
  bool exclusive = true;
};

struct PointLockStatus {
  // Whether the key is locked.
  bool locked = false;
  // Whether the key is locked in exclusive mode.
  bool exclusive = true;
  // The sequence number in the tracked PointLockRequest.
  SequenceNumber seq = 0;
};

struct TrackedKeyInfo {
  // Earliest sequence number that is relevant to this transaction for this key
  SequenceNumber seq;

  uint32_t num_writes;
  uint32_t num_reads;

  bool exclusive;

  explicit TrackedKeyInfo(SequenceNumber seq_no)
      : seq(seq_no), num_writes(0), num_reads(0), exclusive(false) {}

  void Merge(const TrackedKeyInfo& info) {
    assert(seq <= info.seq);
    num_reads += info.num_reads;
    num_writes += info.num_writes;
    exclusive = exclusive || info.exclusive;
  }
};

using TrackedKeyInfos = std::unordered_map<std::string, TrackedKeyInfo>;

class PointLockTracker {
 public:
  class TrackedKeysIterator {
   public:
    explicit TrackedKeysIterator(const TrackedKeyInfos& infos)
        : key_infos_(infos), it_(key_infos_.begin()) {}
    ~TrackedKeysIterator() = default;

    // Whether there are remaining keys.
    bool HasNext() const { return it_ != key_infos_.end(); }

    // Gets the next key.
    //
    // If HasNext is false, calling this method has undefined behavior.
    const std::string& Next() { return (it_++)->first; }

   private:
    const TrackedKeyInfos& key_infos_;
    TrackedKeyInfos::const_iterator it_;
  };

 public:
  PointLockTracker() = default;

  PointLockTracker(const PointLockTracker&) = delete;
  PointLockTracker& operator=(const PointLockTracker&) = delete;

  void Track(const PointLockRequest& lock_request);

  void Clear();

  TrackedKeysIterator* GetKeyIterator() const;

  PointLockStatus GetPointLockStatus(const std::string& key) const;

 private:
  TrackedKeyInfos tracked_keys_;
};
}  // namespace leveldb
