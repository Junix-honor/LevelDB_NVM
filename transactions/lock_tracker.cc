#include "lock_tracker.h"
namespace leveldb {

void PointLockTracker::Track(const PointLockRequest& lock_request) {
  auto it = tracked_keys_.find(lock_request.key);
  if (it == tracked_keys_.end()) {
    auto result = tracked_keys_.emplace(lock_request.key,
                                        TrackedKeyInfo(lock_request.seq));
    it = result.first;
  } else if (lock_request.seq < it->second.seq) {
    // Now tracking this key with an earlier sequence number
    it->second.seq = lock_request.seq;
  }

  if (lock_request.read_only) {
    it->second.num_reads++;
  } else {
    it->second.num_writes++;
  }

  it->second.exclusive = it->second.exclusive || lock_request.exclusive;
}
PointLockTracker::TrackedKeysIterator* PointLockTracker::GetKeyIterator()
    const {
  return new TrackedKeysIterator(tracked_keys_);
}
PointLockStatus PointLockTracker::GetPointLockStatus(
    const std::string& key) const {
  PointLockStatus status;
  auto key_it = tracked_keys_.find(key);
  if (key_it == tracked_keys_.end()) {
    return status;
  }
  const TrackedKeyInfo& key_info = key_it->second;
  status.locked = true;
  status.exclusive = key_info.exclusive;
  status.seq = key_info.seq;
  return status;
}
void PointLockTracker::Clear() { tracked_keys_.clear(); }

}  // namespace leveldb