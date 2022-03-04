#include "persistent_skiplist.h"

namespace leveldb {
// Node结构体
template <typename Key, class Comparator>
struct PersistentSkipList<Key, Comparator>::Node {
  explicit Node(const Key& k, const Key& mem)
      : key_offset(reinterpret_cast<const intptr_t>(mem - k)) {}

  intptr_t const key_offset;

  // Accessors/mutators for links.  Wrapped in methods so we can
  // add the appropriate barriers as necessary.
  Node* Next(int n) {
    assert(n >= 0);
    // Use an 'acquire load' so that we observe a fully initialized
    // version of the returned Node.
    intptr_t offset = next_[n].load(std::memory_order_acquire);
    return (offset != 0) ? reinterpret_cast<Node*>((intptr_t)this - offset)
                         : NULL;
  }
  void SetNext(int n, Node* x) {
    assert(n >= 0);
    // Use a 'release store' so that anybody who reads through this
    // pointer observes a fully initialized version of the inserted node.
    (x != NULL) ? next_[n].store((intptr_t)this - (intptr_t)x,
                                 std::memory_order_release)
                : next_[n].store(0, std::memory_order_release);
  }

  // No-barrier variants that can be safely used in a few locations.
  Node* NoBarrier_Next(int n) {
    assert(n >= 0);
    intptr_t offset = next_[n].load(std::memory_order_relaxed);
    return (offset != 0) ? reinterpret_cast<Node*>((intptr_t)this - offset)
                         : NULL;
  }
  void NoBarrier_SetNext(int n, Node* x) {
    assert(n >= 0);
    (x != NULL) ? next_[n].store((intptr_t)this - (intptr_t)x,
                                 std::memory_order_relaxed)
                : next_[n].store(0, std::memory_order_relaxed);
  }

 private:
  // Array of length equal to the node height.  next_[0] is lowest level link.
  std::atomic<intptr_t> next_[1];
};

// NewNode
template <typename Key, class Comparator>
typename PersistentSkipList<Key, Comparator>::Node*
PersistentSkipList<Key, Comparator>::NewNode(const Key& key, int height) {
  char* const node_memory = allocator_->AllocateAligned(
      sizeof(Node) + sizeof(std::atomic<intptr_t>) * (height - 1));
  return new (node_memory) Node(key, node_memory);
}

//迭代器相关函数
template <typename Key, class Comparator>
inline PersistentSkipList<Key, Comparator>::Iterator::Iterator(
    const PersistentSkipList* list) {
  list_ = list;
  node_ = nullptr;
}

template <typename Key, class Comparator>
inline bool PersistentSkipList<Key, Comparator>::Iterator::Valid() const {
  return node_ != nullptr;
}

template <typename Key, class Comparator>
const Key& PersistentSkipList<Key, Comparator>::Iterator::key() const {
  assert(Valid());
  return reinterpret_cast<Key>((intptr_t)node_ - node_->key_offset);
}

template <typename Key, class Comparator>
inline const intptr_t&
PersistentSkipList<Key, Comparator>::Iterator::key_offset() const {
  assert(Valid());
  return node_->key_offset;
}

template <typename Key, class Comparator>
inline void PersistentSkipList<Key, Comparator>::Iterator::Next() {
  assert(Valid());
  node_ = node_->Next(0);
}

template <typename Key, class Comparator>
inline void PersistentSkipList<Key, Comparator>::Iterator::Prev() {
  // Instead of using explicit "prev" links, we just search for the
  // last node that falls before key.
  assert(Valid());
  node_ = list_->FindLessThan(
      reinterpret_cast<Key>((intptr_t)node_ - node_->key_offset));
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

template <typename Key, class Comparator>
inline void PersistentSkipList<Key, Comparator>::Iterator::Seek(
    const Key& target) {
  node_ = list_->FindGreaterOrEqual(target, nullptr);
}

template <typename Key, class Comparator>
inline void PersistentSkipList<Key, Comparator>::Iterator::SeekToFirst() {
  node_ = list_->head_->Next(0);
}

template <typename Key, class Comparator>
inline void PersistentSkipList<Key, Comparator>::Iterator::SeekToLast() {
  node_ = list_->FindLast();
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

// RandomHeight
template <typename Key, class Comparator>
int PersistentSkipList<Key, Comparator>::RandomHeight() {
  // Increase height with probability 1 in kBranching
  static const unsigned int kBranching = 4;
  int height = 1;
  while (height < kMaxHeight && ((rnd_.Next() % kBranching) == 0)) {
    height++;
  }
  assert(height > 0);
  assert(height <= kMaxHeight);
  return height;
}

// KeyIsAfterNode
template <typename Key, class Comparator>
bool PersistentSkipList<Key, Comparator>::KeyIsAfterNode(
    const Key& key, PersistentSkipList::Node* n) const {
  // null n is considered infinite
  return (n != nullptr) &&
         (compare_(reinterpret_cast<Key>((intptr_t)n - n->key_offset), key) <
          0);
}

// FindGreaterOrEqual
template <typename Key, class Comparator>
typename PersistentSkipList<Key, Comparator>::Node*
PersistentSkipList<Key, Comparator>::FindGreaterOrEqual(
    const Key& key, PersistentSkipList::Node** prev) const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    Node* next = x->Next(level);
    if (KeyIsAfterNode(key, next)) {
      // Keep searching in this list
      x = next;
    } else {
      if (prev != nullptr) prev[level] = x;
      if (level == 0) {
        return next;
      } else {
        // Switch to next list
        level--;
      }
    }
  }
}

// FindLessThan
template <typename Key, class Comparator>
typename PersistentSkipList<Key, Comparator>::Node*
PersistentSkipList<Key, Comparator>::FindLessThan(const Key& key) const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    assert(x == head_ ||
           compare_(reinterpret_cast<Key>((intptr_t)x - x->key_offset), key) <
               0);
    Node* next = x->Next(level);
    if (next == nullptr ||
        compare_(reinterpret_cast<Key>((intptr_t)next - next->key_offset),
                 key) >= 0) {
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
}

// FindLast
template <typename Key, class Comparator>
typename PersistentSkipList<Key, Comparator>::Node*
PersistentSkipList<Key, Comparator>::FindLast() const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    Node* next = x->Next(level);
    if (next == nullptr) {
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
}

// skiplist构造函数
template <typename Key, class Comparator>
PersistentSkipList<Key, Comparator>::PersistentSkipList(
    Comparator cmp, SkipListPmemManager* allocator)
    : compare_(cmp), allocator_(allocator), rnd_(0xdeadbeef) {
  pmem_max_height_ = (int32_t*)GetMaxHeight();
  sequence = (uint64_t*)GetSequence();
  head_ = (Node*)GetSkipListDataStart();
  max_height_.store(*pmem_max_height_, std::memory_order_relaxed);
}

template <typename Key, class Comparator>
void PersistentSkipList<Key, Comparator>::Insert(const Key& key, uint64_t s) {
  // TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
  // here since Insert() is externally synchronized.
  Node* prev[kMaxHeight];
  Node* x = FindGreaterOrEqual(key, prev);

  // Our data structure does not allow duplicate insertion
  assert(x == NULL ||
         !Equal(key, reinterpret_cast<Key>((intptr_t)x - x->key_offset)));

  *sequence = s;
  int height = RandomHeight();
  if (height > GetMaxHeight()) {
    for (int i = GetMaxHeight(); i < height; i++) {
      prev[i] = head_;
    }
    // It is ok to mutate max_height_ without any synchronization
    // with concurrent readers.  A concurrent reader that observes
    // the new value of max_height_ will see either the old value of
    // new level pointers from head_ (nullptr), or a new value set in
    // the loop below.  In the former case the reader will
    // immediately drop to the next level since nullptr sorts after all
    // keys.  In the latter case the reader will use the new node.
    max_height_.store(height, std::memory_order_relaxed);
    *pmem_max_height_ = max_height_;
    // TODO: flush
  }

  x = NewNode(key, height);
  for (int i = 0; i < height; i++) {
    // NoBarrier_SetNext() suffices since we will add a barrier when
    // we publish a pointer to "x" in prev[i].
    x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
    prev[i]->SetNext(i, x);
    // TODO:flush
  }
}

template <typename Key, class Comparator>
bool PersistentSkipList<Key, Comparator>::Contains(const Key& key) const {
  Node* x = FindGreaterOrEqual(key, nullptr);
  if (x != nullptr &&
      Equal(key, reinterpret_cast<Key>((intptr_t)x - x->key_offset))) {
    return true;
  } else {
    return false;
  }
}
template <typename Key, class Comparator>
void PersistentSkipList<Key, Comparator>::clear() {
  allocator_->clear();
  head_ = NewNode(0, kMaxHeight);
  *sequence = 0;
  *pmem_max_height_ = 1;
  max_height_.store(1, std::memory_order_relaxed);
  // TODO: flush
  for (int i = 0; i < kMaxHeight; i++) {
    head_->SetNext(i, nullptr);
  }
}
}  // namespace leveldb