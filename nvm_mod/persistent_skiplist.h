#pragma once

#include "db/dbformat.h"
#include <atomic>
#include <cassert>
#include <cstdlib>
#include <string>

#include "util/allocator.h"
#include "util/random.h"

namespace leveldb {

template <class Comparator>
class PersistentSkipList {
 public:
  static const int MAX_HEIGHT_OFFSET = 0;  // MAX_HEIGHT偏移量
  static const int MAX_HEIGHT_SIZE = 4;    // MAX_HEIGHT大小

  static const int SEQUENCE_OFFSET =
      MAX_HEIGHT_OFFSET + MAX_HEIGHT_SIZE;  // MAX_HEIGHT偏移量
  static const int SEQUENCE_SIZE = 8;       // MAX_HEIGHT大小

  static const int SKIP_LIST_DATA_OFFSET =
      SEQUENCE_OFFSET + SEQUENCE_SIZE;  // 数据偏移量

 private:
  inline char* GetSkipListDataStart() {
    return allocator_->GetDataStart() + SKIP_LIST_DATA_OFFSET;
  }
  inline char* GetPmemMaxHeight() {
    return allocator_->GetDataStart() + MAX_HEIGHT_OFFSET;
  }
  inline char* GetSequence() {
    return allocator_->GetDataStart() + SEQUENCE_OFFSET;
  }

 private:
  struct Node;

 public:
  // Create a new SkipList object that will use "cmp" for comparing keys,
  // and will allocate memory using "*arena".  Objects allocated in the arena
  // must remain allocated for the lifetime of the skiplist object.
  explicit PersistentSkipList(Comparator cmp, Allocator* allocator);

  PersistentSkipList(const PersistentSkipList&) = delete;
  PersistentSkipList& operator=(const PersistentSkipList&) = delete;

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
  void Insert(const char* key, SequenceNumber s = 0);

  // Returns true iff an entry that compares equal to key is in the list.
  bool Contains(const char* key) const;

  void Clear();

  SequenceNumber GetSequenceNumber() {
    return *(reinterpret_cast<SequenceNumber*>(GetSequence()));
  }

  // Iteration over the contents of a skip list
  class Iterator {
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit Iterator(const PersistentSkipList* list);

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    const char* key() const;
    const intptr_t& key_offset() const;

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next();

    // Advances to the previous position.
    // REQUIRES: Valid()
    void Prev();

    // Advance to the first entry with a key >= target
    void Seek(const char* target);

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst();

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToLast();

   private:
    const PersistentSkipList* list_;
    Node* node_;
    // Intentionally copyable
  };
  inline int32_t GetMaxHeight() const {
    return max_height_.load(std::memory_order_relaxed);
  }

 private:
  enum { kMaxHeight = 12 };

  Node* NewNode(const char* key, int height);
  int RandomHeight();
  bool Equal(const char* a, const char* b) const {
    return (compare_(a, b) == 0);
  }

  // Return true if key is greater than the data stored in "n"
  bool KeyIsAfterNode(const char* key, Node* n) const;

  // Return the earliest node that comes at or after key.
  // Return nullptr if there is no such node.
  //
  // If prev is non-null, fills prev[level] with pointer to previous
  // node at "level" for every level in [0..max_height_-1].
  Node* FindGreaterOrEqual(const char* key, Node** prev) const;

  // Return the latest node with a key < key.
  // Return head_ if there is no such node.
  Node* FindLessThan(const char* key) const;

  // Return the last node in the list.
  // Return head_ if list is empty.
  Node* FindLast() const;

  // Immutable after construction
  Comparator const compare_;
  Allocator* const allocator_;  // Arena used for allocations of nodes

  Node* head_;

  // Modified only by Insert().  Read racily by readers, but stale
  // values are ok.
  std::atomic<int32_t> max_height_;  // Height of the entire list
  int32_t* pmem_max_height_;
  uint64_t* sequence;

  // Read/written only by Insert().
  Random rnd_;
};

// Node结构体
template <class Comparator>
struct PersistentSkipList<Comparator>::Node {
  explicit Node(const char* k, const char* mem)
      : key_offset(reinterpret_cast<intptr_t>(mem - k)) {}

  intptr_t key_offset;

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
template <class Comparator>
typename PersistentSkipList<Comparator>::Node*
PersistentSkipList<Comparator>::NewNode(const char* key, int height) {
  char* node_memory = allocator_->AllocateAligned(
      sizeof(Node) + sizeof(std::atomic<intptr_t>) * (height - 1));
  return new (node_memory) Node(key, node_memory);
}

//迭代器相关函数
template <class Comparator>
inline PersistentSkipList<Comparator>::Iterator::Iterator(
    const PersistentSkipList* list) {
  list_ = list;
  node_ = nullptr;
}

template <class Comparator>
inline bool PersistentSkipList<Comparator>::Iterator::Valid() const {
  return node_ != nullptr;
}

template <class Comparator>
const char* PersistentSkipList<Comparator>::Iterator::key() const {
  assert(Valid());
  return reinterpret_cast<const char*>((intptr_t)node_ - node_->key_offset);
}

template <class Comparator>
inline const intptr_t& PersistentSkipList<Comparator>::Iterator::key_offset()
    const {
  assert(Valid());
  return node_->key_offset;
}

template <class Comparator>
inline void PersistentSkipList<Comparator>::Iterator::Next() {
  assert(Valid());
  node_ = node_->Next(0);
}

template <class Comparator>
inline void PersistentSkipList<Comparator>::Iterator::Prev() {
  // Instead of using explicit "prev" links, we just search for the
  // last node that falls before key.
  assert(Valid());
  node_ = list_->FindLessThan(
      reinterpret_cast<char*>((intptr_t)node_ - node_->key_offset));
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

template <class Comparator>
inline void PersistentSkipList<Comparator>::Iterator::Seek(const char* target) {
  node_ = list_->FindGreaterOrEqual(target, nullptr);
}

template <class Comparator>
inline void PersistentSkipList<Comparator>::Iterator::SeekToFirst() {
  node_ = list_->head_->Next(0);
}

template <class Comparator>
inline void PersistentSkipList<Comparator>::Iterator::SeekToLast() {
  node_ = list_->FindLast();
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

// RandomHeight
template <class Comparator>
int PersistentSkipList<Comparator>::RandomHeight() {
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
template <class Comparator>
bool PersistentSkipList<Comparator>::KeyIsAfterNode(
    const char* key, PersistentSkipList::Node* n) const {
  // null n is considered infinite
  return (n != nullptr) &&
         (compare_(reinterpret_cast<char*>((intptr_t)n - n->key_offset), key) <
          0);
}

// FindGreaterOrEqual
template <class Comparator>
typename PersistentSkipList<Comparator>::Node*
PersistentSkipList<Comparator>::FindGreaterOrEqual(
    const char* key, PersistentSkipList::Node** prev) const {
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
template <class Comparator>
typename PersistentSkipList<Comparator>::Node*
PersistentSkipList<Comparator>::FindLessThan(const char* key) const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    assert(x == head_ ||
           compare_(reinterpret_cast<char*>((intptr_t)x - x->key_offset), key) <
               0);
    Node* next = x->Next(level);
    if (next == nullptr ||
        compare_(reinterpret_cast<char*>((intptr_t)next - next->key_offset),
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
template <class Comparator>
typename PersistentSkipList<Comparator>::Node*
PersistentSkipList<Comparator>::FindLast() const {
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
template <class Comparator>
PersistentSkipList<Comparator>::PersistentSkipList(Comparator cmp,
                                                   Allocator* allocator)
    : compare_(cmp), allocator_(allocator), rnd_(0xdeadbeef) {
  pmem_max_height_ = (int32_t*)GetPmemMaxHeight();
  sequence = (uint64_t*)GetSequence();
  head_ = (Node*)GetSkipListDataStart();
  max_height_.store(*pmem_max_height_, std::memory_order_relaxed);
}

template <class Comparator>
void PersistentSkipList<Comparator>::Insert(const char* key, SequenceNumber s) {
  // TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
  // here since Insert() is externally synchronized.
  Node* prev[kMaxHeight];
  Node* x = FindGreaterOrEqual(key, prev);

  // Our data structure does not allow duplicate insertion
  assert(x == NULL ||
         !Equal(key, reinterpret_cast<char*>((intptr_t)x - x->key_offset)));

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
//  allocator_->Sync();
}

template <class Comparator>
bool PersistentSkipList<Comparator>::Contains(const char* key) const {
  Node* x = FindGreaterOrEqual(key, nullptr);
  if (x != nullptr &&
      Equal(key, reinterpret_cast<char*>((intptr_t)x - x->key_offset))) {
    return true;
  } else {
    return false;
  }
}
template <class Comparator>
void PersistentSkipList<Comparator>::Clear() {
  allocator_->Clear();
  pmem_max_height_ =
      reinterpret_cast<int32_t*>(allocator_->Allocate(sizeof(int32_t)));
  sequence =
      reinterpret_cast<uint64_t*>(allocator_->Allocate(sizeof(uint64_t)));

  *sequence = 0;
  *pmem_max_height_ = 1;
  max_height_.store(1, std::memory_order_relaxed);
  head_ = NewNode(0, kMaxHeight);
  // TODO: flush
  for (int i = 0; i < kMaxHeight; i++) {
    head_->SetNext(i, nullptr);
  }
//  allocator_->Sync();
}
}  // namespace leveldb
