#include "memtable_nvm.h"

#include "db/dbformat.h"

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"

#include "util/coding.h"

namespace leveldb {

static Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len;
  const char* p = data;
  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
  return Slice(p, len);
}

MemTableNVM::MemTableNVM(const InternalKeyComparator& comparator,
                         const NVMOption* nvm_option, std::string filename)
    : comparator_(comparator),
      refs_(0),
      allocator_(nvm_option, filename),
      table_(comparator_, &allocator_, MEM_TABLE_DATA_OFFSET) {
  earliest_sequence = (uint64_t*)GetPmemMinSequence();
  max_sequence = (uint64_t*)GetPmemMaxSequence();
}

MemTableNVM::~MemTableNVM() { assert(refs_ == 0); }

size_t MemTableNVM::ApproximateMemoryUsage() {
  return allocator_.MemoryUsage();
}

int MemTableNVM::KeyComparator::operator()(const char* aptr,
                                           const char* bptr) const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(aptr);
  Slice b = GetLengthPrefixedSlice(bptr);
  return comparator.Compare(a, b);
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
static const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  PutVarint32(scratch, target.size());
  scratch->append(target.data(), target.size());
  return scratch->data();
}

class MemTableNVM::MemTableIterator : public Iterator {
 public:
  explicit MemTableIterator(MemTableNVM::Table* table) : iter_(table) {}

  MemTableIterator(const MemTableIterator&) = delete;
  MemTableIterator& operator=(const MemTableIterator&) = delete;

  ~MemTableIterator() override = default;

  bool Valid() const override { return iter_.Valid(); }
  void Seek(const Slice& k) override { iter_.Seek(EncodeKey(&tmp_, k)); }
  void SeekToFirst() override { iter_.SeekToFirst(); }
  void SeekToLast() override { iter_.SeekToLast(); }
  void Next() override { iter_.Next(); }
  void Prev() override { iter_.Prev(); }
  Slice key() const override { return GetLengthPrefixedSlice(iter_.key()); }
  Slice value() const override {
    Slice key_slice = GetLengthPrefixedSlice(iter_.key());
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  Status status() const override { return Status::OK(); }

 private:
  MemTableNVM::Table::Iterator iter_;
  std::string tmp_;  // For passing to EncodeKey
};

Iterator* MemTableNVM::NewIterator() { return new MemTableIterator(&table_); }

void MemTableNVM::Add(SequenceNumber s, ValueType type, const Slice& key,
                      const Slice& value) {
  // Format of an entry is concatenation of:
  //  key_size     : varint32 of internal_key.size()
  //  key bytes    : char[internal_key.size()]
  //  value_size   : varint32 of value.size()
  //  value bytes  : char[value.size()]
  size_t key_size = key.size();
  size_t val_size = value.size();
  size_t internal_key_size = key_size + 8;
  const size_t encoded_len = VarintLength(internal_key_size) +
                             internal_key_size + VarintLength(val_size) +
                             val_size;
  char* pmem_buf = allocator_.Allocate(encoded_len);
  char buf[encoded_len];
  char* p = EncodeVarint32(buf, internal_key_size);
  std::memcpy(p, key.data(), key_size);
  p += key_size;
  EncodeFixed64(p, (s << 8) | type);
  p += 8;
  p = EncodeVarint32(p, val_size);
  std::memcpy(p, value.data(), val_size);
  assert(p + val_size == buf + encoded_len);
  pmem_memcpy_persist(pmem_buf, buf, encoded_len);
  table_.Insert(pmem_buf);
  if (s > *max_sequence) {
    *max_sequence = s;
    allocator_.flush(reinterpret_cast<const char*>(max_sequence), sizeof(max_sequence));
  }
}

bool MemTableNVM::Get(const LookupKey& key, std::string* value,
                      SequenceNumber* seq, Status* s) {
  Slice memkey = key.memtable_key();
  Table::Iterator iter(&table_);
  iter.Seek(memkey.data());
  if (iter.Valid()) {
    // entry format is:
    //    klength  varint32
    //    userkey  char[klength]
    //    tag      uint64
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    const char* entry = iter.key();
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);

    // seq
    const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
    *seq = tag >> 8;

    if (comparator_.comparator.user_comparator()->Compare(
            Slice(key_ptr, key_length - 8), key.user_key()) == 0) {
      // Correct user key
      switch (static_cast<ValueType>(tag & 0xff)) {
        case kTypeValue: {
          Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
          value->assign(v.data(), v.size());
          return true;
        }
        case kTypeDeletion:
          *s = Status::NotFound(Slice());
          return true;
      }
    }
  }
  return false;
}
void MemTableNVM::Clear(uint64_t earliest_seq) {
  allocator_.Clear();
  max_sequence =
      reinterpret_cast<uint64_t*>(allocator_.Allocate(sizeof(uint64_t)));
  *max_sequence = 0;
  allocator_.flush(reinterpret_cast<const char*>(max_sequence), sizeof(max_sequence));

  earliest_sequence =
      reinterpret_cast<uint64_t*>(allocator_.Allocate(sizeof(uint64_t)));
  *earliest_sequence = earliest_seq;
  allocator_.flush(reinterpret_cast<const char*>(earliest_sequence), sizeof(earliest_sequence));
  
  table_.Clear();
  refs_ = 0;
}

}  // namespace leveldb