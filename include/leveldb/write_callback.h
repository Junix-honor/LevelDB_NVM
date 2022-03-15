#pragma once
#include "db/dbformat.h"
namespace leveldb {
class DB;
class WriteCallback {
 public:
  virtual ~WriteCallback() {}

  // Will be called while on the write thread before the write executes.  If
  // this function returns a non-OK status, the write will be aborted and this
  // status will be returned to the caller of DB::Write().
  virtual Status Callback(DB* db) = 0;

  // return true if writes with this callback can be batched with other writes
  virtual bool AllowWriteBatching() = 0;
};
}  // namespace leveldb
