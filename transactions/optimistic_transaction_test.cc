#include "transactions/optimistic_transaction.h"

#include <iostream>

#include "util/testutil.h"

#include "gtest/gtest.h"
#include "transactions/optimistic_transaction_db.h"
#include "transactions/optimistic_transaction_db_impl.h"
namespace leveldb {
class OptimisticTransactionTest : public testing::Test {
 public:
  OptimisticTransactionDB* txn_db;
  std::string dbname_;
  Options options;

  OptimisticTransactionTest() {
    options.create_if_missing = true;
    dbname_ = testing::TempDir() + "db_txn_test";
    DestroyDB(dbname_, options);
    Open();
  }
  ~OptimisticTransactionTest() {
    delete txn_db;
    DestroyDB(dbname_, options);
  }

  void Reopen() {
    delete txn_db;
    txn_db = nullptr;
    Open();
  }

 private:
  void Open() {
    Status s = OptimisticTransactionDB::Open(options, dbname_, &txn_db);
    ASSERT_LEVELDB_OK(s);
    ASSERT_NE(txn_db, nullptr);
  }
};

TEST_F(OptimisticTransactionTest, PutTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  ASSERT_LEVELDB_OK(txn_db->Put(write_options, Slice("foo"), Slice("bar")));
  ASSERT_LEVELDB_OK(txn_db->Put(write_options, Slice("foo2"), Slice("bar")));

  OptimisticTransaction* txn = txn_db->BeginTransaction(write_options);
  ASSERT_NE(txn, nullptr);

  ASSERT_LEVELDB_OK(txn->Put(Slice("foo"), Slice("bar2")));

  ASSERT_LEVELDB_OK(txn_db->Put(write_options, Slice("foo"), Slice("bar3")));

  ASSERT_LEVELDB_OK(txn_db->Get(read_options, "foo", &value));
  ASSERT_EQ(value, "bar3");

  ASSERT_TRUE((txn->Commit()).IsBusy());

  ASSERT_LEVELDB_OK(txn_db->Get(read_options, "foo", &value));
  ASSERT_EQ(value, "bar3");

  txn = txn_db->BeginTransaction(write_options, txn);
  ASSERT_NE(txn, nullptr);

  ASSERT_LEVELDB_OK(txn->Put(Slice("foo2"), Slice("bar2")));

  ASSERT_LEVELDB_OK(txn_db->Get(read_options, "foo2", &value));
  ASSERT_EQ(value, "bar");

  ASSERT_LEVELDB_OK(txn->Rollback());

  ASSERT_LEVELDB_OK(txn_db->Get(read_options, "foo2", &value));
  ASSERT_EQ(value, "bar");
  delete txn;
}

TEST_F(OptimisticTransactionTest, DeleteTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  ASSERT_LEVELDB_OK(txn_db->Put(write_options, Slice("foo"), Slice("bar")));
  ASSERT_LEVELDB_OK(txn_db->Put(write_options, Slice("foo2"), Slice("bar")));

  OptimisticTransaction* txn = txn_db->BeginTransaction(write_options);
  ASSERT_NE(txn, nullptr);

  ASSERT_LEVELDB_OK(txn->Delete(Slice("foo")));

  ASSERT_LEVELDB_OK(txn_db->Get(read_options, "foo", &value));
  ASSERT_EQ(value, "bar");

  Status s = txn->Commit();

  ASSERT_TRUE((txn_db->Get(read_options, "foo", &value)).IsNotFound());
  delete txn;
}

TEST_F(OptimisticTransactionTest, GetTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  ASSERT_LEVELDB_OK(txn_db->Put(write_options, Slice("foo"), Slice("bar")));
  ASSERT_LEVELDB_OK(txn_db->Put(write_options, Slice("foo2"), Slice("bar")));

  OptimisticTransaction* txn = txn_db->BeginTransaction(write_options);
  ASSERT_NE(txn, nullptr);

  ASSERT_LEVELDB_OK(txn->Delete(Slice("foo")));
  ASSERT_LEVELDB_OK(txn->Put(Slice("foo2"), Slice("bar2")));

  ASSERT_LEVELDB_OK(txn_db->Get(read_options, "foo", &value));
  ASSERT_EQ(value, "bar");
  ASSERT_LEVELDB_OK(txn_db->Get(read_options, "foo2", &value));
  ASSERT_EQ(value, "bar");

  ASSERT_TRUE((txn->Get(read_options, "foo", &value)).IsNotFound());
  ASSERT_LEVELDB_OK(txn->Get(read_options, "foo2", &value));
  ASSERT_EQ(value, "bar2");

  Status s = txn->Commit();

  ASSERT_TRUE((txn_db->Get(read_options, "foo", &value)).IsNotFound());
  ASSERT_LEVELDB_OK(txn_db->Get(read_options, "foo2", &value));
  ASSERT_EQ(value, "bar2");
  delete txn;
}

TEST_F(OptimisticTransactionTest, GetForUpdateTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  ASSERT_LEVELDB_OK(txn_db->Put(write_options, Slice("foo"), Slice("bar")));
  ASSERT_LEVELDB_OK(txn_db->Put(write_options, Slice("foo2"), Slice("bar")));

  // GetForUpdate
  OptimisticTransaction* txn = txn_db->BeginTransaction(write_options);
  ASSERT_NE(txn, nullptr);

  ASSERT_LEVELDB_OK(txn->GetForUpdate(read_options, "foo", &value));
  ASSERT_EQ(value, "bar");

  ASSERT_LEVELDB_OK(txn_db->Put(write_options, Slice("foo"), Slice("bar3")));

  ASSERT_TRUE((txn->Commit()).IsBusy());

  ASSERT_LEVELDB_OK(txn_db->Get(read_options, "foo", &value));
  ASSERT_EQ(value, "bar3");

  // Get
  txn = txn_db->BeginTransaction(write_options, txn);
  ASSERT_NE(txn, nullptr);

  ASSERT_LEVELDB_OK(txn->Get(read_options, "foo", &value));
  ASSERT_EQ(value, "bar3");

  ASSERT_LEVELDB_OK(txn_db->Put(write_options, Slice("foo"), Slice("bar4")));

  ASSERT_LEVELDB_OK(txn->Commit());

  ASSERT_LEVELDB_OK(txn_db->Get(read_options, "foo", &value));
  ASSERT_EQ(value, "bar4");
  delete txn;
}

TEST_F(OptimisticTransactionTest, SnapShotTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  ASSERT_LEVELDB_OK(txn_db->Put(write_options, Slice("foo"), Slice("bar")));
  ASSERT_LEVELDB_OK(txn_db->Put(write_options, Slice("foo2"), Slice("bar")));

  // SetSnapshot
  OptimisticTransaction* txn = txn_db->BeginTransaction(write_options);
  ASSERT_NE(txn, nullptr);

  txn->SetSnapshot();

  ASSERT_LEVELDB_OK(txn_db->Put(write_options, Slice("foo"), Slice("bar2")));

  ASSERT_LEVELDB_OK(txn->Put(Slice("foo"), Slice("bar3")));

  ASSERT_TRUE((txn->Commit()).IsBusy());

  ASSERT_LEVELDB_OK(txn_db->Get(read_options, "foo", &value));
  ASSERT_EQ(value, "bar2");

  // Default
  txn = txn_db->BeginTransaction(write_options, txn);
  ASSERT_NE(txn, nullptr);

  ASSERT_LEVELDB_OK(txn_db->Put(write_options, Slice("foo"), Slice("bar4")));

  ASSERT_LEVELDB_OK(txn->Put(Slice("foo"), Slice("bar5")));

  ASSERT_TRUE((txn->Commit()).ok());

  ASSERT_LEVELDB_OK(txn_db->Get(read_options, "foo", &value));
  ASSERT_EQ(value, "bar5");
  delete txn;
}

}  // namespace leveldb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}