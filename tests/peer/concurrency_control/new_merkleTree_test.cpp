#include <leveldb/db.h>
#include "peer/db/phmap_connection.h"
#include "gtest/gtest.h"
using namespace peer::db;
class MyExecutorTest : public ::testing::Test {
protected:
  void SetUp() override {
    util::OpenSSLSHA1::initCrypto();
    util::OpenSSLSHA256::initCrypto();
  }

  void TearDown() override {
    executor.chain.clear();
    executor.deleteDB();
  }
  peer::db::PHMapConnection::MyExecutor executor;
  peer::db::PHMapConnection::WriteBatch batch;
};

// 测试 MyExecutor 的 execute 方法
TEST_F(MyExecutorTest, ExecuteTest) {
  constexpr int recordCount = 3;
  for (int i=0; i<recordCount; i++) {
    batch.Put(std::to_string(i), "0");
  }
  executor.execute(batch);
  ASSERT_EQ(executor.lastBatch.writes.size(), recordCount);
}

TEST_F(MyExecutorTest, SyncWriteBatchTest) {
  constexpr int recordCount = 3;
  for (int i=0; i<recordCount; i++) {
    batch.Put(std::to_string(i), "0");
  }
  executor.execute(batch);
  executor.syncWriteBatch();
  peer::db::PHMapConnection::MyExecutor::Node& lastNode = executor.chain.back();
  ASSERT_FALSE(lastNode.merkleRoot.empty());
}

TEST_F(MyExecutorTest, ReadDBTestTrue) {
  constexpr int recordCount = 3;
  for (int i=0; i<recordCount; i++) {
    batch.Put(std::to_string(i), "0");
  }
  executor.execute(batch);
  executor.syncWriteBatch();
  std::string key = "1";
  auto result = executor.readDB(key);
  ASSERT_EQ(result.message,"0");
  ASSERT_TRUE(result.istrue);
}

TEST_F(MyExecutorTest, ReadDBTestFalse) {
  constexpr int recordCount = 3;
  for (int i=0; i<recordCount; i++) {
    batch.Put(std::to_string(i), "0");
  }
  executor.execute(batch);
  executor.syncWriteBatch();
  std::string key="1";
  leveldb::DB* merkledb;
  leveldb::Options options;
  leveldb::Status status
      = leveldb::DB::Open(options, "/home/user/CLionProjects/mass_bft/searchdb", &merkledb);
  leveldb::Status put_status = merkledb->Put(leveldb::WriteOptions(), key, "1");
  delete merkledb;
  auto result = executor.readDB(key);
  ASSERT_EQ(result.message,"1");
  ASSERT_FALSE(result.istrue.value());
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
