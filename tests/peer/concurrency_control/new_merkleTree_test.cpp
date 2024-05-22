#include <leveldb/db.h>
#include "peer/db/phmap_connection.h"
#include "gtest/gtest.h"
#include <fstream>
using namespace peer::db;
class MyExecutorTest : public ::testing::Test {
protected:
  void SetUp() override {
    util::OpenSSLSHA1::initCrypto();
    util::OpenSSLSHA256::initCrypto();
  }

  void TearDown() override {
    executor.chain.clear();
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
  constexpr int recordCount = 5;
  for (int i=3; i<recordCount; i++) {
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


TEST_F(MyExecutorTest, SyncWriteBatchTest1) {
  constexpr int recordCount = 3;
  for (int i=0; i<recordCount; i++) {
    batch.Put(std::to_string(i), "0");
  }
  executor.execute(batch);
  executor.syncWriteBatch();
  peer::db::PHMapConnection::MyExecutor::Node& lastNode = executor.chain.back();
  ASSERT_FALSE(lastNode.merkleRoot.empty());
}

TEST_F(MyExecutorTest, Prooftest1){
  pmt::Proof proof=executor.fromString("00000010**4a44dc15364204a80fe80e9039455cc1608281820fe2b24f1e5233ade6af1dd5*96a2378a537a5379ca61c1fd65023d593c889cb1417b2812c491f754815ef874*dd1640804928f547325784060380686434a48d9fa90b9786961223423def4016*");
  ASSERT_TRUE(executor.toverify("00",proof).value());
}
TEST_F(MyExecutorTest, Prooftest2){
  std::string str1="00000010**4a44dc15364204a80fe80e9039455cc1608281820fe2b24f1e5233ade6af1dd5*96a2378a537a5379ca61c1fd65023d593c889cb1417b2812c491f754815ef874*dd1640804928f547325784060380686434a48d9fa90b9786961223423def4016*";
  ASSERT_TRUE(executor.iseaqul(str1,"00"));
}
TEST_F(MyExecutorTest, datachange){
  leveldb::DB* merkledb;
  leveldb::Options options;
  options.create_if_missing = false;
  leveldb::Status status
      = leveldb::DB::Open(options, "/home/user/CLionProjects/mass_bft/searchdb", &merkledb);
  if (!status.ok()) {
    LOG(ERROR) << "cant open"<< status.ToString();
  }
  leveldb::WriteOptions writeOptions;
  leveldb::Status put_status = merkledb->Put(writeOptions, "0", "1");
  if (!put_status.ok()) {
    LOG(ERROR) << "cant write";
  }
  delete merkledb;
}

TEST_F(MyExecutorTest, deletedb){
  executor.deleteDB();
  std::string filePath = "/home/user/CLionProjects/mass_bft/searchdb/search.csv";
  std::ofstream ofs(filePath, std::ofstream::out | std::ofstream::trunc);
  if (!ofs) {
    std::cerr << "Error opening file: " << filePath << std::endl;
  }
  ofs.close();
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
