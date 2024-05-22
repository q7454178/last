//
// Created by user on 24-5-22.
//
#include <leveldb/db.h>
#include "peer/db/phmap_connection.h"
#include "gtest/gtest.h"
#include <fstream>
#include "glog/logging.h"
#include <chrono>
#include <optional>
using namespace peer::db;
class SearchTest : public ::testing::Test {
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
  std::vector<std::tuple<std::string, std::string, std::string>> readCSV(const std::string& filename) {
    std::vector<std::tuple<std::string, std::string, std::string>> data;
    std::ifstream file(filename);
    std::string line;

    while (std::getline(file, line)) {
      std::stringstream lineStream(line);
      std::string proof, key, value;

      std::getline(lineStream, proof, ',');
      std::getline(lineStream, key, ',');
      std::getline(lineStream, value, ',');

      data.emplace_back(proof, key, value);
    }

    return data;
  }
};

TEST_F(SearchTest, TestSyncWriteBatchPerformance) {
  executor.deleteDB();
  // 创建数据库连接
  auto dbConnection = peer::db::PHMapConnection::NewConnection("testDB");
  ASSERT_TRUE(dbConnection != nullptr) << "创建数据库连接失败!";

  // 定义批处理回调函数
  auto batchCallback = [](peer::db::PHMapConnection::WriteBatch* batch) {
    // 填充测试数据
    for (int i = 0; i < 10; ++i) {
      batch->Put("key" + std::to_string(i), "value" + std::to_string(i));
    }
    return true;
  };
  auto start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < 100; ++i) {
    bool result = dbConnection->syncWriteBatch(batchCallback);
    ASSERT_TRUE(result) << "syncWriteBatch 在第 " << i + 1 << " 次调用时失败!";
  }
  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  LOG(INFO) << "运行 100 次 syncWriteBatch 的总耗时: " << duration.count() << " 毫秒";
}
TEST_F(SearchTest, ReadPerformanceAndAccuracy) {
  leveldb::DB* db = nullptr;
  leveldb::Options options;
  options.create_if_missing = false;
  leveldb::Status status1 = leveldb::DB::Open(options, "/home/user/CLionProjects/mass_bft/searchdb", &db);
  ASSERT_TRUE(status1.ok()) << "Unable to open LevelDB database: " << status1.ToString();
  std::vector<std::pair<std::string, std::string>> expected_data = {
      {"0", "0"},
      {"1", "0"},
      {"2", "0"}
  };

  constexpr int num_iterations = 100;
  auto total_start = std::chrono::high_resolution_clock::now();

  for (int i = 0; i < num_iterations; ++i) {
    for (const auto& [key, expected_value] : expected_data) {
      std::string value;
      leveldb::Status status = db->Get(leveldb::ReadOptions(), key, &value);
      ASSERT_TRUE(status.ok()) << "Error reading key " << key << ": " << status.ToString();
      ASSERT_EQ(value, expected_value) << "Value mismatch for key " << key;
    }
  }

  auto total_end = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double, std::milli> total_duration = total_end - total_start;

  LOG(INFO) << "运行 " << num_iterations << "次查询的总耗时: " << total_duration.count() << " ms" << std::endl;
}
TEST_F(SearchTest, VerifyPerformance) {
  // 从CSV文件中读取第一行数据
  auto data = readCSV("/home/user/CLionProjects/mass_bft/searchdb/search.csv");
  ASSERT_FALSE(data.empty()) << "CSV file is empty or not found.";

  auto [proofStr, key, value] = data.front();
  auto proof = executor.fromString(proofStr);

  constexpr int num_iterations = 100;
  auto total_start = std::chrono::high_resolution_clock::now();

  for (int i = 0; i < num_iterations; ++i) {
    auto verifyResult = executor.toverify(key + value, proof);
    ASSERT_TRUE(verifyResult.value()) << "Verification failed.";
    ASSERT_TRUE(*verifyResult) << "Verification result is false.";
  }

  auto total_end = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double, std::milli> total_duration = total_end - total_start;

  LOG(INFO) << "运行 " << num_iterations << " 次验证的总耗时: " << total_duration.count() << " ms" << std::endl;
}