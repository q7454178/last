//
// Created by user on 24-3-31.
//
#include <iostream>
#include <utility>
#include "leveldb/db.h"
#include "peer/db/phmap_connection.h"
#include <fstream>
#include <vector>
namespace peer::db {
  void PHMapConnection::MyExecutor::execute(WriteBatch batch) { lastBatch = std::move(batch); }
  void PHMapConnection::MyExecutor::syncWriteBatch() {
    for (const auto& write : lastBatch.writes) {
      store.insert({write.first, write.second});
      // auto block = std::make_unique<pmt::mockDataBlock>(write.first + write.second);
      // dataBlocks.push_back(std::move(block));
    }
    for (auto& de : lastBatch.deletes) {
      auto it = store.find(de);
      if (it != store.end()) {
        store.erase(it);
      } else {
        continue;
      }
    }
    Node node;
    for (auto& construct : store) {
      auto block = std::make_unique<pmt::mockDataBlock>(construct.first + construct.second);
      node.dataBlocks.push_back(std::move(block));
    }

    config.NumRoutines = 4;
    config.Mode = pmt::ModeType::ModeProofGenAndTreeBuild;
    config.RunInParallel = true;
    config.LeafGenParallel = true;
    node.merkleTree = pmt::MerkleTree::New(config, node.dataBlocks);
    node.merkleRoot = node.merkleTree->getRoot();
    // node.proofs=node.merkleTree->getProofs();
    chain.push_back(std::move(node));
    leveldb::DB* merkledb;
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status
        = leveldb::DB::Open(options, "/home/user/CLionProjects/mass_bft/searchdb", &merkledb);
    if (!status.ok()) {
      LOG(ERROR) << "cant open";
    }
    for (const auto& w : store) {
      std::string key = w.first;
      std::string value = w.second;
      leveldb::Status put_status = merkledb->Put(leveldb::WriteOptions(), key, value);
      if (!put_status.ok()) {
        LOG(ERROR) << "cant write";
      }
    }
    delete merkledb;
    Record re;
    std::vector<Record> reall;
    for(const auto& x:store)
    {
      re.timestamp="1";
      re.key=x.first;
      re.value=x.second;
      reall.push_back(re);
    }
  // updateCSV("/home/user/CLionProjects/mass_bft/searchdb/search.csv");
   //writeToCSV("/home/user/CLionProjects/mass_bft/searchdb/search.csv", reall);

  }

  void PHMapConnection::MyExecutor::deleteDB() {
    leveldb::DB* deletedb;
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status
        = leveldb::DB::Open(options, "/home/user/CLionProjects/mass_bft/searchdb", &deletedb);
    if (!status.ok()) {
      LOG(ERROR) << "cant delete";
    }
    leveldb::Iterator* it = deletedb->NewIterator(leveldb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      deletedb->Delete(leveldb::WriteOptions(), it->key());
    }
    if (!it->status().ok()) {
      LOG(ERROR) << "cant delete";
    }
    delete it;
    delete deletedb;
  }

  PHMapConnection::MyExecutor::returned PHMapConnection::MyExecutor::readDB(std::string key) {
    returned returned;
    leveldb::DB* readdb;
    leveldb::Options options;
    options.create_if_missing = true;
    std::string message;
    leveldb::Status open_status
        = leveldb::DB::Open(options, "/home/user/CLionProjects/mass_bft/searchdb", &readdb);
    leveldb::Status get_status = readdb->Get(leveldb::ReadOptions(), key, &message);
    if (get_status.ok()) {
      if (!chain.empty()) {
        returned.message = message;
        std::unique_ptr<pmt::mockDataBlock> blockPtr
            = std::make_unique<pmt::mockDataBlock>(key + message);
        const pmt::DataBlock& dataBlock = *blockPtr;
        Node& lastNode = chain.back();
        auto proofCheck = lastNode.merkleTree->GenerateProof(dataBlock);
        if (proofCheck.has_value()) {
          const pmt::Proof& proof = *proofCheck;
          returned.istrue = lastNode.merkleTree->Verify(dataBlock, proof, lastNode.merkleRoot);
        } else {
          returned.message = message;
          returned.istrue = false;
        }
      } else {
        returned.message = "this list is null";
        returned.istrue = false;
      }
      delete readdb;
      return returned;
    } else {
      delete readdb;
      returned.message = "readDB failed";
      returned.istrue = false;
      return returned;
    }
  }

  void PHMapConnection::MyExecutor::writeToCSV(const std::string& filename, std::vector<Record>& data) {
    std::ofstream file(filename, std::ios::app);
    if (!file.is_open()) {
      std::cerr << "Error opening file " << filename << std::endl;
      return;
    }

    file.seekp(0, std::ios::end);
    bool isEmpty = file.tellp() == 0;
    file.seekp(0, std::ios::beg);

    // 如果文件为空，写入表头
    if (isEmpty) {
      file << "timestamp,key,value,is_latest" << std::endl;
    }

    // 设置本次传入记录的最新标志为 true，并写入 CSV 文件
    for (auto& record : data) {
      record.isLatest = true;
      // 写入记录到 CSV 文件
      file << record.timestamp << "," << record.key << "," << record.value << "," << (record.isLatest ? "1" : "0") << std::endl;
    }

    file.close();
  }

  void PHMapConnection::MyExecutor::updateCSV(const std::string& filename) {
    std::ifstream file(filename);
    if (!file.is_open()) {
      std::cerr << "Error opening file " << filename << std::endl;
      return;
    }

    // 检查文件是否为空
//    if (file.peek() == std::ifstream::traits_type::eof()) {
//      std::cout << "CSV file is empty." << std::endl;
//      file.close();
//      return;
//    }
    file.seekg(0, std::ios::end);
    std::streampos fileSize = file.tellg();

    if (fileSize == 0) {
      std::cout << "File is empty." << std::endl;
      return;
    }
    else  file.seekg(0, std::ios::beg);

    // 读取 CSV 文件中的数据并更新最新标志
    std::vector<std::string> lines;
    std::string line;
    std::vector<Record> data;
    std::getline(file, line);
    while (std::getline(file, line)) {
      lines.push_back(line);
      Record record;
      std::istringstream iss(line);
      std::getline(iss, record.timestamp, ',');
      std::getline(iss, record.key, ',');
      std::getline(iss, record.value, ',');
      record.isLatest = false;  // 将之前数据的最新标志设为 false
      data.push_back(record);
    }

    file.close();

    // 更新 CSV 文件内容，将之前数据的最新标志设为 false
    std::ofstream outFile(filename);
    if (!outFile.is_open()) {
      std::cerr << "Error opening file " << filename << std::endl;
      return;
    }

    outFile << "timestamp,key,value,is_latest" << std::endl;

    // 写入更新后的数据到 CSV 文件
    for (const auto& record : data) {
      outFile << record.timestamp << "," << record.key << "," << record.value << "," << (record.isLatest ? "1" : "0") << std::endl;
    }
    outFile.close();
  }
}