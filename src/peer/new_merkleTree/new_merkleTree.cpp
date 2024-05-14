//
// Created by user on 24-3-31.
//
#include <bitset>
#include <fstream>
#include <iostream>
#include <utility>
#include <vector>

#include "leveldb/db.h"
#include "peer/db/phmap_connection.h"
namespace peer::db {
  void PHMapConnection::MyExecutor::execute(WriteBatch batch) {
    lastBatch.deletes= std::move(batch.deletes);
    lastBatch.writes= std::move(batch.writes);
//        lastBatch = std::move(batch);
//    int a=1;
    return;
  }
  void PHMapConnection::MyExecutor::syncWriteBatch() {
    store.clear();
    for (const auto& write : lastBatch.writes) {
      store.insert({write.first, write.second});
      // auto block = std::make_unique<pmt::mockDataBlock>(write.first + write.second);
      // dataBlocks.push_back(std::move(block));
    }
    if(!putdb())
      std::cout<<"store error";
    if(!getstore())
      std::cout<<"get store error";
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
   chain.push_back(std::move(node));
   if(!putcsv())
     std::cout<<"put to csv error";
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
      file << "proof,key,value,is_latest" << std::endl;
    }

    // 设置本次传入记录的最新标志为 true，并写入 CSV 文件
    for (auto& record : data) {
      record.isLatest = true;
      // 写入记录到 CSV 文件
      file << record.proof << "," << record.key << "," << record.value << "," << (record.isLatest ? "1" : "0") << std::endl;
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
      std::getline(iss, record.proof, ',');
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

    outFile << "proof,key,value,is_latest" << std::endl;

    // 写入更新后的数据到 CSV 文件
    for (const auto& record : data) {
      outFile << record.proof << "," << record.key << "," << record.value << "," << (record.isLatest ? "1" : "0") << std::endl;
    }
    outFile.close();
  }

  bool PHMapConnection::MyExecutor::getstore() {
    std::string dbPath = "/home/user/CLionProjects/mass_bft/searchdb";

    // 打开 LevelDB 数据库
    leveldb::DB* storedb;
    leveldb::Options options;
    options.create_if_missing = false; // 如果数据库不存在是否创建新的
    leveldb::Status status = leveldb::DB::Open(options, dbPath, &storedb);

    if (!status.ok()) {
      std::cerr << "Error opening database: " << status.ToString() << std::endl;
      return false;
    }
    leveldb::Iterator* it = storedb->NewIterator(leveldb::ReadOptions());
    store.clear();
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      store[it->key().ToString()] = it->value().ToString();
    }
    delete it;
    delete storedb;
    return true;
  }
  bool PHMapConnection::MyExecutor::putdb() {
    leveldb::DB* merkledb;
    leveldb::Options options;
    options.create_if_missing = false;
    leveldb::Status status
        = leveldb::DB::Open(options, "/home/user/CLionProjects/mass_bft/searchdb", &merkledb);
    if (!status.ok()) {
      LOG(ERROR) << "cant open"<< status.ToString();
      return false;
    }
    for (const auto& w : store) {
      leveldb::WriteOptions writeOptions;
//      writeOptions.sync = true;
      std::string key = w.first;
      std::string value = w.second;
      leveldb::Status put_status = merkledb->Put(writeOptions, key, value);
      if (!put_status.ok()) {
        LOG(ERROR) << "cant write";
        return false;
      }
    }
    delete merkledb;
    return true;
  }
  bool PHMapConnection::MyExecutor::putcsv() {
    Record re;
    std::vector<Record> reall;
    Node& lastNode = chain.back();
    for(const auto& x:store)
    {
      std::unique_ptr<pmt::mockDataBlock> blockPtr
          = std::make_unique<pmt::mockDataBlock>(x.first + x.second);
      pmt::DataBlock& dataBlock = *blockPtr;
      std::string str=lastNode.merkleTree->GenerateProof(dataBlock)->toString();
      re.proof=str;
      re.key=x.first;
      re.value=x.second;
      reall.push_back(re);
    }
    updateCSV("/home/user/CLionProjects/mass_bft/searchdb/search.csv");
    writeToCSV("/home/user/CLionProjects/mass_bft/searchdb/search.csv", reall);
    return true;
  }
  pmt::Proof PHMapConnection::MyExecutor::fromString(const std::string& str){
    pmt::Proof proof;
    std::string pathStr = str.substr(0, 8);
    std::bitset<8> path(std::stoul(str.substr(0, 8), nullptr, 2));
    std::bitset<32> extended(path.to_ulong());
    uint32_t uintNum = static_cast<uint32_t>(extended.to_ulong());
    std::stringstream ss(str.substr(9)); // Start after the '|'
    std::string item;
    std::vector<const pmt::HashString*> Siblings{};
    while (std::getline(ss, item, '*')) {
      if (!item.empty()) {
        std::array<uint8_t, 32>* k=new std::array<uint8_t, 32>();

        std::string reversedData = OpenSSL::stringToBytes(item);
        std::copy_n(reversedData.begin(),32,k->begin());
        Siblings.push_back(k);
      }
    }
    proof.Siblings=Siblings;
    proof.Path=uintNum;
    return proof;
  }
  std::vector<std::string> PHMapConnection::MyExecutor::splitString(const std::string& str, char delim) {
    std::vector<std::string> tokens;
    std::stringstream ss(str);
    std::string token;
    while (std::getline(ss, token, delim)) {
      tokens.push_back(token);
    }
    return tokens;
  }
  std::optional<bool> PHMapConnection::MyExecutor::toverify(std::string str,pmt::Proof proof) {
    std::string dbPath = "/home/user/CLionProjects/mass_bft/searchdb";

    // 打开 LevelDB 数据库
    leveldb::DB* storedb;
    leveldb::Options options;
    options.create_if_missing = false; // 如果数据库不存在是否创建新的
    leveldb::Status status = leveldb::DB::Open(options, dbPath, &storedb);

    if (!status.ok()) {
      std::cerr << "Error opening database: " << status.ToString() << std::endl;
      return false;
    }
    leveldb::Iterator* it = storedb->NewIterator(leveldb::ReadOptions());
    store.clear();
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      store[it->key().ToString()] = it->value().ToString();
    }
    delete it;
    delete storedb;
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
    node.merkleRoot=node.merkleTree->getRoot();
    std::unique_ptr<pmt::mockDataBlock> blockPtr
        = std::make_unique<pmt::mockDataBlock>(str);
    const pmt::DataBlock& dataBlock = *blockPtr;
    return node.merkleTree->Verify(dataBlock,proof,node.merkleRoot);
  }
  bool PHMapConnection::MyExecutor::iseaqul(std::string str1,std::string str2) {
    pmt::Proof proof1,proof2;
    std::string dbPath = "/home/user/CLionProjects/mass_bft/searchdb";

    // 打开 LevelDB 数据库
    leveldb::DB* storedb;
    leveldb::Options options;
    options.create_if_missing = false; // 如果数据库不存在是否创建新的
    leveldb::Status status = leveldb::DB::Open(options, dbPath, &storedb);

    if (!status.ok()) {
      std::cerr << "Error opening database: " << status.ToString() << std::endl;
      return false;
    }
    leveldb::Iterator* it = storedb->NewIterator(leveldb::ReadOptions());
    store.clear();
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      store[it->key().ToString()] = it->value().ToString();
    }
    delete it;
    delete storedb;
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
    node.merkleRoot=node.merkleTree->getRoot();
    std::unique_ptr<pmt::mockDataBlock> blockPtr
        = std::make_unique<pmt::mockDataBlock>(str2);
    const pmt::DataBlock& dataBlock = *blockPtr;
    proof2=node.merkleTree->GenerateProof(dataBlock).value();
    proof1= fromString(str1);
    return proof1.equal(proof2);
  }
  }