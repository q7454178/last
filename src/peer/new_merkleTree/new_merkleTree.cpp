//
// Created by user on 24-3-31.
//
#include <iostream>
#include <utility>
#include "leveldb/db.h"
#include "peer/db/phmap_connection.h"
namespace peer::db {
  void PHMapConnection::MyExecutor::execute(WriteBatch batch){
    lastBatch=std::move(batch);
 }
  void PHMapConnection::MyExecutor::syncWriteBatch() {
    for (const auto& write : lastBatch.writes) {
      store.insert({write.first,write.second});
      //auto block = std::make_unique<pmt::mockDataBlock>(write.first + write.second);
      //dataBlocks.push_back(std::move(block));
    }
    for (auto& de: lastBatch.deletes) {
      auto it = store.find(de);
      if (it != store.end()) {
        store.erase(it);
      } else {
        continue;
      }
    }
    Node node;
    for(auto& construct :store)
    {
      auto block = std::make_unique<pmt::mockDataBlock>(construct.first + construct.second);
      node.dataBlocks.push_back(std::move(block));
    }

    config.NumRoutines = 4;
    config.Mode = pmt::ModeType::ModeProofGenAndTreeBuild;
    config.RunInParallel = true;
    config.LeafGenParallel = true;
    node.merkleTree = pmt::MerkleTree::New(config, node.dataBlocks);
    node.merkleRoot = node.merkleTree->getRoot();
    node.proofs=node.merkleTree->getProofs();
    chain.push_back(std::move(node));
    leveldb::DB* merkledb;
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(options, "/home/user/CLionProjects/mass_bft/searchdb", &merkledb);
    if (!status.ok()) {
      LOG(ERROR) << "cant open";
    }
    for (const auto& w : store) {
      std::string key =  w.first;
      std::string value = w.second;
      leveldb::Status put_status = merkledb->Put(leveldb::WriteOptions(), key, value);
      if (!put_status.ok()) {
        LOG(ERROR) << "cant write";
      }
    }
    delete merkledb;
  }

  void PHMapConnection::MyExecutor::deleteDB() {
    leveldb::DB* deletedb;
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(options, "/home/user/CLionProjects/mass_bft/searchdb", &deletedb);
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

  std::string PHMapConnection::MyExecutor::readDB(std::string key) {
    leveldb::DB* readdb;
    leveldb::Options options;
    options.create_if_missing = true;
    std::string message;
    leveldb::Status open_status = leveldb::DB::Open(options, "/home/user/CLionProjects/mass_bft/searchdb", &readdb);
    leveldb::Status get_status = readdb->Get(leveldb::ReadOptions(), key, &message);

    if (get_status.ok()) {
      delete readdb;
      return  message;
    }
    else {
      delete readdb;
      return "cant find";
    }
  }

  std::optional<bool> PHMapConnection::MyExecutor::Verify(const pmt::DataBlock& dataBlock, const pmt::Proof& proof) {
    if (!chain.empty()) {
      Node& lastNode = chain.back();
      return lastNode.merkleTree->Verify(dataBlock,proof,lastNode.merkleRoot);
    } else {
      std::cout << "The list is empty." << std::endl;
    }
  }
}
