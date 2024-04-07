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
  }
}
