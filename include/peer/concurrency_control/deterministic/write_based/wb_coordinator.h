//
// Created by user on 23-3-7.
//

#pragma once

#include "peer/concurrency_control/deterministic/coordinator.h"
#include "peer/concurrency_control/deterministic/write_based/wb_worker_fsm.h"

namespace peer::cc {
    class WBCoordinator : public Coordinator<WBWorkerFSM, WBReserveTable, WBCoordinator> {
    public:
        // NOT thread safe
        // processTxnList will take the transactions and move them back after execution
        bool processTxnList(std::vector<std::unique_ptr<proto::Transaction>>& txnList) {
            int totalWorkerCount = (int)workerList.size();
            reserveTable->reset();
            // prepare txn function
            auto afterStart = [&](const Worker& worker, WBWorkerFSM& fsm) {
                auto& fsmTxnList = fsm.getMutableTxnList();
                fsmTxnList.clear();
                auto id = worker.getId();
                for (int i = id; i < (int)txnList.size(); i += totalWorkerCount) {
                    fsmTxnList.push_back(std::move(txnList[i]));
                }
            };
            auto ret = processParallel(InvokerCommand::START, ReceiverState::READY, afterStart);
            if (!ret) {
                LOG(ERROR) << "init txnList failed!";
                return false;
            }
            ret = processParallel(InvokerCommand::EXEC, ReceiverState::FINISH_EXEC, nullptr);
            if (!ret) {
                LOG(ERROR) << "exec txnList failed!";
                return false;
            }
            // First commit, WBCoordinator add this function
            ret = processParallel(InvokerCommand::COMMIT, ReceiverState::FINISH_COMMIT, nullptr);
            if (!ret) {
                LOG(ERROR) << "first commit failed!";
                return false;
            }
            // move back
            auto afterCommit = [&](const Worker& worker, WBWorkerFSM& fsm) {
                auto& fsmTxnList = fsm.getMutableTxnList();
                auto id = worker.getId();
                for (int i = id, j = 0; i < (int)txnList.size(); i += totalWorkerCount) {
                    txnList[i] = std::move(fsmTxnList[j++]);
                }
            };
            // Second commit
            ret = processParallel(InvokerCommand::COMMIT, ReceiverState::FINISH_COMMIT, afterCommit);
            if (!ret) {
                LOG(ERROR) << "second commit failed!";
                return false;
            }
            return true;
        }

        friend class Coordinator;

    protected:
        WBCoordinator() = default;

    private:

    };
}