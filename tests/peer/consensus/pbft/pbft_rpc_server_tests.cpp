//
// Created by user on 23-3-21.
//

#include "gtest/gtest.h"
#include "glog/logging.h"
#include "peer/consensus/pbft/pbft_rpc_server.h"
#include "tests/proto_block_utils.h"


class MockPBFTStateMachine : public peer::consensus::PBFTStateMachine {
public:
    [[nodiscard]] std::optional<::util::OpenSSLED25519::digestType> OnSignMessage(const ::util::NodeConfigPtr&, const std::string&) const override {
        return std::nullopt;
    }
    // Call by followers only
    bool OnVerifyProposal(::util::NodeConfigPtr, const std::string&) override {
        return true;
    }

    bool OnDeliver(::util::NodeConfigPtr,
                   const std::string&,
                   std::vector<::proto::SignatureString>) override {
        return true;
    }

    void OnLeaderStart(::util::NodeConfigPtr, int) override {
    }

    void OnLeaderStop(::util::NodeConfigPtr, int) override {
    }

    // Call by the leader only
    std::optional<std::string> OnRequestProposal(::util::NodeConfigPtr, int, const std::string&) override {
        auto block = tests::ProtoBlockUtils::CreateDemoBlock();
        block->header.number = nextBlockNumber;
        nextBlockNumber++;
        std::string buf;
        block->header.serializeToString(&buf);
        return buf;
    }

private:
    int nextBlockNumber = 0;
};


class PBFTRPCServerTest : public ::testing::Test {
protected:
    void SetUp() override {
        init();
    };

    void TearDown() override {
    };

    void init() {
        auto ms = std::make_unique<util::DefaultKeyStorage>();
        bccsp = std::make_shared<util::BCCSP>(std::move(ms));
        for (int i=0; i<4; i++) {
            util::NodeConfigPtr cfg(new util::NodeConfig);
            cfg->nodeId = i;
            cfg->groupId = 0;
            cfg->ski = "0_" + std::to_string(i);
            cfg->ip = "127.0.0.1";
            auto key = bccsp->generateED25519Key(cfg->ski, false);
            CHECK(key != nullptr);
            localNodes[i] = std::move(cfg);
        }
        stateMachine = std::make_shared<MockPBFTStateMachine>();
    }

    std::unordered_map<int, ::util::NodeConfigPtr> localNodes;
    std::shared_ptr<util::BCCSP> bccsp;
    std::shared_ptr<MockPBFTStateMachine> stateMachine;
};

TEST_F(PBFTRPCServerTest, TestStartServer) {
    util::OpenSSLED25519::initCrypto();
    auto service = std::make_unique<peer::consensus::PBFTRPCService>();
    CHECK(service->checkAndStart(localNodes, bccsp, stateMachine));
    util::MetaRpcServer::Start();
    sleep(3600);
}