#include <iostream>
#include <string>
#include "peer/db/phmap_connection.h"
#include "gtest/gtest.h"
#include "common/parallel_merkle_tree.h"
using namespace std;
void SetUp()  {
  util::OpenSSLSHA1::initCrypto();
  util::OpenSSLSHA256::initCrypto();
}
void TearDown()  {

}
int main(int argc, char* argv[]) {
  SetUp();
  try {
    if (argc < 3) {
      std::cout << "参数不足，需要传递两个参数" << std::endl;
      return 1; // 返回错误码表示参数不足
    }

    std::string keyAndValue = argv[1];
    std::string proof = argv[2];
    peer::db::PHMapConnection::MyExecutor executor;
    pmt::Proof proofs = executor.fromString(proof);
    if(executor.toverify(keyAndValue,proofs).value())
      cout<<"its reliable";
    else
      cout<<"its unreliable";
    return 0; // 返回正常退出码
  } catch (const std::exception& e) {
    // 捕获异常并输出错误信息到标准错误流
    std::cerr << "发生异常: " << e.what() << std::endl;
    return 2; // 返回错误码表示发生异常
  }

}