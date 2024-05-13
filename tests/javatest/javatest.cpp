#include <iostream>
#include <string>
using namespace std;
int main(int argc, char* argv[]) {
  if (argc < 3) {
    std::cerr << "参数不足，需要传递两个参数" << std::endl;
    return 1;
  }

  std::string keyAndValue = argv[1];
  std::string proof = argv[2];

  // 处理传递过来的参数
  std::cout << "接收到的参数 keyAndValue: " << keyAndValue << std::endl;
  std::cout << "接收到的参数 proof: " << proof << std::endl;

  // 在这里可以继续处理传递过来的参数，执行相应的操作

  return 0;
}