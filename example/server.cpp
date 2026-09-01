// ============================================================================
// server（发 JSON 客户端）— 向主程序发送一份 pipeline 配置（POST /update）
// ============================================================================
//
// 内部逻辑：
//   读本地 JSON 文件 → POST /update 一次 → 退出。主程序（client）的 /update handler
//   存 JSON 到 cache 缓冲份 + 置 pending → 主线程调度循环图静止时 commit + parse +
//   check_topology + expand_hp 重建运行图（HTTP 恒 200，不等待建图结果）。单次发一份，
//   测试流程：启动 client → server 发一份 → 停止 client。
//
// 资源消耗：
//   1 次 HTTP POST（JSON 整体进内存）；单线程，无常驻线程池。
//
// 对外接口：
//   server               — 无参：发写死的 DEFAULT_CFG（换用例改下面常量）
//   server <cfg.json> [rpc_port=18080]  — 可选覆盖：路径 / 端口
// ============================================================================

#include "third_party/httplib.h"
#include <cstdio>
#include <fstream>
#include <sstream>
#include <string>
#include <unistd.h>

static constexpr int DEFAULT_PORT = 18080;

// 无参默认发的用例（相对仓库根 pipeline/）；换用例改这里一行。
static const char *DEFAULT_CFG = "pipeline/cfg.json";

/// 读本地 JSON 文件 → POST /update，打印 HTTP 状态码 + 响应体。失败返回 -1。
static int post_json(httplib::Client &cli, const char *path, int port) {
  std::ifstream ifs(path);
  if (!ifs) {
    std::printf("[server] cannot open %s\n", path);
    return -1;
  }
  std::stringstream ss;
  ss << ifs.rdbuf();
  auto res = cli.Post("/update", ss.str(), "application/json");
  if (res) {
    std::printf("[server] POST %s → HTTP %d: %s\n", path, res->status, res->body.c_str());
    return 0;
  }
  std::printf("[server] connection to 127.0.0.1:%d failed\n", port);
  return -1;
}

int main(int argc, char **argv) {
  std::setvbuf(stdout, nullptr, _IOLBF, 0);  // 行缓冲，重定向到文件/管道时日志不丢
  const std::string path = argc > 1 ? std::string(argv[1]) : DEFAULT_CFG;
  const int port = argc > 2 ? std::atoi(argv[2]) : DEFAULT_PORT;
  httplib::Client cli("127.0.0.1", port);
  return post_json(cli, path.c_str(), port) == 0 ? 0 : 1;
}
