#include <iostream>
#include <string>
#include <vector>
#include <memory>
#include <cassert>

#include "algo/algo_func.hpp"

///https://chatgpt.com/share/6a57b6e9-a9cc-83ea-ace0-c8e37079da86

// 定义一些复杂的算法输入输出结构体
struct ImageFrame {
  int width = 0;
  int height = 0;
  std::vector<uint8_t> data;
};

struct HyperParams {
  double threshold = 0.5;
  int max_iters = 100;
  std::string model_path = "/models/algo.onnx";
};

// 满足 nlohmann::json 反序列化契约的宏
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(HyperParams, threshold, max_iters, model_path)

struct DetectionResult {
  int box_count = 0;
  bool is_valid = false;
};

// 🌟 用户的真实算法函数 (2个输入，1个配置，1个输出)
void my_sample_algorithm(const ImageFrame& img, const int& camera_id,
                         const HyperParams& config, DetectionResult& out_res) {
  // 模拟算法内部处理
  out_res.is_valid = (img.width > 0 && camera_id == 1);
  out_res.box_count = config.max_iters / 2;
}

int main() {
  std::cout << "========================================================\n";
  std::cout << "         FINS ALGO ARCHITECTURE BENCHMARK TEST          \n";
  std::cout << "========================================================\n\n";

  constexpr int ITERATIONS = 100000;

  // -----------------------------------------------------------------
  // TEST 1: 测试对象的构建用时 (Object Construction Time)
  // -----------------------------------------------------------------
  auto start_build = std::chrono::high_resolution_clock::now();

  // 利用自动类型推导创建包装对象
  auto algo = std::make_unique<fins::rt::AlgoFunc<decltype(&my_sample_algorithm)>>(&my_sample_algorithm);

  // 绑定端口映射规则
  algo->update_input_ports({"input_image", "camera_id"});
  algo->update_config_ports({"algorithm_config"});
  algo->update_output_ports({"det_output"});

  auto end_build = std::chrono::high_resolution_clock::now();
  auto build_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_build - start_build).count();

  std::cout << "[Test 1] Object Construction & Port Binding Time:\n";
  std::cout << "         -> " << build_ns << " ns (" << (double)build_ns / 1000.0 << " us)\n";
  std::cout << "         (提示：纯虚函数和包装器指针绑定非常轻量，耗时几乎全部在 Vector 字符串内存分配上)\n\n";

  // -----------------------------------------------------------------
  // TEST 2: 测试 JSON 的配置转译更新耗时 (Configuration Translation Time)
  // -----------------------------------------------------------------
  nlohmann::json sample_json = {
      {"threshold", 0.75},
      {"max_iters", 250},
      {"model_path", "/opt/models/v2/detect.engine"}
  };

  auto start_cfg = std::chrono::high_resolution_clock::now();
  for(int i = 0; i < ITERATIONS; ++i) {
      algo->configure("algorithm_config", sample_json);
  }
  auto end_cfg = std::chrono::high_resolution_clock::now();
  auto cfg_us = std::chrono::duration_cast<std::chrono::microseconds>(end_cfg - start_cfg).count();

  std::cout << "[Test 2] JSON Configuration Injection (" << ITERATIONS << " runs):\n";
  std::cout << "         -> Total: " << cfg_us << " us\n";
  std::cout << "         -> Avg per update: " << (double)cfg_us / ITERATIONS << " us\n\n";

  // -----------------------------------------------------------------
  // TEST 3: 测试算法执行流水线的解包与投递耗时 (Pipeline Execution Time)
  // -----------------------------------------------------------------
  // 准备数据 Bundle
  fins::rt::MsgBundle inputs;
  auto img = ImageFrame();
  img.width = 1920;
  img.height = 1080;
  img.data.resize(100);
  *(inputs["input_image"].pub<ImageFrame>()) = img;
  *(inputs["camera_id"].pub<int>()) = 1;

  fins::rt::MsgBundle outputs;

  // 预热一次开辟输出缓冲区
  algo->execute(inputs, outputs);

  auto start_exe = std::chrono::high_resolution_clock::now();
  for(int i = 0; i < ITERATIONS; ++i) {
      outputs.clear();
      algo->execute(inputs, outputs);
  }
  auto end_exe = std::chrono::high_resolution_clock::now();
  auto exe_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_exe - start_exe).count();

  std::cout << "[Test 3] Pipeline Unpacking & Execution (" << ITERATIONS << " runs):\n";
  std::cout << "         -> Total: " << (double)exe_ns / 1000000.0 << " ms\n";
  std::cout << "         -> Avg per execute: " << (double)exe_ns / ITERATIONS << " ns\n";

  // 验证结果是否正确送达
  auto final_res = outputs["det_output"].sub<DetectionResult>();
  std::cout << "         -> Verification [is_valid: " << std::boolalpha << final_res->is_valid
            << ", boxes: " << final_res->box_count << "]\n\n";

  // -----------------------------------------------------------------
  // TEST 4: 原生直接调用基准测试与传参开销分析 (Baseline & Overhead Analysis)
  // -----------------------------------------------------------------
  ImageFrame raw_img;
  raw_img.width = 1920;
  raw_img.height = 1080;
  raw_img.data.resize(100);
  int raw_camera_id = 1;
  HyperParams raw_config;
  raw_config.threshold = 0.75;
  raw_config.max_iters = 250;
  raw_config.model_path = "/opt/models/v2/detect.engine";
  DetectionResult raw_out;

  auto start_raw = std::chrono::high_resolution_clock::now();
  for(int i = 0; i < ITERATIONS; ++i) {
    // 纯 C++ 原生函数调用，无任何框架开销
    my_sample_algorithm(raw_img, raw_camera_id, raw_config, raw_out);
  }
  auto end_raw = std::chrono::high_resolution_clock::now();
  auto raw_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_raw - start_raw).count();

  std::cout << "[Test 4] Native Direct Function Call (" << ITERATIONS << " runs):\n";
  std::cout << "         -> Total: " << (double)raw_ns / 1000000.0 << " ms\n";
  std::cout << "         -> Avg per execute: " << (double)raw_ns / ITERATIONS << " ns\n\n";

  // --- 计算框架传参开销 ---
  // 框架的总执行耗时 = 纯算法耗时 + 框架解包/打包传参开销
  double avg_exe_ns = (double)exe_ns / ITERATIONS;
  double avg_raw_ns = (double)raw_ns / ITERATIONS;
  double overhead_ns = avg_exe_ns - avg_raw_ns;

  std::cout << "[Analysis] Framework Parameter Passing Overhead:\n";
  std::cout << "         -> Framework Execution : " << avg_exe_ns << " ns\n";
  std::cout << "         -> Native Execution    : " << avg_raw_ns << " ns\n";
  std::cout << "         -> Net Passing Overhead: " << overhead_ns << " ns per call\n";
  std::cout << "         (注: 包含 map 查找、RttI 校验、tuple 展开以及 AnyMsg 解包的综合耗时)\n\n";

  // -----------------------------------------------------------------
  // TEST 5: 节点间图路由与智能指针传递开销 (Node-to-Node Transfer Overhead)
  // -----------------------------------------------------------------
  fins::rt::MsgBundle node_a_outputs;
  // 模拟 Node A 产生了一个输出
  *(node_a_outputs["det_output"].pub<DetectionResult>()) = {125, true};

  fins::rt::MsgBundle node_b_inputs;

  auto start_transfer = std::chrono::high_resolution_clock::now();
  for(int i = 0; i < ITERATIONS; ++i) {
    // 模拟框架调度器：将 Node A 的输出传递给 Node B 的输入
    // 这里触发了 std::map 的插入/查找，以及 std::shared_ptr 引用计数的原子加减
    node_b_inputs["upstream_result"] = node_a_outputs["det_output"];
  }
  auto end_transfer = std::chrono::high_resolution_clock::now();
  auto transfer_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_transfer - start_transfer).count();

  std::cout << "[Test 5] Node-to-Node Data Transfer (" << ITERATIONS << " runs):\n";
  std::cout << "         -> Total: " << (double)transfer_ns / 1000000.0 << " ms\n";
  std::cout << "         -> Avg per transfer: " << (double)transfer_ns / ITERATIONS << " ns\n";
  std::cout << "         (注: 包含 Map 路由赋值与 std::shared_ptr 原子拷贝开销)\n\n";

  // -----------------------------------------------------------------
  // TEST 6: 原生赋值耗时对比 (Native Assignment Benchmark)
  // -----------------------------------------------------------------
  // 1. 正常的结构体值拷贝
  DetectionResult raw_src{125, true};
  DetectionResult raw_dst;

  auto start_val = std::chrono::high_resolution_clock::now();
  for(int i = 0; i < ITERATIONS; ++i) {
    raw_dst = raw_src; // 纯内存值拷贝 (8字节)
  }
  auto end_val = std::chrono::high_resolution_clock::now();
  auto val_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_val - start_val).count();

  // 2. 原生 std::shared_ptr 拷贝 (包含原子引用计数增减)
  auto ptr_src = std::make_shared<DetectionResult>(DetectionResult{125, true});
  std::shared_ptr<DetectionResult> ptr_dst;

  auto start_ptr = std::chrono::high_resolution_clock::now();
  for(int i = 0; i < ITERATIONS; ++i) {
    ptr_dst = ptr_src; // 原子操作锁总线开销
  }
  auto end_ptr = std::chrono::high_resolution_clock::now();
  auto ptr_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_ptr - start_ptr).count();

  std::cout << "[Test 6] Native Assignment Comparison (" << ITERATIONS << " runs):\n";
  std::cout << "         -> Raw Value Copy Avg     : " << (double)val_ns / ITERATIONS << " ns\n";
  std::cout << "         -> Raw std::shared_ptr Avg: " << (double)ptr_ns / ITERATIONS << " ns\n";
  std::cout << "         (对比 Test 5: " << (double)transfer_ns / ITERATIONS << " ns)\n";
  std::cout << "========================================================\n";

  return 0;
}