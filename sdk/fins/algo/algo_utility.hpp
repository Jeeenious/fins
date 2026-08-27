/*******************************************************************************
 * algo_utility.hpp — 内置工具算法（数据滞留二件套）
 *
 * 内部逻辑：
 *   框架提供的辅助算法（AlgoBase 派生），pipeline 配置直接写 name 即可实例化
 *   （见 g_state expand_hp ④ 的内置注册表），无需编译插件。两个类分别承担不同的
 *   数据滞留语义，供用户在 producer 与 consumer 之间显式接线：
 *     ① DelayAlgo 纯延迟——函数体透传 + 阻塞延迟：execute 前按 offset_（ms，缺省 0）
 *       std::this_thread::sleep_for 真延迟（占 worker）；offset **非 JSON 配置**——由后续
 *       超周期展开/调度阶段分析经 configure("offset") 注入（本阶段无装配点注入 →
 *       恒 0 纯透传）；offset 不参与图相位（就绪只看前序边）。透传按位置 input[i] →
 *       output[i]（execute 收端口序数组，不做端口名查找——端口对应关系由图侧按端口序打包）。
 *     ② RingBufferAlgo 存环形队列——深度 depth_ 滚动覆盖最旧；"取一个时间段" =
 *       按 back_（0=最新 .. depth_-1=最旧）输出窗内对应帧；depth_=1 即 latest-value
 *       （暂存最近一帧、输出最新帧——原 StagingAlgo 的暂存语义已由本算法覆盖，
 *       StagingAlgo 已删除，统一用 ringbuf）。输入存首个输入帧（inputs[0]），输出
 *       写到全部输出位置（端口序数组）。
 *   配置注入：两算法都经 AlgoBase::configure 由装配侧注入。delay 收 **命名** "offset"
 *   （非 JSON，调度器后续注入通道，key 检查）；ringbuf 收 **位置式** depth/back——
 *   按注入顺序（= NodeInfo.config_cache 位置式值表，config "parameters" 元素值顺序）
 *   第 0 个 → depth_、第 1 个 → back_，key 忽略（"直接按序给进来，不用管字段名"），
 *   超过 2 个抛异常。execute 内不需要配置值（配置在注入时已定内部状态），零 JSON 解析。
 *   算法不存端口名、不做端口序外的名字查找。
 *
 * 资源消耗：
 *   - DelayAlgo：1 个 float offset_（透传 + 按 offset sleep，只耗执行时间）。
 *   - RingBufferAlgo：1 个 std::deque<Message>（至多 depth_ 帧）+ 2 个 size_t + 1 个
 *     size_t cfg_idx_（位置式注入计数）。
 *
 * 对外接口：
 *   - DelayAlgo / RingBufferAlgo：AlgoBase 派生，由 g_state expand_hp ④
 *     按 name（"delay"/"ringbuf"）构造 + configure 注入（delay 收命名 "offset"；
 *     ringbuf 位置式 depth/back，按注入顺序第 0/1 位）；
 *     execute(inputs, outputs) 与 AlgoBase 一致。
 *   - RingBufferAlgo 的 config_cache 位置式支持 depth（容量，缺省 1）/ back（回退下标，
 *     缺省 0）——按注入顺序第 0/1 位。
 ******************************************************************************/
#pragma once

#include <algorithm>
#include <chrono>
#include <deque>
#include <stdexcept>
#include <thread>
#include <vector>
#include "../third_party/json.hpp"
#include "algo_base.hpp"

namespace fins::rt {

  /// ① 纯延迟：函数体透传 + 阻塞延迟——execute 前按 offset_（ms，缺省 0）
  /// std::this_thread::sleep_for 真延迟（占 worker）。offset **非 JSON 配置**，由后续
  /// 超周期展开/调度阶段分析经 configure("offset") 注入（本阶段无注入 → 恒 0 纯透传）；
  /// offset 不参与图相位。透传按位置 input[i] → output[i]（execute 收端口序数组，图侧按
  /// 端口序打包）。
  struct DelayAlgo : AlgoBase {
    void execute(const std::vector<Message> &inputs, std::vector<Message> &outputs) override {
      if (offset_ > 0.0f)
        std::this_thread::sleep_for(std::chrono::duration<float, std::milli>(offset_));
      const size_t m = std::min(inputs.size(), outputs.size());
      for (size_t i = 0; i < m; ++i)
        outputs[i] = inputs[i];   // 按位置透传（共享帧）
    }
    void configure(const std::string &key, const nlohmann::json &v) override {
      if (key == "offset") offset_ = v.get<float>();
      else throw std::runtime_error("[DelayAlgo] unsupported config key: " + key);
    }
    float offset_{0};
  };

  /// ② 存环形队列：深度 depth_（位置式第 0 个配置，缺省 1）滚动覆盖最旧；
  /// "取一个时间段" = 按 back_（位置式第 1 个配置，0=最新 .. depth_-1=最旧，缺省 0）
  /// 输出窗内对应帧。输入存首个输入帧（inputs[0]）；输出写到全部输出位置（端口序数组）。
  /// 配置位置式按序注入（"不用管字段名"）：第 0 个 → depth_、第 1 个 → back_，key 忽略。
  struct RingBufferAlgo : AlgoBase {
    void execute(const std::vector<Message> &inputs, std::vector<Message> &outputs) override {
      if (!inputs.empty()) ring_.push_back(inputs[0]);   // 存首个输入帧
      while (ring_.size() > depth_) ring_.pop_front();   // 覆盖最旧
      if (ring_.empty()) return;
      const size_t i = ring_.size() - 1 - std::min(back_, ring_.size() - 1);
      for (auto &out : outputs) out = ring_[i];          // 写全部输出位置
    }
    void configure(const std::string &, const nlohmann::json &v) override {
      if (cfg_idx_ == 0) { depth_ = v.get<size_t>(); if (depth_ == 0) depth_ = 1; }
      else if (cfg_idx_ == 1) back_ = v.get<size_t>();
      else throw std::runtime_error("[RingBufferAlgo] config count exceeds (depth, back)");
      ++cfg_idx_;
    }
    size_t depth_{1};
    size_t back_{0};
    size_t cfg_idx_{0};         // 位置式注入计数（0→depth_，1→back_）
    std::deque<Message> ring_;  // 最近 depth_ 帧，front=最旧
  };

} // namespace fins::rt
