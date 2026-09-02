/*******************************************************************************
 * algo_base.hpp — 算法基类（AlgoBase）
 *
 * 资源消耗：
 *   AlgoBase 实例：基类无字段（各派生私有状态，如 AlgoFunc 仅 1 个函数指针 +
 *   configs_ 类型化帧表）。
 *
 * 对外接口：
 *   AlgoBase — 纯执行接口：initial()/execute(const vector<Message>&, vector<Message>&)
 *             /configure(key, json) + 虚析构。
 ******************************************************************************/
#pragma once

#include <string>
#include <vector>

#include "../mesg/mesg.hpp"
#include "../third_party/json.hpp"

namespace fins::rt {

  /// 算法统一执行接口（纯接口，无字段）：execute 收按端口序排列的输入/输出数组；
  /// configure 由装配侧按序/按名注入私有状态（AlgoFunc 位置式——configs-first 布局下
  /// 配置段前置，按已注入配置数定位，无需输入参数数）。具体状态由派生私有持有。
  struct AlgoBase {
    virtual ~AlgoBase() = default;
    virtual void initial() {}

    /// 执行：inputs/outputs 按端口序数组（inputs[i] ↔ 输入端口序[i]）。配置不随本调用
    /// 传入——已由装配侧建图时 configure 注入派生私有状态（AlgoFunc 读成员 configs_），
    /// 本处零 JSON 解析、不碰端口名/参数名。
    virtual void execute(const std::vector<Message> &inputs, std::vector<Message> &outputs) = 0;

    /// 配置注入：按序/按名注入算法私有状态（AlgoFunc 位置式——key 忽略，configs-first
    /// 布局下按已注入配置数 configs_.size() 定位，注入时 JSON→类型解码存成员 configs_；
    virtual void configure(const std::string &key, const nlohmann::json &value) = 0;
  };
} // namespace fins::rt
