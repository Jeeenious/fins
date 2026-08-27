/*******************************************************************************
 * algo_base.hpp — 算法基类（AlgoBase）
 *
 * 内部逻辑：
 *   AlgoBase 是全部算法（AlgoFunc 用户函数封装 / 内置 delay/ringbuf / 插件实现）的
 *   统一执行接口，不承载任何解析态——执行状态（顶点 State）与端口名/周期等图侧信息
 *   由外层（Pipeline::NodeInfo + 图顶点）持有，算法只负责执行：
 *     execute(inputs, outputs) 收"按端口序排列"的输入/输出 Message 数组，算法内部
 *     不碰端口名/参数名、不用 map 查找；配置**不随 execute 传入**——由装配侧建图时
 *     经 configure 按序注入派生私有状态（AlgoFunc 持类型化帧成员 configs_，见其头注释；
 *     delay 的 offset 由后续超周期展开分析注入；ringbuf 的 depth/back 亦经此注入）。
 *     execute 内零 JSON 解析——配置在注入时已解码成类型化帧/内部状态，execute 只消费。
 *     configure(key, json) 为注入通道：key 标识配置项（AlgoFunc 位置式注入，key 忽略——
 *     configs-first 布局下按"已注入配置数 configs_.size()"定位，无需输入参数数；
 *     ringbuf 亦位置式 depth/back；delay 的 offset 按名注入）。注入顺序 =
 *     NodeInfo.config_cache 位置式值表顺序（见 g_state）。
 *   端口名连接（同名直连）完全在图侧——AlgoBase 无字段、无 get/update_*_ports 接口。
 *   节点解析态 Pipeline::NodeInfo（Pipeline::parse 的产物）是 **Pipeline 的一部分**，
 *   定义在 g_state.hpp 的 Pipeline 结构体内（见其头注释）；AlgoBase 不感知它。
 *
 * 资源消耗：
 *   AlgoBase 实例：基类无字段（各派生私有状态，如 AlgoFunc 仅 1 个函数指针 +
 *   configs_ 类型化帧表；delay/ringbuf 各自私有标量/队列）。
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
    /// ringbuf 亦位置式 depth/back；delay 的 offset 由后续超周期展开分析按名注入）。
    virtual void configure(const std::string &key, const nlohmann::json &value) = 0;
  };
} // namespace fins::rt
