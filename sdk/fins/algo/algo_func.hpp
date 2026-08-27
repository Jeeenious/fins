/*******************************************************************************
 * algo_func.hpp — 算法函数式封装（AlgoFunc）
 *
 * 内部逻辑：
 *   AlgoFunc 把用户裸函数指针包成 AlgoBase。execute 收"按端口序排列"的输入/输出
 *   Message 数组 + **类型化配置帧** configs_（位置式：configs_[r] ↔ 用户函数第 r 个
 *   配置参数，configure 注入时已 JSON→typed 解码一次），按运行时边界
 *   （configs_.size() / inputs.size()）把函数签名参数序列拆成
 *   "配置段 + 输入段 + 输出段"（**configs inputs outputs 顺着来**），逐段解包成
 *   Args... 引用传给用户函数（共享帧，零拷贝）：
 *     配置段（Is < configs.size()）              — 从 configs_[Is] sub 取（**预解码帧**，
 *       execute 内零解析——JSON→typed 解码发生在 configure 注入时）
 *     输入段（configs.size() ≤ Is < configs.size()+inputs.size()）
 *                                        — 从 inputs[Is-configs.size()] sub 取（只读）
 *     输出段（其余）                            — outputs[..] pub 分配帧传给用户函数写
 *   configure(key, json) 由装配侧按序逐个调用（顺序 = NodeInfo.config_cache 位置式值表，
 *   key 忽略）：**位置式解析**——配置段前置使绝对下标 = 相对下标 = 已注入配置数
 *   configs_.size()（**无需输入参数数/num_inputs_**），按签名逐位展开找 Args[该下标]
 *   的类型（tuple_element_t 编译期取，运行时下标靠递归逐 Is 比较），has_from_json 检查后
 *   json.get<CfgType>() 解码进 Message append 到 configs_；下标超出签名参数数抛异常
 *   （配置个数超过函数签名）。
 *
 *   ★ 参数布局契约（必须遵守，违反即错位）：
 *   用户函数参数签名必须严格按「配置段 + 输入段 + 输出段」排列——配置参数在最前、
 *   输入参数紧跟其后、输出参数最后。AlgoFunc 无端口名/参数名，全靠运行时边界猜段
 *   （Is<configs_.size() 配置、<+inputs.size() 输入、其余输出），无法感知参数语义，
 *   一旦把配置参数放到输入参数之后就会张冠李戴。
 *   顺序保证链（配置段相对序号正确的依据）：
 *     config "parameters" 数组顺序（元素值，位置式）→ NodeInfo.config_cache（vector<json>
 *     值表）→ 建图 ④ 逐个 configure（key 忽略，逐个解码 append configs_）→ execute
 *     配置段相对序号从 0 递增。
 *
 * 资源消耗：
 *   1 份用户函数指针 + configs_（vector<Message> 类型化配置帧，每配置参数 1 帧——
 *   配置注入时解码一次，execute 零解析、零缓存重建）。
 *
 * 对外接口：
 *   AlgoFunc<UserFunc>：模板；构造传用户函数指针；configure(key, json) 位置式解析
 *   （key 忽略）写 configs_；execute(inputs, outputs) 按参数序解包 cfg/in/out。
 ******************************************************************************/
#pragma once

#include <cstddef>
#include <stdexcept>
#include <tuple>
#include <typeinfo>
#include <utility>
#include <vector>

#include "../third_party/json.hpp"
#include "algo_base.hpp"

namespace fins::rt {

  template <typename Arg>
  struct AlgoFunc;

  template <typename... Args>
  struct AlgoFunc<void(*)(Args...)> final : public AlgoBase {
    using UserFunc = void(*)(Args...);
    using ArgsTuple = std::tuple<std::decay_t<Args>...>;

  private:
    UserFunc user_func_;
    std::vector<Message> configs_;  // 位置式类型化配置帧（configure 注入时 JSON→typed 解码一次）

  public:
    explicit AlgoFunc(const UserFunc func) : user_func_(func) {}

    void initial() override {}

    void execute(const std::vector<Message> &inputs, std::vector<Message> &outputs) override {
      invoke_func_(configs_, inputs, outputs, std::make_index_sequence<sizeof...(Args)>{});
    }

    /// 配置注入：**位置式按序解析**（key 忽略）——配置段前置，绝对下标 = 相对下标 =
    /// configs_.size()（无需输入参数数）；按签名逐位展开找 Args[该下标] 类型解码进
    /// Message（tuple_element_t 编译期取 + has_from_json 检查）。
    /// 注入顺序 = NodeInfo.config_cache 位置式值表（装配侧逐个调用）。
    void configure(const std::string &, const nlohmann::json &json) override {
      const size_t idx = configs_.size();
      if (idx >= sizeof...(Args))
        throw std::runtime_error(
            "[Fins Fatal] Config index out of function signature: " + std::to_string(idx));
      configs_.push_back(decode_cfg_<>(json, idx));   // 递归展开签名找 Args[idx] 类型解码（返回 Message）
    }

  private:
    template <size_t... Is>
    void invoke_func_(
      const std::vector<Message> &configs,
      const std::vector<Message> &inputs,
      std::vector<Message> &output,
      std::index_sequence<Is...>) {
      auto ref_tuple = std::forward_as_tuple(
        [&]() -> decltype(auto) {
          if constexpr (Is < sizeof...(Args)) {
            using ParamType = std::decay_t<std::tuple_element_t<Is, ArgsTuple>>;

            // 参数序列 = 配置段 + 输入段 + 输出段（configs inputs outputs 顺着来），
            // 全部按运行时边界定位（无端口名/计数）。
            if (Is < configs.size()) {
              // 配置段（前置）：类型化帧已由 configure 注入时预解码，直接 sub 取（零解析）
              return *(configs[Is].sub<ParamType>());
            } else if (Is < configs.size() + inputs.size()) {
              // 输入段：从输入数组对应序取帧（共享，只读）
              const size_t r = Is - configs.size();
              return *(inputs[r].sub<ParamType>());
            } else {
              // 输出段：在输出数组对应序 pub 分配帧传给用户函数写
              const size_t r = Is - configs.size() - inputs.size();
              return *(output[r].pub<ParamType>());
            }
          }
        }() ...
      );

      std::apply(user_func_, ref_tuple);
    }

    template <size_t Is = 0>
    Message decode_cfg_(const nlohmann::json &json, size_t idx) {
      if constexpr (Is < sizeof...(Args)) {
        if (Is == idx) {
          using CfgType = std::decay_t<std::tuple_element_t<Is, ArgsTuple>>;
          if constexpr (nlohmann::detail::has_from_json<nlohmann::json, CfgType>::value) {
            Message m;
            *(m.pub<CfgType>()) = json.get<CfgType>();
            return m;
          } else {
            throw std::runtime_error(
                "[Fins Fatal] Config type not registered for JSON deserialization: " +
                std::string(typeid(CfgType).name()));
          }
        }
        return decode_cfg_<Is + 1>(json, idx);
      }
      throw std::runtime_error(
          "[Fins Fatal] Config index out of function signature: " + std::to_string(idx));
    }
  };

} // namespace fins::rt
