/*******************************************************************************
 ******************************************************************************/
#pragma once

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
