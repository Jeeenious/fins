/*******************************************************************************
 ******************************************************************************/
#pragma once

#include <optional>
#include <stdexcept>
#include <tuple>
#include <type_traits>
#include <typeinfo>
#include <utility>
#include <vector>

#include "../mesg/hist.hpp"
#include "../third_party/json.hpp"
#include "algo_base.hpp"

namespace fins::rt {

  /// 判定是否为 History<T>（hist 窗口参数）。视图类型不会被生产者当载荷发布，
  /// 故"参数是 History"即"该输入槽是窗口"，无需运行时哨兵标记。
  template <typename T>
  struct is_history : std::false_type {};
  template <typename T>
  struct is_history<History<T>> : std::true_type {};

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
    /// 第 I 个参数的退化类型（decode_cfg_ / decode_input_ / invoke_func_ 同一套定位基准）
    template <size_t I>
    using ParamT = std::decay_t<std::tuple_element_t<I, ArgsTuple>>;

    template <size_t I>
    std::optional<ParamT<I>> decode_input_(const std::vector<Message> &inputs, size_t ncfg) {
      using P = ParamT<I>;
      if (I < ncfg || I >= ncfg + inputs.size())
        return {};   // 非输入段

      const Message &f = inputs[I - ncfg];
      if (f.frame == nullptr) {   // 空帧 → 默认值
        if constexpr (std::is_default_constructible_v<P>)
          return P{};
        return {};
      }

      // 槽必是窗口线格式（视图不可作为载荷发布）；p_shared 自带 type_hash/size/abi 三重校验
      if constexpr (is_history<P>::value) {
        using E = typename P::value_type;
        const auto &w = *f.p_shared<std::vector<Message>>();
        std::vector<std::shared_ptr<const E>> sps;
        sps.reserve(w.size());
        for (const auto &fr: w)
          sps.push_back(fr.p_shared<E>());   // 只拷句柄，载荷不拷贝
        return P{std::move(sps)};
      }

      return {};
    }

    template <size_t Is = 0>
    Message decode_cfg_(const nlohmann::json &json, size_t idx) {
      if constexpr (Is < sizeof...(Args)) {
        if (Is == idx) {
          using CfgType = std::decay_t<std::tuple_element_t<Is, ArgsTuple>>;

          // 视图不可作配置：先挡掉，否则 has_from_json 探测会为它实例化 nlohmann 容器路径
          // （std::insert_iterator<History>）→ 视图没有 insert，硬报错
          if constexpr (is_history<CfgType>::value) {
            throw std::runtime_error(
                "[Fins Fatal] History 视图不可作配置参数: " + std::string(typeid(CfgType).name()));
          } else if constexpr (nlohmann::detail::has_from_json<nlohmann::json, CfgType>::value) {
            Message m;
            *(m.p_mutable<CfgType>()) = json.get<CfgType>();
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


    template <size_t... Is>
    void invoke_func_(
      const std::vector<Message> &configs,
      const std::vector<Message> &inputs,
      std::vector<Message> &output,
      std::index_sequence<Is...>) {
      const size_t ncfg = configs.size();

      // 输入段参数先解码（生存期锚：forward_as_tuple 存的是引用，临时活不过 std::apply）。
      // make_tuple 实参求值顺序未指定（GCC 右到左）→ decode_input_ 内不可有顺序依赖副作用。
      auto cache = std::make_tuple(decode_input_<Is>(inputs, ncfg)...);

      auto ref_tuple = std::forward_as_tuple(
        [&]() -> decltype(auto) {
          if constexpr (Is < sizeof...(Args)) {
            // 参数序列 = 配置段 + 输入段 + 输出段，按运行时边界定位（无端口名/计数）
            if (Is < ncfg) {
              // 配置段：已由 configure 预解码，直接取（零解析）
              return *(configs[Is].p_shared<ParamT<Is>>());
            } else if (Is < ncfg + inputs.size()) {
              // 输入段：代造值（hist 窗口 / 空帧默认值）优先，否则原路取帧
              if (auto &c = std::get<Is>(cache); c.has_value())
                return *c;
              return *(inputs[Is - ncfg].p_shared<ParamT<Is>>());
            } else {
              // 输出段：pub 分配帧传给用户函数写
              return *(output[Is - ncfg - inputs.size()].p_mutable<ParamT<Is>>());
            }
          }
        }() ...
      );

      std::apply(user_func_, ref_tuple);
    }
  };

} // namespace fins::rt
