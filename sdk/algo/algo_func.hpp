#pragma once

#include "../third_party/json.hpp"
#include "algo_base.hpp"

namespace fins::rt {

  template <typename Arg>
  struct AlgoFunc;

  template <typename... Args>
  struct AlgoFunc<void(*)(Args...)> final : public AlgoBase {

    using UserFunc = void(*)(Args...);
    using ArgsTuple = std::tuple<std::decay_t<Args>...>;

    explicit AlgoFunc(const UserFunc func) : user_func_(func) {}

    void initial() override {}

    // 在外面如何构建 bundle，或者说端口绑定机制怎么整？？
    void execute(const MsgBundle& inputs, MsgBundle& outputs) override {
      invoke_user_func_(inputs, outputs, std::make_index_sequence<sizeof...(Args)>{});
    }
    void configure(const std::string& key, const nlohmann::json& value) override {
      update_configs_(key, value, std::make_index_sequence<sizeof...(Args)>{});
    }

    [[nodiscard]] std::vector<std::string> get_input_ports() const override { return input_port_names_; }
    [[nodiscard]] std::vector<std::string> get_output_ports() const override { return output_port_names_; }
    [[nodiscard]] std::vector<std::string> get_config_ports() const override { return config_port_names_; }

    void update_input_ports(std::vector<std::string> inputs) override {
      input_port_names_ = std::move(inputs);
      num_inputs_ = input_port_names_.size();
    }
    void update_output_ports(std::vector<std::string> outputs) override {
      output_port_names_ = std::move(outputs);
      num_outputs_ = output_port_names_.size();
    }
    void update_config_ports(std::vector<std::string> configs) override {
      config_port_names_ = std::move(configs);
      num_configs_ = config_port_names_.size();
    }

  private:
    template <size_t... Is>
    void invoke_user_func_(const MsgBundle& inputs, MsgBundle& output, std::index_sequence<Is...>) {
      auto ref_tuple = std::forward_as_tuple(
        [&]() -> decltype(auto) {
          if constexpr (Is < sizeof...(Args)) {
            using ParamType = std::decay_t<std::tuple_element_t<Is, ArgsTuple>>;

            // --- 处理输入 ---
            if (Is < num_inputs_) {
              const std::string& in_name = input_port_names_[Is];
              auto& msg = const_cast<fins::rt::Message&>(inputs.at(in_name));
              return *(msg.sub<ParamType>());
            }
            // --- 处理配置 ---
            else if (Is >= num_inputs_ && Is < num_inputs_ + num_configs_) {
              const size_t cfg_vec_idx = Is - num_inputs_;
              const std::string& cfg_name = config_port_names_[cfg_vec_idx];
              auto& msg = const_cast<fins::rt::Message&>(configs_.at(cfg_name));
              return *(msg.sub<ParamType>());
            }
            // --- 处理输出 ---
            else {
              const size_t out_vec_idx = Is - num_inputs_ - num_configs_;
              const std::string& out_name = output_port_names_[out_vec_idx];
              auto& msg = output[out_name];
              return *(msg.pub<ParamType>());
            }
          }
        }() ...
    );

      std::apply(user_func_, ref_tuple);
    }

    template <size_t... Is>
    void update_configs_(const std::string& key, const nlohmann::json& value, std::index_sequence<Is...>) {

      if (std::ranges::find(config_port_names_, key) == config_port_names_.end()) {
        throw std::runtime_error("[Fins Error] Unregistered config key: " + key);
      }

      const size_t relative_cfg_idx = std::ranges::find(config_port_names_, key) - config_port_names_.begin();
      const size_t absolute_cfg_idx = relative_cfg_idx + num_inputs_;

      ((Is == absolute_cfg_idx ? (//todo 这里到底哪些类型允许使用，得确定
        [&]() {
          using CfgType = std::decay_t<std::tuple_element_t<Is, ArgsTuple>>;
          using BasicJsonType = nlohmann::json;

          // 🌟 官方标准的合法性探测：检查 adl_serializer 是否支持该类型的 from_json 转换
          // 这个探测极其稳定，绝不会报“无法解析”
          constexpr bool is_decodable = nlohmann::detail::has_from_json<BasicJsonType, CfgType>::value;

          if constexpr (is_decodable) {
            auto& msg = configs_[key];
            msg.frame = nullptr;
            *(msg.pub<CfgType>()) = value.get<CfgType>();
          } else {
            // 那些图像、输出结构体等非配置类型，编译期会安全地走到这里，直接闭嘴
            throw std::runtime_error("[Fins Fatal] Type is not active or registered for JSON deserialization.");
          }
        }(), 0) : 0), ...);
    }

private:
    UserFunc user_func_;
    std::vector<std::string> input_port_names_;
    std::vector<std::string> output_port_names_;
    std::vector<std::string> config_port_names_;

    size_t num_inputs_;
    size_t num_configs_;
    size_t num_outputs_;

    MsgBundle configs_;
};

} // namespace fins::rt
