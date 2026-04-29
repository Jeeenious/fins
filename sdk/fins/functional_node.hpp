/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/

// functional_node.hpp

#pragma once

#include <any>
#include <fins/msg.hpp>
#include <fins/function_tags.hpp>
#include <fins/node.hpp>
#include <functional>
#include <tuple>
#include <type_traits>
#include <vector>

namespace fins {

  class GenericFunctionalNode : public fins::Node {
  public:
    using LogicThunk = std::function<void(GenericFunctionalNode *, const AnyMsg &)>;
    using DefineParametersFunc = std::function<void(GenericFunctionalNode *)>;

    GenericFunctionalNode(const NodeMeta &meta, LogicThunk thunk, DefineParametersFunc define_func) :
        preset_meta_(meta), logic_thunk_(thunk), define_func_(define_func) {}

    void define() override {
      this->meta_ = preset_meta_;

      if (!preset_meta_.inputs.empty()) {
        input_handlers_[0] = [this](const AnyMsg &msg) { this->logic_thunk_(this, msg); };
      }

      if (define_func_) {
        define_func_(this);
      }
    }

    void initialize() override {}
    void run() override {}
    void pause() override {}
    void reset() override {}

    template<int Port, typename T>
    void emit(std::shared_ptr<T> data, AcqTime ts) {
      this->send<Port>(data, ts);
    }

    template<typename T>
    void emit_dynamic(int port, std::shared_ptr<T> data, AcqTime ts) {
      this->send_dynamic(port, data, ts);
    }

    std::vector<std::shared_ptr<void>> parameter_storage;

    template<typename T>
    void register_parameter_with_storage(size_t storage_idx, const std::string &name, const T &default_val) {
      if (parameter_storage.size() <= storage_idx)
        parameter_storage.resize(storage_idx + 1);

      auto val_ptr = std::make_shared<T>(default_val);
      parameter_storage[storage_idx] = val_ptr;

      this->register_parameter<T>(name, [val_ptr](const T &new_val) { *val_ptr = new_val; });
    }

  private:
    NodeMeta preset_meta_;
    LogicThunk logic_thunk_;
    DefineParametersFunc define_func_;
  };

  struct ParameterConfig {
    std::string name;
    std::any default_value;
  };

  template<typename Func>
  class FunctionBuilder {
  public:
    FunctionBuilder(std::string name, Func func) : func_(func) {
#ifdef PKG_SOURCE
      meta_.source = PKG_SOURCE;
#endif
#ifdef PKG_NAME
      meta_.package_name = PKG_NAME;
#endif
      meta_.name = name;
      meta_.category = "Functional";

      using Traits = function_traits<decltype(&Func::operator())>;
      using ArgsTuple = typename Traits::args_tuple;

      using InputArg = std::tuple_element_t<0, ArgsTuple>;
      using InType = strip_wrapper_t<InputArg>;

      meta_.inputs.resize(1);
      meta_.inputs[0] = {"in_0", FINS_TYPE_REGISTER.get_name<InType>()};

      analyze_signature<ArgsTuple>(std::make_index_sequence<std::tuple_size_v<ArgsTuple> - 1>{});
    }

    FunctionBuilder &with_description(std::string desc) {
      meta_.description = desc;
      return *this;
    }

    FunctionBuilder &with_category(std::string cat) {
      meta_.category = cat;
      return *this;
    }

    FunctionBuilder &with_inputs_description(std::vector<std::string> descs) {
      for (size_t i = 0; i < descs.size() && i < meta_.inputs.size(); ++i) {
        meta_.inputs[i].name = descs[i];
      }
      return *this;
    }

    FunctionBuilder &with_outputs_description(std::vector<std::string> descs) {
      for (size_t i = 0; i < descs.size() && i < meta_.outputs.size(); ++i) {
        meta_.outputs[i].name = descs[i];
      }
      return *this;
    }

    template<typename T>
    FunctionBuilder &with_parameter(const std::string &name, T default_val = T()) {
      ParameterConfig cfg;
      cfg.name = name;
      cfg.default_value = default_val;
      parameter_configs_.push_back(cfg);
      meta_.parameters.push_back({name, FINS_TYPE_REGISTER.get_name<T>(), std::to_string(default_val)});
      return *this;
    }

    bool build() {
      using Traits = function_traits<decltype(&Func::operator())>;
      using ArgsTuple = typename Traits::args_tuple;

      auto define_func = [configs = this->parameter_configs_](GenericFunctionalNode *node) {
        apply_parameter_definitions<ArgsTuple>(node, configs,
                                               std::make_index_sequence<std::tuple_size_v<ArgsTuple> - 1>{});
      };

      auto thunk = [f = func_](GenericFunctionalNode *node, const AnyMsg &any_msg) {
        using InputArg = std::tuple_element_t<0, ArgsTuple>;
        using InType = strip_wrapper_t<InputArg>;

        Msg<InType> typed_msg(any_msg);
        Input<InType> input_wrapper(typed_msg);

        call_and_send(node, f, input_wrapper, std::make_index_sequence<std::tuple_size_v<ArgsTuple> - 1>{});
      };

      NodeFactory::get_instance().register_node(
          meta_, [m = meta_, t = thunk, d = define_func]() -> INode * { return new GenericFunctionalNode(m, t, d); });
      return true;
    }

  private:
    template<typename Tuple, size_t... Is>
    void analyze_signature(std::index_sequence<Is...>) {
      ((analyze_arg<std::tuple_element_t<Is + 1, Tuple>>(Is)), ...);
    }

    template<typename ArgT>
    void analyze_arg(size_t) {
      if constexpr (is_output<ArgT>::value) {
        using OutType = strip_wrapper_t<ArgT>;
        meta_.outputs.push_back({"out", FINS_TYPE_REGISTER.get_name<OutType>()});
      }
    }

    template<typename Tuple, size_t... Is>
    static void apply_parameter_definitions(GenericFunctionalNode *node, const std::vector<ParameterConfig> &configs,
                                            std::index_sequence<Is...>) {
      size_t parameter_idx = 0;
      ((define_helper<std::tuple_element_t<Is + 1, Tuple>>(node, configs, Is, parameter_idx)), ...);
    }

    template<typename ArgT>
    static void define_helper(GenericFunctionalNode *node, const std::vector<ParameterConfig> &configs,
                              size_t storage_idx, size_t &extern_config_idx) {
      (void)storage_idx;
      if constexpr (is_parameter<ArgT>::value) {
        using T = strip_wrapper_t<ArgT>;
        if (extern_config_idx < configs.size()) {
          const auto &cfg = configs[extern_config_idx];
          T default_val = T();
          try {
            if (cfg.default_value.has_value()) {
              default_val = std::any_cast<T>(cfg.default_value);
            }
          } catch (...) {
          }

          node->register_parameter_with_storage<T>(storage_idx, cfg.name, default_val);
          extern_config_idx++;
        } else {
          node->register_parameter_with_storage<T>(storage_idx, "param_" + std::to_string(storage_idx), T());
        }
      }
    }

    template<typename ArgT>
    struct ArgHolder {
      using type =
          std::conditional_t<is_output<ArgT>::value, Output<strip_wrapper_t<ArgT>>, Parameter<strip_wrapper_t<ArgT>>>;
    };

    template<typename ArgT>
    static typename ArgHolder<ArgT>::type create_arg(GenericFunctionalNode *node, size_t storage_idx) {
      (void)storage_idx;
      using T = strip_wrapper_t<ArgT>;
      if constexpr (is_output<ArgT>::value) {
        return Output<T>();
      } else {
        void *raw = node->parameter_storage[storage_idx].get();
        T *val = static_cast<T *>(raw);
        return Parameter<T>(*val);
      }
    }

    template<typename ArgT, typename HolderT>
    static void check_and_send(GenericFunctionalNode *node, HolderT &holder, size_t &port_idx, AcqTime ts) {
      (void)ts;
      if constexpr (is_output<ArgT>::value) {
        using T = strip_wrapper_t<ArgT>;
        auto ptr = std::make_shared<T>(std::move(holder.value));
        node->emit_dynamic(port_idx, ptr, ts);
        port_idx++;
      }
    }

    template<typename F, typename InW, size_t... Is>
    static void call_and_send(GenericFunctionalNode *node, F &func, InW &in_wrapper, std::index_sequence<Is...>) {
      using Traits = function_traits<decltype(&Func::operator())>;
      using ArgsTuple = typename Traits::args_tuple;

      std::tuple<typename ArgHolder<std::tuple_element_t<Is + 1, ArgsTuple>>::type...> args(
          create_arg<std::tuple_element_t<Is + 1, ArgsTuple>>(node, Is)...);

      func(in_wrapper, std::get<Is>(args)...);

      size_t out_port_idx = 0;
      ((check_and_send<std::tuple_element_t<Is + 1, ArgsTuple>>(node, std::get<Is>(args), out_port_idx,
                                                                in_wrapper.msg.acq_time)),
       ...);
    }

    Func func_;
    NodeMeta meta_;
    std::vector<ParameterConfig> parameter_configs_;
  };

  template<typename Func>
  auto Function(std::string name, Func func) {
    return FunctionBuilder<Func>(name, func);
  }
} // namespace fins