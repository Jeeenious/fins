#pragma once

#include <dlfcn.h>

#include "algo/algo_base.hpp"
#include "g_state.hpp"
#include "mesg/mesg.hpp"
#include "third_party/json.hpp"
#include "utils/sha256.hpp"

namespace fins::rt {

  /// 修改 plugins_state

  class PluginLoader {

  private:
    // =================================================================
    // PluginContext 作为内部私有结构体，外层完全不可见
    // =================================================================

    struct PluginContext : public std::enable_shared_from_this<PluginContext> {
      void *handle = nullptr;
      std::string so_path;
      std::string version;

      typedef void (*DestroyPluginFunc)(AlgoBase *);
      typedef int (*GetPluginCountFunc)();
      typedef const char *(*GetAlgoNameFunc)(int);
      typedef const char *(*GetAlgoVersionFunc)();
      typedef AlgoBase *(*CreateAlgoFunc)(const char *);

      DestroyPluginFunc destroy_plugin = nullptr;
      GetPluginCountFunc get_plugin_count = nullptr;
      GetAlgoNameFunc get_algo_name = nullptr;
      GetAlgoVersionFunc get_algo_version = nullptr;
      CreateAlgoFunc create_algo = nullptr;

      ~PluginContext() { if (handle) dlclose(handle); handle = nullptr; }
    };

    util::TBBMap<std::shared_ptr<PluginContext>> plugins_reg_;  // [algo_name : algo_version] -> [私有工厂上下文]

  public:
    static PluginLoader &instance() {
      static PluginLoader inst;
      return inst;
    }

  public:
    PluginLoader() = default;
    ~PluginLoader() = default;
    PluginLoader(const PluginLoader &) = delete;
    PluginLoader &operator=(const PluginLoader &) = delete;

    /**
     * @brief 注册 .so，并自动同步状态至全局 Registry
     */
    void register_so(const std::string &so_path) {
      // 1. 状态流转：开始加载
      const auto ctx = std::make_shared<PluginContext>();
      ctx->so_path = so_path;
      ctx->handle = dlopen(so_path.c_str(), RTLD_NOW | RTLD_LOCAL);

      if (!ctx->handle) throw std::runtime_error(dlerror());

      ctx->destroy_plugin = (DestroyPluginFunc) dlsym(ctx->handle, "destroy_plugin");
      ctx->get_plugin_count = (GetPluginCountFunc) dlsym(ctx->handle, "get_plugin_count");
      ctx->get_algo_name = (GetAlgoNameFunc) dlsym(ctx->handle, "get_algo_name");
      ctx->get_algo_version = (GetAlgoVersionFunc) dlsym(ctx->handle, "get_algo_version");
      ctx->create_algo = (CreateAlgoFunc) dlsym(ctx->handle, "create_algo");

      if (!ctx->create_algo || !ctx->destroy_plugin || !ctx->get_plugin_count || !ctx->get_algo_name || !ctx->get_algo_version) {
        throw std::runtime_error("Missing required C-symbols");
      }

      const int count = ctx->get_plugin_count();
      for (int i = 0; i < count; ++i) {
        const std::string name = ctx->get_algo_name(i);
        const std::string version = ctx->get_algo_version(i);

        TBBMAP_SET(plugins_reg_, name + ":" + version, ctx);

        TBBMAP_SET(algo_version, version, name);
      }
    }

    /**
     * @brief 接口 2：【点菜做菜】传入单个节点的明文 JSON 需求，
     * @return 返回武装完毕、可以直接丢给调度器并行跑的 std::shared_ptr<IPlugin>
     * @example {
     * "name": "algo:1.0",
     * "parameters": [
     *    { "name": "kernel_size", "value": 5 },
     *    { "name": "sigma", "value": 1.2 }
     *  ],
     *  "inputs": {
     *    "image_raw": "camera_stream" (camera_stream --> image_raw)
     *  },
     *  "outputs": {
     *    "image_out": "display_queue" (display_queue <-- image_out)
     *  }
     *  }
     */
    std::shared_ptr<AlgoBase> create_plugin(const nlohmann::json &node_json) {
      const std::string name = node_json.at("name").get<std::string>();
      const std::string version = node_json.at("version").get<std::string>();
      const std::string key = name + ":" + version;

      if (!TBBMAP_HAS(plugins_reg_, key)) {
        throw std::runtime_error("PluginLoader: Unregistered algorithm name in map: " + key);
      }

      const auto ctx = TBBMAP_AT(plugins_reg_, key);

      // 3. 跨边界销毁保障（闭包）
      auto destroyer = ctx->destroy_plugin;
      const auto creator = ctx->create_algo(key.c_str());
      std::shared_ptr<AlgoBase> algo_p(creator, [destroyer, ctx](AlgoBase *p) {
        if (destroyer && p) {
          destroyer(p);
        }
      });

      // 5. 端口顺序强行对齐
      std::vector<std::string> inputs_order;
      if (node_json.contains("inputs") && node_json["inputs"].is_object()) {
        for (const auto &[port_name, _]: node_json["inputs"].items()) {
          inputs_order.push_back(port_name);
        }
      }

      std::vector<std::string> outputs_order;
      if (node_json.contains("outputs") && node_json["outputs"].is_object()) {
        for (const auto &[port_name, _]: node_json["outputs"].items()) {
          outputs_order.push_back(port_name);
        }
      }

      std::vector<std::string> configs_order;
      if (node_json.contains("parameters") && node_json["parameters"].is_array()) {
        for (const auto &param : node_json["parameters"]) {
          if (param.contains("name")) {
            configs_order.push_back(param["name"].get<std::string>());
          }
        }
      }

      // 将端口的映射元数据武装给插件 todo 这个开销巨大
      algo_p->update_input_ports(inputs_order);
      algo_p->update_output_ports(outputs_order);
      algo_p->update_config_ports(configs_order);

      // 遍历 parameters 数组，通过反射直接改写用户 C++ 变量的值
      if (node_json.contains("parameters") && node_json["parameters"].is_array()) {
        for (const auto &param: node_json["parameters"]) {
          if (param.contains("name") && param.contains("value")) {
            auto p_name = param["name"].get<std::string>();
            algo_p->update_configs(p_name, param["value"]); // ???
          }
        }
      }

      return algo_p;
    }
  };
} // namespace fins::rt