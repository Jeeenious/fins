#pragma once

#include "../mesg/mesg.hpp"
#include "../third_party/json.hpp"
#include "../utils/logger.hpp"

namespace fins::rt {
  struct AlgoBase {
    virtual ~AlgoBase() = default;
    virtual void initial() = 0;
    [[nodiscard]] virtual std::vector<std::string> get_input_ports() const = 0;
    [[nodiscard]] virtual std::vector<std::string> get_output_ports() const = 0;
    [[nodiscard]] virtual std::vector<std::string> get_config_ports() const = 0;

    // todo friend plugin-loader, avoid other's calling
    virtual void update_input_ports(std::vector<std::string> ports) = 0;
    virtual void update_output_ports(std::vector<std::string> ports) = 0;
    virtual void update_config_ports(std::vector<std::string> ports) = 0;
    virtual void execute(const MsgBundle &inputs, MsgBundle &output) = 0;
    virtual void configure(const std::string& key, const nlohmann::json& value) = 0;
  };
} // namespace fins::rt
