#pragma once

#include "../utils/form.hpp"
#include "../utils/time.hpp"
#include "../utils/abi.hpp"

namespace fins::rt {

  /**
   * @brief 实时消息 / Real-time message
   * @details 在算法实例（Job）之间流动的数据单元。内部复用框架的类型擦除消息
   *          AnyMsg 承载真正的负载与采集时间戳，另外附带端口号、序列号和产出者
   *          标识，供 Graph 做路由与追踪。
   *
   *          A data unit flowing between algorithm instances (Jobs). It reuses
   *          the framework's type-erased AnyMsg for the actual payload and
   *          acquisition timestamp, plus port/sequence/producer metadata for
   *          routing and tracing by the Graph.
   *
   * @see AnyMsg, MessageBuffer, Job
   */
  struct Message {
    std::shared_ptr<void> frame{nullptr};

    size_t type_size{0};                        // 自定义结构体大小 (防御增量修改导致的布局越界)
    size_t type_hash{0};                        // 运行时类型硬核 Hash (用于校验基础类名)
    uint32_t abi_tag{0};                        // 第三方大库 ABI 版本号 (防御跨环境编译大库撕裂)
    const char* type_name{"void"};              // 发生异常时用于打印错误日志的可读类型名

    template<typename T>
    std::shared_ptr<T> pub() {
      if (frame != nullptr) {
        throw std::runtime_error("[Fins Fatal] Attempting to publish a message frame that is already occupied.");
      }

      using RawT = std::decay_t<T>;
      type_size = sizeof(RawT);
      type_name = typeid(RawT).name();
      type_hash = typeid(RawT).hash_code();
      abi_tag = util::ABITag<RawT>::value();
      frame = std::make_shared<RawT>();

      return std::static_pointer_cast<T>(frame);
    }

    template<typename T>
    std::shared_ptr<T> sub() {
      if (frame == nullptr) {
        throw std::runtime_error("[Fins Fatal] Attempting to subscribe to a null message frame.");
      }

      using RawT = std::decay_t<T>;
      size_t target_size = sizeof(RawT);
      size_t target_hash = typeid(RawT).hash_code();
      uint32_t target_abi = util::ABITag<RawT>::value();

      if (type_hash != target_hash) {
        std::ostringstream oss;
        oss << "[Fins Fatal] Type Mismatch! The message packet holds type ["
            << type_name << "], but the plugin is trying to cast it to [" << typeid(RawT).name() << "].";
        throw std::runtime_error(oss.str());
      }
      if (type_size != target_size) {
        std::ostringstream oss;
        oss << "[Fins Fatal] Structural Size Mismatch for [" << type_name
            << "]! Producer size: " << type_size
            << " bytes, Consumer size: " << target_size
            << " bytes. Incremental structure update detected, memory layout is broken!";
        throw std::runtime_error(oss.str());
      }
      if (abi_tag != target_abi) {
        std::ostringstream oss;
        oss << "[Fins Fatal] Third-party ABI Version Conflict for [" << type_name
            << "]! Message came with ABI tag [" << abi_tag
            << "], but consumer library ABI is [" << target_abi
            << "]. Casting this pointer will cause immediate memory corruption!";
        throw std::runtime_error(oss.str());
      }

      return std::static_pointer_cast<T>(frame);
    }
  };

  using MsgBundle = std::map<std::string, Message>;

} // namespace fins::rt