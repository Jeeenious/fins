/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/

// msg.hpp

#pragma once

#include <fins/abi_traits.hpp>
#include <fins/utils/time.hpp>
#include <memory>
#include <typeindex>
#include <typeinfo>

namespace fins {

  /**
   * @brief 类型擦除消息 / Type-erased message
   * @details 运行时多态的消息容器，用于框架内部传输。
   *          用户通常通过 Msg<T> 与之交互，而非直接使用此结构体。
   *
   *          Runtime-polymorphic message container used for internal transport.
   *          Users typically interact with Msg<T> rather than this struct directly.
   */
  struct AnyMsg {
    std::shared_ptr<void> data;
    AcqTime acq_time;
    const char *type_name;
    std::type_index type_id;
    uint32_t abi_tag;

    AnyMsg() : data(nullptr), acq_time{zero()}, type_name(nullptr), type_id(std::type_index(typeid(void))), abi_tag(0) {}

    template<typename T>
    AnyMsg(std::shared_ptr<T> ptr, AcqTime ts) :
        data(std::static_pointer_cast<void>(ptr)), acq_time(ts), type_name(typeid(T).name()),
        type_id(std::type_index(typeid(T))), abi_tag(AbiTag<T>::value()) {}
  };

  /**
   * @brief 类型化消息 / Typed message
   * @details 输入回调中接收的消息容器。提供对数据和时间戳的安全访问。
   *
   *          Message container received in input callbacks. Provides safe access
   *          to data and timestamp.
   *
   * @tparam T 数据类型 / Data type
   *
   * @par 示例 / Example
   * @code
   * void on_image(const fins::Msg<Image>& msg) {
   *   fins::AcqTime ts = msg.acq_time;
   *   const Image& img = *msg;           // 解引用 / dereference
   *   auto pixels = msg->pixels;         // 成员访问 / member access
   *   std::shared_ptr<Image> ptr = msg.ptr();  // 获取 shared_ptr
   * }
   * @endcode
   */
  template<typename T>
  struct Msg {
    std::shared_ptr<T> data;   ///< 数据 shared_ptr / Data shared_ptr
    AcqTime acq_time;          ///< 采集时间戳 / Acquisition timestamp

    Msg() : data(nullptr), acq_time{zero()} {}

    /// 从 AnyMsg 构造 / Construct from AnyMsg
    explicit Msg(const AnyMsg &any) : data(std::static_pointer_cast<T>(any.data)), acq_time(any.acq_time) {}

    /// 获取数据拷贝 / Get data copy
    T get() const { return *data; }
    /// 指针成员访问 / Pointer member access
    T *operator->() const { return data.get(); }
    /// 解引用 / Dereference
    T &operator*() const { return *data; }
    /// 布尔检查（数据是否非空） / Boolean check (whether data is non-null)
    explicit operator bool() const { return data != nullptr; }
    /// 获取 shared_ptr / Get shared_ptr
    std::shared_ptr<T> ptr() const { return data; }
  };

} // namespace fins