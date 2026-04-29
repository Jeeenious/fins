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

  template<typename T>
  struct Msg {
    std::shared_ptr<T> data;
    AcqTime acq_time;

    Msg() : data(nullptr), acq_time{zero()} {}

    explicit Msg(const AnyMsg &any) : data(std::static_pointer_cast<T>(any.data)), acq_time(any.acq_time) {}

    T get() const { return *data; }
    T *operator->() const { return data.get(); }
    T &operator*() const { return *data; }
    explicit operator bool() const { return data != nullptr; }
    std::shared_ptr<T> ptr() const { return data; }
  };

} // namespace fins