/*******************************************************************************
 * Copyright (c) 2026.
 * All rights reserved.
 ******************************************************************************/

/// todo @limitation: 自定义类型

#pragma once

#include <cstdint>
#include <memory>
#include <type_traits>

#if __has_include(<rclcpp/version.h>)
#include <rclcpp/version.h>
#elif __has_include(<rclcpp/rclcpp.hpp>)
#include <rclcpp/rclcpp.hpp>
#endif

#if __has_include(<rosidl_runtime_cpp/traits.hpp>)
#include <rosidl_runtime_cpp/traits.hpp>
#define FINS_HAS_ROS_TRAITS 1
#endif

#if __has_include(<opencv2/core/version.hpp>)
#include <opencv2/core/version.hpp>
#include <opencv2/core.hpp>
#define FINS_HAS_OPENCV 1
#endif

#if __has_include(<pcl/pcl_config.h>)
#include <pcl/pcl_config.h>
#include <pcl/point_cloud.h>
#define FINS_HAS_PCL 1
#endif

namespace fins::util {

  template<typename T, typename Enable = void>
  struct ABITag {
    static constexpr uint32_t value() { return 0; }
  };

#ifdef FINS_HAS_OPENCV
  // 特化：持有 OpenCV 的 ABI 版本标签[cite: 1]
  template<>
  struct ABITag<cv::Mat> {
    static constexpr uint32_t value() {
      return CV_VERSION_MAJOR * 10000 + CV_VERSION_MINOR * 100 + CV_VERSION_REVISION;
    }
  };
#endif

#ifdef FINS_HAS_PCL
  // 特化：持有 PCL 点云容器的 ABI 版本标签[cite: 1]
  template<typename PointT>
  struct ABITag<pcl::PointCloud<PointT>> {
    static constexpr uint32_t value() {
      return PCL_MAJOR_VERSION * 10000 + PCL_MINOR_VERSION * 100 + PCL_REVISION_VERSION;
    }
  };

  template<typename PointT>
  struct ABITag<std::shared_ptr<pcl::PointCloud<PointT>>> {
    static constexpr uint32_t value() {
      return PCL_MAJOR_VERSION * 10000 + PCL_MINOR_VERSION * 100 + PCL_REVISION_VERSION;
    }
  };
#endif

#ifdef FINS_HAS_ROS_TRAITS
  // 特化：利用 rosidl 判定是否为 ROS 消息，并持久化其生态版本[cite: 1]
  template<typename T, typename = void>
  struct is_ros_msg : std::false_type {};

  template<typename T>
  struct is_ros_msg<T, typename std::enable_if<rosidl_generator_traits::is_message<T>::value>::type> : std::true_type {};

  template<typename T>
  struct ABITag<T, typename std::enable_if<is_ros_msg<T>::value>::type> {
    static constexpr uint32_t value() {
#ifdef RCLCPP_VERSION_MAJOR
      return RCLCPP_VERSION_MAJOR * 10000 + RCLCPP_VERSION_MINOR * 100 + RCLCPP_VERSION_PATCH;
#else
      return 0;
#endif
    }
  };
#endif

} // namespace fins