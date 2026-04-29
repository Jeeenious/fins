/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/

// abi_traits.hpp

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

namespace fins {

  template<typename T, typename Enable = void>
  struct AbiTag {
    static constexpr uint32_t value() { return 0; }
  };

#ifdef FINS_HAS_OPENCV
  template<>
  struct AbiTag<cv::Mat> {
    static constexpr uint32_t value() {
      return CV_VERSION_MAJOR * 10000 + CV_VERSION_MINOR * 100 + CV_VERSION_REVISION;
    }
  };
#endif

#ifdef FINS_HAS_PCL
  template<typename PointT>
  struct AbiTag<pcl::PointCloud<PointT>> {
    static constexpr uint32_t value() {
      return PCL_MAJOR_VERSION * 10000 + PCL_MINOR_VERSION * 100 + PCL_REVISION_VERSION;
    }
  };

  template<typename PointT>
  struct AbiTag<std::shared_ptr<pcl::PointCloud<PointT>>> {
    static constexpr uint32_t value() {
      return PCL_MAJOR_VERSION * 10000 + PCL_MINOR_VERSION * 100 + PCL_REVISION_VERSION;
    }
  };
#endif

#ifdef FINS_HAS_ROS_TRAITS
  template<typename T, typename = void>
  struct is_ros_msg : std::false_type {};
  template<typename T>
  struct is_ros_msg<T, typename std::enable_if<rosidl_generator_traits::is_message<T>::value>::type> : std::true_type {
  };

  template<typename T>
  struct AbiTag<T, typename std::enable_if<is_ros_msg<T>::value>::type> {
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