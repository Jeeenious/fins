/*******************************************************************************
 * plugins_pcl_filters.cpp — PCL filters 模块插件（configs-first，pcl::PointCloud 载荷）
 * 过滤子集（每算法按点类型各展开一组，_xyz 与 _xyzrgb）：
 *   passthrough / voxelgrid / statistical_outlier / radius_outlier
 * 统一签名：配置段 → 输入点云(const ref) → 输出点云(ref)。单帧无状态。
 ******************************************************************************/
#include <pcl/pcl_config.h>
#include <pcl/point_cloud.h>
#include <pcl/point_types.h>

#include <pcl/filters/passthrough.h>
#include <pcl/filters/voxel_grid.h>
#include <pcl/filters/statistical_outlier_removal.h>
#include <pcl/filters/radius_outlier_removal.h>

#include "xmacro.hpp"

namespace {

template <typename P>
void impl_passthrough(const std::string &axis, double mn, double mx,
                      const pcl::PointCloud<P> &in, pcl::PointCloud<P> &out) {
  if (axis != "x" && axis != "y" && axis != "z") { out = in; return; }
  pcl::PassThrough<P> f;
  f.setInputCloud(in.makeShared());
  f.setFilterFieldName(axis);
  f.setFilterLimits(mn, mx);
  f.filter(out);
}

template <typename P>
void impl_voxel(double lx, double ly, double lz,
                const pcl::PointCloud<P> &in, pcl::PointCloud<P> &out) {
  pcl::VoxelGrid<P> f;
  f.setInputCloud(in.makeShared());
  f.setLeafSize(lx > 0 ? lx : 0.01, ly > 0 ? ly : 0.01, lz > 0 ? lz : 0.01);
  f.filter(out);
}

template <typename P>
void impl_stat(int mean_k, double stddev,
               const pcl::PointCloud<P> &in, pcl::PointCloud<P> &out) {
  pcl::StatisticalOutlierRemoval<P> f;
  f.setInputCloud(in.makeShared());
  f.setMeanK(mean_k > 0 ? mean_k : 20);
  f.setStddevMulThresh(stddev > 0 ? stddev : 1.0);
  f.filter(out);
}

template <typename P>
void impl_radius(double radius, int min_nbr,
                 const pcl::PointCloud<P> &in, pcl::PointCloud<P> &out) {
  pcl::RadiusOutlierRemoval<P> f;
  f.setInputCloud(in.makeShared());
  f.setRadiusSearch(radius > 0 ? radius : 0.1);
  f.setMinNeighborsInRadius(min_nbr > 0 ? min_nbr : 10);
  f.filter(out);
}

// ── 按点类型包装（模板实现 → 具体导出函数）──

void pcl_passthrough_xyz(const std::string &axis, double mn, double mx,
                         const pcl::PointCloud<pcl::PointXYZ> &in,
                         pcl::PointCloud<pcl::PointXYZ> &out) {
  impl_passthrough<pcl::PointXYZ>(axis, mn, mx, in, out);
}
void pcl_passthrough_xyzrgb(const std::string &axis, double mn, double mx,
                            const pcl::PointCloud<pcl::PointXYZRGB> &in,
                            pcl::PointCloud<pcl::PointXYZRGB> &out) {
  impl_passthrough<pcl::PointXYZRGB>(axis, mn, mx, in, out);
}
void pcl_voxel_xyz(double lx, double ly, double lz,
                   const pcl::PointCloud<pcl::PointXYZ> &in,
                   pcl::PointCloud<pcl::PointXYZ> &out) {
  impl_voxel<pcl::PointXYZ>(lx, ly, lz, in, out);
}
void pcl_voxel_xyzrgb(double lx, double ly, double lz,
                      const pcl::PointCloud<pcl::PointXYZRGB> &in,
                      pcl::PointCloud<pcl::PointXYZRGB> &out) {
  impl_voxel<pcl::PointXYZRGB>(lx, ly, lz, in, out);
}
void pcl_stat_outlier_xyz(int mean_k, double stddev,
                          const pcl::PointCloud<pcl::PointXYZ> &in,
                          pcl::PointCloud<pcl::PointXYZ> &out) {
  impl_stat<pcl::PointXYZ>(mean_k, stddev, in, out);
}
void pcl_stat_outlier_xyzrgb(int mean_k, double stddev,
                             const pcl::PointCloud<pcl::PointXYZRGB> &in,
                             pcl::PointCloud<pcl::PointXYZRGB> &out) {
  impl_stat<pcl::PointXYZRGB>(mean_k, stddev, in, out);
}
void pcl_radius_outlier_xyz(double radius, int min_nbr,
                            const pcl::PointCloud<pcl::PointXYZ> &in,
                            pcl::PointCloud<pcl::PointXYZ> &out) {
  impl_radius<pcl::PointXYZ>(radius, min_nbr, in, out);
}
void pcl_radius_outlier_xyzrgb(double radius, int min_nbr,
                               const pcl::PointCloud<pcl::PointXYZRGB> &in,
                               pcl::PointCloud<pcl::PointXYZRGB> &out) {
  impl_radius<pcl::PointXYZRGB>(radius, min_nbr, in, out);
}

}  // namespace

#define FINS_ALGO_LIST(F) \
  F(pcl_passthrough_xyz)       \
  F(pcl_passthrough_xyzrgb)    \
  F(pcl_voxel_xyz)             \
  F(pcl_voxel_xyzrgb)          \
  F(pcl_stat_outlier_xyz)      \
  F(pcl_stat_outlier_xyzrgb)   \
  F(pcl_radius_outlier_xyz)    \
  F(pcl_radius_outlier_xyzrgb)

FINS_ALGO_EXPORT("1.0.0")
