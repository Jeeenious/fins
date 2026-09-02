/*******************************************************************************
 * plugins_pcl_features.cpp — PCL features 模块插件（configs-first）
 * 首批：法线估计 pcl_normal_xyz（输入 XYZ → 输出 PointCloud<pcl::Normal>）。
 * 单帧无状态，每帧重建 KdTree + 半径搜索计算法线/曲率。
 ******************************************************************************/
#include <pcl/pcl_config.h>
#include <pcl/point_cloud.h>
#include <pcl/point_types.h>

#include <pcl/features/normal_3d.h>
#include <pcl/search/kdtree.h>

#include "xmacro.hpp"

namespace {

// 法线估计：半径搜索，输出 Normal 点云（含 curvature）
void pcl_normal_xyz(double radius,
                    const pcl::PointCloud<pcl::PointXYZ> &in,
                    pcl::PointCloud<pcl::Normal> &out) {
  if (in.empty()) { out.width = 0; out.height = 0; out.points.clear(); return; }
  pcl::search::KdTree<pcl::PointXYZ>::Ptr tree(new pcl::search::KdTree<pcl::PointXYZ>);
  tree->setInputCloud(in.makeShared());
  pcl::NormalEstimation<pcl::PointXYZ, pcl::Normal> ne;
  ne.setInputCloud(in.makeShared());
  ne.setSearchMethod(tree);
  ne.setRadiusSearch(radius > 0 ? radius : 0.05);
  ne.compute(out);
}

}  // namespace

#define FINS_ALGO_LIST(F) \
  F(pcl_normal_xyz)

FINS_ALGO_EXPORT("1.0.0")
