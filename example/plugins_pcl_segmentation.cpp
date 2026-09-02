/*******************************************************************************
 * plugins_pcl_segmentation.cpp — PCL segmentation 模块插件（configs-first）
 * 单帧分割子集：
 *   pcl_plane_keep_xyz        RANSAC 平面 → 保留平面点（→XYZ）
 *   pcl_plane_remove_xyz      RANSAC 平面 → 移除平面点、留其余（→XYZ）
 *   pcl_cluster_largest_xyz   欧式聚类 → 只保留最大团（去噪/小团）（→XYZ）
 *   pcl_cluster_color_xyzrgb  欧式聚类 → 每团一种颜色（→XYZRGB）
 ******************************************************************************/
#include <pcl/pcl_config.h>
#include <pcl/point_cloud.h>
#include <pcl/point_types.h>
#include <pcl/PointIndices.h>

#include <pcl/segmentation/sac_segmentation.h>
#include <pcl/segmentation/extract_clusters.h>
#include <pcl/filters/extract_indices.h>
#include <pcl/search/kdtree.h>

#include "xmacro.hpp"

namespace {

// RANSAC 拟合平面并按 neg 取舍输出（inliers → 保平面；remove → 保非平面）
void plane_select(double dist_thresh, bool remove,
                  const pcl::PointCloud<pcl::PointXYZ> &in,
                  pcl::PointCloud<pcl::PointXYZ> &out) {
  if (in.empty()) { out.width = 0; out.height = 0; out.points.clear(); return; }
  pcl::SACSegmentation<pcl::PointXYZ> seg;
  seg.setOptimizeCoefficients(true);
  seg.setModelType(pcl::SACMODEL_PLANE);
  seg.setMethodType(pcl::SAC_RANSAC);
  seg.setDistanceThreshold(dist_thresh > 0 ? dist_thresh : 0.02);
  seg.setMaxIterations(200);
  seg.setInputCloud(in.makeShared());

  pcl::PointIndices::Ptr inliers(new pcl::PointIndices);
  pcl::ModelCoefficients::Ptr coeff(new pcl::ModelCoefficients);
  seg.segment(*inliers, *coeff);

  pcl::ExtractIndices<pcl::PointXYZ> ex;
  ex.setInputCloud(in.makeShared());
  ex.setIndices(inliers);
  ex.setNegative(remove);   // keep=true→平面点；remove=true→非平面点
  ex.filter(out);
}

// 欧式聚类 → cluster_indices（已按 min/max 尺寸过滤）
void cluster_run(double tol, int mn, int mx,
                 const pcl::PointCloud<pcl::PointXYZ> &in,
                 std::vector<pcl::PointIndices> &cluster_indices) {
  cluster_indices.clear();
  if (in.empty()) return;
  pcl::search::KdTree<pcl::PointXYZ>::Ptr tree(new pcl::search::KdTree<pcl::PointXYZ>);
  tree->setInputCloud(in.makeShared());
  pcl::EuclideanClusterExtraction<pcl::PointXYZ> ec;
  ec.setClusterTolerance(tol > 0 ? tol : 0.05);
  ec.setMinClusterSize(mn > 0 ? mn : 1);
  ec.setMaxClusterSize(mx > 0 ? mx : 1000000);
  ec.setSearchMethod(tree);
  ec.setInputCloud(in.makeShared());
  ec.extract(cluster_indices);
}

// 保平面（inliers）
void pcl_plane_keep_xyz(double dist_thresh,
                        const pcl::PointCloud<pcl::PointXYZ> &in,
                        pcl::PointCloud<pcl::PointXYZ> &out) {
  plane_select(dist_thresh, false, in, out);
}

// 去平面（非平面剩余）
void pcl_plane_remove_xyz(double dist_thresh,
                          const pcl::PointCloud<pcl::PointXYZ> &in,
                          pcl::PointCloud<pcl::PointXYZ> &out) {
  plane_select(dist_thresh, true, in, out);
}

// 只保留最大团
void pcl_cluster_largest_xyz(double tol, int mn, int mx,
                             const pcl::PointCloud<pcl::PointXYZ> &in,
                             pcl::PointCloud<pcl::PointXYZ> &out) {
  std::vector<pcl::PointIndices> clusters;
  cluster_run(tol, mn, mx, in, clusters);
  out.width = 0; out.height = 0; out.points.clear();
  if (clusters.empty()) return;
  const auto *big = &clusters[0];
  for (const auto &c : clusters) if (c.indices.size() > big->indices.size()) big = &c;
  out.points.reserve(big->indices.size());
  for (size_t i : big->indices) out.points.push_back(in.points[i]);
  out.width = out.points.size(); out.height = 1; out.is_dense = in.is_dense;
}

// 每团一种颜色 → XYZRGB
void pcl_cluster_color_xyzrgb(double tol, int mn, int mx,
                              const pcl::PointCloud<pcl::PointXYZ> &in,
                              pcl::PointCloud<pcl::PointXYZRGB> &out) {
  static const std::uint8_t kPalette[6][3] = {
      {255, 0, 0}, {0, 255, 0}, {0, 0, 255},
      {255, 255, 0}, {255, 0, 255}, {0, 255, 255}};
  std::vector<pcl::PointIndices> clusters;
  cluster_run(tol, mn, mx, in, clusters);
  out.points.clear();
  for (size_t ci = 0; ci < clusters.size(); ++ci) {
    const auto &c = clusters[ci];
    const auto &col = kPalette[ci % 6];
    for (size_t i : c.indices) {
      pcl::PointXYZRGB p;
      p.x = in.points[i].x; p.y = in.points[i].y; p.z = in.points[i].z;
      p.r = col[0]; p.g = col[1]; p.b = col[2];
      out.points.push_back(p);
    }
  }
  out.width = out.points.size(); out.height = 1; out.is_dense = true;
}

}  // namespace

#define FINS_ALGO_LIST(F) \
  F(pcl_plane_keep_xyz)       \
  F(pcl_plane_remove_xyz)     \
  F(pcl_cluster_largest_xyz)  \
  F(pcl_cluster_color_xyzrgb)

FINS_ALGO_EXPORT("1.0.0")
