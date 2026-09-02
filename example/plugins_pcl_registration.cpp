/*******************************************************************************
 * plugins_pcl_registration.cpp — PCL registration 模块插件（configs-first）
 * 首批：ICP 精配准 pcl_icp_align_xyz（源 + 目标 两输入 → 对齐后的源 →XYZ）。
 * 单帧无状态；demo 用两个平移错开的平面点云验证收敛。
 ******************************************************************************/
#include <pcl/pcl_config.h>
#include <pcl/point_cloud.h>
#include <pcl/point_types.h>

#include <pcl/registration/icp.h>

#include "xmacro.hpp"

namespace {

// ICP：把 source 对齐到 target，输出对齐后的 source
void pcl_icp_align_xyz(double max_corr, int max_iter, double trans_eps,
                       const pcl::PointCloud<pcl::PointXYZ> &src,
                       const pcl::PointCloud<pcl::PointXYZ> &tgt,
                       pcl::PointCloud<pcl::PointXYZ> &out) {
  out.width = 0; out.height = 0; out.points.clear();
  if (src.empty() || tgt.empty()) return;
  pcl::IterativeClosestPoint<pcl::PointXYZ, pcl::PointXYZ> icp;
  icp.setInputSource(src.makeShared());
  icp.setInputTarget(tgt.makeShared());
  icp.setMaxCorrespondenceDistance(max_corr > 0 ? max_corr : 1.0);
  icp.setMaximumIterations(max_iter > 0 ? max_iter : 50);
  icp.setTransformationEpsilon(trans_eps > 0 ? trans_eps : 1e-6);
  icp.setEuclideanFitnessEpsilon(1e-5);
  icp.align(out);   // 是否收敛可查 icp.hasConverged()；未收敛也输出当前对齐结果
}

}  // namespace

#define FINS_ALGO_LIST(F) \
  F(pcl_icp_align_xyz)

FINS_ALGO_EXPORT("1.0.0")
