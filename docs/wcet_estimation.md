# WCET 估计：基于执行历史统计

> 状态:已落地。`client` 注入 `wcet_updater` 槽;`rollover_hp` 每次超周期回绕时用该顶点
> (算法)最近 N 次 execute 耗时估计下一周期 wcet,覆盖图顶点的静态默认值。默认宏
> `FINS_CAL_WCET=0` 关闭,打开即自整定。算法在 `schedule/wcet_updater.hpp`,两个自包含函数。

## 1. 背景与目标

在 precedence graph 中,`wcet` 是顶点权,直接决定 makespan 估计、调度优先级与截止期检查。
配置给的 `wcet` 是**静态估计**(缺省 1ms),实际执行可能有偏差:估紧 → makespan 低估、
过载漏报;估松 → 容量浪费。

目标:用运行时观测到的 execute 历史动态估计 wcet,让下一周期的调度依据贴合实际。

## 2. 数据来源与单位

- `exec_us_hist_`:`TBBMap<deque<double>>`,键 = **算法键**(`NodeInfo.name`;同算法多实例/
  多节点归并到同一历史槽),值 = 最近 N 次 execute 耗时,单位 **us**(`record_exec` 用
  `steady_clock` 计 execute 前后,不含输入打包/输出路由)。
- 环形队列容量:`exec_hist_cap` 可配,未配置由 `record_exec` 兜底缺省 **100**(满丢最旧)。
- `Workload.wcet` 字段单位 **ms**。估计函数输出 ms(内部完成 µs→ms 换算 ÷1000)。

## 3. 两种估计方法

### 3.1 最高水位(High-Water-Mark)

```
wcet = max(hist) × (1 + margin)
```

- **安全**:不低于任何已观测峰值——对已见的最坏执行零低估。
- **缺点**:被单次尖峰(调度抖动 / 缓存冷 / 邻居抢占)整体抬高,偏保守。

### 3.2 p 分位数(统计估计)

```
wcet = q_p(hist) × (1 + margin)，q_p = 排序 + 线性插值分位数
```

- **抗尖峰**:忽略顶部 `(1−p)` 的稀有超时,用主体分布的分位 + 裕度兜住瞬态抖动。
- `p=0.99` 默认;更激进取 `0.95`,更保守逼近最高水位(`p≥1` 直接退化为最高水位)。
- 对尾部重分布更贴实际,是推荐的默认装配。

两者都通过 `margin` 放裕度,把"历史之外的不确定性"折算进去。

## 4. 本实现(`schedule/wcet_updater.hpp`)

独立头文件提供**两个自包含函数**(无复杂数据结构,仅标准库):

- `fins::sched::wcet_hwm(hist, margin=0.2)`——最高水位,`max(hist)·(1+margin)`
- `fins::sched::wcet_pquantile(hist, p=0.99, margin=0.2)`——p 分位 + 裕度,`p≥1` 退化到 hwm

签名对齐函数槽 `std::function<double(std::deque<double>)>`:入参为 execute 历史(us),
返回值为估计 wcet(ms)。

### 接线与开关

```cpp
// client.cpp main：
wcet_updater = [](std::deque<double> hist) {
  return fins::sched::wcet_pquantile(hist);   // 99% 分位 + 20% 裕度
};
```

- 开关:`#define FINS_CAL_WCET 1`(client.cpp 顶部;默认 0 关闭)
- 调用点:`g_state.hpp` `rollover_hp()` 中 `#if FINS_CAL_WCET` 分支 →
  `update_wcet_estimation()`——遍历图顶点,对有执行历史的普通 job 节点调
  `wcet_updater(该顶点算法历史)` 现算覆盖 `v.wcet`
- 历史键:`update_wcet_estimation` 按 `v.name`(算法键)查历史——与 `record_exec(info.name)`
  的键一致;`v.id = {name}:{k}` 带实例序号,不匹配

## 5. 说明与注意事项

- 只对有执行历史的普通 job 节点生效:`tp:` 时间点顶点无 job → 跳过;无历史顶点 → 保留建图
  期配置默认。
- 首周期无历史 → 保持配置默认,第二周期起才有估计值。
- **联动**:`FINS_CAL_WCET=1` 时 `v.wcet` 会被历史覆盖,直接影响 `FINS_CAL_MAKESPAN` 的
  makespan 估计(两者同图同权,开一个要考虑另一个)。默认保持 `0`,配置 wcet 为基准。
- `wcet_updater` 函数槽在 `FINS_CAL_WCET=0` 时不会被调用(编译期裁剪),注入无副作用。

## 6. 相关代码

- `schedule/wcet_updater.hpp`:`fins::sched::wcet_hwm()` + `wcet_pquantile()`(独立头文件)
- `example/client.cpp`:`#include "schedule/wcet_updater.hpp"` + `wcet_updater` 装配
- `include/g_state.hpp`:`wcet_updater` 槽、`exec_us_hist_/record_exec`、`update_wcet_estimation()`、
  `rollover_hp()` 的 `FINS_CAL_WCET` 分支
