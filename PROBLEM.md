# PROBLEM — WSL DrvFS 下运行中添加 .so 热加载不触发

> 状态：**搁置**（2026-08-28 决定暂不处理，备查）。重启时优先方案 A。
> 相关代码：`sdk/fins/plugin_loader.hpp`（PluginLoader）、`sdk/fins/utils/fs.hpp`（util::Guard 文件事件循环）。

## 问题现象

WSL（插件目录位于 Windows 盘 `/mnt/d`，走 DrvFS/9P）下，PluginLoader 对运行中新增/删除/修改 `.so` 的感知完全失效：

1. 空插件目录启动 → 发配置 → 日志 `deferred (missing algo)`（缺算法，apply 被推迟）。
2. `cp plugin.so` 进插件目录 → **无任何 `[PluginLoader]` 热加载日志、不自动 apply**，必须重发配置才能生效。
3. 预放插件正常（启动时初始扫描用 `fs::directory_iterator`，DrvFS 可用）——所以"预先放 plugins 正常、临时增加 so 不行"。

## 根因

- WSL 的 inotify **只在 ext4 挂载（/home、/tmp、/root）上工作**；`/mnt/d`（Windows 盘）是 DrvFS/9P 协议，`inotify_add_watch` 返回 -1，**文件事件永不触发**。
- PluginLoader 目前唯一依赖 `util::Guard`（utils/fs.hpp）的 inotify 事件循环监听插件目录 → DrvFS 下事件根本到不了，装载永不发生。
- 关键：坏掉的只是"文件变了"的**通知通道**。`dlopen` 装载 `.so` 在 DrvFS 上完全正常，配置 unregister/deferred 只是没等到热加载。

## 候选方案（2026-08-28 讨论，全部未实现）

### 方案 A：WSL 轮询 + 目录 mtime 门（最小改动，推荐）

- 每 500ms 只 `stat` 插件目录**自身**的 mtime；没变 → 零额外调用直接睡；变了 → 全扫子文件做 diff。
- add/delete/rename 都会改目录 mtime → 可捕获；"原地覆盖已有文件"不改目录 mtime → 靠每 N tick（如 5s）补一次全扫兜底 modify。
- 插件目录仅几个 `.so` 时成本可忽略（每次全扫 <1ms）；感知延迟 ≤1 poll 周期（500ms）。
- 改动全在 `plugin_loader.hpp`，复用现有 `on_library_add/modify/delete` 回调链，装配点（client.cpp）不动。

### 方案 B：WSL 起 Windows 侧 ReadDirectoryChangesW 帮手进程（真·事件驱动）

- WSL interop 拉起一个常驻 Windows exe（`ReadDirectoryChangesW` 监听 `D:\...`，事件打 stdout），Linux 进程读管道 → 事件到达即触发回调。
- 零采样延迟，但引入外部进程管理 + `/mnt/d`↔`D:\` 路径映射 + 生命周期/崩溃恢复复杂度；对本用例（500ms 延迟无感）边际收益≈0。

### 方案 C：原生 Windows 构建

- 程序整体跑 Windows，`util::Guard` 的 `_WIN32` 分支（ReadDirectoryChangesW）直接生效。
- 实为**平台移植**而非改监听：插件 ABI 是 `dlopen/.so`，Windows 要 `LoadLibrary/.dll`，测试环境整个从 WSL 迁走。不属此问题范畴。

## 背景：Windows 原生监听机制

Windows 有原生文件监听 `ReadDirectoryChangesW`，`util::Guard` 的 `_WIN32` 分支（utils/fs.hpp:318-394）已实现，把 `FILE_ACTION_ADDED`/`REMOVED`/`MODIFIED` 映射到 `on_add`/`on_delete`/`on_modify`，**纯事件驱动、零扫描**。但 WSL 下程序是 Linux 二进制，编译只会走 `__linux__`（inotify）分支、调不了 Win32 API，故该段为死代码。

## 关联修复（ext4 下有效，本次不动）

- `client.cpp` main 循环 `retry_missing` 缺算法自动重试（含 DoubleBuff 空缓冲坑：无新写入时重复 commit 会切回空缓冲 → 静默建 0 顶点，重试用独立标志区分、不重复 commit）。
- `plugin_loader.hpp` on_library_add 半成品 200ms 延迟重试（已被用户回退，当前磁盘为 204 行版本，无 poll）。

这两层都依赖文件事件能到达，DrvFS 下事件不触发故不够，需要方案 A/B/C 其一补通知通道。
