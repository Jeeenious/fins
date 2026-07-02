# FineVision-SDK

Follow the instructions in https://iwin-fins.feishu.cn/wiki/MptPw6q7ciBWeTkAeoNcMVQCnRc for installation and usage.

Other components for FineVision:

- [FineVision-CLI](https://github.com/Han-Yu-Meng/FineVision-CLI): A command-line interface for building and managing robotics software packages.

- [FineVision-Studio](https://github.com/Han-Yu-Meng/FineVision-Studio): Visual node editor for designing and debugging robotics applications.

- [FineVision-Source](https://github.com/Han-Yu-Meng/FineVision-Source): Software source repositories including drivers and robotics algorithms.

---

## 项目架构总览

FineVision-SDK 是一个基于 C++20 的模块化、插件化机器人软件框架，由上海交通大学 IWIN-FINS 实验室开发。它采用 **数据流编程模型（Dataflow Programming Model）**，应用程序由互联的"节点"（Node）和"管道"（Pipe）组成，通过类型化的消息进行通信。

### 架构分层

```
┌─────────────────────────────────────────────────────┐
│  Agent (运行时 HTTP 服务器)                          │
│  agent/server.hpp, agent/agent.cpp                  │
├─────────────────────────────────────────────────────┤
│  插件系统 & 图管理 (nodelib.hpp)                     │
│  dlopen 加载 .so, JSON 数据流图解析, 热重载          │
├─────────────────────────────────────────────────────┤
│  跨节点通信                                          │
│  Service (RPC), Action (长时间任务)                  │
├─────────────────────────────────────────────────────┤
│  执行引擎                                            │
│  Studio (图生命周期), Step (调度), ThreadManager     │
├─────────────────────────────────────────────────────┤
│  节点抽象                                            │
│  Node, FunctionalNode, NodeFactory                   │
├─────────────────────────────────────────────────────┤
│  消息传递                                            │
│  AnyMsg/Msg<T>, Pipe, SPSCQueue (无锁队列)           │
├─────────────────────────────────────────────────────┤
│  基础设施                                            │
│  日志, 时间, 类型注册, 性能监控, 系统监控             │
└─────────────────────────────────────────────────────┘
```

### 关键设计模式

- **单例模式**: `Studio`、`PipeFactory`、`ThreadManager`、`ServiceManager`、`ActionManager`、`ParameterServer`、`PerformanceMonitor`、`TypeRegister` 均通过 `get_instance()` 访问，并暴露为全局宏（如 `FINS_STUDIO`、`FINS_PIPE_FACTORY` 等）
- **插件系统**: 用户提供的 `.so` 文件导出 C 链接函数，`NodeLib` 通过 `dlopen`/`dlsym` 发现和加载节点，`EXPORT_NODE(ClassName)` 宏自动注册节点
- **类型擦除消息**: `AnyMsg` 携带 `shared_ptr<void>` + `type_index` 实现运行时类型检查，`Msg<T>` 提供静态类型包装
- **两种调度模式**: Light 调度器（直接内联处理，适用于 ARM）和 Hybrid 调度器（线程占用率超过 85% 时自动升级到线程池）
- **热重载**: `NodeLib` 使用 `inotify` 监控插件目录，检测 `.so` 变化后自动停止、卸载、重新加载节点

---

## 目录结构

```
FineVision/
├── .clang-format               # C++ 代码风格配置
├── .gitignore                  # Git 忽略规则
├── CMakeLists.txt              # 根 CMake 构建文件
├── CMakePresets.json           # CMake 预设 (debug/release)
├── LICENSE                     # Mozilla Public License 2.0
├── README.md                   # 项目说明
├── toolchain/
│   └── rk3506.cmake            # RK3506 ARM 交叉编译工具链
├── sdk/                        # SDK 核心库
│   ├── CMakeLists.txt          # SDK 构建 (静态库 fins_sdk + 动态库 fins_shared)
│   └── fins/
│       ├── macros.hpp          # 跨平台 DLL 导出/可见性宏
│       ├── version.hpp         # 版本号定义 (0.1.0)
│       ├── abi_traits.hpp      # OpenCV/PCL/ROS2 消息 ABI 标签
│       ├── msg.hpp             # 类型化消息 (AnyMsg, Msg<T>)
│       ├── spsc_queue.hpp      # 无锁 SPSC 环形缓冲区
│       ├── pipe.hpp            # 命名管道 + PipeFactory 单例
│       ├── step.hpp            # 执行步 (包装节点 + 输入/输出管道)
│       ├── studio.hpp          # Studio 单例 (管理执行图)
│       ├── node.hpp            # 节点抽象 (INode, Node, NodeFactory)
│       ├── functional_node.hpp # 函数式节点 (lambda 构建器)
│       ├── function_tags.hpp   # Input<T>/Output<T>/Parameter<T> 包装器
│       ├── nodelib.hpp         # 插件加载器, JSON 图构建器, 热重载
│       ├── node_log.hpp        # 节点日志缓冲区
│       ├── shared_states.hpp   # 全局状态枚举和原子变量
│       ├── thread_manager.hpp  # 固定大小线程池
│       ├── type/
│       │   ├── string_convert.hpp  # std::to_string 重载
│       │   ├── type_register.hpp   # 类型注册 + 名称查找
│       │   └── type_register.cpp   # 基础类型注册
│       ├── utils/
│       │   ├── fs.hpp              # 路径展开 (~ → $HOME)
│       │   ├── logger.hpp          # 全局线程安全彩色日志
│       │   ├── network.hpp         # get_local_ip() 网络工具
│       │   ├── performance_recorder.hpp # 性能监控 + RAII 计时
│       │   └── time.hpp            # AcqTime 纳秒精度时间
│       ├── action/
│       │   ├── action.hpp          # 总头文件 + 工具函数
│       │   ├── action_tags.hpp     # ActionState 枚举, Goal/Feedback 标签
│       │   ├── action_traits.hpp   # Action Traits 类型萃取
│       │   ├── action_session.hpp  # Action 会话 (状态机)
│       │   ├── action_manager.hpp  # Action 管理器单例
│       │   └── action_manager.cpp  # Action 管理器实现
│       ├── service/
│       │   ├── service_tags.hpp    # Request<T>/Response<T> 标签
│       │   ├── service_traits.hpp  # Service Traits 类型萃取
│       │   ├── service_manager.hpp # Service 管理器单例 (RPC)
│       │   └── service_manager.cpp # Service 管理器实现
│       ├── analysis/
│       │   ├── aoi_analyzer.hpp    # 信息年龄 (AoI) 管道指标
│       │   └── system_monitor.hpp  # CPU/温度/内存 系统监控
│       ├── agent/
│       │   ├── server.hpp          # Agent HTTP 服务器
│       │   ├── agent.cpp           # Agent CLI 入口 (独立可执行文件)
│       │   ├── parameter_server.hpp # YAML 参数服务器
│       │   └── parameter_server.cpp # 参数解析实现
│       ├── inspect/
│       │   └── inspect.cpp         # 插件检查 CLI 工具
│       └── third_party/
│           ├── json.hpp            # nlohmann/json 单头文件
│           ├── httplib.h           # cpp-httplib 单头文件
│           ├── fmt/                # {fmt} 库 (仅头文件模式)
│           └── zenoh-c/            # Eclipse Zenoh C 绑定 (仅 ARM)
└── examples/
    ├── CMakeLists.txt             # 示例构建
    ├── agent.cpp                  # 简单 Agent 示例
    ├── static_agent.cpp           # 静态构建 Agent 示例
    └── static_workspace/
        ├── hello_world.hpp        # HelloWorld 示例节点
        ├── serials.hpp            # Linux 串口封装
        └── zenoh_node.hpp         # Zenoh 发布者节点
```

---

## 文件功能详解

### 构建与配置

| 文件 | 功能说明 |
|------|---------|
| `CMakeLists.txt` | 根构建文件。要求 C++20、CMake ≥ 3.16。支持 x86-64（`-march=native`）和 ARM（`-mcpu=cortex-a7 -mfpu=neon-vfpv4`）。Release 模式启用 `-O3`、函数/数据段 GC、LTO。支持 `ENABLE_ASAN`（内存检测）、`ENABLE_STATIC_BUILD`（静态链接免 dlopen）。WSL 下自动清理 Windows 路径污染 |
| `CMakePresets.json` | CMake 预设配置，提供 Debug 和 Release 两种构建预设 |
| `.clang-format` | 基于 LLVM 风格，120 列宽度限制，自动整理 C++ 代码格式 |
| `toolchain/rk3506.cmake` | Rockchip RK3506 (ARM Cortex-A7) 交叉编译工具链，使用 Buildroot 工具链路径 |

### SDK 核心 (`sdk/fins/`)

#### 基础设施层

| 文件 | 功能说明 |
|------|---------|
| `macros.hpp` | 定义 `FINS_API` 宏，跨平台（Windows/Linux）DLL 导出和符号可见性控制。支持 MSVC 的 `__declspec` 和 GCC/Clang 的 `__visibility__` |
| `version.hpp` | `Version` 结构体，定义 SDK 版本号 (0.1.0)，提供 `to_json`/`from_json` 序列化 |
| `abi_traits.hpp` | ABI 兼容性标记系统。为 OpenCV `cv::Mat`、PCL `pcl::PointCloud`、ROS2 消息类型提供编译时 ABI 标签，确保插件与 SDK 之间的二进制兼容性 |
| `utils/fs.hpp` | 文件系统工具，提供 `expand_path()` 函数，将 `~` 展开为用户 HOME 目录 |
| `utils/logger.hpp` | 全局线程安全彩色日志器。支持 DEBUG/INFO/WARN/ERROR 四个级别。使用 `std::mutex` 保护输出，带时间戳和颜色标记。提供 `FINS_LOG_*` 全局宏方便调用 |
| `utils/time.hpp` | `AcqTime` 类型（基于 `std::chrono::nanoseconds` 纳秒精度时间点）。提供与 `std::chrono::system_clock` 互转、秒/毫秒/微秒/纳秒数值转换、ROS2 `builtin_interfaces::msg::Time` 互转 |
| `utils/network.hpp` | 网络工具，`get_local_ip()` 通过 `getifaddrs()` 获取本机非回环 IPv4 地址 |
| `utils/performance_recorder.hpp` | `PerformanceMonitor` 单例，批量记录消息处理性能数据（节点 ID、端口、时间戳、CPU 耗时、线程 ID）。`ScopedSegmentTimer` RAII 计时器自动记录代码段耗时。数据以 JSONL 格式写入 `~/.fins/performance/runtime_<timestamp>.jsonl` |
| `type/string_convert.hpp` | 为 `std::string` 和 `std::vector<T>` 提供 `std::to_string` 重载，支持参数值的字符串转换 |
| `type/type_register.hpp` / `.cpp` | 中心化类型注册表。`TypeRegister` 单例维护 C++ `type_index` ↔ 字符串名的双向映射，支持自定义类型转换器。预注册类型：`int`、`float`、`double`、`std::string`、`bool`、`std::vector<T>` |

#### 消息传递层

| 文件 | 功能说明 |
|------|---------|
| `spsc_queue.hpp` | 无锁有界 SPSC（单生产者单消费者）环形缓冲区 `SPSCQueue<T, Capacity>`。使用原子操作 + cache line padding 消除 false sharing，适用于高性能线程间数据传输 |
| `msg.hpp` | `AnyMsg` 类型擦除消息：携带 `shared_ptr<void>` 数据、`type_index` 类型信息、时间戳和 ABI 标签。`Msg<T>` 模板提供静态类型安全的消息包装。`MessagePack` 辅助类用于批量消息处理 |
| `pipe.hpp` | `Pipe` 类实现命名管道：生产者推送 `AnyMsg`，消费者通过条件变量阻塞等待。默认容量 32。`PipeFactory` 单例管理所有管道的创建和查找。内含 `AoIAnalyzer` 用于信息年龄指标分析 |

#### 节点层

| 文件 | 功能说明 |
|------|---------|
| `node.hpp` | **核心节点抽象**。`INode` 为纯虚接口：`define()`、`initialize()`、`run()`、`pause()`、`reset()`、`on_input()`、`update_parameter()`。`Node` 为具体实现，提供端口注册、参数管理、Service Client/Server、Action Commander/Actor 注册。`NodeFactory` 单例管理节点创建。提供 `EXPORT_NODE(ClassName)` 宏自动注册节点到工厂，`DEFINE_PLUGIN_ENTRY(state)` 和 `REGISTER_PLUGIN_INIT/DESTROY` 宏生成插件 C ABI 接口 |
| `node_log.hpp` | `NodeLogger` 为每个节点维护环形缓冲区日志（最多 200 条），记录时间戳、级别、消息、文件名和行号。通过 `get_logs()` 获取日志用于遥测上报 |
| `functional_node.hpp` | **函数式节点**。`GenericFunctionalNode` 允许通过 lambda/函数对象创建节点。`FunctionBuilder<Func>` 构建器模式，使用 `Input<T>`/`Output<T>`/`Parameter<T>` 包装器自动推导输入输出端口和参数。`Function()` 工厂函数简化创建流程 |
| `function_tags.hpp` | `Input<T>`（从 Msg 只读）、`Output<T>`（持有输出数据）、`Parameter<T>`（只读参数）包装器类。配套类型萃取模板：`is_input_v`、`is_output_v`、`is_parameter_v`、`remove_input_t`、`strip_function_params` 等 |

#### 执行引擎层

| 文件 | 功能说明 |
|------|---------|
| `step.hpp` | `Step` 包装一个 `INode` 与输入输出管道连接。为每个输入端口创建一个监听线程。支持 **Light 调度器**（直接内联处理消息）和 **Hybrid 调度器**（线程占用率超过 85% 阈值时自动升级到线程池） |
| `studio.hpp` | **执行图管理器**。`Studio` 单例管理所有节点和管道：拓扑排序、生命周期控制（`run()`/`pause()`/`reset()`/`clear()`）、管道连接时的类型检查。`FINS_STUDIO` 全局宏便捷访问 |
| `shared_states.hpp` | 全局原子状态变量：`Running_State`（PAUSE/RUN）、`Process_Strategy`（SERIAL/PARALLEL/POOL）、`Schedule_Strategy`（FCFS/BALANCE_FPS/BALANCE_DELAY） |
| `thread_manager.hpp` | `ThreadManager` 单例管理固定大小（默认 4 个 Worker）线程池。使用哈希分配 Step 到线程（亲和性绑定），`pthread_setaffinity_np` 绑核。每个 Worker 有 2048 容量的 LIFO/未来队列 |

#### 跨节点通信

| 文件 | 功能说明 |
|------|---------|
| `service/service_tags.hpp` | `Request<T...>` 和 `Response<T...>` 类型标签模板，用于标注 Service 的请求和响应类型 |
| `service/service_traits.hpp` | `ServiceTraits<Func>` 类型萃取：从函数签名中提取 `InputTuple`（请求类型元组）、`OutputTuple`（响应类型元组）、`ReturnType`。使用 SFINAE 匹配 `Request`/`Response` 标签 |
| `service/service_manager.hpp` / `.cpp` | **RPC 服务管理器**。`ServiceManager` 单例：注册服务端回调、入队客户端调用、异步 Worker 线程调度并完成 `std::promise`/`std::future`。通过 `type_index` 实现类型安全的服务路由 |
| `action/action_tags.hpp` | `ActionState` 枚举（Accepted/Executing/Succeeded/Canceled/Canceling/Aborted）。`Goal<T...>` 和 `Feedback<T...>` 类型标签模板 |
| `action/action_traits.hpp` | `ActionTraits<Func>` 类型萃取：从 Action 签名中提取 `GoalTuple` 和 `FeedbackTuple` |
| `action/action_session.hpp` | `ActionSessionBase`（抽象）和 `ActionSession<Goal, Feedback>`（模板化）实现 Action 状态机，支持目标接收、反馈发送和状态转换 |
| `action/action_manager.hpp` / `.cpp` | **长时间任务管理器**。`ActionManager` 单例：Commander/Actor 注册、异步目标队列、Worker 线程调度。`TypeErasedActionSession` 提供运行时类型擦除 |

#### 分析监控层

| 文件 | 功能说明 |
|------|---------|
| `analysis/aoi_analyzer.hpp` | **信息年龄 (AoI) 分析器**。在每个管道上追踪平均 AoI、峰值 AoI、系统延迟、违规概率和 FPS。使用 100 样本滑动窗口（1 秒窗口）的环形缓冲区 |
| `analysis/system_monitor.hpp` | **系统资源监控器**。读取 `/proc/stat`（CPU 使用率）、`/sys/class/thermal/thermal_zone0/temp`（温度）、`/proc/meminfo`（内存）。CPU 采样间隔 100ms |

#### 插件系统和图管理

| 文件 | 功能说明 |
|------|---------|
| `nodelib.hpp` | **插件系统和数据流图协调器**。`NodeLib` 整合所有子系统：通过 `dlopen` 加载 `.so` 插件，提取 C ABI 符号（`get_node_count`、`get_node_name`、`get_node_meta_json`、`create_node`、`destroy_node`），解析 JSON 数据流图规范，创建 `Step` 对象，构建依赖图，拓扑排序后按依赖顺序初始化。通过 `inotify` 监控插件目录实现热重载（无状态节点 `STATELESS` 标志） |

#### Agent 运行时

| 文件 | 功能说明 |
|------|---------|
| `agent/server.hpp` | **Agent HTTP 服务器**。基于 cpp-httplib，暴露 REST API：`POST /load_dataflow`（加载 JSON 图）、`POST /apply_parameters`（应用 YAML 参数）、`GET /get_status`（查询运行状态）、`POST /set_status`（设置运行状态）、`POST /reset`（重置）。向编排器注册自身并周期性发送遥测数据（CPU、内存、管道指标、节点日志） |
| `agent/agent.cpp` | **Agent CLI 入口**。解析命令行参数：线程数、插件目录、日志级别、WebUI URL、Agent 名称/IP/端口。启动 `NodeLib`、`AgentServer` 和 `FINS_PERF_MONITOR` |
| `agent/parameter_server.hpp` / `.cpp` | **参数服务器**。`ParameterServer` 单例解析 YAML 风格的参数文件（缩进语法、支持多行数组、注释）。提供模板特化的类型访问器。`ParamLoader` 类支持前缀化参数访问 |
| `inspect/inspect.cpp` | **插件检查 CLI 工具**。加载指定的 `.so` 插件文件，打印其中包含的节点元信息（名称、输入输出端口、参数等），用于调试和验证插件 |

#### 第三方库 (`sdk/fins/third_party/`)

| 文件/目录 | 功能说明 |
|-----------|---------|
| `json.hpp` | nlohmann/json v3.x 单头文件。用于所有 JSON 序列化：节点能力描述、数据流图、遥测数据、性能记录 |
| `httplib.h` | cpp-httplib 单头文件。用于 Agent HTTP 服务器/客户端通信 |
| `fmt/` | {fmt} 库 v10.x，仅头文件模式（`FMT_HEADER_ONLY`）。提供现代化 C++ 格式化输出 |
| `zenoh-c/` | Eclipse Zenoh C 绑定，仅 ARM 平台可用（通过 `FINS_HAS_ZENOH` 宏）。提供发布/订阅中间件的 C 头文件和 `libzenohc.so` 动态库 |

### 示例 (`examples/`)

| 文件 | 功能说明 |
|------|---------|
| `CMakeLists.txt` | 示例构建文件：支持 `agent`（动态加载插件）和 `static_agent`（静态编译节点）两种模式 |
| `agent.cpp` | 简单 Agent 示例。从 `~/.fins/install/` 加载所有 `.so` 插件，连接 `localhost:8080` 编排器，在本机 1896 端口启动 Agent HTTP 服务 |
| `static_agent.cpp` | 静态构建 Agent 示例。在编译时链接示例节点类（无需 `.so` 动态加载），使用 `FINS_STATIC_BUILD` 宏 |
| `static_workspace/hello_world.hpp` | 两个示例节点：`HelloWorldSource`（每 20ms 发布 "Hello, World!" 字符串到输出端口 0）和 `HelloWorldPrinter`（从输入端口 0 接收字符串，每 100 条消息计算平均端到端延迟） |
| `static_workspace/serials.hpp` | Linux 串口通信封装类 `SerialPort`。基于 POSIX termios API，支持打开、读取、配置波特率等串口参数 |
| `static_workspace/zenoh_node.hpp` | `ZenohPublisherNode` 示例节点。通过 Eclipse Zenoh 协议在可配置的 key expression 下发布递增计数器 |

---

## Agent HTTP API 端点

| 方法 | 路由 | 功能 |
|------|------|------|
| POST | `/load_dataflow` | 加载 JSON 格式的数据流图配置并部署执行 |
| POST | `/apply_parameters` | 将 YAML 参数应用到运行中的数据流图 |
| GET | `/get_status` | 查询当前运行状态，返回 `RUNNING` 或 `STOPPED` |
| POST | `/set_status` | 设置执行状态（`RUNNING` 或 `STOPPED`） |
| POST | `/reset` | 重置所有管道的消息队列和节点状态 |
| POST | `/register_agent` | Agent 向编排器（Orchestrator）注册 |
| POST | `/report_telemetry` | Agent 周期性上报遥测数据（系统监控 + 管道指标） |

## 构建配置选项

| CMake 选项 | 说明 |
|------------|------|
| `ENABLE_ASAN` | 启用 AddressSanitizer 内存错误检测 |
| `ENABLE_STATIC_BUILD` | 启用静态构建模式，直接链接节点类而非 dlopen 加载 `.so` |
| `SCHEDULER` | 调度策略选择：`AUTO`（x64=Hybrid, ARM=Light）、`LIGHT`、`THREAD_POOL` |