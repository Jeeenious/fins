cmake_minimum_required(VERSION 3.14)
project(FinsAlgoTest VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(MSVC)
    add_compile_options(/W4)
else()
    add_compile_options(-Wall -Wextra -Wpedantic)
endif()

find_package(TBB REQUIRED)
find_package(fmt REQUIRED)
find_package(Threads REQUIRED)

# 头文件已从 sdk/fins/ 迁移到仓库根 include/，按新布局修正 include 目录：
#   ${SDK_ROOT}/include        头文件根（顶层 *.hpp、algo/、mesg/、utils/）
#   ${SDK_ROOT}/include/utils  utils 内裸名 include（logger.hpp / form.hpp）
#   ${SDK_ROOT}                仓库根（RPC_listener.hpp 等内 third_party/... 引用）
set(SDK_ROOT "${CMAKE_CURRENT_SOURCE_DIR}")
# 输出目录（现代变量；LIBRARY_OUTPUT_PATH/EXECUTABLE_OUTPUT_PATH 是遗留变量）：
#   client/server  → 仓库根 build/（run_client.sh / run_server.sh 默认查找仓库根 client / server）
#   plugin.so      → 仓库根 plugins/（client 默认 plugin_dir=./plugins 从这加载）
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}")
set(LIBRARY_OUTPUT_PATH "${CMAKE_CURRENT_SOURCE_DIR}/plugins")

include_directories(
    ${SDK_ROOT}/include
    ${SDK_ROOT}/include/utils
    ${SDK_ROOT}
)

# =================================================================
# 2. 用户算法插件（单个 .so 装全部算法）
#    依赖 plugins.cpp（6 个 usr_* 算法 + FINS_ALGO_EXPORT 导出 C 工厂）；
#    一个 .so 即含全部算法（loaded_keys = usr_*:1.0.0 × 6），expand_hp 遍历
#    library_g.so_ctx "命中任一即可"——无需多份内容相同的 .so（PLUGIN_ID 未使用）
# =================================================================
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/example/plugins.cpp")
    add_library(plugin SHARED example/plugins.cpp)
    target_include_directories(plugin PRIVATE
        ${SDK_ROOT}/include/utils
        ${SDK_ROOT}/include
        ${SDK_ROOT}
    )
    set_target_properties(plugin PROPERTIES PREFIX "")
endif()

# =================================================================
# 8. 功能测试主程序 + 配置客户端（手动运行，不 POST_BUILD 自动跑）
#    client [rpc_port] [plugin_dir] — 装配 RPC/loader/pool + 持续调度循环，收外部 JSON
#    server [interval|cfg.json] [port] — 轮换发配置 / 单次发配置
#    算法插件 = plugins/plugin.so（含 usr_* 全部算法；plugin_dir 指向该目录）
# =================================================================
add_executable(client example/client.cpp)
target_include_directories(client PRIVATE
    ${SDK_ROOT}/include
    ${SDK_ROOT}/include/utils
    ${SDK_ROOT}
)
target_link_libraries(client PRIVATE TBB::tbb dl fmt::fmt Threads::Threads)

add_executable(server example/server.cpp)
target_include_directories(server PRIVATE
    ${SDK_ROOT}/include
    ${SDK_ROOT}
)
target_link_libraries(server PRIVATE TBB::tbb dl fmt::fmt Threads::Threads)
