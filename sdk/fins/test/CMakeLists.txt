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

# SDK 根目录 (test/../..)
set(SDK_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../..")

include_directories(
    ${SDK_ROOT}/fins
    ${SDK_ROOT}/fins/utils
    ${SDK_ROOT}/fins/third_party
)

find_package(Threads REQUIRED)

# =================================================================
# 2. 用户算法插件（单个 .so 装全部算法）
#    依赖 plugins.cpp（6 个 usr_* 算法 + FINS_ALGO_EXPORT 导出 C 工厂）；
#    一个 .so 即含全部算法（loaded_keys = usr_*:1.0.0 × 6），expand_hp 遍历
#    library_g.so_ctx "命中任一即可"——无需多份内容相同的 .so（PLUGIN_ID 未使用）
# =================================================================
if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/plugins.cpp)
    add_library(plugin SHARED plugins.cpp)
    target_include_directories(plugin PRIVATE
        ${SDK_ROOT}/fins
        ${SDK_ROOT}/fins/utils
        ${SDK_ROOT}/fins/third_party
    )
    set_target_properties(plugin PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/plugins"
        PREFIX ""
    )
endif()

# =================================================================
# 8. 功能测试主程序 + 配置客户端（手动运行，不 POST_BUILD 自动跑）
#    client [rpc_port] [plugin_dir] — 装配 RPC/loader/pool + 持续调度循环，收外部 JSON
#    server [interval|cfg.json] [port] — 轮换发配置 / 单次发配置
#    算法插件 = plugins/plugin.so（含 usr_* 全部算法；plugin_dir 指向该目录）
# =================================================================
add_executable(client client.cpp)
target_include_directories(client PRIVATE
    ${SDK_ROOT}/fins
    ${SDK_ROOT}/fins/utils
    ${SDK_ROOT}/fins/third_party
)
target_link_libraries(client PRIVATE TBB::tbb dl fmt::fmt Threads::Threads)

add_executable(server server.cpp)
target_include_directories(server PRIVATE
    ${SDK_ROOT}/fins
    ${SDK_ROOT}/fins/utils
    ${SDK_ROOT}/fins/third_party
)
target_link_libraries(server PRIVATE TBB::tbb dl fmt::fmt Threads::Threads)
