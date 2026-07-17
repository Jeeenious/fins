cmake_minimum_required(VERSION 3.14)
project(FinsAlgoTest VERSION 1.0.0 LANGUAGES CXX)

# =================================================================
# 1. 编译选项配置（核心：必须开启 C++17）
# =================================================================
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# 开启常见警告，方便在线下排查元编程时的潜在隐患
if(MSVC)
    add_compile_options(/W4)
else()
    add_compile_options(-Wall -Wextra -Wpedantic)
endif()

find_package(TBB REQUIRED)

# =================================================================
# 2. 头文件与依赖库路径映射
# =================================================================
# 自动把根目录、utils 目录以及 third_party 包含进来
# 这样你的代码里写 #include "third_party/json.hpp" 就能完美找到
include_directories(
        ..
        ../utils
        ../third_party
)

# =================================================================
# 3. 如果你的项目用到了 TBB（因为代码里有 fins::util::TBBMap）
#    取消下面的注释可以全自动寻找并链接系统中的 TBB 库
# =================================================================
# find_package(TBB REQUIRED)

# =================================================================
# 4. 构建测试可执行程序
# =================================================================
# 假设你的文件结构长这样：
# ├── CMakeLists.txt
# ├── algo_base.hpp
# ├── algo_func.hpp
# ├── rt_message.hpp  (存放 Message 和 MsgBundle 结构体的头文件)
# └── test_algo_func.cpp (刚才写的那段单元测试代码)

add_executable(test_algo_pipeline
        test_algo_func.cpp
)

# 如果开启了上面的 TBB，记得把 TBB 链接给你的测试程序
 target_link_libraries(test_algo_pipeline PRIVATE TBB::tbb)

# =================================================================
# 5. 编译完成后自动运行测试（可选，跑起来更爽）
# =================================================================
add_custom_command(TARGET test_algo_pipeline POST_BUILD
        COMMAND $<TARGET_FILE:test_algo_pipeline>
        COMMENT "Running AlgoFunc automated pipeline test case..."
)