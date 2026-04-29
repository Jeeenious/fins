set(CMAKE_SYSTEM_NAME Linux)
set(CMAKE_SYSTEM_PROCESSOR arm)

set(SDK_PATH "/home/fins/Workspace")

set(TOOLCHAIN_BIN "${SDK_PATH}/buildroot/output/atk_dlrk3506/host/bin")

set(CMAKE_C_COMPILER   "${TOOLCHAIN_BIN}/arm-buildroot-linux-gnueabihf-gcc")
set(CMAKE_CXX_COMPILER "${TOOLCHAIN_BIN}/arm-buildroot-linux-gnueabihf-g++")

set(CMAKE_SYSROOT "${SDK_PATH}/buildroot/output/atk_dlrk3506/host/arm-buildroot-linux-gnueabihf/sysroot")

set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} --sysroot=${CMAKE_SYSROOT}")

set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_PACKAGE ONLY)