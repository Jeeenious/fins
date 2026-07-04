# CMakeLists.txt

cmake_minimum_required(VERSION 3.16)
project(finevision_sdk)

# Compile Options

option(ENABLE_ASAN "Enable AddressSanitizer" OFF) 
option(ENABLE_LTO "Enable Link Time Optimization" ON)
option(ENABLE_STATIC_BUILD "Enable static build of node library" OFF)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED YES)

if(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64" OR CMAKE_SYSTEM_PROCESSOR MATCHES "amd64")
    message(STATUS "Detected x64 architecture - Applying x64 optimizations")
    add_compile_options(-march=native)
    set(FINS_PLATFORM "x64")

elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "arm" OR CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64")
    message(STATUS "Detected ARM architecture - Applying ARM optimizations")
    add_compile_options(-mcpu=cortex-a7 -mfpu=neon-vfpv4 -mfloat-abi=hard)
    add_compile_definitions(FINS_ARM_ARCH)
    set(FINS_PLATFORM "ARM")
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Release")
    add_compile_options(-O3 -ffunction-sections -fdata-sections)
    
    if(NOT APPLE)
        add_link_options(-Wl,--gc-sections)
    endif()

    if(ENABLE_LTO)
        include(CheckIPOSupported)
        check_ipo_supported(RESULT supported OUTPUT error)
        if(supported)
            set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
            message(STATUS "LTO enabled")
        endif()
    endif()
endif()

if(ENABLE_ASAN)
    add_compile_options(-fsanitize=address -fno-omit-frame-pointer -g)
    add_link_options(-fsanitize=address)
endif()

if(ENABLE_STATIC_BUILD)
    add_compile_definitions(FINS_STATIC_BUILD)
    add_compile_definitions(PKG_SOURCE="static_workspace")
    message(STATUS "Static build enabled")
endif()

# Scheduler Policy Configuration

set(FINS_SCHED_POLICY "AUTO" CACHE STRING "Scheduler policy: AUTO, LIGHT, THREAD_POOL")
set_property(CACHE FINS_SCHED_POLICY PROPERTY STRINGS "AUTO" "LIGHT" "THREAD_POOL")

if(FINS_SCHED_POLICY STREQUAL "AUTO")
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "arm" OR CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64")
        set(USE_LIGHT_SCHED_DEFAULT ON)
        message(STATUS "Policy: AUTO (Detected ARM) -> Defaulting to LIGHT scheduler")
    else()
        set(USE_LIGHT_SCHED_DEFAULT OFF)
        message(STATUS "Policy: AUTO (Detected x64) -> Supporting Hybrid/Thread Pool scheduler")
    endif()
elseif(FINS_SCHED_POLICY STREQUAL "LIGHT")
    set(USE_LIGHT_SCHED_DEFAULT ON)
else()
    set(USE_LIGHT_SCHED_DEFAULT OFF)
endif()

if(USE_LIGHT_SCHED_DEFAULT)
    add_compile_definitions(FINS_LIGHT_SCHEDULER)
endif()

# WSL Environment Sanitization

if(EXISTS "/proc/sys/fs/binfmt_misc/WSLInterop")
    message(STATUS "WSL Environment Detected: Sanitizing environment...")

    set(_current_path "$ENV{PATH}")
    string(REPLACE ":" ";" _path_list "${_current_path}")
    set(_clean_path_list "")
    
    foreach(_p ${_path_list})
        if(NOT "${_p}" MATCHES "^/mnt/")
            list(APPEND _clean_path_list "${_p}")
        endif()
    endforeach()
    
    list(JOIN _clean_path_list ":" _clean_path)
    set(ENV{PATH} "${_clean_path}")
    message(STATUS "PATH sanitized: Windows paths removed from CMake visibility.")

    list(APPEND CMAKE_SYSTEM_IGNORE_PATH "/mnt/c" "/mnt/d" "/mnt/e" "/mnt/f" "/mnt/g" "/mnt")
    list(APPEND CMAKE_IGNORE_PATH "/mnt/c" "/mnt/d" "/mnt/e" "/mnt/f" "/mnt/g" "/mnt")
    list(APPEND CMAKE_IGNORE_PREFIX_PATH "/mnt/c" "/mnt/d" "/mnt/e" "/mnt/f" "/mnt/g" "/mnt")

    get_cmake_property(_cacheVars CACHE_VARIABLES)
    foreach(_var ${_cacheVars})
        get_property(_val CACHE ${_var} PROPERTY VALUE)
        if("${_val}" MATCHES "^/mnt/")
            if("${_var}" MATCHES "(DIR|LIBRARY|INCLUDE|FILE|PATH|PROGRAM)$")
                message(STATUS "🧹 Clearing polluted cache var: ${_var} = ${_val}")
                unset(${_var} CACHE)
            endif()
        endif()
    endforeach()
endif()

add_subdirectory(sdk)
add_subdirectory(examples)

if(ament_cmake_FOUND)
    ament_export_cmake_extra_files(cmake/finevision-extras.cmake)
    ament_package()
endif()
