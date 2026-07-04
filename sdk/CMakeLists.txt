# sdk/CMakeLists.txt

cmake_minimum_required(VERSION 3.16)
project(finevision VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_VISIBILITY_INLINES_HIDDEN 1)

find_package(ament_cmake QUIET)

file(GLOB_RECURSE raw_sources CONFIGURE_DEPENDS "*.cpp")

set(SDK_SOURCES "")
foreach(src ${raw_sources})
    if(NOT src MATCHES "/build/" AND 
       NOT src MATCHES "agent.cpp" AND 
       NOT src MATCHES "inspect.cpp")
        list(APPEND SDK_SOURCES ${src})
    endif()
endforeach()

add_library(fins_sdk STATIC ${SDK_SOURCES})
set_target_properties(fins_sdk PROPERTIES POSITION_INDEPENDENT_CODE ON)

target_compile_definitions(fins_sdk PUBLIC FMT_HEADER_ONLY)
target_include_directories(fins_sdk PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/fins/third_party/fmt/include>
    $<INSTALL_INTERFACE:include>
)

if (FINS_PLATFORM STREQUAL "ARM")
    message(STATUS "[Config] ARM Platform detected. Integrating Zenoh-C SHARED library (.so).")
    set(ZENOH_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/fins/third_party/zenoh-c")
    add_library(zenoh_shared SHARED IMPORTED)
    set_target_properties(zenoh_shared PROPERTIES
        IMPORTED_LOCATION "${ZENOH_ROOT}/lib/libzenohc.so"
        INTERFACE_INCLUDE_DIRECTORIES "${ZENOH_ROOT}/include"
    )
    set_target_properties(zenoh_shared PROPERTIES
        INTERFACE_LINK_LIBRARIES "pthread;dl;m;rt"
    )
    target_link_libraries(fins_sdk PUBLIC zenoh_shared)
    target_compile_definitions(fins_sdk PUBLIC FINS_HAS_ZENOH)
endif()

if(ament_cmake_FOUND)
    install(TARGETS fins_sdk EXPORT fins_sdk
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib
        RUNTIME DESTINATION bin
    )
    install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/fins/" 
        DESTINATION include/fins
        FILES_MATCHING PATTERN "*.h" PATTERN "*.hpp"
    )
    ament_register_extension("ament_cmake" "fins" "cmake/fins-extras.cmake")
    
    ament_export_targets(fins_sdk)
    ament_export_dependencies(Threads)
    ament_package()
else()
    install(TARGETS fins_sdk 
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib
    )
    install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/fins/" 
        DESTINATION include/fins
        FILES_MATCHING PATTERN "*.h" PATTERN "*.hpp"
    )
endif()