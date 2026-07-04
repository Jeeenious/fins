# ~/.fins/sdk/cmake/FineVision-extras.cmake

if(NOT COMMAND fins_add_node)
    macro(fins_add_node _target)
        add_library(${_target} SHARED ${ARGN})
        
        if(TARGET FineVision::fins_sdk)
            target_link_libraries(${_target} PRIVATE FineVision::fins_sdk)
        elseif(TARGET fins_sdk)
            target_link_libraries(${_target} PRIVATE fins_sdk)
        endif()
        
        target_link_libraries(${_target} PRIVATE Threads::Threads ${CMAKE_DL_LIBS})
        target_compile_definitions(${_target} PRIVATE FMT_HEADER_ONLY FINS_NODE)
        
        if(NOT DEFINED FINS_META_SOURCE)
            set(FINS_META_SOURCE "workspace")
        endif()
        if(NOT DEFINED FINS_META_NAME)
            set(FINS_META_NAME "${PROJECT_NAME}")
        endif()
        
        add_compile_definitions(PKG_NAME="${FINS_META_NAME}")
        add_compile_definitions(PKG_SOURCE="${FINS_META_SOURCE}")

        set_target_properties(${_target} PROPERTIES 
            OUTPUT_NAME "${FINS_META_SOURCE}_${_target}"
            POSITION_INDEPENDENT_CODE ON
        )
        
        install(TARGETS ${_target}
            DESTINATION "$ENV{HOME}/.fins/install"
        )
    endmacro()
endif()

if(NOT COMMAND fins_link_ros_dependencies)
    function(fins_link_ros_dependencies target)
        foreach(pkg ${ARGN})
            find_package(${pkg} REQUIRED)
            message(STATUS "[FINS] Linking ${pkg} to ${target}")

            if(TARGET ${pkg}::${pkg})
                target_link_libraries(${target} PRIVATE ${pkg}::${pkg})
            elseif(TARGET ${pkg})
                target_link_libraries(${target} PRIVATE ${pkg})
            else()
                if(DEFINED ${pkg}_INCLUDE_DIRS)
                    target_include_directories(${target} PRIVATE ${${pkg}_INCLUDE_DIRS})
                elseif(DEFINED ${pkg}_INCLUDE_DIR)
                    target_include_directories(${target} PRIVATE ${${pkg}_INCLUDE_DIR})
                endif()

                if(DEFINED ${pkg}_LIBRARIES)
                    target_link_libraries(${target} PRIVATE ${${pkg}_LIBRARIES})
                elseif(DEFINED ${pkg}_LIBRARY)
                    target_link_libraries(${target} PRIVATE ${${pkg}_LIBRARY})
                endif()
            endif()
        endforeach()
    endfunction()
endif()

macro(fins_optional_ros_dependency target pkg_name)
    find_package(${pkg_name} QUIET)
    if(${pkg_name}_FOUND)
        message(STATUS ">> Optional Package '${pkg_name}': FOUND. Enabling...")
        fins_link_ros_dependencies(${target} ${pkg_name})
        string(TOUPPER ${pkg_name} _PKG_UPPER)
        target_compile_definitions(${target} PRIVATE WITH_${_PKG_UPPER})
    else()
        message(STATUS ">> Optional Package '${pkg_name}': NOT FOUND. Skipping.")
    endif()
endmacro()