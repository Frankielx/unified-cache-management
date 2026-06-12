if(NOT RUNTIME_ENVIRONMENT STREQUAL "ascend")
    message(FATAL_ERROR "BUILD_UCM_ASU requires RUNTIME_ENVIRONMENT=ascend. Current value: ${RUNTIME_ENVIRONMENT}")
endif()

if(NOT DEFINED ASCEND_ROOT)
    set(ASCEND_ROOT "/usr/local/Ascend/ascend-toolkit/latest" CACHE PATH "Path to Ascend root directory")
endif()

find_path(ASU_ASCEND_INCLUDE_DIR
    NAMES acl/acl.h
    HINTS
        ${ASCEND_ROOT}/include
        ${ASCEND_ROOT}/aarch64-linux/include
        ${ASCEND_ROOT}/arm64-linux/include
    NO_DEFAULT_PATH
)
if(NOT ASU_ASCEND_INCLUDE_DIR)
    message(FATAL_ERROR "Cannot find acl/acl.h under ASCEND_ROOT=${ASCEND_ROOT}")
endif()

find_library(ASU_ASCENDCL_LIB
    NAMES ascendcl
    HINTS
        ${ASCEND_ROOT}/lib64
        ${ASCEND_ROOT}/aarch64-linux/lib64
    NO_DEFAULT_PATH
)
if(NOT ASU_ASCENDCL_LIB)
    message(FATAL_ERROR "Cannot find libascendcl under ASCEND_ROOT=${ASCEND_ROOT}")
endif()

find_library(ASU_HCOMM_LIB
    NAMES hcomm
    HINTS
        ${ASCEND_ROOT}
    PATH_SUFFIXES
        lib64
        aarch64-linux/lib64
)
if(NOT ASU_HCOMM_LIB)
    message(WARNING "Cannot find libhcomm under ASCEND_ROOT=${ASCEND_ROOT}")
endif()

find_path(ASU_ASCEND_ARCH_INCLUDE_DIR
    NAMES hcomm/hcomm_res_defs.h
    HINTS
        ${ASCEND_ROOT}
    PATH_SUFFIXES
        aarch64-linux/include
        arm64-linux/include
)
if(NOT ASU_ASCEND_ARCH_INCLUDE_DIR)
    message(WARNING "Cannot find hcomm/hcomm_res_defs.h under ASCEND_ROOT=${ASCEND_ROOT}")
endif()

message(STATUS "ASU ASCEND include: ${ASU_ASCEND_INCLUDE_DIR}")
message(STATUS "ASU ASCEND arch include: ${ASU_ASCEND_ARCH_INCLUDE_DIR}")
message(STATUS "ASU ASCENDCL lib: ${ASU_ASCENDCL_LIB}")
message(STATUS "ASU HCOMM lib: ${ASU_HCOMM_LIB}")

add_library(asu_ascend_deps INTERFACE)
target_include_directories(asu_ascend_deps INTERFACE
    ${ASU_ASCEND_INCLUDE_DIR}
    $<$<BOOL:${ASU_ASCEND_ARCH_INCLUDE_DIR}>:${ASU_ASCEND_ARCH_INCLUDE_DIR}>
)
target_link_libraries(asu_ascend_deps INTERFACE
    ${ASU_ASCENDCL_LIB}
    $<$<BOOL:${ASU_HCOMM_LIB}>:${ASU_HCOMM_LIB}>
)

file(GLOB ASU_COMMON_SOURCES CONFIGURE_DEPENDS common/*.cpp)
file(GLOB ASU_TRANSPORT_SOURCES CONFIGURE_DEPENDS trans/src/*.cpp)
file(GLOB LOGGER_SOURCES CONFIGURE_DEPENDS 
    ${UCM_ROOT_DIR}/ucm/shared/infra/logger/*.cc
    ${UCM_ROOT_DIR}/ucm/shared/infra/logger/cc/*.cc
)
list(APPEND ASU_TRANSPORT_SOURCES ${ASU_COMMON_SOURCES})
list(APPEND ASU_TRANSPORT_SOURCES ${LOGGER_SOURCES})
list(APPEND ASU_TRANSPORT_SOURCES ${UCM_ROOT_DIR}/ucm/shared/trans/ascend/ascend_buffer.cc)
add_library(asu_transport SHARED ${ASU_TRANSPORT_SOURCES})
target_include_directories(asu_transport
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/trans/include
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/common
        ${CMAKE_CURRENT_SOURCE_DIR}/trans/src
        ${UCM_ROOT_DIR}/ucm/shared/infra
        ${UCM_ROOT_DIR}/ucm/shared/infra/logger
        ${UCM_ROOT_DIR}/ucm/shared/infra/logger/cc
        ${UCM_ROOT_DIR}/ucm/shared/infra/status
        ${UCM_ROOT_DIR}/ucm/shared
)
target_compile_definitions(asu_transport PRIVATE SPDLOG_FMT_EXTERNAL)
target_link_libraries(asu_transport
    PUBLIC
        pthread fmt spdlog z
    PRIVATE
        asu_ascend_deps
)

file(GLOB ASU_CLIENT_SOURCES CONFIGURE_DEPENDS client/src/*.cpp)
add_library(asu_client SHARED ${ASU_CLIENT_SOURCES})
target_include_directories(asu_client
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/client/include
)
target_include_directories(asu_client
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/common
        ${CMAKE_CURRENT_SOURCE_DIR}/client/src
        ${UCM_ROOT_DIR}/ucm/shared/infra
        ${UCM_ROOT_DIR}/ucm/transport/kv/common/include
)
target_link_libraries(asu_client PUBLIC asu_transport kv_common pthread)
target_compile_definitions(asu_client PRIVATE SPDLOG_FMT_EXTERNAL)

file(RELATIVE_PATH INSTALL_REL_PATH ${UCM_ROOT_DIR} ${CMAKE_CURRENT_SOURCE_DIR})
set_target_properties(asu_client PROPERTIES INSTALL_RPATH "$ORIGIN")
set_target_properties(asu_transport PROPERTIES INSTALL_RPATH "$ORIGIN")
install(TARGETS asu_client asu_transport LIBRARY DESTINATION ${INSTALL_REL_PATH} COMPONENT ucm)
install(TARGETS asu_client asu_transport LIBRARY DESTINATION ucm/store/asu COMPONENT ucm)

if(BUILD_UNIT_TESTS)
    target_compile_definitions(asu_transport PRIVATE ASU_BUILD_TESTS)
    include(GoogleTest)
    file(GLOB_RECURSE ASU_TEST_SOURCE_FILES CONFIGURE_DEPENDS test/case/*.cc)
    file(GLOB_RECURSE ASU_TRANSPORT_TEST_SOURCE_FILES CONFIGURE_DEPENDS trans/test/*.cpp)
    list(APPEND ASU_TEST_SOURCE_FILES ${ASU_TRANSPORT_TEST_SOURCE_FILES})
    file(GLOB_RECURSE ASU_CLIENT_TEST_SOURCE_FILES CONFIGURE_DEPENDS client/test/*.cpp)
    list(APPEND ASU_TEST_SOURCE_FILES ${ASU_CLIENT_TEST_SOURCE_FILES})
    add_executable(asu.test ${ASU_TEST_SOURCE_FILES})
    target_compile_definitions(asu.test PRIVATE ASU_BUILD_TESTS)
    target_include_directories(asu.test PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/common
        ${CMAKE_CURRENT_SOURCE_DIR}/client/src
        ${CMAKE_CURRENT_SOURCE_DIR}/trans/src
        ${UCM_ROOT_DIR}/ucm/shared/infra
    )
    target_link_libraries(asu.test PRIVATE
        asu_client
        asu_ascend_deps
        gtest_main gtest
    )
    gtest_discover_tests(asu.test)
endif()

option(BUILD_PROVIDER_DEMO "Build AICPUTransProvider demo" OFF)
if(BUILD_PROVIDER_DEMO)
    add_executable(aicpu_send_with_provider test/demo/aicpu_send_with_provider.cc)
    target_include_directories(aicpu_send_with_provider PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/trans/include
        ${CMAKE_CURRENT_SOURCE_DIR}/trans/src
        ${UCM_ROOT_DIR}/ucm/shared/infra
        ${UCM_ROOT_DIR}/ucm/shared/infra/logger
        ${UCM_ROOT_DIR}/ucm/shared/infra/logger/cc
        ${UCM_ROOT_DIR}/ucm/shared/infra/status
        ${UCM_ROOT_DIR}/ucm/shared
    )
    target_compile_definitions(aicpu_send_with_provider PRIVATE SPDLOG_FMT_EXTERNAL)
    target_link_libraries(aicpu_send_with_provider PRIVATE
        asu_transport
        asu_ascend_deps
        pthread
    )

    add_executable(aicpu_multi_channel_demo test/demo/aicpu_multi_channel_demo.cc)
    target_include_directories(aicpu_multi_channel_demo PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/trans/include
        ${CMAKE_CURRENT_SOURCE_DIR}/trans/src
        ${UCM_ROOT_DIR}/ucm/shared/infra
        ${UCM_ROOT_DIR}/ucm/shared/infra/logger
        ${UCM_ROOT_DIR}/ucm/shared/infra/logger/cc
        ${UCM_ROOT_DIR}/ucm/shared/infra/status
        ${UCM_ROOT_DIR}/ucm/shared
    )
    target_compile_definitions(aicpu_multi_channel_demo PRIVATE SPDLOG_FMT_EXTERNAL)
    target_link_libraries(aicpu_multi_channel_demo PRIVATE
        asu_transport
        asu_ascend_deps
        pthread
    )
endif()
