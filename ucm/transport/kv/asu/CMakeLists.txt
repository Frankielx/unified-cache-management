# Try to get CANN library path from environment or explicit parameter
if(NOT CANN_LIB_DIR)
    if(DEFINED ENV{ASCEND_HOME_PATH})
        set(CANN_LIB_DIR "$ENV{ASCEND_HOME_PATH}/lib64")
        message(STATUS "Using CANN_LIB_DIR from ASCEND_HOME_PATH: ${CANN_LIB_DIR}")
    elseif(DEFINED ENV{ASCEND_HOME})
        set(CANN_LIB_DIR "$ENV{ASCEND_HOME}/lib64")
        message(STATUS "Using CANN_LIB_DIR from ASCEND_HOME: ${CANN_LIB_DIR}")
    else()
        message(FATAL_ERROR 
            "CANN library path not found. Please either:\n"
            "  1. Source CANN environment: source /usr/local/Ascend/cann/set_env.sh\n"
            "  2. Or set CANN_LIB_DIR explicitly: -DCANN_LIB_DIR=/path/to/cann/lib64"
        )
    endif()
endif()

file(GLOB ASU_TRANSPORT_SOURCES CONFIGURE_DEPENDS trans/src/*.cpp)
file(GLOB LOGGER_SOURCES CONFIGURE_DEPENDS 
    ${UCM_ROOT_DIR}/ucm/shared/infra/logger/*.cc
    ${UCM_ROOT_DIR}/ucm/shared/infra/logger/cc/*.cc
)
add_library(asu_transport SHARED ${ASU_TRANSPORT_SOURCES} ${LOGGER_SOURCES})
target_include_directories(asu_transport
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/trans/include
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/common
        ${CMAKE_CURRENT_SOURCE_DIR}/trans/src
        ${UCM_ROOT_DIR}/ucm/shared/infra
        ${UCM_ROOT_DIR}/ucm/shared/infra/logger
        ${UCM_ROOT_DIR}/ucm/shared/infra/logger/cc
)
target_compile_definitions(asu_transport PRIVATE SPDLOG_FMT_EXTERNAL)
target_compile_options(asu_transport PRIVATE -include ${CMAKE_CURRENT_SOURCE_DIR}/common/fmt_compat.h)
target_link_libraries(asu_transport PUBLIC pthread fmt spdlog z)
target_link_directories(asu_transport PRIVATE ${CANN_LIB_DIR})
target_link_libraries(asu_transport PRIVATE hcomm ascendcl)

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

# Demo: aicpu_send_with_provider
if(BUILD_PROVIDER_DEMO)
    add_executable(aicpu_send_with_provider test/demo/aicpu_send_with_provider.cc)
    target_include_directories(aicpu_send_with_provider PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/trans/src
    )
    target_link_directories(aicpu_send_with_provider PRIVATE ${CANN_LIB_DIR})
    target_link_libraries(aicpu_send_with_provider PRIVATE asu_transport hcomm ascendcl c_sec)
endif()

if(BUILD_UNIT_TESTS)
    target_compile_definitions(asu_transport PRIVATE ASU_BUILD_TESTS)
    include(GoogleTest)
    file(GLOB_RECURSE ASU_TEST_SOURCE_FILES CONFIGURE_DEPENDS test/case/*.cc)
    add_executable(asu.test ${ASU_TEST_SOURCE_FILES})
    target_compile_definitions(asu.test PRIVATE ASU_BUILD_TESTS)
    target_include_directories(asu.test PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/common
        ${CMAKE_CURRENT_SOURCE_DIR}/client/src
        ${CMAKE_CURRENT_SOURCE_DIR}/trans/src
        ${UCM_ROOT_DIR}/ucm/shared/infra
    )
    target_compile_options(asu.test PRIVATE -include ${CMAKE_CURRENT_SOURCE_DIR}/common/fmt_compat.h)
    target_link_libraries(asu.test PRIVATE
        asu_client
        gtest_main gtest
    )
    gtest_discover_tests(asu.test)
endif()
