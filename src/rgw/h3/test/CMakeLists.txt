find_package(GTest REQUIRED)

add_library(test_base INTERFACE)
target_include_directories(test_base INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(test_base INTERFACE rgw_h3_quiche GTest::gtest_main)
