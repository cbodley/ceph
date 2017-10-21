set(test_buffer_srcs
  test_buffer.cc
  $<TARGET_OBJECTS:seastar_buffer_obj>)
add_executable(unittest_seastar_buffer ${test_buffer_srcs})
add_ceph_unittest(unittest_seastar_buffer)
set_target_properties(unittest_seastar_buffer PROPERTIES CXX_STANDARD 14)
target_link_libraries(unittest_seastar_buffer common Seastar::Seastar)

set(test_messenger_srcs
  test_messenger.cc
  $<TARGET_OBJECTS:seastar_buffer_obj>
  $<TARGET_OBJECTS:crimson_net_objs>)
add_executable(unittest_seastar_messenger ${test_messenger_srcs})
add_ceph_unittest(unittest_seastar_messenger)
set_target_properties(unittest_seastar_messenger PROPERTIES CXX_STANDARD 14)
target_link_libraries(unittest_seastar_messenger common Seastar::Seastar)
