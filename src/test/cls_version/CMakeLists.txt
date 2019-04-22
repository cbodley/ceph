# ceph_test_cls_version
add_executable(ceph_test_cls_version
  test_cls_version.cc
  )
target_link_libraries(ceph_test_cls_version
  librados
  cls_version_client
  libRADOS
  global
  ${UNITTEST_LIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  radostest-cxx
  )


if(WITH_BOOST_CONTEXT)
  target_link_libraries(ceph_test_cls_version Boost::coroutine Boost::context)
endif()
