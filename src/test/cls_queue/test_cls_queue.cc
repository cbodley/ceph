// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"

#include "cls/rgw/cls_rgw_types.h"
#include "cls/queue/cls_queue_client.h"
#include "cls/queue/cls_queue_ops.h"

#include "gtest/gtest.h"
#include "test/librados/test_cxx.h"
#include "global/global_context.h"

#include <errno.h>
#include <string>
#include <vector>
#include <map>
#include <set>

using namespace librados;

librados::Rados rados;
librados::IoCtx ioctx;
string pool_name;


/* must be the first test! */
TEST(cls_queue, init)
{
  pool_name = get_temp_pool_name();
  /* create pool */
  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));
}


string str_int(string s, int i)
{
  char buf[32];
  snprintf(buf, sizeof(buf), "-%d", i);
  s.append(buf);

  return s;
}

/* test garbage collection */
static void create_obj(cls_rgw_obj& obj, int i, int j)
{
  char buf[32];
  snprintf(buf, sizeof(buf), "-%d.%d", i, j);
  obj.pool = "pool";
  obj.pool.append(buf);
  obj.key.name = "oid";
  obj.key.name.append(buf);
  obj.loc = "loc";
  obj.loc.append(buf);
}

static bool cmp_objs(cls_rgw_obj& obj1, cls_rgw_obj& obj2)
{
  return (obj1.pool == obj2.pool) &&
         (obj1.key == obj2.key) &&
         (obj1.loc == obj2.loc);
}

TEST(cls_queue, gc_queue_ops1)
{
  //Testing queue ops when data size is NOT a multiple of queue size
  string queue_name = "my-queue";
  uint64_t queue_size = (1024 + 320), num_urgent_data_entries = 10;
  librados::ObjectWriteOperation op;
  cls_rgw_gc_create_queue(op, queue_name, queue_size, num_urgent_data_entries);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));

  uint64_t size = 0;
  int ret = cls_rgw_gc_get_queue_size(ioctx, queue_name, size);
  std::cerr << "[          ] queue size = " << size << std::endl;
  ASSERT_EQ(0, ret);

  //Test Dequeue, when Queue is empty
  cls_rgw_gc_obj_info deq_info;
  ASSERT_EQ(-ENOENT, cls_rgw_gc_dequeue(ioctx, queue_name, deq_info));

  //Test enqueue
  for (int i = 0; i < 2; i++) {
    char buf[32];
    snprintf(buf, sizeof(buf), "chain-%d", i);
    string tag = buf;
    librados::ObjectWriteOperation op;
    cls_rgw_gc_obj_info info;

    cls_rgw_obj obj1, obj2;
    create_obj(obj1, i, 1);
    create_obj(obj2, i, 2);
    info.chain.objs.push_back(obj1);
    info.chain.objs.push_back(obj2);

    info.tag = tag;
    cls_rgw_gc_enqueue(op, 0, info);
    if (i == 1) {
      ASSERT_EQ(-ENOSPC, ioctx.operate(queue_name, &op));
    } else {
      ASSERT_EQ(0, ioctx.operate(queue_name, &op));
    }
  }

  //Test subsequent dequeue
  cls_rgw_gc_obj_info deq_info1;
  cls_rgw_gc_dequeue(ioctx, queue_name, deq_info1);

  cls_rgw_gc_dequeue(ioctx, queue_name, deq_info1);

  //Test enqueue again
  for (int i = 0; i < 1; i++) {
    char buf[32];
    snprintf(buf, sizeof(buf), "chain-%d", i);
    string tag = buf;
    librados::ObjectWriteOperation op;
    cls_rgw_gc_obj_info info;

    cls_rgw_obj obj1, obj2;
    create_obj(obj1, i, 1);
    create_obj(obj2, i, 2);
    info.chain.objs.push_back(obj1);
    info.chain.objs.push_back(obj2);

    info.tag = tag;
    cls_rgw_gc_enqueue(op, 0, info);
    ASSERT_EQ(0, ioctx.operate(queue_name, &op));
  }

  //Dequeue again
  cls_rgw_gc_dequeue(ioctx, queue_name, deq_info1);
}

TEST(cls_queue, gc_queue_ops2)
{
  //Testing queue ops when data size is a multiple of queue size
  string queue_name = "my-second-queue";
  uint64_t queue_size = (1024 + 330), num_urgent_data_entries = 10;
  librados::ObjectWriteOperation op;
  cls_rgw_gc_create_queue(op, queue_name, queue_size, num_urgent_data_entries);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));

  uint64_t size = 0;
  int ret = cls_rgw_gc_get_queue_size(ioctx, queue_name, size);
  std::cerr << "[          ] queue size = " << size << std::endl;
  ASSERT_EQ(0, ret);

  //Test enqueue
  for (int i = 0; i < 3; i++) {
    char buf[32];
    snprintf(buf, sizeof(buf), "chain-%d", i);
    string tag = buf;
    librados::ObjectWriteOperation op;
    cls_rgw_gc_obj_info info;

    cls_rgw_obj obj1, obj2;
    create_obj(obj1, i, 1);
    create_obj(obj2, i, 2);
    info.chain.objs.push_back(obj1);
    info.chain.objs.push_back(obj2);

    info.tag = tag;
    cls_rgw_gc_enqueue(op, 0, info);
    if (i == 2) {
      ASSERT_EQ(-ENOSPC, ioctx.operate(queue_name, &op));
    } else {
      ASSERT_EQ(0, ioctx.operate(queue_name, &op));
    }
  }

  //Test subsequent dequeue
  cls_rgw_gc_obj_info deq_info1;
  cls_rgw_gc_dequeue(ioctx, queue_name, deq_info1);
  cls_rgw_gc_dequeue(ioctx, queue_name, deq_info1);
  cls_rgw_gc_dequeue(ioctx, queue_name, deq_info1);

  //Test enqueue again
  for (int i = 0; i < 1; i++) {
    char buf[32];
    snprintf(buf, sizeof(buf), "chain-%d", i);
    string tag = buf;
    librados::ObjectWriteOperation op;
    cls_rgw_gc_obj_info info;

    cls_rgw_obj obj1, obj2;
    create_obj(obj1, i, 1);
    create_obj(obj2, i, 2);
    info.chain.objs.push_back(obj1);
    info.chain.objs.push_back(obj2);

    info.tag = tag;
    cls_rgw_gc_enqueue(op, 0, info);
    ASSERT_EQ(0, ioctx.operate(queue_name, &op));
  }
  //Dequeue again
  cls_rgw_gc_dequeue(ioctx, queue_name, deq_info1);
}

TEST(cls_queue, gc_queue_ops3)
{
  //Testing list queue
  string queue_name = "my-third-queue";
  uint64_t queue_size = (1024 + 330), num_urgent_data_entries = 10;
  librados::ObjectWriteOperation op;
  cls_rgw_gc_create_queue(op, queue_name, queue_size, num_urgent_data_entries);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));

  uint64_t size = 0;
  int ret = cls_rgw_gc_get_queue_size(ioctx, queue_name, size);
  std::cerr << "[          ] queue size = " << size << std::endl;
  ASSERT_EQ(0, ret);

  //Test list queue, when queue is empty
  list<cls_rgw_gc_obj_info> list_info;
  string marker1, next_marker1;
  uint64_t max1 = 2;
  bool expired_only1 = false, truncated1;
  cls_rgw_gc_list_queue(ioctx, queue_name, marker1, max1, expired_only1, list_info, &truncated1, next_marker1);
  ASSERT_EQ(0, list_info.size());

  //Test enqueue
  for (int i = 0; i < 3; i++) {
    char buf[32];
    snprintf(buf, sizeof(buf), "chain-%d", i);
    string tag = buf;
    librados::ObjectWriteOperation op;
    cls_rgw_gc_obj_info info;

    cls_rgw_obj obj1, obj2;
    create_obj(obj1, i, 1);
    create_obj(obj2, i, 2);
    info.chain.objs.push_back(obj1);
    info.chain.objs.push_back(obj2);

    info.tag = tag;
    cls_rgw_gc_enqueue(op, 0, info);
    if (i == 2) {
      ASSERT_EQ(-ENOSPC, ioctx.operate(queue_name, &op));
    } else {
      ASSERT_EQ(0, ioctx.operate(queue_name, &op));
    }
  }

  //Test list queue
  list<cls_rgw_gc_obj_info> list_info1, list_info2, list_info3;
  string marker, next_marker;
  uint64_t max = 2;
  bool expired_only = false, truncated;
  cls_rgw_gc_list_queue(ioctx, queue_name, marker, max, expired_only, list_info1, &truncated, next_marker);
  ASSERT_EQ(2, list_info1.size());

  int i = 0;
  for (auto it : list_info1) {
    char buf[32];
    snprintf(buf, sizeof(buf), "chain-%d", i);
    string tag = buf;
    ASSERT_EQ(tag, it.tag);
    i++;
  }

  max = 1;
  truncated = false;
  cls_rgw_gc_list_queue(ioctx, queue_name, marker, max, expired_only, list_info2, &truncated, next_marker);
  auto it = list_info2.front();
  ASSERT_EQ(1, list_info2.size());
  ASSERT_EQ(true, truncated);
  ASSERT_EQ("chain-0", it.tag);
  std::cerr << "[          ] next_marker is: = " << next_marker << std::endl;

  marker = next_marker;
  cls_rgw_gc_list_queue(ioctx, queue_name, marker, max, expired_only, list_info3, &truncated, next_marker);
  it = list_info3.front();
  ASSERT_EQ(1, list_info3.size());
  ASSERT_EQ(false, truncated);
  ASSERT_EQ("chain-1", it.tag);
}

TEST(cls_queue, gc_queue_ops4)
{
  //Testing remove queue entries
  string queue_name = "my-fourth-queue";
  uint64_t queue_size = (1024 + 495), num_urgent_data_entries = 10;
  librados::ObjectWriteOperation op;
  cls_rgw_gc_create_queue(op, queue_name, queue_size, num_urgent_data_entries);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));

  uint64_t size = 0;
  int ret = cls_rgw_gc_get_queue_size(ioctx, queue_name, size);
  std::cerr << "[          ] queue size = " << size << std::endl;
  ASSERT_EQ(0, ret);

  //Test remove queue, when queue is empty
  librados::ObjectWriteOperation remove_op;
  string marker1;
  uint64_t num_entries = 2;
  cls_rgw_gc_remove_queue(remove_op,  marker1, num_entries);
  ASSERT_EQ(0, ioctx.operate(queue_name, &remove_op));

  cls_rgw_gc_obj_info defer_info;

  //Test enqueue
  for (int i = 0; i < 2; i++) {
    char buf[32];
    snprintf(buf, sizeof(buf), "chain-%d", i);
    string tag = buf;
    librados::ObjectWriteOperation op;
    cls_rgw_gc_obj_info info;

    cls_rgw_obj obj1, obj2;
    create_obj(obj1, i, 1);
    create_obj(obj2, i, 2);
    info.chain.objs.push_back(obj1);
    info.chain.objs.push_back(obj2);

    info.tag = tag;
    cls_rgw_gc_enqueue(op, 5, info);
    ASSERT_EQ(0, ioctx.operate(queue_name, &op));
    if (i == 0)
      defer_info = info;
  }

  //Test defer entry for 1st element
  librados::ObjectWriteOperation defer_op;
  cls_rgw_gc_defer_entry_queue(defer_op, 10, defer_info);
  ASSERT_EQ(0, ioctx.operate(queue_name, &defer_op));

  //Test list queue
  list<cls_rgw_gc_obj_info> list_info1, list_info2, list_info3;
  string marker, next_marker;
  uint64_t max = 2;
  bool expired_only = false, truncated;
  cls_rgw_gc_list_queue(ioctx, queue_name, marker, max, expired_only, list_info1, &truncated, next_marker);
  ASSERT_EQ(2, list_info1.size());

  int i = 0;
  for (auto it : list_info1) {
    std::cerr << "[          ] list info tag = " << it.tag << std::endl;
    if (i == 0) {
      ASSERT_EQ("chain-1", it.tag);
    }
    if (i == 1) {
      ASSERT_EQ("chain-0", it.tag);
    }
    i++;
  }

  //Test remove entries
  num_entries = 2;
  cls_rgw_gc_remove_queue(remove_op,  marker1, num_entries);
  ASSERT_EQ(0, ioctx.operate(queue_name, &remove_op));

  //Test list queue again
  cls_rgw_gc_list_queue(ioctx, queue_name, marker, max, expired_only, list_info1, &truncated, next_marker);
  ASSERT_EQ(0, list_info1.size());

}

TEST(cls_queue, gc_queue_ops5)
{
  //Testing remove queue entries
  string queue_name = "my-fifth-queue";
  uint64_t queue_size = (1024 + 495), num_urgent_data_entries = 10;
  librados::ObjectWriteOperation op;
  cls_rgw_gc_create_queue(op, queue_name, queue_size, num_urgent_data_entries);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));

  uint64_t size = 0;
  int ret = cls_rgw_gc_get_queue_size(ioctx, queue_name, size);
  std::cerr << "[          ] queue size = " << size << std::endl;
  ASSERT_EQ(0, ret);

  //Test remove queue, when queue is empty
  librados::ObjectWriteOperation remove_op;
  string marker1;
  uint64_t num_entries = 2;
  cls_rgw_gc_remove_queue(remove_op,  marker1, num_entries);
  ASSERT_EQ(0, ioctx.operate(queue_name, &remove_op));

  cls_rgw_gc_obj_info defer_info;

  //Test enqueue
  for (int i = 0; i < 2; i++) {
    char buf[32];
    snprintf(buf, sizeof(buf), "chain-%d", i);
    string tag = buf;
    librados::ObjectWriteOperation op;
    cls_rgw_gc_obj_info info;

    cls_rgw_obj obj1, obj2;
    create_obj(obj1, i, 1);
    create_obj(obj2, i, 2);
    info.chain.objs.push_back(obj1);
    info.chain.objs.push_back(obj2);

    info.tag = tag;
    cls_rgw_gc_enqueue(op, 5, info);
    ASSERT_EQ(0, ioctx.operate(queue_name, &op));
    defer_info = info;
  }

  //Test defer entry for last element
  librados::ObjectWriteOperation defer_op;
  cls_rgw_gc_defer_entry_queue(defer_op, 10, defer_info);
  ASSERT_EQ(0, ioctx.operate(queue_name, &defer_op));

  //Test list queue
  list<cls_rgw_gc_obj_info> list_info1, list_info2, list_info3;
  string marker, next_marker;
  uint64_t max = 2;
  bool expired_only = false, truncated;
  cls_rgw_gc_list_queue(ioctx, queue_name, marker, max, expired_only, list_info1, &truncated, next_marker);
  ASSERT_EQ(2, list_info1.size());

  int i = 0;
  for (auto it : list_info1) {
    char buf[32];
    snprintf(buf, sizeof(buf), "chain-%d", i);
    string tag = buf;
    ASSERT_EQ(tag, it.tag);
    i++;
  }

  //Test remove entries
  num_entries = 2;
  cls_rgw_gc_remove_queue(remove_op,  marker1, num_entries);
  ASSERT_EQ(0, ioctx.operate(queue_name, &remove_op));

  //Test list queue again
  cls_rgw_gc_list_queue(ioctx, queue_name, marker, max, expired_only, list_info1, &truncated, next_marker);
  ASSERT_EQ(0, list_info1.size());

}

TEST(cls_queue, gc_queue_ops6)
{
  //Testing remove queue entries
  string queue_name = "my-sixth-queue";
  uint64_t queue_size = (1024 + 495), num_urgent_data_entries = 10;
  librados::ObjectWriteOperation op;
  cls_rgw_gc_create_queue(op, queue_name, queue_size, num_urgent_data_entries);
  ASSERT_EQ(0, ioctx.operate(queue_name, &op));

  uint64_t size = 0;
  int ret = cls_rgw_gc_get_queue_size(ioctx, queue_name, size);
  std::cerr << "[          ] queue size = " << size << std::endl;
  ASSERT_EQ(0, ret);

  //Test enqueue
  for (int i = 0; i < 3; i++) {
    char buf[32];
    snprintf(buf, sizeof(buf), "chain-%d", i);
    string tag = buf;
    librados::ObjectWriteOperation op;
    cls_rgw_gc_obj_info info;

    cls_rgw_obj obj1, obj2;
    create_obj(obj1, i, 1);
    create_obj(obj2, i, 2);
    info.chain.objs.push_back(obj1);
    info.chain.objs.push_back(obj2);

    info.tag = tag;
    if (i == 2) {
      cls_rgw_gc_enqueue(op, 300, info);
    } else {
      cls_rgw_gc_enqueue(op, 0, info);
    }
    ASSERT_EQ(0, ioctx.operate(queue_name, &op));
  }
  //Test list queue for expired entries only
  list<cls_rgw_gc_obj_info> list_info1;
  string marker, next_marker, marker1;
  uint64_t max = 10;
  bool expired_only = true, truncated;
  cls_rgw_gc_list_queue(ioctx, queue_name, marker, max, expired_only, list_info1, &truncated, next_marker);
  ASSERT_EQ(2, list_info1.size());

  int i = 0;
  for (auto it : list_info1) {
    char buf[32];
    snprintf(buf, sizeof(buf), "chain-%d", i);
    string tag = buf;
    ASSERT_EQ(tag, it.tag);
    i++;
  }

  //Test remove entries
  librados::ObjectWriteOperation remove_op;
  auto num_entries = list_info1.size();
  cls_rgw_gc_remove_queue(remove_op,  marker1, num_entries);
  ASSERT_EQ(0, ioctx.operate(queue_name, &remove_op));

  //Test list queue again for all entries
  expired_only = false;
  cls_rgw_gc_list_queue(ioctx, queue_name, marker, max, expired_only, list_info1, &truncated, next_marker);
  ASSERT_EQ(1, list_info1.size());

}
/* must be last test! */
TEST(cls_queue, finalize)
{
  /* remove pool */
  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}
