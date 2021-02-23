// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_datalog_notify.h"
#include "rgw_datalog.h"
#include "rgw_coroutine.h"
#include "rgw_cr_rest.h"

#include <boost/asio/yield.hpp>

class DatalogNotifyCR : public RGWCoroutine {
  RGWHTTPManager* http;
  RGWRESTConn* conn;
  const char* source_zone;
  const bc::flat_map<int, bc::flat_set<rgw_data_notify_entry>>& shards;
 public:
  DatalogNotifyCR(RGWHTTPManager* http, RGWRESTConn* conn, const char* source_zone,
                  const bc::flat_map<int, bc::flat_set<rgw_data_notify_entry> >& shards)
    : RGWCoroutine(conn->get_ctx()), conn(conn), shards(shards)
  {}

  int operate() override {
    reenter(this) {
      // try the notify2 API
      yield {
        rgw_http_param_pair params[] = {
          { "type", "data" },
          { "notify2", nullptr },
          { "source-zone", source_zone },
          { nullptr, nullptr }
        };
        using PostNotify2 = RGWPostRESTResourceCR<bc::flat_map<int, bc::flat_set<rgw_data_notify_entry>>, int>;
        call(new PostNotify2(cct, conn, http, "/admin/log", params, shards, nullptr));
      }

      // if the remote doesn't understand notify v2, retry with notify v1
      if (retcode == -ERR_METHOD_NOT_ALLOWED) yield {
        rgw_http_param_pair params[] = {
          { "type", "data" },
          { "notify", nullptr },
          { "source-zone", source_zone },
          { nullptr, nullptr }
        };
        auto encoder = rgw_data_notify_v1_encoder{shards};
        using PostNotify1 = RGWPostRESTResourceCR<rgw_data_notify_v1_encoder, int>;
        call(new PostNotify1(cct, conn, http, "/admin/log", params, encoder, nullptr));
      }

      if (retcode < 0) {
        return set_cr_error(retcode);
      }
      return set_cr_done();
    }
    return 0;
  }
};

RGWCoroutine* rgw_datalog_notify_peer_cr(RGWHTTPManager* http, RGWRESTConn* conn,
                                         const char* source_zone,
                                         const bc::flat_map<int, bc::flat_set<rgw_data_notify_entry>>& shards)
{
  return new DatalogNotifyCR(http, conn, source_zone, shards);
}

// custom encoding for v1 notify API
struct EntryEncoderV1 {
  const rgw_data_notify_entry& entry;
};
struct SetEncoderV1 {
  const bc::flat_set<rgw_data_notify_entry>& entries;
};

// encode rgw_data_notify_entry as string
void encode_json(const char *name, const EntryEncoderV1& e, Formatter *f)
{
  f->dump_string(name, e.entry.key); // encode the key only
}
// encode set<rgw_data_notify_entry> as set<string>
void encode_json(const char *name, const SetEncoderV1& e, Formatter *f)
{
  f->open_array_section(name);
  for (auto& entry : e.entries) {
    encode_json("obj", EntryEncoderV1{entry}, f);
  }
  f->close_section();
}
// encode map<int, set<rgw_data_notify_entry>> as map<int, set<string>>
void encode_json(const char *name, const rgw_data_notify_v1_encoder& e, Formatter *f)
{
  f->open_array_section(name);
  for (auto& [key, val] : e.shards) {
    f->open_object_section("entry");
    encode_json("key", key, f);
    encode_json("val", SetEncoderV1{val}, f);
    f->close_section();
  }
  f->close_section();
}

struct EntryDecoderV1 {
  rgw_data_notify_entry& entry;
};
struct SetDecoderV1 {
  bc::flat_set<rgw_data_notify_entry>& entries;
};

// decode string into rgw_data_notify_entry
void decode_json_obj(EntryDecoderV1& d, JSONObj *obj)
{
  decode_json_obj(d.entry, obj);
  d.entry.gen = 0;
}
// decode set<string> into set<rgw_data_notify_entry>
void decode_json_obj(SetDecoderV1& d, JSONObj *obj)
{
  for (JSONObjIter o = obj->find_first(); !o.end(); ++o) {
    rgw_data_notify_entry val;
    auto decoder = EntryDecoderV1{val};
    decode_json_obj(decoder, *o);
    d.entries.insert(std::move(val));
  }
}
// decode map<int, set<string>> into map<int, set<rgw_data_notify_entry>>
void decode_json_obj(rgw_data_notify_v1_decoder& d, JSONObj *obj)
{
  for (JSONObjIter o = obj->find_first(); !o.end(); ++o) {
    int shard_id = 0;
    JSONDecoder::decode_json("key", shard_id, *o);
    bc::flat_set<rgw_data_notify_entry> val;
    SetDecoderV1 decoder{val};
    JSONDecoder::decode_json("val", decoder, *o);
    d.shards[shard_id] = std::move(val);
  }
}
