#pragma once

#include "rgw_common.h"
#include <cpp_redis/cpp_redis>
#include <string>
#include <iostream>

#include <boost/lexical_cast.hpp>
#include <boost/redis/connection.hpp>

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

namespace rgw { namespace d4n {

namespace net = boost::asio;
using boost::redis::config;
using boost::redis::connection;
using boost::redis::request;
using boost::redis::response;

struct Address {
  std::string host;
  int port;
};

struct CacheObj {
  std::string objName; /* S3 object name */
  std::string bucketName; /* S3 bucket name */
  time_t creationTime; /* Creation time of the S3 Object */
  bool dirty;
  std::vector<std::string> hostsList; /* List of hostnames <ip:port> of object locations for multiple backends */
};

struct CacheBlock {
  CacheObj cacheObj;
  uint64_t version; /* RADOS object block ID */
  uint64_t size; /* Block size in bytes */
  int globalWeight = 0; /* LFUDA policy variable */
  std::vector<std::string> hostsList; /* List of hostnames <ip:port> of block locations */
};

class Directory {
  public:
    Directory() {}
    CephContext* cct;
};

class ObjectDirectory: public Directory { // weave into write workflow -Sam
  public:
    ObjectDirectory(net::io_context& io_context) {
      conn = new connection{boost::asio::make_strand(io_context)};
    }
    ObjectDirectory(net::io_context& io_context, std::string host, int port) {
      conn = new connection{boost::asio::make_strand(io_context)};
      addr.host = host;
      addr.port = port;
    }
    ~ObjectDirectory() {
      delete conn;
    }

    int init(/*CephContext* _cct, const DoutPrefixProvider* dpp*/) {
      //cct = _cct;

      config cfg;
      cfg.addr.host = "127.0.0.1";//cct->_conf->rgw_d4n_host; // TODO: Replace with cache address
      cfg.addr.port = "6379";//std::to_string(cct->_conf->rgw_d4n_port);

      if (!cfg.addr.host.length() || !cfg.addr.port.length()) {
	//ldpp_dout(dpp, 10) << "D4N Directory: Object directory endpoint was not configured correctly" << dendl;
	return -EDESTADDRREQ;
      }

      conn->async_run(cfg, {}, net::detached);

      return 0;
    }

    int find_client(cpp_redis::client* client);
    int exist_key(std::string key);
    Address get_addr() { return addr; }

    int set_value(CacheObj* object);
    int get_value(CacheObj* object);
    int copy_value(CacheObj* object, CacheObj* copyObject);
    int del_value(CacheObj* object);

  private:
    connection* conn;
    cpp_redis::client client;
    Address addr;
    std::string build_index(CacheObj* object);
};

class BlockDirectory: public Directory {
  public:
    BlockDirectory(net::io_context& io_context) {
      conn = new connection{boost::asio::make_strand(io_context)};
    }
    BlockDirectory(net::io_context& io_context, std::string host, int port) {
      conn = new connection{boost::asio::make_strand(io_context)};
      addr.host = host;
      addr.port = port;
    }
    ~BlockDirectory() {
      delete conn;
    }
    
    int init(/*CephContext* _cct, const DoutPrefixProvider* dpp*/) {
      //cct = _cct;

      config cfg;
      cfg.addr.host = "127.0.0.1";//cct->_conf->rgw_d4n_host; // TODO: Replace with cache address
      cfg.addr.port = "6379";//std::to_string(cct->_conf->rgw_d4n_port);

      if (!cfg.addr.host.length() || !cfg.addr.port.length()) {
	//ldpp_dout(dpp, 10) << "D4N Directory: Block directory endpoint was not configured correctly" << dendl;
	return -EDESTADDRREQ;
      }

      conn->async_run(cfg, {}, net::detached); 

      return 0;
    }
	
    int find_client(cpp_redis::client* client);
    int exist_key(std::string key, optional_yield y);
    Address get_addr() { return addr; }

    int set_value(CacheBlock* block, optional_yield y);
    int get_value(CacheBlock* block, optional_yield y);
    int copy_value(CacheBlock* block, CacheBlock* copyBlock);
    int del_value(CacheBlock* block, optional_yield y);

    int update_field(CacheBlock* block, std::string field, std::string value);
    auto conn_cancel() { conn->cancel(); }

  private:
    connection* conn;
    cpp_redis::client client;
    Address addr;
    std::string build_index(CacheBlock* block);
};

} } // namespace rgw::d4n