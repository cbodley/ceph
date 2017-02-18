// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef RGW_ASIO_CLIENT_H
#define RGW_ASIO_CLIENT_H

#include <boost/asio/ip/tcp.hpp>
#include <beast/http/message.hpp>
#include "include/assert.h"

#include "rgw_client_io.h"

// bufferlist to represent the message body
// XXX: beast::http::async_read() currently has to read the entire body into
// memory before completing.  we need the ability to parse only the headers,
// then read/parse the body as needed by RGWAsioClientIO::read_data()
// see https://github.com/vinniefalco/Beast/issues/154
class RGWBufferlistBody {
 public:
  using value_type = ceph::bufferlist;

  class reader;
  class writer;

  template <bool isRequest, typename Headers>
  using message_type = beast::http::message<isRequest, RGWBufferlistBody,
                                            Headers>;
};

class RGWAsioClientIO : public rgw::io::RestfulClient,
                        public rgw::io::BuffererSink {
  using tcp = boost::asio::ip::tcp;
  tcp::socket socket;

  using body_type = RGWBufferlistBody;
  using request_type = beast::http::request<body_type>;
  request_type request;

  bufferlist::const_iterator body_iter;

  bool conn_keepalive{false};
  bool conn_close{false};
  RGWEnv env;

  rgw::io::StaticOutputBufferer<> txbuf;

  size_t write_data(const char *buf, size_t len) override;
  size_t read_data(char *buf, size_t max);

 public:
  RGWAsioClientIO(tcp::socket&& socket, request_type&& request);
  ~RGWAsioClientIO();

  void init_env(CephContext *cct) override;
  size_t complete_request() override;
  void flush() override;
  size_t send_status(int status, const char *status_name) override;
  size_t send_100_continue() override;
  size_t send_header(const boost::string_ref& name,
                     const boost::string_ref& value) override;
  size_t send_content_length(uint64_t len) override;
  size_t complete_header() override;

  size_t recv_body(char* buf, size_t max) override {
    return read_data(buf, max);
  }

  size_t send_body(const char* buf, size_t len) override {
    return write_data(buf, len);
  }

  RGWEnv& get_env() noexcept override {
    return env;
  }
};

// used by beast::http::read() to read the body into a bufferlist
class RGWBufferlistBody::reader {
  value_type& bl;
 public:
  template<bool isRequest, typename Headers>
  explicit reader(message_type<isRequest, Headers>& m) noexcept
    : bl(m.body) {}

  void init(boost::system::error_code& ec) noexcept {}

  void write(const void* data, size_t size, boost::system::error_code&) {
    bl.append(reinterpret_cast<const char*>(data), size);
  }
};

// used by beast::http::write() to write the buffered body
class RGWBufferlistBody::writer {
  const value_type& bl;
 public:
  template<bool isRequest, typename Headers>
  explicit writer(const message_type<isRequest, Headers>& msg)
    : bl(msg.body) {}

  void init(boost::system::error_code& ec) noexcept {}
  uint64_t content_length() const { return bl.length(); }

  template<typename Write>
  boost::tribool operator()(beast::http::resume_context&&,
                            boost::system::error_code&, Write&& write) {
    // translate from bufferlist to a ConstBufferSequence for beast
    std::vector<boost::asio::const_buffer> buffers;
    buffers.reserve(bl.get_num_buffers());
    for (auto& ptr : bl.buffers()) {
      buffers.emplace_back(ptr.c_str(), ptr.length());
    }
    write(buffers);
    return true;
  }
};

#endif // RGW_ASIO_CLIENT_H
