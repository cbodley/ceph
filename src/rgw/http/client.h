// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <atomic>

#include <boost/asio.hpp>

#include <boost/circular_buffer.hpp>
#include <boost/intrusive/list.hpp>

#include "common/async/completion.h"
#include "common/async/timed_handler.h"
#include "common/ceph_time.h"
#include "common/ceph_mutex.h"

namespace rgw::http {

// Connection requirements:
// - supports tcp::socket and ssl::stream
// - timeouts on read/write

// ConnectionPool requirements:
// - connection states:
//   - waiting: at max connections, waiting for outstanding to return
//   - resolving: pending async_resolve()
//   - connecting: resolved, pending async_connect()
//   - outstanding: connected, passed to caller and not yet returned
//   - idle: returned by caller, avaiable for reuse
//   - timeout: terminal state reachable from resolving/connecting/idle
// - async_get()
//   - use associated executor/allocator for all intermediate operations
//   - if there's an idle connection, enter outstanding state and return it
//   - else if at max connection count, enter waiting state
//   - else enter resolving state
// - put(conn, ec)
//   - if there's a waiting connection:
//     - if !ec, enter outstanding state and return conn to waiter
//     - else enter resolving state
//   - if !ec, enter idle state
//   - else destroy conn
// - close()
//   - cancels waiting states
//   - cancels resolving states
//   - closes sockets associated with connecting/outstanding/idle connections

namespace async = ceph::async;

// async stream wrapper that adds a timeout to each read and write
template <typename Stream, typename Clock>
class TimedStream {
 public:
  using Timer = boost::asio::basic_waitable_timer<Clock>;
 private:
  Stream stream;
  Timer timer;
  ceph::timespan timeout;

 public:
  TimedStream(Stream&& stream, Timer&& timer, ceph::timespan timeout)
    : stream(std::move(stream)), timer(std::move(timer)), timeout(timeout) {}
  TimedStream(TimedStream&&) = default;
  TimedStream& operator=(TimedStream&&) = default;

  auto get_executor() { return stream.get_executor(); }

  using next_layer_type = Stream;
  next_layer_type& next_layer() { return stream; }

  using lowest_layer_type = typename Stream::lowest_layer_type;
  lowest_layer_type& lowest_layer() { return stream.lowest_layer(); }

  // AsyncReadStream
  template <typename MutableBufferSequence, typename ReadHandler>
  auto async_read_some(const MutableBufferSequence& buffers,
                       ReadHandler&& token) {
    using Signature = void(boost::system::error_code, size_t);
    boost::asio::async_completion<ReadHandler, Signature> init(token);
    auto& handler = init.completion_handler;
    if (timeout.count()) {
      auto h = async::timed_handler(get_executor(), lowest_layer(),
                                    timer, timeout, std::move(handler));
      stream.async_read_some(buffers, std::move(h));
    } else {
      stream.async_read_some(buffers, std::move(handler));
    }
    return init.result.get();
  }

  // AsyncWriteStream
  template <typename ConstBufferSequence, typename WriteHandler>
  auto async_write_some(const ConstBufferSequence& buffers,
                        WriteHandler&& token) {
    using Signature = void(boost::system::error_code, size_t);
    boost::asio::async_completion<WriteHandler, Signature> init(token);
    auto& handler = init.completion_handler;
    if (timeout.count()) {
      auto h = async::timed_handler(get_executor(), lowest_layer(),
                                    timer, timeout, std::move(handler));
      stream.async_write_some(buffers, std::move(h));
    } else {
      stream.async_write_some(buffers, std::move(handler));
    }
    return init.result.get();
  }

  // TODO: SyncReadStream/SyncWriteStream
};

using boost::asio::ip::tcp;

// a tcp connection pool
template <typename Stream, typename Clock>
class ConnectionPool {
 public:
  class Connection; // inherits from Stream

  ConnectionPool(boost::asio::io_context& context,
                 std::string host, std::string service,
                 uint32_t max_connections,
                 uint32_t max_retries,
                 ceph::timespan connect_timeout,
                 ceph::timespan idle_timeout)
    : context(context), host(std::move(host)), service(std::move(service)),
      max_connections(max_connections),
      retries_left(max_retries),
      connect_timeout(connect_timeout),
      idle_timeout(idle_timeout),
      idle(max_connections)
  {}
  ~ConnectionPool() {
    ceph_assert(connecting.empty());
    ceph_assert(waiters.empty());
  }

  // TODO: get()

  template <typename CompletionToken> // void(error_code, Connection)
  auto async_get(CompletionToken&& token);

  // TODO: cancel()

  // cancel any pending dns resolution and close all sockets
  void close();

  using Timer = boost::asio::basic_waitable_timer<Clock>;
 private:
  boost::asio::io_context& context;
  std::string host;
  std::string service;

  ceph::mutex mutex = ceph::make_mutex("rgw::http::ConnectionPool");
  uint32_t count = 0;
  const uint32_t max_connections;

  uint32_t retries_left;
  const ceph::timespan connect_timeout;
  const ceph::timespan idle_timeout;

  // connected sockets that are available for use
  boost::circular_buffer<tcp::socket> idle;

  // connection states
  enum class State : uint8_t { Waiting, Resolving, Connecting, Canceled };

  struct Connect : boost::intrusive::list_base_hook<> {
    tcp::resolver resolver;
    tcp::socket socket;
    Timer timer;
    typename Clock::time_point started;
    State state;

    Connect(boost::asio::io_context& context,
            typename Clock::time_point started, State state)
      : resolver(context), socket(context), timer(context),
        started(started), state(state) {}
  };
  using Signature = void(boost::system::error_code, std::optional<Connection>);
  using Completion = async::Completion<Signature, async::AsBase<Connect>>;

  boost::intrusive::list<Completion> connecting;

  // track calls that are waiting for the next available connection
  struct Wait : boost::intrusive::list_base_hook<> {
    std::unique_ptr<Completion> completion;

    Wait(std::unique_ptr<Completion>&& completion)
      : completion(std::move(completion)) {}
  };
  using WaitSignature = void(boost::system::error_code,
                             std::unique_ptr<Completion> completion,
                             std::optional<Connection>);
  using WaitCompletion = async::Completion<WaitSignature, async::AsBase<Wait>>;
  boost::intrusive::list<WaitCompletion> waiters;

  template <typename Executor, typename Alloc>
  void on_resolve(const Executor& ex, Alloc alloc,
                  std::unique_ptr<Completion>&& completion,
                  boost::system::error_code ec,
                  const tcp::resolver::results_type& addrs);

  template <typename Executor, typename Alloc>
  struct ResolveHandler;

  template <typename Executor, typename Alloc>
  void on_connect(const Executor& ex, Alloc alloc,
                  std::unique_ptr<Completion>&& completion,
                  boost::system::error_code ec,
                  const tcp::endpoint& endpoint);

  template <typename Executor, typename Alloc>
  struct ConnectHandler;

  template <typename Executor, typename Alloc>
  void on_wait(const Executor& ex, Alloc alloc,
               boost::system::error_code ec,
               std::unique_ptr<Completion> completion,
               std::optional<Connection> conn);

  template <typename Executor, typename Alloc>
  struct WaitHandler;

  void put(Connection&& connection);
  void put(boost::system::error_code ec);
};

// handler wrapper that notifies a callback on errors
template <typename Handler, typename Callback>
struct ErrorCallbackHandler {
  Handler handler;
  Callback callback;
  ErrorCallbackHandler(Handler&& handler, Callback&& callback)
    : handler(std::move(handler)), callback(std::move(callback)) {}
  template <typename ...Args>
  void operator()(boost::system::error_code ec, Args&& ...args) {
    if (ec) { callback(ec); }
    handler(ec, std::forward<Args>(args)...);
  }
  using allocator_type = boost::asio::associated_allocator_t<Handler>;
  allocator_type get_allocator() const noexcept {
    return boost::asio::get_associated_allocator(handler);
  }
};

template <typename T>
class value_ptr {
  T* p;
 public:
  value_ptr(T* p = nullptr) noexcept : p(p) {}
  value_ptr(value_ptr&& o) noexcept : p(std::exchange(o.p, nullptr)) {}
  value_ptr& operator=(value_ptr&& o) noexcept {
    p = std::exchange(o.p, nullptr);
    return *this;
  }
  operator bool() const { return static_cast<bool>(p); }
  T* get() { return p; }
  const T* get() const { return p; }
  T* operator->() { return p; }
  const T* operator->() const { return p; }
  T& operator*() { return *p; }
  const T& operator*() const { return *p; }
};

template <typename Stream, typename Clock>
class ConnectionPool<Stream, Clock>::Connection : public TimedStream<Stream, Clock> {
  value_ptr<ConnectionPool> pool;

  void on_error(boost::system::error_code ec) {
    if (!ec || ec == boost::asio::error::operation_aborted) {
      return;
    }
    if (auto p = std::move(pool); p) {
      p->put(ec);
    }
  }
  template <typename Handler>
  auto make_error_handler(Handler&& handler) {
    return ErrorCallbackHandler(std::move(handler),
      [this] (boost::system::error_code ec) {
        return on_error(ec);
      });
  }
 public:
  Connection(ConnectionPool* pool, Stream&& stream,
             Timer&& timer, ceph::timespan timeout)
    : TimedStream<Stream, Clock>(std::move(stream), std::move(timer), timeout),
      pool(pool)
  {}
  ~Connection() {
    if (pool) {
      pool->put(std::move(*this));
    }
  }
  Connection(Connection&&) = default;
  Connection& operator=(Connection&&) = default;

  template <typename MutableBufferSequence, typename CompletionToken>
  auto async_read_some(const MutableBufferSequence& buffers,
                       CompletionToken&& token) {
    using Signature = void(boost::system::error_code, size_t);
    boost::asio::async_completion<CompletionToken, Signature> init(token);
    auto h = make_error_handler(std::move(init.completion_handler));
    TimedStream<Stream, Clock>::async_read_some(buffers, std::move(h));
    return init.result.get();
  }

  template <typename ConstBufferSequence, typename CompletionToken>
  auto async_write_some(const ConstBufferSequence& buffers,
                        CompletionToken&& token) {
    using Signature = void(boost::system::error_code, size_t);
    boost::asio::async_completion<CompletionToken, Signature> init(token);
    auto h = make_error_handler(std::move(init.completion_handler));
    TimedStream<Stream, Clock>::async_write_some(buffers, std::move(h));
    return init.result.get();
  }

  void close() {
    TimedStream<Stream, Clock>::close();
    on_error(boost::asio::error::connection_aborted);
  }

  void close(boost::system::error_code& ec) {
    TimedStream<Stream, Clock>::close(ec);
    on_error(boost::asio::error::connection_aborted);
  }
};

template <typename Stream, typename Clock>
template <typename CompletionToken>
auto ConnectionPool<Stream, Clock>::async_get(CompletionToken&& token)
{
  boost::asio::async_completion<CompletionToken, Signature> init(token);
  auto& handler = init.completion_handler;
  auto ex1 = context.get_executor();
  auto ex2 = boost::asio::get_associated_executor(handler, ex1);
  auto alloc = boost::asio::get_associated_allocator(handler);
  {
    auto lock = std::scoped_lock{mutex};
    if (!idle.empty()) {
      auto socket = std::move(idle.front());
      idle.pop_front();
      auto conn = Connection(this, std::move(socket),
                             Timer(context), idle_timeout);
      // post a successful completion
      boost::system::error_code ec;
      auto h = async::bind_handler(std::move(handler), ec, std::move(conn));
      ex2.post(async::forward_handler(std::move(h)), alloc);
    } else if (count == max_connections) {
      auto completion = Completion::create(ex1, std::move(handler),
                                           context, Clock::zero(),
                                           State::Waiting);
      // wait for the next connection
      auto waiter = WaitCompletion::create(ex1, WaitHandler(ex2, alloc, *this),
                                           std::move(completion));
      // transfer ownership to the waiters list
      waiters.push_back(*waiter.release());
    } else {
      count++;
      auto completion = Completion::create(ex1, std::move(handler),
                                           context, Clock::now(),
                                           State::Resolving);
      auto& c = *completion;
      connecting.push_back(c);

      if (connect_timeout.count()) {
        c.timer.expires_at(c.started + connect_timeout);
        c.timer.async_wait(async::CancelHandler(ex2, alloc, c.resolver));
      }
      auto h = ResolveHandler(ex2, alloc, *this, std::move(completion));
      c.resolver.async_resolve(host, service, std::move(h));
    }
  }
  return init.result.get();
}

template <typename Stream, typename Clock>
void ConnectionPool<Stream, Clock>::put(Connection&& connection)
{
  std::scoped_lock lock(mutex);

  if (!waiters.empty()) {
    auto& c = waiters.front();
    waiters.pop_front();
    auto wait_completion = std::unique_ptr<WaitCompletion>{&c};
    auto completion = std::move(c.completion);
    async::post(std::move(wait_completion), boost::system::error_code{},
                std::move(completion), std::move(connection));
  } else {
    --count;
    idle.push_back(std::move(connection.next_layer()));
  }
}

template <typename Stream, typename Clock>
void ConnectionPool<Stream, Clock>::put(boost::system::error_code ec)
{
  std::scoped_lock lock(mutex);

  if (!waiters.empty()) {
    auto& c = waiters.front();
    waiters.pop_front();
    auto wait_completion = std::unique_ptr<WaitCompletion>{&c};
    auto completion = std::move(c.completion);
    ec.clear(); // will reconnect to recover
    async::post(std::move(wait_completion), ec,
                std::move(completion), std::nullopt);
  } else {
    --count;
  }
}

template <typename Stream, typename Clock>
void ConnectionPool<Stream, Clock>::close()
{
  boost::intrusive::list<WaitCompletion> cancel_waiters;
  {
    std::scoped_lock lock(mutex);

    for (auto& c : connecting) {
      if (c.state == State::Resolving) {
        c.resolver.cancel();
      } else if (c.state == State::Connecting) {
        c.socket.close();
      }
      c.state = State::Canceled;
    }
    cancel_waiters = std::move(waiters);
  }

  boost::system::error_code ec = boost::asio::error::operation_aborted;
  while (!cancel_waiters.empty()) {
    auto& c = cancel_waiters.front();
    cancel_waiters.pop_front();
    auto completion = std::move(c.completion);
    async::dispatch(std::unique_ptr<WaitCompletion>{&c}, ec,
                    std::move(completion), std::nullopt);
  }
}

template <typename Stream, typename Clock>
template <typename Executor, typename Alloc>
void ConnectionPool<Stream, Clock>::on_resolve(const Executor& ex, Alloc alloc,
                                               std::unique_ptr<Completion>&& completion,
                                               boost::system::error_code ec,
                                               const tcp::resolver::results_type& addrs)
{
  std::scoped_lock lock(mutex);

  auto& c = *completion;
  if (!ec && c.state == State::Canceled) {
    ec = boost::asio::error::operation_aborted;
  }
  if (ec) {
    // TODO: retries
    connecting.erase(connecting.iterator_to(c));
    async::defer(std::move(completion), ec, std::nullopt);
    return;
  }
  c.state = State::Connecting;
  if (connect_timeout.count()) {
    c.timer.expires_at(c.started + connect_timeout);
    c.timer.async_wait(async::CancelHandler(ex, alloc, c.socket));
  }
  auto h = ConnectHandler(ex, alloc, *this, std::move(completion));
  boost::asio::async_connect(c.socket, addrs, std::move(h));
}

template <typename Stream, typename Clock>
template <typename Executor, typename Alloc>
struct ConnectionPool<Stream, Clock>::ResolveHandler {
  Executor ex;
  Alloc alloc;
  ConnectionPool& pool;
  std::unique_ptr<Completion> completion;
  ResolveHandler(const Executor& ex, Alloc alloc, ConnectionPool& pool,
                 std::unique_ptr<Completion> completion)
    : ex(ex), alloc(alloc), pool(pool), completion(std::move(completion))
  {}
  void operator()(boost::system::error_code ec,
                  tcp::resolver::results_type addrs) {
    pool.on_resolve(ex, alloc, std::move(completion), ec, addrs);
  }
  using executor_type = Executor;
  executor_type get_executor() const noexcept { return ex; }
  using allocator_type = Alloc;
  allocator_type get_allocator() const noexcept { return alloc; }
};

template <typename Stream, typename Clock>
template <typename Executor, typename Alloc>
void ConnectionPool<Stream, Clock>::on_connect(const Executor& ex, Alloc alloc,
                                               std::unique_ptr<Completion>&& completion,
                                               boost::system::error_code ec,
                                               const tcp::endpoint& endpoint)
{
  std::scoped_lock lock(mutex);

  auto& c = *completion;
  if (!ec && c.state == State::Canceled) {
    ec = boost::asio::error::operation_aborted;
  }
  if (ec) {
    // TODO: retries
    connecting.erase(connecting.iterator_to(c));
    async::defer(std::move(completion), ec, std::nullopt);
    return;
  }
  // TODO: if constexpr(ssl)

  auto socket = std::move(c.socket);
  auto timer = std::move(c.timer);
  timer.cancel();

  connecting.erase(connecting.iterator_to(c));

  std::optional<Connection> conn;
  if (!ec) {
    if (c.state == State::Canceled) {
      ec = boost::asio::error::operation_aborted;
    } else {
      // TODO: handshake if ssl
      conn.emplace(this, std::move(socket), std::move(timer), idle_timeout);
    }
  }
  async::defer(std::move(completion), ec, std::move(conn));
}

template <typename Stream, typename Clock>
template <typename Executor, typename Alloc>
struct ConnectionPool<Stream, Clock>::ConnectHandler {
  Executor ex;
  Alloc alloc;
  ConnectionPool& pool;
  std::unique_ptr<Completion> completion;
  ConnectHandler(const Executor& ex, Alloc alloc, ConnectionPool& pool,
                 std::unique_ptr<Completion>&& completion)
    : ex(ex), alloc(alloc), pool(pool), completion(std::move(completion))
  {}
  void operator()(boost::system::error_code ec, const tcp::endpoint& endpoint) {
    pool.on_connect(ex, alloc, std::move(completion), ec, endpoint);
  }
  using executor_type = Executor;
  executor_type get_executor() const noexcept { return ex; }
  using allocator_type = Alloc;
  allocator_type get_allocator() const noexcept { return alloc; }
};

template <typename Stream, typename Clock>
template <typename Executor, typename Alloc>
void ConnectionPool<Stream, Clock>::on_wait(const Executor& ex, Alloc alloc,
                                            boost::system::error_code ec,
                                            std::unique_ptr<Completion> completion,
                                            std::optional<Connection> conn)
{
  auto& c = *completion;
  if (ec) {
    async::dispatch(std::move(completion), ec, std::nullopt);
  } else if (conn) {
    async::dispatch(std::move(completion), ec, std::move(conn));
  } else {
    std::scoped_lock lock(mutex);

    c.started = Clock::now();
    c.state = State::Resolving;
    connecting.push_back(c);

    if (connect_timeout.count()) {
      c.timer.expires_at(c.started + connect_timeout);
      c.timer.async_wait(async::CancelHandler(ex, alloc, c.resolver));
    }
    auto h = ResolveHandler(ex, alloc, *this, std::move(completion));
    c.resolver.async_resolve(host, service, std::move(h));
  }
}

template <typename Stream, typename Clock>
template <typename Executor, typename Alloc>
struct ConnectionPool<Stream, Clock>::WaitHandler {
  Executor ex;
  Alloc alloc;
  ConnectionPool& pool;
  WaitHandler(const Executor& ex, Alloc alloc, ConnectionPool& pool)
    : ex(ex), alloc(alloc), pool(pool) {}
  void operator()(boost::system::error_code ec,
                  std::unique_ptr<Completion> completion,
                  std::optional<Connection> conn) {
    pool.on_wait(ex, alloc, ec, std::move(completion), std::move(conn));
  }
  using executor_type = Executor;
  executor_type get_executor() const noexcept { return ex; }
  using allocator_type = Alloc;
  allocator_type get_allocator() const noexcept { return alloc; }
};

} // namespace rgw::http

namespace boost::asio {

// associated_executor trait for Connection::ErrorHandler
template <typename Handler, typename Callback, typename Executor>
struct associated_executor<rgw::http::ErrorCallbackHandler<Handler, Callback>, Executor> {
  using type = associated_executor_t<Handler, Executor>;
  static type get(const rgw::http::ErrorCallbackHandler<Handler, Callback>& h,
                  const Executor& ex = Executor()) noexcept {
    return get_associated_executor(h.handler, ex);
  }
};

} // namespace boost::asio
