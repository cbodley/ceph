#pragma once

#include <atomic>
#include <limits>
#include <memory>

#include <boost/asio/basic_waitable_timer.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include "common/ceph_time.h"

namespace rgw {

// a thread-safe pool with 'Count' blocks of 'Size' bytes each. throws bad_alloc
// for requests to allocate more than Size at a time, or if all blocks are
// already allocated
template <size_t Count, size_t Size, size_t BlockAlign = 8>
class circular_buffered_pool {
  using size_type = uint32_t;
  static_assert(Size <= std::numeric_limits<size_type>::max());
  using index_type = uint_fast8_t;
  static_assert(Count <= std::numeric_limits<index_type>::max());

  struct alignas(BlockAlign) block {
    char data[Size];
    // size of 0 is unallocated. allocations try an atomic compare-exchange
    // to replace 0 with the allocation size
    std::atomic<size_type> size = 0;
  };
  block blocks[Count];

  // successful allocations at a given block index will update 'next' to point
  // at the next block index, because that block is the most likely to be free.
  // 'next' only serves as a hint, so is not itself atomic
  index_type next = 0;

 public:
  ~circular_buffered_pool() {
    for (const auto& b : blocks) {
      assert(!b.size); // destructed before deallocate
    }
  }

  void* allocate(size_t n) {
    if (n == 0 || n > sizeof(block::data)) {
      throw std::bad_alloc();
    }
    // search for a free block, starting from 'next'
    const size_t pos = next;
    for (size_t tries = 0; tries < Count; ++tries) {
      const size_t index = (pos + tries) % Count;
      auto b = &blocks[index];
      size_type zero = 0; // try to exchange 0 with n
      if (b->size.compare_exchange_strong(zero, n)) {
        next = (index + 1) % Count;
        return b;
      }
    }
    throw std::bad_alloc();
  }

  void deallocate(void* p, uint32_t n) {
    auto b = static_cast<block*>(p);
    const bool deallocated = b->size.compare_exchange_strong(n, 0);
    assert(deallocated); // assert that the previous value was n
  }
};

// size of boost::asio::detail::wait_handler<timeout_handler<Stream,Alloc>>
static constexpr size_t max_timeout_handler_size = 64;

// a pool that can allocate up to 4 timeout handlers at once. we generally only
// have one or two outstanding: the 'current' unexpired handler, and sometimes
// the previous handler whose cancelation is pending. double that for headroom
using basic_timer_pool = circular_buffered_pool<4, max_timeout_handler_size>;


// timeout handlers may outlive their timer. when basic_waitable_timer
// destructs, it cancels any outstanding timers and schedules their handlers on
// the executor. but the executor probably won't execute them inline. the
// timer_pool's lifetime must be extended past the destruction of the timer
// itself, in order to free the memory of these canceled handlers
template <typename T>
using ref_counter = boost::intrusive_ref_counter<T, boost::thread_safe_counter>;

// combines the basic_timer_pool with an atomic reference count
struct timer_pool : ref_counter<timer_pool>, basic_timer_pool {
  using basic_timer_pool::basic_timer_pool;
};

using timer_pool_ptr = boost::intrusive_ptr<timer_pool>;


// a custom allocator that uses a timer_pool for timer handler allocations
template <typename T>
struct timer_allocator {
  timer_pool_ptr pool;
 public:
  using value_type = T;

  timer_allocator(timer_pool_ptr pool = nullptr) noexcept
      : pool(std::move(pool)) {}

  // supports copy, move, converting copy, and converting move
  timer_allocator(const timer_allocator&) = default;
  timer_allocator& operator=(const timer_allocator&) = default;

  timer_allocator(timer_allocator&&) = default;
  timer_allocator& operator=(timer_allocator&&) = default;

  template <typename U>
  timer_allocator(const timer_allocator<U>& o) noexcept
      : pool(o.pool) {}
  template <typename U>
  timer_allocator& operator=(const timer_allocator<U>& o) noexcept {
    pool = o.pool;
    return *this;
  }

  template <typename U>
  timer_allocator(timer_allocator<U>&& o) noexcept
      : pool(std::move(o.pool)) {}
  template <typename U>
  timer_allocator& operator=(timer_allocator<U>&& o) noexcept {
    pool = std::move(o.pool);
    return *this;
  }

  T* allocate(size_t n) {
    return static_cast<T*>(pool->allocate(n * sizeof(T)));
  }
  void deallocate(T* p, size_t n) {
    pool->deallocate(p, n * sizeof(T));
  }

  friend bool operator==(const timer_allocator& lhs,
                         const timer_allocator& rhs) {
    return lhs.pool == rhs.pool;
  }
  friend bool operator!=(const timer_allocator& lhs,
                         const timer_allocator& rhs) {
    return lhs.pool != rhs.pool;
  }
};

// an allocator-aware WaitHandler that closes a stream if the timeout expires
template <typename Stream, typename Alloc>
struct timeout_handler {
  Stream* stream;
  Alloc alloc;

  timeout_handler(Stream* stream, const Alloc& alloc)
      : stream(stream), alloc(alloc)
  {}

  void operator()(boost::system::error_code ec) {
    if (!ec) { // wait was not canceled
      stream->close();
    }
  }

  using allocator_type = Alloc;
  const allocator_type& get_allocator() const noexcept {
    return alloc;
  }
};

// a timeout timer for stream operations
template <typename Clock>
class basic_timeout_timer {
 public:
  using clock_type = Clock;
  using duration = typename clock_type::duration;

  // timer_pool is tuned to the allocation patterns of this specific executor
  using executor_type = boost::asio::io_context::executor_type;

  explicit basic_timeout_timer(const executor_type& ex)
      : timer(ex), pool(new timer_pool) {}

  basic_timeout_timer(const basic_timeout_timer&) = delete;
  basic_timeout_timer& operator=(const basic_timeout_timer&) = delete;

  template <typename Stream>
  void expires_after(Stream& stream, duration dur) {
    timer.expires_after(dur);
    timer.async_wait(timeout_handler{&stream, timer_allocator<char>{pool}});
  }

  void cancel() {
    timer.cancel();
  }

 private:
  using Timer = boost::asio::basic_waitable_timer<clock_type,
        boost::asio::wait_traits<clock_type>, executor_type>;
  Timer timer;
  timer_pool_ptr pool;
};

} // namespace rgw
