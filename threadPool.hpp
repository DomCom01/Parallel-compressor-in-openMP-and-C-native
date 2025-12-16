#pragma once

#include <vector>
#include <deque>
#include <future>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <algorithm>
#include <chrono>

#include "taskFactory.hpp"
#include "affinity.hpp"


class threadPool {
#if !defined(__cpp_lib_move_only_function) || __cpp_lib_move_only_function < 202110
  using Task = std::function<void()>;
#else
  using Task = std::move_only_function<void()>;
#endif	
  static constexpr std::chrono::microseconds SPIN_MICROSECONDS{200};
  
  std::vector<std::jthread> workers_;
  std::vector<unsigned> affinity_;
  std::mutex m_;
  std::condition_variable cv_;
  std::condition_variable cv_wait_;
  bool done_ = false;
  std::deque<Task> q_;
  std::atomic<std::size_t> inflight_{0};

  void worker_loop() {
	for(;;) {
	  Task task;
	  {
		std::unique_lock<std::mutex> lk(m_);
		cv_.wait(lk, [&]{ return done_ || !q_.empty(); });
		if (done_ && q_.empty()) return;
		task = std::move(q_.front());
		q_.pop_front();
	  }
	  
	  task();
	  
	  // if the task was the last one inflight, signal idle
	  dec_inflight_and_notify();
	}
  }
  
  // we must decrement inflight_ under m_ (the same mutex used in wait_completion())
  // otherwise a Worker could set inflight_ to 0 and notify just before the waiting
  // thread go to sleep (i.e., enters cv_wait_.wait()), causing a lost wake-up
  void dec_inflight_and_notify() noexcept {
	std::unique_lock<std::mutex> lk(m_);
	if (inflight_.fetch_sub(1, std::memory_order_release) == 1) {
	  lk.unlock();
	  cv_wait_.notify_all();
	} 
  }
	
public:
  explicit threadPool(unsigned n = std::max(1u, std::thread::hardware_concurrency()),
					  std::string affinity={}) {
	if (n==0) throw std::invalid_argument("threadPool: n must be >0");
	workers_.reserve(n);
	affinity_ = affinity::parse_cpu_list(affinity);
	
	for (unsigned i=0; i<n; ++i) {
	  // Choose CPU for this worker if provided (wrap-around if fewer CPUs)
	  int cpu = -1;
	  if (!affinity_.empty())
		cpu = static_cast<int>(affinity_[i % affinity_.size()]);
	  
	  workers_.emplace_back([this, cpu, i, AFF=affinity::get_current_cpu]{
		if (cpu>=0) affinity::pin_thread_to_core(cpu);
		worker_loop();
	  });
	}
  }
  threadPool(const threadPool&)            = delete;
  threadPool& operator=(const threadPool&) = delete;
  threadPool(threadPool&&)                 = delete;
  threadPool& operator=(threadPool&&)      = delete;
  
  ~threadPool() {
	wait_completion();
	{
	  std::lock_guard<std::mutex> lk(m_);
	  done_ = true;
	}
	cv_.notify_all();
	
	// wait for the termination af all workers_
	for (auto& thread : workers_)
	  thread.join();
  }
  
  template<class F, class... Args>
  auto submit(F&& f, Args&&... args)
	-> std::future<std::invoke_result_t<F, Args...>> {
	static_assert(std::is_invocable_v<F, Args...>, "F(Args...) not callable");
	
	auto [task,future] = make_task(std::forward<F>(f),std::forward<Args>(args)...);
	{
	  std::lock_guard<std::mutex> lk(m_);
	  if (done_)
		throw std::runtime_error("threadPool: submit() after object destruction\n");
	  q_.emplace_back(std::move(task));
	  inflight_.fetch_add(1,std::memory_order_relaxed);
	}
	cv_.notify_one();
	return future;
  }
  // wait for all tasks to be completed
  void wait_completion() {        
	std::unique_lock<std::mutex> lk(m_);
	cv_wait_.wait(lk, [&]() { return inflight_.load(std::memory_order_acquire)==0; });
  }
  
  
  // Cooperative wait (helping), if the future is not ready, it gets tasks from pool
  template<class T>
  T wait_future(std::future<T>& fut) {
	using namespace std::chrono_literals;
	for (;;) {
	  if (fut.wait_for(0s) == std::future_status::ready)
		break;
	  
	  Task task;
	  {
		std::unique_lock<std::mutex> lk(m_);
		if (q_.empty()) {
		  cv_.wait_for(lk, SPIN_MICROSECONDS, [&]{
			return done_ || !q_.empty()
			  || fut.wait_for(0s) == std::future_status::ready;
		  });
		  continue;
		}
		task = std::move(q_.front());
		q_.pop_front();
	  }
	  
	  task();
	  dec_inflight_and_notify();
	}
	return fut.get();
  }
  
  // Overload for future<void>
  void wait_future(std::future<void>& fut) {
	using namespace std::chrono_literals;
	for (;;) {
	  if (fut.wait_for(0s) == std::future_status::ready)
		break;
	  
	  Task task;
	  {
		std::unique_lock<std::mutex> lk(m_);
		if (q_.empty()) {
		  cv_.wait_for(lk, SPIN_MICROSECONDS, [&]{
			return done_ || !q_.empty()
			  || fut.wait_for(0s) == std::future_status::ready;
		  });
		  continue;
		}
		task = std::move(q_.front());
		q_.pop_front();
	  }
	  
	  task();
	  dec_inflight_and_notify();
	}
	fut.get();
  }
};
