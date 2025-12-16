#pragma once
#include <future>
#include <functional>
#include <tuple>
#include <type_traits>

#if !defined(__cpp_lib_move_only_function) || __cpp_lib_move_only_function < 202110

// C++20 version: it uses std::function<void()> + shared_ptr

template<class F, class...Args>       // any callable F with zero or more arguments
auto make_task(F&& f, Args&&... args) // return a pair
-> std::pair<std::function<void()>,
             std::future<std::invoke_result_t<std::decay_t<F>, std::decay_t<Args>...>>> {
    using Fn = std::decay_t<F>;// normalize parameters (strip ref, const, array->ptr)
    using Tp = std::tuple<std::decay_t<Args>...>; //normalize arguments in a tuple
    using R  = std::invoke_result_t<Fn, std::decay_t<Args>...>; // get the result type

	// create an object on the heap and returns a std::shared_ptr;
	// the allocated object will be automatically deleted when there are no more
	// references to it. packaged_task is move-only, the shared_ptr is needed to put it
	// in a std::function<void()> that requires that the callable is copyable
    auto pkg = std::make_shared<std::packaged_task<R()>>(
        [fn = Fn(std::forward<F>(f)),
		 tp = Tp(std::forward<Args>(args)...)]() mutable -> R {
            if constexpr (std::is_void_v<R>)
			     std::apply(std::move(fn), std::move(tp));
            else
				return std::apply(std::move(fn), std::move(tp));
        });
    auto fut = pkg->get_future();
	// build the generic task; the lambda, when called, runs the package_task
    std::function<void()> task = [pkg = std::move(pkg)]() mutable { (*pkg)(); };
    return { std::move(task), std::move(fut) }; // return the task and its future
}
#else
// C++23 version: move_only_function<void()> and no shared_ptr needed
template<class F, class... Args>
auto make_task(F&& f, Args&&... args)
-> std::pair<std::move_only_function<void()>,
             std::future<std::invoke_result_t<std::decay_t<F>, std::decay_t<Args>...>>> {
    using Fn = std::decay_t<F>;
    using Tp = std::tuple<std::decay_t<Args>...>;
    using R  = std::invoke_result_t<Fn, std::decay_t<Args>...>;

    std::packaged_task<R()> pt(
        [fn = Fn(std::forward<F>(f)), tp = Tp(std::forward<Args>(args)...)]() mutable -> R {
            if constexpr (std::is_void_v<R>) std::apply(std::move(fn), std::move(tp));
            else return std::apply(std::move(fn), std::move(tp));
        });
    auto fut = pt.get_future();
    std::move_only_function<void()> task = std::move(pt);
    return { std::move(task), std::move(fut) };
}
#endif
