#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <utility>

namespace amm {

template <class T> class BoundedMpscQueue {
public:
    explicit BoundedMpscQueue(std::size_t capacity) : capacity_(capacity) {
        if (capacity == 0) throw std::invalid_argument("queue capacity must be positive");
    }

    bool push(T value, const std::atomic_bool& stop_requested) {
        std::unique_lock lock(mutex_);
        if (queue_.size() >= capacity_ && !closed_ && !stop_requested.load(std::memory_order_relaxed)) {
            ++blocked_pushes_;
        }
        not_full_.wait(lock, [&] {
            return queue_.size() < capacity_ || closed_ || stop_requested.load(std::memory_order_relaxed);
        });
        if (closed_ || stop_requested.load(std::memory_order_relaxed)) return false;
        if constexpr (requires(T t) { t.enqueue_sequence; }) {
            value.enqueue_sequence = next_sequence_++;
        }
        queue_.push_back(std::move(value));
        if (queue_.size() > high_water_) high_water_ = queue_.size();
        not_empty_.notify_one();
        return true;
    }

    std::optional<T> pop() {
        std::unique_lock lock(mutex_);
        not_empty_.wait(lock, [&] { return !queue_.empty() || closed_; });
        if (queue_.empty()) return std::nullopt;
        T value = std::move(queue_.front());
        queue_.pop_front();
        not_full_.notify_one();
        return value;
    }

    void close() {
        std::lock_guard lock(mutex_);
        closed_ = true;
        not_empty_.notify_all();
        not_full_.notify_all();
    }

    void wake_all() {
        std::lock_guard lock(mutex_);
        not_empty_.notify_all();
        not_full_.notify_all();
    }

    [[nodiscard]] std::size_t size() const { std::lock_guard lock(mutex_); return queue_.size(); }
    [[nodiscard]] std::size_t high_water_mark() const { std::lock_guard lock(mutex_); return high_water_; }
    [[nodiscard]] std::uint64_t blocked_pushes() const { std::lock_guard lock(mutex_); return blocked_pushes_; }

private:
    const std::size_t capacity_;
    mutable std::mutex mutex_;
    std::condition_variable not_empty_;
    std::condition_variable not_full_;
    std::deque<T> queue_;
    bool closed_{};
    std::uint64_t next_sequence_{1};
    std::size_t high_water_{};
    std::uint64_t blocked_pushes_{};
};

} // namespace amm

