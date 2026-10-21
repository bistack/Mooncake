#include "batch_io_scheduler.h"

#include <algorithm>
#include <condition_variable>
#include <list>
#include <mutex>
#include <new>
#include <optional>
#include <thread>
#include <utility>
#include <vector>

namespace mooncake {

class BatchIoScheduler::Impl {
   public:
    struct Batch {
        std::optional<ErrorCode> error;
        size_t queued = 0;
        size_t submitted = 0;
        size_t completed = 0;
        bool submission_finished = false;
        std::condition_variable done;
    };
    struct Task {
        std::shared_ptr<Batch> batch;
        SubBatch sub_batch;
    };
    tl::expected<void, ErrorCode> Init(size_t jobs) {
        try {
            workers_.resize(jobs);
            for (size_t i = 0; i < jobs; ++i) {
                workers_[i] = std::thread([this] { Work(); });
            }
        } catch (...) {
            Shutdown();
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        return {};
    }

    ~Impl() { Shutdown(); }

    tl::expected<void, ErrorCode> SubmitAndWait(
        const std::shared_ptr<Batch>& batch, std::list<Task>& tasks) {
        if (tasks.empty()) return {};
        std::unique_lock<std::mutex> lock(mutex_);
        if (stopping_) {
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
        batch->queued = tasks.size();
        queue_.splice(queue_.end(), tasks);
        changed_.notify_all();
        batch->done.wait(lock, [&] {
            return batch->submission_finished &&
                   batch->submitted == batch->completed;
        });
        if (batch->error) return tl::make_unexpected(*batch->error);
        return {};
    }

    void Shutdown() {
        std::lock_guard<std::mutex> shutdown_lock(shutdown_mutex_);
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stopping_ = true;
            while (!queue_.empty()) {
                FailLocked(queue_.front().batch,
                           ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
            }
            changed_.notify_all();
        }
        for (auto& worker : workers_) {
            if (worker.joinable()) worker.join();
        }
    }

   private:
    void FailLocked(std::shared_ptr<Batch> batch, ErrorCode error) {
        if (!batch->error) batch->error = error;
        for (auto it = queue_.begin(); it != queue_.end();) {
            if (it->batch == batch) {
                it = queue_.erase(it);
            } else {
                ++it;
            }
        }
        batch->queued = 0;
        batch->submission_finished = true;
        batch->done.notify_all();
    }

    void Work() {
        std::unique_lock<std::mutex> lock(mutex_);
        for (;;) {
            changed_.wait(lock, [&] { return stopping_ || !queue_.empty(); });
            if (stopping_) return;
            auto task = std::move(queue_.front());
            queue_.pop_front();
            ++task.batch->submitted;
            if (--task.batch->queued == 0)
                task.batch->submission_finished = true;
            lock.unlock();
            tl::expected<void, ErrorCode> result;
            try {
                result = task.sub_batch.execute(task.sub_batch.slice_idx_begin,
                                                task.sub_batch.slice_idx_end);
            } catch (...) {
                result = tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }
            lock.lock();
            if (!result) FailLocked(task.batch, result.error());
            ++task.batch->completed;
            task.batch->done.notify_all();
        }
    }

    std::mutex mutex_;
    std::mutex shutdown_mutex_;
    std::condition_variable changed_;
    bool stopping_ = false;
    std::list<Task> queue_;
    std::vector<std::thread> workers_;
};

tl::expected<std::unique_ptr<BatchIoScheduler>, ErrorCode>
BatchIoScheduler::Create(size_t jobs) {
    if (jobs == 0) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    try {
        auto impl = std::make_unique<Impl>();
        auto result = impl->Init(jobs);
        if (!result) return tl::make_unexpected(result.error());
        return std::unique_ptr<BatchIoScheduler>(
            new BatchIoScheduler(std::move(impl)));
    } catch (...) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
}

BatchIoScheduler::BatchIoScheduler(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
BatchIoScheduler::~BatchIoScheduler() = default;

struct BatchIoScheduler::PendingBatch::State {
    std::shared_ptr<Impl::Batch> batch = std::make_shared<Impl::Batch>();
    std::list<Impl::Task> tasks;
};

BatchIoScheduler::PendingBatch::PendingBatch() = default;
BatchIoScheduler::PendingBatch::~PendingBatch() = default;

tl::expected<void, ErrorCode> BatchIoScheduler::AddSubBatch(
    PendingBatch& batch, SubBatch&& sub_batch) {
    if (batch.error_) return tl::make_unexpected(*batch.error_);
    if (sub_batch.slice_idx_end < sub_batch.slice_idx_begin ||
        !sub_batch.execute) {
        batch.error_ = ErrorCode::INVALID_PARAMS;
    } else {
        try {
            if (!batch.state_)
                batch.state_ = std::make_unique<PendingBatch::State>();
            batch.state_->tasks.push_back(
                Impl::Task{batch.state_->batch, std::move(sub_batch)});
            return {};
        } catch (const std::bad_alloc&) {
            batch.error_ = ErrorCode::NO_AVAILABLE_HANDLE;
        } catch (...) {
            batch.error_ = ErrorCode::INTERNAL_ERROR;
        }
    }
    return tl::make_unexpected(*batch.error_);
}

tl::expected<void, ErrorCode> BatchIoScheduler::PrepareSubBatches(
    PendingBatch& batch, size_t slice_count, size_t sub_batch_size,
    const Execute& execute) {
    if (batch.error_) return tl::make_unexpected(*batch.error_);
    if (sub_batch_size < 4 || !execute) {
        batch.error_ = ErrorCode::INVALID_PARAMS;
        return tl::make_unexpected(*batch.error_);
    }
    try {
        for (size_t begin = 0; begin < slice_count;) {
            size_t end = begin + std::min(sub_batch_size, slice_count - begin);
            // Merge the small tail before allocating the final task node.
            if (slice_count - end <= sub_batch_size / 4) end = slice_count;
            auto result = AddSubBatch(batch, SubBatch{begin, end, execute});
            if (!result) return result;
            begin = end;
        }
        return {};
    } catch (const std::bad_alloc&) {
        batch.error_ = ErrorCode::NO_AVAILABLE_HANDLE;
    } catch (...) {
        batch.error_ = ErrorCode::INTERNAL_ERROR;
    }
    return tl::make_unexpected(*batch.error_);
}

tl::expected<void, ErrorCode> BatchIoScheduler::SubmitAndWait(
    PendingBatch& batch) {
    auto state = std::move(batch.state_);
    auto error = std::exchange(batch.error_, std::nullopt);
    if (error) return tl::make_unexpected(*error);
    if (!state) return {};
    return impl_->SubmitAndWait(state->batch, state->tasks);
}

void BatchIoScheduler::Shutdown() { impl_->Shutdown(); }

}  // namespace mooncake
