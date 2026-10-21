#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <optional>

#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

// Executes batches synchronously. Callers keep referenced plans and buffers
// valid until SubmitAndWait returns. Callback failures are returned as
// ErrorCode.
class BatchIoScheduler {
   public:
    using Execute = std::function<tl::expected<void, ErrorCode>(
        size_t slice_idx_begin, size_t slice_idx_end)>;

    // Create a scheduler with jobs > 0 concurrent workers.
    // Returns INVALID_PARAMS for zero jobs, INTERNAL_ERROR on startup failure.
    static tl::expected<std::unique_ptr<BatchIoScheduler>, ErrorCode> Create(
        size_t jobs);
    ~BatchIoScheduler();
    BatchIoScheduler(const BatchIoScheduler&) = delete;
    BatchIoScheduler& operator=(const BatchIoScheduler&) = delete;

    // Private, unpublished tasks. Destroying this object cancels them without
    // executing callbacks. Do not access the same batch concurrently.
    class PendingBatch {
       public:
        PendingBatch();
        ~PendingBatch();
        PendingBatch(const PendingBatch&) = delete;
        PendingBatch& operator=(const PendingBatch&) = delete;

       private:
        friend class BatchIoScheduler;
        struct State;
        std::unique_ptr<State> state_;
        std::optional<ErrorCode> error_;
    };

    // Split one file's read plans into groups of sub_batch_size (>= 4).
    // A final group of at most floor(sub_batch_size / 4) Slices joins the
    // previous group. Append directly to unpublished task nodes.
    // No callback runs here. Any error invalidates the entire pending batch.
    tl::expected<void, ErrorCode> PrepareSubBatches(PendingBatch& batch,
                                                    size_t slice_count,
                                                    size_t sub_batch_size,
                                                    const Execute& execute);
    // Publish all prepared tasks without allocating, then wait for every
    // started callback to finish. Consumes the batch, including on error.
    // Callbacks may execute concurrently and out of order; referenced plans
    // and buffers must remain valid until this returns. First callback errors
    // cancel queued tasks; callback exceptions return INTERNAL_ERROR.
    tl::expected<void, ErrorCode> SubmitAndWait(PendingBatch& batch);

    // Stop accepting work, cancel queued work, and join running callbacks.
    // Safe to call repeatedly; callers must still ensure
    // the scheduler object outlives every SubmitAndWait invocation.
    void Shutdown();

   private:
    // Slice-index range [slice_idx_begin, slice_idx_end) within one file's
    // read plan list for one sub batch.
    // Each index selects one whole Slice. This is not a buffer address range;
    // the selected Slices may point to separate, noncontiguous buffers.
    struct SubBatch {
        size_t slice_idx_begin;
        size_t slice_idx_end;
        Execute execute{};
    };

    // Append an already prepared range without publishing it.
    tl::expected<void, ErrorCode> AddSubBatch(PendingBatch& batch,
                                              SubBatch&& sub_batch);

    class Impl;
    explicit BatchIoScheduler(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace mooncake
