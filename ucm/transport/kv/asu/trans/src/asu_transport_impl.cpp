/**
 * MIT License
 *
 * Copyright (c) 2026 Huawei Technologies Co., Ltd. All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 * */
#include "asu_transport_impl.h"
#include <algorithm>
#include <chrono>
#include <memory>
#include <thread>
#include "asu_transport/asu_transport.h"
#include "asu_transport/types.h"
#include "connection_internal.h"
#include "debug_log.h"

namespace UC::ASU {

AsuTransportImpl::~AsuTransportImpl() { Shutdown(); }

Status AsuTransportImpl::Init(const TransportConfig& config)
{
    debug_log("AsuTransportImpl::Init", "start");
    if (worker_.joinable()) {
        debug_log("AsuTransportImpl::Init", "already initialized");
        return Status::OK();
    }
    config_ = config;

    std::uint32_t qp_num =
        config_.query_qp_num + config_.load_qp_num + config_.store_qp_num;
    debug_log("AsuTransportImpl::Init",
              "endpoints=" + std::to_string(config_.endpoints.size())
              + " qp_num=" + std::to_string(qp_num));
    for (const auto& ep : config_.endpoints) {
        auto s = conn_manager_.AddGroup(ep, qp_num);
        if (!s.ok()) {
            debug_log("AsuTransportImpl::Init", "AddGroup FAILED: " + s.message);
            return s;
        }
    }

    conn_manager_.StartRecoverLoop();

    auto queue_depth =
        std::max<std::size_t>(2, static_cast<std::size_t>(config_.max_inflight_tasks));
    execute_queue_.Setup(queue_depth + 1);
    stop_.store(false, std::memory_order_release);
    worker_ = std::thread(&AsuTransportImpl::WorkerLoop, this);
    debug_log("AsuTransportImpl::Init",
              "OK: queue_depth=" + std::to_string(queue_depth));
    return Status::OK();
}

Status AsuTransportImpl::Shutdown()
{
    debug_log("AsuTransportImpl::Shutdown", "start");
    if (!worker_.joinable()) {
        debug_log("AsuTransportImpl::Shutdown", "already shutdown");
        return Status::OK();
    }

    stop_.store(true, std::memory_order_release);
    debug_log("AsuTransportImpl::Shutdown", "stopping worker thread");
    if (worker_.joinable()) { worker_.join(); }

    task_manager_.DrainAll(TransportTaskState::CANCELED);

    conn_manager_.StopRecoverLoop();
    conn_manager_.Shutdown();
    debug_log("AsuTransportImpl::Shutdown", "OK");
    return Status::OK();
}

Status AsuTransportImpl::CheckHealth()
{
    return Status::OK();
}

Status AsuTransportImpl::Query(const std::vector<CacheKey>& keys, const QueryOptions& options,
                                QueryResult& result)
{
    TaskId task_id{kInvalidTaskId};
    auto status = QueryAsync(keys, options, task_id);
    if (!status.ok()) { return status; }

    TaskResult task_result;
    const auto timeout_ms = options.timeout_ms == 0 ? config_.query_timeout_ms : options.timeout_ms;
    status = Wait(task_id, timeout_ms, task_result);
    if (!status.ok()) { return status; }
    if (task_result.query_result.has_value()) { result = *task_result.query_result; }
    return task_result.status;
}

Status AsuTransportImpl::QueryAsync(const std::vector<CacheKey>& keys, const QueryOptions& options,
                                     TaskId& task_id)
{
    auto ctx = std::make_unique<TransportTaskContext>();
    ctx->op_type = TransportOpType::QUERY;
    ctx->keys = BatchView<CacheKey>{keys.data(), keys.size()};
    ctx->query_options = options;
    ctx->entry_status.assign(keys.size(), Status::OK());
    return SubmitAsync(std::move(ctx), task_id);
}

Status AsuTransportImpl::LoadAsync(const std::vector<KVBuffer>& entries, TaskId& task_id)
{
    auto ctx = std::make_unique<TransportTaskContext>();
    ctx->op_type = TransportOpType::LOAD;
    ctx->entries = BatchView<KVBuffer>{entries.data(), entries.size()};
    ctx->entry_status.assign(entries.size(), Status::OK());
    return SubmitAsync(std::move(ctx), task_id);
}

Status AsuTransportImpl::StoreAsync(const std::vector<KVBuffer>& entries, TaskId& task_id)
{
    auto ctx = std::make_unique<TransportTaskContext>();
    ctx->op_type = TransportOpType::STORE;
    ctx->entries = BatchView<KVBuffer>{entries.data(), entries.size()};
    ctx->entry_status.assign(entries.size(), Status::OK());
    return SubmitAsync(std::move(ctx), task_id);
}

Status AsuTransportImpl::DeleteAsync(const std::vector<CacheKey>& keys, TaskId& task_id)
{
    auto ctx = std::make_unique<TransportTaskContext>();
    ctx->op_type = TransportOpType::DELETE;
    ctx->keys = BatchView<CacheKey>{keys.data(), keys.size()};
    ctx->entry_status.assign(keys.size(), Status::OK());
    return SubmitAsync(std::move(ctx), task_id);
}

Status AsuTransportImpl::Cancel(TaskId task_id)
{
    return Status::Error(StatusCode::UNSUPPORTED, "cancel is not supported now");
}

Status AsuTransportImpl::Check(TaskId task_id, TaskResult& result)
{
    auto ctx = task_manager_.Get(task_id);
    if (!ctx) {
        debug_log("AsuTransportImpl::Check",
                  "task_id=" + std::to_string(task_id) + " NOT FOUND");
        return Status::Error(StatusCode::TASK_NOT_FOUND, "transport task not found");
    }
    std::unique_lock<std::mutex> lock(ctx->wait_mu);
    if (!ctx->Done()) {
        debug_log("AsuTransportImpl::Check",
                  "task_id=" + std::to_string(task_id) + " IN_PROGRESS");
        result.status = Status::Error(StatusCode::IN_PROGRESS, "transport task in progress");
        return Status::OK();
    }
    debug_log("AsuTransportImpl::Check",
              "task_id=" + std::to_string(task_id) + " DONE");
    BuildResult(*ctx, result);
    lock.unlock();
    task_manager_.Remove(task_id);
    return Status::OK();
}

Status AsuTransportImpl::Wait(TaskId task_id, std::uint64_t timeout_ms, TaskResult& result)
{
    debug_log("AsuTransportImpl::Wait",
              "task_id=" + std::to_string(task_id) + " timeout_ms=" + std::to_string(timeout_ms));
    auto ctx = task_manager_.Get(task_id);
    if (!ctx) {
        debug_log("AsuTransportImpl::Wait",
                  "task_id=" + std::to_string(task_id) + " NOT FOUND");
        return Status::Error(StatusCode::TASK_NOT_FOUND, "transport task not found");
    }

    std::unique_lock<std::mutex> lock(ctx->wait_mu);
    const bool done = timeout_ms == 0
                          ? (ctx->cv.wait(lock, [ctx] { return ctx->Done(); }), true)
                          : ctx->cv.wait_for(lock, std::chrono::milliseconds(timeout_ms),
                                             [ctx] { return ctx->Done(); });
    if (!done) {
        debug_log("AsuTransportImpl::Wait",
                  "task_id=" + std::to_string(task_id) + " TIMEOUT");
        auto prev_state = ctx->state.exchange(TransportTaskState::FAILED, std::memory_order_acq_rel);
        if (prev_state == TransportTaskState::INFLIGHT) {
            auto* ch = ctx->channel.load(std::memory_order_acquire);
            if (ch) {
                debug_log("AsuTransportImpl::Wait",
                          "timeout: prev_state=INFLIGHT, inflight-1 on ch_id="
                          + std::to_string(ch->channel_id));
                ch->inflight_count.fetch_sub(1, std::memory_order_relaxed);
            }
            BuildResult(*ctx, result);
            result.status = Status::Error(StatusCode::RESULT_TIMEOUT, "transport task result timeout");
            lock.unlock();
            task_manager_.Remove(task_id);
            return result.status;
        }
        debug_log("AsuTransportImpl::Wait",
                  "timeout: prev_state=PENDING, submit timeout (CompleteTask will CAS undo)");
        BuildResult(*ctx, result);
        result.status = Status::Error(StatusCode::SUBMIT_TIMEOUT, "transport task submit timeout");
        lock.unlock();
        task_manager_.Remove(task_id);
        return result.status;
    }
    debug_log("AsuTransportImpl::Wait",
              "task_id=" + std::to_string(task_id) + " DONE");
    BuildResult(*ctx, result);
    lock.unlock();
    task_manager_.Remove(task_id);
    return Status::OK();
}

Status AsuTransportImpl::RegisterRegions(const std::vector<MemoryRegion>& regions,
                                          std::vector<RegisterResult>& results)
{
    results.clear();
    results.assign(regions.size(), RegisterResult{Status::OK(), kInvalidMRHandle});
    return Status::OK();
}

Status AsuTransportImpl::BindRegisteredRegions(const std::vector<RegisteredMemory>& regions,
                                                std::vector<RegisterResult>& results)
{
    results.clear();
    results.assign(regions.size(), RegisterResult{Status::OK(), kInvalidMRHandle});
    return Status::OK();
}

Status AsuTransportImpl::UnregisterRegions(const std::vector<MRHandle>& handles)
{
    return Status::OK();
}

Status AsuTransportImpl::SubmitAsync(std::unique_ptr<TransportTaskContext> ctx, TaskId& task_id)
{
    debug_log("AsuTransportImpl::SubmitAsync",
              "op_type=" + std::to_string(static_cast<int>(ctx->op_type)));
    auto status = task_manager_.Submit(std::move(ctx), task_id);
    if (!status.ok()) {
        debug_log("AsuTransportImpl::SubmitAsync", "Submit FAILED: " + status.message);
        return status;
    }

    auto raw_ctx = task_manager_.Get(task_id);
    if (!raw_ctx) {
        debug_log("AsuTransportImpl::SubmitAsync", "ctx disappeared after submit");
        task_id = kInvalidTaskId;
        return Status::Error(StatusCode::INTERNAL_ERROR, "transport task disappeared after submit");
    }

    std::lock_guard<std::mutex> lock(producer_mu_);
    if (!execute_queue_.TryPush(std::move(raw_ctx))) {
        debug_log("AsuTransportImpl::SubmitAsync", "queue full");
        task_manager_.Remove(task_id);
        task_id = kInvalidTaskId;
        return Status::Error(StatusCode::RESOURCE_BUSY, "transport task queue is full");
    }
    debug_log("AsuTransportImpl::SubmitAsync",
              "OK: task_id=" + std::to_string(task_id));
    return Status::OK();
}

void AsuTransportImpl::WorkerLoop()
{
    debug_log("AsuTransportImpl::WorkerLoop", "started");
    execute_queue_.ConsumerLoop(stop_, [this](TransportTaskContextPtr ctx) {
        if (!ctx) { return; }
        CompleteTask(ctx);
    });
    debug_log("AsuTransportImpl::WorkerLoop", "stopped");
}

void AsuTransportImpl::CompleteTask(const TransportTaskContextPtr& ctx)
{
    int retries = 2;
    ConnectionChannel* channel = conn_manager_.SelectConnection();
    Status s;

    while (retries-- > 0 && channel) {
        if (ctx->state.load(std::memory_order_acquire) != TransportTaskState::PENDING) {
            debug_log("AsuTransportImpl::CompleteTask",
                      "task_id=" + std::to_string(ctx->task_id)
                      + " state!=PENDING (Wait timeout/Cancel), undo inflight on ch_id="
                      + std::to_string(channel->channel_id));
            channel->inflight_count.fetch_sub(1, std::memory_order_relaxed);
            return;
        }
        ctx->channel.store(channel, std::memory_order_release);
        s = channel->StubSend(ctx.get());
        if (s.ok()) {
            TransportTaskState expected = TransportTaskState::PENDING;
            if (!ctx->state.compare_exchange_strong(expected, TransportTaskState::INFLIGHT,
                                                     std::memory_order_acq_rel)) {
                debug_log("AsuTransportImpl::CompleteTask",
                          "task_id=" + std::to_string(ctx->task_id)
                          + " CAS PENDING->INFLIGHT failed (state="
                          + std::to_string(static_cast<int>(expected))
                          + "), undo inflight on ch_id="
                          + std::to_string(channel->channel_id));
                channel->inflight_count.fetch_sub(1, std::memory_order_relaxed);
                return;
            }
            debug_log("AsuTransportImpl::CompleteTask",
                      "task_id=" + std::to_string(ctx->task_id)
                      + " Send OK + INFLIGHT on ch_id=" + std::to_string(channel->channel_id));
            return;
        }
        debug_log("AsuTransportImpl::CompleteTask",
                  "task_id=" + std::to_string(ctx->task_id)
                  + " Send FAILED on ch_id=" + std::to_string(channel->channel_id)
                  + " retries_left=" + std::to_string(retries));
        channel->inflight_count.fetch_sub(1, std::memory_order_relaxed);
        conn_manager_.ReportFailure(channel);
        channel = conn_manager_.SelectConnection();
    }

    if (channel) {
        channel->inflight_count.fetch_sub(1, std::memory_order_relaxed);
    }

    debug_log("AsuTransportImpl::CompleteTask",
              "task_id=" + std::to_string(ctx->task_id) + " no available channel, state->FAILED");
    std::lock_guard<std::mutex> lock(ctx->wait_mu);
    ctx->final_status = Status::Error(StatusCode::NO_ACTIVE_CONNECTION, "no available channel");
    ctx->state.store(TransportTaskState::FAILED, std::memory_order_release);
    ctx->cv.notify_all();
}

void AsuTransportImpl::BuildResult(const TransportTaskContext& ctx, TaskResult& result)
{
    result.status = ctx.final_status;
    result.entry_status = ctx.entry_status;
    result.query_result.reset();
    if (ctx.op_type == TransportOpType::QUERY) { result.query_result = ctx.query_result; }
}

std::unique_ptr<AsuTransport> CreateAsuTransport() { return std::make_unique<AsuTransportImpl>(); }

}  // namespace UC::ASU