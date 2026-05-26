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
#include "connection_internal.h"
#include "debug_log.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <sstream>

namespace UC::ASU {



ConnectionManager::ConnectionManager() = default;

ConnectionManager::~ConnectionManager() { Shutdown(); }

Status ConnectionManager::AddGroup(const AsuEndpoint& endpoint, std::uint32_t qp_num)
{
    debug_log("ConnectionManager::AddGroup",
              "endpoint=" + endpoint.ip + " qp_num=" + std::to_string(qp_num));
    auto handles = StubCreateConnection(endpoint, qp_num);
    if (handles.size() != qp_num) {
        debug_log("ConnectionManager::AddGroup",
                  "FAILED: got " + std::to_string(handles.size())
                  + " handles, expected " + std::to_string(qp_num));
        return Status::Error(StatusCode::CONNECTION_ERROR,
                             "CreateConnection returned wrong number of handles");
    }

    auto group = std::make_unique<ConnectionGroup>();
    auto gid = static_cast<std::uint32_t>(groups_.size());
    group->group_id = gid;
    group->endpoint = endpoint;

    for (auto& handle : handles) {
        group->AddChannel(handle);
    }
    group->active_count.store(qp_num, std::memory_order_release);

    {
        std::lock_guard<std::mutex> lock(structure_mu_);
        groups_.push_back(std::move(group));
    }
    debug_log("ConnectionManager::AddGroup",
              "OK: group_id=" + std::to_string(gid)
              + " active_count=" + std::to_string(qp_num));
    return Status::OK();
}

Status ConnectionManager::Shutdown()
{
    debug_log("ConnectionManager::Shutdown", "start");
    StopRecoverLoop();
    {
        std::lock_guard<std::mutex> lock(structure_mu_);
        for (auto& group : groups_) {
            for (auto& ch : group->channels) {
                if (ch->native_qp) {
                    StubDeleteConnections({ch->native_qp});
                    ch->native_qp = nullptr;
                }
            }
        }
        groups_.clear();
    }
    {
        std::lock_guard<std::mutex> lock(drain_mu_);
        drain_list_.clear();
    }
    debug_log("ConnectionManager::Shutdown", "done");
    return Status::OK();
}

ConnectionChannel* ConnectionManager::SelectConnection()
{
    auto policy = routing_policy_.load(std::memory_order_acquire);
    auto* ch = policy == RoutingPolicy::LEAST_LOADED ? SelectByLeastLoaded() : SelectByRoundRobin();
    if (ch) {
        debug_log("ConnectionManager::SelectConnection",
                  "policy=" + std::string(policy == RoutingPolicy::LEAST_LOADED ? "LEAST_LOADED" : "ROUND_ROBIN")
                  + " ch_id=" + std::to_string(ch->channel_id)
                  + " group_id=" + std::to_string(ch->group->group_id)
                  + " inflight=" + std::to_string(ch->inflight_count.load(std::memory_order_relaxed)));
    } else {
        debug_log("ConnectionManager::SelectConnection",
                  "policy=" + std::string(policy == RoutingPolicy::LEAST_LOADED ? "LEAST_LOADED" : "ROUND_ROBIN")
                  + " NO available channel");
    }
    return ch;
}

void ConnectionManager::SetRoutingPolicy(RoutingPolicy policy)
{
    routing_policy_.store(policy, std::memory_order_release);
}

void ConnectionManager::ReportFailure(ConnectionChannel* channel)
{
    auto old_count = channel->error_count.fetch_add(1, std::memory_order_relaxed);
    debug_log("ConnectionManager::ReportFailure",
              "ch_id=" + std::to_string(channel->channel_id)
              + " group_id=" + std::to_string(channel->group->group_id)
              + " error_count=" + std::to_string(old_count + 1)
              + " threshold=" + std::to_string(kFailureThreshold));
    if (old_count + 1 < kFailureThreshold) {
        debug_log("ConnectionManager::ReportFailure", "below threshold, skip drain");
        return;
    }

    if (!channel->BeginDrain()) {
        debug_log("ConnectionManager::ReportFailure", "BeginDrain CAS failed (already draining/failed)");
        return;
    }

    debug_log("ConnectionManager::ReportFailure",
              "BeginDrain OK, ch_id=" + std::to_string(channel->channel_id) + " state=DRAINING");
    channel->group->active_count.fetch_sub(1, std::memory_order_release);

    auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now().time_since_epoch())
                      .count();
    channel->drain_start_time.store(static_cast<std::uint64_t>(now_ms),
                                    std::memory_order_release);

    {
        std::lock_guard<std::mutex> lock(drain_mu_);
        drain_list_.push_back(channel);
    }
}

void ConnectionManager::StartRecoverLoop()
{
    debug_log("ConnectionManager::StartRecoverLoop", "start");
    if (recover_worker_.joinable()) { return; }
    stop_recover_.store(false, std::memory_order_release);
    recover_worker_ = std::thread(&ConnectionManager::RecoverLoop, this);
}

void ConnectionManager::StopRecoverLoop()
{
    debug_log("ConnectionManager::StopRecoverLoop", "start");
    stop_recover_.store(true, std::memory_order_release);
    if (recover_worker_.joinable()) { recover_worker_.join(); }
}

void ConnectionManager::RecoverLoop()
{
    debug_log("ConnectionManager::RecoverLoop", "started");
    while (!stop_recover_.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(kRecoverIntervalMs));
        if (stop_recover_.load(std::memory_order_acquire)) { break; }

        std::vector<ConnectionChannel*> completed;
        {
            std::lock_guard<std::mutex> lock(drain_mu_);
            auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now().time_since_epoch())
                              .count();
            for (auto* ch : drain_list_) {
                auto inflight = ch->inflight_count.load(std::memory_order_acquire);
                if (inflight == 0) {
                    debug_log("ConnectionManager::RecoverLoop",
                              "ch_id=" + std::to_string(ch->channel_id) + " inflight=0, drain complete");
                    completed.push_back(ch);
                } else {
                    auto elapsed =
                        now_ms - static_cast<std::int64_t>(
                                     ch->drain_start_time.load(std::memory_order_acquire));
                    if (elapsed >= static_cast<std::int64_t>(kDrainTimeoutMs)) {
                        debug_log("ConnectionManager::RecoverLoop",
                                  "ch_id=" + std::to_string(ch->channel_id)
                                  + " inflight=" + std::to_string(inflight)
                                  + " elapsed=" + std::to_string(elapsed) + "ms, drain timeout");
                        completed.push_back(ch);
                    }
                }
            }
        }

        for (auto* ch : completed) {
            {
                std::lock_guard<std::mutex> lock(structure_mu_);
                ch->FinishDrain();
                debug_log("ConnectionManager::RecoverLoop",
                          "FinishDrain ch_id=" + std::to_string(ch->channel_id) + " state=FAILED");
                auto* new_ch = ch->group->RebuildChannel();
                if (new_ch) {
                    ch->group->active_count.fetch_add(1, std::memory_order_release);
                    debug_log("ConnectionManager::RecoverLoop",
                              "RebuildChannel OK: new_ch_id=" + std::to_string(new_ch->channel_id)
                              + " group_id=" + std::to_string(ch->group->group_id));
                } else {
                    debug_log("ConnectionManager::RecoverLoop",
                              "RebuildChannel FAILED group_id=" + std::to_string(ch->group->group_id));
                }
            }

            {
                std::lock_guard<std::mutex> lock(drain_mu_);
                drain_list_.erase(
                    std::remove(drain_list_.begin(), drain_list_.end(), ch),
                    drain_list_.end());
            }
        }
    }
    debug_log("ConnectionManager::RecoverLoop", "stopped");
}

ConnectionChannel* ConnectionManager::SelectByRoundRobin()
{
    ConnectionChannel* selected = nullptr;
    {
        std::lock_guard<std::mutex> lock(structure_mu_);
        std::vector<ConnectionChannel*> candidates;
        for (auto& group : groups_) {
            for (auto& ch : group->channels) {
                if (ch->state.load(std::memory_order_acquire) == ChannelState::ACTIVE
                    && ch->inflight_count.load(std::memory_order_relaxed)
                       < kMaxInflightPerChannel) {
                    candidates.push_back(ch.get());
                }
            }
        }
        if (candidates.empty()) { return nullptr; }
        auto idx = rr_index_.fetch_add(1, std::memory_order_relaxed);
        selected = candidates[idx % candidates.size()];
        selected->inflight_count.fetch_add(1, std::memory_order_relaxed);
    }
    return selected;
}

ConnectionChannel* ConnectionManager::SelectByLeastLoaded()
{
    ConnectionChannel* selected = nullptr;
    {
        std::lock_guard<std::mutex> lock(structure_mu_);
        std::vector<ConnectionChannel*> candidates;
        for (auto& group : groups_) {
            for (auto& ch : group->channels) {
                if (ch->state.load(std::memory_order_acquire) == ChannelState::ACTIVE
                    && ch->inflight_count.load(std::memory_order_relaxed)
                       < kMaxInflightPerChannel) {
                    candidates.push_back(ch.get());
                }
            }
        }
        if (candidates.empty()) { return nullptr; }
        auto* best = candidates[0];
        for (auto* ch : candidates) {
            if (ch->inflight_count.load(std::memory_order_relaxed)
                < best->inflight_count.load(std::memory_order_relaxed)) {
                best = ch;
            }
        }
        best->inflight_count.fetch_add(1, std::memory_order_relaxed);
        selected = best;
    }
    return selected;
}

Status ConnectionChannel::StubSend(TransportTaskContext* ctx)
{
    debug_log("ConnectionChannel::StubSend",
              "ch_id=" + std::to_string(channel_id)
              + " state=" + std::to_string(static_cast<int>(state.load(std::memory_order_acquire))));
    ctx->flagbuffer_status.store(1, std::memory_order_release);
    debug_log("ConnectionChannel::StubSend",
              "ch_id=" + std::to_string(channel_id) + " stub: flagbuffer->1, notify_all");
    return Status::OK();
}

bool ConnectionChannel::BeginDrain()
{
    ChannelState expected = ChannelState::ACTIVE;
    if (!state.compare_exchange_strong(expected, ChannelState::DRAINING,
                                        std::memory_order_acq_rel)) {
        debug_log("ConnectionChannel::BeginDrain",
                  "CAS FAILED: current_state=" + std::to_string(static_cast<int>(expected))
                  + " (expected ACTIVE=0)");
        return false;
    }
    debug_log("ConnectionChannel::BeginDrain",
              "CAS OK: ch_id=" + std::to_string(channel_id) + " ACTIVE->DRAINING");
    return true;
}

void ConnectionChannel::FinishDrain()
{
    debug_log("ConnectionChannel::FinishDrain",
              "ch_id=" + std::to_string(channel_id)
              + " native_qp=" + std::to_string(reinterpret_cast<std::uintptr_t>(native_qp)));
    if (native_qp) {
        StubDeleteConnections({native_qp});
    }
    native_qp = nullptr;
    state.store(ChannelState::FAILED, std::memory_order_release);
    debug_log("ConnectionChannel::FinishDrain",
              "ch_id=" + std::to_string(channel_id) + " state->FAILED");
}

ConnectionChannel* ConnectionGroup::AddChannel(ConnectionHandle handle)
{
    auto ch = std::make_unique<ConnectionChannel>();
    ch->channel_id = static_cast<std::uint32_t>(channels.size());
    ch->state.store(ChannelState::ACTIVE, std::memory_order_release);
    ch->group = this;
    ch->native_qp = handle;
    auto* raw = ch.get();
    channels.push_back(std::move(ch));
    debug_log("ConnectionGroup::AddChannel",
              "group_id=" + std::to_string(group_id)
              + " ch_id=" + std::to_string(raw->channel_id)
              + " total_channels=" + std::to_string(channels.size()));
    return raw;
}

ConnectionChannel* ConnectionGroup::RebuildChannel()
{
    debug_log("ConnectionGroup::RebuildChannel",
              "group_id=" + std::to_string(group_id));
    auto handles = StubCreateConnection(endpoint, 1);
    if (handles.empty()) {
        debug_log("ConnectionGroup::RebuildChannel", "FAILED: StubCreateConnection returned empty");
        return nullptr;
    }
    auto* ch = AddChannel(handles[0]);
    debug_log("ConnectionGroup::RebuildChannel",
              "OK: new_ch_id=" + std::to_string(ch->channel_id));
    return ch;
}

bool ConnectionGroup::HasActiveChannel() const
{
    for (auto& ch : channels) {
        if (ch->state.load(std::memory_order_acquire) == ChannelState::ACTIVE) {
            return true;
        }
    }
    return false;
}

std::vector<ConnectionHandle> StubCreateConnection(const AsuEndpoint& endpoint, std::uint32_t qp_num)
{
    return std::vector<ConnectionHandle>(qp_num, nullptr);
}

std::vector<Status> StubDeleteConnections(const std::vector<ConnectionHandle>& handles)
{
    std::vector<Status> results;
    results.reserve(handles.size());
    for (std::size_t i = 0; i < handles.size(); ++i) {
        results.push_back(Status::OK());
    }
    return results;
}

std::int64_t ConnectionManager::TotalInflightCount()
{
    std::int64_t sum = 0;
    std::lock_guard<std::mutex> lock(structure_mu_);
    for (const auto& group : groups_) {
        for (const auto& ch : group->channels) {
            sum += ch->inflight_count.load(std::memory_order_acquire);
        }
    }
    return sum;
}

}  // namespace UC::ASU