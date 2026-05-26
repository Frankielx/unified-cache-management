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
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <vector>
#include "asu_transport/asu_transport.h"
#include "asu_transport/types.h"
#include "connection_manager.h"
#include "transport_task_manager.h"

namespace UC::ASU {

enum class ChannelState {
    ACTIVE,
    DRAINING,
    FAILED,
};

struct ConnectionChannel {
    std::uint32_t channel_id{0};
    std::atomic<ChannelState> state{ChannelState::ACTIVE};
    ConnectionGroup* group{nullptr};

    std::atomic<std::uint32_t> inflight_count{0};
    std::atomic<std::uint64_t> inflight_bytes{0};
    std::atomic<std::uint32_t> error_count{0};
    std::atomic<std::uint64_t> last_error_time{0};
    std::atomic<std::uint64_t> drain_start_time{0};

    void* native_qp{nullptr};

    Status StubSend(TransportTaskContext* ctx);
    bool BeginDrain();
    void FinishDrain();
};

struct ConnectionGroup {
    std::uint32_t group_id{0};
    AsuEndpoint endpoint;

    std::vector<std::unique_ptr<ConnectionChannel>> channels;
    std::atomic<std::uint32_t> active_count{0};

    ConnectionChannel* AddChannel(ConnectionHandle handle);
    ConnectionChannel* RebuildChannel();
    bool HasActiveChannel() const;
};

std::vector<ConnectionHandle> StubCreateConnection(const AsuEndpoint& endpoint,
                                                    std::uint32_t qp_num);
std::vector<Status> StubDeleteConnections(const std::vector<ConnectionHandle>& handles);



}  // namespace UC::ASU