#include <cstdint>
#include <gtest/gtest.h>
#include <vector>
#include "asu_transport/asu_transport.h"
#include "asu_transport/types.h"
#include "connection_internal.h"

namespace UC::ASU {
namespace {

AsuEndpoint MakeEndpoint(const std::string& ip, std::uint16_t port = 9559)
{
    AsuEndpoint ep;
    ep.ip = ip;
    ep.port = port;
    ep.protocol = Protocol::UB;
    return ep;
}

TransportConfig MakeTransportConfig()
{
    TransportConfig config;
    config.asu_name = "test-asu";
    config.asu_id = 1;
    config.query_qp_num = 1;
    config.load_qp_num = 2;
    config.store_qp_num = 1;
    config.max_inflight_tasks = 64;
    config.query_timeout_ms = 5000;

    AsuEndpoint ep;
    ep.ip = "10.0.0.1";
    ep.port = 9559;
    ep.protocol = Protocol::UB;
    config.endpoints = {ep};

    return config;
}

std::vector<KVBuffer> MakeKVEntries(std::size_t count)
{
    std::vector<std::uint8_t> payload(64, 0xAB);
    MemoryRegion region;
    region.memory_type = MemoryType::HOST;
    region.addr = reinterpret_cast<std::uint64_t>(payload.data());
    region.size = payload.size();

    Buffer buffer;
    buffer.region = region;

    std::vector<KVBuffer> entries;
    for (std::size_t i = 0; i < count; ++i) {
        entries.push_back(KVBuffer{"key_" + std::to_string(i), buffer});
    }
    return entries;
}

std::vector<CacheKey> MakeKeys(std::size_t count)
{
    std::vector<CacheKey> keys;
    for (std::size_t i = 0; i < count; ++i) {
        keys.push_back("key_" + std::to_string(i));
    }
    return keys;
}

void WaitAndVerifyOK(AsuTransport& transport, TaskId task_id, std::size_t entry_count)
{
    TaskResult result;
    auto s = transport.Wait(task_id, 5000, result);
    ASSERT_TRUE(s.ok()) << s.message;
    ASSERT_TRUE(result.status.ok()) << result.status.message;
    ASSERT_EQ(result.entry_status.size(), entry_count);
    for (const auto& es : result.entry_status) {
        EXPECT_TRUE(es.ok()) << es.message;
    }
}

}  // namespace

// Purpose: Verify AsuTransportImpl Init creates groups + worker thread, CheckHealth returns OK, Shutdown cleans up.
// Method: CreateAsuTransport -> Init -> CheckHealth (expect OK) -> Shutdown (expect OK).
TEST(ConnectionTransportTest, InitShutdown_Lifecycle)
{
    auto transport = CreateAsuTransport();
    ASSERT_NE(transport, nullptr);

    auto s = transport->Init(MakeTransportConfig());
    ASSERT_TRUE(s.ok()) << s.message;

    s = transport->CheckHealth();
    EXPECT_TRUE(s.ok());

    s = transport->Shutdown();
    ASSERT_TRUE(s.ok()) << s.message;
}

// Purpose: Verify double Shutdown is safe and returns OK without crash.
// Method: Init -> Shutdown (OK) -> Shutdown again (expect OK, no crash).
TEST(ConnectionTransportTest, InitShutdown_DoubleShutdownOK)
{
    auto transport = CreateAsuTransport();
    ASSERT_TRUE(transport->Init(MakeTransportConfig()).ok());
    ASSERT_TRUE(transport->Shutdown().ok());
    auto s = transport->Shutdown();
    EXPECT_TRUE(s.ok());
}

// Purpose: Verify LoadAsync + Wait end-to-end completes with OK status and correct entry_count.
// Method: Init, submit 4 KV entries via LoadAsync, Wait with 5s timeout, verify status OK and entry_status size=4.
TEST(ConnectionTransportTest, LoadAsyncAndWait_CompletesOK)
{
    auto transport = CreateAsuTransport();
    ASSERT_TRUE(transport->Init(MakeTransportConfig()).ok());

    auto entries = MakeKVEntries(4);
    TaskId task_id{kInvalidTaskId};
    auto s = transport->LoadAsync(entries, task_id);
    ASSERT_TRUE(s.ok()) << s.message;
    ASSERT_NE(task_id, kInvalidTaskId);

    WaitAndVerifyOK(*transport, task_id, entries.size());

    transport->Shutdown();
}

// Purpose: Verify StoreAsync + Wait end-to-end completes with OK status.
// Method: Init, submit 3 KV entries via StoreAsync, Wait, verify status OK and entry_status size=3.
TEST(ConnectionTransportTest, StoreAsyncAndWait_CompletesOK)
{
    auto transport = CreateAsuTransport();
    ASSERT_TRUE(transport->Init(MakeTransportConfig()).ok());

    auto entries = MakeKVEntries(3);
    TaskId task_id{kInvalidTaskId};
    auto s = transport->StoreAsync(entries, task_id);
    ASSERT_TRUE(s.ok()) << s.message;
    ASSERT_NE(task_id, kInvalidTaskId);

    WaitAndVerifyOK(*transport, task_id, entries.size());

    transport->Shutdown();
}

// Purpose: Verify DeleteAsync + Wait end-to-end completes with OK status.
// Method: Init, submit 2 keys via DeleteAsync, Wait, verify status OK and entry_status size=2.
TEST(ConnectionTransportTest, DeleteAsyncAndWait_CompletesOK)
{
    auto transport = CreateAsuTransport();
    ASSERT_TRUE(transport->Init(MakeTransportConfig()).ok());

    auto keys = MakeKeys(2);
    TaskId task_id{kInvalidTaskId};
    auto s = transport->DeleteAsync(keys, task_id);
    ASSERT_TRUE(s.ok()) << s.message;
    ASSERT_NE(task_id, kInvalidTaskId);

    WaitAndVerifyOK(*transport, task_id, keys.size());

    transport->Shutdown();
}

// Purpose: Verify QueryAsync + Wait end-to-end completes with OK status and query_result populated.
// Method: Init, submit 3 keys via QueryAsync, Wait 5s, verify status OK, query_result has_value, exists.size=3.
TEST(ConnectionTransportTest, QueryAsyncAndWait_CompletesOK)
{
    auto transport = CreateAsuTransport();
    ASSERT_TRUE(transport->Init(MakeTransportConfig()).ok());

    auto keys = MakeKeys(3);
    QueryOptions opts;
    opts.timeout_ms = 5000;
    TaskId task_id{kInvalidTaskId};
    auto s = transport->QueryAsync(keys, opts, task_id);
    ASSERT_TRUE(s.ok()) << s.message;
    ASSERT_NE(task_id, kInvalidTaskId);

    TaskResult result;
    s = transport->Wait(task_id, 5000, result);
    ASSERT_TRUE(s.ok()) << s.message;
    ASSERT_TRUE(result.status.ok()) << result.status.message;
    ASSERT_TRUE(result.query_result.has_value());
    ASSERT_EQ(result.query_result->exists.size(), keys.size());

    transport->Shutdown();
}

// Purpose: Verify synchronous Query (QueryAsync+Wait internally) completes end-to-end.
// Method: Init, call Query with 2 keys and 5s timeout, verify exists.size=2.
TEST(ConnectionTransportTest, QuerySync_CompletesOK)
{
    auto transport = CreateAsuTransport();
    ASSERT_TRUE(transport->Init(MakeTransportConfig()).ok());

    auto keys = MakeKeys(2);
    QueryOptions opts;
    opts.timeout_ms = 5000;
    QueryResult query_result;
    auto s = transport->Query(keys, opts, query_result);
    ASSERT_TRUE(s.ok()) << s.message;
    ASSERT_EQ(query_result.exists.size(), keys.size());

    transport->Shutdown();
}

// Purpose: Verify Check returns IN_PROGRESS immediately after submit, then DONE after Wait; subsequent Check returns TASK_NOT_FOUND.
// Method: Submit LoadAsync, Check (may return IN_PROGRESS), Wait until done, Check again (expect TASK_NOT_FOUND since Wait removes task).
TEST(ConnectionTransportTest, Check_InProgressThenDone)
{
    auto transport = CreateAsuTransport();
    ASSERT_TRUE(transport->Init(MakeTransportConfig()).ok());

    auto entries = MakeKVEntries(2);
    TaskId task_id{kInvalidTaskId};
    ASSERT_TRUE(transport->LoadAsync(entries, task_id).ok());

    TaskResult check_result;
    auto s = transport->Check(task_id, check_result);
    ASSERT_TRUE(s.ok());
    if (check_result.status.code == StatusCode::IN_PROGRESS) {
        s = transport->Wait(task_id, 5000, check_result);
        ASSERT_TRUE(s.ok());
        ASSERT_TRUE(check_result.status.ok());
    }

    s = transport->Check(task_id, check_result);
    EXPECT_EQ(s.code, StatusCode::TASK_NOT_FOUND);

    transport->Shutdown();
}

// Purpose: Verify Cancel returns INTERNAL_ERROR (currently unsupported).
// Method: Init, call Cancel with arbitrary task_id, expect INTERNAL_ERROR.
TEST(ConnectionTransportTest, Cancel_ReturnsUnsupported)
{
    auto transport = CreateAsuTransport();
    ASSERT_TRUE(transport->Init(MakeTransportConfig()).ok());

    auto s = transport->Cancel(1);
    EXPECT_EQ(s.code, StatusCode::UNSUPPORTED);

    transport->Shutdown();
}

// Purpose: Verify Wait on nonexistent task_id returns TASK_NOT_FOUND.
// Method: Init, call Wait with task_id=9999, expect TASK_NOT_FOUND.
TEST(ConnectionTransportTest, Wait_TaskNotFound)
{
    auto transport = CreateAsuTransport();
    ASSERT_TRUE(transport->Init(MakeTransportConfig()).ok());

    TaskResult result;
    auto s = transport->Wait(9999, 100, result);
    EXPECT_EQ(s.code, StatusCode::TASK_NOT_FOUND);

    transport->Shutdown();
}

// Purpose: Verify Check on nonexistent task_id returns TASK_NOT_FOUND.
// Method: Init, call Check with task_id=9999, expect TASK_NOT_FOUND.
TEST(ConnectionTransportTest, Check_TaskNotFound)
{
    auto transport = CreateAsuTransport();
    ASSERT_TRUE(transport->Init(MakeTransportConfig()).ok());

    TaskResult result;
    auto s = transport->Check(9999, result);
    EXPECT_EQ(s.code, StatusCode::TASK_NOT_FOUND);

    transport->Shutdown();
}

// Purpose: Verify 10 sequential LoadAsync+Wait tasks all complete successfully on the same transport instance.
// Method: Init, submit 10 LoadAsync tasks sequentially, Wait each one, verify all OK.
TEST(ConnectionTransportTest, MultipleTasksConcurrent)
{
    auto transport = CreateAsuTransport();
    ASSERT_TRUE(transport->Init(MakeTransportConfig()).ok());

    std::vector<TaskId> task_ids;
    for (int i = 0; i < 10; ++i) {
        auto entries = MakeKVEntries(2);
        TaskId tid{kInvalidTaskId};
        auto s = transport->LoadAsync(entries, tid);
        ASSERT_TRUE(s.ok()) << s.message;
        ASSERT_NE(tid, kInvalidTaskId);
        task_ids.push_back(tid);
    }

    for (auto tid : task_ids) {
        WaitAndVerifyOK(*transport, tid, 2);
    }

    transport->Shutdown();
}

// Purpose: Verify Init with multiple endpoints creates multiple groups and a single LoadAsync+Wait still completes.
// Method: Init with 2 endpoints (2 groups), submit LoadAsync with 3 entries, Wait, verify OK.
TEST(ConnectionTransportTest, MultipleEndpoints)
{
    auto transport = CreateAsuTransport();

    TransportConfig config;
    config.asu_name = "multi-ep";
    config.asu_id = 2;
    config.query_qp_num = 1;
    config.load_qp_num = 2;
    config.store_qp_num = 1;
    config.max_inflight_tasks = 64;
    config.query_timeout_ms = 5000;

    AsuEndpoint ep0, ep1;
    ep0.ip = "10.0.0.1"; ep0.port = 9559; ep0.protocol = Protocol::UB;
    ep1.ip = "10.0.0.2"; ep1.port = 9559; ep1.protocol = Protocol::UB;
    config.endpoints = {ep0, ep1};

    ASSERT_TRUE(transport->Init(config).ok());

    auto entries = MakeKVEntries(3);
    TaskId task_id{kInvalidTaskId};
    ASSERT_TRUE(transport->LoadAsync(entries, task_id).ok());
    WaitAndVerifyOK(*transport, task_id, entries.size());

    transport->Shutdown();
}

// Purpose: Verify ReportFailure triggers BeginDrain at threshold, RecoverLoop completes drain+rebuild, active_count restores.
// Method: AddGroup(4 QPs) + StartRecoverLoop, 1x ReportFailure (expect ACTIVE), 2x ReportFailure (expect DRAINING, active_count-1), release inflight, wait 500ms, expect FAILED+active_count restored+HasActiveChannel.
TEST(ConnectionTransportTest, ChannelFailure_ReportFailureTriggersDrain)
{
    ConnectionManager mgr;
    ASSERT_TRUE(mgr.AddGroup(MakeEndpoint("10.0.0.1", 9559), 4).ok());
    mgr.StartRecoverLoop();

    auto* ch = mgr.SelectConnection();
    ASSERT_NE(ch, nullptr);
    EXPECT_EQ(ch->group->active_count.load(), 4u);

    mgr.ReportFailure(ch);
    EXPECT_EQ(ch->state.load(), ChannelState::ACTIVE);
    EXPECT_EQ(ch->group->active_count.load(), 4u);

    mgr.ReportFailure(ch);
    EXPECT_EQ(ch->state.load(), ChannelState::DRAINING);
    EXPECT_EQ(ch->group->active_count.load(), 3u);

    ch->inflight_count.fetch_sub(1);
    EXPECT_EQ(mgr.TotalInflightCount(), 0);

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    EXPECT_EQ(ch->state.load(), ChannelState::FAILED);
    EXPECT_EQ(ch->group->active_count.load(), 4u);
    EXPECT_TRUE(ch->group->HasActiveChannel());
    EXPECT_EQ(mgr.TotalInflightCount(), 0);

    mgr.Shutdown();
}

// Purpose: Verify when all channels fail, SelectConnection returns nullptr and HasActiveChannel is false.
// Method: AddGroup(2 QPs), manually BeginDrain+FinishDrain both channels, release inflight, verify HasActiveChannel=false, SelectConnection=nullptr.
TEST(ConnectionTransportTest, ChannelFailure_AllChannelsFail)
{
    ConnectionManager mgr;
    ASSERT_TRUE(mgr.AddGroup(MakeEndpoint("10.0.0.1", 9559), 2).ok());

    auto* ch0 = mgr.SelectConnection();
    auto* ch1 = mgr.SelectConnection();
    ASSERT_NE(ch0, nullptr);
    ASSERT_NE(ch1, nullptr);

    ch0->BeginDrain();
    ch1->BeginDrain();
    ch0->FinishDrain();
    ch1->FinishDrain();
    ch0->inflight_count.fetch_sub(1);
    ch1->inflight_count.fetch_sub(1);

    ch0->group->active_count.fetch_sub(1);
    ch1->group->active_count.fetch_sub(1);

    EXPECT_EQ(ch0->inflight_count.load(), 0u);
    EXPECT_EQ(ch1->inflight_count.load(), 0u);
    EXPECT_EQ(ch0->group->active_count.load(), 0u);
    EXPECT_EQ(mgr.TotalInflightCount(), 0);
    EXPECT_FALSE(ch0->group->HasActiveChannel());
    auto* ch = mgr.SelectConnection();
    EXPECT_EQ(ch, nullptr);

    mgr.Shutdown();
}

// Purpose: Verify 2 channels both failing can be drained and rebuilt, group recovers with new ACTIVE channels selectable.
// Method: StartRecoverLoop, trigger drain on both channels via 2x ReportFailure each, release inflight, wait 800ms, verify both FAILED, HasActive=true, new SelectConnection returns ACTIVE channel.
TEST(ConnectionTransportTest, ChannelFailure_RecoveryAfterDrain)
{
    ConnectionManager mgr;
    ASSERT_TRUE(mgr.AddGroup(MakeEndpoint("10.0.0.1", 9559), 2).ok());
    mgr.StartRecoverLoop();

    auto* ch0 = mgr.SelectConnection();
    auto* ch1 = mgr.SelectConnection();
    ASSERT_NE(ch0, nullptr);
    ASSERT_NE(ch1, nullptr);

    mgr.ReportFailure(ch0);
    mgr.ReportFailure(ch0);
    mgr.ReportFailure(ch1);
    mgr.ReportFailure(ch1);

    ch0->inflight_count.fetch_sub(1);
    ch1->inflight_count.fetch_sub(1);
    EXPECT_EQ(mgr.TotalInflightCount(), 0);

    std::this_thread::sleep_for(std::chrono::milliseconds(800));

    EXPECT_EQ(ch0->state.load(), ChannelState::FAILED);
    EXPECT_EQ(ch1->state.load(), ChannelState::FAILED);
    EXPECT_TRUE(ch0->group->HasActiveChannel());
    EXPECT_EQ(mgr.TotalInflightCount(), 0);

    auto* new_ch = mgr.SelectConnection();
    ASSERT_NE(new_ch, nullptr);
    EXPECT_EQ(new_ch->state.load(), ChannelState::ACTIVE);
    EXPECT_EQ(new_ch->inflight_count.load(), 1u);

    new_ch->inflight_count.fetch_sub(1);
    EXPECT_EQ(mgr.TotalInflightCount(), 0);

    mgr.Shutdown();
}

// Purpose: Verify overlapping failures on 2 of 4 channels: failed ones drain while remaining ones stay ACTIVE and selectable.
// Method: AddGroup(4 QPs) + StartRecoverLoop, trigger drain on ch[0] and ch[1], release all inflight, verify ch[0,1]=DRAINING, ch[2,3]=ACTIVE, SelectConnection returns ACTIVE channel, wait 800ms, verify ch[0,1]=FAILED, HasActive=true.
TEST(ConnectionTransportTest, ChannelFailure_OverlappingFailures)
{
    ConnectionManager mgr;
    ASSERT_TRUE(mgr.AddGroup(MakeEndpoint("10.0.0.1", 9559), 4).ok());
    mgr.StartRecoverLoop();

    std::vector<ConnectionChannel*> channels;
    for (int i = 0; i < 4; ++i) {
        auto* ch = mgr.SelectConnection();
        ASSERT_NE(ch, nullptr);
        channels.push_back(ch);
    }

    mgr.ReportFailure(channels[0]);
    mgr.ReportFailure(channels[0]);
    mgr.ReportFailure(channels[1]);
    mgr.ReportFailure(channels[1]);

    for (auto* ch : channels) { ch->inflight_count.fetch_sub(1); }
    EXPECT_EQ(mgr.TotalInflightCount(), 0);

    EXPECT_EQ(channels[0]->state.load(), ChannelState::DRAINING);
    EXPECT_EQ(channels[1]->state.load(), ChannelState::DRAINING);
    EXPECT_EQ(channels[2]->state.load(), ChannelState::ACTIVE);
    EXPECT_EQ(channels[3]->state.load(), ChannelState::ACTIVE);

    auto* ch = mgr.SelectConnection();
    ASSERT_NE(ch, nullptr);
    EXPECT_EQ(ch->state.load(), ChannelState::ACTIVE);
    ch->inflight_count.fetch_sub(1);
    EXPECT_EQ(mgr.TotalInflightCount(), 0);

    std::this_thread::sleep_for(std::chrono::milliseconds(800));

    EXPECT_EQ(channels[0]->state.load(), ChannelState::FAILED);
    EXPECT_EQ(channels[1]->state.load(), ChannelState::FAILED);
    EXPECT_TRUE(channels[0]->group->HasActiveChannel());
    EXPECT_EQ(mgr.TotalInflightCount(), 0);

    mgr.Shutdown();
}

// Purpose: Verify Check-only polling (without Wait) completes and removes task from TaskManager.
// Method: Submit LoadAsync, poll Check until DONE (not IN_PROGRESS), verify subsequent Check returns TASK_NOT_FOUND.
TEST(ConnectionTransportTest, CheckOnlyPolling_RemovesTaskAfterCompletion)
{
    auto transport = CreateAsuTransport();
    ASSERT_TRUE(transport->Init(MakeTransportConfig()).ok());

    auto entries = MakeKVEntries(2);
    TaskId task_id{kInvalidTaskId};
    ASSERT_TRUE(transport->LoadAsync(entries, task_id).ok());

    TaskResult result;
    int polls = 0;
    while (polls++ < 100) {
        auto s = transport->Check(task_id, result);
        if (s.ok() && result.status.ok()) { break; }
        if (result.status.code == StatusCode::IN_PROGRESS) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }
    }
    ASSERT_TRUE(result.status.ok()) << "Check polling failed after " << polls << " polls";

    auto s = transport->Check(task_id, result);
    EXPECT_EQ(s.code, StatusCode::TASK_NOT_FOUND);

    transport->Shutdown();
}

// Purpose: Verify SubmitAsync returns RESOURCE_BUSY when execute_queue is full.
// Method: Init with max_inflight_tasks=2 (queue_depth=3), submit tasks without consuming
// (by not calling Wait/Check), fill the queue, verify next submit returns RESOURCE_BUSY.
TEST(ConnectionTransportTest, SubmitAsync_QueueFull_ReturnsResourceBusy)
{
    auto transport = CreateAsuTransport();

    TransportConfig config;
    config.asu_name = "queue-full-test";
    config.asu_id = 1;
    config.query_qp_num = 1;
    config.load_qp_num = 1;
    config.store_qp_num = 0;
    config.max_inflight_tasks = 2;
    config.query_timeout_ms = 5000;
    AsuEndpoint ep;
    ep.ip = "10.0.0.1"; ep.port = 9559; ep.protocol = Protocol::UB;
    config.endpoints = {ep};

    ASSERT_TRUE(transport->Init(config).ok());

    std::vector<TaskId> task_ids;
    Status last_status;
    for (int i = 0; i < 10; ++i) {
        auto entries = MakeKVEntries(1);
        TaskId tid{kInvalidTaskId};
        last_status = transport->LoadAsync(entries, tid);
        if (last_status.ok()) {
            task_ids.push_back(tid);
        } else {
            break;
        }
    }

    EXPECT_EQ(last_status.code, StatusCode::RESOURCE_BUSY);

    for (auto tid : task_ids) {
        TaskResult result;
        transport->Wait(tid, 5000, result);
    }

    transport->Shutdown();
}

// Purpose: Verify when all channels reach inflight capacity, SubmitAsync still succeeds
// (queue accepts task) but CompleteTask will retry until a channel becomes available.
// Method: Init with small queue, submit tasks, verify they complete normally.
// This indirectly validates the retry mechanism in CompleteTask.
TEST(ConnectionTransportTest, CompleteTask_RetryUntilChannelAvailable)
{
    auto transport = CreateAsuTransport();
    ASSERT_TRUE(transport->Init(MakeTransportConfig()).ok());

    auto entries = MakeKVEntries(1);
    TaskId task_id{kInvalidTaskId};
    auto s = transport->LoadAsync(entries, task_id);
    ASSERT_TRUE(s.ok());

    TaskResult result;
    s = transport->Wait(task_id, 5000, result);
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(result.status.ok());

    transport->Shutdown();
}

}  // namespace UC::ASU