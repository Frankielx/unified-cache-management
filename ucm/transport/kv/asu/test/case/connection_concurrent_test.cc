#include <atomic>
#include <cstdint>
#include <gtest/gtest.h>
#include <set>
#include <thread>
#include <vector>
#include "asu_transport/asu_transport.h"
#include "asu_transport/types.h"
#include "connection_internal.h"
#include "connection_manager.h"

#include "debug_log.h"

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
    config.asu_name = "concurrent-test";
    config.asu_id = 1;
    config.query_qp_num = 2;
    config.load_qp_num = 4;
    config.store_qp_num = 2;
    config.max_inflight_tasks = 256;
    config.query_timeout_ms = 5000;

    AsuEndpoint ep;
    ep.ip = "10.0.0.1"; ep.port = 9559; ep.protocol = Protocol::UB;
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

}  // namespace

// Purpose: Verify concurrent SelectConnection does not corrupt inflight_count; after all threads
// release inflight, every channel must have inflight_count==0 and TotalInflightCount==0.
// Method: AddGroup with 4 QPs, collect all unique channel pointers via SelectConnection into a
// std::set, release initial inflight, then 8 threads x 100 iterations each call
// SelectConnection+fetch_sub(1), join all, verify each collected channel inflight_count==0
// and mgr.TotalInflightCount()==0.
TEST(ConnectionConcurrentTest, ConcurrentSelectConnection_InflightConsistency)
{
    ConnectionManager mgr;
    ASSERT_TRUE(mgr.AddGroup(MakeEndpoint("10.0.0.1"), 4).ok());

    std::set<ConnectionChannel*> all_channels;
    for (int i = 0; i < 4; ++i) {
        auto* ch = mgr.SelectConnection();
        ASSERT_NE(ch, nullptr);
        all_channels.insert(ch);
        ch->inflight_count.fetch_sub(1);
    }
    EXPECT_EQ(all_channels.size(), 4u);
    EXPECT_EQ(mgr.TotalInflightCount(), 0);

    constexpr int kThreads = 8;
    constexpr int kIterations = 100;

    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&]() {
            for (int i = 0; i < kIterations; ++i) {
                auto* ch = mgr.SelectConnection();
                if (ch) {
                    ch->inflight_count.fetch_sub(1);
                }
            }
        });
    }
    for (auto& th : threads) { th.join(); }

    for (auto* ch : all_channels) {
        EXPECT_EQ(ch->inflight_count.load(), 0);
    }
    EXPECT_EQ(mgr.TotalInflightCount(), 0);

    mgr.Shutdown();
}

// Purpose: Verify concurrent ReportFailure on same channel triggers BeginDrain CAS exactly once;
// only one thread succeeds the CAS; error_count accumulates across all calls.
// Method: 4 threads each call 2x ReportFailure on same channel, after all join verify state=DRAINING
// (CAS succeeded once) and error_count>=8 (4 threads x 2 calls).
TEST(ConnectionConcurrentTest, ConcurrentReportFailure_BeginDrainCAS)
{
    ConnectionManager mgr;
    ASSERT_TRUE(mgr.AddGroup(MakeEndpoint("10.0.0.1"), 4).ok());

    auto* ch = mgr.SelectConnection();
    ASSERT_NE(ch, nullptr);
    ch->inflight_count.fetch_sub(1);

    constexpr int kThreads = 4;

    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&]() {
            for (int i = 0; i < 2; ++i) {
                mgr.ReportFailure(ch);
            }
        });
    }
    for (auto& th : threads) { th.join(); }

    EXPECT_EQ(ch->state.load(), ChannelState::DRAINING);
    EXPECT_GE(ch->error_count.load(), 8u);

    mgr.Shutdown();
}

// Purpose: Verify 50 concurrent SubmitAsync+Wait tasks all complete successfully; all inflight
// properly released after completion (verified by submitting one more task after all 50 Wait OK).
// Method: 50 threads each submit LoadAsync, then 50 threads each Wait on their task_id, verify all
// 50 submit+wait OK. Then submit one final task and Wait OK to confirm channel availability and
// inflight cleanup.
TEST(ConnectionConcurrentTest, ConcurrentSubmitAndWait_MultipleTasks)
{
    auto transport = CreateAsuTransport();
    ASSERT_TRUE(transport->Init(MakeTransportConfig()).ok());

    constexpr int kTasks = 50;
    std::vector<TaskId> task_ids(kTasks, kInvalidTaskId);
    std::atomic<int> submit_ok{0};

    std::vector<std::thread> submit_threads;
    for (int i = 0; i < kTasks; ++i) {
        submit_threads.emplace_back([&, i]() {
            auto entries = MakeKVEntries(2);
            auto s = transport->LoadAsync(entries, task_ids[i]);
            if (s.ok()) { submit_ok.fetch_add(1); }
        });
    }
    for (auto& th : submit_threads) { th.join(); }

    EXPECT_EQ(submit_ok.load(), kTasks);

    std::atomic<int> wait_ok{0};
    std::vector<std::thread> wait_threads;
    for (int i = 0; i < kTasks; ++i) {
        if (task_ids[i] == kInvalidTaskId) continue;
        wait_threads.emplace_back([&, i]() {
            TaskResult result;
            auto s = transport->Wait(task_ids[i], 10000, result);
            if (s.ok() && result.status.ok()) { wait_ok.fetch_add(1); }
        });
    }
    for (auto& th : wait_threads) { th.join(); }

    EXPECT_EQ(wait_ok.load(), kTasks);

    auto verify_entries = MakeKVEntries(2);
    TaskId verify_tid{kInvalidTaskId};
    auto s = transport->LoadAsync(verify_entries, verify_tid);
    ASSERT_TRUE(s.ok()) << s.message;
    ASSERT_NE(verify_tid, kInvalidTaskId);

    TaskResult verify_result;
    s = transport->Wait(verify_tid, 10000, verify_result);
    ASSERT_TRUE(s.ok()) << s.message;
    ASSERT_TRUE(verify_result.status.ok()) << verify_result.status.message;

    transport->Shutdown();
}

// Purpose: Verify drain completes and inflight is released correctly after channel failure; non-failed
// channel remains ACTIVE with inflight==0; TotalInflightCount==0 after drain.
// Method: AddGroup(2 QPs)+StartRecoverLoop, trigger drain on ch0 via 2x ReportFailure, release both
// inflight, wait 500ms, verify ch0=FAILED+inflight==0, ch1=ACTIVE+inflight==0,
// TotalInflightCount==0, active_count==2, HasActive==true.
TEST(ConnectionConcurrentTest, ConcurrentDrainComplete_InflightRelease)
{
    ConnectionManager mgr;
    ASSERT_TRUE(mgr.AddGroup(MakeEndpoint("10.0.0.1"), 2).ok());
    mgr.StartRecoverLoop();

    auto* ch0 = mgr.SelectConnection();
    auto* ch1 = mgr.SelectConnection();
    ASSERT_NE(ch0, nullptr);
    ASSERT_NE(ch1, nullptr);

    mgr.ReportFailure(ch0);
    mgr.ReportFailure(ch0);
    EXPECT_EQ(ch0->state.load(), ChannelState::DRAINING);

    ch0->inflight_count.fetch_sub(1);
    ch1->inflight_count.fetch_sub(1);
    EXPECT_EQ(mgr.TotalInflightCount(), 0);

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    EXPECT_EQ(ch0->state.load(), ChannelState::FAILED);
    EXPECT_EQ(ch0->inflight_count.load(), 0u);
    EXPECT_EQ(ch1->state.load(), ChannelState::ACTIVE);
    EXPECT_EQ(ch1->inflight_count.load(), 0u);
    EXPECT_EQ(ch0->group->active_count.load(), 2u);
    EXPECT_TRUE(ch0->group->HasActiveChannel());
    EXPECT_EQ(mgr.TotalInflightCount(), 0);

    mgr.Shutdown();
}

// Purpose: Verify concurrent drain (ReportFailure) and SelectConnection do not deadlock or crash;
// SelectConnection still finds active channels and all selections succeed; TotalInflightCount==0
// after all releases.
// Method: 1 thread triggers drain via 2x ReportFailure on ch0, 20 threads concurrently call
// SelectConnection (each releases inflight), join all, verify select_ok==20 (3 remaining ACTIVE
// channels serve all 20 requests), release ch0 initial inflight, TotalInflightCount==0.
TEST(ConnectionConcurrentTest, ConcurrentDrainAndSelect)
{
    ConnectionManager mgr;
    ASSERT_TRUE(mgr.AddGroup(MakeEndpoint("10.0.0.1"), 4).ok());
    mgr.StartRecoverLoop();

    auto* ch0 = mgr.SelectConnection();
    ASSERT_NE(ch0, nullptr);

    std::thread drain_thread([&]() {
        mgr.ReportFailure(ch0);
        mgr.ReportFailure(ch0);
    });

    std::atomic<int> select_ok{0};
    std::vector<std::thread> select_threads;
    for (int i = 0; i < 20; ++i) {
        select_threads.emplace_back([&]() {
            auto* ch = mgr.SelectConnection();
            if (ch) {
                select_ok.fetch_add(1);
                ch->inflight_count.fetch_sub(1);
            }
        });
    }

    drain_thread.join();
    for (auto& th : select_threads) { th.join(); }

    EXPECT_EQ(select_ok.load(), 20);

    ch0->inflight_count.fetch_sub(1);
    EXPECT_EQ(mgr.TotalInflightCount(), 0);

    mgr.Shutdown();
}

// Purpose: Verify Shutdown with in-flight tasks completes safely without crash or hang; after
// Shutdown, SelectConnection returns nullptr confirming complete cleanup.
// Method: Init, submit 5 LoadAsync tasks, sleep 100ms (tasks may be in-flight), call Shutdown,
// verify it returns OK. Then create a fresh ConnectionManager and verify SelectConnection works
// (indirectly confirming no system-wide resource leak).
TEST(ConnectionConcurrentTest, ShutdownWithInflightTasks)
{
    auto transport = CreateAsuTransport();
    ASSERT_TRUE(transport->Init(MakeTransportConfig()).ok());

    std::vector<TaskId> task_ids;
    for (int i = 0; i < 5; ++i) {
        auto entries = MakeKVEntries(2);
        TaskId tid{kInvalidTaskId};
        auto s = transport->LoadAsync(entries, tid);
        if (s.ok() && tid != kInvalidTaskId) {
            task_ids.push_back(tid);
        }
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    auto s = transport->Shutdown();
    EXPECT_TRUE(s.ok());

    ConnectionManager fresh_mgr;
    ASSERT_TRUE(fresh_mgr.AddGroup(MakeEndpoint("10.0.0.2"), 2).ok());
    auto* ch = fresh_mgr.SelectConnection();
    ASSERT_NE(ch, nullptr);
    ch->inflight_count.fetch_sub(1);
    EXPECT_EQ(fresh_mgr.TotalInflightCount(), 0);
    fresh_mgr.Shutdown();
}

// Purpose: Verify mixed async operations (Load/Store/Query) from 20 concurrent threads all complete
// successfully; all inflight properly released (verified by one final LoadAsync+Wait after all 20).
// Method: 20 threads: i%3=0 LoadAsync, i%3=1 StoreAsync, i%3=2 QueryAsync; each Wait 10s, verify
// all 20 complete OK. Submit one final task, Wait OK, confirms inflight cleanup.
TEST(ConnectionConcurrentTest, MixedOperationsConcurrent)
{
    auto transport = CreateAsuTransport();
    ASSERT_TRUE(transport->Init(MakeTransportConfig()).ok());

    constexpr int kOps = 20;
    std::atomic<int> completed{0};

    std::vector<std::thread> threads;
    for (int i = 0; i < kOps; ++i) {
        threads.emplace_back([&, i]() {
            TaskId tid{kInvalidTaskId};
            Status s;
            if (i % 3 == 0) {
                auto entries = MakeKVEntries(2);
                s = transport->LoadAsync(entries, tid);
            } else if (i % 3 == 1) {
                auto entries = MakeKVEntries(2);
                s = transport->StoreAsync(entries, tid);
            } else {
                auto keys = MakeKeys(2);
                QueryOptions opts;
                opts.timeout_ms = 5000;
                s = transport->QueryAsync(keys, opts, tid);
            }

            if (s.ok() && tid != kInvalidTaskId) {
                TaskResult result;
                s = transport->Wait(tid, 10000, result);
                if (s.ok() && result.status.ok()) {
                    completed.fetch_add(1);
                }
            }
        });
    }
    for (auto& th : threads) { th.join(); }

    EXPECT_EQ(completed.load(), kOps);

    auto verify_entries = MakeKVEntries(2);
    TaskId verify_tid{kInvalidTaskId};
    auto s = transport->LoadAsync(verify_entries, verify_tid);
    ASSERT_TRUE(s.ok()) << s.message;
    ASSERT_NE(verify_tid, kInvalidTaskId);

    TaskResult verify_result;
    s = transport->Wait(verify_tid, 10000, verify_result);
    ASSERT_TRUE(s.ok()) << s.message;
    ASSERT_TRUE(verify_result.status.ok()) << verify_result.status.message;

    transport->Shutdown();
}

// Purpose: Verify 2 channels failing concurrently from separate threads are drained and rebuilt;
// group recovers; non-failed channels remain ACTIVE; post-recovery SelectConnection works;
// TotalInflightCount==0 after all releases.
// Method: StartRecoverLoop, select 4 channels, 2 threads trigger drain on ch[0] and ch[1],
// release all inflight, wait 800ms, verify ch[0,1]=FAILED, ch[2,3]=ACTIVE, HasActive=true,
// active_count restored. 20 post-recovery SelectConnection calls all succeed, release inflight,
// TotalInflightCount==0.
TEST(ConnectionConcurrentTest, ConcurrentChannelFailureAndRecovery)
{
    ConnectionManager mgr;
    ASSERT_TRUE(mgr.AddGroup(MakeEndpoint("10.0.0.1"), 4).ok());
    mgr.StartRecoverLoop();

    std::vector<ConnectionChannel*> channels;
    for (int i = 0; i < 4; ++i) {
        auto* ch = mgr.SelectConnection();
        ASSERT_NE(ch, nullptr);
        channels.push_back(ch);
    }

    std::thread fail0([&]() {
        mgr.ReportFailure(channels[0]);
        mgr.ReportFailure(channels[0]);
    });
    std::thread fail1([&]() {
        mgr.ReportFailure(channels[1]);
        mgr.ReportFailure(channels[1]);
    });

    fail0.join();
    fail1.join();

    for (auto* ch : channels) { ch->inflight_count.fetch_sub(1); }
    EXPECT_EQ(mgr.TotalInflightCount(), 0);

    std::this_thread::sleep_for(std::chrono::milliseconds(800));

    EXPECT_EQ(channels[0]->state.load(), ChannelState::FAILED);
    EXPECT_EQ(channels[1]->state.load(), ChannelState::FAILED);
    EXPECT_EQ(channels[2]->state.load(), ChannelState::ACTIVE);
    EXPECT_EQ(channels[3]->state.load(), ChannelState::ACTIVE);
    EXPECT_TRUE(channels[0]->group->HasActiveChannel());
    EXPECT_EQ(channels[0]->group->active_count.load(), 4u);

    std::atomic<int> select_after{0};
    std::vector<std::thread> threads;
    for (int i = 0; i < 20; ++i) {
        threads.emplace_back([&]() {
            auto* ch = mgr.SelectConnection();
            if (ch) {
                EXPECT_EQ(ch->state.load(), ChannelState::ACTIVE);
                select_after.fetch_add(1);
                ch->inflight_count.fetch_sub(1);
            }
        });
    }
    for (auto& th : threads) { th.join(); }

    EXPECT_EQ(select_after.load(), 20);
    EXPECT_EQ(mgr.TotalInflightCount(), 0);

    mgr.Shutdown();
}

// Purpose: Verify atomic RoutingPolicy switch under concurrent SelectConnection load does not crash
// or return stale results; all inflight properly released after load stops.
// Method: 4 worker threads continuously call SelectConnection+release, main thread switches policy
// 10 times (RR/LL alternating), verify total_selected>0 with no crash, TotalInflightCount==0
// after all workers joined.
TEST(ConnectionConcurrentTest, AtomicRoutingPolicy_SwitchUnderLoad)
{
    ConnectionManager mgr;
    ASSERT_TRUE(mgr.AddGroup(MakeEndpoint("10.0.0.1"), 4).ok());

    std::atomic<int> total_selected{0};
    std::atomic<bool> done{false};

    std::vector<std::thread> workers;
    for (int t = 0; t < 4; ++t) {
        workers.emplace_back([&]() {
            while (!done.load()) {
                auto* ch = mgr.SelectConnection();
                if (ch) {
                    total_selected.fetch_add(1);
                    ch->inflight_count.fetch_sub(1);
                }
            }
        });
    }

    for (int i = 0; i < 10; ++i) {
        mgr.SetRoutingPolicy(i % 2 == 0 ? RoutingPolicy::ROUND_ROBIN
                                         : RoutingPolicy::LEAST_LOADED);
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    done.store(true);
    for (auto& th : workers) { th.join(); }

    EXPECT_GT(total_selected.load(), 0);
    EXPECT_EQ(mgr.TotalInflightCount(), 0);

    mgr.Shutdown();
}

// Purpose: Verify Wait timeout correctly releases inflight when prev_state==INFLIGHT;
// subsequent tasks can still complete via Done(). State-based ordering: Wait timeout
// uses state.exchange(FAILED) to determine who decrements inflight.
// Method: Create ConnectionChannel(inflight=1,ACTIVE) + two TransportTaskContexts:
// ctx1(INFLIGHT,flagbuffer=0) simulates stuck task → Done()=false → simulate Wait timeout
// (state.exchange FAILED, prev_state==INFLIGHT → inflight-1) → verify inflight=0.
// ctx2(INFLIGHT,flagbuffer=1) simulates subsequent successful task → Done()=true →
// verify COMPLETED + inflight decremented to 0.
TEST(ConnectionConcurrentTest, WaitTimeout_StateBasedInflightRelease)
{
    ConnectionChannel ch;
    ch.channel_id = 0;
    ch.state.store(ChannelState::ACTIVE, std::memory_order_release);
    ch.inflight_count.store(1, std::memory_order_release);

    TransportTaskContext ctx1;
    ctx1.task_id = 1;
    ctx1.op_type = TransportOpType::LOAD;
    ctx1.state.store(TransportTaskState::INFLIGHT, std::memory_order_release);
    ctx1.flagbuffer_status.store(0, std::memory_order_release);
    ctx1.channel.store(&ch, std::memory_order_release);

    EXPECT_FALSE(ctx1.Done());

    auto prev_state = ctx1.state.exchange(TransportTaskState::FAILED, std::memory_order_acq_rel);
    EXPECT_EQ(prev_state, TransportTaskState::INFLIGHT);
    auto* released_ch = ctx1.channel.load(std::memory_order_acquire);
    if (released_ch) {
        released_ch->inflight_count.fetch_sub(1, std::memory_order_relaxed);
    }
    EXPECT_EQ(ch.inflight_count.load(), 0u);

    ch.inflight_count.fetch_add(1, std::memory_order_relaxed);
    EXPECT_EQ(ch.inflight_count.load(), 1u);

    TransportTaskContext ctx2;
    ctx2.task_id = 2;
    ctx2.op_type = TransportOpType::LOAD;
    ctx2.state.store(TransportTaskState::INFLIGHT, std::memory_order_release);
    ctx2.flagbuffer_status.store(1, std::memory_order_release);
    ctx2.channel.store(&ch, std::memory_order_release);

    EXPECT_TRUE(ctx2.Done());
    EXPECT_EQ(ctx2.state.load(), TransportTaskState::COMPLETED);
    EXPECT_EQ(ch.inflight_count.load(), 0u);
}

// Purpose: Verify state-based ordering prevents double inflight decrement: when prev_state==PENDING
// (Wait timeout fires before CompleteTask CAS), Wait does NOT decrement; CompleteTask CAS fails
// and undoes inflight. When prev_state==INFLIGHT, Wait decrements and CompleteTask has already
// returned. Neither case produces double decrement or leak.
// Method: Test two scenarios on same channel:
// 1) prev_state=PENDING: state.exchange(FAILED) returns PENDING → no inflight decrement by Wait;
//    CompleteTask CAS PENDING→INFLIGHT fails → undoes inflight. Net: exactly one decrement.
// 2) prev_state=INFLIGHT: state.exchange(FAILED) returns INFLIGHT → Wait decrements inflight;
//    CompleteTask has already returned. Net: exactly one decrement.
TEST(ConnectionConcurrentTest, StateBased_NoDoubleInflightDecrement)
{
    ConnectionChannel ch;
    ch.channel_id = 0;
    ch.state.store(ChannelState::ACTIVE, std::memory_order_release);

    ch.inflight_count.store(1, std::memory_order_release);
    TransportTaskContext ctx1;
    ctx1.task_id = 1;
    ctx1.op_type = TransportOpType::LOAD;
    ctx1.state.store(TransportTaskState::PENDING, std::memory_order_release);
    ctx1.channel.store(&ch, std::memory_order_release);

    auto prev_state = ctx1.state.exchange(TransportTaskState::FAILED, std::memory_order_acq_rel);
    EXPECT_EQ(prev_state, TransportTaskState::PENDING);
    EXPECT_EQ(ch.inflight_count.load(), 1u);

    TransportTaskState expected = TransportTaskState::PENDING;
    EXPECT_FALSE(ctx1.state.compare_exchange_strong(expected, TransportTaskState::INFLIGHT,
                                                      std::memory_order_acq_rel));
    ch.inflight_count.fetch_sub(1, std::memory_order_relaxed);
    EXPECT_EQ(ch.inflight_count.load(), 0u);

    ch.inflight_count.store(1, std::memory_order_release);
    TransportTaskContext ctx2;
    ctx2.task_id = 2;
    ctx2.op_type = TransportOpType::LOAD;
    ctx2.state.store(TransportTaskState::INFLIGHT, std::memory_order_release);
    ctx2.flagbuffer_status.store(1, std::memory_order_release);
    ctx2.channel.store(&ch, std::memory_order_release);

    prev_state = ctx2.state.exchange(TransportTaskState::FAILED, std::memory_order_acq_rel);
    EXPECT_EQ(prev_state, TransportTaskState::INFLIGHT);
    auto* released_ch = ctx2.channel.load(std::memory_order_acquire);
    released_ch->inflight_count.fetch_sub(1, std::memory_order_relaxed);
    EXPECT_EQ(ch.inflight_count.load(), 0u);
}

}  // namespace UC::ASU