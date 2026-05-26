#include <cstdint>
#include <gtest/gtest.h>
#include "connection_internal.h"
#include "connection_manager.h"

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

}  // namespace

// Purpose: Verify AddGroup creates correct number of channels with ACTIVE state and proper active_count for a single endpoint.
// Method: Add 1 group with 4 QPs, select a channel, assert state=ACTIVE, group_id=0, channels.size=4, active_count=4.
TEST(ConnectionManagerTest, AddGroup_SingleEndpoint)
{
    ConnectionManager mgr;
    auto s = mgr.AddGroup(MakeEndpoint("10.0.0.1"), 4);
    ASSERT_TRUE(s.ok());

    auto* ch = mgr.SelectConnection();
    ASSERT_NE(ch, nullptr);
    EXPECT_EQ(ch->inflight_count.load(), 1u);
    EXPECT_EQ(ch->state.load(), ChannelState::ACTIVE);
    EXPECT_EQ(ch->group->group_id, 0u);
    EXPECT_EQ(ch->group->channels.size(), 4u);
    EXPECT_EQ(ch->group->active_count.load(), 4u);
    EXPECT_EQ(mgr.TotalInflightCount(), 1);

    ch->inflight_count.fetch_sub(1);
    EXPECT_EQ(mgr.TotalInflightCount(), 0);

    mgr.Shutdown();
}

// Purpose: Verify multiple AddGroup calls create independent groups and SelectConnection can route across groups.
// Method: Add 2 groups (2+3 QPs), call SelectConnection twice, verify both channels belong to valid group_ids (0 or 1).
TEST(ConnectionManagerTest, AddGroup_MultipleEndpoints)
{
    ConnectionManager mgr;
    ASSERT_TRUE(mgr.AddGroup(MakeEndpoint("10.0.0.1"), 2).ok());
    ASSERT_TRUE(mgr.AddGroup(MakeEndpoint("10.0.0.2"), 3).ok());

    auto* ch1 = mgr.SelectConnection();
    ASSERT_NE(ch1, nullptr);
    auto* ch2 = mgr.SelectConnection();
    ASSERT_NE(ch2, nullptr);

    EXPECT_TRUE(ch1->group->group_id == 0u || ch1->group->group_id == 1u);
    EXPECT_TRUE(ch2->group->group_id == 0u || ch2->group->group_id == 1u);

    ch1->inflight_count.fetch_sub(1);
    ch2->inflight_count.fetch_sub(1);

    mgr.Shutdown();
}

// Purpose: Verify RoundRobin policy distributes selections across channels rather than always picking the same one.
// Method: Set ROUND_ROBIN, call SelectConnection 8 times (release inflight each time), verify channel_ids vary.
TEST(ConnectionManagerTest, SelectConnection_RoundRobin)
{
    ConnectionManager mgr;
    ASSERT_TRUE(mgr.AddGroup(MakeEndpoint("10.0.0.1"), 4).ok());
    mgr.SetRoutingPolicy(RoutingPolicy::ROUND_ROBIN);

    std::vector<std::uint32_t> ids;
    for (int i = 0; i < 8; ++i) {
        auto* ch = mgr.SelectConnection();
        ASSERT_NE(ch, nullptr);
        ids.push_back(ch->channel_id);
        ch->inflight_count.fetch_sub(1);
    }

    bool varied = false;
    for (std::size_t i = 1; i < ids.size(); ++i) {
        if (ids[i] != ids[0]) { varied = true; break; }
    }
    EXPECT_TRUE(varied);

    mgr.Shutdown();
}

// Purpose: Verify LeastLoaded policy selects the channel with lowest inflight_count, balancing load evenly.
// Method: Set LEAST_LOADED, call SelectConnection 3 times, verify each channel has inflight_count=1 (even distribution).
TEST(ConnectionManagerTest, SelectConnection_LeastLoaded)
{
    ConnectionManager mgr;
    ASSERT_TRUE(mgr.AddGroup(MakeEndpoint("10.0.0.1"), 3).ok());
    mgr.SetRoutingPolicy(RoutingPolicy::LEAST_LOADED);

    auto* ch0 = mgr.SelectConnection();
    ASSERT_NE(ch0, nullptr);
    EXPECT_EQ(ch0->inflight_count.load(), 1u);

    auto* ch1 = mgr.SelectConnection();
    ASSERT_NE(ch1, nullptr);

    auto* ch2 = mgr.SelectConnection();
    ASSERT_NE(ch2, nullptr);

    EXPECT_EQ(ch0->inflight_count.load(), 1u);
    EXPECT_EQ(ch1->inflight_count.load(), 1u);
    EXPECT_EQ(ch2->inflight_count.load(), 1u);

    ch0->inflight_count.fetch_sub(1);
    ch1->inflight_count.fetch_sub(1);
    ch2->inflight_count.fetch_sub(1);

    mgr.Shutdown();
}

// Purpose: Verify SelectConnection returns nullptr when all channels are in FAILED state (no available connection).
// Method: Manually BeginDrain+FinishDrain 2 channels to FAILED, release inflight, call SelectConnection, expect nullptr.
TEST(ConnectionManagerTest, SelectConnection_NoActiveChannel)
{
    ConnectionManager mgr;
    ASSERT_TRUE(mgr.AddGroup(MakeEndpoint("10.0.0.1"), 2).ok());

    auto* ch0 = mgr.SelectConnection();
    auto* ch1 = mgr.SelectConnection();

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

    auto* ch = mgr.SelectConnection();
    EXPECT_EQ(ch, nullptr);

    mgr.Shutdown();
}

// Purpose: Verify ReportFailure does not trigger drain when error_count is below threshold, channel stays ACTIVE.
// Method: Call ReportFailure once (threshold=2), verify error_count=1 and state remains ACTIVE.
TEST(ConnectionManagerTest, ReportFailure_BelowThreshold)
{
    ConnectionManager mgr;
    ASSERT_TRUE(mgr.AddGroup(MakeEndpoint("10.0.0.1"), 2).ok());

    auto* ch = mgr.SelectConnection();
    ASSERT_NE(ch, nullptr);
    ch->inflight_count.fetch_sub(1);

    mgr.ReportFailure(ch);
    EXPECT_EQ(ch->error_count.load(), 1u);
    EXPECT_EQ(ch->state.load(), ChannelState::ACTIVE);

    mgr.Shutdown();
}

// Purpose: Verify ReportFailure at threshold triggers BeginDrain (CAS->DRAINING), RecoverLoop completes drain+rebuild, active_count restores.
// Method: 2 ReportFailure calls reach threshold -> state=DRAINING, active_count-1 -> release inflight -> wait 500ms -> state=FAILED, RebuildChannel -> active_count restored.
TEST(ConnectionManagerTest, ReportFailure_AtThreshold_TriggersDrain)
{
    ConnectionManager mgr;
    ASSERT_TRUE(mgr.AddGroup(MakeEndpoint("10.0.0.1"), 2).ok());
    mgr.StartRecoverLoop();

    auto* ch = mgr.SelectConnection();
    ASSERT_NE(ch, nullptr);
    auto active_before = ch->group->active_count.load();

    mgr.ReportFailure(ch);
    EXPECT_EQ(ch->state.load(), ChannelState::ACTIVE);

    mgr.ReportFailure(ch);
    EXPECT_EQ(ch->state.load(), ChannelState::DRAINING);
    EXPECT_EQ(ch->group->active_count.load(), active_before - 1);

    ch->inflight_count.fetch_sub(1);
    EXPECT_EQ(mgr.TotalInflightCount(), 0);

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    EXPECT_EQ(ch->state.load(), ChannelState::FAILED);
    EXPECT_TRUE(ch->group->HasActiveChannel());
    EXPECT_EQ(ch->group->active_count.load(), active_before);
    EXPECT_EQ(mgr.TotalInflightCount(), 0);

    mgr.Shutdown();
}

// Purpose: Verify BeginDrain uses CAS(ACTIVE->DRAINING) ensuring only one caller succeeds; second call returns false.
// Method: Call BeginDrain twice on same channel; first returns true+state=DRAINING, second returns false.
TEST(ConnectionManagerTest, BeginDrain_CAS_OnlyOnce)
{
    ConnectionManager mgr;
    ASSERT_TRUE(mgr.AddGroup(MakeEndpoint("10.0.0.1"), 2).ok());

    auto* ch = mgr.SelectConnection();
    ch->inflight_count.fetch_sub(1);

    EXPECT_TRUE(ch->BeginDrain());
    EXPECT_FALSE(ch->BeginDrain());
    EXPECT_EQ(ch->state.load(), ChannelState::DRAINING);

    mgr.Shutdown();
}

// Purpose: Verify FinishDrain deletes native_qp and sets channel state to FAILED.
// Method: Manually call BeginDrain then FinishDrain, verify native_qp=nullptr and state=FAILED.
TEST(ConnectionManagerTest, FinishDrain_DeletesQPAndSetsFailed)
{
    ConnectionManager mgr;
    ASSERT_TRUE(mgr.AddGroup(MakeEndpoint("10.0.0.1"), 2).ok());

    auto* ch = mgr.SelectConnection();
    ch->inflight_count.fetch_sub(1);

    ch->BeginDrain();
    ch->FinishDrain();

    EXPECT_EQ(ch->native_qp, nullptr);
    EXPECT_EQ(ch->state.load(), ChannelState::FAILED);

    mgr.Shutdown();
}

// Purpose: Verify RebuildChannel creates a new ACTIVE channel on a FAILED group with incremented channel_id and increased channel count.
// Method: BeginDrain+FinishDrain to set FAILED, then RebuildChannel, verify new channel state=ACTIVE, channel_id=2, channels.size=3, HasActiveChannel=true.
TEST(ConnectionManagerTest, RebuildChannel_CreatesNewActive)
{
    ConnectionManager mgr;
    ASSERT_TRUE(mgr.AddGroup(MakeEndpoint("10.0.0.1"), 2).ok());

    auto* ch = mgr.SelectConnection();
    ch->inflight_count.fetch_sub(1);

    ch->BeginDrain();
    ch->FinishDrain();

    auto* new_ch = ch->group->RebuildChannel();
    ASSERT_NE(new_ch, nullptr);
    EXPECT_EQ(new_ch->state.load(), ChannelState::ACTIVE);
    EXPECT_EQ(new_ch->inflight_count.load(), 0u);
    EXPECT_EQ(new_ch->channel_id, 2u);
    EXPECT_EQ(ch->group->channels.size(), 3u);
    EXPECT_TRUE(ch->group->HasActiveChannel());

    mgr.Shutdown();
}

// Purpose: Verify HasActiveChannel returns true when some channels are ACTIVE and false when all are DRAINING/FAILED.
// Method: 2 channels - initially HasActive=true, after one BeginDrain still true, after both BeginDrain+FinishDrain becomes false.
TEST(ConnectionManagerTest, HasActiveChannel_TrueAndFalse)
{
    ConnectionManager mgr;
    ASSERT_TRUE(mgr.AddGroup(MakeEndpoint("10.0.0.1"), 2).ok());

    auto* ch0 = mgr.SelectConnection();
    auto* ch1 = mgr.SelectConnection();

    EXPECT_TRUE(ch0->group->HasActiveChannel());

    ch0->BeginDrain();
    EXPECT_TRUE(ch0->group->HasActiveChannel());

    ch1->BeginDrain();
    ch0->FinishDrain();
    ch1->FinishDrain();
    EXPECT_FALSE(ch0->group->HasActiveChannel());

    ch0->inflight_count.fetch_sub(1);
    ch1->inflight_count.fetch_sub(1);

    mgr.Shutdown();
}

// Purpose: Verify Shutdown cleans up all groups and channels; SelectConnection returns nullptr afterwards.
// Method: Add 2 groups, call Shutdown, verify it returns OK, then SelectConnection returns nullptr.
TEST(ConnectionManagerTest, Shutdown_CleansUp)
{
    ConnectionManager mgr;
    ASSERT_TRUE(mgr.AddGroup(MakeEndpoint("10.0.0.1"), 4).ok());
    ASSERT_TRUE(mgr.AddGroup(MakeEndpoint("10.0.0.2"), 2).ok());

    auto s = mgr.Shutdown();
    EXPECT_TRUE(s.ok());

    auto* ch = mgr.SelectConnection();
    EXPECT_EQ(ch, nullptr);
}

// Purpose: Verify RecoverLoop automatically completes drain+rebuild cycle after channel failure.
// Method: Start RecoverLoop, trigger drain via 2x ReportFailure, release inflight, wait 500ms, verify state=FAILED, channels.size increased, HasActive=true.
TEST(ConnectionManagerTest, RecoverLoop_DrainAndRebuild)
{
    ConnectionManager mgr;
    ASSERT_TRUE(mgr.AddGroup(MakeEndpoint("10.0.0.1"), 3).ok());
    mgr.StartRecoverLoop();

    auto* ch = mgr.SelectConnection();
    ASSERT_NE(ch, nullptr);
    auto initial_channels = ch->group->channels.size();

    mgr.ReportFailure(ch);
    mgr.ReportFailure(ch);
    EXPECT_EQ(ch->state.load(), ChannelState::DRAINING);

    ch->inflight_count.fetch_sub(1);
    EXPECT_EQ(mgr.TotalInflightCount(), 0);

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    EXPECT_EQ(ch->state.load(), ChannelState::FAILED);
    EXPECT_EQ(ch->group->channels.size(), initial_channels + 1);
    EXPECT_TRUE(ch->group->HasActiveChannel());
    EXPECT_EQ(mgr.TotalInflightCount(), 0);

    mgr.Shutdown();
}

// Purpose: Verify all 4 channels can be failed and rebuilt simultaneously; group eventually recovers HasActiveChannel.
// Method: For each of 4 channels, call 2x ReportFailure + release inflight, wait 800ms, verify all FAILED and HasActiveChannel=true.
TEST(ConnectionManagerTest, MultipleFailures_DrainAndRebuildAll)
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

    for (auto* ch : channels) {
        mgr.ReportFailure(ch);
        mgr.ReportFailure(ch);
        ch->inflight_count.fetch_sub(1);
    }
    EXPECT_EQ(mgr.TotalInflightCount(), 0);

    std::this_thread::sleep_for(std::chrono::milliseconds(800));

    for (auto* ch : channels) {
        EXPECT_EQ(ch->state.load(), ChannelState::FAILED);
    }
    EXPECT_TRUE(channels[0]->group->HasActiveChannel());
    EXPECT_EQ(channels[0]->group->active_count.load(), 4u);
    EXPECT_EQ(mgr.TotalInflightCount(), 0);

    mgr.Shutdown();
}

// Purpose: Verify that when all channels in a group are FAILED (simulating rebuild failure
// scenario where no new channel is created), the group correctly reports HasActive=false,
// active_count=0, channels.size unchanged, and SelectConnection returns nullptr.
// Method: AddGroup(2 QPs), manually BeginDrain+FinishDrain both channels + decrement
// active_count (simulating drain without successful rebuild), release inflight, verify
// group offline state. Then RebuildChannel succeeds (StubCreateConnection always OK),
// verifying recovery restores active_count and HasActive.
TEST(ConnectionManagerTest, AllChannelsFailed_GroupOffline)
{
    ConnectionManager mgr;
    ASSERT_TRUE(mgr.AddGroup(MakeEndpoint("10.0.0.1"), 2).ok());

    auto* ch0 = mgr.SelectConnection();
    auto* ch1 = mgr.SelectConnection();
    ASSERT_NE(ch0, nullptr);
    ASSERT_NE(ch1, nullptr);

    ch0->BeginDrain();
    ch1->BeginDrain();
    ch0->FinishDrain();
    ch1->FinishDrain();

    ch0->group->active_count.fetch_sub(1, std::memory_order_release);
    ch1->group->active_count.fetch_sub(1, std::memory_order_release);

    ch0->inflight_count.fetch_sub(1);
    ch1->inflight_count.fetch_sub(1);

    EXPECT_EQ(ch0->state.load(), ChannelState::FAILED);
    EXPECT_EQ(ch1->state.load(), ChannelState::FAILED);
    EXPECT_EQ(ch0->group->channels.size(), 2u);
    EXPECT_EQ(ch0->group->active_count.load(), 0u);
    EXPECT_FALSE(ch0->group->HasActiveChannel());
    EXPECT_EQ(mgr.TotalInflightCount(), 0);

    auto* sel = mgr.SelectConnection();
    EXPECT_EQ(sel, nullptr);

    auto* new_ch0 = ch0->group->RebuildChannel();
    ASSERT_NE(new_ch0, nullptr);
    EXPECT_EQ(new_ch0->state.load(), ChannelState::ACTIVE);
    ch0->group->active_count.fetch_add(1, std::memory_order_release);
    EXPECT_TRUE(ch0->group->HasActiveChannel());
    EXPECT_EQ(ch0->group->active_count.load(), 1u);

    auto* new_ch1 = ch0->group->RebuildChannel();
    ASSERT_NE(new_ch1, nullptr);
    ch0->group->active_count.fetch_add(1, std::memory_order_release);
    EXPECT_EQ(ch0->group->active_count.load(), 2u);

    auto* recovered = mgr.SelectConnection();
    ASSERT_NE(recovered, nullptr);
    EXPECT_EQ(recovered->state.load(), ChannelState::ACTIVE);
    recovered->inflight_count.fetch_sub(1);
    EXPECT_EQ(mgr.TotalInflightCount(), 0);

    mgr.Shutdown();
}

// Purpose: Verify RecoverLoop forces drain completion when drain timeout elapses even with
// inflight > 0 (stuck task); leaked inflight is later released by Wait timeout mechanism.
// Method: AddGroup(2 QPs)+StartRecoverLoop, trigger drain on ch0, set drain_start_time=0
// (simulate elapsed >= 30s), keep ch0 inflight=1 (stuck), release ch1 inflight, wait 500ms,
// verify ch0=FAILED despite inflight=1, HasActive=true (rebuild succeeded), then simulate
// Wait timeout release (inflight-1), verify TotalInflightCount=0.
TEST(ConnectionManagerTest, DrainTimeout_ForceCompletionWithInflight)
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

    ch0->drain_start_time.store(0, std::memory_order_release);
    ch1->inflight_count.fetch_sub(1);
    EXPECT_EQ(mgr.TotalInflightCount(), 1);

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    EXPECT_EQ(ch0->state.load(), ChannelState::FAILED);
    EXPECT_EQ(ch0->inflight_count.load(), 1u);
    EXPECT_TRUE(ch0->group->HasActiveChannel());

    ch0->inflight_count.fetch_sub(1);
    EXPECT_EQ(ch0->inflight_count.load(), 0u);
    EXPECT_EQ(mgr.TotalInflightCount(), 0);

    mgr.Shutdown();
}

// Purpose: Verify StubCreateConnection returns the requested number of null handles (stub placeholder).
// Method: Request 3 QPs, verify 3 nullptr handles returned.
TEST(ConnectionManagerTest, StubCreateConnection_ReturnsNullHandles)
{
    auto handles = StubCreateConnection(MakeEndpoint("10.0.0.1"), 3);
    EXPECT_EQ(handles.size(), 3u);
    for (auto& h : handles) {
        EXPECT_EQ(h, nullptr);
    }
}

// Purpose: Verify StubDeleteConnections returns OK for all input handles (stub placeholder).
// Method: Pass 2 nullptr handles, verify 2 OK statuses returned.
TEST(ConnectionManagerTest, StubDeleteConnections_ReturnsAllOK)
{
    std::vector<ConnectionHandle> handles{nullptr, nullptr};
    auto results = StubDeleteConnections(handles);
    EXPECT_EQ(results.size(), 2u);
    for (auto& r : results) {
        EXPECT_TRUE(r.ok());
    }
}

// Purpose: Verify ConnectionChannel::Send stub returns OK and sets flagbuffer_status=1 on valid ctx in ACTIVE state.
// Method: Construct a channel with ACTIVE state, create a valid TransportTaskContext, call Send(ctx), verify return is OK, flagbuffer_status=1.
TEST(ConnectionManagerTest, Send_ReturnsOK)
{
    ConnectionChannel ch;
    ch.channel_id = 0;
    ch.state.store(ChannelState::ACTIVE);
    TransportTaskContext ctx;
    EXPECT_TRUE(ch.StubSend(&ctx).ok());
    EXPECT_EQ(ctx.flagbuffer_status.load(), 1u);
}

// Purpose: Verify the full state transition chain ACTIVE -> DRAINING -> FAILED works correctly.
// Method: Offline channel, call BeginDrain (expect ACTIVE->DRAINING), then FinishDrain (expect DRAINING->FAILED).
TEST(ConnectionManagerTest, ChannelState_Transitions)
{
    ConnectionChannel ch;
    ch.channel_id = 0;
    EXPECT_EQ(ch.state.load(), ChannelState::ACTIVE);

    EXPECT_TRUE(ch.BeginDrain());
    EXPECT_EQ(ch.state.load(), ChannelState::DRAINING);

    ch.FinishDrain();
    EXPECT_EQ(ch.state.load(), ChannelState::FAILED);
}

// Purpose: Verify SelectConnection returns nullptr when all channels reach
// inflight capacity limit; channels below limit are still selectable.
// Method: AddGroup(2 QPs), set both channels' inflight_count to kMaxInflightPerChannel,
// call SelectConnection -> nullptr. Reduce one channel's inflight by 1, call
// SelectConnection -> that channel is selected (LeastLoaded picks it). Release all inflight.
TEST(ConnectionManagerTest, SelectConnection_InflightCapacityLimit)
{
    ConnectionManager mgr;
    ASSERT_TRUE(mgr.AddGroup(MakeEndpoint("10.0.0.1"), 2).ok());

    auto* ch0 = mgr.SelectConnection();
    auto* ch1 = mgr.SelectConnection();
    ASSERT_NE(ch0, nullptr);
    ASSERT_NE(ch1, nullptr);

    ch0->inflight_count.store(256, std::memory_order_release);
    ch1->inflight_count.store(256, std::memory_order_release);

    auto* sel = mgr.SelectConnection();
    EXPECT_EQ(sel, nullptr);

    ch0->inflight_count.fetch_sub(1, std::memory_order_relaxed);
    sel = mgr.SelectConnection();
    ASSERT_NE(sel, nullptr);
    EXPECT_EQ(sel, ch0);
    EXPECT_EQ(ch0->inflight_count.load(), 256u);

    ch0->inflight_count.store(0, std::memory_order_release);
    ch1->inflight_count.store(0, std::memory_order_release);
    EXPECT_EQ(mgr.TotalInflightCount(), 0);

    mgr.Shutdown();
}

}  // namespace UC::ASU