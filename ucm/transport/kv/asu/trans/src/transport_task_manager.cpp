#include "connection_internal.h"
#include "debug_log.h"
#include "transport_task_manager.h"

namespace UC::ASU {

bool TransportTaskContext::Done()
{
    auto s = state.load(std::memory_order_acquire);
    if (s == TransportTaskState::COMPLETED || s == TransportTaskState::FAILED ||
        s == TransportTaskState::CANCELED) {
        debug_log("TransportTaskContext::Done",
                  "task_id=" + std::to_string(task_id)
                  + " state=" + std::to_string(static_cast<int>(s)) + " (terminal)");
        return true;
    }
    if (s == TransportTaskState::INFLIGHT) {
        if (flagbuffer_status.load(std::memory_order_acquire) >= 1) {
            debug_log("TransportTaskContext::Done",
                    "task_id=" + std::to_string(task_id)
                    + " INFLIGHT + flagbuffer ready->COMPLETED");
            final_status = Status::OK();
            if (op_type == TransportOpType::QUERY) {
                query_result.exists.assign(keys.size, 0);
                query_result.prefix_hit_keys = 0;
            }
            state.store(TransportTaskState::COMPLETED, std::memory_order_release);
            auto* ch = channel.load(std::memory_order_acquire);
            if (ch) {
                debug_log("TransportTaskContext::Done",
                        "inflight-1 on ch_id=" + std::to_string(ch->channel_id));
                ch->inflight_count.fetch_sub(1, std::memory_order_relaxed);
            }
            return true;
        } else {
            debug_log("TransportTaskContext::Done",
                    "task_id=" + std::to_string(task_id)
                    + " INFLIGHT but flagbuffer not ready");
        }
    }
    return false;
}

}  // namespace UC::ASU