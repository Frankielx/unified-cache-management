#include "transport_task_manager.h"
#include "logger.h"

namespace UC::ASU {

bool TransportTaskContext::Done() const
{
    auto s = state.load(std::memory_order_acquire);
    return s == TransportTaskState::COMPLETED || s == TransportTaskState::FAILED ||
           s == TransportTaskState::CANCELED;
}

void TransportTaskManager::Shutdown()
{
    return;
}

}  // namespace UC::ASU