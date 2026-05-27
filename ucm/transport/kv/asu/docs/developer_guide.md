# ConnectionManager and TransProvider Design Document

## 1. Overall Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                     AsuTransportImpl                              │
│                                                                  │
│   ┌──────────────────────┐    ┌──────────────────────────────┐  │
│   │  ConnectionManager   │    │     TransProvider             │  │
│   │                      │    │     (Abstract Base Class)     │  │
│   │  - Connection Select │    │                               │  │
│   │  - Fault Detection   │◄───│  - CreateConnection           │  │
│   │  - Endpoint Sharing  │    │  - DeleteConnections          │  │
│   │                      │    │  - Send                       │  │
│   │  ┌────────────────┐  │    │  - RegisterMemory             │  │
│   │  │ConnectionGroup │  │    │  - UnregisterMemory           │  │
│   │  │  └─ Channel[]  │  │    │  - AllocThread                │  │
│   │  │channelCache_   │  │    │  - FreeThread                 │  │
│   │  │drainList_      │  │    │                               │  │
│   │  └────────────────┘  │    └──────────┬───────────────────┘  │
│   │                      │               │                       │
│   │  RecoverLoop (BG)    │               ▼                       │
│   └──────────────────────┘    ┌──────────────────────────────┐  │
│                               │   AICPUTransProvider          │  │
│                               │   (HCOMM Implementation)      │  │
│                               │                               │  │
│                               │  endpoint_: Single HCOMM ep   │  │
│                               │  localIp_: Bound IP           │  │
│                               │  endpointRefCount_: Ref count │  │
│                               │                               │  │
│                               │  HcommEndpointCreate          │  │
│                               │  HcommChannelCreate           │  │
│                               │  HcommThreadAlloc             │  │
│                               │  HcommMemReg/Unreg            │  │
│                               │  HcommMemExport/Import        │  │
│                               └──────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
```

## 2. ConnectionManager

### 2.1 Core Responsibilities

- Manage multiple ConnectionGroups (one group per remote endpoint)
- Provide connection selection strategies (Round Robin / Least Loaded)
- Detect connection faults and automatically recover
- Maintain channelCache to accelerate connection selection

### 2.2 Data Structures

```cpp
class ConnectionManager {
    // Connection groups: one group per remote endpoint
    std::vector<std::unique_ptr<ConnectionGroup>> groups_;
    
    // Active channel cache (flattened for fast selection)
    std::vector<std::shared_ptr<ConnectionChannel>> channelCache_;
    std::atomic<bool> cacheDirty_{false};
    
    // Channels pending recovery
    std::vector<std::shared_ptr<ConnectionChannel>> drainList_;
    
    // Connection creation callback
    CreateConnectionFunc createFn_;
};

class ConnectionGroup {
    std::uint32_t groupId;
    AsuEndpoint endpoint;
    std::vector<std::shared_ptr<ConnectionChannel>> channels;
};

class ConnectionChannel {
    std::uint32_t channelId;
    ConnectionGroup* group;
    TransProvider::ConnectionHandle handle_;  // void*, points to LinkContext
    std::atomic<std::uint32_t> inflightCount{0};
    std::atomic<ChannelState> state{ChannelState::ACTIVE};
    std::atomic<std::uint32_t> errorCount{0};
};
```

### 2.3 Connection Selection Strategies

**Round Robin**:
```cpp
idx = rrIndex_.fetch_add(1)
start = idx % total
for i in [0, total):
    pos = (start + i) % total
    channel = channelCache_[pos]
    if channel->state == ACTIVE && inflight < 256:
        IncrementInflight()
        return channel
```

**Least Loaded**:
```cpp
min_inflight = MAX
for channel in channelCache_:
    if channel->state == ACTIVE && inflight < min_inflight:
        min_inflight = inflight
        selected = channel
        if min_inflight == 0: break
IncrementInflight(selected)
return selected
```

**Multi-IP Load Balancing**: `channelCache_` contains channels from all groups. `SelectConnection` selects from the entire cache, naturally supporting cross-link load balancing.

### 2.4 Fault Detection and Recovery

**Fault Detection**:
```
ReportFailure(channel):
    errorCount++
    if errorCount < 2: return  // Below threshold
    MarkForDrain()  // CAS: ACTIVE → DRAINING
    cacheDirty = true
    drainList.push_back(channel)
```

**Fault Recovery (RecoverLoop, executes every 100ms)**:
```
swap(drainList_, to_recover)
for each channel in to_recover:
    if inflight > 0: put back to drainList, continue waiting
    else:
        createFn_(endpoint, 1) create new connection
        ├─ Failed → put back to drainList, retry next time
        └─ Success →
            RemoveChannel(old_channel)
            AddChannel(new_channel)
            cacheDirty = true
```

**Key Design**: Only reclaim when `inflight==0`, no timeout-based forced reclamation to avoid use-after-free.

### 2.5 Endpoint Sharing

The provider maintains only one endpoint, shared by all connections. Created on first `CreateConnection` call, subsequent calls validate `localIp` consistency:

```
endpoint_: Single HCOMM endpoint
localIp_: localIp bound to endpoint
endpointRefCount_: Reference count

CreateConnection(localIp):
    GetOrCreateEndpoint(localIp)
    ├─ endpoint_ exists and localIp matches → endpointRefCount_++
    ├─ endpoint_ exists and localIp mismatch → return error
    └─ endpoint_ is null → HcommEndpointCreate, endpointRefCount_=1
    HcommChannelCreate(endpoint_, ...)
    HcommThreadAlloc(...)

DeleteConnection(handle):
    HcommThreadFree(...)
    HcommChannelDestroy(...)
    ReleaseEndpoint(localIp) → endpointRefCount_--
       └─ When endpointRefCount_ == 0: HcommEndpointDestroy, endpoint_=nullptr
```

### 2.6 Shutdown Cleanup Order

```
ConnectionManager::Shutdown():
    1. channelCache_.clear()
    2. drainList_.clear()
    3. groups_.clear()  // Destroy ConnectionGroup last
```

Clear references first, then destroy objects to ensure all shared_ptr references are released before destruction.

### 2.7 Locks and Synchronization

| Lock | Protected Objects | Used By | Type |
|------|-------------------|---------|------|
| `structureMu_` | groups_, channelCache_ | Worker/Recover | std::shared_mutex |
| `drainMu_` | drainList_ | Worker/Poller/Recover | std::shared_mutex |

## 3. TransProvider

### 3.1 Abstract Base Class

```cpp
class TransProvider {
public:
    using ConnectionHandle = void*;
    using ThreadHandle = void*;
    using MemHandle = void*;

    // Connection management
    virtual Status CreateConnection(localIp, remoteIp, port, qpNum, timeout, &handles) = 0;
    virtual std::vector<Status> DeleteConnections(handles) = 0;

    // Data transmission
    virtual std::vector<Status> Send(ioBatches, kernelCount, quietCount) = 0;

    // Memory management
    virtual Status RegisterMemory(handle, memDescs, &memHandles) = 0;
    virtual std::vector<Status> UnregisterMemory(unregDescs) = 0;
    virtual Status GetMemTokenId(memHandle, &tokenId) = 0;

    // Thread management
    virtual Status AllocThread(threadNum, notifyNumPerThread, &threads) = 0;
    virtual std::vector<Status> FreeThread(threads) = 0;
};
```

### 3.2 AICPUTransProvider (HCOMM Implementation)

#### Internal Structures

```cpp
struct LinkContext {
    std::string localIp;
    uint64_t channel;
    uint64_t thread;
    aclrtStream stream;
    std::string remoteIp;
    uint16_t remotePort;
};

void* endpoint_{nullptr};
std::string localIp_;
uint32_t endpointRefCount_{0};

aclrtBinHandle kernelBin_{nullptr};
aclrtFuncHandle kernelFunc_{nullptr};
```

#### HCOMM Parameters

| Parameter | Value | Description |
|-----------|-------|-------------|
| Engine | `COMM_ENGINE_AICPU_TS` | AICPU transport engine |
| Endpoint Type | `ENDPOINT_LOC_TYPE_DEVICE` | Device endpoint |
| Socket Role | `HCOMM_SOCKET_ROLE_RESERVED` | Reserved role |
| notifyNum | 0 | AICPU_TS doesn't need notify |
| exchangeAllMems | true | Exchange all registered memory |

#### Send Implementation

Send constructs HixlSendParam and launches the AICPU kernel via ACL API:

```cpp
std::vector<Status> Send(ioBatches, kernelCount, quietCount) {
    for each batch:
        ctx = GetLinkContext(batch.connectionHandle)
        
        // Construct HixlSendParam
        args.thread = ctx->thread
        args.channel = ctx->channel
        args.local_src = batch.sendBuffer
        args.len = batch.len
        
        // Launch kernel via ACL
        aclrtKernelArgsInit(kernelFunc_, &argsHandle)
        aclrtKernelArgsAppend(argsHandle, &args, sizeof(args), &paramHandle)
        aclrtKernelArgsFinalize(argsHandle)
        aclrtLaunchKernelWithConfig(kernelFunc_, 1, ctx->stream, &cfg, argsHandle, nullptr)
        aclrtSynchronizeStream(ctx->stream)
        
        if batch.flagBuffer:
            *flagBuffer = 1  // Mark task complete
        results.push_back(OK)
    return results
}
```

#### Memory Export/Import (Demo Only)

These interfaces are only used by the demo program for cross-process memory sharing, not used by asu_transport core logic:

```cpp
// Export: Call HcommMemExport to get descriptor
Status ExportMemory(handle, memHandle, &exportDesc, &exportLen) {
    endpoint = endpoint_
    HcommMemExport(endpoint, memHandle, &exportDesc, &exportLen)
    return OK
}

// Import: Call HcommMemImport to import remote memory
Status ImportMemory(handle, importDesc, importLen, &importedHandle) {
    endpoint = endpoint_
    CommMem outMem
    HcommMemImport(endpoint, importDesc, importLen, &outMem)
    importedMemMap_[outMem.addr] = { addr, size, memDesc }
    *importedHandle = outMem.addr
    return OK
}

// Get imported memory info
Status GetImportedMemoryInfo(handle, importedHandle, &addr, &size) {
    info = importedMemMap_[importedHandle]
    *addr = info.addr
    *size = info.size
    return OK
}
```

### 3.3 Smart Pointer Lifecycle

**ConnectionChannel**:
- Holders: `ConnectionGroup::channels`, `channelCache_`, `drainList_`, `PendingRequest::channel`
- Lifecycle: Automatically destructed when all references are released

**LinkContext**:
- Lifecycle: Freed in `DeleteConnections` via `delete ctx`

**Endpoint**:
- Holder: `AICPUTransProvider::endpoint_` (single value, shared by all connections)
- Lifecycle: `HcommEndpointDestroy` when `endpointRefCount_ == 0`

## 4. Configuration Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| queryQpNum | 1 | Number of QPs for Query operations |
| loadQpNum | 2 | Number of QPs for Load operations |
| storeQpNum | 1 | Number of QPs for Store operations |
| maxInflightTasks | 64 | Maximum concurrent tasks |
| queryTimeoutMs | 5000 | Query timeout |
| loadTimeoutMs | 5000 | Load timeout |
| storeTimeoutMs | 5000 | Store timeout |
| kFailureThreshold | 2 | Failure threshold to trigger drain |
| kRecoverIntervalMs | 100 | RecoverLoop check interval |
| kMaxInflightPerChannel | 256 | Maximum inflight count per channel |

## 5. Build Instructions

> **CANN Version Requirement**: 9.1.0-beta.1

### 5.1 Dependency Installation

```bash
# Install system dependencies
dnf install -y fmt-devel spdlog-devel zlib-devel
```

Header files are included in the `trans/include/` directory:
- `acl/` — CANN ACL headers
- `hcomm/` — HCOMM communication library headers (includes `securec.h` and `securectype.h`, required because `EndpointDescInit` and `HcommChannelDescInit` in `hcomm_res_defs.h` use `memset_s`)
- `hixl_kernel/` — HixlSend kernel headers

Dynamic library dependencies (requires CANN package installation):
- `libhcomm.so` — HCOMM communication library
- `libascendcl.so` — CANN ACL runtime library

### 5.2 HIXL and HCOMM Build and Installation

> **Note**: If HIXL and HCOMM source code has not been modified, no need to rebuild and reinstall. You can skip this section.

ASU depends on HIXL and HCOMM dynamic libraries, which need to be built from source and installed to the CANN directory.

#### 5.2.1 Get Source Code

```bash
# Clone HIXL
git clone -b xxx https://gitcode.com/xxx/hixl.git /path/to/hixl

# Clone HCOMM
git clone -b xxx https://gitcode.com/xxx/hcomm.git /path/to/hcomm
```

#### 5.2.2 Build and Install HIXL

```bash
pushd /path/to/hixl
rm -rf ./build ./build_out
git pull
git checkout xxx
bash build.sh --pkg
yes | bash build_out/cann-hixl_9.1.0-beta.1_linux-aarch64.run --full --install-path=/usr/local/Ascend/cann-9.1.0-beta.1
bash build.sh --examples
popd
```

#### 5.2.3 Build and Install HCOMM

```bash
pushd /path/to/hcomm
rm -rf ./build ./build_out
git pull
git checkout xxx
bash build.sh --pkg
yes | bash build_out/cann-hcomm_9.1.0-beta.1_linux-aarch64.run --full --install-path=/usr/local/Ascend/cann-9.1.0-beta.1
popd
```

#### 5.2.4 Verify Installation

```bash
# Check HCOMM dynamic library
ls -l /usr/local/Ascend/cann-9.1.0-beta.1/aarch64-linux/lib64/libhcomm.so

# Check HIXL dynamic library
ls -l /usr/local/Ascend/cann-9.1.0-beta.1/aarch64-linux/lib64/libcann_hixl.so

# Check if dynamic libraries can be found by linker
ldconfig -p | grep -E "libhcomm|libcann_hixl"
```

### 5.3 ASU Build Commands

**Method 1: Using CANN Environment Variables (Recommended)**

```bash
# Source CANN environment
source /usr/local/Ascend/cann/set_env.sh

# Create build directory
mkdir -p build_asu && cd build_asu

# Configure CMake (automatically detects CANN library path from environment variables)
cmake \
  -DUCM_ROOT_DIR=/path/to/unified-cache-management \
  /path/to/unified-cache-management/ucm/transport/kv/asu

# Build
make asu_transport aicpu_send_with_provider -j$(nproc)
```

**Method 2: Explicitly Specify CANN Library Path**

```bash
# Create build directory
mkdir -p build_asu && cd build_asu

# Configure CMake
cmake \
  -DUCM_ROOT_DIR=/path/to/unified-cache-management \
  -DCANN_LIB_DIR=/usr/local/Ascend/cann-9.1.0-beta.1/aarch64-linux/lib64 \
  /path/to/unified-cache-management/ucm/transport/kv/asu

# Build
make asu_transport aicpu_send_with_provider -j$(nproc)
```

**Notes:**
- CMakeLists.txt automatically detects environment variables `ASCEND_HOME_PATH` or `ASCEND_HOME` (set by `set_env.sh`)
- If environment variables exist, automatically appends `/lib64` path
- If environment variables don't exist and `CANN_LIB_DIR` is not specified, an error message will be displayed
- Method 1 is recommended as it follows the standard CANN workflow and doesn't require modifying build commands when upgrading CANN versions

### 5.4 Build Artifacts

| Artifact | Description |
|----------|-------------|
| `libasu_transport.so` | ASU transport layer dynamic library |
| `aicpu_send_with_provider` | Demo executable |

### 5.5 Notes

- `UCM_ROOT_DIR` points to the unified-cache-management project root directory
- CANN library path can be automatically detected from environment variables (`ASCEND_HOME_PATH` or `ASCEND_HOME`) after sourcing `set_env.sh`, or explicitly specified via `CANN_LIB_DIR` parameter
- Logger module source code is automatically compiled into `libasu_transport.so`, no separate compilation needed
- `asu_client` will fail to compile due to missing `kv_common` dependency, can be ignored
- HIXL compilation is required to generate kernel configuration files and dynamic libraries
- Ensure CANN 9.1.0-beta.1 is properly installed before compiling HCOMM

## 6. Demo Program: aicpu_send_with_provider

### 6.1 Overview

`aicpu_send_with_provider` is an end-to-end test program demonstrating how to use `AICPUTransProvider` for AICPU data transfer between two processes.

Main workflow:
1. Create AICPUTransProvider and initialize
2. Create connection and register mailbox memory
3. Export memory descriptor and exchange via file
4. Import peer memory, establish complete connection
5. Rank 0 launches HixlSend kernel to send data
6. Rank 1 launches HixlRecv kernel to receive and verify data

### 6.2 Build

The demo program is located at `test/demo/aicpu_send_with_provider.cc` and is built together with the `asu_transport` library:

```bash
cd /path/to/build_asu

# Build demo (automatically builds dependent asu_transport)
make aicpu_send_with_provider -j$(nproc)
```

After successful build, the executable is generated at: `/path/to/build_asu/aicpu_send_with_provider`

### 6.3 Run

Two processes need to be started: sender (Rank 0) and receiver (Rank 1).

**Rank 0 (Sender):**

```bash
export ASCEND_GLOBAL_LOG_LEVEL=1
export ASCEND_SLOG_PRINT_TO_STDOUT=0

./aicpu_send_with_provider \
    --rank=0 \
    --logic-dev=0 \
    --phy-dev=2 \
    --ip=192.168.100.1 \
    --bytes=4096 \
    --local-file=/tmp/r0.bin \
    --peer-file=/tmp/r1.bin \
    --done-file=/tmp/hixl.done \
    --kernel-json=/path/to/libcann_hixl_kernel.json \
    --message="Hello from Rank 0"
```

**Rank 1 (Receiver):**

```bash
export ASCEND_GLOBAL_LOG_LEVEL=1
export ASCEND_SLOG_PRINT_TO_STDOUT=0

./aicpu_send_with_provider \
    --rank=1 \
    --logic-dev=1 \
    --phy-dev=3 \
    --ip=192.168.100.2 \
    --bytes=4096 \
    --local-file=/tmp/r1.bin \
    --peer-file=/tmp/r0.bin \
    --done-file=/tmp/hixl.done \
    --kernel-json=/path/to/libcann_hixl_kernel.json \
    --message="Hello from Rank 1"
```

### 6.4 Parameter Description

| Parameter | Description | Example |
|-----------|-------------|---------|
| `--rank` | Process role (0=sender, 1=receiver) | `--rank=0` |
| `--logic-dev` | ACL logical device ID (starts from 0 in container) | `--logic-dev=0` |
| `--phy-dev` | NPU physical device ID (shown by npu-smi) | `--phy-dev=2` |
| `--ip` | Local RoCE NIC IP address | `--ip=192.168.100.1` |
| `--bytes` | Mailbox buffer size (bytes) | `--bytes=4096` |
| `--local-file` | Local descriptor file path | `--local-file=/tmp/r0.bin` |
| `--peer-file` | Peer descriptor file path | `--peer-file=/tmp/r1.bin` |
| `--done-file` | Completion flag file path | `--done-file=/tmp/hixl.done` |
| `--kernel-json` | HixlSend kernel configuration file path | `--kernel-json=.../libcann_hixl_kernel.json` |
| `--message` | Message content to send | `--message="Hello"` |

### 6.5 Notes

1. **Startup Order**: Start Rank 1 first, then Rank 0. Rank 0 will wait for Rank 1's descriptor file to be ready.

2. **Device ID Mapping**:
   - `logic-dev`: ACL logical ID in container, numbered from 0
   - `phy-dev`: Physical NPU ID, view via `npu-smi info`

3. **Network Configuration**:
   - Both ranks' `--ip` must be RoCE NIC IPs
   - Query NPU's RoCE IP via `hccn_tool -i <phy-dev> -ip -g`
   - Ensure both IPs are in the same subnet and network is connected

4. **File Exchange**:
   - `--local-file` and `--peer-file` need cross-configuration
   - Rank 0's `local-file` is Rank 1's `peer-file`, and vice versa

5. **Kernel JSON**:
   - Need to compile hixl project first to generate kernel configuration file
   - Typically located at `hixl/build/device_build/src/ops/hixl_kernel/libcann_hixl_kernel.json`

6. **Runtime Environment**:
   - Need to source CANN environment variables: `source /usr/local/Ascend/cann/set_env.sh`
   - Ensure `LD_LIBRARY_PATH` includes the directory containing `libasu_transport.so`
