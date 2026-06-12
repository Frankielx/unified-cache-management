/*
 * Multi-Channel Demo: 2 channels, 2 batches per channel, 4 consecutive messages
 *
 * This demo demonstrates:
 * - Creating 2 connections (channels)
 * - Allocating 2 threads (one per channel)
 * - Sending 4 messages consecutively (2 per channel)
 * - Each message uses a different mailbox buffer
 *
 * Message layout:
 *   Channel 0: Message-A ("Hello-A"), Message-B ("Hello-B")
 *   Channel 1: Message-C ("Hello-C"), Message-D ("Hello-D")
 *
 * Run two processes:
 *   Rank 0: ./aicpu_multi_channel_demo --rank=0 --logic-dev=0 --phy-dev=0 --ip=192.168.190.170 \
 *             --local-file=/tmp/r0.bin --peer-file=/tmp/r1.bin --done-file=/tmp/hixl.done \
 *             --kernel-json=./libcann_hixl_kernel.json
 *
 *   Rank 1: ./aicpu_multi_channel_demo --rank=1 --logic-dev=2 --phy-dev=2 --ip=192.168.190.172 \
 *             --local-file=/tmp/r1.bin --peer-file=/tmp/r0.bin --done-file=/tmp/hixl.done \
 *             --kernel-json=./libcann_hixl_kernel.json
 */

#include <arpa/inet.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <unistd.h>

#include <chrono>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#include "acl/acl.h"
#include "hixl_kernel/hixl_send.h"
#include "hcomm/hcomm_primitives.h"
#include "hcomm/hcomm_res_defs.h"
#include "aicpu_trans_provider.h"

namespace {

constexpr uint32_t kMagic = 0x48585344;
constexpr uint32_t kVersion = 2;  // Version 2 for multi-mem-desc format
constexpr uint16_t kDefaultPort = 16666;
constexpr uint32_t kNumChannels = 2;
constexpr uint32_t kNumBatchesPerChannel = 2;
constexpr uint32_t kNumMessages = kNumChannels * kNumBatchesPerChannel;

const char* kMessages[kNumMessages] = {
    "Hello-A", "Hello-B", "Hello-C", "Hello-D"
};

struct PeerFileHeader {
    uint32_t magic;
    uint32_t version;
    uint32_t numDescs;
    EndpointDesc endpoint;
};

struct Options {
    int rank = -1;
    int logicDev = -1;
    uint32_t phyDev = 0;
    uint64_t bytes = 4096;
    std::string ip;
    std::string localFile;
    std::string peerFile;
    std::string doneFile;
    std::string kernelJson;
    std::string ipMap;
};

void Log(const Options &opt, const char *fmt, ...)
{
    printf("[rank %d] ", opt.rank);
    va_list ap;
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    printf("\n");
    fflush(stdout);
}

int Fail(const char *what, int ret, int line)
{
    fprintf(stderr, "FAIL line %d: %s ret=%d\n", line, what, ret);
    return ret == 0 ? 1 : ret;
}

void PrintBytes(const Options &opt, const char *label, const uint8_t *data, size_t len)
{
    const size_t shown = len < 64U ? len : 64U;
    printf("[rank %d] %s first %zu/%zu bytes:", opt.rank, label, shown, len);
    for (size_t i = 0; i < shown; ++i) {
        printf(" %02x", static_cast<unsigned int>(data[i]));
    }
    printf("\n");
    fflush(stdout);
}

#define CHECK_RET(expr) do { int _ret = static_cast<int>(expr); if (_ret != 0) return Fail(#expr, _ret, __LINE__); } while (0)
#define CHECK_ACL(expr) do { aclError _ret = (expr); if (_ret != ACL_SUCCESS) return Fail(#expr, static_cast<int>(_ret), __LINE__); } while (0)
#define CHECK_STATUS(expr) do { auto _s = (expr); if (!_s.ok()) { fprintf(stderr, "FAIL: %s: %s\n", #expr, _s.message.c_str()); return 1; } } while (0)
#define CHECK_VEC_STATUS(expr) do { auto _vs = (expr); for (const auto& _s : _vs) { if (!_s.ok()) { fprintf(stderr, "FAIL: %s: %s\n", #expr, _s.message.c_str()); return 1; } } } while (0)

bool ParseIntArg(const char *arg, const char *name, int *out)
{
    const size_t n = strlen(name);
    if (strncmp(arg, name, n) != 0 || arg[n] != '=') {
        return false;
    }
    *out = atoi(arg + n + 1);
    return true;
}

bool ParseU64Arg(const char *arg, const char *name, uint64_t *out)
{
    const size_t n = strlen(name);
    if (strncmp(arg, name, n) != 0 || arg[n] != '=') {
        return false;
    }
    *out = strtoull(arg + n + 1, nullptr, 0);
    return true;
}

bool ParseStrArg(const char *arg, const char *name, std::string *out)
{
    const size_t n = strlen(name);
    if (strncmp(arg, name, n) != 0 || arg[n] != '=') {
        return false;
    }
    *out = arg + n + 1;
    return true;
}

void PrintUsage(const char *prog)
{
    fprintf(stderr,
        "usage: %s --rank=0|1 --logic-dev=N --phy-dev=N --ip=A.B.C.D --bytes=N \\\n"
        "          --local-file=/tmp/r0.bin --peer-file=/tmp/r1.bin \\\n"
        "          --done-file=/tmp/hixl.done --kernel-json=/path/libcann_hixl_kernel.json \\\n"
        "          [--ip-map=/tmp/npu_ip_map.txt]\n",
        prog);
}

bool ParseOptions(int argc, char **argv, Options *opt)
{
    for (int i = 1; i < argc; ++i) {
        int tmp = 0;
        uint64_t u64 = 0;
        if (ParseIntArg(argv[i], "--rank", &tmp)) {
            opt->rank = tmp;
        } else if (ParseIntArg(argv[i], "--logic-dev", &tmp)) {
            opt->logicDev = tmp;
        } else if (ParseIntArg(argv[i], "--phy-dev", &tmp)) {
            opt->phyDev = static_cast<uint32_t>(tmp);
        } else if (ParseU64Arg(argv[i], "--bytes", &u64)) {
            opt->bytes = u64;
        } else if (ParseStrArg(argv[i], "--ip", &opt->ip)) {
        } else if (ParseStrArg(argv[i], "--local-file", &opt->localFile)) {
        } else if (ParseStrArg(argv[i], "--peer-file", &opt->peerFile)) {
        } else if (ParseStrArg(argv[i], "--done-file", &opt->doneFile)) {
        } else if (ParseStrArg(argv[i], "--kernel-json", &opt->kernelJson)) {
        } else if (ParseStrArg(argv[i], "--ip-map", &opt->ipMap)) {
        } else {
            return false;
        }
    }

    return (opt->rank == 0 || opt->rank == 1) && opt->logicDev >= 0 && 
           !opt->ip.empty() &&
           !opt->localFile.empty() && !opt->peerFile.empty() && 
           !opt->doneFile.empty() && !opt->kernelJson.empty();
}

// Write peer file with multiple memory descriptors
int WritePeerFile(const std::string &path, const EndpointDesc &ep,
                  const std::vector<std::vector<char>>& descs)
{
    PeerFileHeader hdr{};
    hdr.magic = kMagic;
    hdr.version = kVersion;
    hdr.numDescs = descs.size();
    hdr.endpoint = ep;

    std::ofstream os(path, std::ios::binary | std::ios::trunc);
    if (!os) {
        perror(path.c_str());
        return 1;
    }
    os.write(reinterpret_cast<const char *>(&hdr), sizeof(hdr));
    
    // Write each descriptor: [descLen(4 bytes)][desc data]
    for (const auto& desc : descs) {
        uint32_t descLen = desc.size();
        os.write(reinterpret_cast<const char*>(&descLen), sizeof(descLen));
        os.write(desc.data(), descLen);
    }
    return os.good() ? 0 : 1;
}

// Read peer file with multiple memory descriptors
int ReadPeerFile(const std::string &path, EndpointDesc *ep, std::vector<std::vector<char>> *descs)
{
    std::ifstream is(path, std::ios::binary);
    if (!is) {
        return 1;
    }
    PeerFileHeader hdr{};
    is.read(reinterpret_cast<char *>(&hdr), sizeof(hdr));
    if (!is.good() || hdr.magic != kMagic || hdr.version != kVersion || hdr.numDescs == 0) {
        return 1;
    }
    
    descs->resize(hdr.numDescs);
    for (uint32_t i = 0; i < hdr.numDescs; ++i) {
        uint32_t descLen = 0;
        is.read(reinterpret_cast<char*>(&descLen), sizeof(descLen));
        if (!is.good() || descLen == 0) {
            return 1;
        }
        (*descs)[i].resize(descLen);
        is.read((*descs)[i].data(), descLen);
        if (!is.good()) {
            return 1;
        }
    }
    
    *ep = hdr.endpoint;
    return 0;
}

int WaitForPeerFile(const Options &opt, const std::string &path,
                    EndpointDesc *ep, std::vector<std::vector<char>> *descs)
{
    uint32_t attempts = 0;
    for (;;) {
        if (ReadPeerFile(path, ep, descs) == 0) {
            return 0;
        }
        ++attempts;
        if (attempts % 50 == 0) {
            Log(opt, "still waiting for peer descriptor after %u ms", attempts * 100);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

int LoadKernel(const std::string &json, const char *funcName, aclrtBinHandle *bin, aclrtFuncHandle *func)
{
    aclrtBinaryLoadOptions loadOptions{};
    aclrtBinaryLoadOption option{};
    option.type = ACL_RT_BINARY_LOAD_OPT_CPU_KERNEL_MODE;
    option.value.cpuKernelMode = 0;
    loadOptions.numOpt = 1;
    loadOptions.options = &option;
    CHECK_ACL(aclrtBinaryLoadFromFile(json.c_str(), &loadOptions, bin));
    CHECK_ACL(aclrtBinaryGetFunction(*bin, funcName, func));
    return 0;
}

int LaunchKernel(aclrtFuncHandle func, aclrtStream stream, void *args, size_t argsSize)
{
    aclrtArgsHandle argsHandle = nullptr;
    aclrtParamHandle paramHandle = nullptr;
    CHECK_ACL(aclrtKernelArgsInit(func, &argsHandle));
    CHECK_ACL(aclrtKernelArgsAppend(argsHandle, args, argsSize, &paramHandle));
    CHECK_ACL(aclrtKernelArgsFinalize(argsHandle));

    aclrtLaunchKernelCfg cfg{};
    aclrtLaunchKernelAttr attr{};
    attr.id = ACL_RT_LAUNCH_KERNEL_ATTR_TIMEOUT;
    attr.value.timeout = 120;
    cfg.numAttrs = 1;
    cfg.attrs = &attr;
    CHECK_ACL(aclrtLaunchKernelWithConfig(func, 1, stream, &cfg, argsHandle, nullptr));
    CHECK_ACL(aclrtSynchronizeStream(stream));
    return 0;
}

int TouchDoneFile(const std::string &path)
{
    std::ofstream os(path, std::ios::trunc);
    os << "done\n";
    return os.good() ? 0 : 1;
}

} // namespace

int main(int argc, char **argv)
{
    Options opt;
    if (!ParseOptions(argc, argv, &opt)) {
        PrintUsage(argv[0]);
        return 1;
    }

    const uint64_t mailboxBytes = sizeof(HcommSendRecvMailboxHeader) + opt.bytes;

    Log(opt, "starting: logicDev=%d phyDev=%u ip=%s bytes=%llu mailboxBytes=%llu",
        opt.logicDev, opt.phyDev, opt.ip.c_str(),
        static_cast<unsigned long long>(opt.bytes),
        static_cast<unsigned long long>(mailboxBytes));
    Log(opt, "config: %u channels, %u batches/channel, %u total messages",
        kNumChannels, kNumBatchesPerChannel, kNumMessages);
    for (uint32_t i = 0; i < kNumMessages; ++i) {
        Log(opt, "  message[%u]: \"%s\"", i, kMessages[i]);
    }
    Log(opt, "files: local=%s peer=%s done=%s kernelJson=%s ipMap=%s",
        opt.localFile.c_str(), opt.peerFile.c_str(), opt.doneFile.c_str(), opt.kernelJson.c_str(),
        opt.ipMap.c_str());

    CHECK_ACL(aclInit(nullptr));
    CHECK_ACL(aclrtSetDevice(opt.logicDev));

    UC::ASU::AICPUTransProvider provider(opt.kernelJson, opt.ipMap);

    // Step 1: InitEndpoint
    Log(opt, "init endpoint: localIp=%s phyDev=%u", opt.ip.c_str(), opt.phyDev);
    CHECK_STATUS(provider.InitEndpoint(opt.ip));

    const EndpointDesc& localEp = provider.GetLocalEndpointDesc();

    // Step 2: Allocate kNumMessages mailbox buffers
    void *mailboxBufs[kNumMessages] = {};
    std::vector<uint8_t> hostData[kNumMessages];
    
    for (uint32_t i = 0; i < kNumMessages; ++i) {
        Log(opt, "allocating mailbox buffer[%u]", i);
        CHECK_ACL(aclrtMalloc(&mailboxBufs[i], mailboxBytes, ACL_MEM_MALLOC_HUGE_ONLY));
        CHECK_ACL(aclrtMemset(mailboxBufs[i], mailboxBytes, 0, mailboxBytes));
        
        hostData[i].assign(kMessages[i], kMessages[i] + strlen(kMessages[i]));
        
        if (opt.rank == 0) {
            auto *payload = static_cast<uint8_t *>(mailboxBufs[i]) + sizeof(HcommSendRecvMailboxHeader);
            Log(opt, "copying message[%u] to mailbox payload", i);
            CHECK_ACL(aclrtMemcpy(payload, hostData[i].size(), hostData[i].data(), 
                                  hostData[i].size(), ACL_MEMCPY_HOST_TO_DEVICE));
        } else {
            Log(opt, "receiver mailbox[%u] payload left zeroed", i);
        }
    }

    // Step 3: Register all mailbox memories
    Log(opt, "registering %u mailbox memories", kNumMessages);
    std::vector<UC::ASU::TransProvider::RegisterMemoryDesc> memDescs;
    for (uint32_t i = 0; i < kNumMessages; ++i) {
        memDescs.push_back({UC::ASU::TransProvider::MemType::MEM_DEVICE, 
                           reinterpret_cast<uintptr_t>(mailboxBufs[i]), mailboxBytes});
    }
    std::vector<UC::ASU::TransProvider::MemHandle> memHandles;
    CHECK_STATUS(provider.RegisterMemory(nullptr, memDescs, memHandles));
    if (memHandles.size() != kNumMessages) {
        fprintf(stderr, "Failed to register all memories\n");
        return 1;
    }
    for (uint32_t i = 0; i < kNumMessages; ++i) {
        Log(opt, "memory[%u] registered: handle=%p", i, memHandles[i]);
    }

    // Step 4: Export all memory descriptors
    Log(opt, "exporting %u mailbox descriptors", kNumMessages);
    std::vector<std::vector<char>> exportDescs(kNumMessages);
    for (uint32_t i = 0; i < kNumMessages; ++i) {
        void *exportDesc = nullptr;
        uint32_t exportLen = 0;
        CHECK_STATUS(provider.ExportMemory(nullptr, memHandles[i], &exportDesc, &exportLen));
        exportDescs[i].assign(static_cast<char*>(exportDesc), 
                              static_cast<char*>(exportDesc) + exportLen);
    }

    // Step 5: Write peer file
    CHECK_RET(WritePeerFile(opt.localFile, localEp, exportDescs));
    Log(opt, "wrote peer file: path=%s numDescs=%u", opt.localFile.c_str(), kNumMessages);

    // Step 6: Read peer file
    EndpointDesc peerEp;
    std::vector<std::vector<char>> peerDescs;
    Log(opt, "waiting for peer file: path=%s", opt.peerFile.c_str());
    CHECK_RET(WaitForPeerFile(opt, opt.peerFile, &peerEp, &peerDescs));
    
    if (peerDescs.size() != kNumMessages) {
        fprintf(stderr, "Peer file has wrong number of descriptors: %zu\n", peerDescs.size());
        return 1;
    }

    char peerIpBuf[64] = {};
    inet_ntop(AF_INET, &peerEp.commAddr.addr, peerIpBuf, sizeof(peerIpBuf));
    Log(opt, "loaded peer file: peerIp=%s peerPhyDev=%u numDescs=%zu",
        peerIpBuf, peerEp.loc.device.devPhyId, peerDescs.size());

    // Step 7: Import all peer memories
    Log(opt, "importing %u peer mailbox memories", kNumMessages);
    UC::ASU::TransProvider::MemHandle peerMemHandles[kNumMessages] = {};
    uint64_t peerMemAddrs[kNumMessages] = {};
    uint64_t peerMemSizes[kNumMessages] = {};
    
    for (uint32_t i = 0; i < kNumMessages; ++i) {
        CHECK_STATUS(provider.ImportMemory(nullptr, peerDescs[i].data(), 
                                           peerDescs[i].size(), &peerMemHandles[i]));
        CHECK_STATUS(provider.GetImportedMemoryInfo(nullptr, peerMemHandles[i], 
                                                     &peerMemAddrs[i], &peerMemSizes[i]));
        Log(opt, "peer memory[%u] imported: handle=%p addr=0x%llx size=%llu",
            i, peerMemHandles[i],
            static_cast<unsigned long long>(peerMemAddrs[i]),
            static_cast<unsigned long long>(peerMemSizes[i]));
    }

    // Step 8: Create kNumChannels connections, each with 1 QP
    std::vector<UC::ASU::TransProvider::ConnectionHandle> connections;
    for (uint32_t i = 0; i < kNumChannels; ++i) {
        Log(opt, "creating connection[%u]: local=%s peer=%s port=%u",
            i, opt.ip.c_str(), peerIpBuf, kDefaultPort);
        std::vector<UC::ASU::TransProvider::ConnectionHandle> batch;
        CHECK_STATUS(provider.CreateConnection(opt.ip, peerIpBuf, kDefaultPort, 1, 5000, batch));
        if (batch.size() != 1) {
            fprintf(stderr, "Failed to create connection[%u]\n", i);
            return 1;
        }
        connections.push_back(batch[0]);
        Log(opt, "QP[%u] created: handle=%p channel=%llu", i, batch[0],
            static_cast<unsigned long long>(provider.GetChannelHandle(batch[0])));
    }

    // Step 9: Allocate kNumChannels threads
    Log(opt, "allocating %u AICPU TS threads", kNumChannels);
    std::vector<uint32_t> notifyNumPerThread(kNumChannels, 0);
    std::vector<UC::ASU::TransProvider::ThreadHandle> threads;
    CHECK_STATUS(provider.AllocThread(kNumChannels, notifyNumPerThread, threads));
    if (threads.size() != kNumChannels) {
        fprintf(stderr, "Failed to allocate %u threads\n", kNumChannels);
        return 1;
    }
    for (uint32_t i = 0; i < kNumChannels; ++i) {
        Log(opt, "thread[%u] allocated: handle=%p", i, threads[i]);
    }

    // Step 10: Send/Receive kNumMessages consecutively through single channel
    if (opt.rank == 0) {
        // Sender: Send 4 messages consecutively through single channel
        Log(opt, "=== SENDER: Sending %u messages consecutively through %u channel ===", 
            kNumMessages, kNumChannels);
        
        for (uint32_t msgIdx = 0; msgIdx < kNumMessages; ++msgIdx) {
            uint32_t channelIdx = msgIdx / kNumBatchesPerChannel;
            
            UC::ASU::TransProvider::SendIoBatch batch{};
            batch.connectionHandle = connections[channelIdx];
            batch.sendBuffer = mailboxBufs[msgIdx];
            batch.len = hostData[msgIdx].size();
            batch.remoteMailboxAddr = peerMemAddrs[msgIdx];
            batch.remoteMailboxSize = peerMemSizes[msgIdx];

            Log(opt, "sending message[%u] via channel[%u]: \"%s\" len=%zu remoteAddr=0x%llx",
                msgIdx, channelIdx, kMessages[msgIdx], hostData[msgIdx].size(),
                static_cast<unsigned long long>(peerMemAddrs[msgIdx]));

            auto sendResults = provider.Send({batch}, 0, 0);
            if (sendResults.empty() || !sendResults[0].ok()) {
                fprintf(stderr, "provider.Send[%u] failed: %s\n", msgIdx,
                        sendResults.empty() ? "no result" : sendResults[0].message.c_str());
                return 1;
            }

            PrintBytes(opt, "sent payload", hostData[msgIdx].data(), hostData[msgIdx].size());
            Log(opt, "message[%u] sent successfully", msgIdx);
        }
        
        Log(opt, "all %u messages sent, writing done file", kNumMessages);
        CHECK_RET(TouchDoneFile(opt.doneFile));
        
    } else {
        // Receiver: Receive 4 messages consecutively through single channel
        Log(opt, "=== RECEIVER: Receiving %u messages consecutively through %u channel ===", 
            kNumMessages, kNumChannels);
        
        aclrtStream stream = nullptr;
        CHECK_ACL(aclrtCreateStream(&stream));
        aclrtBinHandle bin = nullptr;
        aclrtFuncHandle func = nullptr;
        CHECK_RET(LoadKernel(opt.kernelJson, "HixlRecv", &bin, &func));

        for (uint32_t msgIdx = 0; msgIdx < kNumMessages; ++msgIdx) {
            uint32_t channelIdx = msgIdx / kNumBatchesPerChannel;
            
            // Build sendRecvContext for this message
            HcommSendRecvChannelContext sendRecvContext{};
            sendRecvContext.magic = HCOMM_SEND_RECV_CHANNEL_CONTEXT_MAGIC;
            sendRecvContext.version = HCOMM_SEND_RECV_CHANNEL_CONTEXT_VERSION;
            sendRecvContext.transportChannel = provider.GetChannelHandle(connections[channelIdx]);
            sendRecvContext.localMailboxAddr = reinterpret_cast<uint64_t>(mailboxBufs[msgIdx]);
            sendRecvContext.localMailboxSize = mailboxBytes;
            sendRecvContext.remoteMailboxAddr = peerMemAddrs[msgIdx];
            sendRecvContext.remoteMailboxSize = peerMemSizes[msgIdx];

            void *sendRecvContextBuf = nullptr;
            CHECK_ACL(aclrtMalloc(&sendRecvContextBuf, sizeof(sendRecvContext), ACL_MEM_MALLOC_HUGE_ONLY));
            CHECK_ACL(aclrtMemcpy(sendRecvContextBuf, sizeof(sendRecvContext), &sendRecvContext, 
                                  sizeof(sendRecvContext), ACL_MEMCPY_HOST_TO_DEVICE));

            void *recvBuf = nullptr;
            void *receivedLenBuf = nullptr;
            CHECK_ACL(aclrtMalloc(&recvBuf, opt.bytes, ACL_MEM_MALLOC_HUGE_ONLY));
            CHECK_ACL(aclrtMemset(recvBuf, opt.bytes, 0, opt.bytes));
            CHECK_ACL(aclrtMalloc(&receivedLenBuf, sizeof(uint64_t), ACL_MEM_MALLOC_HUGE_ONLY));
            CHECK_ACL(aclrtMemset(receivedLenBuf, sizeof(uint64_t), 0, sizeof(uint64_t)));

            HixlRecvParam args{};
            args.thread = reinterpret_cast<uint64_t>(threads[channelIdx]);
            args.channel = reinterpret_cast<uint64_t>(sendRecvContextBuf);
            args.local_dst = recvBuf;
            args.dst_capacity = opt.bytes;
            args.received_len = static_cast<uint64_t *>(receivedLenBuf);
            args.timeout_ms = 120000;
            
            Log(opt, "receiving message[%u] via channel[%u]: capacity=%llu timeoutMs=%u",
                msgIdx, channelIdx,
                static_cast<unsigned long long>(args.dst_capacity), args.timeout_ms);
            
            const int recvLaunchRet = LaunchKernel(func, stream, &args, sizeof(args));
            Log(opt, "HixlRecv[%u] returned ret=%d", msgIdx, recvLaunchRet);

            if (recvLaunchRet != 0) {
                return Fail("LaunchKernel(HixlRecv)", recvLaunchRet, __LINE__);
            }

            uint64_t receivedLen = 0;
            CHECK_ACL(aclrtMemcpy(&receivedLen, sizeof(receivedLen), receivedLenBuf, 
                                  sizeof(receivedLen), ACL_MEMCPY_DEVICE_TO_HOST));
            
            if (receivedLen != hostData[msgIdx].size()) {
                fprintf(stderr, "verify failed: message[%u] expected receivedLen %zu, got %llu\n",
                    msgIdx, hostData[msgIdx].size(),
                    static_cast<unsigned long long>(receivedLen));
                return 1;
            }

            std::vector<uint8_t> got(receivedLen, 0);
            CHECK_ACL(aclrtMemcpy(got.data(), receivedLen, recvBuf, receivedLen, ACL_MEMCPY_DEVICE_TO_HOST));
            const std::string received(got.begin(), got.end());
            
            if (got != hostData[msgIdx]) {
                fprintf(stderr, "verify failed: message[%u] expected \"%s\", got \"%s\"\n", 
                        msgIdx, kMessages[msgIdx], received.c_str());
                return 1;
            }
            
            Log(opt, "verify ok: message[%u] received \"%s\" (%llu bytes)",
                msgIdx, received.c_str(), static_cast<unsigned long long>(receivedLen));
            
            CHECK_ACL(aclrtFree(sendRecvContextBuf));
            CHECK_ACL(aclrtFree(receivedLenBuf));
            CHECK_ACL(aclrtFree(recvBuf));
        }
        
        CHECK_ACL(aclrtDestroyStream(stream));
    }

    // Cleanup
    Log(opt, "releasing resources");
    CHECK_VEC_STATUS(provider.FreeThread(threads));
    for (uint32_t i = 0; i < kNumMessages; ++i) {
        CHECK_STATUS(provider.UnimportMemory(nullptr, peerMemHandles[i]));
    }
    {
        std::vector<UC::ASU::TransProvider::UnregisterMemoryDesc> unregDescs;
        for (auto& mh : memHandles) {
            unregDescs.push_back({nullptr, mh});
        }
        CHECK_VEC_STATUS(provider.UnregisterMemory(unregDescs));
    }
    CHECK_VEC_STATUS(provider.DeleteConnections(connections));
    for (uint32_t i = 0; i < kNumMessages; ++i) {
        CHECK_ACL(aclrtFree(mailboxBufs[i]));
    }
    CHECK_ACL(aclrtResetDevice(opt.logicDev));
    CHECK_ACL(aclFinalize());
    Log(opt, "done");
    return 0;
}
