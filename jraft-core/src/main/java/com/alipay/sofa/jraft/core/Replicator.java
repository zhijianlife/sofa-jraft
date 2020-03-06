/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alipay.sofa.jraft.core;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.CatchUpClosure;
import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.RaftOutter;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.option.ReplicatorOptions;
import com.alipay.sofa.jraft.rpc.RaftClientService;
import com.alipay.sofa.jraft.rpc.RpcRequests.AppendEntriesRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.AppendEntriesResponse;
import com.alipay.sofa.jraft.rpc.RpcRequests.InstallSnapshotRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.InstallSnapshotResponse;
import com.alipay.sofa.jraft.rpc.RpcRequests.TimeoutNowRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.TimeoutNowResponse;
import com.alipay.sofa.jraft.rpc.RpcResponseClosure;
import com.alipay.sofa.jraft.rpc.RpcResponseClosureAdapter;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.util.ByteBufferCollector;
import com.alipay.sofa.jraft.util.OnlyForTest;
import com.alipay.sofa.jraft.util.Recyclable;
import com.alipay.sofa.jraft.util.RecyclableByteBufferList;
import com.alipay.sofa.jraft.util.RecycleUtil;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.ThreadId;
import com.alipay.sofa.jraft.util.Utils;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.google.protobuf.Message;
import com.google.protobuf.ZeroByteStringHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Replicator for replicating log entry from leader to followers.
 *
 * 用于复制 LogEntry 给其它 follower 节点，多个 Replicator 构造成一个 ReplicatorGroup
 *
 * - nextIndex: 需要发送给目标节点的下一个日志条目的索引值（初始化为 Leader 最后索引值加 1）
 * - matchIndex: 对于每一个节点，已经复制给该节点的日志的最大索引值（初始化为 0，单调递增）
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-04 10:32:02 AM
 */
@ThreadSafe
public class Replicator implements ThreadId.OnError {

    private static final Logger LOG = LoggerFactory.getLogger(Replicator.class);

    private final RaftClientService rpcService;

    // Next sending log index
    // 需要发送给目标节点的下一个日志条目的索引值（初始化为 Leader 最后索引值加 1）
    private volatile long nextIndex;
    /** 记录请求连续响应错误的次数 */
    private int consecutiveErrorTimes = 0;
    private boolean hasSucceeded;
    private long timeoutNowIndex;
    private volatile long lastRpcSendTimestamp;
    private volatile long heartbeatCounter = 0;
    private volatile long appendEntriesCounter = 0;
    private volatile long installSnapshotCounter = 0;
    protected Stat statInfo = new Stat();
    private ScheduledFuture<?> blockTimer;

    // Cached the latest RPC in-flight request.
    private Inflight rpcInFly;
    // Heartbeat RPC future
    private Future<Message> heartbeatInFly;
    // Timeout request RPC future
    private Future<Message> timeoutNowInFly;
    // In-flight RPC requests, FIFO queue
    private final ArrayDeque<Inflight> inflights = new ArrayDeque<>();

    private long waitId = -1L;

    /** replicator id */
    protected ThreadId id;
    private final ReplicatorOptions options;
    private final RaftOptions raftOptions;

    private ScheduledFuture<?> heartbeatTimer;
    private volatile SnapshotReader reader;
    private CatchUpClosure catchUpClosure;
    private final TimerManager timerManager;
    private final NodeMetrics nodeMetrics;
    /** 当前 Replicator 的运行状态 */
    private volatile State state;

    /** 请求序列 */
    private int reqSeq = 0; // Request sequence
    // Response sequence
    private int requiredNextSeq = 0;
    // Replicator state reset version
    /** 当前 replicator 的状态版本 */
    private int version = 0;

    // Pending response queue;
    private final PriorityQueue<RpcResponse> pendingResponses = new PriorityQueue<>(50);

    private int getAndIncrementReqSeq() {
        final int prev = this.reqSeq;
        this.reqSeq++;
        if (this.reqSeq < 0) {
            this.reqSeq = 0;
        }
        return prev;
    }

    private int getAndIncrementRequiredNextSeq() {
        final int prev = this.requiredNextSeq;
        this.requiredNextSeq++;
        if (this.requiredNextSeq < 0) {
            this.requiredNextSeq = 0;
        }
        return prev;
    }

    /**
     * Replicator state
     *
     * @author dennis
     */
    public enum State {
        /** 正在探取 follower 的状态信息 */
        Probe, // probe follower state
        /** 正在给 follower 安装快照 */
        Snapshot, // installing snapshot to follower
        /** 正常数据复制 */
        Replicate, // replicate logs normally
        Destroyed // destroyed
    }

    public Replicator(final ReplicatorOptions replicatorOptions, final RaftOptions raftOptions) {
        super();
        this.options = replicatorOptions;
        this.nodeMetrics = this.options.getNode().getNodeMetrics();
        // 初始化为 leader 最后一条日志的 index 加 1
        this.nextIndex = this.options.getLogManager().getLastLogIndex() + 1;
        this.timerManager = replicatorOptions.getTimerManager();
        this.raftOptions = raftOptions;
        this.rpcService = replicatorOptions.getRaftRpcService();
    }

    /**
     * Internal state
     *
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-04 10:38:45 AM
     */
    enum RunningState {
        IDLE, // idle
        BLOCKING, // blocking state
        APPENDING_ENTRIES, // appending log entries
        INSTALLING_SNAPSHOT // installing snapshot
    }

    /**
     * Statistics structure
     *
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-04 10:38:53 AM
     */
    static class Stat {
        RunningState runningState;
        long firstLogIndex;
        long lastLogIncluded;
        long lastLogIndex;
        long lastTermIncluded;

        @Override
        public String toString() {
            return "<running=" + this.runningState + ", firstLogIndex=" + this.firstLogIndex + ", lastLogIncluded="
                    + this.lastLogIncluded + ", lastLogIndex=" + this.lastLogIndex + ", lastTermIncluded="
                    + this.lastTermIncluded + ">";
        }

    }

    // In-flight request type
    enum RequestType {
        Snapshot, // install snapshot
        AppendEntries // replicate logs
    }

    /**
     * In-flight request.
     *
     * @author dennis
     */
    static class Inflight {
        // In-flight request count
        final int count;
        // Start log index
        final long startIndex;
        // Entries size in bytes
        final int size;
        // RPC future
        final Future<Message> rpcFuture;
        final RequestType requestType;
        // Request sequence.
        final int seq;

        public Inflight(final RequestType requestType, final long startIndex, final int count, final int size,
                        final int seq, final Future<Message> rpcFuture) {
            super();
            this.seq = seq;
            this.requestType = requestType;
            this.count = count;
            this.startIndex = startIndex;
            this.size = size;
            this.rpcFuture = rpcFuture;
        }

        @Override
        public String toString() {
            return "Inflight [count=" + this.count + ", startIndex=" + this.startIndex + ", size=" + this.size
                    + ", rpcFuture=" + this.rpcFuture + ", requestType=" + this.requestType + ", seq=" + this.seq + "]";
        }

        boolean isSendingLogEntries() {
            return this.requestType == RequestType.AppendEntries && this.count > 0;
        }
    }

    /**
     * RPC response for AppendEntries/InstallSnapshot.
     *
     * @author dennis
     */
    static class RpcResponse implements Comparable<RpcResponse> {
        final Status status;
        final Message request;
        final Message response;
        final long rpcSendTime;
        final int seq;
        final RequestType requestType;

        public RpcResponse(final RequestType reqType, final int seq, final Status status, final Message request,
                           final Message response, final long rpcSendTime) {
            super();
            this.requestType = reqType;
            this.seq = seq;
            this.status = status;
            this.request = request;
            this.response = response;
            this.rpcSendTime = rpcSendTime;
        }

        @Override
        public String toString() {
            return "RpcResponse [status=" + this.status + ", request=" + this.request + ", response=" + this.response
                    + ", rpcSendTime=" + this.rpcSendTime + ", seq=" + this.seq + ", requestType=" + this.requestType
                    + "]";
        }

        /**
         * Sort by sequence.
         */
        @Override
        public int compareTo(final RpcResponse o) {
            return Integer.compare(this.seq, o.seq);
        }
    }

    @OnlyForTest
    ArrayDeque<Inflight> getInflights() {
        return this.inflights;
    }

    @OnlyForTest
    State getState() {
        return this.state;
    }

    @OnlyForTest
    void setState(final State state) {
        this.state = state;
    }

    @OnlyForTest
    int getReqSeq() {
        return this.reqSeq;
    }

    @OnlyForTest
    int getRequiredNextSeq() {
        return this.requiredNextSeq;
    }

    @OnlyForTest
    int getVersion() {
        return this.version;
    }

    @OnlyForTest
    public PriorityQueue<RpcResponse> getPendingResponses() {
        return this.pendingResponses;
    }

    @OnlyForTest
    long getWaitId() {
        return this.waitId;
    }

    @OnlyForTest
    ScheduledFuture<?> getBlockTimer() {
        return this.blockTimer;
    }

    @OnlyForTest
    long getTimeoutNowIndex() {
        return this.timeoutNowIndex;
    }

    @OnlyForTest
    ReplicatorOptions getOpts() {
        return this.options;
    }

    @OnlyForTest
    long getRealNextIndex() {
        return this.nextIndex;
    }

    @OnlyForTest
    Future<Message> getRpcInFly() {
        if (this.rpcInFly == null) {
            return null;
        }
        return this.rpcInFly.rpcFuture;
    }

    @OnlyForTest
    Future<Message> getHeartbeatInFly() {
        return this.heartbeatInFly;
    }

    @OnlyForTest
    ScheduledFuture<?> getHeartbeatTimer() {
        return this.heartbeatTimer;
    }

    @OnlyForTest
    void setHasSucceeded() {
        this.hasSucceeded = true;
    }

    @OnlyForTest
    Future<Message> getTimeoutNowInFly() {
        return this.timeoutNowInFly;
    }

    /**
     * Adds a in-flight request
     *
     * 记录一个处于处理中的请求，Leader 维护一个 queue，每发出一批 logEntry 就向 queue 中 添加一个代表这一批 logEntry 的 Inflight，
     * 这样当它知道某一批 logEntry 复制失败之后，就可以依赖 queue 中的 Inflight 把该批次 logEntry 以及后续的所有日志重新复制给 follower。
     * 既保证日志复制能够完成，又保证了复制日志的顺序不变
     *
     * @param reqType type of request
     * @param count count if request
     * @param size size in bytes
     */
    private void addInflight(final RequestType reqType, final long startIndex, final int count, final int size,
                             final int seq, final Future<Message> rpcInfly) {
        this.rpcInFly = new Inflight(reqType, startIndex, count, size, seq, rpcInfly);
        this.inflights.add(this.rpcInFly);
        this.nodeMetrics.recordSize("replicate-inflights-count", this.inflights.size());
    }

    /**
     * Returns the next in-flight sending index, return -1 when can't send more in-flight requests.
     *
     * @return next in-flight sending index
     */
    long getNextSendIndex() {
        // Fast path
        if (this.inflights.isEmpty()) {
            return this.nextIndex;
        }
        // Too many in-flight requests.
        if (this.inflights.size() > this.raftOptions.getMaxReplicatorInflightMsgs()) {
            return -1L;
        }
        // Last request should be a AppendEntries request and has some entries.
        if (this.rpcInFly != null && this.rpcInFly.isSendingLogEntries()) {
            return this.rpcInFly.startIndex + this.rpcInFly.count;
        }
        return -1L;
    }

    private Inflight pollInflight() {
        return this.inflights.poll();
    }

    /**
     * 启动心跳超时处理器
     *
     * @param startMs
     */
    private void startHeartbeatTimer(final long startMs) {
        final long dueTime = startMs + this.options.getDynamicHeartBeatTimeoutMs();
        try {
            this.heartbeatTimer = this.timerManager.schedule(() -> onTimeout(this.id), dueTime - Utils.nowMs(), TimeUnit.MILLISECONDS);
        } catch (final Exception e) {
            LOG.error("Fail to schedule heartbeat timer", e);
            onTimeout(this.id);
        }
    }

    void installSnapshot() {
        // 已经在给目标 follower 节点安装 snapshot，跳过
        if (this.state == State.Snapshot) {
            LOG.warn("Replicator {} is installing snapshot, ignore the new request.", this.options.getPeerId());
            this.id.unlock();
            return;
        }

        boolean doUnlock = true;
        try {
            Requires.requireTrue(this.reader == null,
                    "Replicator %s already has a snapshot reader, current state is %s", this.options.getPeerId(), this.state);

            this.reader = this.options.getSnapshotStorage().open();
            if (this.reader == null) {
                final NodeImpl node = this.options.getNode();
                final RaftException error = new RaftException(EnumOutter.ErrorType.ERROR_TYPE_SNAPSHOT);
                error.setStatus(new Status(RaftError.EIO, "Fail to open snapshot"));
                this.id.unlock();
                doUnlock = false;
                node.onError(error);
                return;
            }
            final String uri = this.reader.generateURIForCopy();
            if (uri == null) {
                final NodeImpl node = this.options.getNode();
                final RaftException error = new RaftException(EnumOutter.ErrorType.ERROR_TYPE_SNAPSHOT);
                error.setStatus(new Status(RaftError.EIO, "Fail to generate uri for snapshot reader"));
                releaseReader();
                this.id.unlock();
                doUnlock = false;
                node.onError(error);
                return;
            }
            final RaftOutter.SnapshotMeta meta = this.reader.load();
            if (meta == null) {
                final String snapshotPath = this.reader.getPath();
                final NodeImpl node = this.options.getNode();
                final RaftException error = new RaftException(EnumOutter.ErrorType.ERROR_TYPE_SNAPSHOT);
                error.setStatus(new Status(RaftError.EIO, "Fail to load meta from %s", snapshotPath));
                releaseReader();
                this.id.unlock();
                doUnlock = false;
                node.onError(error);
                return;
            }
            final InstallSnapshotRequest.Builder rb = InstallSnapshotRequest.newBuilder();
            rb.setTerm(this.options.getTerm());
            rb.setGroupId(this.options.getGroupId());
            rb.setServerId(this.options.getServerId().toString());
            rb.setPeerId(this.options.getPeerId().toString());
            rb.setMeta(meta);
            rb.setUri(uri);

            this.statInfo.runningState = RunningState.INSTALLING_SNAPSHOT;
            this.statInfo.lastLogIncluded = meta.getLastIncludedIndex();
            this.statInfo.lastTermIncluded = meta.getLastIncludedTerm();

            final InstallSnapshotRequest request = rb.build();
            this.state = State.Snapshot;
            // noinspection NonAtomicOperationOnVolatileField
            this.installSnapshotCounter++;
            final long monotonicSendTimeMs = Utils.monotonicMs();
            final int stateVersion = this.version;
            final int seq = getAndIncrementReqSeq();
            final Future<Message> rpcFuture = this.rpcService.installSnapshot(this.options.getPeerId().getEndpoint(),
                    request, new RpcResponseClosureAdapter<InstallSnapshotResponse>() {

                        @Override
                        public void run(final Status status) {
                            onRpcReturned(Replicator.this.id, RequestType.Snapshot, status, request, getResponse(), seq,
                                    stateVersion, monotonicSendTimeMs);
                        }
                    });
            addInflight(RequestType.Snapshot, this.nextIndex, 0, 0, seq, rpcFuture);
        } finally {
            if (doUnlock) {
                this.id.unlock();
            }
        }
    }

    @SuppressWarnings("unused")
    static boolean onInstallSnapshotReturned(final ThreadId id, final Replicator r, final Status status,
                                             final InstallSnapshotRequest request,
                                             final InstallSnapshotResponse response) {
        boolean success = true;
        r.releaseReader();
        // noinspection ConstantConditions
        do {
            final StringBuilder sb = new StringBuilder("Node "). //
                    append(r.options.getGroupId()).append(":").append(r.options.getServerId()). //
                    append(" received InstallSnapshotResponse from ").append(r.options.getPeerId()). //
                    append(" lastIncludedIndex=").append(request.getMeta().getLastIncludedIndex()). //
                    append(" lastIncludedTerm=").append(request.getMeta().getLastIncludedTerm());
            if (!status.isOk()) {
                sb.append(" error:").append(status);
                LOG.info(sb.toString());
                if (++r.consecutiveErrorTimes % 10 == 0) {
                    LOG.warn("Fail to install snapshot at peer={}, error={}", r.options.getPeerId(), status);
                }
                success = false;
                break;
            }
            if (!response.getSuccess()) {
                sb.append(" success=false");
                LOG.info(sb.toString());
                success = false;
                break;
            }
            // success
            r.nextIndex = request.getMeta().getLastIncludedIndex() + 1;
            sb.append(" success=true");
            LOG.info(sb.toString());
        } while (false);
        // We don't retry installing the snapshot explicitly.
        // id is unlock in sendEntries
        if (!success) {
            //should reset states
            r.resetInflights();
            r.state = State.Probe;
            r.block(Utils.nowMs(), status.getCode());
            return false;
        }
        r.hasSucceeded = true;
        r.notifyOnCaughtUp(RaftError.SUCCESS.getNumber(), false);
        if (r.timeoutNowIndex > 0 && r.timeoutNowIndex < r.nextIndex) {
            r.sendTimeoutNow(false, false);
        }
        // id is unlock in _send_entriesheartbeatCounter
        r.state = State.Replicate;
        return true;
    }

    private void sendEmptyEntries(final boolean isHeartbeat) {
        this.sendEmptyEntries(isHeartbeat, null);
    }

    /**
     * Send probe or heartbeat request
     *
     * 发送探针或心跳请求，其中探针请求的目的是获取 Follower 已经拥有的的日志位置，以便于向 Follower 发送后续的日志
     *
     * @param isHeartbeat if current entries is heartbeat
     * @param heartBeatClosure heartbeat callback
     */
    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    private void sendEmptyEntries(final boolean isHeartbeat, // false 表示是一个探针请求
                                  final RpcResponseClosure<AppendEntriesResponse> heartBeatClosure) {
        final AppendEntriesRequest.Builder rb = AppendEntriesRequest.newBuilder();
        // 给 AppendEntriesRequest 填充一些必要的参数
        if (!fillCommonFields(rb, this.nextIndex - 1, isHeartbeat)) {
            // id is unlock in installSnapshot
            installSnapshot();
            if (isHeartbeat && heartBeatClosure != null) {
                Utils.runClosureInThread(heartBeatClosure, new Status(RaftError.EAGAIN,
                        "Fail to send heartbeat to peer %s", this.options.getPeerId()));
            }
            return;
        }

        try {
            final long monotonicSendTimeMs = Utils.monotonicMs();
            // 空的 AppendEntriesRequest 请求
            final AppendEntriesRequest request = rb.build();

            // 心跳请求
            if (isHeartbeat) {
                // Sending a heartbeat request
                this.heartbeatCounter++;
                RpcResponseClosure<AppendEntriesResponse> heartbeatDone;
                // Prefer passed-in closure.
                if (heartBeatClosure != null) {
                    heartbeatDone = heartBeatClosure;
                } else {
                    heartbeatDone = new RpcResponseClosureAdapter<AppendEntriesResponse>() {

                        @Override
                        public void run(final Status status) {
                            onHeartbeatReturned(Replicator.this.id, status, request, getResponse(), monotonicSendTimeMs);
                        }
                    };
                }

                // 发送心跳请求给目标节点
                this.heartbeatInFly = this.rpcService.appendEntries(
                        // 请求超时时间为选举周期的 1/2
                        this.options.getPeerId().getEndpoint(), request, this.options.getElectionTimeoutMs() / 2, heartbeatDone);
            }
            // 探针请求
            else {
                // Sending a probe request.
                this.statInfo.runningState = RunningState.APPENDING_ENTRIES;
                this.statInfo.firstLogIndex = this.nextIndex;
                this.statInfo.lastLogIndex = this.nextIndex - 1;

                this.appendEntriesCounter++;
                // 标识当前正在执行探针请求
                this.state = State.Probe;
                final int stateVersion = this.version;
                // 请求序列加 1
                final int seq = getAndIncrementReqSeq();
                // 发送 AppendEntries 请求
                final Future<Message> rpcFuture = this.rpcService.appendEntries(
                        this.options.getPeerId().getEndpoint(),
                        request,
                        -1, // 不限超时
                        new RpcResponseClosureAdapter<AppendEntriesResponse>() {

                            @Override
                            public void run(final Status status) {
                                // 处理探针请求响应
                                onRpcReturned(
                                        Replicator.this.id,
                                        RequestType.AppendEntries,
                                        status,
                                        request,
                                        getResponse(),
                                        seq,
                                        stateVersion,
                                        monotonicSendTimeMs);
                            }

                        });

                this.addInflight(RequestType.AppendEntries, this.nextIndex, 0, 0, seq, rpcFuture);
            }
            LOG.debug("Node {} send HeartbeatRequest to {} term {} lastCommittedIndex {}",
                    this.options.getNode().getNodeId(), this.options.getPeerId(), this.options.getTerm(), request.getCommittedIndex());
        } finally {
            this.id.unlock();
        }
    }

    /**
     * 根据当前的 offset 和 nextSendingIndex 计算出当前的偏移量，然后去 LogManager 找到对应的 LogEntry，
     * 再把 LogEntry 里面的属性设置到 emb 中，并把 LogEntry 里面的数据加入到 dateBuffer 中
     *
     * @param nextSendingIndex
     * @param offset
     * @param emb
     * @param dateBuffer
     * @return
     */
    boolean prepareEntry(final long nextSendingIndex, final int offset,
                         final RaftOutter.EntryMeta.Builder emb, final RecyclableByteBufferList dateBuffer) {
        // 容量已超
        if (dateBuffer.getCapacity() >= this.raftOptions.getMaxBodySize()) {
            return false;
        }

        // 计算本次要获取的 LogEntry index
        final long logIndex = nextSendingIndex + offset;
        // 从 LogManager 中获取对应的 LogEntry
        final LogEntry entry = this.options.getLogManager().getEntry(logIndex);
        if (entry == null) {
            return false;
        }
        emb.setTerm(entry.getId().getTerm());
        if (entry.hasChecksum()) {
            emb.setChecksum(entry.getChecksum()); //since 1.2.6
        }
        emb.setType(entry.getType());
        if (entry.getPeers() != null) {
            Requires.requireTrue(!entry.getPeers().isEmpty(), "Empty peers at logIndex=%d", logIndex);
            for (final PeerId peer : entry.getPeers()) {
                emb.addPeers(peer.toString());
            }
            if (entry.getOldPeers() != null) {
                for (final PeerId peer : entry.getOldPeers()) {
                    emb.addOldPeers(peer.toString());
                }
            }
        } else {
            Requires.requireTrue(entry.getType() != EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION,
                    "Empty peers but is ENTRY_TYPE_CONFIGURATION type at logIndex=%d", logIndex);
        }
        final int remaining = entry.getData() != null ? entry.getData().remaining() : 0;
        emb.setDataLen(remaining);
        if (entry.getData() != null) {
            // should slice entry data
            dateBuffer.add(entry.getData().slice());
        }
        return true;
    }

    /**
     * 创建并启动到目标节点的 Replicator
     *
     * @param opts
     * @param raftOptions
     * @return
     */
    public static ThreadId start(final ReplicatorOptions opts, final RaftOptions raftOptions) {
        if (opts.getLogManager() == null || opts.getBallotBox() == null || opts.getNode() == null) {
            throw new IllegalArgumentException("Invalid ReplicatorOptions.");
        }
        // 新建一个 Replicator 实例
        final Replicator replicator = new Replicator(opts, raftOptions);
        // 检查到目标 follower 节点的连通性
        if (!replicator.rpcService.connect(opts.getPeerId().getEndpoint())) {
            LOG.error("Fail to init sending channel to {}", opts.getPeerId());
            // Return and it will be retried later.
            return null;
        }

        // Start replication
        replicator.id = new ThreadId(replicator, replicator);
        replicator.id.lock();
        LOG.info("Replicator={}@{} is started", replicator.id, replicator.options.getPeerId());
        replicator.catchUpClosure = null;
        replicator.lastRpcSendTimestamp = Utils.monotonicMs();
        // 启动心跳超时处理器，这里只是在规定时间超时后执行 ThreadId.setError 方法，并不是启动一个周期性的心跳程序
        replicator.startHeartbeatTimer(Utils.nowMs());
        // id.unlock in sendEmptyEntries
        // 向目标节点发送一个探针请求，其目的是为了获取 Follower 当前的日志位置
        replicator.sendEmptyEntries(false);
        return replicator.id;
    }

    private static String getReplicatorMetricName(final ReplicatorOptions opts) {
        return "replicator-" + opts.getNode().getGroupId() + "/" + opts.getPeerId();
    }

    public static void waitForCaughtUp(final ThreadId id, final long maxMargin, final long dueTime, final CatchUpClosure done) {
        final Replicator r = (Replicator) id.lock();

        if (r == null) {
            Utils.runClosureInThread(done, new Status(RaftError.EINVAL, "No such replicator"));
            return;
        }
        try {
            if (r.catchUpClosure != null) {
                LOG.error("Previous wait_for_caught_up is not over");
                Utils.runClosureInThread(done, new Status(RaftError.EINVAL, "Duplicated call"));
                return;
            }
            done.setMaxMargin(maxMargin);
            if (dueTime > 0) {
                done.setTimer(r.timerManager.schedule(() -> onCatchUpTimedOut(id), dueTime - Utils.nowMs(),
                        TimeUnit.MILLISECONDS));
            }
            r.catchUpClosure = done;
        } finally {
            id.unlock();
        }
    }

    @Override
    public String toString() {
        return "Replicator [state=" + this.state + ", statInfo=" + this.statInfo + ",peerId="
                + this.options.getPeerId() + "]";
    }

    static void onBlockTimeoutInNewThread(final ThreadId id) {
        if (id != null) {
            continueSending(id, RaftError.ETIMEDOUT.getNumber());
        }
    }

    /**
     * Unblock and continue sending right now.
     */
    static void unBlockAndSendNow(final ThreadId id) {
        if (id == null) {
            // It was destroyed already
            return;
        }
        final Replicator r = (Replicator) id.lock();
        if (r == null) {
            return;
        }
        try {
            if (r.blockTimer != null) {
                if (r.blockTimer.cancel(true)) {
                    onBlockTimeout(id);
                }
            }
        } finally {
            id.unlock();
        }
    }

    static boolean continueSending(final ThreadId id, final int errCode) {
        if (id == null) {
            //It was destroyed already
            return true;
        }
        final Replicator r = (Replicator) id.lock();
        if (r == null) {
            return false;
        }
        r.waitId = -1;
        if (errCode == RaftError.ETIMEDOUT.getNumber()) {
            // Send empty entries after block timeout to check the correct
            // _next_index otherwise the replicator is likely waits in            executor.shutdown();
            // _wait_more_entries and no further logs would be replicated even if the
            // last_index of this followers is less than |next_index - 1|
            r.sendEmptyEntries(false);
        } else if (errCode != RaftError.ESTOP.getNumber()) {
            // id is unlock in _send_entries
            r.sendEntries();
        } else {
            LOG.warn("Replicator {} stops sending entries.", id);
            id.unlock();
        }
        return true;
    }

    static void onBlockTimeout(final ThreadId arg) {
        Utils.runInThread(() -> onBlockTimeoutInNewThread(arg));
    }

    void block(final long startTimeMs, @SuppressWarnings("unused") final int errorCode) {
        // TODO: Currently we don't care about error_code which indicates why the
        // very RPC fails. To make it better there should be different timeout for
        // each individual error (e.g. we don't need check every
        // heartbeat_timeout_ms whether a dead follower has come back), but it's just
        // fine now.
        final long dueTime = startTimeMs + this.options.getDynamicHeartBeatTimeoutMs();
        try {
            LOG.debug("Blocking {} for {} ms", this.options.getPeerId(), this.options.getDynamicHeartBeatTimeoutMs());
            this.blockTimer = this.timerManager.schedule(() -> onBlockTimeout(this.id), dueTime - Utils.nowMs(),
                    TimeUnit.MILLISECONDS);
            this.statInfo.runningState = RunningState.BLOCKING;
            this.id.unlock();
        } catch (final Exception e) {
            LOG.error("Fail to add timer", e);
            // id unlock in sendEmptyEntries.
            sendEmptyEntries(false);
        }
    }

    @Override
    public void onError(final ThreadId id, final Object data, final int errorCode) {
        final Replicator r = (Replicator) data;
        if (errorCode == RaftError.ESTOP.getNumber()) {
            try {
                for (final Inflight inflight : r.inflights) {
                    if (inflight != r.rpcInFly) {
                        inflight.rpcFuture.cancel(true);
                    }
                }
                if (r.rpcInFly != null) {
                    r.rpcInFly.rpcFuture.cancel(true);
                    r.rpcInFly = null;
                }
                if (r.heartbeatInFly != null) {
                    r.heartbeatInFly.cancel(true);
                    r.heartbeatInFly = null;
                }
                if (r.timeoutNowInFly != null) {
                    r.timeoutNowInFly.cancel(true);
                    r.timeoutNowInFly = null;
                }
                if (r.heartbeatTimer != null) {
                    r.heartbeatTimer.cancel(true);
                    r.heartbeatTimer = null;
                }
                if (r.blockTimer != null) {
                    r.blockTimer.cancel(true);
                    r.blockTimer = null;
                }
                if (r.waitId >= 0) {
                    r.options.getLogManager().removeWaiter(r.waitId);
                }
                r.notifyOnCaughtUp(errorCode, true);
            } finally {
                r.destroy();
            }
        } else if (errorCode == RaftError.ETIMEDOUT.getNumber()) {
            id.unlock();
            Utils.runInThread(() -> sendHeartbeat(id));
        } else {
            id.unlock();
            // noinspection ConstantConditions
            Requires.requireTrue(false, "Unknown error code for replicator: " + errorCode);
        }
    }

    private static void onCatchUpTimedOut(final ThreadId id) {
        final Replicator r = (Replicator) id.lock();
        if (r == null) {
            return;
        }
        try {
            r.notifyOnCaughtUp(RaftError.ETIMEDOUT.getNumber(), false);
        } finally {
            id.unlock();
        }
    }

    private void notifyOnCaughtUp(final int code, final boolean beforeDestroy) {
        if (this.catchUpClosure == null) {
            return;
        }
        if (code != RaftError.ETIMEDOUT.getNumber()) {
            if (this.nextIndex - 1 + this.catchUpClosure.getMaxMargin() < this.options.getLogManager().getLastLogIndex()) {
                return;
            }
            if (this.catchUpClosure.isErrorWasSet()) {
                return;
            }
            this.catchUpClosure.setErrorWasSet(true);
            if (code != RaftError.SUCCESS.getNumber()) {
                this.catchUpClosure.getStatus().setError(code, RaftError.describeCode(code));
            }
            if (this.catchUpClosure.hasTimer()) {
                if (!beforeDestroy && !this.catchUpClosure.getTimer().cancel(true)) {
                    // There's running timer task, let timer task trigger
                    // on_caught_up to void ABA problem
                    return;
                }
            }
        } else {
            // timed out
            if (!this.catchUpClosure.isErrorWasSet()) {
                this.catchUpClosure.getStatus().setError(code, RaftError.describeCode(code));
            }
        }
        final CatchUpClosure savedClosure = this.catchUpClosure;
        this.catchUpClosure = null;
        Utils.runClosureInThread(savedClosure, savedClosure.getStatus());
    }

    private static void onTimeout(final ThreadId id) {
        if (id != null) {
            id.setError(RaftError.ETIMEDOUT.getNumber());
        } else {
            LOG.warn("Replicator id is null when timeout, maybe it's destroyed.");
        }
    }

    /**
     * 销毁当前的复制器
     */
    void destroy() {
        final ThreadId savedId = this.id;
        LOG.info("Replicator {} is going to quit", savedId);
        this.id = null;
        releaseReader();
        // Unregister replicator metric set
        if (this.options.getNode().getNodeMetrics().isEnabled()) {
            this.options.getNode().getNodeMetrics().getMetricRegistry().remove(getReplicatorMetricName(this.options));
        }
        this.state = State.Destroyed;
        savedId.unlockAndDestroy();
    }

    private void releaseReader() {
        if (this.reader != null) {
            Utils.closeQuietly(this.reader);
            this.reader = null;
        }
    }

    /**
     * 处理心跳请求响应回调
     *
     * @param id
     * @param status
     * @param request
     * @param response
     * @param rpcSendTime
     */
    static void onHeartbeatReturned(final ThreadId id,
                                    final Status status,
                                    final AppendEntriesRequest request,
                                    final AppendEntriesResponse response,
                                    final long rpcSendTime) {
        if (id == null) {
            // replicator already was destroyed.
            return;
        }
        final long startTimeMs = Utils.nowMs();
        Replicator replicator;
        // 已经 destroy，忽略响应
        if ((replicator = (Replicator) id.lock()) == null) {
            return;
        }

        try {
            // 异常响应
            if (!status.isOk()) {
                // 切换状态为探针状态
                replicator.state = State.Probe;
                if (++replicator.consecutiveErrorTimes % 10 == 0) {
                    LOG.warn("Fail to issue RPC to {}, consecutiveErrorTimes={}, error={}", replicator.options.getPeerId(), replicator.consecutiveErrorTimes, status);
                }
                replicator.startHeartbeatTimer(startTimeMs);
                return;
            }

            /* 正常响应 */

            // 重置响应连续错误的次数
            replicator.consecutiveErrorTimes = 0;

            // 响应的 term 值比当前 leader 节点的 term 值更大
            if (response.getTerm() > replicator.options.getTerm()) {
                final NodeImpl node = replicator.options.getNode();
                replicator.notifyOnCaughtUp(RaftError.EPERM.getNumber(), true);
                replicator.destroy();
                node.increaseTermTo(response.getTerm(),
                        new Status(RaftError.EHIGHERTERMRESPONSE,
                                "Leader receives higher term heartbeat_response from peer:%s", replicator.options.getPeerId()));
                return;
            }

            // 更新到目标 follower 节点的最近一次 RPC 请求时间戳
            if (rpcSendTime > replicator.lastRpcSendTimestamp) {
                replicator.lastRpcSendTimestamp = rpcSendTime;
            }
            replicator.startHeartbeatTimer(startTimeMs);
        } finally {
            id.unlock();
        }
    }

    /**
     * 处理 RPC 响应
     *
     * @param id
     * @param reqType
     * @param status
     * @param request
     * @param response
     * @param seq
     * @param stateVersion
     * @param rpcSendTime
     */
    @SuppressWarnings("ContinueOrBreakFromFinallyBlock")
    static void onRpcReturned(final ThreadId id, final RequestType reqType, final Status status, final Message request,
                              final Message response, final int seq, final int stateVersion, final long rpcSendTime) {
        // 当前 replicator 已经被销毁
        if (id == null) {
            return;
        }
        // 当前 replicator 已经被销毁
        Replicator replicator;
        if ((replicator = (Replicator) id.lock()) == null) {
            return;
        }

        final long startTimeMs = Utils.nowMs();

        // replicator 的状态版本已经变更
        if (stateVersion != replicator.version) {
            LOG.debug("Replicator {} ignored old version response {}, current version is {}, request is {}\n, and response is {}\n, status is {}.",
                    replicator, stateVersion, replicator.version, request, response, status);
            id.unlock();
            return;
        }

        // 使用优先级队列按照 seq 排序，最小的会在第一个
        final PriorityQueue<RpcResponse> holdingQueue = replicator.pendingResponses;
        holdingQueue.add(new RpcResponse(reqType, seq, status, request, response, rpcSendTime));

        // 等待待处理的响应数目超过阈值（默认为 256）
        if (holdingQueue.size() > replicator.raftOptions.getMaxReplicatorInflightMsgs()) {
            LOG.warn("Too many pending responses {} for replicator {}, maxReplicatorInflightMsgs={}",
                    holdingQueue.size(), replicator.options.getPeerId(), replicator.raftOptions.getMaxReplicatorInflightMsgs());
            // 重新发送探针，清空数据
            replicator.resetInflights();
            replicator.state = State.Probe;
            replicator.sendEmptyEntries(false);
            return;
        }

        boolean continueSendEntries = false;

        try {
            int processed = 0;
            while (!holdingQueue.isEmpty()) {
                // 获取编号最小的响应
                final RpcResponse queuedPipelinedResponse = holdingQueue.peek();

                // 编号不匹配
                // sequence mismatch, waiting for next response.
                if (queuedPipelinedResponse.seq != replicator.requiredNextSeq) {
                    if (processed > 0) {
                        break;
                    } else {
                        //Do not processed any responses, UNLOCK id and return.
                        continueSendEntries = false;
                        id.unlock();
                        return;
                    }
                }
                holdingQueue.remove();
                processed++;
                // 获取第一个待处理请求
                final Inflight inflight = replicator.pollInflight();
                if (inflight == null) {
                    // The previous in-flight requests were cleared.
                    continue;
                }
                // 顺序对不上，可能乱了，重来
                if (inflight.seq != queuedPipelinedResponse.seq) {
                    // reset state
                    LOG.warn("Replicator {} response sequence out of order, expect {}, but it is {}, reset state to try again.",
                            replicator, inflight.seq, queuedPipelinedResponse.seq);
                    replicator.resetInflights();
                    replicator.state = State.Probe;
                    continueSendEntries = false;
                    replicator.block(Utils.nowMs(), RaftError.EREQUEST.getNumber());
                    return;
                }
                try {
                    // 按类型处理响应
                    switch (queuedPipelinedResponse.requestType) {
                        case AppendEntries:
                            continueSendEntries = onAppendEntriesReturned(id, inflight, queuedPipelinedResponse.status,
                                    (AppendEntriesRequest) queuedPipelinedResponse.request,
                                    (AppendEntriesResponse) queuedPipelinedResponse.response, rpcSendTime, startTimeMs, replicator);
                            break;
                        case Snapshot:
                            continueSendEntries = onInstallSnapshotReturned(id, replicator, queuedPipelinedResponse.status,
                                    (InstallSnapshotRequest) queuedPipelinedResponse.request,
                                    (InstallSnapshotResponse) queuedPipelinedResponse.response);
                            break;
                    }
                } finally {
                    if (continueSendEntries) {
                        // Success, increase the response sequence.
                        replicator.getAndIncrementRequiredNextSeq();
                    } else {
                        // The id is already unlocked in onAppendEntriesReturned/onInstallSnapshotReturned, we SHOULD break out.
                        break;
                    }
                }
            }
        } finally {
            if (continueSendEntries) {
                // unlock in sendEntries.
                replicator.sendEntries();
            }
        }
    }

    /**
     * Reset in-flight state.
     */
    void resetInflights() {
        this.version++;
        this.inflights.clear();
        this.pendingResponses.clear();
        final int rs = Math.max(this.reqSeq, this.requiredNextSeq);
        this.reqSeq = this.requiredNextSeq = rs;
        releaseReader();
    }

    /**
     * 处理日志复制响应
     *
     * @param id
     * @param inflight
     * @param status
     * @param request
     * @param response
     * @param rpcSendTime
     * @param startTimeMs
     * @param replicator
     * @return
     */
    private static boolean onAppendEntriesReturned(final ThreadId id, final Inflight inflight, final Status status,
                                                   final AppendEntriesRequest request,
                                                   final AppendEntriesResponse response, final long rpcSendTime,
                                                   final long startTimeMs, final Replicator replicator) {
        // 校验数据序列
        if (inflight.startIndex != request.getPrevLogIndex() + 1) {
            LOG.warn("Replicator {} received invalid AppendEntriesResponse, in-flight startIndex={}, requset prevLogIndex={}, reset the replicator state and probe again.",
                    replicator, inflight.startIndex, request.getPrevLogIndex());
            replicator.resetInflights();
            replicator.state = State.Probe;
            // unlock id in sendEmptyEntries
            replicator.sendEmptyEntries(false);
            return false;
        }
        // record metrics
        if (request.getEntriesCount() > 0) {
            replicator.nodeMetrics.recordLatency("replicate-entries", Utils.monotonicMs() - rpcSendTime);
            replicator.nodeMetrics.recordSize("replicate-entries-count", request.getEntriesCount());
            replicator.nodeMetrics.recordSize("replicate-entries-bytes", request.getData() != null ? request.getData().size() : 0);
        }

        final boolean isLogDebugEnabled = LOG.isDebugEnabled();
        StringBuilder sb = null;
        if (isLogDebugEnabled) {
            sb = new StringBuilder("Node "). //
                    append(replicator.options.getGroupId()).append(":").append(replicator.options.getServerId()). //
                    append(" received AppendEntriesResponse from "). //
                    append(replicator.options.getPeerId()). //
                    append(" prevLogIndex=").append(request.getPrevLogIndex()). //
                    append(" prevLogTerm=").append(request.getPrevLogTerm()). //
                    append(" count=").append(request.getEntriesCount());
        }
        // 如果 follower 因为崩溃、RPC 调用失败等原因未收到成功的响应，则阻塞一段时间后重试
        if (!status.isOk()) {
            // If the follower crashes, any RPC to the follower fails immediately,
            // so we need to block the follower for a while instead of looping until
            // it comes back or be removed
            // dummy_id is unlock in block
            if (isLogDebugEnabled) {
                sb.append(" fail, sleep.");
                LOG.debug(sb.toString());
            }
            if (++replicator.consecutiveErrorTimes % 10 == 0) {
                LOG.warn("Fail to issue RPC to {}, consecutiveErrorTimes={}, error={}",
                        replicator.options.getPeerId(), replicator.consecutiveErrorTimes, status);
            }
            replicator.resetInflights();
            replicator.state = State.Probe;
            // unlock in in block
            replicator.block(startTimeMs, status.getCode());
            return false;
        }
        replicator.consecutiveErrorTimes = 0;
        // 响应失败
        if (!response.getSuccess()) {
            // Leader 切换，可能出现了一次网络分析，从新更随新的 Leader
            if (response.getTerm() > replicator.options.getTerm()) {
                if (isLogDebugEnabled) {
                    sb.append(" fail, greater term ").append(response.getTerm()).append(" expect term ").append(replicator.options.getTerm());
                    LOG.debug(sb.toString());
                }
                // 调整自己的 term 值
                final NodeImpl node = replicator.options.getNode();
                replicator.notifyOnCaughtUp(RaftError.EPERM.getNumber(), true);
                replicator.destroy();
                node.increaseTermTo(response.getTerm(), new Status(RaftError.EHIGHERTERMRESPONSE,
                        "Leader receives higher term heartbeat_response from peer:%s", replicator.options.getPeerId()));
                return false;
            }
            if (isLogDebugEnabled) {
                sb.append(" fail, find nextIndex remote lastLogIndex ").append(response.getLastLogIndex()).append(" local nextIndex ").append(replicator.nextIndex);
                LOG.debug(sb.toString());
            }
            if (rpcSendTime > replicator.lastRpcSendTimestamp) {
                replicator.lastRpcSendTimestamp = rpcSendTime;
            }
            // Fail, reset the state to try again from nextIndex.
            replicator.resetInflights();
            // prev_log_index and prev_log_term doesn't match
            if (response.getLastLogIndex() + 1 < replicator.nextIndex) {
                LOG.debug("LastLogIndex at peer={} is {}", replicator.options.getPeerId(), response.getLastLogIndex());
                // The peer contains less logs than leader
                replicator.nextIndex = response.getLastLogIndex() + 1;
            } else {
                // The peer contains logs from old term which should be truncated,
                // decrease _last_log_at_peer by one to test the right index to keep
                if (replicator.nextIndex > 1) {
                    LOG.debug("logIndex={} dismatch", replicator.nextIndex);
                    replicator.nextIndex--;
                } else {
                    LOG.error("Peer={} declares that log at index=0 doesn't match, which is not supposed to happen", replicator.options.getPeerId());
                }
            }
            // dummy_id is unlock in _send_heartbeat
            replicator.sendEmptyEntries(false);
            return false;
        }
        if (isLogDebugEnabled) {
            sb.append(", success");
            LOG.debug(sb.toString());
        }
        // success
        if (response.getTerm() != replicator.options.getTerm()) {
            replicator.resetInflights();
            replicator.state = State.Probe;
            LOG.error("Fail, response term {} dismatch, expect term {}", response.getTerm(), replicator.options.getTerm());
            id.unlock();
            return false;
        }
        if (rpcSendTime > replicator.lastRpcSendTimestamp) {
            replicator.lastRpcSendTimestamp = rpcSendTime;
        }
        final int entriesSize = request.getEntriesCount();
        if (entriesSize > 0) {
            replicator.options.getBallotBox().commitAt(replicator.nextIndex, replicator.nextIndex + entriesSize - 1, replicator.options.getPeerId());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Replicated logs in [{}, {}] to peer {}", replicator.nextIndex, replicator.nextIndex + entriesSize - 1,
                        replicator.options.getPeerId());
            }
        } else {
            // The request is probe request, change the state into Replicate.
            replicator.state = State.Replicate;
        }
        replicator.nextIndex += entriesSize;
        replicator.hasSucceeded = true;
        replicator.notifyOnCaughtUp(RaftError.SUCCESS.getNumber(), false);
        // dummy_id is unlock in _send_entries
        if (replicator.timeoutNowIndex > 0 && replicator.timeoutNowIndex < replicator.nextIndex) {
            replicator.sendTimeoutNow(false, false);
        }
        return true;
    }

    /**
     * 添加一些必要的参数到 AppendEntriesRequest 请求中
     *
     * @param rb
     * @param prevLogIndex
     * @param isHeartbeat
     * @return
     */
    private boolean fillCommonFields(final AppendEntriesRequest.Builder rb, long prevLogIndex, final boolean isHeartbeat) {
        final long prevLogTerm = this.options.getLogManager().getTerm(prevLogIndex);
        if (prevLogTerm == 0 && prevLogIndex != 0) {
            // 探针请求
            if (!isHeartbeat) {
                Requires.requireTrue(prevLogIndex < this.options.getLogManager().getFirstLogIndex());
                LOG.debug("logIndex={} was compacted", prevLogIndex);
                return false;
            }
            // 心跳请求
            else {
                // The log at prev_log_index has been compacted, which indicates
                // we is or is going to install snapshot to the follower. So we let
                // both prev_log_index and prev_log_term be 0 in the heartbeat
                // request so that follower would do nothing besides updating its
                // leader timestamp.
                prevLogIndex = 0;
            }
        }
        rb.setTerm(this.options.getTerm());
        rb.setGroupId(this.options.getGroupId());
        rb.setServerId(this.options.getServerId().toString());
        rb.setPeerId(this.options.getPeerId().toString());
        rb.setPrevLogIndex(prevLogIndex);
        rb.setPrevLogTerm(prevLogTerm);
        rb.setCommittedIndex(this.options.getBallotBox().getLastCommittedIndex());
        return true;
    }

    private void waitMoreEntries(final long nextWaitIndex) {
        try {
            LOG.debug("Node {} waits more entries", this.options.getNode().getNodeId());
            if (this.waitId >= 0) {
                return;
            }
            this.waitId = this.options.getLogManager().wait(nextWaitIndex - 1,
                    (arg, errorCode) -> continueSending((ThreadId) arg, errorCode), this.id);
            this.statInfo.runningState = RunningState.IDLE;
        } finally {
            this.id.unlock();
        }
    }

    /**
     * Send as many requests as possible.
     */
    void sendEntries() {
        boolean doUnlock = true;
        try {
            long prevSendIndex = -1;
            while (true) {
                final long nextSendingIndex = getNextSendIndex();
                if (nextSendingIndex > prevSendIndex) {
                    if (sendEntries(nextSendingIndex)) {
                        prevSendIndex = nextSendingIndex;
                    } else {
                        doUnlock = false;
                        // id already unlock in sendEntries when it returns false.
                        break;
                    }
                } else {
                    break;
                }
            }
        } finally {
            if (doUnlock) {
                this.id.unlock();
            }
        }

    }

    /**
     * Send log entries to follower, returns true when success, otherwise false and unlock the id.
     *
     * 批量向 follower 发送日志
     *
     * @param nextSendingIndex next sending index 下一个需要发送的 entry index
     * @return send result.
     */
    private boolean sendEntries(final long nextSendingIndex) {
        final AppendEntriesRequest.Builder rb = AppendEntriesRequest.newBuilder();
        if (!fillCommonFields(rb, nextSendingIndex - 1, false)) {
            // unlock id in installSnapshot
            installSnapshot();
            return false;
        }

        ByteBufferCollector dataBuf = null;
        // 一次获取最大的 entry 数目，默认为 1024
        final int maxEntriesSize = this.raftOptions.getMaxEntriesSize();
        final RecyclableByteBufferList byteBufList = RecyclableByteBufferList.newInstance();
        try {
            // 构造本次需要传输的 entry 集合，封装到 byteBufList 中
            for (int i = 0; i < maxEntriesSize; i++) {
                final RaftOutter.EntryMeta.Builder emb = RaftOutter.EntryMeta.newBuilder();
                // 根据当前的 i 和 nextSendingIndex 计算出当前的偏移量，然后去 LogManager 找到对应的 LogEntry，
                // 再把 LogEntry 里面的属性设置到 emb 中，并把 LogEntry 里面的数据加入到 RecyclableByteBufferList 中，
                // nextSendingIndex 表示下一个需要发送的 entry index, i 表示偏移量
                if (!prepareEntry(nextSendingIndex, i, emb, byteBufList)) {
                    break;
                }
                rb.addEntries(emb.build());
            } // end of for

            // 本次没有数据需要发送
            if (rb.getEntriesCount() == 0) {
                if (nextSendingIndex < this.options.getLogManager().getFirstLogIndex()) {
                    installSnapshot();
                    return false;
                }
                // _id is unlock in _wait_more
                waitMoreEntries(nextSendingIndex);
                return false;
            }

            // 将 byteBufList 中的数据设置到 AppendEntriesRequest 的 data 字段
            if (byteBufList.getCapacity() > 0) {
                dataBuf = ByteBufferCollector.allocateByRecyclers(byteBufList.getCapacity());
                for (final ByteBuffer b : byteBufList) {
                    dataBuf.put(b);
                }
                final ByteBuffer buf = dataBuf.getBuffer();
                buf.flip();
                rb.setData(ZeroByteStringHelper.wrap(buf));
            }
        } finally {
            // 回收空间
            RecycleUtil.recycle(byteBufList);
        }

        final AppendEntriesRequest request = rb.build();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Node {} send AppendEntriesRequest to {} term {} lastCommittedIndex {} prevLogIndex {} prevLogTerm {} logIndex {} count {}",
                    this.options.getNode().getNodeId(), this.options.getPeerId(), this.options.getTerm(),
                    request.getCommittedIndex(), request.getPrevLogIndex(), request.getPrevLogTerm(), nextSendingIndex,
                    request.getEntriesCount());
        }
        this.statInfo.runningState = RunningState.APPENDING_ENTRIES;
        this.statInfo.firstLogIndex = rb.getPrevLogIndex() + 1;
        this.statInfo.lastLogIndex = rb.getPrevLogIndex() + rb.getEntriesCount();

        final Recyclable recyclable = dataBuf;
        final int v = this.version;
        final long monotonicSendTimeMs = Utils.monotonicMs();
        final int seq = getAndIncrementReqSeq();

        // 发送 AppendEntriesRequest 请求
        final Future<Message> rpcFuture = this.rpcService.appendEntries(
                this.options.getPeerId().getEndpoint(), request, -1,
                new RpcResponseClosureAdapter<AppendEntriesResponse>() {

                    @Override
                    public void run(final Status status) {
                        RecycleUtil.recycle(recyclable);
                        onRpcReturned(
                                Replicator.this.id,
                                RequestType.AppendEntries,
                                status,
                                request,
                                getResponse(),
                                seq,
                                v,
                                monotonicSendTimeMs);
                    }

                });
        this.addInflight(RequestType.AppendEntries, nextSendingIndex, request.getEntriesCount(), request.getData().size(), seq, rpcFuture);
        return true;

    }

    public static void sendHeartbeat(final ThreadId id, final RpcResponseClosure<AppendEntriesResponse> closure) {
        final Replicator r = (Replicator) id.lock();
        if (r == null) {
            Utils.runClosureInThread(closure, new Status(RaftError.EHOSTDOWN, "Peer %s is not connected", id));
            return;
        }
        //id unlock in send empty entries.
        r.sendEmptyEntries(true, closure);
    }

    private static void sendHeartbeat(final ThreadId id) {
        final Replicator r = (Replicator) id.lock();
        if (r == null) {
            return;
        }
        // unlock in sendEmptyEntries
        r.sendEmptyEntries(true);
    }

    @SuppressWarnings("SameParameterValue")
    private void sendTimeoutNow(final boolean unlockId, final boolean stopAfterFinish) {
        this.sendTimeoutNow(unlockId, stopAfterFinish, -1);
    }

    private void sendTimeoutNow(final boolean unlockId, final boolean stopAfterFinish, final int timeoutMs) {
        final TimeoutNowRequest.Builder rb = TimeoutNowRequest.newBuilder();
        rb.setTerm(this.options.getTerm());
        rb.setGroupId(this.options.getGroupId());
        rb.setServerId(this.options.getServerId().toString());
        rb.setPeerId(this.options.getPeerId().toString());
        try {
            if (!stopAfterFinish) {
                // This RPC is issued by transfer_leadership, save this call_id so that
                // the RPC can be cancelled by stop.
                this.timeoutNowInFly = timeoutNow(rb, false, timeoutMs);
                this.timeoutNowIndex = 0;
            } else {
                timeoutNow(rb, true, timeoutMs);
            }
        } finally {
            if (unlockId) {
                this.id.unlock();
            }
        }

    }

    private Future<Message> timeoutNow(final TimeoutNowRequest.Builder rb, final boolean stopAfterFinish,
                                       final int timeoutMs) {
        final TimeoutNowRequest request = rb.build();
        return this.rpcService.timeoutNow(this.options.getPeerId().getEndpoint(), request, timeoutMs,
                new RpcResponseClosureAdapter<TimeoutNowResponse>() {

                    @Override
                    public void run(final Status status) {
                        if (Replicator.this.id != null) {
                            onTimeoutNowReturned(Replicator.this.id, status, request, getResponse(), stopAfterFinish);
                        }
                    }

                });
    }

    @SuppressWarnings("unused")
    static void onTimeoutNowReturned(final ThreadId id, final Status status, final TimeoutNowRequest request,
                                     final TimeoutNowResponse response, final boolean stopAfterFinish) {
        final Replicator r = (Replicator) id.lock();
        if (r == null) {
            return;
        }

        final boolean isLogDebugEnabled = LOG.isDebugEnabled();
        StringBuilder sb = null;
        if (isLogDebugEnabled) {
            sb = new StringBuilder("Node "). //
                    append(r.options.getGroupId()).append(":").append(r.options.getServerId()). //
                    append(" received TimeoutNowResponse from "). //
                    append(r.options.getPeerId());
        }
        if (!status.isOk()) {
            if (isLogDebugEnabled) {
                sb.append(" fail:").append(status);
                LOG.debug(sb.toString());
            }
            if (stopAfterFinish) {
                r.notifyOnCaughtUp(RaftError.ESTOP.getNumber(), true);
                r.destroy();
            } else {
                id.unlock();
            }
            return;
        }
        if (isLogDebugEnabled) {
            sb.append(response.getSuccess() ? " success" : " fail");
            LOG.debug(sb.toString());
        }
        if (response.getTerm() > r.options.getTerm()) {
            final NodeImpl node = r.options.getNode();
            r.notifyOnCaughtUp(RaftError.EPERM.getNumber(), true);
            r.destroy();
            node.increaseTermTo(response.getTerm(), new Status(RaftError.EHIGHERTERMRESPONSE,
                    "Leader receives higher term timeout_now_response from peer:%s", r.options.getPeerId()));
            return;
        }
        if (stopAfterFinish) {
            r.notifyOnCaughtUp(RaftError.ESTOP.getNumber(), true);
            r.destroy();
        } else {
            id.unlock();
        }

    }

    public static boolean stop(final ThreadId id) {
        id.setError(RaftError.ESTOP.getNumber());
        return true;
    }

    public static boolean join(final ThreadId id) {
        id.join();
        return true;
    }

    public static long getLastRpcSendTimestamp(final ThreadId id) {
        final Replicator r = (Replicator) id.getData();
        if (r == null) {
            return 0L;
        }
        return r.lastRpcSendTimestamp;
    }

    public static boolean transferLeadership(final ThreadId id, final long logIndex) {
        final Replicator r = (Replicator) id.lock();
        if (r == null) {
            return false;
        }
        // dummy is unlock in _transfer_leadership
        return r.transferLeadership(logIndex);
    }

    private boolean transferLeadership(final long logIndex) {
        if (this.hasSucceeded && this.nextIndex > logIndex) {
            // _id is unlock in _send_timeout_now
            this.sendTimeoutNow(true, false);
            return true;
        }
        // Register log_index so that _on_rpc_return trigger
        // _send_timeout_now if _next_index reaches log_index
        this.timeoutNowIndex = logIndex;
        this.id.unlock();
        return true;
    }

    public static boolean stopTransferLeadership(final ThreadId id) {
        final Replicator r = (Replicator) id.lock();
        if (r == null) {
            return false;
        }
        r.timeoutNowIndex = 0;
        id.unlock();
        return true;
    }

    public static boolean sendTimeoutNowAndStop(final ThreadId id, final int timeoutMs) {
        final Replicator r = (Replicator) id.lock();
        if (r == null) {
            return false;
        }
        // id unlock in sendTimeoutNow
        r.sendTimeoutNow(true, true, timeoutMs);
        return true;
    }

    public static long getNextIndex(final ThreadId id) {
        final Replicator r = (Replicator) id.lock();
        if (r == null) {
            return 0;
        }
        long nextIdx = 0;
        if (r.hasSucceeded) {
            nextIdx = r.nextIndex;
        }
        id.unlock();
        return nextIdx;
    }

    /**
     * Replicator metric set.
     *
     * @author dennis
     */
    private static final class ReplicatorMetricSet implements MetricSet {
        private final ReplicatorOptions opts;
        private final Replicator r;

        private ReplicatorMetricSet(final ReplicatorOptions opts, final Replicator r) {
            this.opts = opts;
            this.r = r;
        }

        @Override
        public Map<String, Metric> getMetrics() {
            final Map<String, Metric> gauges = new HashMap<>();
            gauges.put("log-lags",
                    (Gauge<Long>) () -> this.opts.getLogManager().getLastLogIndex() - (this.r.nextIndex - 1));
            gauges.put("next-index", (Gauge<Long>) () -> this.r.nextIndex);
            gauges.put("heartbeat-times", (Gauge<Long>) () -> this.r.heartbeatCounter);
            gauges.put("install-snapshot-times", (Gauge<Long>) () -> this.r.installSnapshotCounter);
            gauges.put("append-entries-times", (Gauge<Long>) () -> this.r.appendEntriesCounter);
            return gauges;
        }
    }

}
