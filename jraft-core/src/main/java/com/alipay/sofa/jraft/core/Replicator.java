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

import com.alipay.sofa.jraft.Node;
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
import com.alipay.sofa.jraft.rpc.RpcUtils;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.util.ByteBufferCollector;
import com.alipay.sofa.jraft.util.OnlyForTest;
import com.alipay.sofa.jraft.util.Recyclable;
import com.alipay.sofa.jraft.util.RecyclableByteBufferList;
import com.alipay.sofa.jraft.util.RecycleUtil;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.ThreadId;
import com.alipay.sofa.jraft.util.Utils;
import com.alipay.sofa.jraft.util.internal.ThrowUtil;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.google.protobuf.Message;
import com.google.protobuf.ZeroByteStringHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Replicator for replicating log entry from leader to followers.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-04 10:32:02 AM
 */
@ThreadSafe
public class Replicator implements ThreadId.OnError {

    private static final Logger LOG = LoggerFactory.getLogger(Replicator.class);

    private final RaftClientService rpcService;
    /** 记录待发送的下一个 logIndex 位置 */
    private volatile long nextIndex;
    private int consecutiveErrorTimes = 0;
    private boolean hasSucceeded;
    private long timeoutNowIndex;
    /** 记录最近一次成功向目标节点发送 RPC 请求的时间戳 */
    private volatile long lastRpcSendTimestamp;
    private volatile long heartbeatCounter = 0;
    private volatile long appendEntriesCounter = 0;
    private volatile long installSnapshotCounter = 0;
    protected Stat statInfo = new Stat();
    private ScheduledFuture<?> blockTimer;

    /** 记录最近的 inflight RPC 请求 */
    private Inflight rpcInFly;
    // Heartbeat RPC future
    private Future<Message> heartbeatInFly;
    // Timeout request RPC future
    private Future<Message> timeoutNowInFly;
    /** FIFO 队列，记录 inflight RPC 请求列表 */
    private final ArrayDeque<Inflight> inflights = new ArrayDeque<>();

    private long waitId = -1L;
    protected ThreadId id;
    private final ReplicatorOptions options;
    private final RaftOptions raftOptions;

    private ScheduledFuture<?> heartbeatTimer;
    private volatile SnapshotReader reader;
    private CatchUpClosure catchUpClosure;
    private final Scheduler timerManager;
    private final NodeMetrics nodeMetrics;
    private volatile State state;

    /** 请求序列 */
    private int reqSeq = 0;
    /** 期望的响应序列 */
    private int requiredNextSeq = 0;
    /** 状态版本，当重置 inflight 请求队列时会递增，以实现忽略版本不匹配的 inflight 请求响应 */
    private int version = 0;

    /**
     * 记录已经收到但是还没有被处理的响应，按照请求序列从小到大排序，
     * 响应的顺序是未知的，但是需要保证处理的顺序
     */
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
        Probe, // probe follower state
        Snapshot, // installing snapshot to follower
        Replicate, // replicate logs normally
        Destroyed // destroyed
    }

    public Replicator(final ReplicatorOptions replicatorOptions, final RaftOptions raftOptions) {
        super();
        this.options = replicatorOptions;
        this.nodeMetrics = this.options.getNode().getNodeMetrics();
        this.nextIndex = this.options.getLogManager().getLastLogIndex() + 1;
        this.timerManager = replicatorOptions.getTimerManager();
        this.raftOptions = raftOptions;
        this.rpcService = replicatorOptions.getRaftRpcService();
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

    enum ReplicatorEvent {
        CREATED, // created
        ERROR, // error
        DESTROYED // destroyed
    }

    /**
     * User can implement the ReplicatorStateListener interface by themselves.
     * So they can do some their own logic codes when replicator created, destroyed or had some errors.
     *
     * @author zongtanghu
     *
     * 2019-Aug-20 2:32:10 PM
     */
    public interface ReplicatorStateListener {

        /**
         * Called when this replicator has been created.
         *
         * @param peer replicator related peerId
         */
        void onCreated(final PeerId peer);

        /**
         * Called when this replicator has some errors.
         *
         * @param peer replicator related peerId
         * @param status replicator's error detailed status
         */
        void onError(final PeerId peer, final Status status);

        /**
         * Called when this replicator has been destroyed.
         *
         * @param peer replicator related peerId
         */
        void onDestroyed(final PeerId peer);
    }

    /**
     * Notify replicator event(such as created, error, destroyed) to replicatorStateListener which is implemented by users.
     *
     * @param replicator replicator object
     * @param event replicator's state listener event type
     * @param status replicator's error detailed status
     */
    private static void notifyReplicatorStatusListener(final Replicator replicator, final ReplicatorEvent event,
                                                       final Status status) {
        final ReplicatorOptions replicatorOpts = Requires.requireNonNull(replicator.getOpts(), "replicatorOptions");
        final Node node = Requires.requireNonNull(replicatorOpts.getNode(), "node");
        final PeerId peer = Requires.requireNonNull(replicatorOpts.getPeerId(), "peer");

        final List<ReplicatorStateListener> listenerList = node.getReplicatorStatueListeners();
        for (int i = 0; i < listenerList.size(); i++) {
            final ReplicatorStateListener listener = listenerList.get(i);
            if (listener != null) {
                try {
                    switch (event) {
                        case CREATED:
                            RpcUtils.runInThread(() -> listener.onCreated(peer));
                            break;
                        case ERROR:
                            RpcUtils.runInThread(() -> listener.onError(peer, status));
                            break;
                        case DESTROYED:
                            RpcUtils.runInThread(() -> listener.onDestroyed(peer));
                            break;
                        default:
                            break;
                    }
                } catch (final Exception e) {
                    LOG.error("Fail to notify ReplicatorStatusListener, listener={}, event={}.", listener, event);
                }
            }
        }
    }

    /**
     * Notify replicator event(such as created, error, destroyed) to replicatorStateListener which is implemented by users for none status.
     *
     * @param replicator replicator object
     * @param event replicator's state listener event type
     */
    private static void notifyReplicatorStatusListener(final Replicator replicator, final ReplicatorEvent event) {
        notifyReplicatorStatusListener(replicator, event, null);
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
        /** 请求中的 LogEntry 数目 */
        final int count;
        /** 请求对应的起始 nextIndex 值 */
        final long startIndex;
        /** LogEntry 的总字节长度 */
        final int size;
        /** RPC future */
        final Future<Message> rpcFuture;
        /** 请求类型：复制日志 or 安装快照 */
        final RequestType requestType;
        /** 请求序列，用于匹配请求和响应，保证按照请求的顺序处理响应 */
        final int seq;

        public Inflight(final RequestType requestType,
                        final long startIndex,
                        final int count,
                        final int size,
                        final int seq,
                        final Future<Message> rpcFuture) {
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
     * @param reqType type of request
     * @param count count if request
     * @param size size in bytes
     */
    private void addInflight(final RequestType reqType,
                             final long startIndex,
                             final int count,
                             final int size,
                             final int seq,
                             final Future<Message> rpcInfly) {
        // 更新本地记录的最新的 inflight RPC 请求
        this.rpcInFly = new Inflight(reqType, startIndex, count, size, seq, rpcInfly);
        // 标记当前请求为 inflight
        this.inflights.add(this.rpcInFly);
        this.nodeMetrics.recordSize("replicate-inflights-count", this.inflights.size());
    }

    /**
     * Returns the next in-flight sending index, return -1 when can't send more in-flight requests.
     *
     * @return next in-flight sending index
     */
    long getNextSendIndex() {
        // 没有 inflight 请求，从 nextIndex 开始发送
        if (this.inflights.isEmpty()) {
            return this.nextIndex;
        }
        // 太多 inflight 请求，暂停发送新的 AppendEntries 请求
        if (this.inflights.size() > this.raftOptions.getMaxReplicatorInflightMsgs()) {
            return -1L;
        }
        // Last request should be a AppendEntries request and has some entries.
        // 最近一次发送的 RPC 请求是携带 LogEntry 的 AppendEntries 请求
        if (this.rpcInFly != null && this.rpcInFly.isSendingLogEntries()) {
            // 计算并返回接下去待发送的 LogEntry 对应的 logIndex 值
            return this.rpcInFly.startIndex + this.rpcInFly.count;
        }
        return -1L;
    }

    private Inflight pollInflight() {
        return this.inflights.poll();
    }

    private void startHeartbeatTimer(final long startMs) {
        final long dueTime = startMs + this.options.getDynamicHeartBeatTimeoutMs();
        try {
            this.heartbeatTimer = this.timerManager.schedule(
                    () -> onTimeout(this.id), dueTime - Utils.nowMs(), TimeUnit.MILLISECONDS);
        } catch (final Exception e) {
            LOG.error("Fail to schedule heartbeat timer", e);
            onTimeout(this.id);
        }
    }

    void installSnapshot() {
        // 正在给目标 Follower 节点安装快照，无需重复执行
        if (this.state == State.Snapshot) {
            LOG.warn("Replicator {} is installing snapshot, ignore the new request.", this.options.getPeerId());
            this.id.unlock();
            return;
        }
        boolean doUnlock = true;
        try {
            Requires.requireTrue(this.reader == null,
                    "Replicator %s already has a snapshot reader, current state is %s", this.options.getPeerId(), this.state);
            // 创建并初始化快照读取器，具体实现为 LocalSnapshotReader 类
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
            // 生一个快照访问地址
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
            // 加载快照元数据信息
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
            // 构造安装快照请求
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
            // 标记当前运行状态为正在给目标节点安装快照
            this.state = State.Snapshot;
            // noinspection NonAtomicOperationOnVolatileField
            this.installSnapshotCounter++;
            final long monotonicSendTimeMs = Utils.monotonicMs();
            final int stateVersion = this.version;
            // 递增请求序列
            final int seq = getAndIncrementReqSeq();
            // 向目标节点发送安装快照请求
            final Future<Message> rpcFuture = this.rpcService.installSnapshot(
                    this.options.getPeerId().getEndpoint(),
                    request,
                    new RpcResponseClosureAdapter<InstallSnapshotResponse>() {

                        @Override
                        public void run(final Status status) {
                            onRpcReturned(Replicator.this.id, RequestType.Snapshot, status, request, getResponse(), seq, stateVersion, monotonicSendTimeMs);
                        }
                    });
            // 标记当前请求为 in-flight
            addInflight(RequestType.Snapshot, this.nextIndex, 0, 0, seq, rpcFuture);
        } finally {
            if (doUnlock) {
                this.id.unlock();
            }
        }
    }

    @SuppressWarnings("unused")
    static boolean onInstallSnapshotReturned(final ThreadId id,
                                             final Replicator r,
                                             final Status status,
                                             final InstallSnapshotRequest request,
                                             final InstallSnapshotResponse response) {
        boolean success = true;
        // 关闭快照数据读取器
        r.releaseReader();
        // noinspection ConstantConditions
        do {
            final StringBuilder sb = new StringBuilder("Node "). //
                    append(r.options.getGroupId()).append(":").append(r.options.getServerId()). //
                    append(" received InstallSnapshotResponse from ").append(r.options.getPeerId()). //
                    append(" lastIncludedIndex=").append(request.getMeta().getLastIncludedIndex()). //
                    append(" lastIncludedTerm=").append(request.getMeta().getLastIncludedTerm());
            // 目标 Follower 节点运行异常
            if (!status.isOk()) {
                sb.append(" error:").append(status);
                LOG.info(sb.toString());
                notifyReplicatorStatusListener(r, ReplicatorEvent.ERROR, status);
                if (++r.consecutiveErrorTimes % 10 == 0) {
                    LOG.warn("Fail to install snapshot at peer={}, error={}", r.options.getPeerId(), status);
                }
                success = false;
                break;
            }
            // 目标 Follower 节点拒绝本次安装快照的请求
            if (!response.getSuccess()) {
                sb.append(" success=false");
                LOG.info(sb.toString());
                success = false;
                break;
            }
            // 目标 Follower 节点成功处理本次安装快照的请求，更新 nextIndex
            r.nextIndex = request.getMeta().getLastIncludedIndex() + 1;
            sb.append(" success=true");
            LOG.info(sb.toString());
        } while (false);
        // We don't retry installing the snapshot explicitly.
        // id is unlock in sendEntries
        // 给目标节点安装快照失败，清空 inflight 请求，重新发送探针请求
        if (!success) {
            //should reset states
            r.resetInflights();
            r.state = State.Probe;
            r.block(Utils.nowMs(), status.getCode());
            return false;
        }
        r.hasSucceeded = true;
        // 回调 CatchUpClosure
        r.notifyOnCaughtUp(RaftError.SUCCESS.getNumber(), false);
        if (r.timeoutNowIndex > 0 && r.timeoutNowIndex < r.nextIndex) {
            r.sendTimeoutNow(false, false);
        }
        // id is unlock in _send_entriesheartbeatCounter
        r.state = State.Replicate;
        return true;
    }

    /**
     * 发送空的 AppendEntries 请求
     *
     * @param isHeartbeat 是否是心跳请求
     */
    private void sendEmptyEntries(final boolean isHeartbeat) {
        sendEmptyEntries(isHeartbeat, null);
    }

    /**
     * Send probe or heartbeat request
     *
     * @param isHeartbeat if current entries is heartbeat
     * @param heartBeatClosure heartbeat callback
     */
    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    private void sendEmptyEntries(final boolean isHeartbeat,
                                  final RpcResponseClosure<AppendEntriesResponse> heartBeatClosure) {
        // 构建 AppendEntries 请求
        final AppendEntriesRequest.Builder rb = AppendEntriesRequest.newBuilder();
        // 为 AppendEntries 请求填充基础参数，包括当前节点的 term 值、groupId、节点 ID，以及 committedLogIndex 等等
        // 如果返回 false 说明待发送的部分日志已经变为快照，需要先给目标节点安装快照
        if (!fillCommonFields(rb, this.nextIndex - 1, isHeartbeat)) {
            // id is unlock in installSnapshot
            installSnapshot();
            if (isHeartbeat && heartBeatClosure != null) {
                RpcUtils.runClosureInThread(heartBeatClosure,
                        new Status(RaftError.EAGAIN, "Fail to send heartbeat to peer %s", this.options.getPeerId()));
            }
            return;
        }
        try {
            final long monotonicSendTimeMs = Utils.monotonicMs();
            final AppendEntriesRequest request = rb.build();

            // 心跳请求
            if (isHeartbeat) {
                // Sending a heartbeat request
                this.heartbeatCounter++;
                RpcResponseClosure<AppendEntriesResponse> heartbeatDone;
                // 参数指定的响应回调优先
                if (heartBeatClosure != null) {
                    heartbeatDone = heartBeatClosure;
                }
                // 设置默认的心跳请求响应回调
                else {
                    heartbeatDone = new RpcResponseClosureAdapter<AppendEntriesResponse>() {

                        @Override
                        public void run(final Status status) {
                            onHeartbeatReturned(Replicator.this.id, status, request, getResponse(), monotonicSendTimeMs);
                        }
                    };
                }
                // 发送心跳请求
                this.heartbeatInFly = this.rpcService.appendEntries(
                        this.options.getPeerId().getEndpoint(),
                        request,
                        this.options.getElectionTimeoutMs() / 2,
                        heartbeatDone);
            }
            // 探针请求
            else {
                // Sending a probe request.
                this.statInfo.runningState = RunningState.APPENDING_ENTRIES;
                this.statInfo.firstLogIndex = this.nextIndex;
                this.statInfo.lastLogIndex = this.nextIndex - 1;
                this.appendEntriesCounter++;
                this.state = State.Probe;
                final int stateVersion = this.version;
                // 递增请求序列
                final int seq = getAndIncrementReqSeq();
                // 向目标节点发送 AppendEntries 请求
                final Future<Message> rpcFuture = this.rpcService.appendEntries(
                        this.options.getPeerId().getEndpoint(),
                        request,
                        -1,
                        new RpcResponseClosureAdapter<AppendEntriesResponse>() {

                            @Override
                            public void run(final Status status) {
                                // 处理响应
                                onRpcReturned(Replicator.this.id,
                                        RequestType.AppendEntries,
                                        status,
                                        request,
                                        getResponse(),
                                        seq,
                                        stateVersion,
                                        monotonicSendTimeMs);
                            }

                        });

                // 将当前请求标记为 inflight，并记录到 inflight 队列中
                addInflight(RequestType.AppendEntries, this.nextIndex, 0, 0, seq, rpcFuture);
            }
            LOG.debug("Node {} send HeartbeatRequest to {} term {} lastCommittedIndex {}",
                    this.options.getNode().getNodeId(), this.options.getPeerId(), this.options.getTerm(), request.getCommittedIndex());
        } finally {
            this.id.unlock();
        }
    }

    /**
     * 获取指定 logIndex 的 LogEntry 数据，填充到 emb 和 dataBuffer 中
     *
     * @param nextSendingIndex
     * @param offset
     * @param emb
     * @param dataBuffer
     * @return 返回 false 说明已经填满
     */
    boolean prepareEntry(final long nextSendingIndex,
                         final int offset,
                         final RaftOutter.EntryMeta.Builder emb,
                         final RecyclableByteBufferList dataBuffer) {
        // 数据量已经超过阈值
        if (dataBuffer.getCapacity() >= this.raftOptions.getMaxBodySize()) {
            return false;
        }
        // 基于偏移量计算当前处理的 LogEntry 的 logIndex 值
        final long logIndex = nextSendingIndex + offset;
        // 从本地获取对应的 LogEntry 数据
        final LogEntry entry = this.options.getLogManager().getEntry(logIndex);
        if (entry == null) {
            return false;
        }
        // 将 LogEntry 拆分为元数据和数据体分别填充 EntryMeta 和 RecyclableByteBufferList
        emb.setTerm(entry.getId().getTerm());
        // 设置 checksum
        if (entry.hasChecksum()) {
            emb.setChecksum(entry.getChecksum()); // since 1.2.6
        }
        // 设置 LogEntry 类型
        emb.setType(entry.getType());
        if (entry.getPeers() != null) {
            Requires.requireTrue(!entry.getPeers().isEmpty(), "Empty peers at logIndex=%d", logIndex);
            fillMetaPeers(emb, entry);
        } else {
            Requires.requireTrue(entry.getType() != EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION,
                    "Empty peers but is ENTRY_TYPE_CONFIGURATION type at logIndex=%d", logIndex);
        }
        // 设置数据长度
        final int remaining = entry.getData() != null ? entry.getData().remaining() : 0;
        emb.setDataLen(remaining);
        // 填充数据到 dataBuffer
        if (entry.getData() != null) {
            // should slice entry data
            dataBuffer.add(entry.getData().slice());
        }
        return true;
    }

    private void fillMetaPeers(final RaftOutter.EntryMeta.Builder emb, final LogEntry entry) {
        for (final PeerId peer : entry.getPeers()) {
            emb.addPeers(peer.toString());
        }
        if (entry.getOldPeers() != null) {
            for (final PeerId peer : entry.getOldPeers()) {
                emb.addOldPeers(peer.toString());
            }
        }
        if (entry.getLearners() != null) {
            for (final PeerId peer : entry.getLearners()) {
                emb.addLearners(peer.toString());
            }
        }
        if (entry.getOldLearners() != null) {
            for (final PeerId peer : entry.getOldLearners()) {
                emb.addOldLearners(peer.toString());
            }
        }
    }

    /**
     * 创建并启动到目标节点的复制器
     *
     * @param opts
     * @param raftOptions
     * @return
     */
    public static ThreadId start(final ReplicatorOptions opts, final RaftOptions raftOptions) {
        if (opts.getLogManager() == null || opts.getBallotBox() == null || opts.getNode() == null) {
            throw new IllegalArgumentException("Invalid ReplicatorOptions.");
        }
        // 创建复制器 Replicator 对象
        final Replicator r = new Replicator(opts, raftOptions);
        // 检查到目标节点的连通性
        if (!r.rpcService.connect(opts.getPeerId().getEndpoint())) {
            LOG.error("Fail to init sending channel to {}.", opts.getPeerId());
            // Return and it will be retried later.
            return null;
        }

        // Register replicator metric set.
        final MetricRegistry metricRegistry = opts.getNode().getNodeMetrics().getMetricRegistry();
        if (metricRegistry != null) {
            try {
                final String replicatorMetricName = getReplicatorMetricName(opts);
                if (!metricRegistry.getNames().contains(replicatorMetricName)) {
                    metricRegistry.register(replicatorMetricName, new ReplicatorMetricSet(opts, r));
                }
            } catch (final IllegalArgumentException e) {
                // ignore
            }
        }

        // Start replication
        r.id = new ThreadId(r, r);
        // 获取与当前 Replicator 绑定的不可重入锁
        r.id.lock();
        // 发布 CREATED 事件
        notifyReplicatorStatusListener(r, ReplicatorEvent.CREATED);
        LOG.info("Replicator={}@{} is started", r.id, r.options.getPeerId());
        r.catchUpClosure = null;
        // 更新最近一次发送 RPC 请求的时间戳
        r.lastRpcSendTimestamp = Utils.monotonicMs();
        // 启动心跳超时计时器
        r.startHeartbeatTimer(Utils.nowMs());
        // 发送探针请求，以获取接下去发往目标节点的正确 logIndex 位置，并启动日志复制进程
        // id.unlock in sendEmptyEntries
        r.sendEmptyEntries(false);
        return r.id;
    }

    private static String getReplicatorMetricName(final ReplicatorOptions opts) {
        return "replicator-" + opts.getNode().getGroupId() + "/" + opts.getPeerId();
    }

    public static void waitForCaughtUp(final ThreadId id, final long maxMargin, final long dueTime,
                                       final CatchUpClosure done) {
        final Replicator r = (Replicator) id.lock();

        if (r == null) {
            RpcUtils.runClosureInThread(done, new Status(RaftError.EINVAL, "No such replicator"));
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
        return "Replicator [state=" + this.state + ", statInfo=" + this.statInfo + ", peerId="
                + this.options.getPeerId() + ", type=" + this.options.getReplicatorType() + "]";
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
        // 当前 Replicator 已被销毁
        if (id == null) {
            // It was destroyed already
            return true;
        }
        final Replicator r = (Replicator) id.lock();
        if (r == null) {
            return false;
        }
        r.waitId = -1;
        // 超时，重新发送探针请求
        if (errCode == RaftError.ETIMEDOUT.getNumber()) {
            r.blockTimer = null;
            // Send empty entries after block timeout to check the correct
            // _next_index otherwise the replicator is likely waits in executor.shutdown();
            // _wait_more_entries and no further logs would be replicated even if the
            // last_index of this followers is less than |next_index - 1|
            r.sendEmptyEntries(false);
        }
        // LogManager 正常运行，继续尝试向目标 Follower 节点发送数据
        else if (errCode != RaftError.ESTOP.getNumber()) {
            // id is unlock in _send_entries
            r.sendEntries();
        }
        // LogManager 被停止，停止向目标节点发送日志数据
        else {
            LOG.warn("Replicator {} stops sending entries.", id);
            id.unlock();
        }
        return true;
    }

    static void onBlockTimeout(final ThreadId arg) {
        RpcUtils.runInThread(() -> onBlockTimeoutInNewThread(arg));
    }

    void block(final long startTimeMs, @SuppressWarnings("unused") final int errorCode) {
        // TODO: Currently we don't care about error_code which indicates why the
        // very RPC fails. To make it better there should be different timeout for
        // each individual error (e.g. we don't need check every
        // heartbeat_timeout_ms whether a dead follower has come back), but it's just
        // fine now.
        if (this.blockTimer != null) {
            // already in blocking state,return immediately.
            this.id.unlock();
            return;
        }
        final long dueTime = startTimeMs + this.options.getDynamicHeartBeatTimeoutMs();
        try {
            LOG.debug("Blocking {} for {} ms", this.options.getPeerId(), this.options.getDynamicHeartBeatTimeoutMs());
            this.blockTimer = this.timerManager.schedule(() -> onBlockTimeout(this.id), dueTime - Utils.nowMs(), TimeUnit.MILLISECONDS);
            this.statInfo.runningState = RunningState.BLOCKING;
            this.id.unlock();
        } catch (final Exception e) {
            this.blockTimer = null;
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
            RpcUtils.runInThread(() -> sendHeartbeat(id));
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
        RpcUtils.runClosureInThread(savedClosure, savedClosure.getStatus());
    }

    private static void onTimeout(final ThreadId id) {
        if (id != null) {
            // ETIMEDOUT 错误会触发再次向目标节点发送心跳请求
            id.setError(RaftError.ETIMEDOUT.getNumber());
        } else {
            LOG.warn("Replicator id is null when timeout, maybe it's destroyed.");
        }
    }

    void destroy() {
        final ThreadId savedId = this.id;
        LOG.info("Replicator {} is going to quit", savedId);
        releaseReader();
        // Unregister replicator metric set
        if (this.nodeMetrics.isEnabled()) {
            this.nodeMetrics.getMetricRegistry() //
                    .removeMatching(MetricFilter.startsWith(getReplicatorMetricName(this.options)));
        }
        this.state = State.Destroyed;
        notifyReplicatorStatusListener((Replicator) savedId.getData(), ReplicatorEvent.DESTROYED);
        savedId.unlockAndDestroy();
        this.id = null;
    }

    private void releaseReader() {
        if (this.reader != null) {
            Utils.closeQuietly(this.reader);
            this.reader = null;
        }
    }

    static void onHeartbeatReturned(final ThreadId id,
                                    final Status status,
                                    final AppendEntriesRequest request,
                                    final AppendEntriesResponse response,
                                    final long rpcSendTime) {
        // 复制器已经被销毁
        if (id == null) {
            // replicator already was destroyed.
            return;
        }
        final long startTimeMs = Utils.nowMs();
        Replicator r;
        if ((r = (Replicator) id.lock()) == null) {
            return;
        }
        boolean doUnlock = true;
        try {
            final boolean isLogDebugEnabled = LOG.isDebugEnabled();
            StringBuilder sb = null;
            if (isLogDebugEnabled) {
                sb = new StringBuilder("Node ") //
                        .append(r.options.getGroupId()) //
                        .append(':') //
                        .append(r.options.getServerId()) //
                        .append(" received HeartbeatResponse from ") //
                        .append(r.options.getPeerId()) //
                        .append(" prevLogIndex=") //
                        .append(request.getPrevLogIndex()) //
                        .append(" prevLogTerm=") //
                        .append(request.getPrevLogTerm());
            }
            // Follower 节点运行异常
            if (!status.isOk()) {
                if (isLogDebugEnabled) {
                    sb.append(" fail, sleep, status=").append(status);
                    LOG.debug(sb.toString());
                }
                r.state = State.Probe;
                notifyReplicatorStatusListener(r, ReplicatorEvent.ERROR, status);
                if (++r.consecutiveErrorTimes % 10 == 0) {
                    LOG.warn("Fail to issue RPC to {}, consecutiveErrorTimes={}, error={}",
                            r.options.getPeerId(), r.consecutiveErrorTimes, status);
                }
                // 重新启动心跳计时器
                r.startHeartbeatTimer(startTimeMs);
                return;
            }
            r.consecutiveErrorTimes = 0;
            // 目标 Follower 节点的 term 值更大，说明有新的 Leader 节点
            if (response.getTerm() > r.options.getTerm()) {
                if (isLogDebugEnabled) {
                    sb.append(" fail, greater term ") //
                            .append(response.getTerm()) //
                            .append(" expect term ") //
                            .append(r.options.getTerm());
                    LOG.debug(sb.toString());
                }
                final NodeImpl node = r.options.getNode();
                r.notifyOnCaughtUp(RaftError.EPERM.getNumber(), true);
                // 销毁当前复制器
                r.destroy();
                // 递增当前节点的 term 值
                node.increaseTermTo(response.getTerm(), new Status(RaftError.EHIGHERTERMRESPONSE,
                        "Leader receives higher term heartbeat_response from peer:%s", r.options.getPeerId()));
                return;
            }
            // Follower 节点拒绝响应，重新发送探针请求，并启动心跳计时器
            if (!response.getSuccess() && response.hasLastLogIndex()) {
                if (isLogDebugEnabled) {
                    sb.append(" fail, response term ") //
                            .append(response.getTerm()) //
                            .append(" lastLogIndex ") //
                            .append(response.getLastLogIndex());
                    LOG.debug(sb.toString());
                }
                LOG.warn("Heartbeat to peer {} failure, try to send a probe request.", r.options.getPeerId());
                doUnlock = false;
                r.sendEmptyEntries(false);
                r.startHeartbeatTimer(startTimeMs);
                return;
            }
            if (isLogDebugEnabled) {
                LOG.debug(sb.toString());
            }
            // 更新 RPC 请求时间戳
            if (rpcSendTime > r.lastRpcSendTimestamp) {
                r.lastRpcSendTimestamp = rpcSendTime;
            }
            // 启动心跳计时器
            r.startHeartbeatTimer(startTimeMs);
        } finally {
            if (doUnlock) {
                id.unlock();
            }
        }
    }

    /**
     * 处理 AppendEntries 响应
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
    static void onRpcReturned(final ThreadId id,
                              final RequestType reqType,
                              final Status status,
                              final Message request,
                              final Message response,
                              final int seq,
                              final int stateVersion,
                              final long rpcSendTime) {
        if (id == null) {
            return;
        }
        final long startTimeMs = Utils.nowMs();
        Replicator r;
        // 获取当前 Replicator 对应的不可重入锁
        if ((r = (Replicator) id.lock()) == null) {
            return;
        }

        // 状态版本发生变化，说明 inflight 队列被重置过，忽略重置之前请求对应的响应
        if (stateVersion != r.version) {
            LOG.debug("Replicator {} ignored old version response {}, current version is {}, request is {}\n, and response is {}\n, status is {}.",
                    r, stateVersion, r.version, request, response, status);
            id.unlock();
            return;
        }

        // 获取等待处理的响应优先级队列，按照请求序列从小到大排序
        final PriorityQueue<RpcResponse> holdingQueue = r.pendingResponses;
        holdingQueue.add(new RpcResponse(reqType, seq, status, request, response, rpcSendTime));

        // 太多等待处理的响应（默认为 256 个），而期望请求序列对应的响应迟迟不来，重置请求 inflight 队列，重新发送探针请求
        if (holdingQueue.size() > r.raftOptions.getMaxReplicatorInflightMsgs()) {
            LOG.warn("Too many pending responses {} for replicator {}, maxReplicatorInflightMsgs={}",
                    holdingQueue.size(), r.options.getPeerId(), r.raftOptions.getMaxReplicatorInflightMsgs());
            r.resetInflights();
            r.state = State.Probe;
            r.sendEmptyEntries(false);
            return;
        }

        // 标识是否继续发送 AppendEntries 请求
        boolean continueSendEntries = false;

        final boolean isLogDebugEnabled = LOG.isDebugEnabled();
        StringBuilder sb = null;
        if (isLogDebugEnabled) {
            sb = new StringBuilder("Replicator ").append(r).append(" is processing RPC responses, ");
        }
        try {
            // 记录已经处理的响应数
            int processed = 0;
            // 遍历处理响应
            while (!holdingQueue.isEmpty()) {
                // 获取收到的请求序列最小的响应
                final RpcResponse queuedPipelinedResponse = holdingQueue.peek();

                // Sequence mismatch, waiting for next response.
                // 响应乱序，继续等待期望序列的响应
                if (queuedPipelinedResponse.seq != r.requiredNextSeq) {
                    if (processed > 0) {
                        if (isLogDebugEnabled) {
                            sb.append("has processed ").append(processed).append(" responses, ");
                        }
                        break;
                    } else {
                        // Do not processed any responses, UNLOCK id and return.
                        continueSendEntries = false;
                        id.unlock();
                        return;
                    }
                }

                /* 开始处理请求对应的响应 */

                holdingQueue.remove();
                processed++;
                // 获取 inflight 请求
                final Inflight inflight = r.pollInflight();
                if (inflight == null) {
                    // 响应对应的请求已经被清除，忽略当前响应
                    if (isLogDebugEnabled) {
                        sb.append("ignore response because request not found: ").append(queuedPipelinedResponse).append(",\n");
                    }
                    continue;
                }
                // 请求序列与响应中记录的请求序列匹配不上，重置请求 inflight 队列，阻塞一会后重新发送探针请求
                if (inflight.seq != queuedPipelinedResponse.seq) {
                    // reset state
                    LOG.warn("Replicator {} response sequence out of order, expect {}, but it is {}, reset state to try again.",
                            r, inflight.seq, queuedPipelinedResponse.seq);
                    r.resetInflights();
                    r.state = State.Probe;
                    continueSendEntries = false;
                    r.block(Utils.nowMs(), RaftError.EREQUEST.getNumber());
                    return;
                }

                // 依据响应类型分别处理
                try {
                    switch (queuedPipelinedResponse.requestType) {
                        // 处理 AppendEntries 请求
                        case AppendEntries:
                            continueSendEntries = onAppendEntriesReturned(
                                    id,
                                    inflight,
                                    queuedPipelinedResponse.status,
                                    (AppendEntriesRequest) queuedPipelinedResponse.request,
                                    (AppendEntriesResponse) queuedPipelinedResponse.response,
                                    rpcSendTime,
                                    startTimeMs,
                                    r);
                            break;
                        // 处理 InstallSnapshot 请求
                        case Snapshot:
                            continueSendEntries = onInstallSnapshotReturned(
                                    id,
                                    r,
                                    queuedPipelinedResponse.status,
                                    (InstallSnapshotRequest) queuedPipelinedResponse.request,
                                    (InstallSnapshotResponse) queuedPipelinedResponse.response);
                            break;
                    }
                } finally {
                    if (continueSendEntries) {
                        // Success, increase the response sequence.
                        r.getAndIncrementRequiredNextSeq();
                    } else {
                        // The id is already unlocked in onAppendEntriesReturned/onInstallSnapshotReturned, we SHOULD break out.
                        break;
                    }
                }
            }
        } finally {
            if (isLogDebugEnabled) {
                sb.append("after processed, continue to send entries: ").append(continueSendEntries);
                LOG.debug(sb.toString());
            }
            // 继续发送 AppendEntries 请求
            if (continueSendEntries) {
                // unlock in sendEntries.
                r.sendEntries();
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
     * 处理 AppendEntries 请求响应
     *
     * @param id
     * @param inflight
     * @param status
     * @param request
     * @param response
     * @param rpcSendTime
     * @param startTimeMs
     * @param r
     * @return
     */
    private static boolean onAppendEntriesReturned(final ThreadId id,
                                                   final Inflight inflight,
                                                   final Status status,
                                                   final AppendEntriesRequest request,
                                                   final AppendEntriesResponse response,
                                                   final long rpcSendTime,
                                                   final long startTimeMs,
                                                   final Replicator r) {
        // inflight 请求与响应中记录的请求对应的 logIndex 不匹配，重置请求 inflight 队列，重新发送探针请求
        if (inflight.startIndex != request.getPrevLogIndex() + 1) {
            LOG.warn("Replicator {} received invalid AppendEntriesResponse, in-flight startIndex={}, request prevLogIndex={}, reset the replicator state and probe again.",
                    r, inflight.startIndex, request.getPrevLogIndex());
            r.resetInflights();
            r.state = State.Probe;
            // unlock id in sendEmptyEntries
            r.sendEmptyEntries(false);
            return false;
        }
        // record metrics
        if (request.getEntriesCount() > 0) {
            r.nodeMetrics.recordLatency("replicate-entries", Utils.monotonicMs() - rpcSendTime);
            r.nodeMetrics.recordSize("replicate-entries-count", request.getEntriesCount());
            r.nodeMetrics.recordSize("replicate-entries-bytes", request.getData() != null ? request.getData().size() : 0);
        }

        final boolean isLogDebugEnabled = LOG.isDebugEnabled();
        StringBuilder sb = null;
        if (isLogDebugEnabled) {
            sb = new StringBuilder("Node ") //
                    .append(r.options.getGroupId()) //
                    .append(':') //
                    .append(r.options.getServerId()) //
                    .append(" received AppendEntriesResponse from ") //
                    .append(r.options.getPeerId()) //
                    .append(" prevLogIndex=") //
                    .append(request.getPrevLogIndex()) //
                    .append(" prevLogTerm=") //
                    .append(request.getPrevLogTerm()) //
                    .append(" count=") //
                    .append(request.getEntriesCount());
        }

        // 目标 Follower 发生错误，重置请求 inflight 队列，重新发送探针请求
        if (!status.isOk()) {
            // If the follower crashes, any RPC to the follower fails immediately,
            // so we need to block the follower for a while instead of looping until it comes back or be removed
            // dummy_id is unlock in block
            if (isLogDebugEnabled) {
                sb.append(" fail, sleep, status=").append(status);
                LOG.debug(sb.toString());
            }
            notifyReplicatorStatusListener(r, ReplicatorEvent.ERROR, status);
            if (++r.consecutiveErrorTimes % 10 == 0) {
                LOG.warn("Fail to issue RPC to {}, consecutiveErrorTimes={}, error={}",
                        r.options.getPeerId(), r.consecutiveErrorTimes, status);
            }
            // 重置 inflight 队列，阻塞一会儿重新发送探针请求
            r.resetInflights();
            r.state = State.Probe;
            // unlock in in block
            r.block(startTimeMs, status.getCode());
            return false;
        }

        /* 目标 Follower 节点运行正常 */

        r.consecutiveErrorTimes = 0;
        // 目标 Follower 节点拒绝响应
        if (!response.getSuccess()) {
            // Follower 节点的 term 值更大
            if (response.getTerm() > r.options.getTerm()) {
                if (isLogDebugEnabled) {
                    sb.append(" fail, greater term ").append(response.getTerm()).append(" expect term ").append(r.options.getTerm());
                    LOG.debug(sb.toString());
                }
                final NodeImpl node = r.options.getNode();
                r.notifyOnCaughtUp(RaftError.EPERM.getNumber(), true);
                // 销毁当前复制器 Replicator
                r.destroy();
                // 提升当前节点的 term 值，并执行 stepdown
                node.increaseTermTo(response.getTerm(), new Status(RaftError.EHIGHERTERMRESPONSE,
                        "Leader receives higher term heartbeat_response from peer:%s", r.options.getPeerId()));
                return false;
            }
            if (isLogDebugEnabled) {
                sb.append(" fail, find nextIndex remote lastLogIndex ")
                        .append(response.getLastLogIndex())
                        .append(" local nextIndex ").append(r.nextIndex);
                LOG.debug(sb.toString());
            }
            // 更新最近一次向目标节点发送 RPC 请求的时间戳
            if (rpcSendTime > r.lastRpcSendTimestamp) {
                r.lastRpcSendTimestamp = rpcSendTime;
            }
            // 重置 inflight 队列，调整 nextIndex 之后重新发送探针请求
            r.resetInflights();
            // prev_log_index and prev_log_term doesn't match
            if (response.getLastLogIndex() + 1 < r.nextIndex) {
                LOG.debug("LastLogIndex at peer={} is {}", r.options.getPeerId(), response.getLastLogIndex());
                // The peer contains less logs than leader
                r.nextIndex = response.getLastLogIndex() + 1;
            }
            // Follower 节点本地的 logIndex 更大，可能包含老的 Leader 节点复制的日志，
            // 递减 nextIndex 之后重试，直到找到两个节点相同日志的交叉点为止
            else {
                // The peer contains logs from old term which should be truncated,
                // decrease _last_log_at_peer by one to test the right index to keep
                if (r.nextIndex > 1) {
                    LOG.debug("logIndex={} dismatch", r.nextIndex);
                    r.nextIndex--;
                } else {
                    LOG.error("Peer={} declares that log at index=0 doesn't match, which is not supposed to happen", r.options.getPeerId());
                }
            }
            // dummy_id is unlock in _send_heartbeat
            // 重新发送探针请求
            r.sendEmptyEntries(false);
            return false;
        }
        if (isLogDebugEnabled) {
            sb.append(", success");
            LOG.debug(sb.toString());
        }

        /* 目标 Follower 节点响应成功 */

        // 请求期间 term 值已经发生变化，当前节点可能已经不是 Leader 节点，清空 inflight 队列
        if (response.getTerm() != r.options.getTerm()) {
            r.resetInflights();
            r.state = State.Probe;
            LOG.error("Fail, response term {} dismatch, expect term {}", response.getTerm(), r.options.getTerm());
            id.unlock();
            return false;
        }
        // 更新最近一次向目标节点发送 RPC 请求的时间戳
        if (rpcSendTime > r.lastRpcSendTimestamp) {
            r.lastRpcSendTimestamp = rpcSendTime;
        }
        final int entriesSize = request.getEntriesCount();
        // 如果是复制日志请求，当 Follower 节点复制成功之后需要尝试执行 BallotBox#commitAt 以检测当前日志是否被过半数的节点成功复制
        if (entriesSize > 0) {
            if (r.options.getReplicatorType().isFollower()) {
                // Only commit index when the response is from follower.
                r.options.getBallotBox().commitAt(r.nextIndex, r.nextIndex + entriesSize - 1, r.options.getPeerId());
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Replicated logs in [{}, {}] to peer {}", r.nextIndex, r.nextIndex + entriesSize - 1, r.options.getPeerId());
            }
        }

        r.state = State.Replicate;
        r.blockTimer = null;
        // 更新待发送的下一个 logIndex 位置
        r.nextIndex += entriesSize;
        r.hasSucceeded = true;
        // 回调 CatchUpClosure
        r.notifyOnCaughtUp(RaftError.SUCCESS.getNumber(), false);
        // dummy_id is unlock in _send_entries
        if (r.timeoutNowIndex > 0 && r.timeoutNowIndex < r.nextIndex) {
            r.sendTimeoutNow(false, false);
        }
        return true;
    }

    /**
     * 为 AppendEntries 请求填充基础参数
     *
     * @param rb
     * @param prevLogIndex
     * @param isHeartbeat
     * @return
     */
    private boolean fillCommonFields(final AppendEntriesRequest.Builder rb, long prevLogIndex, final boolean isHeartbeat) {
        // 获取 prevLogIndex 对应的 term 值
        final long prevLogTerm = this.options.getLogManager().getTerm(prevLogIndex);
        if (prevLogTerm == 0 && prevLogIndex != 0) {
            if (!isHeartbeat) {
                Requires.requireTrue(prevLogIndex < this.options.getLogManager().getFirstLogIndex());
                LOG.debug("logIndex={} was compacted", prevLogIndex);
                return false;
            } else {
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
            // 已经设置过等待
            if (this.waitId >= 0) {
                return;
            }
            // 设置一个回调，当有可复制日志时触发再次往目标 Follower 节点发送数据
            this.waitId = this.options.getLogManager().wait(
                    nextWaitIndex - 1,
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
                // 获取下一个待发送的 LogEntry 对应的 logIndex 值，如果返回 -1 表示暂停复制
                final long nextSendingIndex = getNextSendIndex();
                if (nextSendingIndex > prevSendIndex) {
                    // 向目标节点复制 nextSendingIndex 位置之后的 LogEntry 数据
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
     * @param nextSendingIndex next sending index
     * @return send result.
     */
    private boolean sendEntries(final long nextSendingIndex) {
        // 构建 AppendEntries 请求
        final AppendEntriesRequest.Builder rb = AppendEntriesRequest.newBuilder();
        // 为 AppendEntries 请求填充基础参数，包括当前节点的 term 值、groupId、节点 ID，以及 committedLogIndex 等等
        // 如果返回 false 说明待发送的部分日志已经变为快照，需要先给目标节点安装快照
        if (!fillCommonFields(rb, nextSendingIndex - 1, false)) {
            // unlock id in installSnapshot
            installSnapshot();
            return false;
        }

        ByteBufferCollector dataBuf = null;
        // 获取单次批量发送 LogEntry 的数目上线
        final int maxEntriesSize = this.raftOptions.getMaxEntriesSize();
        final RecyclableByteBufferList byteBufList = RecyclableByteBufferList.newInstance();
        try {
            for (int i = 0; i < maxEntriesSize; i++) {
                final RaftOutter.EntryMeta.Builder emb = RaftOutter.EntryMeta.newBuilder();
                // 获取指定 logIndex 的 LogEntry 数据，填充到 emb 和 byteBufList 中，
                // 如果返回 false 说明容量已满
                if (!prepareEntry(nextSendingIndex, i, emb, byteBufList)) {
                    break;
                }
                rb.addEntries(emb.build());
            }

            // 未获取到任何 LogEntry 数据，可能目标数据已经变为快照了，也可能是真的没有数据可以复制
            if (rb.getEntriesCount() == 0) {
                // nextSendingIndex < firstLogIndex，说明对应区间的数据已变为快照，需要先给目标节点安装快照
                if (nextSendingIndex < this.options.getLogManager().getFirstLogIndex()) {
                    installSnapshot();
                    return false;
                }
                // 说明没有新的数据可以复制，设置一个回调等待新的数据到来之后重新触发 sendEntries 操作
                waitMoreEntries(nextSendingIndex);
                return false;
            }

            // 将日志数据填充到 AppendEntries 请求中
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
            RecycleUtil.recycle(byteBufList);
        }

        final AppendEntriesRequest request = rb.build();
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Node {} send AppendEntriesRequest to {} term {} lastCommittedIndex {} prevLogIndex {} prevLogTerm {} logIndex {} count {}",
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
        // 递增请求序列
        final int seq = getAndIncrementReqSeq();

        Future<Message> rpcFuture = null;
        try {
            // 向目标节点发送 AppendEntries RPC 请求
            rpcFuture = this.rpcService.appendEntries(
                    this.options.getPeerId().getEndpoint(),
                    request,
                    -1,
                    new RpcResponseClosureAdapter<AppendEntriesResponse>() {

                        @Override
                        public void run(final Status status) {
                            RecycleUtil.recycle(recyclable); // TODO: recycle on send success, not response received.
                            onRpcReturned(Replicator.this.id, RequestType.AppendEntries, status, request, getResponse(), seq, v, monotonicSendTimeMs);
                        }
                    });
        } catch (final Throwable t) {
            RecycleUtil.recycle(recyclable);
            ThrowUtil.throwException(t);
        }
        // 将本次请求标记为 inflight
        addInflight(RequestType.AppendEntries, nextSendingIndex, request.getEntriesCount(), request.getData().size(), seq, rpcFuture);

        return true;
    }

    public static void sendHeartbeat(final ThreadId id, final RpcResponseClosure<AppendEntriesResponse> closure) {
        final Replicator r = (Replicator) id.lock();
        if (r == null) {
            RpcUtils.runClosureInThread(closure, new Status(RaftError.EHOSTDOWN, "Peer %s is not connected", id));
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
        // 向目标 Follower 节点发送心跳请求
        r.sendEmptyEntries(true);
    }

    @SuppressWarnings("SameParameterValue")
    private void sendTimeoutNow(final boolean unlockId, final boolean stopAfterFinish) {
        sendTimeoutNow(unlockId, stopAfterFinish, -1);
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
            notifyReplicatorStatusListener(r, ReplicatorEvent.ERROR, status);
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
            sendTimeoutNow(true, false);
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

}
