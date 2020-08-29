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

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.FSMCaller;
import com.alipay.sofa.jraft.JRaftServiceFactory;
import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.NodeManager;
import com.alipay.sofa.jraft.ReadOnlyService;
import com.alipay.sofa.jraft.ReplicatorGroup;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.CatchUpClosure;
import com.alipay.sofa.jraft.closure.ClosureQueue;
import com.alipay.sofa.jraft.closure.ClosureQueueImpl;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.closure.SynchronizedClosure;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.conf.ConfigurationEntry;
import com.alipay.sofa.jraft.conf.ConfigurationManager;
import com.alipay.sofa.jraft.entity.Ballot;
import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.entity.LeaderChangeContext;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.entity.NodeId;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.RaftOutter;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.entity.UserLog;
import com.alipay.sofa.jraft.error.LogIndexOutOfBoundsException;
import com.alipay.sofa.jraft.error.LogNotFoundException;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.option.BallotBoxOptions;
import com.alipay.sofa.jraft.option.BootstrapOptions;
import com.alipay.sofa.jraft.option.FSMCallerOptions;
import com.alipay.sofa.jraft.option.LogManagerOptions;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.option.RaftMetaStorageOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.option.ReadOnlyOption;
import com.alipay.sofa.jraft.option.ReadOnlyServiceOptions;
import com.alipay.sofa.jraft.option.ReplicatorGroupOptions;
import com.alipay.sofa.jraft.option.SnapshotExecutorOptions;
import com.alipay.sofa.jraft.rpc.RaftClientService;
import com.alipay.sofa.jraft.rpc.RaftServerService;
import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.alipay.sofa.jraft.rpc.RpcRequests.AppendEntriesRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.AppendEntriesResponse;
import com.alipay.sofa.jraft.rpc.RpcRequests.InstallSnapshotRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.InstallSnapshotResponse;
import com.alipay.sofa.jraft.rpc.RpcRequests.ReadIndexRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.ReadIndexResponse;
import com.alipay.sofa.jraft.rpc.RpcRequests.RequestVoteRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.RequestVoteResponse;
import com.alipay.sofa.jraft.rpc.RpcRequests.TimeoutNowRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.TimeoutNowResponse;
import com.alipay.sofa.jraft.rpc.RpcResponseClosure;
import com.alipay.sofa.jraft.rpc.RpcResponseClosureAdapter;
import com.alipay.sofa.jraft.rpc.impl.core.DefaultRaftClientService;
import com.alipay.sofa.jraft.storage.LogManager;
import com.alipay.sofa.jraft.storage.LogStorage;
import com.alipay.sofa.jraft.storage.RaftMetaStorage;
import com.alipay.sofa.jraft.storage.SnapshotExecutor;
import com.alipay.sofa.jraft.storage.impl.LogManagerImpl;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotExecutorImpl;
import com.alipay.sofa.jraft.util.Describer;
import com.alipay.sofa.jraft.util.DisruptorBuilder;
import com.alipay.sofa.jraft.util.DisruptorMetricSet;
import com.alipay.sofa.jraft.util.JRaftServiceLoader;
import com.alipay.sofa.jraft.util.JRaftSignalHandler;
import com.alipay.sofa.jraft.util.LogExceptionHandler;
import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.alipay.sofa.jraft.util.OnlyForTest;
import com.alipay.sofa.jraft.util.Platform;
import com.alipay.sofa.jraft.util.RepeatedTimer;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.RpcFactoryHelper;
import com.alipay.sofa.jraft.util.SignalHelper;
import com.alipay.sofa.jraft.util.ThreadHelper;
import com.alipay.sofa.jraft.util.ThreadId;
import com.alipay.sofa.jraft.util.Utils;
import com.alipay.sofa.jraft.util.timer.RaftTimerFactory;
import com.google.protobuf.Message;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The raft replica node implementation.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-03 4:26:51 PM
 */
public class NodeImpl implements Node, RaftServerService {

    private static final Logger LOG = LoggerFactory
            .getLogger(NodeImpl.class);

    static {
        try {
            if (SignalHelper.supportSignal()) {
                // TODO support windows signal
                if (!Platform.isWindows()) {
                    final List<JRaftSignalHandler> handlers = JRaftServiceLoader.load(JRaftSignalHandler.class) //
                            .sort();
                    SignalHelper.addSignal(SignalHelper.SIG_USR2, handlers);
                }
            }
        } catch (final Throwable t) {
            LOG.error("Fail to add signal.", t);
        }
    }

    public final static RaftTimerFactory TIMER_FACTORY = JRaftUtils.raftTimerFactory();

    // Max retry times when applying tasks.
    private static final int MAX_APPLY_RETRY_TIMES = 3;

    public static final AtomicInteger GLOBAL_NUM_NODES = new AtomicInteger(0);

    /** Internal states */
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    protected final Lock writeLock = this.readWriteLock
            .writeLock();
    protected final Lock readLock = this.readWriteLock
            .readLock();
    private volatile State state;
    private volatile CountDownLatch shutdownLatch;
    private long currTerm;
    /**
     * 对于 Follower 节点而言，记录最近一次收到来自 leader 节点请求的时间戳，
     * 对于 Leader 节点而言，记录最近一次检查当前 Leader 节点是否有效的时间戳（向所有活跃节点发送 RPC 请求的最小时间戳）
     */
    private volatile long lastLeaderTimestamp;
    private PeerId leaderId = new PeerId();
    private PeerId votedId;
    private final Ballot voteCtx = new Ballot();
    private final Ballot prevVoteCtx = new Ballot();
    private ConfigurationEntry conf;
    private StopTransferArg stopTransferArg;
    /** Raft group and node options and identifier */
    private final String groupId;
    private NodeOptions options;
    private RaftOptions raftOptions;
    private final PeerId serverId;
    /** Other services */
    private final ConfigurationCtx confCtx;
    private LogStorage logStorage;
    private RaftMetaStorage metaStorage;
    private ClosureQueue closureQueue;
    private ConfigurationManager configManager;
    private LogManager logManager;
    private FSMCaller fsmCaller;
    private BallotBox ballotBox;
    private SnapshotExecutor snapshotExecutor;
    private ReplicatorGroup replicatorGroup;
    private final List<Closure> shutdownContinuations = new ArrayList<>();
    private RaftClientService rpcService;
    private ReadOnlyService readOnlyService;
    /** Timers */
    private Scheduler timerManager;
    /** 预选举计时器 */
    private RepeatedTimer electionTimer;
    private RepeatedTimer voteTimer;
    private RepeatedTimer stepDownTimer;
    private RepeatedTimer snapshotTimer;
    private ScheduledFuture<?> transferTimer;
    private ThreadId wakingCandidate;
    /** Disruptor to run node service */
    private Disruptor<LogEntryAndClosure> applyDisruptor;
    private RingBuffer<LogEntryAndClosure> applyQueue;

    /** Metrics */
    private NodeMetrics metrics;

    private NodeId nodeId;
    private JRaftServiceFactory serviceFactory;

    /** ReplicatorStateListeners */
    private final CopyOnWriteArrayList<Replicator.ReplicatorStateListener> replicatorStateListeners = new CopyOnWriteArrayList<>();
    /** Node's target leader election priority value */
    private volatile int targetPriority; // 目标优先级，用于控制当前节点是否继续发起预选举
    /** The number of elections time out for current node */
    private volatile int electionTimeoutCounter;

    /**
     * Node service event.
     *
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-03 4:29:55 PM
     */
    private static class LogEntryAndClosure {
        LogEntry entry;
        Closure done;
        long expectedTerm;
        CountDownLatch shutdownLatch;

        public void reset() {
            this.entry = null;
            this.done = null;
            this.expectedTerm = 0;
            this.shutdownLatch = null;
        }
    }

    private static class LogEntryAndClosureFactory implements EventFactory<LogEntryAndClosure> {

        @Override
        public LogEntryAndClosure newInstance() {
            return new LogEntryAndClosure();
        }
    }

    /**
     * Event handler.
     *
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-03 4:30:07 PM
     */
    private class LogEntryAndClosureHandler implements EventHandler<LogEntryAndClosure> {
        // task list for batch
        private final List<LogEntryAndClosure> tasks = new ArrayList<>(NodeImpl.this.raftOptions.getApplyBatch());

        @Override
        public void onEvent(final LogEntryAndClosure event, final long sequence, final boolean endOfBatch)
                throws Exception {
            if (event.shutdownLatch != null) {
                if (!this.tasks.isEmpty()) {
                    executeApplyingTasks(this.tasks);
                    this.tasks.clear();
                }
                final int num = GLOBAL_NUM_NODES.decrementAndGet();
                LOG.info("The number of active nodes decrement to {}.", num);
                event.shutdownLatch.countDown();
                return;
            }

            this.tasks.add(event);
            if (this.tasks.size() >= NodeImpl.this.raftOptions.getApplyBatch() || endOfBatch) {
                executeApplyingTasks(this.tasks);
                this.tasks.clear();
            }
        }
    }

    /**
     * Configuration commit context.
     *
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-03 4:29:38 PM
     */
    private static class ConfigurationCtx {
        enum Stage {
            STAGE_NONE, // none stage
            STAGE_CATCHING_UP, // the node is catching-up
            STAGE_JOINT, // joint stage
            STAGE_STABLE // stable stage
        }

        final NodeImpl node;
        Stage stage;
        // Peers change times
        int nchanges;
        long version;
        // peers
        List<PeerId> newPeers = new ArrayList<>();
        List<PeerId> oldPeers = new ArrayList<>();
        List<PeerId> addingPeers = new ArrayList<>();
        // learners
        List<PeerId> newLearners = new ArrayList<>();
        List<PeerId> oldLearners = new ArrayList<>();
        Closure done;

        public ConfigurationCtx(final NodeImpl node) {
            super();
            this.node = node;
            this.stage = Stage.STAGE_NONE;
            this.version = 0;
            this.done = null;
        }

        /**
         * Start change configuration.
         */
        void start(final Configuration oldConf, final Configuration newConf, final Closure done) {
            if (isBusy()) {
                if (done != null) {
                    Utils.runClosureInThread(done, new Status(RaftError.EBUSY, "Already in busy stage."));
                }
                throw new IllegalStateException("Busy stage");
            }
            if (this.done != null) {
                if (done != null) {
                    Utils.runClosureInThread(done, new Status(RaftError.EINVAL, "Already have done closure."));
                }
                throw new IllegalArgumentException("Already have done closure");
            }
            this.done = done;
            this.stage = Stage.STAGE_CATCHING_UP;
            this.oldPeers = oldConf.listPeers();
            this.newPeers = newConf.listPeers();
            this.oldLearners = oldConf.listLearners();
            this.newLearners = newConf.listLearners();
            final Configuration adding = new Configuration();
            final Configuration removing = new Configuration();
            newConf.diff(oldConf, adding, removing);
            this.nchanges = adding.size() + removing.size();

            addNewLearners();
            if (adding.isEmpty()) {
                nextStage();
                return;
            }
            addNewPeers(adding);
        }

        private void addNewPeers(final Configuration adding) {
            this.addingPeers = adding.listPeers();
            LOG.info("Adding peers: {}.", this.addingPeers);
            for (final PeerId newPeer : this.addingPeers) {
                if (!this.node.replicatorGroup.addReplicator(newPeer)) {
                    LOG.error("Node {} start the replicator failed, peer={}.", this.node.getNodeId(), newPeer);
                    onCaughtUp(this.version, newPeer, false);
                    return;
                }
                final OnCaughtUp caughtUp = new OnCaughtUp(this.node, this.node.currTerm, newPeer, this.version);
                final long dueTime = Utils.nowMs() + this.node.options.getElectionTimeoutMs();
                if (!this.node.replicatorGroup.waitCaughtUp(newPeer, this.node.options.getCatchupMargin(), dueTime,
                        caughtUp)) {
                    LOG.error("Node {} waitCaughtUp, peer={}.", this.node.getNodeId(), newPeer);
                    onCaughtUp(this.version, newPeer, false);
                    return;
                }
            }
        }

        private void addNewLearners() {
            final Set<PeerId> addingLearners = new HashSet<>(this.newLearners);
            addingLearners.removeAll(this.oldLearners);
            LOG.info("Adding learners: {}.", this.addingPeers);
            for (final PeerId newLearner : addingLearners) {
                if (!this.node.replicatorGroup.addReplicator(newLearner, ReplicatorType.Learner)) {
                    LOG.error("Node {} start the learner replicator failed, peer={}.", this.node.getNodeId(),
                            newLearner);
                }
            }
        }

        void onCaughtUp(final long version, final PeerId peer, final boolean success) {
            if (version != this.version) {
                LOG.warn("Ignore onCaughtUp message, mismatch configuration context version, expect {}, but is {}.",
                        this.version, version);
                return;
            }
            Requires.requireTrue(this.stage == Stage.STAGE_CATCHING_UP, "Stage is not in STAGE_CATCHING_UP");
            if (success) {
                this.addingPeers.remove(peer);
                if (this.addingPeers.isEmpty()) {
                    nextStage();
                    return;
                }
                return;
            }
            LOG.warn("Node {} fail to catch up peer {} when trying to change peers from {} to {}.",
                    this.node.getNodeId(), peer, this.oldPeers, this.newPeers);
            reset(new Status(RaftError.ECATCHUP, "Peer %s failed to catch up.", peer));
        }

        void reset() {
            reset(null);
        }

        void reset(final Status st) {
            if (st != null && st.isOk()) {
                this.node.stopReplicator(this.newPeers, this.oldPeers);
                this.node.stopReplicator(this.newLearners, this.oldLearners);
            } else {
                this.node.stopReplicator(this.oldPeers, this.newPeers);
                this.node.stopReplicator(this.oldLearners, this.newLearners);
            }
            clearPeers();
            clearLearners();

            this.version++;
            this.stage = Stage.STAGE_NONE;
            this.nchanges = 0;
            if (this.done != null) {
                Utils.runClosureInThread(this.done, st != null ? st : new Status(RaftError.EPERM, "Leader stepped down."));
                this.done = null;
            }
        }

        private void clearLearners() {
            this.newLearners.clear();
            this.oldLearners.clear();
        }

        private void clearPeers() {
            this.newPeers.clear();
            this.oldPeers.clear();
            this.addingPeers.clear();
        }

        /**
         * Invoked when this node becomes the leader, write a configuration change log as the first log.
         */
        void flush(final Configuration conf, final Configuration oldConf) {
            Requires.requireTrue(!isBusy(), "Flush when busy");
            this.newPeers = conf.listPeers();
            this.newLearners = conf.listLearners();
            if (oldConf == null || oldConf.isEmpty()) {
                this.stage = Stage.STAGE_STABLE;
                this.oldPeers = this.newPeers;
                this.oldLearners = this.newLearners;
            } else {
                this.stage = Stage.STAGE_JOINT;
                this.oldPeers = oldConf.listPeers();
                this.oldLearners = oldConf.listLearners();
            }
            this.node.unsafeApplyConfiguration(conf, oldConf == null || oldConf.isEmpty() ? null : oldConf, true);
        }

        void nextStage() {
            Requires.requireTrue(isBusy(), "Not in busy stage");
            switch (this.stage) {
                case STAGE_CATCHING_UP:
                    if (this.nchanges > 0) {
                        this.stage = Stage.STAGE_JOINT;
                        this.node.unsafeApplyConfiguration(
                                new Configuration(this.newPeers, this.newLearners),
                                new Configuration(this.oldPeers), false);
                        return;
                    }
                case STAGE_JOINT:
                    this.stage = Stage.STAGE_STABLE;
                    // 再次应用配置变更，剔除老的配置信息
                    this.node.unsafeApplyConfiguration(new Configuration(this.newPeers, this.newLearners), null, false);
                    break;
                case STAGE_STABLE:
                    // 当前集群节点配置是否包含当前节点
                    final boolean shouldStepDown = !this.newPeers.contains(this.node.serverId);
                    reset(new Status());
                    if (shouldStepDown) {
                        this.node.stepDown(this.node.currTerm, true,
                                new Status(RaftError.ELEADERREMOVED, "This node was removed."));
                    }
                    break;
                case STAGE_NONE:
                    // noinspection ConstantConditions
                    Requires.requireTrue(false, "Can't reach here");
                    break;
            }
        }

        boolean isBusy() {
            return this.stage != Stage.STAGE_NONE;
        }
    }

    public NodeImpl() {
        this(null, null);
    }

    public NodeImpl(final String groupId, final PeerId serverId) {
        super();
        if (groupId != null) {
            Utils.verifyGroupId(groupId);
        }
        this.groupId = groupId;
        this.serverId = serverId != null ? serverId.copy() : null;
        this.state = State.STATE_UNINITIALIZED;
        this.currTerm = 0;
        updateLastLeaderTimestamp(Utils.monotonicMs());
        this.confCtx = new ConfigurationCtx(this);
        this.wakingCandidate = null;
        final int num = GLOBAL_NUM_NODES.incrementAndGet();
        LOG.info("The number of active nodes increment to {}.", num);
    }

    /**
     * 初始化快照数据存储
     *
     * @return
     */
    private boolean initSnapshotStorage() {
        // 未设置 snapshotUri，说明不希望启动快照模块
        if (StringUtils.isEmpty(this.options.getSnapshotUri())) {
            LOG.warn("Do not set snapshot uri, ignore initSnapshotStorage.");
            return true;
        }
        // 实例化快照执行器，用于处理快照相关操作
        this.snapshotExecutor = new SnapshotExecutorImpl();
        final SnapshotExecutorOptions opts = new SnapshotExecutorOptions();
        opts.setUri(this.options.getSnapshotUri());
        opts.setFsmCaller(this.fsmCaller);
        opts.setNode(this);
        opts.setLogManager(this.logManager);
        opts.setAddr(this.serverId != null ? this.serverId.getEndpoint() : null);
        opts.setInitTerm(this.currTerm);
        opts.setFilterBeforeCopyRemote(this.options.isFilterBeforeCopyRemote());
        // get snapshot throttle
        opts.setSnapshotThrottle(this.options.getSnapshotThrottle());
        // 初始化快照执行器
        return this.snapshotExecutor.init(opts);
    }

    /**
     * 初始化日志数据存储
     *
     * @return
     */
    private boolean initLogStorage() {
        Requires.requireNonNull(this.fsmCaller, "Null fsm caller");
        // 实例化日志存储服务，基于 RocksDBLogStorage 实现类
        this.logStorage = this.serviceFactory.createLogStorage(this.options.getLogUri(), this.raftOptions);
        // 创建并初始化日志管理器
        this.logManager = new LogManagerImpl();
        final LogManagerOptions opts = new LogManagerOptions();
        // 设置 LogEntry 编解码器工厂，默认使用 LogEntryV2CodecFactory
        opts.setLogEntryCodecFactory(this.serviceFactory.createLogEntryCodecFactory());
        // 设置日志存储服务
        opts.setLogStorage(this.logStorage);
        // 设置集群节点配置管理器
        opts.setConfigurationManager(this.configManager);
        // 设置状态机调度器
        opts.setFsmCaller(this.fsmCaller);
        opts.setNodeMetrics(this.metrics);
        opts.setDisruptorBufferSize(this.raftOptions.getDisruptorBufferSize());
        opts.setRaftOptions(this.raftOptions);
        // 初始化 LogManager
        return this.logManager.init(opts);
    }

    /**
     * 初始化元数据存储
     *
     * @return
     */
    private boolean initMetaStorage() {
        // 实例化元数据存储服务，基于 LocalRaftMetaStorage 实现类
        this.metaStorage = this.serviceFactory
                .createRaftMetaStorage(this.options.getRaftMetaUri(), this.raftOptions);
        RaftMetaStorageOptions opts = new RaftMetaStorageOptions();
        opts.setNode(this);
        // 初始化元数据存储服务
        if (!this.metaStorage.init(opts)) {
            LOG.error("Node {} init meta storage failed, uri={}.", this.serverId, this.options.getRaftMetaUri());
            return false;
        }
        // 基于本地元数据恢复 currentTerm 和 votedFor 属性值
        this.currTerm = this.metaStorage.getTerm();
        this.votedId = this.metaStorage.getVotedFor().copy();
        return true;
    }

    private void handleSnapshotTimeout() {
        this.writeLock.lock();
        try {
            if (!this.state.isActive()) {
                return;
            }
        } finally {
            this.writeLock.unlock();
        }
        // do_snapshot in another thread to avoid blocking the timer thread.
        Utils.runInThread(() -> doSnapshot(null));
    }

    /**
     * 预选举
     */
    private void handleElectionTimeout() {
        boolean doUnlock = true;
        this.writeLock.lock();
        try {
            // 预选举必须由 Follower 节点发起
            if (this.state != State.STATE_FOLLOWER) {
                return;
            }

            // 与当前 Leader 节点的租约还有效，暂不发起预选举
            if (isCurrentLeaderValid()) {
                return;
            }

            /* 尝试开始发起预选举 */

            // 清空本地记录的 leaderId
            resetLeaderId(PeerId.emptyPeer(),
                    new Status(RaftError.ERAFTTIMEDOUT, "Lost connection from leader %s.", this.leaderId));

            // 基于节点优先级判断是否继续发起预选举
            if (!allowLaunchElection()) {
                return;
            }

            doUnlock = false;
            // 发起预选举
            preVote();
        } finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
        }
    }

    /**
     * Whether to allow for launching election or not by comparing node's priority with target
     * priority. And at the same time, if next leader is not elected until next election
     * timeout, it decays its local target priority exponentially.
     *
     * @return Whether current node will launch election or not.
     */
    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    private boolean allowLaunchElection() {

        // Priority 0 is a special value so that a node will never participate in election.
        if (this.serverId.isPriorityNotElected()) {
            // 当前节点不参选
            return false;
        }

        // If this nodes disable priority election, then it can make a election.
        if (this.serverId.isPriorityDisabled()) {
            // 当前节点未启用优先级策略
            return true;
        }

        // If current node's priority < target_priority,
        // it does not initiate leader election and waits for the next election timeout.
        if (this.serverId.getPriority() < this.targetPriority) {
            // 当前节点优先级小于目标优先级
            this.electionTimeoutCounter++;

            // If next leader is not elected until next election timeout,
            // it decays its local target priority exponentially.
            if (this.electionTimeoutCounter > 1) {
                // 衰减目标优先级
                decayTargetPriority();
                this.electionTimeoutCounter = 0;
            }

            if (this.electionTimeoutCounter == 1) {
                return false;
            }
        }

        return this.serverId.getPriority() >= this.targetPriority;
    }

    /**
     * Decay（衰变） targetPriority value based on gap value.
     */
    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    private void decayTargetPriority() {
        // Default Gap value should be bigger than 10.
        final int decayPriorityGap = Math.max(this.options.getDecayPriorityGap(), 10);
        final int gap = Math.max(decayPriorityGap, (this.targetPriority / 5));

        final int prevTargetPriority = this.targetPriority;
        this.targetPriority = Math.max(ElectionPriority.MinValue, (this.targetPriority - gap));
        LOG.info("Node {} priority decay, from: {}, to: {}.", getNodeId(), prevTargetPriority, this.targetPriority);
    }

    /**
     * Check and set configuration for node.At the same time, if configuration is changed,
     * then compute and update the target priority value.
     *
     * @param inLock whether the writeLock has already been locked in other place.
     */
    private void checkAndSetConfiguration(final boolean inLock) {
        if (!inLock) {
            this.writeLock.lock();
        }
        try {
            final ConfigurationEntry prevConf = this.conf;
            this.conf = this.logManager.checkAndSetConfiguration(prevConf);

            if (this.conf != prevConf) {
                // Update target priority value
                final int prevTargetPriority = this.targetPriority;
                this.targetPriority = getMaxPriorityOfNodes(this.conf.getConf().getPeers());
                if (prevTargetPriority != this.targetPriority) {
                    LOG.info("Node {} target priority value has changed from: {}, to: {}.", getNodeId(),
                            prevTargetPriority, this.targetPriority);
                }
                this.electionTimeoutCounter = 0;
            }
        } finally {
            if (!inLock) {
                this.writeLock.unlock();
            }
        }
    }

    /**
     * Get max priority value for all nodes in the same Raft group, and update current node's target priority value.
     *
     * @param peerIds peer nodes in the same Raft group
     */
    private int getMaxPriorityOfNodes(final List<PeerId> peerIds) {
        Requires.requireNonNull(peerIds, "Null peer list");

        int maxPriority = Integer.MIN_VALUE;
        for (final PeerId peerId : peerIds) {
            final int priorityVal = peerId.getPriority();
            maxPriority = Math.max(priorityVal, maxPriority);
        }

        return maxPriority;
    }

    /**
     * 初始化状态机调度器
     *
     * @param bootstrapId
     * @return
     */
    private boolean initFSMCaller(final LogId bootstrapId) {
        if (this.fsmCaller == null) {
            LOG.error("Fail to init fsm caller, null instance, bootstrapId={}.", bootstrapId);
            return false;
        }
        // 创建封装 Closure 的队列，基于 LinkedList 实现
        this.closureQueue = new ClosureQueueImpl();
        final FSMCallerOptions opts = new FSMCallerOptions();
        opts.setAfterShutdown(status -> afterShutdown());
        opts.setLogManager(this.logManager);
        opts.setFsm(this.options.getFsm());
        opts.setClosureQueue(this.closureQueue);
        opts.setNode(this);
        opts.setBootstrapId(bootstrapId);
        opts.setDisruptorBufferSize(this.raftOptions.getDisruptorBufferSize());
        // 初始化状态机调度器
        return this.fsmCaller.init(opts);
    }

    private static class BootstrapStableClosure extends LogManager.StableClosure {

        private final SynchronizedClosure done = new SynchronizedClosure();

        public BootstrapStableClosure() {
            super(null);
        }

        public Status await() throws InterruptedException {
            return this.done.await();
        }

        @Override
        public void run(final Status status) {
            this.done.run(status);
        }
    }

    public boolean bootstrap(final BootstrapOptions opts) throws InterruptedException {
        if (opts.getLastLogIndex() > 0 && (opts.getGroupConf().isEmpty() || opts.getFsm() == null)) {
            LOG.error("Invalid arguments for bootstrap, groupConf={}, fsm={}, lastLogIndex={}.", opts.getGroupConf(),
                    opts.getFsm(), opts.getLastLogIndex());
            return false;
        }
        if (opts.getGroupConf().isEmpty()) {
            LOG.error("Bootstrapping an empty node makes no sense.");
            return false;
        }
        Requires.requireNonNull(opts.getServiceFactory(), "Null jraft service factory");
        this.serviceFactory = opts.getServiceFactory();
        // Term is not an option since changing it is very dangerous
        final long bootstrapLogTerm = opts.getLastLogIndex() > 0 ? 1 : 0;
        final LogId bootstrapId = new LogId(opts.getLastLogIndex(), bootstrapLogTerm);
        this.options = new NodeOptions();
        this.raftOptions = this.options.getRaftOptions();
        this.metrics = new NodeMetrics(opts.isEnableMetrics());
        this.options.setFsm(opts.getFsm());
        this.options.setLogUri(opts.getLogUri());
        this.options.setRaftMetaUri(opts.getRaftMetaUri());
        this.options.setSnapshotUri(opts.getSnapshotUri());

        this.configManager = new ConfigurationManager();
        // Create fsmCaller at first as logManager needs it to report error
        this.fsmCaller = new FSMCallerImpl();

        if (!initLogStorage()) {
            LOG.error("Fail to init log storage.");
            return false;
        }
        if (!initMetaStorage()) {
            LOG.error("Fail to init meta storage.");
            return false;
        }
        if (this.currTerm == 0) {
            this.currTerm = 1;
            if (!this.metaStorage.setTermAndVotedFor(1, new PeerId())) {
                LOG.error("Fail to set term.");
                return false;
            }
        }

        if (opts.getFsm() != null && !initFSMCaller(bootstrapId)) {
            LOG.error("Fail to init fsm caller.");
            return false;
        }

        final LogEntry entry = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION);
        entry.getId().setTerm(this.currTerm);
        entry.setPeers(opts.getGroupConf().listPeers());
        entry.setLearners(opts.getGroupConf().listLearners());

        final List<LogEntry> entries = new ArrayList<>();
        entries.add(entry);

        final BootstrapStableClosure bootstrapDone = new BootstrapStableClosure();
        this.logManager.appendEntries(entries, bootstrapDone);
        if (!bootstrapDone.await().isOk()) {
            LOG.error("Fail to append configuration.");
            return false;
        }

        if (opts.getLastLogIndex() > 0) {
            if (!initSnapshotStorage()) {
                LOG.error("Fail to init snapshot storage.");
                return false;
            }
            final SynchronizedClosure snapshotDone = new SynchronizedClosure();
            this.snapshotExecutor.doSnapshot(snapshotDone);
            if (!snapshotDone.await().isOk()) {
                LOG.error("Fail to save snapshot, status={}.", snapshotDone.getStatus());
                return false;
            }
        }

        if (this.logManager.getFirstLogIndex() != opts.getLastLogIndex() + 1) {
            throw new IllegalStateException("First and last log index mismatch");
        }
        if (opts.getLastLogIndex() > 0) {
            if (this.logManager.getLastLogIndex() != opts.getLastLogIndex()) {
                throw new IllegalStateException("Last log index mismatch");
            }
        } else {
            if (this.logManager.getLastLogIndex() != opts.getLastLogIndex() + 1) {
                throw new IllegalStateException("Last log index mismatch");
            }
        }

        return true;
    }

    private int heartbeatTimeout(final int electionTimeout) {
        return Math.max(electionTimeout / this.raftOptions.getElectionHeartbeatFactor(), 10);
    }

    private int randomTimeout(final int timeoutMs) {
        return ThreadLocalRandom.current().nextInt(timeoutMs, timeoutMs + this.raftOptions.getMaxElectionDelayMs());
    }

    @Override
    public boolean init(final NodeOptions opts) {
        Requires.requireNonNull(opts, "Null node options");
        Requires.requireNonNull(opts.getRaftOptions(), "Null raft options");
        Requires.requireNonNull(opts.getServiceFactory(), "Null jraft service factory");
        this.serviceFactory = opts.getServiceFactory();
        this.options = opts;
        this.raftOptions = opts.getRaftOptions();
        this.metrics = new NodeMetrics(opts.isEnableMetrics());
        this.serverId.setPriority(opts.getElectionPriority());
        this.electionTimeoutCounter = 0;

        // 节点 IP 不允许设置为 0.0.0.0
        if (this.serverId.getIp().equals(Utils.IP_ANY)) {
            LOG.error("Node can't started from IP_ANY.");
            return false;
        }

        // 正常在初始化 Node 之前需要调用 NodeManager#addAddress 方法记录当前节点地址
        if (!NodeManager.getInstance().serverExists(this.serverId.getEndpoint())) {
            LOG.error("No RPC server attached to, did you forget to call addService?");
            return false;
        }

        // 创建并初始化延时任务调度器 TimerManager，负责 JRaft 内部的延时任务调度
        this.timerManager = TIMER_FACTORY.getRaftScheduler(
                this.options.isSharedTimerPool(),
                this.options.getTimerPoolSize(), "JRaft-Node-ScheduleThreadPool");

        // Init timers
        final String suffix = getNodeId().toString();

        // 创建正式选举计时器（周期： 1s ~ 2s）
        String name = "JRaft-VoteTimer-" + suffix;
        this.voteTimer = new RepeatedTimer(name, this.options.getElectionTimeoutMs(),
                TIMER_FACTORY.getVoteTimer(this.options.isSharedVoteTimer(), name)) {

            @Override
            protected void onTrigger() {
                handleVoteTimeout();
            }

            @Override
            protected int adjustTimeout(final int timeoutMs) {
                return randomTimeout(timeoutMs);
            }
        };

        // 创建预选举计时器（周期：1s ~ 2s）
        name = "JRaft-ElectionTimer-" + suffix;
        this.electionTimer = new RepeatedTimer(name, this.options.getElectionTimeoutMs(),
                TIMER_FACTORY.getElectionTimer(this.options.isSharedElectionTimer(), name)) {

            @Override
            protected void onTrigger() {
                handleElectionTimeout();
            }

            @Override
            protected int adjustTimeout(final int timeoutMs) {
                return randomTimeout(timeoutMs);
            }
        };

        // 创建角色降级计时器（周期：0.5s）
        name = "JRaft-StepDownTimer-" + suffix;
        this.stepDownTimer = new RepeatedTimer(name, this.options.getElectionTimeoutMs() >> 1,
                TIMER_FACTORY.getStepDownTimer(this.options.isSharedStepDownTimer(), name)) {

            @Override
            protected void onTrigger() {
                handleStepDownTimeout();
            }
        };

        // 创建快照周期性生成计时器（周期：1h）
        name = "JRaft-SnapshotTimer-" + suffix;
        this.snapshotTimer = new RepeatedTimer(name, this.options.getSnapshotIntervalSecs() * 1000,
                TIMER_FACTORY.getSnapshotTimer(this.options.isSharedSnapshotTimer(), name)) {

            private volatile boolean firstSchedule = true;

            @Override
            protected void onTrigger() {
                handleSnapshotTimeout();
            }

            @Override
            protected int adjustTimeout(final int timeoutMs) {
                if (!this.firstSchedule) {
                    return timeoutMs;
                }

                // Randomize the first snapshot trigger timeout
                this.firstSchedule = false;
                if (timeoutMs > 0) {
                    int half = timeoutMs / 2;
                    return half + ThreadLocalRandom.current().nextInt(half);
                } else {
                    return timeoutMs;
                }
            }
        };

        // 创建集群节点配置管理器
        this.configManager = new ConfigurationManager();

        // 初始化 Task 处理相关的 disruptor 队列，用于异步处理业务调用 Node#apply 方法向集群提交的 Task 列表
        this.applyDisruptor = DisruptorBuilder.<LogEntryAndClosure>newInstance() //
                .setRingBufferSize(this.raftOptions.getDisruptorBufferSize()) //
                .setEventFactory(new LogEntryAndClosureFactory()) //
                .setThreadFactory(new NamedThreadFactory("JRaft-NodeImpl-Disruptor-", true)) //
                .setProducerType(ProducerType.MULTI) //
                .setWaitStrategy(new BlockingWaitStrategy()) //
                .build();
        this.applyDisruptor.handleEventsWith(new LogEntryAndClosureHandler());
        this.applyDisruptor.setDefaultExceptionHandler(new LogExceptionHandler<Object>(getClass().getSimpleName()));
        this.applyQueue = this.applyDisruptor.start();
        if (this.metrics.getMetricRegistry() != null) {
            this.metrics.getMetricRegistry().register("jraft-node-impl-disruptor", new DisruptorMetricSet(this.applyQueue));
        }

        // 创建状态机调度器
        this.fsmCaller = new FSMCallerImpl();

        // 初始化日志数据存储模块
        if (!initLogStorage()) {
            LOG.error("Node {} initLogStorage failed.", getNodeId());
            return false;
        }

        // 初始化元数据存储模块
        if (!initMetaStorage()) {
            LOG.error("Node {} initMetaStorage failed.", getNodeId());
            return false;
        }

        // 初始化状态机调度器
        if (!initFSMCaller(new LogId(0, 0))) {
            LOG.error("Node {} initFSMCaller failed.", getNodeId());
            return false;
        }

        // 创建并初始化选票箱 BallotBox，每个节点绑定一个选票箱
        this.ballotBox = new BallotBox();
        final BallotBoxOptions ballotBoxOpts = new BallotBoxOptions();
        ballotBoxOpts.setWaiter(this.fsmCaller);
        // closureQueue 在初始化 FSMCaller 时创建，相互共用
        ballotBoxOpts.setClosureQueue(this.closureQueue);
        if (!this.ballotBox.init(ballotBoxOpts)) {
            LOG.error("Node {} init ballotBox failed.", getNodeId());
            return false;
        }

        // 初始化快照数据存储模块
        if (!initSnapshotStorage()) {
            LOG.error("Node {} initSnapshotStorage failed.", getNodeId());
            return false;
        }

        // 对日志数据进行一致性校验
        final Status st = this.logManager.checkConsistency();
        if (!st.isOk()) {
            LOG.error("Node {} is initialized with inconsistent log, status={}.", getNodeId(), st);
            return false;
        }

        // 初始化集群节点配置，优先从日志中恢复
        this.conf = new ConfigurationEntry();
        this.conf.setId(new LogId());
        // if have log using conf in log, else using conf in options
        if (this.logManager.getLastLogIndex() > 0) {
            checkAndSetConfiguration(false);
        } else {
            this.conf.setConf(this.options.getInitialConf());
            // 以初始节点中的最大优先级初始化 targetPriority，用于控制当前节点是否继续发起预选举
            this.targetPriority = getMaxPriorityOfNodes(this.conf.getConf().getPeers());
        }

        // 如果初始集群列表不为空，则需要校验其有效性，即 peers 不为空，且不能和 learners 有交集
        if (!this.conf.isEmpty()) {
            Requires.requireTrue(this.conf.isValid(), "Invalid conf: %s", this.conf);
        } else {
            LOG.info("Init node {} with empty conf.", this.serverId);
        }

        // TODO RPC service and ReplicatorGroup is in cycle dependent, refactor it
        // 创建复制器 Replicator 管理组
        this.replicatorGroup = new ReplicatorGroupImpl();
        // 创建 RPC 客户端
        this.rpcService = new DefaultRaftClientService(this.replicatorGroup);
        final ReplicatorGroupOptions rgOpts = new ReplicatorGroupOptions();
        rgOpts.setHeartbeatTimeoutMs(heartbeatTimeout(this.options.getElectionTimeoutMs()));
        rgOpts.setElectionTimeoutMs(this.options.getElectionTimeoutMs());
        rgOpts.setLogManager(this.logManager);
        rgOpts.setBallotBox(this.ballotBox);
        rgOpts.setNode(this);
        rgOpts.setRaftRpcClientService(this.rpcService);
        rgOpts.setSnapshotStorage(this.snapshotExecutor != null ? this.snapshotExecutor.getSnapshotStorage() : null);
        rgOpts.setRaftOptions(this.raftOptions);
        rgOpts.setTimerManager(this.timerManager);

        // Adds metric registry to RPC service.
        this.options.setMetricRegistry(this.metrics.getMetricRegistry());

        // 初始化 RPC 客户端
        if (!this.rpcService.init(this.options)) {
            LOG.error("Fail to init rpc service.");
            return false;
        }
        // 初始化复制器管理组
        this.replicatorGroup.init(new NodeId(this.groupId, this.serverId), rgOpts);

        // 创建并初始化只读服务，用于支持线性一致性读
        this.readOnlyService = new ReadOnlyServiceImpl();
        final ReadOnlyServiceOptions rosOpts = new ReadOnlyServiceOptions();
        rosOpts.setFsmCaller(this.fsmCaller);
        rosOpts.setNode(this);
        rosOpts.setRaftOptions(this.raftOptions);
        if (!this.readOnlyService.init(rosOpts)) {
            LOG.error("Fail to init readOnlyService.");
            return false;
        }

        // 切换节点角色为 FOLLOWER
        this.state = State.STATE_FOLLOWER;

        if (LOG.isInfoEnabled()) {
            LOG.info("Node {} init, term={}, lastLogId={}, conf={}, oldConf={}.",
                    getNodeId(), this.currTerm, this.logManager.getLastLogId(false), this.conf.getConf(), this.conf.getOldConf());
        }

        // 如果启用了快照生成机制，则启动周期性快照生成任务
        if (this.snapshotExecutor != null && this.options.getSnapshotIntervalSecs() > 0) {
            LOG.debug("Node {} start snapshot timer, term={}.", getNodeId(), this.currTerm);
            this.snapshotTimer.start();
        }

        // 尝试角色降级，主要用于初始化本地状态，并启动预选举计时器
        if (!this.conf.isEmpty()) {
            stepDown(this.currTerm, false, new Status());
        }

        if (!NodeManager.getInstance().add(this)) {
            LOG.error("NodeManager add {} failed.", getNodeId());
            return false;
        }

        // Now the raft node is started , have to acquire the writeLock to avoid race conditions
        this.writeLock.lock();
        // 如果当前集群只有自己一个节点，则尝试选举自己为主节点
        if (this.conf.isStable() && this.conf.getConf().size() == 1 && this.conf.getConf().contains(this.serverId)) {
            // The group contains only this server which must be the LEADER, trigger the timer immediately.
            electSelf();
        } else {
            this.writeLock.unlock();
        }

        return true;
    }

    @OnlyForTest
    void tryElectSelf() {
        this.writeLock.lock();
        // unlock in electSelf
        electSelf();
    }

    // should be in writeLock
    private void electSelf() {
        long oldTerm;
        try {
            LOG.info("Node {} start vote and grant vote self, term={}.", getNodeId(), this.currTerm);
            // 当前节点不是一个合法节点
            if (!this.conf.contains(this.serverId)) {
                LOG.warn("Node {} can't do electSelf as it is not in {}.", getNodeId(), this.conf);
                return;
            }
            // 当前节点第一次尝试正式选举，需要暂时停止预选举计时器，避免期间再次触发预选举
            if (this.state == State.STATE_FOLLOWER) {
                LOG.debug("Node {} stop election timer, term={}.", getNodeId(), this.currTerm);
                this.electionTimer.stop();
            }
            // 清空本地记录的 Leader 节点 ID
            resetLeaderId(PeerId.emptyPeer(), new Status(RaftError.ERAFTTIMEDOUT,
                    "A follower's leader_id is reset to NULL as it begins to request_vote."));
            // 切换角色为 CANDIDATE
            this.state = State.STATE_CANDIDATE;
            // 正式投票环境真正递增 term 值，而预选举阶段不会
            this.currTerm++;
            // 更新 votedId 字段，标记投票给自己
            this.votedId = this.serverId.copy();
            LOG.debug("Node {} start vote timer, term={} .", getNodeId(), this.currTerm);
            // 启动正式选举计时器，当选举超时会再次触发正式选举进程
            this.voteTimer.start();
            // 初始化正式选举选票
            this.voteCtx.init(this.conf.getConf(), this.conf.isStable() ? null : this.conf.getOldConf());
            oldTerm = this.currTerm;
        } finally {
            this.writeLock.unlock();
        }

        // 从本地加载最新的 logIndex 和对应的 term 值
        final LogId lastLogId = this.logManager.getLastLogId(true);

        this.writeLock.lock();
        try {
            // vote need defense ABA after unlock&writeLock
            if (oldTerm != this.currTerm) {
                LOG.warn("Node {} raise term {} when getLastLogId.", getNodeId(), this.currTerm);
                return;
            }
            // 遍历向除自己以外的所有连通节点发送 RequestVote RPC 请求，以征集选票
            for (final PeerId peer : this.conf.listPeers()) {
                if (peer.equals(this.serverId)) {
                    continue;
                }
                if (!this.rpcService.connect(peer.getEndpoint())) {
                    LOG.warn("Node {} channel init failed, address={}.", getNodeId(), peer.getEndpoint());
                    continue;
                }
                final OnRequestVoteRpcDone done = new OnRequestVoteRpcDone(peer, this.currTerm, this);
                done.request = RequestVoteRequest.newBuilder() //
                        .setPreVote(false) // It's not a pre-vote request. 标记是正式选举
                        .setGroupId(this.groupId) //
                        .setServerId(this.serverId.toString()) //
                        .setPeerId(peer.toString()) //
                        .setTerm(this.currTerm) // 这里是递增后的 term 值
                        .setLastLogIndex(lastLogId.getIndex()) //
                        .setLastLogTerm(lastLogId.getTerm()) //
                        .build();
                // 发送 RPC 请求
                this.rpcService.requestVote(peer.getEndpoint(), done.request, done);
            }

            // 更本地元数据信息
            this.metaStorage.setTermAndVotedFor(this.currTerm, this.serverId);
            // 给自己投上一票
            this.voteCtx.grant(this.serverId);
            // 检查是否竞选成功
            if (this.voteCtx.isGranted()) {
                // 成为 leader
                becomeLeader();
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    private void resetLeaderId(final PeerId newLeaderId, final Status status) {
        if (newLeaderId.isEmpty()) {
            // 当前节点不是 Leader，且正追随着某个 Leader 节点，则 FSMCaller#onStopFollowing
            if (!this.leaderId.isEmpty() && this.state.compareTo(State.STATE_TRANSFERRING) > 0) {
                this.fsmCaller.onStopFollowing(
                        new LeaderChangeContext(this.leaderId.copy(), this.currTerm, status));
            }
            // 清空本地记录的 Leader 节点
            this.leaderId = PeerId.emptyPeer();
        } else {
            if (this.leaderId == null || this.leaderId.isEmpty()) {
                this.fsmCaller.onStartFollowing(new LeaderChangeContext(newLeaderId, this.currTerm, status));
            }
            this.leaderId = newLeaderId.copy();
        }
    }

    // in writeLock
    private void checkStepDown(final long requestTerm, final PeerId serverId) {
        final Status status = new Status();
        // 请求的 term 值大于当前节点的 term 值
        if (requestTerm > this.currTerm) {
            status.setError(RaftError.ENEWLEADER, "Raft node receives message from new leader with higher term.");
            stepDown(requestTerm, false, status);
        }
        // 当前节点不是 FOLLOWER 角色
        else if (this.state != State.STATE_FOLLOWER) {
            status.setError(RaftError.ENEWLEADER, "Candidate receives message from new leader with the same term.");
            stepDown(requestTerm, false, status);
        }
        // 当前节点不知道 leader 节点
        else if (this.leaderId.isEmpty()) {
            status.setError(RaftError.ENEWLEADER, "Follower receives message from new leader with the same term.");
            stepDown(requestTerm, false, status);
        }
        // 更新本地记录的 leader 节点
        if (this.leaderId == null || this.leaderId.isEmpty()) {
            resetLeaderId(serverId, status);
        }
    }

    /**
     * 成为 Leader
     */
    private void becomeLeader() {
        // 前置角色必须是 CANDIDATE
        Requires.requireTrue(this.state == State.STATE_CANDIDATE, "Illegal state: " + this.state);
        LOG.info("Node {} become leader of group, term={}, conf={}, oldConf={}.",
                getNodeId(), this.currTerm, this.conf.getConf(), this.conf.getOldConf());
        // 停止正式选举计时器
        stopVoteTimer();
        // 切换角色为 LEADER
        this.state = State.STATE_LEADER;
        // 更新本地记录的 leader 节点 ID
        this.leaderId = this.serverId.copy();
        // 设置复制器组的 term 值
        this.replicatorGroup.resetTerm(this.currTerm);

        // 处理非 learner 节点：遍历将集群中除自己以外的非 learner 节点纳为自己的 follower，并建立到这些节点的复制关系
        for (final PeerId peer : this.conf.listPeers()) {
            if (peer.equals(this.serverId)) {
                continue;
            }
            LOG.debug("Node {} add a replicator, term={}, peer={}.", getNodeId(), this.currTerm, peer);
            if (!this.replicatorGroup.addReplicator(peer)) {
                LOG.error("Fail to add a replicator, peer={}.", peer);
            }
        }

        // 处理 learner 节点：遍历将集群中除自己以外的 learner 节点纳为自己的 follower，并建立到这些节点的复制关系
        // learner 节点只是作为 follower 复制日志，不会对日志的提交做决策
        for (final PeerId peer : this.conf.listLearners()) {
            LOG.debug("Node {} add a learner replicator, term={}, peer={}.", getNodeId(), this.currTerm, peer);
            if (!this.replicatorGroup.addReplicator(peer, ReplicatorType.Learner)) {
                LOG.error("Fail to add a learner replicator, peer={}.", peer);
            }
        }

        // 重置选票箱
        this.ballotBox.resetPendingIndex(this.logManager.getLastLogIndex() + 1);
        // Register _conf_ctx to reject configuration changing before the first log is committed.
        if (this.confCtx.isBusy()) {
            throw new IllegalStateException();
        }
        // 将当前集群配置信息写入日志
        this.confCtx.flush(this.conf.getConf(), this.conf.getOldConf());
        // 启动 stepdown 计时器
        this.stepDownTimer.start();
    }

    // should be in writeLock
    private void stepDown(final long term, final boolean wakeupCandidate, final Status status) {
        LOG.debug("Node {} stepDown, term={}, newTerm={}, wakeupCandidate={}.",
                getNodeId(), this.currTerm, term, wakeupCandidate);
        if (!this.state.isActive()) {
            return;
        }
        if (this.state == State.STATE_CANDIDATE) {
            stopVoteTimer();
        } else if (this.state.compareTo(State.STATE_TRANSFERRING) <= 0) {
            // 停止 stepdown 计时器
            stopStepDownTimer();
            // 清空选票箱
            this.ballotBox.clearPendingTasks();
            // signal fsm leader stop immediately
            if (this.state == State.STATE_LEADER) {
                // 向状态机调度器发布 LEADER_STOP 事件
                onLeaderStop(status);
            }
        }
        // reset leader_id
        resetLeaderId(PeerId.emptyPeer(), status);

        // soft state in memory
        this.state = State.STATE_FOLLOWER;
        this.confCtx.reset();
        updateLastLeaderTimestamp(Utils.monotonicMs());
        if (this.snapshotExecutor != null) {
            this.snapshotExecutor.interruptDownloadingSnapshots(term);
        }

        // meta state
        if (term > this.currTerm) {
            this.currTerm = term;
            this.votedId = PeerId.emptyPeer();
            this.metaStorage.setTermAndVotedFor(term, this.votedId);
        }

        if (wakeupCandidate) {
            this.wakingCandidate = this.replicatorGroup.stopAllAndFindTheNextCandidate(this.conf);
            if (this.wakingCandidate != null) {
                Replicator.sendTimeoutNowAndStop(this.wakingCandidate, this.options.getElectionTimeoutMs());
            }
        } else {
            this.replicatorGroup.stopAll();
        }
        if (this.stopTransferArg != null) {
            if (this.transferTimer != null) {
                this.transferTimer.cancel(true);
            }
            // There is at most one StopTransferTimer at the same term, it's safe to
            // mark stopTransferArg to NULL
            this.stopTransferArg = null;
        }
        // Learner node will not trigger the election timer.
        if (!isLearner()) {
            this.electionTimer.restart();
        } else {
            LOG.info("Node {} is a learner, election timer is not started.", this.nodeId);
        }
    }

    // Should be in readLock
    private boolean isLearner() {
        return this.conf.listLearners().contains(this.serverId);
    }

    private void stopStepDownTimer() {
        if (this.stepDownTimer != null) {
            this.stepDownTimer.stop();
        }
    }

    private void stopVoteTimer() {
        if (this.voteTimer != null) {
            this.voteTimer.stop();
        }
    }

    class LeaderStableClosure extends LogManager.StableClosure {

        public LeaderStableClosure(final List<LogEntry> entries) {
            super(entries);
        }

        @Override
        public void run(final Status status) {
            if (status.isOk()) {
                NodeImpl.this.ballotBox.commitAt(
                        this.firstLogIndex, this.firstLogIndex + this.nEntries - 1, NodeImpl.this.serverId);
            } else {
                LOG.error("Node {} append [{}, {}] failed, status={}.",
                        getNodeId(), this.firstLogIndex, this.firstLogIndex + this.nEntries - 1, status);
            }
        }
    }

    private void executeApplyingTasks(final List<LogEntryAndClosure> tasks) {
        this.writeLock.lock();
        try {
            final int size = tasks.size();
            // 只有 Leader 节点允许处理 task
            if (this.state != State.STATE_LEADER) {
                final Status st = new Status();
                if (this.state != State.STATE_TRANSFERRING) {
                    st.setError(RaftError.EPERM, "Is not leader.");
                } else {
                    st.setError(RaftError.EBUSY, "Is transferring leadership.");
                }
                LOG.debug("Node {} can't apply, status={}.", getNodeId(), st);
                final List<LogEntryAndClosure> savedTasks = new ArrayList<>(tasks);
                // 快速失败
                Utils.runInThread(() -> {
                    for (int i = 0; i < size; i++) {
                        savedTasks.get(i).done.run(st);
                    }
                });
                return;
            }
            final List<LogEntry> entries = new ArrayList<>(size);
            // 遍历处理 task 集合
            for (int i = 0; i < size; i++) {
                final LogEntryAndClosure task = tasks.get(i);
                // 如果 task 期望校验 term 值，则校验当前节点的 term 值是否是期望的 term 值
                if (task.expectedTerm != -1 && task.expectedTerm != this.currTerm) {
                    LOG.debug("Node {} can't apply task whose expectedTerm={} doesn't match currTerm={}.",
                            getNodeId(), task.expectedTerm, this.currTerm);
                    if (task.done != null) {
                        final Status st = new Status(RaftError.EPERM,
                                "expected_term=%d doesn't match current_term=%d", task.expectedTerm, this.currTerm);
                        Utils.runClosureInThread(task.done, st);
                    }
                    continue;
                }
                // 为每个 task 创建并初始化对应的选票，用于决策对应的 LogEntry 是否能够被提交
                if (!this.ballotBox.appendPendingTask(this.conf.getConf(),
                        this.conf.isStable() ? null : this.conf.getOldConf(), task.done)) {
                    Utils.runClosureInThread(task.done, new Status(RaftError.EINTERNAL, "Fail to append task."));
                    continue;
                }
                // set task entry info before adding to list.
                task.entry.getId().setTerm(this.currTerm);
                task.entry.setType(EnumOutter.EntryType.ENTRY_TYPE_DATA);
                entries.add(task.entry);
            }
            // 追加日志数据到本地文件系统，完成之后回调 LeaderStableClosure
            this.logManager.appendEntries(entries, new LeaderStableClosure(entries));
            // update conf.first
            checkAndSetConfiguration(true);
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Returns the node metrics.
     *
     * @return returns metrics of current node.
     */
    @Override
    public NodeMetrics getNodeMetrics() {
        return this.metrics;
    }

    /**
     * Returns the JRaft service factory for current node.
     *
     * @return the service factory
     * @since 1.2.6
     */
    public JRaftServiceFactory getServiceFactory() {
        return this.serviceFactory;
    }

    @Override
    public void readIndex(final byte[] requestContext, final ReadIndexClosure done) {
        // 当前节点正在被关闭
        if (this.shutdownLatch != null) {
            Utils.runClosureInThread(done,
                    new Status(RaftError.ENODESHUTDOWN, "Node is shutting down."));
            throw new IllegalStateException("Node is shutting down");
        }
        Requires.requireNonNull(done, "Null closure");
        // 向 ReadOnlyService 提交一个 ReadIndex 请求，
        // 当本地数据与 Leader 节点数据在特定的位置（lastCommittedIndex）同步时，响应回调
        this.readOnlyService.addRequest(requestContext, done);
    }

    /**
     * ReadIndex response closure
     *
     * @author dennis
     */
    private class ReadIndexHeartbeatResponseClosure extends RpcResponseClosureAdapter<AppendEntriesResponse> {
        final ReadIndexResponse.Builder respBuilder;
        final RpcResponseClosure<ReadIndexResponse> closure;
        final int quorum;
        final int failPeersThreshold;
        int ackSuccess;
        int ackFailures;
        boolean isDone;

        public ReadIndexHeartbeatResponseClosure(final RpcResponseClosure<ReadIndexResponse> closure,
                                                 final ReadIndexResponse.Builder rb, final int quorum,
                                                 final int peersCount) {
            super();
            this.closure = closure;
            this.respBuilder = rb;
            this.quorum = quorum;
            this.failPeersThreshold = peersCount % 2 == 0 ? (quorum - 1) : quorum;
            this.ackSuccess = 0;
            this.ackFailures = 0;
            this.isDone = false;
        }

        @Override
        public synchronized void run(final Status status) {
            if (this.isDone) {
                return;
            }
            if (status.isOk() && getResponse().getSuccess()) {
                this.ackSuccess++;
            } else {
                this.ackFailures++;
            }
            // Include leader self vote yes.
            if (this.ackSuccess + 1 >= this.quorum) {
                this.respBuilder.setSuccess(true);
                this.closure.setResponse(this.respBuilder.build());
                this.closure.run(Status.OK());
                this.isDone = true;
            } else if (this.ackFailures >= this.failPeersThreshold) {
                this.respBuilder.setSuccess(false);
                this.closure.setResponse(this.respBuilder.build());
                this.closure.run(Status.OK());
                this.isDone = true;
            }
        }
    }

    /**
     * Handle read index request.
     */
    @Override
    public void handleReadIndexRequest(final ReadIndexRequest request, final RpcResponseClosure<ReadIndexResponse> done) {
        final long startMs = Utils.monotonicMs();
        this.readLock.lock();
        try {
            switch (this.state) {
                // 当前节点是 LEADER 角色
                case STATE_LEADER:
                    // 基于 ReadIndexRead 或 LeaseRead 策略验证当前 Leader 节点是否仍然有效
                    readLeader(request, ReadIndexResponse.newBuilder(), done);
                    break;
                // 当前节点是 FOLLOWER 角色
                case STATE_FOLLOWER:
                    // 向 Leader 节点发送 ReadIndex 请求
                    readFollower(request, done);
                    break;
                // 当前正在执行 LEADER 节点切换
                case STATE_TRANSFERRING:
                    done.run(new Status(RaftError.EBUSY, "Is transferring leadership."));
                    break;
                default:
                    done.run(new Status(RaftError.EPERM, "Invalid state for readIndex: %s.", this.state));
                    break;
            }
        } finally {
            this.readLock.unlock();
            this.metrics.recordLatency("handle-read-index", Utils.monotonicMs() - startMs);
            this.metrics.recordSize("handle-read-index-entries", request.getEntriesCount());
        }
    }

    private int getQuorum() {
        final Configuration c = this.conf.getConf();
        if (c.isEmpty()) {
            return 0;
        }
        return c.getPeers().size() / 2 + 1;
    }

    private void readFollower(final ReadIndexRequest request, final RpcResponseClosure<ReadIndexResponse> closure) {
        if (this.leaderId == null || this.leaderId.isEmpty()) {
            closure.run(new Status(RaftError.EPERM, "No leader at term %d.", this.currTerm));
            return;
        }
        // send request to leader.
        final ReadIndexRequest newRequest = ReadIndexRequest.newBuilder() //
                .mergeFrom(request) //
                .setPeerId(this.leaderId.toString()) //
                .build();
        this.rpcService.readIndex(this.leaderId.getEndpoint(), newRequest, -1, closure);
    }

    private void readLeader(final ReadIndexRequest request,
                            final ReadIndexResponse.Builder respBuilder,
                            final RpcResponseClosure<ReadIndexResponse> closure) {
        // 获取仲裁值，即集群节点的半数加 1
        final int quorum = getQuorum();

        // 当前集群只有一个节点，直接返回 lastCommittedIndex 值
        if (quorum <= 1) {
            // Only one peer, fast path.
            respBuilder.setSuccess(true) //
                    .setIndex(this.ballotBox.getLastCommittedIndex());
            closure.setResponse(respBuilder.build());
            closure.run(Status.OK());
            return;
        }

        // 获取本地记录的 lastCommittedIndex 值
        final long lastCommittedIndex = this.ballotBox.getLastCommittedIndex();
        // 校验 term 值是否发生变化，以保证对应的 lastCommittedIndex 值是有效的
        if (this.logManager.getTerm(lastCommittedIndex) != this.currTerm) {
            // Reject read only request when this leader has not committed any log entry at its term
            closure.run(new Status(
                    RaftError.EAGAIN,
                    "ReadIndex request rejected because leader has not committed any log entry at its term, logIndex=%d, currTerm=%d.",
                    lastCommittedIndex, this.currTerm));
            return;
        }
        // 记录 lastCommittedIndex 到请求响应对象中
        respBuilder.setIndex(lastCommittedIndex);

        // 对于来自 Follower 节点或 Learner 节点的请求，peerId 字段会记录这些节点已知的 leaderId 值，所以不为 null
        if (request.getPeerId() != null) {
            // request from follower or learner, check if the follower/learner is in current conf.
            final PeerId peer = new PeerId();
            peer.parse(request.getServerId());
            // 请求来源节点并不是当前 Leader 节点管理范围内的节点
            if (!this.conf.contains(peer) && !this.conf.containsLearner(peer)) {
                closure.run(new Status(RaftError.EPERM,
                        "Peer %s is not in current configuration: %s.", peer, this.conf));
                return;
            }
        }

        // 基于参数决策是走 ReadIndexRead 还是 LeaseRead 策略，默认走 ReadIndexRead 策略，
        // 如果是 LeaseRead，则基于时间戳检查集群中是否有过半数的节点仍然认可当前 Leader 节点，
        ReadOnlyOption readOnlyOpt = this.raftOptions.getReadOnlyOptions();
        if (readOnlyOpt == ReadOnlyOption.ReadOnlyLeaseBased && !isLeaderLeaseValid()) {
            // If leader lease timeout, we must change option to ReadOnlySafe
            readOnlyOpt = ReadOnlyOption.ReadOnlySafe;
        }

        switch (readOnlyOpt) {
            // ReadIndexRead 策略
            case ReadOnlySafe:
                final List<PeerId> peers = this.conf.getConf().getPeers();
                Requires.requireTrue(peers != null && !peers.isEmpty(), "Empty peers");
                // 向所有的 Follower 节点发送心跳请求，以检查当前 Leader 节点是否仍然有效
                final ReadIndexHeartbeatResponseClosure heartbeatDone =
                        new ReadIndexHeartbeatResponseClosure(closure, respBuilder, quorum, peers.size());
                // Send heartbeat requests to followers
                for (final PeerId peer : peers) {
                    if (peer.equals(this.serverId)) {
                        continue;
                    }
                    this.replicatorGroup.sendHeartbeat(peer, heartbeatDone);
                }
                break;
            // LeaseRead 策略，能够走到这里，说明集群中有超过半数的节点仍然认可当前 Leader 节点
            case ReadOnlyLeaseBased:
                // Responses to followers and local node.
                respBuilder.setSuccess(true);
                closure.setResponse(respBuilder.build());
                closure.run(Status.OK());
                break;
        }
    }

    @Override
    public void apply(final Task task) {
        // 当前节点被关闭
        if (this.shutdownLatch != null) {
            Utils.runClosureInThread(task.getDone(), new Status(RaftError.ENODESHUTDOWN, "Node is shutting down."));
            throw new IllegalStateException("Node is shutting down");
        }
        Requires.requireNonNull(task, "Null task");

        // 创建一个 LogEntry 对象，用于封装 Task 中的数据
        final LogEntry entry = new LogEntry();
        entry.setData(task.getData());
        int retryTimes = 0;
        try {
            // 将 Task 及其对应的 LogEntry 对象以事件的形式投递给 Disruptor 队列
            final EventTranslator<LogEntryAndClosure> translator = (event, sequence) -> {
                event.reset();
                event.done = task.getDone();
                event.entry = entry;
                event.expectedTerm = task.getExpectedTerm();
            };
            while (true) {
                if (this.applyQueue.tryPublishEvent(translator)) {
                    break;
                } else {
                    // 重试 3 次
                    retryTimes++;
                    if (retryTimes > MAX_APPLY_RETRY_TIMES) {
                        Utils.runClosureInThread(task.getDone(),
                                new Status(RaftError.EBUSY, "Node is busy, has too many tasks."));
                        LOG.warn("Node {} applyQueue is overload.", getNodeId());
                        this.metrics.recordTimes("apply-task-overload-times", 1);
                        return;
                    }
                    ThreadHelper.onSpinWait();
                }
            }

        } catch (final Exception e) {
            LOG.error("Fail to apply task.", e);
            Utils.runClosureInThread(task.getDone(), new Status(RaftError.EPERM, "Node is down."));
        }
    }

    @Override
    public Message handlePreVoteRequest(final RequestVoteRequest request) {
        boolean doUnlock = true;
        this.writeLock.lock();
        try {
            // 当前节点处于非活跃状态，响应错误
            if (!this.state.isActive()) {
                LOG.warn("Node {} is not in active state, currTerm={}.", getNodeId(), this.currTerm);
                return RpcFactoryHelper //
                        .responseFactory() //
                        .newResponse(RequestVoteResponse.getDefaultInstance(), RaftError.EINVAL,
                                "Node %s is not in active state, state %s.", getNodeId(), this.state.name());
            }
            // 解析发起投票的节点 ID
            final PeerId candidateId = new PeerId();
            if (!candidateId.parse(request.getServerId())) {
                // 解析错误，响应错误
                LOG.warn("Node {} received PreVoteRequest from {} serverId bad format.", getNodeId(), request.getServerId());
                return RpcFactoryHelper //
                        .responseFactory() //
                        .newResponse(RequestVoteResponse.getDefaultInstance(), RaftError.EINVAL,
                                "Parse candidateId failed: %s.", request.getServerId());
            }
            boolean granted = false;
            // noinspection ConstantConditions
            do {
                // 当前节点与对应 leader 节点之间的租约仍然有效，拒绝投票
                if (this.leaderId != null && !this.leaderId.isEmpty() && isCurrentLeaderValid()) {
                    LOG.info("Node {} ignore PreVoteRequest from {}, term={}, currTerm={}, because the leader {}'s lease is still valid.",
                            getNodeId(), request.getServerId(), request.getTerm(), this.currTerm, this.leaderId);
                    break;
                }
                // 发起投票节点的 term 值小于当前节点，拒绝投票
                if (request.getTerm() < this.currTerm) {
                    LOG.info("Node {} ignore PreVoteRequest from {}, term={}, currTerm={}.",
                            getNodeId(), request.getServerId(), request.getTerm(), this.currTerm);
                    // A follower replicator may not be started when this node become leader, so we must check it.
                    // 如果当前节点是 leader 节点，检查与发起投票节点之间的复制关系
                    checkReplicator(candidateId);
                    break;
                } else if (request.getTerm() == this.currTerm + 1) {
                    // A follower replicator may not be started when this node become leader, so we must check it.
                    // check replicator state
                    checkReplicator(candidateId);
                }
                doUnlock = false;
                this.writeLock.unlock();

                // 获取本地最新的 LogId
                final LogId lastLogId = this.logManager.getLastLogId(true);

                doUnlock = true;
                this.writeLock.lock();
                // 封装请求中的 logIndex 和 term 值
                final LogId requestLastLogId = new LogId(request.getLastLogIndex(), request.getLastLogTerm());
                // 如果请求的 term 值更大，或者在 term 值相等的前提下，请求的 logIndex 大于等于当前节点的 logIndex 值
                // 则投上自己的一票
                granted = requestLastLogId.compareTo(lastLogId) >= 0;

                LOG.info(
                        "Node {} received PreVoteRequest from {}, term={}, currTerm={}, granted={}, requestLastLogId={}, lastLogId={}.",
                        getNodeId(), request.getServerId(), request.getTerm(), this.currTerm, granted, requestLastLogId,
                        lastLogId);
            } while (false);

            // 响应
            return RequestVoteResponse.newBuilder() //
                    .setTerm(this.currTerm) //
                    .setGranted(granted) //
                    .build();
        } finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
        }
    }

    // in read_lock
    private boolean isLeaderLeaseValid() {
        final long monotonicNowMs = Utils.monotonicMs();
        // 检查距离最近校验当前 Leader 节点有效性的时间是否在租约范围内
        if (checkLeaderLease(monotonicNowMs)) {
            return true;
        }
        // 检查管理的所有 Follower 节点是否有超过半数仍然认为当前 Leader 节点有效
        checkDeadNodes0(this.conf.getConf().getPeers(), monotonicNowMs, false, null);
        return checkLeaderLease(monotonicNowMs);
    }

    private boolean checkLeaderLease(final long monotonicNowMs) {
        // 最近一次向所有活跃 Follower 节点成功发送 RPC 请求的最早时间距离指定时间是否在有效租约范围内
        return monotonicNowMs - this.lastLeaderTimestamp < this.options.getLeaderLeaseTimeoutMs();
    }

    private boolean isCurrentLeaderValid() {
        return Utils.monotonicMs() - this.lastLeaderTimestamp < this.options.getElectionTimeoutMs();
    }

    private void updateLastLeaderTimestamp(final long lastLeaderTimestamp) {
        this.lastLeaderTimestamp = lastLeaderTimestamp;
    }

    private void checkReplicator(final PeerId candidateId) {
        if (this.state == State.STATE_LEADER) {
            // 如果当前节点是 leader，则检查与候选节点之间的复制关系
            this.replicatorGroup.checkReplicator(candidateId, false);
        }
    }

    @Override
    public Message handleRequestVoteRequest(final RequestVoteRequest request) {
        boolean doUnlock = true;
        this.writeLock.lock();
        try {
            // 节点处于非活跃状态，响应错误
            if (!this.state.isActive()) {
                LOG.warn("Node {} is not in active state, currTerm={}.", getNodeId(), this.currTerm);
                return RpcFactoryHelper //
                        .responseFactory() //
                        .newResponse(RequestVoteResponse.getDefaultInstance(), RaftError.EINVAL,
                                "Node %s is not in active state, state %s.", getNodeId(), this.state.name());
            }
            // 解析发起正式选举的节点 ID
            final PeerId candidateId = new PeerId();
            // 解析失败，响应错误
            if (!candidateId.parse(request.getServerId())) {
                LOG.warn("Node {} received RequestVoteRequest from {} serverId bad format.", getNodeId(), request.getServerId());
                return RpcFactoryHelper //
                        .responseFactory() //
                        .newResponse(RequestVoteResponse.getDefaultInstance(), RaftError.EINVAL,
                                "Parse candidateId failed: %s.", request.getServerId());
            }

            // noinspection ConstantConditions
            do {
                // check term
                if (request.getTerm() >= this.currTerm) {
                    LOG.info("Node {} received RequestVoteRequest from {}, term={}, currTerm={}.",
                            getNodeId(), request.getServerId(), request.getTerm(), this.currTerm);
                    // 候选节点的 term 值大于当前节点，执行 stepdown
                    if (request.getTerm() > this.currTerm) {
                        // increase current term, change state to follower
                        stepDown(request.getTerm(), false, new Status(RaftError.EHIGHERTERMRESPONSE,
                                "Raft node receives higher term RequestVoteRequest."));
                    }
                }
                // 候选节点的 term 值小于当前节点，拒绝投票
                else {
                    // ignore older term
                    LOG.info("Node {} ignore RequestVoteRequest from {}, term={}, currTerm={}.",
                            getNodeId(), request.getServerId(), request.getTerm(), this.currTerm);
                    break;
                }
                doUnlock = false;
                this.writeLock.unlock();

                // 从本地获取最新的 logIndex 和对应的 term 值
                final LogId lastLogId = this.logManager.getLastLogId(true);

                doUnlock = true;
                this.writeLock.lock();
                // vote need ABA check after unlock&writeLock
                if (request.getTerm() != this.currTerm) {
                    LOG.warn("Node {} raise term {} when get lastLogId.", getNodeId(), this.currTerm);
                    break;
                }

                // 如果 logIsOk，则说明候选节点的 term 值大于当前节点，或者 term 相同，但是候选节点的 logIndex 不必当前节点小
                final boolean logIsOk = new LogId(request.getLastLogIndex(), request.getLastLogTerm())
                        .compareTo(lastLogId) >= 0;

                // 如果 logIsOk，且当前节点目前没有投票给其它节点
                if (logIsOk && (this.votedId == null || this.votedId.isEmpty())) {
                    stepDown(request.getTerm(), false, new Status(RaftError.EVOTEFORCANDIDATE,
                            "Raft node votes for some candidate, step down to restart election_timer."));
                    this.votedId = candidateId.copy();
                    // 更新本地元数据信息
                    this.metaStorage.setVotedFor(candidateId);
                }
            } while (false);

            // 发送 RequestVote RPC 响应
            return RequestVoteResponse.newBuilder() //
                    .setTerm(this.currTerm) //
                    .setGranted(request.getTerm() == this.currTerm && candidateId.equals(this.votedId)) //
                    .build();
        } finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
        }
    }

    private static class FollowerStableClosure extends LogManager.StableClosure {

        final long committedIndex;
        final AppendEntriesResponse.Builder responseBuilder;
        final NodeImpl node;
        final RpcRequestClosure done;
        final long term;

        public FollowerStableClosure(final AppendEntriesRequest request,
                                     final AppendEntriesResponse.Builder responseBuilder, final NodeImpl node,
                                     final RpcRequestClosure done, final long term) {
            super(null);
            this.committedIndex = Math.min(
                    // committed index is likely less than the lastLogIndex
                    request.getCommittedIndex(),
                    // The logs after the appended entries can not be trust, so we can't commit them even if their indexes are less than request's committed index.
                    request.getPrevLogIndex() + request.getEntriesCount());
            this.responseBuilder = responseBuilder;
            this.node = node;
            this.done = done;
            this.term = term;
        }

        @Override
        public void run(final Status status) {

            if (!status.isOk()) {
                this.done.run(status);
                return;
            }

            this.node.readLock.lock();
            try {
                if (this.term != this.node.currTerm) {
                    // The change of term indicates that leader has been changed during
                    // appending entries, so we can't respond ok to the old leader
                    // because we are not sure if the appended logs would be truncated
                    // by the new leader:
                    //  - If they won't be truncated and we respond failure to the old
                    //    leader, the new leader would know that they are stored in this
                    //    peer and they will be eventually committed when the new leader
                    //    found that quorum of the cluster have stored.
                    //  - If they will be truncated and we responded success to the old
                    //    leader, the old leader would possibly regard those entries as
                    //    committed (very likely in a 3-nodes cluster) and respond
                    //    success to the clients, which would break the rule that
                    //    committed entries would never be truncated.
                    // So we have to respond failure to the old leader and set the new
                    // term to make it stepped down if it didn't.
                    this.responseBuilder.setSuccess(false).setTerm(this.node.currTerm);
                    this.done.sendResponse(this.responseBuilder.build());
                    return;
                }
            } finally {
                // It's safe to release lock as we know everything is ok at this point.
                this.node.readLock.unlock();
            }

            // Don't touch node any more.
            this.responseBuilder.setSuccess(true).setTerm(this.term);

            // Ballot box is thread safe and tolerates disorder.
            this.node.ballotBox.setLastCommittedIndex(this.committedIndex);

            this.done.sendResponse(this.responseBuilder.build());
        }
    }

    @Override
    public Message handleAppendEntriesRequest(final AppendEntriesRequest request, final RpcRequestClosure done) {
        boolean doUnlock = true;
        final long startMs = Utils.monotonicMs();
        this.writeLock.lock();
        final int entriesCount = request.getEntriesCount();
        try {
            // 当前节点处于非活跃状态，响应错误
            if (!this.state.isActive()) {
                LOG.warn("Node {} is not in active state, currTerm={}.", getNodeId(), this.currTerm);
                return RpcFactoryHelper //
                        .responseFactory() //
                        .newResponse(AppendEntriesResponse.getDefaultInstance(), RaftError.EINVAL,
                                "Node %s is not in active state, state %s.", getNodeId(), this.state.name());
            }

            // 解析请求来源节点 ID
            final PeerId serverId = new PeerId();
            if (!serverId.parse(request.getServerId())) {
                // 解析失败，响应错误
                LOG.warn("Node {} received AppendEntriesRequest from {} serverId bad format.", getNodeId(), request.getServerId());
                return RpcFactoryHelper //
                        .responseFactory() //
                        .newResponse(AppendEntriesResponse.getDefaultInstance(),
                                RaftError.EINVAL, "Parse serverId failed: %s.", request.getServerId());
            }

            // 校验请求中的 term 值，如果小于当前节点，则拒绝请求并返回自己当前的 term 值
            if (request.getTerm() < this.currTerm) {
                LOG.warn("Node {} ignore stale AppendEntriesRequest from {}, term={}, currTerm={}.",
                        getNodeId(), request.getServerId(), request.getTerm(), this.currTerm);
                return AppendEntriesResponse.newBuilder() //
                        .setSuccess(false) //
                        .setTerm(this.currTerm) //
                        .build();
            }

            // 基于请求和节点本地状态判断是否需要执行 stepdown
            checkStepDown(request.getTerm(), serverId);

            // 请求来源节点并不是当前节点所知道的 leader 节点，
            // 可能出现网络分区，尝试将 term 值加 1，以触发 leader 节点 stepdown
            if (!serverId.equals(this.leaderId)) {
                LOG.error("Another peer {} declares that it is the leader at term {} which was occupied by leader {}.",
                        serverId, this.currTerm, this.leaderId);
                // Increase the term by 1 and make both leaders step down to minimize the loss of split brain
                stepDown(request.getTerm() + 1, false,
                        new Status(RaftError.ELEADERCONFLICT, "More than one leader in the same term."));
                return AppendEntriesResponse.newBuilder() //
                        .setSuccess(false) //
                        .setTerm(request.getTerm() + 1) //
                        .build();
            }

            // 更新本地记录的最近一次收到来自 leader 节点请求的时间戳
            updateLastLeaderTimestamp(Utils.monotonicMs());

            // 当前是复制日志的 AppendEntries 请求，但是本地正在安装快照，响应错误
            if (entriesCount > 0 && this.snapshotExecutor != null && this.snapshotExecutor.isInstallingSnapshot()) {
                LOG.warn("Node {} received AppendEntriesRequest while installing snapshot.", getNodeId());
                return RpcFactoryHelper //
                        .responseFactory() //
                        .newResponse(AppendEntriesResponse.getDefaultInstance(), RaftError.EBUSY,
                                "Node %s:%s is installing snapshot.", this.groupId, this.serverId);
            }

            final long prevLogIndex = request.getPrevLogIndex();
            final long prevLogTerm = request.getPrevLogTerm();
            final long localPrevLogTerm = this.logManager.getTerm(prevLogIndex);
            // 请求中 logIndex 对应的 term 值与本地不匹配
            if (localPrevLogTerm != prevLogTerm) {
                final long lastLogIndex = this.logManager.getLastLogIndex();
                LOG.warn("Node {} reject term_unmatched AppendEntriesRequest from {}, " +
                                "term={}, prevLogIndex={}, prevLogTerm={}, localPrevLogTerm={}, lastLogIndex={}, entriesSize={}.",
                        getNodeId(), request.getServerId(), request.getTerm(), prevLogIndex, prevLogTerm, localPrevLogTerm, lastLogIndex, entriesCount);

                return AppendEntriesResponse.newBuilder() //
                        .setSuccess(false) //
                        .setTerm(this.currTerm) //
                        .setLastLogIndex(lastLogIndex) //
                        .build();
            }

            // 心跳或者探针请求
            if (entriesCount == 0) {
                // 返回本地当前的 term 值以及对应的 logIndex
                final AppendEntriesResponse.Builder respBuilder = AppendEntriesResponse.newBuilder() //
                        .setSuccess(true) //
                        .setTerm(this.currTerm) //
                        .setLastLogIndex(this.logManager.getLastLogIndex());
                doUnlock = false;
                this.writeLock.unlock();
                // see the comments at FollowerStableClosure#run()
                // 基于 leader 的 committedIndex 更新本地的 lastCommittedIndex 值
                this.ballotBox.setLastCommittedIndex(Math.min(request.getCommittedIndex(), prevLogIndex));
                return respBuilder.build();
            }

            /* 复制日志数据请求 */

            // Parse request
            long index = prevLogIndex;
            final List<LogEntry> entries = new ArrayList<>(entriesCount);
            ByteBuffer allData = null;
            if (request.hasData()) {
                allData = request.getData().asReadOnlyByteBuffer();
            }

            final List<RaftOutter.EntryMeta> entriesList = request.getEntriesList();
            // 遍历逐一解析请求中的 LogEntry 数据，记录到 entries 列表中
            for (int i = 0; i < entriesCount; i++) {
                index++;
                // 获取 LogEntry 元数据信息
                final RaftOutter.EntryMeta entry = entriesList.get(i);

                // 基于元数据和数据体构造 LogEntry 对象
                final LogEntry logEntry = logEntryFromMeta(index, allData, entry);

                if (logEntry != null) {
                    // 如果启用了 checksum 机制，则校验 checksum 值
                    if (this.raftOptions.isEnableLogEntryChecksum() && logEntry.isCorrupted()) {
                        // checksum 值不匹配，说明数据可能被篡改
                        long realChecksum = logEntry.checksum();
                        LOG.error(
                                "Corrupted log entry received from leader, index={}, term={}, expectedChecksum={}, realChecksum={}",
                                logEntry.getId().getIndex(), logEntry.getId().getTerm(), logEntry.getChecksum(), realChecksum);
                        return RpcFactoryHelper //
                                .responseFactory() //
                                .newResponse(AppendEntriesResponse.getDefaultInstance(), RaftError.EINVAL,
                                        "The log entry is corrupted, index=%d, term=%d, expectedChecksum=%d, realChecksum=%d",
                                        logEntry.getId().getIndex(), logEntry.getId().getTerm(), logEntry.getChecksum(), realChecksum);
                    }
                    entries.add(logEntry);
                }
            }

            final FollowerStableClosure closure = new FollowerStableClosure(request,
                    AppendEntriesResponse.newBuilder().setTerm(this.currTerm), this, done, this.currTerm);
            // 将 LogEntry 数据写入本地磁盘
            this.logManager.appendEntries(entries, closure);
            // update configuration after _log_manager updated its memory status
            checkAndSetConfiguration(true);
            return null;
        } finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
            this.metrics.recordLatency("handle-append-entries", Utils.monotonicMs() - startMs);
            this.metrics.recordSize("handle-append-entries-count", entriesCount);
        }
    }

    private LogEntry logEntryFromMeta(final long index, final ByteBuffer allData, final RaftOutter.EntryMeta entry) {
        // 忽略 ENTRY_TYPE_UNKNOWN 类型的 LogEntry 数据
        if (entry.getType() != EnumOutter.EntryType.ENTRY_TYPE_UNKNOWN) {
            // 给 LogEntry 对象填充基本的元数据信息
            final LogEntry logEntry = new LogEntry();
            logEntry.setId(new LogId(index, entry.getTerm()));
            logEntry.setType(entry.getType());
            if (entry.hasChecksum()) {
                logEntry.setChecksum(entry.getChecksum()); // since 1.2.6
            }

            // 基于元数据中记录的数据长度获取对应的 LogEntry 数据体，并填充到 LogEntry 对象中
            final long dataLen = entry.getDataLen();
            if (dataLen > 0) {
                final byte[] bs = new byte[(int) dataLen];
                assert allData != null;
                allData.get(bs, 0, bs.length);
                logEntry.setData(ByteBuffer.wrap(bs));
            }

            // 针对 ENTRY_TYPE_CONFIGURATION 类型的 LogEntry，解析并填充集群节点配置数据

            if (entry.getPeersCount() > 0) {
                if (entry.getType() != EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION) {
                    throw new IllegalStateException(
                            "Invalid log entry that contains peers but is not ENTRY_TYPE_CONFIGURATION type: " + entry.getType());
                }

                // 填充集群节点配置信息
                fillLogEntryPeers(entry, logEntry);
            } else if (entry.getType() == EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION) {
                throw new IllegalStateException(
                        "Invalid log entry that contains zero peers but is ENTRY_TYPE_CONFIGURATION type");
            }
            return logEntry;
        }
        return null;
    }

    private void fillLogEntryPeers(final RaftOutter.EntryMeta entry, final LogEntry logEntry) {
        // TODO refactor
        if (entry.getPeersCount() > 0) {
            final List<PeerId> peers = new ArrayList<>(entry.getPeersCount());
            for (final String peerStr : entry.getPeersList()) {
                final PeerId peer = new PeerId();
                peer.parse(peerStr);
                peers.add(peer);
            }
            logEntry.setPeers(peers);
        }

        if (entry.getOldPeersCount() > 0) {
            final List<PeerId> oldPeers = new ArrayList<>(entry.getOldPeersCount());
            for (final String peerStr : entry.getOldPeersList()) {
                final PeerId peer = new PeerId();
                peer.parse(peerStr);
                oldPeers.add(peer);
            }
            logEntry.setOldPeers(oldPeers);
        }

        if (entry.getLearnersCount() > 0) {
            final List<PeerId> peers = new ArrayList<>(entry.getLearnersCount());
            for (final String peerStr : entry.getLearnersList()) {
                final PeerId peer = new PeerId();
                peer.parse(peerStr);
                peers.add(peer);
            }
            logEntry.setLearners(peers);
        }

        if (entry.getOldLearnersCount() > 0) {
            final List<PeerId> peers = new ArrayList<>(entry.getOldLearnersCount());
            for (final String peerStr : entry.getOldLearnersList()) {
                final PeerId peer = new PeerId();
                peer.parse(peerStr);
                peers.add(peer);
            }
            logEntry.setOldLearners(peers);
        }
    }

    // called when leader receive greater term in AppendEntriesResponse
    void increaseTermTo(final long newTerm, final Status status) {
        this.writeLock.lock();
        try {
            if (newTerm < this.currTerm) {
                return;
            }
            stepDown(newTerm, false, status);
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Peer catch up callback
     *
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-11 2:10:02 PM
     */
    private static class OnCaughtUp extends CatchUpClosure {
        private final NodeImpl node;
        private final long term;
        private final PeerId peer;
        private final long version;

        public OnCaughtUp(final NodeImpl node, final long term, final PeerId peer, final long version) {
            super();
            this.node = node;
            this.term = term;
            this.peer = peer;
            this.version = version;
        }

        @Override
        public void run(final Status status) {
            this.node.onCaughtUp(this.peer, this.term, this.version, status);
        }
    }

    private void onCaughtUp(final PeerId peer, final long term, final long version, final Status st) {
        this.writeLock.lock();
        try {
            // check current_term and state to avoid ABA problem
            if (term != this.currTerm && this.state != State.STATE_LEADER) {
                // term has changed and nothing should be done, otherwise there will be
                // an ABA problem.
                return;
            }
            if (st.isOk()) {
                // Caught up successfully
                this.confCtx.onCaughtUp(version, peer, true);
                return;
            }
            // Retry if this peer is still alive
            if (st.getCode() == RaftError.ETIMEDOUT.getNumber()
                    && Utils.monotonicMs() - this.replicatorGroup.getLastRpcSendTimestamp(peer) <= this.options
                    .getElectionTimeoutMs()) {
                LOG.debug("Node {} waits peer {} to catch up.", getNodeId(), peer);
                final OnCaughtUp caughtUp = new OnCaughtUp(this, term, peer, version);
                final long dueTime = Utils.nowMs() + this.options.getElectionTimeoutMs();
                if (this.replicatorGroup.waitCaughtUp(peer, this.options.getCatchupMargin(), dueTime, caughtUp)) {
                    return;
                }
                LOG.warn("Node {} waitCaughtUp failed, peer={}.", getNodeId(), peer);
            }
            LOG.warn("Node {} caughtUp failed, status={}, peer={}.", getNodeId(), st, peer);
            this.confCtx.onCaughtUp(version, peer, false);
        } finally {
            this.writeLock.unlock();
        }
    }

    private void checkDeadNodes(final Configuration conf, final long monotonicNowMs) {
        // Check learner replicators at first.
        for (PeerId peer : conf.getLearners()) {
            // 确定到所有 Learner 节点的复制关系都建立了
            checkReplicator(peer);
        }
        // Ensure quorum nodes alive.
        final List<PeerId> peers = conf.listPeers();
        final Configuration deadNodes = new Configuration();
        // 如果集群中认同当前 Leader 节点的 Follower 节点数过半，则无需让权
        if (checkDeadNodes0(peers, monotonicNowMs, true, deadNodes)) {
            return;
        }
        LOG.warn("Node {} steps down when alive nodes don't satisfy quorum, " +
                "term={}, deadNodes={}, conf={}.", getNodeId(), this.currTerm, deadNodes, conf);
        final Status status = new Status();
        status.setError(RaftError.ERAFTTIMEDOUT, "Majority of the group dies: %d/%d", deadNodes.size(), peers.size());
        // 集群中认同当前 Leader 节点的 Follower 节点数小于一半，执行让权操作
        stepDown(this.currTerm, false, status);
    }

    private boolean checkDeadNodes0(final List<PeerId> peers,
                                    final long monotonicNowMs,
                                    final boolean checkReplicator,
                                    final Configuration deadNodes) {
        // 获取租约时长，默认为选举超时的 90%
        final int leaderLeaseTimeoutMs = this.options.getLeaderLeaseTimeoutMs();
        int aliveCount = 0;
        // 记录向所有活跃节点发送 RPC 请求的最小时间戳
        long startLease = Long.MAX_VALUE;
        // 遍历逐个检查目标 Follower 节点
        for (final PeerId peer : peers) {
            if (peer.equals(this.serverId)) {
                aliveCount++;
                continue;
            }
            // 检查到目标节点之间的复制关系，避免因为缺失复制关系而误将目标节点判为死亡
            if (checkReplicator) {
                checkReplicator(peer);
            }
            // 获取最近一次成功向目标节点发送 RPC 请求的时间戳
            final long lastRpcSendTimestamp = this.replicatorGroup.getLastRpcSendTimestamp(peer);
            // 到目标节点的租约仍然有效，则视目标节点仍然活跃
            if (monotonicNowMs - lastRpcSendTimestamp <= leaderLeaseTimeoutMs) {
                aliveCount++; // 活跃节点数加 1
                // 更新向所有活跃节点发送 RPC 请求的最小时间戳
                if (startLease > lastRpcSendTimestamp) {
                    startLease = lastRpcSendTimestamp;
                }
                continue;
            }
            // 记录死亡节点
            if (deadNodes != null) {
                deadNodes.addPeer(peer);
            }
        }
        // 活跃节点数过半，说明当前 Leader 节点仍然有效，更新时间戳（向所有活跃节点发送 RPC 请求的最小时间戳）
        if (aliveCount >= peers.size() / 2 + 1) {
            updateLastLeaderTimestamp(startLease);
            return true;
        }
        return false;
    }

    // in read_lock
    private List<PeerId> getAliveNodes(final Collection<PeerId> peers, final long monotonicNowMs) {
        final int leaderLeaseTimeoutMs = this.options.getLeaderLeaseTimeoutMs();
        final List<PeerId> alivePeers = new ArrayList<>();
        for (final PeerId peer : peers) {
            if (peer.equals(this.serverId)) {
                alivePeers.add(peer.copy());
                continue;
            }
            if (monotonicNowMs - this.replicatorGroup.getLastRpcSendTimestamp(peer) <= leaderLeaseTimeoutMs) {
                alivePeers.add(peer.copy());
            }
        }
        return alivePeers;
    }

    private void handleStepDownTimeout() {
        this.writeLock.lock();
        try {
            // 当前节点不是 LEADER 角色，无需让权
            if (this.state.compareTo(State.STATE_TRANSFERRING) > 0) {
                LOG.debug("Node {} stop step-down timer, term={}, state={}.", getNodeId(), this.currTerm, this.state);
                return;
            }
            final long monotonicNowMs = Utils.monotonicMs();
            // 检查集群中是否有超过半数的 Follower 节点仍然在响应自己的心跳请求，如果不是则执行让权操作
            checkDeadNodes(this.conf.getConf(), monotonicNowMs);
            if (!this.conf.getOldConf().isEmpty()) {
                checkDeadNodes(this.conf.getOldConf(), monotonicNowMs);
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Configuration changed callback.
     *
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-11 2:53:43 PM
     */
    private class ConfigurationChangeDone implements Closure {
        private final long term;
        private final boolean leaderStart;

        public ConfigurationChangeDone(final long term, final boolean leaderStart) {
            super();
            this.term = term;
            this.leaderStart = leaderStart;
        }

        @Override
        public void run(final Status status) {
            if (status.isOk()) {
                // 尝试让集群节点配置趋于稳定
                onConfigurationChangeDone(this.term);
                if (this.leaderStart) {
                    // 回调状态机 StateMachine#onLeaderStart 逻辑
                    getOptions().getFsm().onLeaderStart(this.term);
                }
            } else {
                LOG.error("Fail to run ConfigurationChangeDone, status: {}.", status);
            }
        }
    }

    private void unsafeApplyConfiguration(final Configuration newConf, final Configuration oldConf, final boolean leaderStart) {
        Requires.requireTrue(this.confCtx.isBusy(), "ConfigurationContext is not busy");
        final LogEntry entry = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION);
        entry.setId(new LogId(0, this.currTerm));
        entry.setPeers(newConf.listPeers());
        entry.setLearners(newConf.listLearners());
        if (oldConf != null) {
            entry.setOldPeers(oldConf.listPeers());
            entry.setOldLearners(oldConf.listLearners());
        }
        final ConfigurationChangeDone configurationChangeDone = new ConfigurationChangeDone(this.currTerm, leaderStart);
        // Use the new_conf to deal the quorum of this very log
        if (!this.ballotBox.appendPendingTask(newConf, oldConf, configurationChangeDone)) {
            Utils.runClosureInThread(configurationChangeDone, new Status(RaftError.EINTERNAL, "Fail to append task."));
            return;
        }
        final List<LogEntry> entries = new ArrayList<>();
        entries.add(entry);
        this.logManager.appendEntries(entries, new LeaderStableClosure(entries));
        checkAndSetConfiguration(false);
    }

    private void unsafeRegisterConfChange(final Configuration oldConf, final Configuration newConf, final Closure done) {

        Requires.requireTrue(newConf.isValid(), "Invalid new conf: %s", newConf);
        // The new conf entry(will be stored in log manager) should be valid
        Requires.requireTrue(new ConfigurationEntry(null, newConf, oldConf).isValid(), "Invalid conf entry: %s",
                newConf);

        if (this.state != State.STATE_LEADER) {
            LOG.warn("Node {} refused configuration changing as the state={}.", getNodeId(), this.state);
            if (done != null) {
                final Status status = new Status();
                if (this.state == State.STATE_TRANSFERRING) {
                    status.setError(RaftError.EBUSY, "Is transferring leadership.");
                } else {
                    status.setError(RaftError.EPERM, "Not leader");
                }
                Utils.runClosureInThread(done, status);
            }
            return;
        }
        // check concurrent conf change
        if (this.confCtx.isBusy()) {
            LOG.warn("Node {} refused configuration concurrent changing.", getNodeId());
            if (done != null) {
                Utils.runClosureInThread(done, new Status(RaftError.EBUSY, "Doing another configuration change."));
            }
            return;
        }
        // Return immediately when the new peers equals to current configuration
        if (this.conf.getConf().equals(newConf)) {
            Utils.runClosureInThread(done);
            return;
        }
        this.confCtx.start(oldConf, newConf, done);
    }

    private void afterShutdown() {
        List<Closure> savedDoneList = null;
        this.writeLock.lock();
        try {
            if (!this.shutdownContinuations.isEmpty()) {
                savedDoneList = new ArrayList<>(this.shutdownContinuations);
            }
            if (this.logStorage != null) {
                this.logStorage.shutdown();
            }
            this.state = State.STATE_SHUTDOWN;
        } finally {
            this.writeLock.unlock();
        }
        if (savedDoneList != null) {
            for (final Closure closure : savedDoneList) {
                Utils.runClosureInThread(closure);
            }
        }
    }

    @Override
    public NodeOptions getOptions() {
        return this.options;
    }

    public Scheduler getTimerManager() {
        return this.timerManager;
    }

    @Override
    public RaftOptions getRaftOptions() {
        return this.raftOptions;
    }

    @OnlyForTest
    long getCurrentTerm() {
        this.readLock.lock();
        try {
            return this.currTerm;
        } finally {
            this.readLock.unlock();
        }
    }

    @OnlyForTest
    ConfigurationEntry getConf() {
        this.readLock.lock();
        try {
            return this.conf;
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public void shutdown() {
        shutdown(null);
    }

    public void onConfigurationChangeDone(final long term) {
        this.writeLock.lock();
        try {
            // 期间 term 值发生变更，或者当前节点已经不是 Leader，直接跳过
            if (term != this.currTerm || this.state.compareTo(State.STATE_TRANSFERRING) > 0) {
                LOG.warn("Node {} process onConfigurationChangeDone at term {} while state={}, currTerm={}.",
                        getNodeId(), term, this.state, this.currTerm);
                return;
            }
            // 将集群配置状态切换到下一个阶段
            this.confCtx.nextStage();
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public PeerId getLeaderId() {
        this.readLock.lock();
        try {
            return this.leaderId.isEmpty() ? null : this.leaderId;
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public String getGroupId() {
        return this.groupId;
    }

    public PeerId getServerId() {
        return this.serverId;
    }

    @Override
    public NodeId getNodeId() {
        if (this.nodeId == null) {
            this.nodeId = new NodeId(this.groupId, this.serverId);
        }
        return this.nodeId;
    }

    public RaftClientService getRpcService() {
        return this.rpcService;
    }

    public void onError(final RaftException error) {
        LOG.warn("Node {} got error: {}.", getNodeId(), error);
        if (this.fsmCaller != null) {
            // onError of fsmCaller is guaranteed to be executed once.
            this.fsmCaller.onError(error);
        }
        if (this.readOnlyService != null) {
            this.readOnlyService.setError(error);
        }
        this.writeLock.lock();
        try {
            // If it is leader, need to wake up a new one;
            // If it is follower, also step down to call on_stop_following.
            if (this.state.compareTo(State.STATE_FOLLOWER) <= 0) {
                stepDown(this.currTerm, this.state == State.STATE_LEADER, new Status(RaftError.EBADNODE,
                        "Raft node(leader or candidate) is in error."));
            }
            if (this.state.compareTo(State.STATE_ERROR) < 0) {
                this.state = State.STATE_ERROR;
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    public void handleRequestVoteResponse(final PeerId peerId, final long term, final RequestVoteResponse response) {
        this.writeLock.lock();
        try {
            // 当前节点以及不是 CANDIDATE 角色，可能以及竞选成功，或者被打回 FOLLOWER 角色了，忽略响应
            if (this.state != State.STATE_CANDIDATE) {
                LOG.warn("Node {} received invalid RequestVoteResponse from {}, state not in STATE_CANDIDATE but {}.",
                        getNodeId(), peerId, this.state);
                return;
            }

            // 期间 term 值已经发生变化，忽略响应
            if (term != this.currTerm) {
                LOG.warn("Node {} received stale RequestVoteResponse from {}, term={}, currTerm={}.",
                        getNodeId(), peerId, term, this.currTerm);
                return;
            }

            // 目标节点的 term 值比当前节点大，需要执行 stepdown
            if (response.getTerm() > this.currTerm) {
                LOG.warn("Node {} received invalid RequestVoteResponse from {}, term={}, expect={}.",
                        getNodeId(), peerId, response.getTerm(), this.currTerm);
                stepDown(response.getTerm(), false, new Status(RaftError.EHIGHERTERMRESPONSE,
                        "Raft node receives higher term request_vote_response."));
                return;
            }
            // check granted quorum?
            if (response.getGranted()) {
                this.voteCtx.grant(peerId);
                // 如果票数过半，则竞选成功
                if (this.voteCtx.isGranted()) {
                    becomeLeader();
                }
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    private class OnRequestVoteRpcDone extends RpcResponseClosureAdapter<RequestVoteResponse> {

        final long startMs;
        final PeerId peer;
        final long term;
        final NodeImpl node;
        RequestVoteRequest request;

        public OnRequestVoteRpcDone(final PeerId peer, final long term, final NodeImpl node) {
            super();
            this.startMs = Utils.monotonicMs();
            this.peer = peer;
            this.term = term;
            this.node = node;
        }

        @Override
        public void run(final Status status) {
            NodeImpl.this.metrics.recordLatency("request-vote", Utils.monotonicMs() - this.startMs);
            if (!status.isOk()) {
                LOG.warn("Node {} RequestVote to {} error: {}.", this.node.getNodeId(), this.peer, status);
            } else {
                this.node.handleRequestVoteResponse(this.peer, this.term, getResponse());
            }
        }
    }

    /**
     * 处理 RequestVoteResponse 响应
     *
     * @param peerId
     * @param term
     * @param response
     */
    public void handlePreVoteResponse(final PeerId peerId, final long term, final RequestVoteResponse response) {
        boolean doUnlock = true;
        this.writeLock.lock();
        try {
            // 当前节点已经不是 Follower 角色，可能已经预选举成功了，忽略响应
            if (this.state != State.STATE_FOLLOWER) {
                LOG.warn("Node {} received invalid PreVoteResponse from {}, " +
                        "state not in STATE_FOLLOWER but {}.", getNodeId(), peerId, this.state);
                return;
            }
            // 当前节点的 term 值已经发生变化，忽略响应
            if (term != this.currTerm) {
                LOG.warn("Node {} received invalid PreVoteResponse from {}, " +
                        "term={}, currTerm={}.", getNodeId(), peerId, term, this.currTerm);
                return;
            }
            // 目标节点的 term 值较当前节点更大，需要 stepdown，主要是更新本地的 term 值
            if (response.getTerm() > this.currTerm) {
                LOG.warn("Node {} received invalid PreVoteResponse from {}, term {}, expect={}.",
                        getNodeId(), peerId, response.getTerm(), this.currTerm);
                stepDown(response.getTerm(), false, new Status(RaftError.EHIGHERTERMRESPONSE,
                        "Raft node receives higher term pre_vote_response."));
                return;
            }
            LOG.info("Node {} received PreVoteResponse from {}, term={}, granted={}.",
                    getNodeId(), peerId, response.getTerm(), response.getGranted());
            // check granted quorum?
            if (response.getGranted()) {
                // 目标节点同意投票
                this.prevVoteCtx.grant(peerId);
                // 检查是否预选举成功
                if (this.prevVoteCtx.isGranted()) {
                    doUnlock = false;
                    // 进入正式投票环境
                    electSelf();
                }
            }
        } finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
        }
    }

    private class OnPreVoteRpcDone extends RpcResponseClosureAdapter<RequestVoteResponse> {

        final long startMs;
        final PeerId peer;
        final long term;
        RequestVoteRequest request;

        public OnPreVoteRpcDone(final PeerId peer, final long term) {
            super();
            this.startMs = Utils.monotonicMs();
            this.peer = peer;
            this.term = term;
        }

        @Override
        public void run(final Status status) {
            NodeImpl.this.metrics.recordLatency("pre-vote", Utils.monotonicMs() - this.startMs);
            if (!status.isOk()) {
                LOG.warn("Node {} PreVote to {} error: {}.", getNodeId(), this.peer, status);
            } else {
                handlePreVoteResponse(this.peer, this.term, getResponse());
            }
        }
    }

    // in writeLock
    private void preVote() {
        long oldTerm;
        try {
            LOG.info("Node {} term {} start preVote.", getNodeId(), this.currTerm);
            // 当前节点正在安装快照，则放弃预选举
            if (this.snapshotExecutor != null && this.snapshotExecutor.isInstallingSnapshot()) {
                LOG.warn("Node {} term {} doesn't do preVote when installing snapshot as the configuration may be out of date.",
                        getNodeId(), this.currTerm);
                return;
            }
            // 当前节点不是一个有效的节点
            if (!this.conf.contains(this.serverId)) {
                LOG.warn("Node {} can't do preVote as it is not in conf <{}>.", getNodeId(), this.conf);
                return;
            }
            oldTerm = this.currTerm;
        } finally {
            this.writeLock.unlock();
        }

        // 从本地磁盘获取最新的 LogId
        final LogId lastLogId = this.logManager.getLastLogId(true);

        boolean doUnlock = true;
        this.writeLock.lock();
        try {
            // pre_vote need defense ABA after unlock&writeLock
            if (oldTerm != this.currTerm) {
                LOG.warn("Node {} raise term {} when get lastLogId.", getNodeId(), this.currTerm);
                return;
            }

            // 初始化预选举选票
            this.prevVoteCtx.init(this.conf.getConf(), this.conf.isStable() ? null : this.conf.getOldConf());
            // 遍历向除自己以外的所有连通节点发送 RequestVote RPC 请求，以征集选票
            for (final PeerId peer : this.conf.listPeers()) {
                if (peer.equals(this.serverId)) {
                    continue;
                }
                if (!this.rpcService.connect(peer.getEndpoint())) {
                    LOG.warn("Node {} channel init failed, address={}.", getNodeId(), peer.getEndpoint());
                    continue;
                }
                final OnPreVoteRpcDone done = new OnPreVoteRpcDone(peer, this.currTerm);
                done.request = RequestVoteRequest.newBuilder() //
                        .setPreVote(true) // it's a pre-vote request. 标记为预选举
                        .setGroupId(this.groupId) //
                        .setServerId(this.serverId.toString()) //
                        .setPeerId(peer.toString()) //
                        .setTerm(this.currTerm + 1) // next term， 预选举阶段不会真正递增 term 值
                        .setLastLogIndex(lastLogId.getIndex()) //
                        .setLastLogTerm(lastLogId.getTerm()) //
                        .build();
                // 发送请求
                this.rpcService.preVote(peer.getEndpoint(), done.request, done);
            }
            // 自己给自己投上一票
            this.prevVoteCtx.grant(this.serverId);
            // 检查是否赢得选票
            if (this.prevVoteCtx.isGranted()) {
                doUnlock = false;
                // 如果赢得选票，则继续发起选举进程
                electSelf();
            }
        } finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
        }
    }

    private void handleVoteTimeout() {
        this.writeLock.lock();
        if (this.state != State.STATE_CANDIDATE) {
            this.writeLock.unlock();
            return;
        }

        if (this.raftOptions.isStepDownWhenVoteTimedout()) {
            LOG.warn(
                    "Candidate node {} term {} steps down when election reaching vote timeout: fail to get quorum vote-granted.",
                    this.nodeId, this.currTerm);
            stepDown(this.currTerm, false, new Status(RaftError.ETIMEDOUT,
                    "Vote timeout: fail to get quorum vote-granted."));
            // unlock in preVote
            preVote();
        } else {
            LOG.debug("Node {} term {} retry to vote self.", getNodeId(), this.currTerm);
            // unlock in electSelf
            electSelf();
        }
    }

    @Override
    public boolean isLeader() {
        this.readLock.lock();
        try {
            return this.state == State.STATE_LEADER;
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public void shutdown(final Closure done) {
        List<RepeatedTimer> timers = null;
        this.writeLock.lock();
        try {
            LOG.info("Node {} shutdown, currTerm={} state={}.", getNodeId(), this.currTerm, this.state);
            if (this.state.compareTo(State.STATE_SHUTTING) < 0) {
                NodeManager.getInstance().remove(this);
                // If it is leader, set the wakeup_a_candidate with true;
                // If it is follower, call on_stop_following in step_down
                if (this.state.compareTo(State.STATE_FOLLOWER) <= 0) {
                    stepDown(this.currTerm, this.state == State.STATE_LEADER,
                            new Status(RaftError.ESHUTDOWN, "Raft node is going to quit."));
                }
                this.state = State.STATE_SHUTTING;
                // Stop all timers
                timers = stopAllTimers();
                if (this.readOnlyService != null) {
                    this.readOnlyService.shutdown();
                }
                if (this.logManager != null) {
                    this.logManager.shutdown();
                }
                if (this.metaStorage != null) {
                    this.metaStorage.shutdown();
                }
                if (this.snapshotExecutor != null) {
                    this.snapshotExecutor.shutdown();
                }
                if (this.wakingCandidate != null) {
                    Replicator.stop(this.wakingCandidate);
                }
                if (this.fsmCaller != null) {
                    this.fsmCaller.shutdown();
                }
                if (this.rpcService != null) {
                    this.rpcService.shutdown();
                }
                if (this.applyQueue != null) {
                    final CountDownLatch latch = new CountDownLatch(1);
                    this.shutdownLatch = latch;
                    Utils.runInThread(
                            () -> this.applyQueue.publishEvent((event, sequence) -> event.shutdownLatch = latch));
                } else {
                    final int num = GLOBAL_NUM_NODES.decrementAndGet();
                    LOG.info("The number of active nodes decrement to {}.", num);
                }
                if (this.timerManager != null) {
                    this.timerManager.shutdown();
                }
            }

            if (this.state != State.STATE_SHUTDOWN) {
                if (done != null) {
                    this.shutdownContinuations.add(done);
                }
                return;
            }

            // This node is down, it's ok to invoke done right now. Don't invoke this
            // in place to avoid the dead writeLock issue when done.Run() is going to acquire
            // a writeLock which is already held by the caller
            if (done != null) {
                Utils.runClosureInThread(done);
            }
        } finally {
            this.writeLock.unlock();

            // Destroy all timers out of lock
            if (timers != null) {
                destroyAllTimers(timers);
            }
        }
    }

    // Should in lock
    private List<RepeatedTimer> stopAllTimers() {
        final List<RepeatedTimer> timers = new ArrayList<>();
        if (this.electionTimer != null) {
            this.electionTimer.stop();
            timers.add(this.electionTimer);
        }
        if (this.voteTimer != null) {
            this.voteTimer.stop();
            timers.add(this.voteTimer);
        }
        if (this.stepDownTimer != null) {
            this.stepDownTimer.stop();
            timers.add(this.stepDownTimer);
        }
        if (this.snapshotTimer != null) {
            this.snapshotTimer.stop();
            timers.add(this.snapshotTimer);
        }
        return timers;
    }

    private void destroyAllTimers(final List<RepeatedTimer> timers) {
        for (final RepeatedTimer timer : timers) {
            timer.destroy();
        }
    }

    @Override
    public synchronized void join() throws InterruptedException {
        if (this.shutdownLatch != null) {
            if (this.readOnlyService != null) {
                this.readOnlyService.join();
            }
            if (this.logManager != null) {
                this.logManager.join();
            }
            if (this.snapshotExecutor != null) {
                this.snapshotExecutor.join();
            }
            if (this.wakingCandidate != null) {
                Replicator.join(this.wakingCandidate);
            }
            this.shutdownLatch.await();
            this.applyDisruptor.shutdown();
            this.shutdownLatch = null;
        }
        if (this.fsmCaller != null) {
            this.fsmCaller.join();
        }
    }

    private static class StopTransferArg {
        final NodeImpl node;
        final long term;
        final PeerId peer;

        public StopTransferArg(final NodeImpl node, final long term, final PeerId peer) {
            super();
            this.node = node;
            this.term = term;
            this.peer = peer;
        }
    }

    private void handleTransferTimeout(final long term, final PeerId peer) {
        LOG.info("Node {} failed to transfer leadership to peer {}, reached timeout.", getNodeId(), peer);
        this.writeLock.lock();
        try {
            if (term == this.currTerm) {
                this.replicatorGroup.stopTransferLeadership(peer);
                if (this.state == State.STATE_TRANSFERRING) {
                    this.fsmCaller.onLeaderStart(term);
                    this.state = State.STATE_LEADER;
                    this.stopTransferArg = null;
                }
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    private void onTransferTimeout(final StopTransferArg arg) {
        arg.node.handleTransferTimeout(arg.term, arg.peer);
    }

    /**
     * Retrieve current configuration this node seen so far. It's not a reliable way to
     * retrieve cluster peers info, you should use {@link #listPeers()} instead.
     *
     * @return current configuration.
     * @since 1.0.3
     */
    public Configuration getCurrentConf() {
        this.readLock.lock();
        try {
            if (this.conf != null && this.conf.getConf() != null) {
                return this.conf.getConf().copy();
            }
            return null;
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public List<PeerId> listPeers() {
        this.readLock.lock();
        try {
            if (this.state != State.STATE_LEADER) {
                throw new IllegalStateException("Not leader");
            }
            return this.conf.getConf().listPeers();
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public List<PeerId> listAlivePeers() {
        this.readLock.lock();
        try {
            if (this.state != State.STATE_LEADER) {
                throw new IllegalStateException("Not leader");
            }
            return getAliveNodes(this.conf.getConf().getPeers(), Utils.monotonicMs());
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public List<PeerId> listLearners() {
        this.readLock.lock();
        try {
            if (this.state != State.STATE_LEADER) {
                throw new IllegalStateException("Not leader");
            }
            return this.conf.getConf().listLearners();
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public List<PeerId> listAliveLearners() {
        this.readLock.lock();
        try {
            if (this.state != State.STATE_LEADER) {
                throw new IllegalStateException("Not leader");
            }
            return getAliveNodes(this.conf.getConf().getLearners(), Utils.monotonicMs());
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public void addPeer(final PeerId peer, final Closure done) {
        Requires.requireNonNull(peer, "Null peer");
        this.writeLock.lock();
        try {
            Requires.requireTrue(!this.conf.getConf().contains(peer), "Peer already exists in current configuration");

            final Configuration newConf = new Configuration(this.conf.getConf());
            newConf.addPeer(peer);
            unsafeRegisterConfChange(this.conf.getConf(), newConf, done);
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void removePeer(final PeerId peer, final Closure done) {
        Requires.requireNonNull(peer, "Null peer");
        this.writeLock.lock();
        try {
            Requires.requireTrue(this.conf.getConf().contains(peer), "Peer not found in current configuration");

            final Configuration newConf = new Configuration(this.conf.getConf());
            newConf.removePeer(peer);
            unsafeRegisterConfChange(this.conf.getConf(), newConf, done);
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void changePeers(final Configuration newPeers, final Closure done) {
        Requires.requireNonNull(newPeers, "Null new peers");
        Requires.requireTrue(!newPeers.isEmpty(), "Empty new peers");
        this.writeLock.lock();
        try {
            LOG.info("Node {} change peers from {} to {}.", getNodeId(), this.conf.getConf(), newPeers);
            unsafeRegisterConfChange(this.conf.getConf(), newPeers, done);
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public Status resetPeers(final Configuration newPeers) {
        Requires.requireNonNull(newPeers, "Null new peers");
        Requires.requireTrue(!newPeers.isEmpty(), "Empty new peers");
        Requires.requireTrue(newPeers.isValid(), "Invalid new peers: %s", newPeers);
        this.writeLock.lock();
        try {
            if (newPeers.isEmpty()) {
                LOG.warn("Node {} set empty peers.", getNodeId());
                return new Status(RaftError.EINVAL, "newPeers is empty");
            }
            if (!this.state.isActive()) {
                LOG.warn("Node {} is in state {}, can't set peers.", getNodeId(), this.state);
                return new Status(RaftError.EPERM, "Bad state: %s", this.state);
            }
            // bootstrap?
            if (this.conf.getConf().isEmpty()) {
                LOG.info("Node {} set peers to {} from empty.", getNodeId(), newPeers);
                this.conf.setConf(newPeers);
                stepDown(this.currTerm + 1, false, new Status(RaftError.ESETPEER, "Set peer from empty configuration"));
                return Status.OK();
            }
            if (this.state == State.STATE_LEADER && this.confCtx.isBusy()) {
                LOG.warn("Node {} set peers need wait current conf changing.", getNodeId());
                return new Status(RaftError.EBUSY, "Changing to another configuration");
            }
            // check equal, maybe retry direct return
            if (this.conf.getConf().equals(newPeers)) {
                return Status.OK();
            }
            final Configuration newConf = new Configuration(newPeers);
            LOG.info("Node {} set peers from {} to {}.", getNodeId(), this.conf.getConf(), newPeers);
            this.conf.setConf(newConf);
            this.conf.getOldConf().reset();
            stepDown(this.currTerm + 1, false, new Status(RaftError.ESETPEER, "Raft node set peer normally"));
            return Status.OK();
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void addLearners(final List<PeerId> learners, final Closure done) {
        checkPeers(learners);
        this.writeLock.lock();
        try {
            final Configuration newConf = new Configuration(this.conf.getConf());
            for (final PeerId peer : learners) {
                newConf.addLearner(peer);
            }
            unsafeRegisterConfChange(this.conf.getConf(), newConf, done);
        } finally {
            this.writeLock.unlock();
        }

    }

    private void checkPeers(final List<PeerId> peers) {
        Requires.requireNonNull(peers, "Null peers");
        Requires.requireTrue(!peers.isEmpty(), "Empty peers");
        for (final PeerId peer : peers) {
            Requires.requireNonNull(peer, "Null peer");
        }
    }

    @Override
    public void removeLearners(final List<PeerId> learners, final Closure done) {
        checkPeers(learners);
        this.writeLock.lock();
        try {
            final Configuration newConf = new Configuration(this.conf.getConf());
            for (final PeerId peer : learners) {
                newConf.removeLearner(peer);
            }
            unsafeRegisterConfChange(this.conf.getConf(), newConf, done);
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void resetLearners(final List<PeerId> learners, final Closure done) {
        checkPeers(learners);
        this.writeLock.lock();
        try {
            final Configuration newConf = new Configuration(this.conf.getConf());
            newConf.setLearners(new LinkedHashSet<>(learners));
            unsafeRegisterConfChange(this.conf.getConf(), newConf, done);
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void snapshot(final Closure done) {
        doSnapshot(done);
    }

    /**
     * 生成快照，可能是快照计时器超时触发，也可以是 CLI 主动调用
     *
     * @param done
     */
    private void doSnapshot(final Closure done) {
        if (this.snapshotExecutor != null) {
            // 调用 SnapshotExecutor 生成快照
            this.snapshotExecutor.doSnapshot(done);
        } else {
            if (done != null) {
                final Status status = new Status(RaftError.EINVAL, "Snapshot is not supported");
                Utils.runClosureInThread(done, status);
            }
        }
    }

    @Override
    public void resetElectionTimeoutMs(final int electionTimeoutMs) {
        Requires.requireTrue(electionTimeoutMs > 0, "Invalid electionTimeoutMs");
        this.writeLock.lock();
        try {
            this.options.setElectionTimeoutMs(electionTimeoutMs);
            this.replicatorGroup.resetHeartbeatInterval(heartbeatTimeout(this.options.getElectionTimeoutMs()));
            this.replicatorGroup.resetElectionTimeoutInterval(electionTimeoutMs);
            LOG.info("Node {} reset electionTimeout, currTimer {} state {} new electionTimeout {}.", getNodeId(),
                    this.currTerm, this.state, electionTimeoutMs);
            this.electionTimer.reset(electionTimeoutMs);
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public Status transferLeadershipTo(final PeerId peer) {
        Requires.requireNonNull(peer, "Null peer");
        this.writeLock.lock();
        try {
            if (this.state != State.STATE_LEADER) {
                LOG.warn("Node {} can't transfer leadership to peer {} as it is in state {}.", getNodeId(), peer, this.state);
                return new Status(this.state == State.STATE_TRANSFERRING ? RaftError.EBUSY : RaftError.EPERM, "Not a leader");
            }
            if (this.confCtx.isBusy()) {
                // It's very messy to deal with the case when the |peer| received
                // TimeoutNowRequest and increase the term while somehow another leader
                // which was not replicated with the newest configuration has been
                // elected. If no add_peer with this very |peer| is to be invoked ever
                // after nor this peer is to be killed, this peer will spin in the voting
                // procedure and make the each new leader stepped down when the peer
                // reached vote timeout and it starts to vote (because it will increase
                // the term of the group)
                // To make things simple, refuse the operation and force users to
                // invoke transfer_leadership_to after configuration changing is
                // completed so that the peer's configuration is up-to-date when it
                // receives the TimeOutNowRequest.
                LOG.warn(
                        "Node {} refused to transfer leadership to peer {} when the leader is changing the configuration.",
                        getNodeId(), peer);
                return new Status(RaftError.EBUSY, "Changing the configuration");
            }

            PeerId peerId = peer.copy();
            // if peer_id is ANY_PEER(0.0.0.0:0:0), the peer with the largest
            // last_log_id will be selected.
            if (peerId.equals(PeerId.ANY_PEER)) {
                LOG.info("Node {} starts to transfer leadership to any peer.", getNodeId());
                if ((peerId = this.replicatorGroup.findTheNextCandidate(this.conf)) == null) {
                    return new Status(-1, "Candidate not found for any peer");
                }
            }
            if (peerId.equals(this.serverId)) {
                LOG.info("Node {} transferred leadership to self.", this.serverId);
                return Status.OK();
            }
            if (!this.conf.contains(peerId)) {
                LOG.info("Node {} refused to transfer leadership to peer {} as it is not in {}.", getNodeId(), peer,
                        this.conf);
                return new Status(RaftError.EINVAL, "Not in current configuration");
            }

            final long lastLogIndex = this.logManager.getLastLogIndex();
            if (!this.replicatorGroup.transferLeadershipTo(peerId, lastLogIndex)) {
                LOG.warn("No such peer {}.", peer);
                return new Status(RaftError.EINVAL, "No such peer %s", peer);
            }
            this.state = State.STATE_TRANSFERRING;
            final Status status = new Status(RaftError.ETRANSFERLEADERSHIP,
                    "Raft leader is transferring leadership to %s", peerId);
            onLeaderStop(status);
            LOG.info("Node {} starts to transfer leadership to peer {}.", getNodeId(), peer);
            final StopTransferArg stopArg = new StopTransferArg(this, this.currTerm, peerId);
            this.stopTransferArg = stopArg;
            this.transferTimer = this.timerManager.schedule(
                    () -> onTransferTimeout(stopArg),
                    this.options.getElectionTimeoutMs(), TimeUnit.MILLISECONDS);

        } finally {
            this.writeLock.unlock();
        }
        return Status.OK();
    }

    private void onLeaderStop(final Status status) {
        this.replicatorGroup.clearFailureReplicators();
        this.fsmCaller.onLeaderStop(status);
    }

    @Override
    public Message handleTimeoutNowRequest(final TimeoutNowRequest request, final RpcRequestClosure done) {
        boolean doUnlock = true;
        this.writeLock.lock();
        try {
            if (request.getTerm() != this.currTerm) {
                final long savedCurrTerm = this.currTerm;
                if (request.getTerm() > this.currTerm) {
                    stepDown(request.getTerm(), false, new Status(RaftError.EHIGHERTERMREQUEST,
                            "Raft node receives higher term request"));
                }
                LOG.info("Node {} received TimeoutNowRequest from {} while currTerm={} didn't match requestTerm={}.",
                        getNodeId(), request.getPeerId(), savedCurrTerm, request.getTerm());
                return TimeoutNowResponse.newBuilder() //
                        .setTerm(this.currTerm) //
                        .setSuccess(false) //
                        .build();
            }
            if (this.state != State.STATE_FOLLOWER) {
                LOG.info("Node {} received TimeoutNowRequest from {}, while state={}, term={}.", getNodeId(),
                        request.getServerId(), this.state, this.currTerm);
                return TimeoutNowResponse.newBuilder() //
                        .setTerm(this.currTerm) //
                        .setSuccess(false) //
                        .build();
            }

            final long savedTerm = this.currTerm;
            final TimeoutNowResponse resp = TimeoutNowResponse.newBuilder() //
                    .setTerm(this.currTerm + 1) //
                    .setSuccess(true) //
                    .build();
            // Parallelize response and election
            done.sendResponse(resp);
            doUnlock = false;
            electSelf();
            LOG.info("Node {} received TimeoutNowRequest from {}, term={}.", getNodeId(), request.getServerId(),
                    savedTerm);
        } finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
        }
        return null;
    }

    @Override
    public Message handleInstallSnapshot(final InstallSnapshotRequest request, final RpcRequestClosure done) {
        // 当前节点未启动快照机制
        if (this.snapshotExecutor == null) {
            return RpcFactoryHelper //
                    .responseFactory() //
                    .newResponse(InstallSnapshotResponse.getDefaultInstance(), RaftError.EINVAL, "Not supported snapshot");
        }

        // 解析请求来源节点 ID
        final PeerId serverId = new PeerId();
        if (!serverId.parse(request.getServerId())) {
            // 解析失败
            LOG.warn("Node {} ignore InstallSnapshotRequest from {} bad server id.", getNodeId(), request.getServerId());
            return RpcFactoryHelper //
                    .responseFactory() //
                    .newResponse(InstallSnapshotResponse.getDefaultInstance(),
                            RaftError.EINVAL, "Parse serverId failed: %s", request.getServerId());
        }

        this.writeLock.lock();
        try {
            // 当前节点处于非活跃状态
            if (!this.state.isActive()) {
                LOG.warn("Node {} ignore InstallSnapshotRequest as it is not in active state {}.", getNodeId(), this.state);
                return RpcFactoryHelper //
                        .responseFactory() //
                        .newResponse(InstallSnapshotResponse.getDefaultInstance(), RaftError.EINVAL,
                                "Node %s:%s is not in active state, state %s.", this.groupId, this.serverId, this.state.name());
            }

            // 请求节点的 term 值小于当前节点
            if (request.getTerm() < this.currTerm) {
                LOG.warn("Node {} ignore stale InstallSnapshotRequest from {}, term={}, currTerm={}.",
                        getNodeId(), request.getPeerId(), request.getTerm(), this.currTerm);
                return InstallSnapshotResponse.newBuilder() //
                        .setTerm(this.currTerm) //
                        .setSuccess(false) //
                        .build();
            }

            // 基于请求和节点本地状态判断是否需要执行 stepdown
            checkStepDown(request.getTerm(), serverId);

            // 请求来源节点并不是当前节点所知道的 leader 节点，
            // 可能出现网络分区，尝试将 term 值加 1，以触发 leader 节点 stepdown
            if (!serverId.equals(this.leaderId)) {
                LOG.error("Another peer {} declares that it is the leader at term {} which was occupied by leader {}.",
                        serverId, this.currTerm, this.leaderId);
                // Increase the term by 1 and make both leaders step down to minimize the loss of split brain
                stepDown(request.getTerm() + 1, false,
                        new Status(RaftError.ELEADERCONFLICT, "More than one leader in the same term."));
                return InstallSnapshotResponse.newBuilder() //
                        .setTerm(request.getTerm() + 1) //
                        .setSuccess(false) //
                        .build();
            }
        } finally {
            this.writeLock.unlock();
        }
        final long startMs = Utils.monotonicMs();
        try {
            if (LOG.isInfoEnabled()) {
                LOG.info(
                        "Node {} received InstallSnapshotRequest from {}, lastIncludedLogIndex={}, lastIncludedLogTerm={}, lastLogId={}.",
                        getNodeId(), request.getServerId(), request.getMeta().getLastIncludedIndex(), request.getMeta().getLastIncludedTerm(), this.logManager.getLastLogId(false));
            }
            // 调用快照执行器 SnapshotExecutor 安装快照
            this.snapshotExecutor.installSnapshot(request, InstallSnapshotResponse.newBuilder(), done);
            return null;
        } finally {
            this.metrics.recordLatency("install-snapshot", Utils.monotonicMs() - startMs);
        }
    }

    public void updateConfigurationAfterInstallingSnapshot() {
        checkAndSetConfiguration(false);
    }

    private void stopReplicator(final Collection<PeerId> keep, final Collection<PeerId> drop) {
        if (drop != null) {
            for (final PeerId peer : drop) {
                if (!keep.contains(peer) && !peer.equals(this.serverId)) {
                    this.replicatorGroup.stopReplicator(peer);
                }
            }
        }
    }

    @Override
    public UserLog readCommittedUserLog(final long index) {
        if (index <= 0) {
            throw new LogIndexOutOfBoundsException("Request index is invalid: " + index);
        }

        final long savedLastAppliedIndex = this.fsmCaller.getLastAppliedIndex();

        if (index > savedLastAppliedIndex) {
            throw new LogIndexOutOfBoundsException("Request index " + index + " is greater than lastAppliedIndex: "
                    + savedLastAppliedIndex);
        }

        long curIndex = index;
        LogEntry entry = this.logManager.getEntry(curIndex);
        if (entry == null) {
            throw new LogNotFoundException("User log is deleted at index: " + index);
        }

        do {
            if (entry.getType() == EnumOutter.EntryType.ENTRY_TYPE_DATA) {
                return new UserLog(curIndex, entry.getData());
            } else {
                curIndex++;
            }
            if (curIndex > savedLastAppliedIndex) {
                throw new IllegalStateException("No user log between index:" + index + " and last_applied_index:"
                        + savedLastAppliedIndex);
            }
            entry = this.logManager.getEntry(curIndex);
        } while (entry != null);

        throw new LogNotFoundException("User log is deleted at index: " + curIndex);
    }

    @Override
    public void addReplicatorStateListener(final Replicator.ReplicatorStateListener replicatorStateListener) {
        Requires.requireNonNull(replicatorStateListener, "replicatorStateListener");
        this.replicatorStateListeners.add(replicatorStateListener);
    }

    @Override
    public void removeReplicatorStateListener(final Replicator.ReplicatorStateListener replicatorStateListener) {
        Requires.requireNonNull(replicatorStateListener, "replicatorStateListener");
        this.replicatorStateListeners.remove(replicatorStateListener);
    }

    @Override
    public void clearReplicatorStateListeners() {
        this.replicatorStateListeners.clear();
    }

    @Override
    public List<Replicator.ReplicatorStateListener> getReplicatorStatueListeners() {
        return this.replicatorStateListeners;
    }

    @Override
    public int getNodeTargetPriority() {
        return this.targetPriority;
    }

    @Override
    public void describe(final Printer out) {
        // node
        final String _nodeId;
        final String _state;
        final String _leaderId;
        final long _currTerm;
        final String _conf;
        final int _targetPriority;
        this.readLock.lock();
        try {
            _nodeId = String.valueOf(getNodeId());
            _state = String.valueOf(this.state);
            _leaderId = String.valueOf(this.leaderId);
            _currTerm = this.currTerm;
            _conf = String.valueOf(this.conf);
            _targetPriority = this.targetPriority;
        } finally {
            this.readLock.unlock();
        }
        out.print("nodeId: ") //
                .println(_nodeId);
        out.print("state: ") //
                .println(_state);
        out.print("leaderId: ") //
                .println(_leaderId);
        out.print("term: ") //
                .println(_currTerm);
        out.print("conf: ") //
                .println(_conf);
        out.print("targetPriority: ") //
                .println(_targetPriority);

        // timers
        out.println("electionTimer: ");
        this.electionTimer.describe(out);

        out.println("voteTimer: ");
        this.voteTimer.describe(out);

        out.println("stepDownTimer: ");
        this.stepDownTimer.describe(out);

        out.println("snapshotTimer: ");
        this.snapshotTimer.describe(out);

        // logManager
        out.println("logManager: ");
        this.logManager.describe(out);

        // fsmCaller
        out.println("fsmCaller: ");
        this.fsmCaller.describe(out);

        // ballotBox
        out.println("ballotBox: ");
        this.ballotBox.describe(out);

        // snapshotExecutor
        out.println("snapshotExecutor: ");
        this.snapshotExecutor.describe(out);

        // replicators
        out.println("replicatorGroup: ");
        this.replicatorGroup.describe(out);

        // log storage
        if (this.logStorage instanceof Describer) {
            out.println("logStorage: ");
            ((Describer) this.logStorage).describe(out);
        }
    }

    @Override
    public String toString() {
        return "JRaftNode [nodeId=" + getNodeId() + "]";
    }
}
