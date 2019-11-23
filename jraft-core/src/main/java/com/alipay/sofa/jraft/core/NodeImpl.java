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
import com.alipay.sofa.jraft.rpc.RpcResponseFactory;
import com.alipay.sofa.jraft.rpc.impl.core.BoltRaftClientService;
import com.alipay.sofa.jraft.storage.LogManager;
import com.alipay.sofa.jraft.storage.LogStorage;
import com.alipay.sofa.jraft.storage.RaftMetaStorage;
import com.alipay.sofa.jraft.storage.SnapshotExecutor;
import com.alipay.sofa.jraft.storage.impl.LogManagerImpl;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotExecutorImpl;
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
import com.alipay.sofa.jraft.util.SignalHelper;
import com.alipay.sofa.jraft.util.ThreadHelper;
import com.alipay.sofa.jraft.util.ThreadId;
import com.alipay.sofa.jraft.util.Utils;
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
import java.util.List;
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

    private static final Logger LOG = LoggerFactory.getLogger(NodeImpl.class);

    static {
        try {
            if (SignalHelper.supportSignal()) {
                // TODO support windows signal
                if (!Platform.isWindows()) {
                    final List<JRaftSignalHandler> handlers = JRaftServiceLoader.load(JRaftSignalHandler.class).sort();
                    SignalHelper.addSignal(SignalHelper.SIG_USR2, handlers);
                }
            }
        } catch (final Throwable t) {
            LOG.error("Fail to add signal.", t);
        }
    }

    /** Max retry times when applying tasks */
    private static final int MAX_APPLY_RETRY_TIMES = 3;

    /** 节点计数器 */
    public static final AtomicInteger GLOBAL_NUM_NODES = new AtomicInteger(0);

    /** Internal states */
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    protected final Lock writeLock = this.readWriteLock.writeLock();
    protected final Lock readLock = this.readWriteLock.readLock();
    /** 记录当前节点的状态 */
    private volatile State state;
    private volatile CountDownLatch shutdownLatch;
    /** 当前节点的 term 值 */
    private long currTerm;
    /*** 时间戳 */
    private volatile long lastLeaderTimestamp;
    /** 记录 leader 节点 */
    private PeerId leaderId = new PeerId();
    /** 赢得预投票的节点 */
    private PeerId votedId;
    private final Ballot voteCtx = new Ballot();
    private final Ballot prevVoteCtx = new Ballot();
    /** 记录整个集群的节点信息 */
    private ConfigurationEntry conf;
    private StopTransferArg stopTransferArg;

    /** Raft group and node options and identifier */
    private final String groupId; // 组 ID
    private NodeOptions options;
    private RaftOptions raftOptions;

    /** 当前节点 */
    private final PeerId serverId;
    /** Other services */
    private final ConfigurationCtx confCtx;
    private LogStorage logStorage;
    private RaftMetaStorage metaStorage;
    private ClosureQueue closureQueue;
    private ConfigurationManager configManager;
    private LogManager logManager;
    /** 有限状态机调用程序 */
    private FSMCaller fsmCaller;
    private BallotBox ballotBox;
    private SnapshotExecutor snapshotExecutor;
    private ReplicatorGroup replicatorGroup;
    private final List<Closure> shutdownContinuations = new ArrayList<>();
    private RaftClientService rpcService;
    private ReadOnlyService readOnlyService;

    /* Timers */

    /** 定时任务管理器 */
    private TimerManager timerManager;
    /** 预投票定时器 */
    private RepeatedTimer electionTimer;
    /** 选举定时器 */
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

    /** 创建 Raft 服务的抽象工厂 */
    private JRaftServiceFactory serviceFactory;

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
     * 处理 LogEntry
     *
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-03 4:30:07 PM
     */
    private class LogEntryAndClosureHandler implements EventHandler<LogEntryAndClosure> {
        // task list for batch
        private final List<LogEntryAndClosure> tasks = new ArrayList<>(raftOptions.getApplyBatch());

        @Override
        public void onEvent(final LogEntryAndClosure event, final long sequence, final boolean endOfBatch) throws Exception {
            // 当前节点已经关闭
            if (event.shutdownLatch != null) {
                // 但是还有未处理完成的 task
                if (!tasks.isEmpty()) {
                    // 在真正关闭之前，尝试继续处理这些未完成的 task
                    executeApplyingTasks(tasks);
                }
                final int num = GLOBAL_NUM_NODES.decrementAndGet();
                LOG.info("The number of active nodes decrement to {}.", num);
                event.shutdownLatch.countDown();
                return;
            }

            tasks.add(event);
            // 批量处理，一次处理 32 个
            if (tasks.size() >= raftOptions.getApplyBatch() || endOfBatch) {
                executeApplyingTasks(this.tasks);
                tasks.clear();
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
        int nchanges;
        long version;
        List<PeerId> newPeers = new ArrayList<>();
        List<PeerId> oldPeers = new ArrayList<>();
        List<PeerId> addingPeers = new ArrayList<>();
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
            if (this.isBusy()) {
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
            final Configuration adding = new Configuration();
            final Configuration removing = new Configuration();
            newConf.diff(oldConf, adding, removing);
            this.nchanges = adding.size() + removing.size();
            if (adding.isEmpty()) {
                this.nextStage();
                return;
            }
            this.addingPeers = adding.listPeers();
            LOG.info("Adding peers: {}.", this.addingPeers);
            for (final PeerId newPeer : this.addingPeers) {
                if (!this.node.replicatorGroup.addReplicator(newPeer)) {
                    LOG.error("Node {} start the replicator failed, peer={}.", this.node.getNodeId(), newPeer);
                    this.onCaughtUp(this.version, newPeer, false);
                    return;
                }
                final OnCaughtUp caughtUp = new OnCaughtUp(this.node, this.node.currTerm, newPeer, this.version);
                final long dueTime = Utils.nowMs() + this.node.options.getElectionTimeoutMs();
                if (!this.node.replicatorGroup.waitCaughtUp(newPeer, this.node.options.getCatchupMargin(), dueTime,
                        caughtUp)) {
                    LOG.error("Node {} waitCaughtUp, peer={}.", this.node.getNodeId(), newPeer);
                    this.onCaughtUp(this.version, newPeer, false);
                    return;
                }
            }
        }

        void onCaughtUp(final long version, final PeerId peer, final boolean success) {
            if (version != this.version) {
                return;
            }
            Requires.requireTrue(this.stage == Stage.STAGE_CATCHING_UP, "Stage is not in STAGE_CATCHING_UP");
            if (success) {
                this.addingPeers.remove(peer);
                if (this.addingPeers.isEmpty()) {
                    this.nextStage();
                    return;
                }
                return;
            }
            LOG.warn("Node {} fail to catch up peer {} when trying to change peers from {} to {}.",
                    this.node.getNodeId(), peer, this.oldPeers, this.newPeers);
            this.reset(new Status(RaftError.ECATCHUP, "Peer %s failed to catch up.", peer));
        }

        void reset() {
            this.reset(null);
        }

        void reset(final Status st) {
            if (st != null && st.isOk()) {
                this.node.stopReplicator(this.newPeers, this.oldPeers);
            } else {
                this.node.stopReplicator(this.oldPeers, this.newPeers);
            }
            this.newPeers.clear();
            this.oldPeers.clear();
            this.addingPeers.clear();
            this.version++;
            this.stage = Stage.STAGE_NONE;
            this.nchanges = 0;
            if (this.done != null) {
                Utils.runClosureInThread(this.done, st != null ? st : new Status(RaftError.EPERM,
                        "Leader stepped down."));
                this.done = null;
            }
        }

        /**
         * Invoked when this node becomes the leader, write a configuration change log as the first log.
         */
        void flush(final Configuration conf, final Configuration oldConf) {
            Requires.requireTrue(!this.isBusy(), "Flush when busy");
            this.newPeers = conf.listPeers();
            if (oldConf == null || oldConf.isEmpty()) {
                this.stage = Stage.STAGE_STABLE;
                this.oldPeers = this.newPeers;
            } else {
                this.stage = Stage.STAGE_JOINT;
                this.oldPeers = oldConf.listPeers();
            }
            this.node.unsafeApplyConfiguration(conf, oldConf == null || oldConf.isEmpty() ? null : oldConf, true);
        }

        void nextStage() {
            Requires.requireTrue(this.isBusy(), "Not in busy stage");
            switch (this.stage) {
                case STAGE_CATCHING_UP:
                    if (this.nchanges > 1) {
                        this.stage = Stage.STAGE_JOINT;
                        this.node.unsafeApplyConfiguration(new Configuration(this.newPeers), new Configuration(
                                this.oldPeers), false);
                        return;
                    }
                    // Skip joint consensus since only one peers has been changed here. Make
                    // it a one-stage change to be compatible with the legacy
                    // implementation.
                case STAGE_JOINT:
                    this.stage = Stage.STAGE_STABLE;
                    this.node.unsafeApplyConfiguration(new Configuration(this.newPeers), null, false);
                    break;
                case STAGE_STABLE:
                    final boolean shouldStepDown = !this.newPeers.contains(this.node.serverId);
                    this.reset(new Status());
                    if (shouldStepDown) {
                        this.node.stepDown(this.node.currTerm, true, new Status(RaftError.ELEADERREMOVED,
                                "This node was removed."));
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
        // 校验组 ID 的合法性
        if (groupId != null) {
            Utils.verifyGroupId(groupId);
        }
        this.groupId = groupId;
        this.serverId = serverId != null ? serverId.copy() : null;
        this.state = State.STATE_UNINITIALIZED;
        // 初始 term 值为 0
        this.currTerm = 0;
        this.updateLastLeaderTimestamp(Utils.monotonicMs());
        this.confCtx = new ConfigurationCtx(this);
        this.wakingCandidate = null;
        final int num = GLOBAL_NUM_NODES.incrementAndGet();
        LOG.info("The number of active nodes increment to {}.", num);
    }

    private boolean initSnapshotStorage() {
        if (StringUtils.isEmpty(this.options.getSnapshotUri())) {
            LOG.warn("Do not set snapshot uri, ignore initSnapshotStorage.");
            return true;
        }
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
        return this.snapshotExecutor.init(opts);
    }

    private boolean initLogStorage() {
        Requires.requireNonNull(this.fsmCaller, "Null fsm caller");
        this.logStorage = this.serviceFactory.createLogStorage(this.options.getLogUri(), this.raftOptions);
        this.logManager = new LogManagerImpl();
        final LogManagerOptions opts = new LogManagerOptions();
        opts.setLogEntryCodecFactory(this.serviceFactory.createLogEntryCodecFactory());
        opts.setLogStorage(this.logStorage);
        opts.setConfigurationManager(this.configManager);
        opts.setFsmCaller(this.fsmCaller);
        opts.setNodeMetrics(this.metrics);
        opts.setDisruptorBufferSize(this.raftOptions.getDisruptorBufferSize());
        opts.setRaftOptions(this.raftOptions);
        return this.logManager.init(opts);
    }

    private boolean initMetaStorage() {
        this.metaStorage = this.serviceFactory.createRaftMetaStorage(this.options.getRaftMetaUri(), this.raftOptions);
        RaftMetaStorageOptions opts = new RaftMetaStorageOptions();
        opts.setNode(this);
        if (!this.metaStorage.init(opts)) {
            LOG.error("Node {} init meta storage failed, uri={}.", this.serverId, this.options.getRaftMetaUri());
            return false;
        }
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
        Utils.runInThread(() -> this.doSnapshot(null));
    }

    private void handleElectionTimeout() {
        boolean doUnlock = true;
        this.writeLock.lock();
        try {
            // 当前节点如果不是 follower，则没有必要执行 pre-vote
            if (this.state != State.STATE_FOLLOWER) {
                return;
            }
            // 当前租约有效
            if (this.isCurrentLeaderValid()) {
                return;
            }
            // 租约已失效
            this.resetLeaderId(
                    PeerId.emptyPeer(),
                    new Status(RaftError.ERAFTTIMEDOUT, "Lost connection from leader %s.", this.leaderId));
            doUnlock = false;
            // 发起预投票，如果没有得到半数以上的节点响应，则放弃参选
            this.preVote();
        } finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
        }
    }

    private boolean initFSMCaller(final LogId bootstrapId) {
        if (this.fsmCaller == null) {
            LOG.error("Fail to init fsm caller, null instance, bootstrapId={}.", bootstrapId);
            return false;
        }
        this.closureQueue = new ClosureQueueImpl();
        final FSMCallerOptions opts = new FSMCallerOptions();
        opts.setAfterShutdown(status -> this.afterShutdown());
        opts.setLogManager(this.logManager);
        opts.setFsm(this.options.getFsm());
        opts.setClosureQueue(this.closureQueue);
        opts.setNode(this);
        opts.setBootstrapId(bootstrapId);
        opts.setDisruptorBufferSize(this.raftOptions.getDisruptorBufferSize());
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
            LOG.error("Invalid arguments for bootstrap, groupConf={}, fsm={}, lastLogIndex={}.",
                    opts.getGroupConf(), opts.getFsm(), opts.getLastLogIndex());
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

        if (!this.initLogStorage()) {
            LOG.error("Fail to init log storage.");
            return false;
        }
        if (!this.initMetaStorage()) {
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

        if (opts.getFsm() != null && !this.initFSMCaller(bootstrapId)) {
            LOG.error("Fail to init fsm caller.");
            return false;
        }

        final LogEntry entry = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION);
        entry.getId().setTerm(this.currTerm);
        entry.setPeers(opts.getGroupConf().listPeers());

        final List<LogEntry> entries = new ArrayList<>();
        entries.add(entry);

        final BootstrapStableClosure bootstrapDone = new BootstrapStableClosure();
        this.logManager.appendEntries(entries, bootstrapDone);
        if (!bootstrapDone.await().isOk()) {
            LOG.error("Fail to append configuration.");
            return false;
        }

        if (opts.getLastLogIndex() > 0) {
            if (!this.initSnapshotStorage()) {
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

    /**
     * 随机生成一个 [timeoutMs, timeoutMs + 1000) 之间的超时时间
     *
     * @param timeoutMs
     * @return
     */
    private int randomTimeout(final int timeoutMs) {
        return ThreadLocalRandom.current().nextInt(timeoutMs, timeoutMs + this.raftOptions.getMaxElectionDelayMs());
    }

    @Override
    public boolean init(final NodeOptions opts) {
        Requires.requireNonNull(opts, "Null node options");
        Requires.requireNonNull(opts.getRaftOptions(), "Null raft options");
        Requires.requireNonNull(opts.getServiceFactory(), "Null jraft service factory");
        // 默认为 DefaultJRaftServiceFactory 实现
        this.serviceFactory = opts.getServiceFactory();
        this.options = opts;
        this.raftOptions = opts.getRaftOptions();
        this.metrics = new NodeMetrics(opts.isEnableMetrics());

        // 校验节点 IP，不允许为 0.0.0.0
        if (this.serverId.getIp().equals(Utils.IP_ANY)) {
            LOG.error("Node can't started from IP_ANY.");
            return false;
        }

        // 节点已经存在
        if (!NodeManager.getInstance().serverExists(this.serverId.getEndpoint())) {
            LOG.error("No RPC server attached to, did you forget to call addService?");
            return false;
        }

        // 定时任务管理器
        this.timerManager = new TimerManager();
        if (!this.timerManager.init(this.options.getTimerPoolSize())) {
            LOG.error("Fail to init timer manager.");
            return false;
        }

        // 选举定时任务，如果选举超时，且当前的节点又是 CANDIDATE 角色，则发起选举
        this.voteTimer = new RepeatedTimer("JRaft-VoteTimer", this.options.getElectionTimeoutMs()) {

            @Override
            protected void onTrigger() {
                handleVoteTimeout();
            }

            @Override
            protected int adjustTimeout(final int timeoutMs) {
                // 随机产生一个延时时间戳
                return randomTimeout(timeoutMs);
            }
        };

        // 预投票定时任务，候选者在发起投票之前先发起预投票，如果得票数未过半则放弃参选
        this.electionTimer = new RepeatedTimer("JRaft-ElectionTimer", this.options.getElectionTimeoutMs()) {

            @Override
            protected void onTrigger() {
                handleElectionTimeout();
            }

            @Override
            protected int adjustTimeout(final int timeoutMs) {
                return randomTimeout(timeoutMs);
            }
        };

        /*
         * 定时检查是否需要降级（stepdown），当前 leader 可能出现没有半数以上 follower 响应的情况，一般出现在网络分区的情况下
         * 所以需要定期检查 leader 的 follower 是否有那么多，没有的话就需要强制让 leader 下台
         */
        this.stepDownTimer = new RepeatedTimer("JRaft-StepDownTimer", this.options.getElectionTimeoutMs() >> 1) {

            @Override
            protected void onTrigger() {
                handleStepDownTimeout();
            }
        };

        // 快照定时生成器
        this.snapshotTimer = new RepeatedTimer("JRaft-SnapshotTimer", this.options.getSnapshotIntervalSecs() * 1000) {

            @Override
            protected void onTrigger() {
                handleSnapshotTimeout();
            }
        };

        this.configManager = new ConfigurationManager();

        this.applyDisruptor = DisruptorBuilder.<LogEntryAndClosure>newInstance()
                .setRingBufferSize(raftOptions.getDisruptorBufferSize())
                .setEventFactory(new LogEntryAndClosureFactory())
                .setThreadFactory(new NamedThreadFactory("JRaft-NodeImpl-Disruptor-", true))
                .setProducerType(ProducerType.MULTI)
                .setWaitStrategy(new BlockingWaitStrategy())
                .build();
        this.applyDisruptor.handleEventsWith(new LogEntryAndClosureHandler());
        this.applyDisruptor.setDefaultExceptionHandler(new LogExceptionHandler<Object>(this.getClass().getSimpleName()));
        this.applyQueue = this.applyDisruptor.start();
        if (this.metrics.getMetricRegistry() != null) {
            this.metrics.getMetricRegistry().register("jraft-node-impl-disruptor", new DisruptorMetricSet(this.applyQueue));
        }

        // FSMCaller 用于对业务 StateMachine 的状态转换调用和日志的写入等
        this.fsmCaller = new FSMCallerImpl();

        // TODO source reading, by zhenchao 2019-11-23 16:17:24

        // 初始化日志存储
        if (!this.initLogStorage()) {
            LOG.error("Node {} initLogStorage failed.", this.getNodeId());
            return false;
        }
        // 初始化元数据存储
        if (!this.initMetaStorage()) {
            LOG.error("Node {} initMetaStorage failed.", this.getNodeId());
            return false;
        }
        // 初始化 FSMCaller
        if (!this.initFSMCaller(new LogId(0, 0))) {
            LOG.error("Node {} initFSMCaller failed.", this.getNodeId());
            return false;
        }
        // 初始化投票箱
        this.ballotBox = new BallotBox();
        final BallotBoxOptions ballotBoxOpts = new BallotBoxOptions();
        ballotBoxOpts.setWaiter(this.fsmCaller);
        ballotBoxOpts.setClosureQueue(this.closureQueue);
        if (!this.ballotBox.init(ballotBoxOpts)) {
            LOG.error("Node {} init ballotBox failed.", this.getNodeId());
            return false;
        }

        // 初始化快照存储
        if (!this.initSnapshotStorage()) {
            LOG.error("Node {} initSnapshotStorage failed.", this.getNodeId());
            return false;
        }

        // 检查日志文件索引的一致性
        final Status st = this.logManager.checkConsistency();
        if (!st.isOk()) {
            LOG.error("Node {} is initialized with inconsistent log, status={}.", this.getNodeId(), st);
            return false;
        }
        // 初始化配置信息
        this.conf = new ConfigurationEntry();
        this.conf.setId(new LogId());
        // if have log using conf in log, else using conf in options
        if (this.logManager.getLastLogIndex() > 0) {
            this.conf = this.logManager.checkAndSetConfiguration(this.conf);
        } else {
            this.conf.setConf(this.options.getInitialConf());
        }

        // TODO RPC service and ReplicatorGroup is in cycle dependent, refactor it
        this.replicatorGroup = new ReplicatorGroupImpl();
        this.rpcService = new BoltRaftClientService(this.replicatorGroup);
        final ReplicatorGroupOptions rgOpts = new ReplicatorGroupOptions();
        rgOpts.setHeartbeatTimeoutMs(this.heartbeatTimeout(this.options.getElectionTimeoutMs()));
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

        // 初始化 RPC 服务
        if (!this.rpcService.init(this.options)) {
            LOG.error("Fail to init rpc service.");
            return false;
        }
        this.replicatorGroup.init(new NodeId(this.groupId, this.serverId), rgOpts);

        this.readOnlyService = new ReadOnlyServiceImpl();
        final ReadOnlyServiceOptions rosOpts = new ReadOnlyServiceOptions();
        rosOpts.setFsmCaller(this.fsmCaller);
        rosOpts.setNode(this);
        rosOpts.setRaftOptions(this.raftOptions);

        // 初始化只读服务
        if (!this.readOnlyService.init(rosOpts)) {
            LOG.error("Fail to init readOnlyService.");
            return false;
        }

        // 初始化节点的初始状态
        this.state = State.STATE_FOLLOWER;

        if (LOG.isInfoEnabled()) {
            LOG.info("Node {} init, term={}, lastLogId={}, conf={}, oldConf={}.", this.getNodeId(), this.currTerm,
                    this.logManager.getLastLogId(false), this.conf.getConf(), this.conf.getOldConf());
        }

        if (this.snapshotExecutor != null && this.options.getSnapshotIntervalSecs() > 0) {
            LOG.debug("Node {} start snapshot timer, term={}.", this.getNodeId(), this.currTerm);
            this.snapshotTimer.start();
        }

        if (!this.conf.isEmpty()) {
            this.stepDown(this.currTerm, false, new Status());
        }

        // 将当前节点添加到管理器中
        if (!NodeManager.getInstance().add(this)) {
            LOG.error("NodeManager add {} failed.", this.getNodeId());
            return false;
        }

        // Now the raft node is started, have to acquire the writeLock to avoid race conditions
        this.writeLock.lock();
        if (this.conf.isStable() && this.conf.getConf().size() == 1 && this.conf.getConf().contains(this.serverId)) {
            // The group contains only this server which must be the LEADER, trigger the timer immediately.
            // 当前集群只有一个节点，直接竞选成为 leader
            this.electSelf();
        } else {
            this.writeLock.unlock();
        }

        return true;
    }

    /**
     * 投票选择自己为 leader
     *
     * should be in writeLock
     */
    private void electSelf() {
        long oldTerm;
        try {
            LOG.info("Node {} start vote and grant vote self, term={}.", this.getNodeId(), this.currTerm);
            // 当前节点是未知节点，则放弃参选
            if (!this.conf.contains(this.serverId)) {
                LOG.warn("Node {} can't do electSelf as it is not in {}.", this.getNodeId(), this.conf);
                return;
            }
            // 当前节点为 follower，马上要开始选举了，先把预选举关掉
            if (this.state == State.STATE_FOLLOWER) {
                LOG.debug("Node {} stop election timer, term={}.", this.getNodeId(), this.currTerm);
                this.electionTimer.stop();
            }
            // 清空本地记录的 leaderId
            this.resetLeaderId(PeerId.emptyPeer(), new Status(RaftError.ERAFTTIMEDOUT,
                    "A follower's leader_id is reset to NULL as it begins to request_vote."));
            // 设置节点状态为 CANDIDATE，参与竞选
            this.state = State.STATE_CANDIDATE;
            this.currTerm++; // term 值加 1
            this.votedId = this.serverId.copy();
            LOG.debug("Node {} start vote timer, term={} .", this.getNodeId(), this.currTerm);
            // 启动选举定时器
            this.voteTimer.start();
            // 初始化投票箱
            this.voteCtx.init(this.conf.getConf(), this.conf.isStable() ? null : this.conf.getOldConf());
            oldTerm = this.currTerm;
        } finally {
            this.writeLock.unlock();
        }

        // 获取本地记录的最新的日志 ID
        final LogId lastLogId = this.logManager.getLastLogId(true);

        this.writeLock.lock();
        try {
            // vote need defense ABA after unlock&writeLock
            if (oldTerm != this.currTerm) {
                LOG.warn("Node {} raise term {} when getLastLogId.", this.getNodeId(), this.currTerm);
                return;
            }

            // 遍历处理所有的节点
            for (final PeerId peer : this.conf.listPeers()) {
                // 忽略节点自己
                if (peer.equals(this.serverId)) {
                    continue;
                }
                // 目标节点连接不通，先跳过
                if (!this.rpcService.connect(peer.getEndpoint())) {
                    LOG.warn("Node {} channel init failed, address={}.", this.getNodeId(), peer.getEndpoint());
                    continue;
                }
                // 新建一个投票请求响应回调
                final OnRequestVoteRpcDone done = new OnRequestVoteRpcDone(peer, this.currTerm, this);
                done.request = RequestVoteRequest.newBuilder()
                        .setPreVote(false) // It's not a pre-vote request.
                        .setGroupId(this.groupId)
                        .setServerId(this.serverId.toString())
                        .setPeerId(peer.toString())
                        .setTerm(this.currTerm)
                        .setLastLogIndex(lastLogId.getIndex())
                        .setLastLogTerm(lastLogId.getTerm())
                        .build();
                // 询问目标节点是否同意自己当选
                this.rpcService.requestVote(peer.getEndpoint(), done.request, done);
            }

            this.metaStorage.setTermAndVotedFor(this.currTerm, this.serverId);
            // 投自己一票
            this.voteCtx.grant(this.serverId);
            if (this.voteCtx.isGranted()) {
                // 当前节点成为 leader
                this.becomeLeader();
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * 重置本地记录的 leader 节点
     *
     * @param newLeaderId
     * @param status
     */
    private void resetLeaderId(final PeerId newLeaderId, final Status status) {
        if (newLeaderId.isEmpty()) {
            // 当前已经有 leader 节点，且当前节点不是 LEADER 节点
            if (!this.leaderId.isEmpty() && this.state.compareTo(State.STATE_TRANSFERRING) > 0) {
                // 停止跟随当前 leader 节点
                this.fsmCaller.onStopFollowing(new LeaderChangeContext(this.leaderId.copy(), this.currTerm, status));
            }
            // 情况本地记录的 leader 节点
            this.leaderId = PeerId.emptyPeer();
        } else {
            // 当前没有 leader 节点，则开始追随新的 leader 节点
            if (this.leaderId == null || this.leaderId.isEmpty()) {
                this.fsmCaller.onStartFollowing(new LeaderChangeContext(newLeaderId, this.currTerm, status));
            }
            // 更新 leader 节点 ID
            this.leaderId = newLeaderId.copy();
        }
    }

    // in writeLock
    private void checkStepDown(final long requestTerm, final PeerId serverId) {
        final Status status = new Status();
        if (requestTerm > this.currTerm) {
            status.setError(RaftError.ENEWLEADER, "Raft node receives message from new leader with higher term.");
            this.stepDown(requestTerm, false, status);
        } else if (this.state != State.STATE_FOLLOWER) {
            status.setError(RaftError.ENEWLEADER, "Candidate receives message from new leader with the same term.");
            this.stepDown(requestTerm, false, status);
        } else if (this.leaderId.isEmpty()) {
            status.setError(RaftError.ENEWLEADER, "Follower receives message from new leader with the same term.");
            this.stepDown(requestTerm, false, status);
        }
        // save current leader
        if (this.leaderId == null || this.leaderId.isEmpty()) {
            this.resetLeaderId(serverId, status);
        }
    }

    /**
     * 节点成为 leader
     */
    private void becomeLeader() {
        // 成为 leader 前置节点状态必须是 CANDIDATE
        Requires.requireTrue(this.state == State.STATE_CANDIDATE, "Illegal state: " + this.state);
        LOG.info("Node {} become leader of group, term={}, conf={}, oldConf={}.",
                this.getNodeId(), this.currTerm, this.conf.getConf(), this.conf.getOldConf());
        // cancel candidate vote timer
        // 先停止选举定时器
        this.stopVoteTimer();
        this.state = State.STATE_LEADER;
        this.leaderId = this.serverId.copy();
        // 重置集群新的 term 值
        this.replicatorGroup.resetTerm(this.currTerm);
        // 遍历所有的节点
        for (final PeerId peer : this.conf.listPeers()) {
            // 跳过当前节点
            if (peer.equals(this.serverId)) {
                continue;
            }
            LOG.debug("Node {} add replicator, term={}, peer={}.", this.getNodeId(), this.currTerm, peer);
            // 将其它节点设置为自己的 follower
            if (!this.replicatorGroup.addReplicator(peer)) {
                LOG.error("Fail to add replicator, peer={}.", peer);
            }
        }
        // init commit manager
        this.ballotBox.resetPendingIndex(this.logManager.getLastLogIndex() + 1);
        // Register _conf_ctx to reject configuration changing before the first log is committed.
        if (this.confCtx.isBusy()) {
            throw new IllegalStateException();
        }
        this.confCtx.flush(this.conf.getConf(), this.conf.getOldConf());
        // 对于 leader 节点而言，需要定期检查是否需要下台
        this.stepDownTimer.start();
    }

    /**
     * LEADER 下台
     *
     * 1. 如果当前的节点是 CANDIDATE，则暂时不要投票
     * 2. 如果当前的节点是 LEADER 或正在交权，需要关闭当前节点的 stepDownTimer
     * 3. 如果当前是 LEADER，需要告诉状态机 leader 下台了，可以在状态机中对下台的动作做处理
     * 4. 重置当前节点记录的 leaderId，把当前节点的 state 状态设置为 Follower，重置 confCtx 上下文
     * 5. 停止当前的快照生成，设置新的任期，让所有的复制节点停止工作
     * 6. 启动 electionTimer
     *
     * @param term
     * @param wakeupCandidate
     * @param status
     */
    private void stepDown(final long term, final boolean wakeupCandidate, final Status status) {
        LOG.debug("Node {} stepDown, term={}, newTerm={}, wakeupCandidate={}.", this.getNodeId(), this.currTerm, term, wakeupCandidate);
        // 当前节点处于不活跃状态，直接返回
        if (!this.state.isActive()) {
            return;
        }

        // 当前节点是 CANDIDATE，则停止选举
        if (this.state == State.STATE_CANDIDATE) {
            this.stopVoteTimer();
        }
        // 当前节点是 LEADER 或正在交权
        else if (this.state.compareTo(State.STATE_TRANSFERRING) <= 0) {
            // 停止 stepDownTimer
            this.stopStepDownTimer();
            // 清空选票
            this.ballotBox.clearPendingTasks();
            // signal fsm leader stop immediately
            // 如果当前节点是 LEADER，发送 LEADER 下台的通知
            if (this.state == State.STATE_LEADER) {
                this.onLeaderStop(status);
            }
        }

        // 重置本地记录的 leader 节点
        this.resetLeaderId(PeerId.emptyPeer(), status); // reset leader_id

        // 重置本地节点角色
        this.state = State.STATE_FOLLOWER; // soft state in memory
        // 重置 Configuration 上下文
        this.confCtx.reset();
        this.updateLastLeaderTimestamp(Utils.monotonicMs());
        if (this.snapshotExecutor != null) {
            // 停止当前快照生成
            this.snapshotExecutor.interruptDownloadingSnapshots(term);
        }

        // meta state
        if (term > this.currTerm) {
            this.currTerm = term; // 更新 term 值
            this.votedId = PeerId.emptyPeer();
            this.metaStorage.setTermAndVotedFor(term, this.votedId);
        }

        if (wakeupCandidate) {
            this.wakingCandidate = this.replicatorGroup.stopAllAndFindTheNextCandidate(this.conf);
            if (this.wakingCandidate != null) {
                Replicator.sendTimeoutNowAndStop(this.wakingCandidate, this.options.getElectionTimeoutMs());
            }
        } else {
            // 停止 group 中所有的 Replicator
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

        // 启动
        this.electionTimer.start();
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
                NodeImpl.this.ballotBox.commitAt(this.firstLogIndex, this.firstLogIndex + this.nEntries - 1,
                        NodeImpl.this.serverId);
            } else {
                LOG.error("Node {} append [{}, {}] failed.", NodeImpl.this.getNodeId(), this.firstLogIndex, this.firstLogIndex
                        + this.nEntries - 1);
            }
        }
    }

    /**
     * 批量处理 task
     *
     * @param tasks
     */
    private void executeApplyingTasks(final List<LogEntryAndClosure> tasks) {
        this.writeLock.lock();
        try {
            final int size = tasks.size();
            // 当前节点已经不是 Leader
            if (this.state != State.STATE_LEADER) {
                final Status st = new Status();
                if (this.state != State.STATE_TRANSFERRING) {
                    st.setError(RaftError.EPERM, "Is not leader.");
                } else {
                    // 当前 leader 节点正在移交权利
                    st.setError(RaftError.EBUSY, "Is transferring leadership.");
                }
                LOG.debug("Node {} can't apply, status={}.", this.getNodeId(), st);
                // 为每个任务应用 done 回调
                for (int i = 0; i < size; i++) {
                    Utils.runClosureInThread(tasks.get(i).done, st);
                }
                return;
            }

            /* 当前节点是 leader 节点 */

            // 将 task 转换成 LogEntry 集合
            final List<LogEntry> entries = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                final LogEntryAndClosure task = tasks.get(i);
                // 如果当前 term 不是期望的 term，则响应 EPERM 异常
                if (task.expectedTerm != -1 && task.expectedTerm != this.currTerm) {
                    LOG.debug("Node {} can't apply task whose expectedTerm={} doesn't match currTerm={}.",
                            this.getNodeId(), task.expectedTerm, this.currTerm);
                    if (task.done != null) {
                        final Status st = new Status(RaftError.EPERM, "expected_term=%d doesn't match current_term=%d", task.expectedTerm, this.currTerm);
                        Utils.runClosureInThread(task.done, st);
                    }
                    continue;
                }
                // 基于配置创建选票，并投入选票箱
                if (!ballotBox.appendPendingTask(conf.getConf(),
                        conf.isStable() ? null : conf.getOldConf(), task.done)) {
                    Utils.runClosureInThread(task.done, new Status(RaftError.EINTERNAL, "Fail to append task."));
                    continue;
                }
                // set task entry info before adding to list.
                task.entry.getId().setTerm(this.currTerm);
                task.entry.setType(EnumOutter.EntryType.ENTRY_TYPE_DATA);
                entries.add(task.entry);
            }
            // 将 logEntry 写入本地，异步回调
            logManager.appendEntries(entries, new LeaderStableClosure(entries));
            // update conf.first
            this.conf = logManager.checkAndSetConfiguration(this.conf);
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
        if (this.shutdownLatch != null) {
            Utils.runClosureInThread(done, new Status(RaftError.ENODESHUTDOWN, "Node is shutting down."));
            throw new IllegalStateException("Node is shutting down");
        }
        Requires.requireNonNull(done, "Null closure");
        this.readOnlyService.addRequest(requestContext, done);
    }

    /**
     * ReadIndex response closure
     *
     * ReadIndex 心跳请求响应回调
     *
     * @author dennis
     */
    private class ReadIndexHeartbeatResponseClosure extends RpcResponseClosureAdapter<AppendEntriesResponse> {

        final ReadIndexResponse.Builder respBuilder;
        final RpcResponseClosure<ReadIndexResponse> closure;
        final int quorum;
        final int failPeersThreshold;
        /** 成功响应计数 */
        int ackSuccess;
        /** 失败响应计数 */
        int ackFailures;
        /** 标识响应已经处理，避免重复处理 */
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
            if (status.isOk() && this.getResponse().getSuccess()) {
                this.ackSuccess++;
            } else {
                this.ackFailures++;
            }
            // Include leader self vote yes.
            // 超过半数节点成功响应
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
            // 依据当前节点状态，分而治之
            switch (this.state) {
                // 当前节点是 leader 节点
                case STATE_LEADER:
                    // 接收 ReadIndex 请求，提供线性一致读
                    this.readLeader(request, ReadIndexResponse.newBuilder(), done);
                    break;
                // 当前节点是 follower 节点
                case STATE_FOLLOWER:
                    this.readFollower(request, done);
                    break;
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
        // 检查 leader 是否存在
        if (this.leaderId == null || this.leaderId.isEmpty()) {
            closure.run(new Status(RaftError.EPERM, "No leader at term %d.", this.currTerm));
            return;
        }
        // send request to leader.
        // 向 leader 节点请求数据
        final ReadIndexRequest newRequest = ReadIndexRequest.newBuilder()
                .mergeFrom(request)
                .setPeerId(this.leaderId.toString())
                .build();
        this.rpcService.readIndex(this.leaderId.getEndpoint(), newRequest, -1, closure);
    }

    private void readLeader(final ReadIndexRequest request,
                            final ReadIndexResponse.Builder respBuilder,
                            final RpcResponseClosure<ReadIndexResponse> closure) {
        // 获取当前集群节点数目
        final int quorum = this.getQuorum();
        if (quorum <= 1) {
            // 当前集群只有一个 peer 节点，直接获取 BallotBox 最新提交索引
            respBuilder.setSuccess(true).setIndex(this.ballotBox.getLastCommittedIndex());
            closure.setResponse(respBuilder.build());
            closure.run(Status.OK());
            return;
        }

        final long lastCommittedIndex = this.ballotBox.getLastCommittedIndex();
        // 校验 term 值
        if (this.logManager.getTerm(lastCommittedIndex) != this.currTerm) {
            // term 值不一致，说明当前 leader 节点在任期内每天提交任何日志，拒绝只读请求
            // Reject read only request when this leader has not committed any log entry at its term
            closure.run(new Status(
                    RaftError.EAGAIN,
                    "ReadIndex request rejected because leader has not committed any log entry at its term, logIndex=%d, currTerm=%d.",
                    lastCommittedIndex, this.currTerm));
            return;
        }
        respBuilder.setIndex(lastCommittedIndex);

        if (request.getPeerId() != null) {
            // request from follower, check if the follower is in current conf.
            final PeerId peer = new PeerId();
            peer.parse(request.getServerId());
            if (!this.conf.contains(peer)) {
                // 当前发送请求的 follower 节点未知
                closure.run(new Status(RaftError.EPERM, "Peer %s is not in current configuration: {}.", peer, this.conf));
                return;
            }
        }

        ReadOnlyOption readOnlyOpt = this.raftOptions.getReadOnlyOptions();
        // 配置期望使用租约，但是租约无效，转而使用 ReadOnlySafe 策略
        if (readOnlyOpt == ReadOnlyOption.ReadOnlyLeaseBased && !this.isLeaderLeaseValid()) {
            // If leader lease timeout, we must change option to ReadOnlySafe
            readOnlyOpt = ReadOnlyOption.ReadOnlySafe;
        }

        switch (readOnlyOpt) {
            // ReadIndex 线性一致策略
            case ReadOnlySafe:
                final List<PeerId> peers = this.conf.getConf().getPeers();
                Requires.requireTrue(peers != null && !peers.isEmpty(), "Empty peers");
                // 设置心跳的响应回调函数
                final ReadIndexHeartbeatResponseClosure heartbeatDone =
                        new ReadIndexHeartbeatResponseClosure(closure, respBuilder, quorum, peers.size());
                // Send heartbeat requests to followers
                // 向 Followers 节点发起一轮心跳请求，如果半数以上节点回应，则 leader 节点能够确定自己仍然是 leader
                for (final PeerId peer : peers) {
                    if (peer.equals(this.serverId)) {
                        continue;
                    }
                    // 向各个 follower 发送心跳请求
                    this.replicatorGroup.sendHeartbeat(peer, heartbeatDone);
                }
                break;
            // Lease 线程一致策略
            // 租约期内不会发生选举，所以 leader 不会发生变化
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
        // 当前节点已经关闭，拒绝处理
        if (this.shutdownLatch != null) {
            Utils.runClosureInThread(task.getDone(), new Status(RaftError.ENODESHUTDOWN, "Node is shutting down."));
            throw new IllegalStateException("Node is shutting down");
        }
        Requires.requireNonNull(task, "Null task");

        // 创建 LogEntry 对象，并设置数据
        final LogEntry entry = new LogEntry();
        entry.setData(task.getData());

        // 将 task 封装成 EventTranslator 对象记录到 Disruptor 中
        int retryTimes = 0;
        try {
            final EventTranslator<LogEntryAndClosure> translator = (event, sequence) -> {
                event.reset();
                event.done = task.getDone();
                event.entry = entry;
                event.expectedTerm = task.getExpectedTerm();
            };
            // 尝试将 translator 添加到 RingBuffer 中，如果当前节点繁忙（有太多 task 需要处理）则最多重试 3 次
            while (true) {
                if (applyQueue.tryPublishEvent(translator)) {
                    break;
                } else {
                    retryTimes++;
                    if (retryTimes > MAX_APPLY_RETRY_TIMES) {
                        Utils.runClosureInThread(task.getDone(), new Status(RaftError.EBUSY, "Node is busy, has too many tasks."));
                        LOG.warn("Node {} applyQueue is overload.", this.getNodeId());
                        this.metrics.recordTimes("apply-task-overload-times", 1);
                        return;
                    }
                    ThreadHelper.onSpinWait();
                }
            }
        } catch (final Exception e) {
            Utils.runClosureInThread(task.getDone(), new Status(RaftError.EPERM, "Node is down."));
        }
    }

    @Override
    public Message handlePreVoteRequest(final RequestVoteRequest request) {
        boolean doUnlock = true;
        this.writeLock.lock();
        try {
            // 当前节点已经失效，则不处理请求
            if (!this.state.isActive()) {
                LOG.warn("Node {} is not in active state, currTerm={}.", this.getNodeId(), this.currTerm);
                return RpcResponseFactory.newResponse(
                        RaftError.EINVAL, "Node %s is not in active state, state %s.", this.getNodeId(), this.state.name());
            }
            // 封装请求来源节点 ID
            final PeerId candidateId = new PeerId();
            if (!candidateId.parse(request.getServerId())) {
                // 请求来源方的 serverId 无法解析
                LOG.warn("Node {} received PreVoteRequest from {} serverId bad format.", this.getNodeId(), request.getServerId());
                return RpcResponseFactory.newResponse(RaftError.EINVAL, "Parse candidateId failed: %s.", request.getServerId());
            }
            boolean granted = false;
            // noinspection ConstantConditions
            do {
                // leader 已经存在且有效，忽略本次 pre-vote 请求
                if (this.leaderId != null && !this.leaderId.isEmpty() && this.isCurrentLeaderValid()) {
                    LOG.info(
                            "Node {} ignore PreVoteRequest from {}, term={}, currTerm={}, because the leader {}'s lease is still valid.",
                            this.getNodeId(), request.getServerId(), request.getTerm(), this.currTerm, this.leaderId);
                    break;
                }

                // 请求的 term 值小于当前节点已知的 term 值，忽略本次 pre-vote 请求
                if (request.getTerm() < this.currTerm) {
                    LOG.info("Node {} ignore PreVoteRequest from {}, term={}, currTerm={}.",
                            this.getNodeId(), request.getServerId(), request.getTerm(), this.currTerm);
                    // A follower replicator may not be started when this node become leader, so we must check it.
                    // 当前节点可能是 leader 节点，校验发起请求的节点是不是 follower，如果不是需要启动其复制机制
                    this.checkReplicator(candidateId);
                    break;
                }
                // 请求节点的 term 值与当前节点的 term 值相等
                else if (request.getTerm() == this.currTerm + 1) {
                    // A follower replicator may not be started when this node become leader, so we must check it.
                    // check replicator state
                    // 当前节点可能是 leader 节点，校验发起请求的节点是不是 follower，如果不是需要启动其复制机制
                    this.checkReplicator(candidateId);
                }
                doUnlock = false;
                this.writeLock.unlock();

                // 获取本地最新的日志 ID
                final LogId lastLogId = this.logManager.getLastLogId(true);

                doUnlock = true;
                this.writeLock.lock();
                // 请求方最新的日志 ID
                final LogId requestLastLogId = new LogId(request.getLastLogIndex(), request.getLastLogTerm());
                // 当前请求节点的 term 值大于本地节点的 term 值，或者 term 值相等，但是请求节点的最新 log index 更大（或等于），则同意投票
                granted = requestLastLogId.compareTo(lastLogId) >= 0;
                LOG.info("Node {} received PreVoteRequest from {}, term={}, currTerm={}, granted={}, requestLastLogId={}, lastLogId={}.",
                        this.getNodeId(), request.getServerId(), request.getTerm(), this.currTerm, granted, requestLastLogId, lastLogId);
            } while (false);

            return RequestVoteResponse.newBuilder()
                    .setTerm(this.currTerm)
                    .setGranted(granted)
                    .build();
        } finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
        }
    }

    /**
     * 检查租约是否有效
     *
     * @return
     */
    private boolean isLeaderLeaseValid() {
        final long monotonicNowMs = Utils.monotonicMs();
        if (this.checkLeaderLease(monotonicNowMs)) {
            return true;
        }
        this.checkDeadNodes0(this.conf.getConf().getPeers(), monotonicNowMs, false, null);
        return this.checkLeaderLease(monotonicNowMs);
    }

    private boolean checkLeaderLease(final long monotonicNowMs) {
        return monotonicNowMs - this.lastLeaderTimestamp < this.options.getLeaderLeaseTimeoutMs();
    }

    private boolean isCurrentLeaderValid() {
        return Utils.monotonicMs() - this.lastLeaderTimestamp < this.options.getElectionTimeoutMs();
    }

    private void updateLastLeaderTimestamp(final long lastLeaderTimestamp) {
        this.lastLeaderTimestamp = lastLeaderTimestamp;
    }

    /**
     * 如果当前节点是 leader 节点，则检查节点 candidate 是不是自己的 follower 节点
     *
     * @param candidateId
     */
    private void checkReplicator(final PeerId candidateId) {
        if (this.state == State.STATE_LEADER) {
            this.replicatorGroup.checkReplicator(candidateId, false);
        }
    }

    @Override
    public Message handleRequestVoteRequest(final RequestVoteRequest request) {
        boolean doUnlock = true;
        this.writeLock.lock();
        try {
            // 当前节点已经失效
            if (!this.state.isActive()) {
                LOG.warn("Node {} is not in active state, currTerm={}.", this.getNodeId(), this.currTerm);
                return RpcResponseFactory.newResponse(RaftError.EINVAL, "Node %s is not in active state, state %s.", this.getNodeId(), this.state.name());
            }
            final PeerId candidateId = new PeerId();
            if (!candidateId.parse(request.getServerId())) {
                LOG.warn("Node {} received RequestVoteRequest from {} serverId bad format.", this.getNodeId(), request.getServerId());
                return RpcResponseFactory.newResponse(RaftError.EINVAL, "Parse candidateId failed: %s.", request.getServerId());
            }

            // noinspection ConstantConditions
            do {
                // check term
                if (request.getTerm() >= this.currTerm) {
                    LOG.info("Node {} received RequestVoteRequest from {}, term={}, currTerm={}.", this.getNodeId(),
                            request.getServerId(), request.getTerm(), this.currTerm);
                    // increase current term, change state to follower
                    if (request.getTerm() > this.currTerm) {
                        this.stepDown(request.getTerm(), false, new Status(RaftError.EHIGHERTERMRESPONSE,
                                "Raft node receives higher term RequestVoteRequest."));
                    }
                } else {
                    // ignore older term
                    LOG.info("Node {} ignore RequestVoteRequest from {}, term={}, currTerm={}.", this.getNodeId(),
                            request.getServerId(), request.getTerm(), this.currTerm);
                    break;
                }
                doUnlock = false;
                this.writeLock.unlock();

                final LogId lastLogId = this.logManager.getLastLogId(true);

                doUnlock = true;
                this.writeLock.lock();
                // vote need ABA check after unlock&writeLock
                if (request.getTerm() != this.currTerm) {
                    LOG.warn("Node {} raise term {} when get lastLogId.", this.getNodeId(), this.currTerm);
                    break;
                }

                final boolean logIsOk = new LogId(request.getLastLogIndex(), request.getLastLogTerm()).compareTo(lastLogId) >= 0;

                if (logIsOk && (this.votedId == null || this.votedId.isEmpty())) {
                    this.stepDown(request.getTerm(), false, new Status(RaftError.EVOTEFORCANDIDATE,
                            "Raft node votes for some candidate, step down to restart election_timer."));
                    this.votedId = candidateId.copy();
                    this.metaStorage.setVotedFor(candidateId);
                }
            } while (false);

            return RequestVoteResponse.newBuilder()
                    .setTerm(this.currTerm)
                    .setGranted(request.getTerm() == this.currTerm && candidateId.equals(this.votedId))
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
            if (!this.state.isActive()) {
                LOG.warn("Node {} is not in active state, currTerm={}.", this.getNodeId(), this.currTerm);
                return RpcResponseFactory.newResponse(RaftError.EINVAL, "Node %s is not in active state, state %s.",
                        this.getNodeId(), this.state.name());
            }

            final PeerId serverId = new PeerId();
            if (!serverId.parse(request.getServerId())) {
                LOG.warn("Node {} received AppendEntriesRequest from {} serverId bad format.", this.getNodeId(),
                        request.getServerId());
                return RpcResponseFactory.newResponse(RaftError.EINVAL, "Parse serverId failed: %s.",
                        request.getServerId());
            }

            // Check stale term
            if (request.getTerm() < this.currTerm) {
                LOG.warn("Node {} ignore stale AppendEntriesRequest from {}, term={}, currTerm={}.", this.getNodeId(),
                        request.getServerId(), request.getTerm(), this.currTerm);
                return AppendEntriesResponse.newBuilder() //
                        .setSuccess(false) //
                        .setTerm(this.currTerm) //
                        .build();
            }

            // Check term and state to step down
            this.checkStepDown(request.getTerm(), serverId);
            if (!serverId.equals(this.leaderId)) {
                LOG.error("Another peer {} declares that it is the leader at term {} which was occupied by leader {}.",
                        serverId, this.currTerm, this.leaderId);
                // Increase the term by 1 and make both leaders step down to minimize the
                // loss of split brain
                this.stepDown(request.getTerm() + 1, false, new Status(RaftError.ELEADERCONFLICT,
                        "More than one leader in the same term."));
                return AppendEntriesResponse.newBuilder() //
                        .setSuccess(false) //
                        .setTerm(request.getTerm() + 1) //
                        .build();
            }

            this.updateLastLeaderTimestamp(Utils.monotonicMs());

            if (entriesCount > 0 && this.snapshotExecutor != null && this.snapshotExecutor.isInstallingSnapshot()) {
                LOG.warn("Node {} received AppendEntriesRequest while installing snapshot.", this.getNodeId());
                return RpcResponseFactory.newResponse(RaftError.EBUSY, "Node %s:%s is installing snapshot.",
                        this.groupId, this.serverId);
            }

            final long prevLogIndex = request.getPrevLogIndex();
            final long prevLogTerm = request.getPrevLogTerm();
            final long localPrevLogTerm = this.logManager.getTerm(prevLogIndex);
            if (localPrevLogTerm != prevLogTerm) {
                final long lastLogIndex = this.logManager.getLastLogIndex();

                LOG.warn(
                        "Node {} reject term_unmatched AppendEntriesRequest from {}, term={}, prevLogIndex={}, prevLogTerm={}, localPrevLogTerm={}, lastLogIndex={}, entriesSize={}.",
                        this.getNodeId(), request.getServerId(), request.getTerm(), prevLogIndex, prevLogTerm, localPrevLogTerm,
                        lastLogIndex, entriesCount);

                return AppendEntriesResponse.newBuilder() //
                        .setSuccess(false) //
                        .setTerm(this.currTerm) //
                        .setLastLogIndex(lastLogIndex) //
                        .build();
            }

            if (entriesCount == 0) {
                // heartbeat
                final AppendEntriesResponse.Builder respBuilder = AppendEntriesResponse.newBuilder() //
                        .setSuccess(true) //
                        .setTerm(this.currTerm) //
                        .setLastLogIndex(this.logManager.getLastLogIndex());
                doUnlock = false;
                this.writeLock.unlock();
                // see the comments at FollowerStableClosure#run()
                this.ballotBox.setLastCommittedIndex(Math.min(request.getCommittedIndex(), prevLogIndex));
                return respBuilder.build();
            }

            // Parse request
            long index = prevLogIndex;
            final List<LogEntry> entries = new ArrayList<>(entriesCount);
            ByteBuffer allData = null;
            if (request.hasData()) {
                allData = request.getData().asReadOnlyByteBuffer();
            }

            final List<RaftOutter.EntryMeta> entriesList = request.getEntriesList();
            for (int i = 0; i < entriesCount; i++) {
                final RaftOutter.EntryMeta entry = entriesList.get(i);
                index++;
                if (entry.getType() != EnumOutter.EntryType.ENTRY_TYPE_UNKNOWN) {
                    final LogEntry logEntry = new LogEntry();
                    logEntry.setId(new LogId(index, entry.getTerm()));
                    logEntry.setType(entry.getType());
                    if (entry.hasChecksum()) {
                        logEntry.setChecksum(entry.getChecksum()); // since 1.2.6
                    }
                    final long dataLen = entry.getDataLen();
                    if (dataLen > 0) {
                        final byte[] bs = new byte[(int) dataLen];
                        assert allData != null;
                        allData.get(bs, 0, bs.length);
                        logEntry.setData(ByteBuffer.wrap(bs));
                    }

                    if (entry.getPeersCount() > 0) {
                        if (entry.getType() != EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION) {
                            throw new IllegalStateException(
                                    "Invalid log entry that contains peers but is not ENTRY_TYPE_CONFIGURATION type: "
                                            + entry.getType());
                        }

                        final List<PeerId> peers = new ArrayList<>(entry.getPeersCount());
                        for (final String peerStr : entry.getPeersList()) {
                            final PeerId peer = new PeerId();
                            peer.parse(peerStr);
                            peers.add(peer);
                        }
                        logEntry.setPeers(peers);

                        if (entry.getOldPeersCount() > 0) {
                            final List<PeerId> oldPeers = new ArrayList<>(entry.getOldPeersCount());
                            for (final String peerStr : entry.getOldPeersList()) {
                                final PeerId peer = new PeerId();
                                peer.parse(peerStr);
                                oldPeers.add(peer);
                            }
                            logEntry.setOldPeers(oldPeers);
                        }
                    } else if (entry.getType() == EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION) {
                        throw new IllegalStateException(
                                "Invalid log entry that contains zero peers but is ENTRY_TYPE_CONFIGURATION type");
                    }

                    // Validate checksum
                    if (this.raftOptions.isEnableLogEntryChecksum() && logEntry.isCorrupted()) {
                        long realChecksum = logEntry.checksum();
                        LOG.error(
                                "Corrupted log entry received from leader, index={}, term={}, expectedChecksum={}, realChecksum={}",
                                logEntry.getId().getIndex(), logEntry.getId().getTerm(), logEntry.getChecksum(),
                                realChecksum);
                        return RpcResponseFactory.newResponse(RaftError.EINVAL,
                                "The log entry is corrupted, index=%d, term=%d, expectedChecksum=%d, realChecksum=%d",
                                logEntry.getId().getIndex(), logEntry.getId().getTerm(), logEntry.getChecksum(),
                                realChecksum);
                    }

                    entries.add(logEntry);
                }
            }

            final FollowerStableClosure closure = new FollowerStableClosure(request, AppendEntriesResponse.newBuilder()
                    .setTerm(this.currTerm), this, done, this.currTerm);
            this.logManager.appendEntries(entries, closure);
            // update configuration after _log_manager updated its memory status
            this.conf = this.logManager.checkAndSetConfiguration(this.conf);
            return null;
        } finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
            this.metrics.recordLatency("handle-append-entries", Utils.monotonicMs() - startMs);
            this.metrics.recordSize("handle-append-entries-count", entriesCount);
        }
    }

    // called when leader receive greater term in AppendEntriesResponse
    void increaseTermTo(final long newTerm, final Status status) {
        this.writeLock.lock();
        try {
            if (newTerm < this.currTerm) {
                return;
            }
            this.stepDown(newTerm, false, status);
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
                LOG.debug("Node {} waits peer {} to catch up.", this.getNodeId(), peer);
                final OnCaughtUp caughtUp = new OnCaughtUp(this, term, peer, version);
                final long dueTime = Utils.nowMs() + this.options.getElectionTimeoutMs();
                if (this.replicatorGroup.waitCaughtUp(peer, this.options.getCatchupMargin(), dueTime, caughtUp)) {
                    return;
                }
                LOG.warn("Node {} waitCaughtUp failed, peer={}.", this.getNodeId(), peer);
            }
            LOG.warn("Node {} caughtUp failed, status={}, peer={}.", this.getNodeId(), st, peer);
            this.confCtx.onCaughtUp(version, peer, false);
        } finally {
            this.writeLock.unlock();
        }
    }

    private void checkDeadNodes(final Configuration conf, final long monotonicNowMs) {
        final List<PeerId> peers = conf.listPeers();
        final Configuration deadNodes = new Configuration();
        if (this.checkDeadNodes0(peers, monotonicNowMs, true, deadNodes)) {
            return;
        }
        LOG.warn("Node {} steps down when alive nodes don't satisfy quorum, term={}, deadNodes={}, conf={}.",
                this.getNodeId(), this.currTerm, deadNodes, conf);
        final Status status = new Status();
        status.setError(RaftError.ERAFTTIMEDOUT, "Majority of the group dies: %d/%d", deadNodes.size(), peers.size());
        this.stepDown(this.currTerm, false, status);
    }

    private boolean checkDeadNodes0(final List<PeerId> peers,
                                    final long monotonicNowMs,
                                    final boolean checkReplicator,
                                    final Configuration deadNodes) {
        final int leaderLeaseTimeoutMs = this.options.getLeaderLeaseTimeoutMs();
        int aliveCount = 0;
        long startLease = Long.MAX_VALUE;
        for (final PeerId peer : peers) {
            if (peer.equals(this.serverId)) {
                aliveCount++;
                continue;
            }
            if (checkReplicator) {
                this.checkReplicator(peer);
            }
            final long lastRpcSendTimestamp = this.replicatorGroup.getLastRpcSendTimestamp(peer);
            if (monotonicNowMs - lastRpcSendTimestamp <= leaderLeaseTimeoutMs) {
                aliveCount++;
                if (startLease > lastRpcSendTimestamp) {
                    startLease = lastRpcSendTimestamp;
                }
                continue;
            }
            if (deadNodes != null) {
                deadNodes.addPeer(peer);
            }
        }
        if (aliveCount >= peers.size() / 2 + 1) {
            this.updateLastLeaderTimestamp(startLease);
            return true;
        }
        return false;
    }

    // in read_lock
    private List<PeerId> getAliveNodes(final List<PeerId> peers, final long monotonicNowMs) {
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
            if (this.state.compareTo(State.STATE_TRANSFERRING) > 0) {
                LOG.debug("Node {} stop step-down timer, term={}, state={}.", this.getNodeId(), this.currTerm, this.state);
                return;
            }
            final long monotonicNowMs = Utils.monotonicMs();
            this.checkDeadNodes(this.conf.getConf(), monotonicNowMs);
            if (!this.conf.getOldConf().isEmpty()) {
                this.checkDeadNodes(this.conf.getOldConf(), monotonicNowMs);
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
                NodeImpl.this.onConfigurationChangeDone(this.term);
                if (this.leaderStart) {
                    NodeImpl.this.getOptions().getFsm().onLeaderStart(this.term);
                }
            } else {
                LOG.error("Fail to run ConfigurationChangeDone, status: {}.", status);
            }
        }
    }

    private void unsafeApplyConfiguration(final Configuration newConf, final Configuration oldConf,
                                          final boolean leaderStart) {
        Requires.requireTrue(this.confCtx.isBusy(), "ConfigurationContext is not busy");
        final LogEntry entry = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION);
        entry.setId(new LogId(0, this.currTerm));
        entry.setPeers(newConf.listPeers());
        if (oldConf != null) {
            entry.setOldPeers(oldConf.listPeers());
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
        this.conf = this.logManager.checkAndSetConfiguration(this.conf);
    }

    private void unsafeRegisterConfChange(final Configuration oldConf, final Configuration newConf, final Closure done) {
        if (this.state != State.STATE_LEADER) {
            LOG.warn("Node {} refused configuration changing as the state={}.", this.getNodeId(), this.state);
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
            LOG.warn("Node {} refused configuration concurrent changing.", this.getNodeId());
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

    public TimerManager getTimerManager() {
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
        this.shutdown(null);
    }

    public void onConfigurationChangeDone(final long term) {
        this.writeLock.lock();
        try {
            if (term != this.currTerm || this.state.compareTo(State.STATE_TRANSFERRING) > 0) {
                LOG.warn("Node {} process onConfigurationChangeDone at term {} while state={}, currTerm={}.",
                        this.getNodeId(), term, this.state, this.currTerm);
                return;
            }
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
        LOG.warn("Node {} got error: {}.", this.getNodeId(), error);
        if (this.fsmCaller != null) {
            // onError of fsmCaller is guaranteed to be executed once.
            this.fsmCaller.onError(error);
        }
        this.writeLock.lock();
        try {
            // If it is leader, need to wake up a new one;
            // If it is follower, also step down to call on_stop_following.
            if (this.state.compareTo(State.STATE_FOLLOWER) <= 0) {
                this.stepDown(this.currTerm, this.state == State.STATE_LEADER, new Status(RaftError.EBADNODE,
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
            if (this.state != State.STATE_CANDIDATE) {
                LOG.warn("Node {} received invalid RequestVoteResponse from {}, state not in STATE_CANDIDATE but {}.",
                        this.getNodeId(), peerId, this.state);
                return;
            }
            // check stale term
            if (term != this.currTerm) {
                LOG.warn("Node {} received stale RequestVoteResponse from {}, term={}, currTerm={}.", this.getNodeId(),
                        peerId, term, this.currTerm);
                return;
            }
            // check response term
            if (response.getTerm() > this.currTerm) {
                LOG.warn("Node {} received invalid RequestVoteResponse from {}, term={}, expect={}.", this.getNodeId(),
                        peerId, response.getTerm(), this.currTerm);
                this.stepDown(response.getTerm(), false, new Status(RaftError.EHIGHERTERMRESPONSE,
                        "Raft node receives higher term request_vote_response."));
                return;
            }
            // check granted quorum?
            if (response.getGranted()) {
                this.voteCtx.grant(peerId);
                if (this.voteCtx.isGranted()) {
                    this.becomeLeader();
                }
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * 投票请求响应回调
     */
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
                this.node.handleRequestVoteResponse(this.peer, this.term, this.getResponse());
            }
        }
    }

    /**
     * 处理 pre-vote 响应
     *
     * @param peerId
     * @param term
     * @param response
     */
    public void handlePreVoteResponse(final PeerId peerId, final long term, final RequestVoteResponse response) {
        boolean doUnlock = true;
        this.writeLock.lock();
        try {
            // 当前节点已经不是 follower，则忽略 pre-vote 响应
            if (this.state != State.STATE_FOLLOWER) {
                LOG.warn("Node {} received invalid PreVoteResponse from {}, state not in STATE_FOLLOWER but {}.",
                        this.getNodeId(), peerId, this.state);
                return;
            }
            // term 值已经发生变化
            if (term != this.currTerm) {
                LOG.warn("Node {} received invalid PreVoteResponse from {}, term={}, currTerm={}.",
                        this.getNodeId(), peerId, term, this.currTerm);
                return;
            }
            // 响应的 term 值大于当前节点的 term 值，则说明有其它节点竞选成功
            if (response.getTerm() > this.currTerm) {
                LOG.warn("Node {} received invalid PreVoteResponse from {}, term {}, expect={}.",
                        this.getNodeId(), peerId, response.getTerm(), this.currTerm);
                this.stepDown(response.getTerm(), false,
                        new Status(RaftError.EHIGHERTERMRESPONSE, "Raft node receives higher term pre_vote_response."));
                return;
            }
            LOG.info("Node {} received PreVoteResponse from {}, term={}, granted={}.",
                    this.getNodeId(), peerId, response.getTerm(), response.getGranted());
            // check granted quorum?
            if (response.getGranted()) {
                this.prevVoteCtx.grant(peerId);
                // 获取半数以上的投票
                if (this.prevVoteCtx.isGranted()) {
                    doUnlock = false;
                    // 选举自己为 leader
                    this.electSelf();
                }
            }
        } finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
        }
    }

    /**
     * pre-vote 响应回调
     */
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
            metrics.recordLatency("pre-vote", Utils.monotonicMs() - this.startMs);
            if (!status.isOk()) {
                LOG.warn("Node {} PreVote to {} error: {}.", NodeImpl.this.getNodeId(), this.peer, status);
            } else {
                handlePreVoteResponse(this.peer, this.term, this.getResponse());
            }
        }
    }

    /**
     * 预投票
     *
     * in writeLock
     */
    private void preVote() {
        long oldTerm;
        try {
            LOG.info("Node {} term {} start preVote.", this.getNodeId(), this.currTerm);
            // 当前正在安装 Snapshot，则不能参选
            if (this.snapshotExecutor != null && this.snapshotExecutor.isInstallingSnapshot()) {
                LOG.warn(
                        "Node {} term {} doesn't do preVote when installing snapshot as the configuration may be out of date.",
                        this.getNodeId(), currTerm);
                return;
            }
            // 如果当前节点不包含在集群节点列表中则不应该参选
            if (!this.conf.contains(this.serverId)) {
                LOG.warn("Node {} can't do preVote as it is not in conf <{}>.", this.getNodeId(), this.conf);
                return;
            }
            oldTerm = this.currTerm;
        } finally {
            this.writeLock.unlock();
        }

        // 返回最新的 log 实体类
        final LogId lastLogId = this.logManager.getLastLogId(true);

        boolean doUnlock = true;
        this.writeLock.lock();
        try {
            // pre_vote need defense ABA after unlock&writeLock
            if (oldTerm != this.currTerm) {
                LOG.warn("Node {} raise term {} when get lastLogId.", this.getNodeId(), this.currTerm);
                return;
            }
            // 初始化预投票箱
            this.prevVoteCtx.init(this.conf.getConf(), this.conf.isStable() ? null : this.conf.getOldConf());
            // 遍历集群中所有的节点
            for (final PeerId peer : this.conf.listPeers()) {
                // 跳过当前节点
                if (peer.equals(this.serverId)) {
                    continue;
                }
                // 校验目标节点是否可连接，跳过不可连接的节点
                if (!this.rpcService.connect(peer.getEndpoint())) {
                    LOG.warn("Node {} channel init failed, address={}.", this.getNodeId(), peer.getEndpoint());
                    continue;
                }
                // 回调类，用于处理 pre-vote 响应
                final OnPreVoteRpcDone done = new OnPreVoteRpcDone(peer, this.currTerm);
                // 构造一个 RequestVoteRequest 请求
                done.request = RequestVoteRequest.newBuilder() //
                        .setPreVote(true) // 标识当前是预投票
                        .setGroupId(this.groupId) //
                        .setServerId(this.serverId.toString()) //
                        .setPeerId(peer.toString()) //
                        .setTerm(this.currTerm + 1) // term 值加 1
                        .setLastLogIndex(lastLogId.getIndex()) //
                        .setLastLogTerm(lastLogId.getTerm()) //
                        .build();
                // 发起预投票
                this.rpcService.preVote(peer.getEndpoint(), done.request, done);
            } // end of for

            // 投票给自己
            this.prevVoteCtx.grant(this.serverId);
            if (this.prevVoteCtx.isGranted()) {
                doUnlock = false;
                this.electSelf();
            }
        } finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
        }
    }

    private void handleVoteTimeout() {
        this.writeLock.lock();
        // 当前节点状态为 CANDIDATE，尝试竞选
        if (this.state == State.STATE_CANDIDATE) {
            LOG.debug("Node {} term {} retry elect.", this.getNodeId(), this.currTerm);
            this.electSelf();
        } else {
            this.writeLock.unlock();
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
        this.writeLock.lock();
        try {
            LOG.info("Node {} shutdown, currTerm={} state={}.", this.getNodeId(), this.currTerm, this.state);
            if (this.state.compareTo(State.STATE_SHUTTING) < 0) {
                NodeManager.getInstance().remove(this);
                // If it is leader, set the wakeup_a_candidate with true;
                // If it is follower, call on_stop_following in step_down
                if (this.state.compareTo(State.STATE_FOLLOWER) <= 0) {
                    this.stepDown(this.currTerm, this.state == State.STATE_LEADER,
                            new Status(RaftError.ESHUTDOWN, "Raft node is going to quit."));
                }
                this.state = State.STATE_SHUTTING;
                // Destroy all timers
                if (this.electionTimer != null) {
                    this.electionTimer.destroy();
                }
                if (this.voteTimer != null) {
                    this.voteTimer.destroy();
                }
                if (this.stepDownTimer != null) {
                    this.stepDownTimer.destroy();
                }
                if (this.snapshotTimer != null) {
                    this.snapshotTimer.destroy();
                }
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
                    Utils.runInThread(() -> {
                        this.shutdownLatch = new CountDownLatch(1);
                        this.applyQueue.publishEvent((event, sequence) -> event.shutdownLatch = this.shutdownLatch);
                    });
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
        }
    }

    @Override
    public synchronized void join() throws InterruptedException {
        if (this.shutdownLatch != null) {
            if (this.readOnlyService != null) {
                this.readOnlyService.join();
            }
            if (this.fsmCaller != null) {
                this.fsmCaller.join();
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
        LOG.info("Node {} failed to transfer leadership to peer {}, reached timeout.", this.getNodeId(), peer);
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
            return this.getAliveNodes(this.conf.getConf().getPeers(), Utils.monotonicMs());
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
            this.unsafeRegisterConfChange(this.conf.getConf(), newConf, done);
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
            this.unsafeRegisterConfChange(this.conf.getConf(), newConf, done);
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
            LOG.info("Node {} change peers from {} to {}.", this.getNodeId(), this.conf.getConf(), newPeers);
            this.unsafeRegisterConfChange(this.conf.getConf(), newPeers, done);
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public Status resetPeers(final Configuration newPeers) {
        Requires.requireNonNull(newPeers, "Null new peers");
        Requires.requireTrue(!newPeers.isEmpty(), "Empty new peers");
        this.writeLock.lock();
        try {
            if (newPeers.isEmpty()) {
                LOG.warn("Node {} set empty peers.", this.getNodeId());
                return new Status(RaftError.EINVAL, "newPeers is empty");
            }
            if (!this.state.isActive()) {
                LOG.warn("Node {} is in state {}, can't set peers.", this.getNodeId(), this.state);
                return new Status(RaftError.EPERM, "Bad state: %s", this.state);
            }
            // bootstrap?
            if (this.conf.getConf().isEmpty()) {
                LOG.info("Node {} set peers to {} from empty.", this.getNodeId(), newPeers);
                this.conf.setConf(newPeers);
                this.stepDown(this.currTerm + 1, false, new Status(RaftError.ESETPEER, "Set peer from empty configuration"));
                return Status.OK();
            }
            if (this.state == State.STATE_LEADER && this.confCtx.isBusy()) {
                LOG.warn("Node {} set peers need wait current conf changing.", this.getNodeId());
                return new Status(RaftError.EBUSY, "Changing to another configuration");
            }
            // check equal, maybe retry direct return
            if (this.conf.getConf().equals(newPeers)) {
                return Status.OK();
            }
            final Configuration newConf = new Configuration(newPeers);
            LOG.info("Node {} set peers from {} to {}.", this.getNodeId(), this.conf.getConf(), newPeers);
            this.conf.setConf(newConf);
            this.conf.getOldConf().reset();
            this.stepDown(this.currTerm + 1, false, new Status(RaftError.ESETPEER, "Raft node set peer normally"));
            return Status.OK();
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void snapshot(final Closure done) {
        this.doSnapshot(done);
    }

    private void doSnapshot(final Closure done) {
        if (this.snapshotExecutor != null) {
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
            this.replicatorGroup.resetHeartbeatInterval(this.heartbeatTimeout(this.options.getElectionTimeoutMs()));
            this.replicatorGroup.resetElectionTimeoutInterval(electionTimeoutMs);
            LOG.info("Node {} reset electionTimeout, currTimer {} state {} new electionTimeout {}.", this.getNodeId(),
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
                LOG.warn("Node {} can't transfer leadership to peer {} as it is in state {}.", this.getNodeId(), peer,
                        this.state);
                return new Status(this.state == State.STATE_TRANSFERRING ? RaftError.EBUSY : RaftError.EPERM,
                        "Not a leader");
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
                        this.getNodeId(), peer);
                return new Status(RaftError.EBUSY, "Changing the configuration");
            }

            PeerId peerId = peer.copy();
            // if peer_id is ANY_PEER(0.0.0.0:0:0), the peer with the largest
            // last_log_id will be selected.
            if (peerId.equals(PeerId.ANY_PEER)) {
                LOG.info("Node {} starts to transfer leadership to any peer.", this.getNodeId());
                if ((peerId = this.replicatorGroup.findTheNextCandidate(this.conf)) == null) {
                    return new Status(-1, "Candidate not found for any peer");
                }
            }
            if (peerId.equals(this.serverId)) {
                LOG.info("Node {} transferred leadership to self.", this.serverId);
                return Status.OK();
            }
            if (!this.conf.contains(peerId)) {
                LOG.info("Node {} refused to transfer leadership to peer {} as it is not in {}.", this.getNodeId(), peer,
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
            this.onLeaderStop(status);
            LOG.info("Node {} starts to transfer leadership to peer {}.", this.getNodeId(), peer);
            final StopTransferArg stopArg = new StopTransferArg(this, this.currTerm, peerId);
            this.stopTransferArg = stopArg;
            this.transferTimer = this.timerManager.schedule(() -> this.onTransferTimeout(stopArg),
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
                    this.stepDown(request.getTerm(), false, new Status(RaftError.EHIGHERTERMREQUEST,
                            "Raft node receives higher term request"));
                }
                LOG.info("Node {} received TimeoutNowRequest from {} while currTerm={} didn't match requestTerm={}.",
                        this.getNodeId(), request.getPeerId(), savedCurrTerm, request.getTerm());
                return TimeoutNowResponse.newBuilder() //
                        .setTerm(this.currTerm) //
                        .setSuccess(false) //
                        .build();
            }
            if (this.state != State.STATE_FOLLOWER) {
                LOG.info("Node {} received TimeoutNowRequest from {}, while state={}, term={}.", this.getNodeId(),
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
            this.electSelf();
            LOG.info("Node {} received TimeoutNowRequest from {}, term={}.", this.getNodeId(), request.getServerId(), savedTerm);
        } finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
        }
        return null;
    }

    @Override
    public Message handleInstallSnapshot(final InstallSnapshotRequest request, final RpcRequestClosure done) {
        if (this.snapshotExecutor == null) {
            return RpcResponseFactory.newResponse(RaftError.EINVAL, "Not supported snapshot");
        }
        final PeerId serverId = new PeerId();
        if (!serverId.parse(request.getServerId())) {
            LOG.warn("Node {} ignore InstallSnapshotRequest from {} bad server id.", this.getNodeId(), request.getServerId());
            return RpcResponseFactory.newResponse(RaftError.EINVAL, "Parse serverId failed: %s", request.getServerId());
        }

        this.writeLock.lock();
        try {
            if (!this.state.isActive()) {
                LOG.warn("Node {} ignore InstallSnapshotRequest as it is not in active state {}.", this.getNodeId(),
                        this.state);
                return RpcResponseFactory.newResponse(RaftError.EINVAL, "Node %s:%s is not in active state, state %s.",
                        this.groupId, this.serverId, this.state.name());
            }

            if (request.getTerm() < this.currTerm) {
                LOG.warn("Node {} ignore stale InstallSnapshotRequest from {}, term={}, currTerm={}.", this.getNodeId(),
                        request.getPeerId(), request.getTerm(), this.currTerm);
                return InstallSnapshotResponse.newBuilder() //
                        .setTerm(this.currTerm) //
                        .setSuccess(false) //
                        .build();
            }

            this.checkStepDown(request.getTerm(), serverId);

            if (!serverId.equals(this.leaderId)) {
                LOG.error("Another peer {} declares that it is the leader at term {} which was occupied by leader {}.",
                        serverId, this.currTerm, this.leaderId);
                // Increase the term by 1 and make both leaders step down to minimize the
                // loss of split brain
                this.stepDown(request.getTerm() + 1, false, new Status(RaftError.ELEADERCONFLICT,
                        "More than one leader in the same term."));
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
                        this.getNodeId(), request.getServerId(), request.getMeta().getLastIncludedIndex(), request.getMeta()
                                .getLastIncludedTerm(), this.logManager.getLastLogId(false));
            }
            this.snapshotExecutor.installSnapshot(request, InstallSnapshotResponse.newBuilder(), done);
            return null;
        } finally {
            this.metrics.recordLatency("install-snapshot", Utils.monotonicMs() - startMs);
        }
    }

    public void updateConfigurationAfterInstallingSnapshot() {
        this.writeLock.lock();
        try {
            this.conf = this.logManager.checkAndSetConfiguration(this.conf);
        } finally {
            this.writeLock.unlock();
        }
    }

    private void stopReplicator(final List<PeerId> keep, final List<PeerId> drop) {
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
    public void describe(final Printer out) {
        // node
        final String _nodeId;
        final String _state;
        final long _currTerm;
        final String _conf;
        this.readLock.lock();
        try {
            _nodeId = String.valueOf(this.getNodeId());
            _state = String.valueOf(this.state);
            _currTerm = this.currTerm;
            _conf = String.valueOf(this.conf);
        } finally {
            this.readLock.unlock();
        }
        out.print("nodeId: ") //
                .println(_nodeId);
        out.print("state: ") //
                .println(_state);
        out.print("term: ") //
                .println(_currTerm);
        out.print("conf: ") //
                .println(_conf);

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
    }

    @Override
    public String toString() {
        return "JRaftNode [nodeId=" + this.getNodeId() + "]";
    }
}
