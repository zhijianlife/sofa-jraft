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

import com.alipay.remoting.util.ConcurrentHashSet;
import com.alipay.sofa.jraft.ReplicatorGroup;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.CatchUpClosure;
import com.alipay.sofa.jraft.conf.ConfigurationEntry;
import com.alipay.sofa.jraft.entity.NodeId;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.option.ReplicatorGroupOptions;
import com.alipay.sofa.jraft.option.ReplicatorOptions;
import com.alipay.sofa.jraft.rpc.RpcRequests.AppendEntriesResponse;
import com.alipay.sofa.jraft.rpc.RpcResponseClosure;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.ThreadId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Replicator group for a raft group.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-04 1:54:51 PM
 */
public class ReplicatorGroupImpl implements ReplicatorGroup {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicatorGroupImpl.class);

    /** [peerId, replicatorId], 记录与目标节点关联的 Replicator 对象 */
    private final ConcurrentMap<PeerId, ThreadId> replicatorMap = new ConcurrentHashMap<>();

    /** common replicator options */
    private ReplicatorOptions commonOptions;
    private int dynamicTimeoutMs = -1;
    private int electionTimeoutMs = -1;
    private RaftOptions raftOptions;
    private final Set<PeerId> failureReplicators = new ConcurrentHashSet<>();

    @Override
    public boolean init(final NodeId nodeId, final ReplicatorGroupOptions opts) {
        this.dynamicTimeoutMs = opts.getHeartbeatTimeoutMs();
        this.electionTimeoutMs = opts.getElectionTimeoutMs();
        this.raftOptions = opts.getRaftOptions();
        this.commonOptions = new ReplicatorOptions();
        this.commonOptions.setDynamicHeartBeatTimeoutMs(this.dynamicTimeoutMs);
        this.commonOptions.setElectionTimeoutMs(this.electionTimeoutMs);
        this.commonOptions.setRaftRpcService(opts.getRaftRpcClientService());
        this.commonOptions.setLogManager(opts.getLogManager());
        this.commonOptions.setBallotBox(opts.getBallotBox());
        this.commonOptions.setNode(opts.getNode());
        this.commonOptions.setTerm(0);
        this.commonOptions.setGroupId(nodeId.getGroupId());
        this.commonOptions.setServerId(nodeId.getPeerId());
        this.commonOptions.setSnapshotStorage(opts.getSnapshotStorage());
        this.commonOptions.setRaftRpcService(opts.getRaftRpcClientService());
        this.commonOptions.setTimerManager(opts.getTimerManager());
        return true;
    }

    @Override
    public boolean addReplicator(final PeerId peer) {
        Requires.requireTrue(this.commonOptions.getTerm() != 0);
        // 已经添加了 Replicator，直接返回
        if (this.replicatorMap.containsKey(peer)) {
            this.failureReplicators.remove(peer);
            return true;
        }
        final ReplicatorOptions opts = this.commonOptions.copy();
        opts.setPeerId(peer);
        // 创建并启动一个到目标节点的 Replicator
        final ThreadId rid = Replicator.start(opts, this.raftOptions);
        // 启动失败
        if (rid == null) {
            LOG.error("Fail to start replicator to peer={}.", peer);
            this.failureReplicators.add(peer);
            return false;
        }
        // 记录
        return this.replicatorMap.put(peer, rid) == null;
    }

    @Override
    public void sendHeartbeat(final PeerId peer, final RpcResponseClosure<AppendEntriesResponse> closure) {
        final ThreadId rid = this.replicatorMap.get(peer);
        if (rid == null) {
            if (closure != null) {
                closure.run(new Status(RaftError.EHOSTDOWN, "Peer %s is not connected", peer));
            }
            return;
        }
        // 发送心跳请求
        Replicator.sendHeartbeat(rid, closure);
    }

    @Override
    public void checkReplicator(final PeerId peer, final boolean lockNode) {
        final ThreadId rid = this.replicatorMap.get(peer);
        // noinspection StatementWithEmptyBody
        if (rid == null) {
            // Create replicator if it's not found for leader.
            final NodeImpl node = this.commonOptions.getNode();
            if (lockNode) {
                node.writeLock.lock();
            }
            try {
                if (node.isLeader() && this.failureReplicators.contains(peer) && this.addReplicator(peer)) {
                    this.failureReplicators.remove(peer);
                }
            } finally {
                if (lockNode) {
                    node.writeLock.unlock();
                }
            }
        } else { // NOPMD
            // Unblock it right now.
            // Replicator.unBlockAndSendNow(rid);
        }
    }

    @Override
    public ThreadId getReplicator(final PeerId peer) {
        return this.replicatorMap.get(peer);
    }

    @Override
    public void clearFailureReplicators() {
        this.failureReplicators.clear();
    }

    @Override
    public boolean waitCaughtUp(final PeerId peer, final long maxMargin, final long dueTime, final CatchUpClosure done) {
        final ThreadId rid = this.replicatorMap.get(peer);
        if (rid == null) {
            return false;
        }

        Replicator.waitForCaughtUp(rid, maxMargin, dueTime, done);
        return true;
    }

    @Override
    public long getLastRpcSendTimestamp(final PeerId peer) {
        final ThreadId rid = this.replicatorMap.get(peer);
        if (rid == null) {
            return 0L;
        }
        return Replicator.getLastRpcSendTimestamp(rid);
    }

    @Override
    public boolean stopAll() {
        final List<ThreadId> rids = new ArrayList<>(this.replicatorMap.values());
        this.replicatorMap.clear();
        this.failureReplicators.clear();
        for (final ThreadId rid : rids) {
            Replicator.stop(rid);
        }
        return true;
    }

    @Override
    public boolean stopReplicator(final PeerId peer) {
        LOG.info("Stop replicator to {}.", peer);
        this.failureReplicators.remove(peer);
        final ThreadId rid = this.replicatorMap.remove(peer);
        if (rid == null) {
            return false;
        }
        // Calling ReplicatorId.stop might lead to calling stopReplicator again,
        // erase entry first to avoid race condition
        return Replicator.stop(rid);
    }

    @Override
    public boolean resetTerm(final long newTerm) {
        if (newTerm <= this.commonOptions.getTerm()) {
            return false;
        }
        this.commonOptions.setTerm(newTerm);
        return true;
    }

    @Override
    public boolean resetHeartbeatInterval(final int newIntervalMs) {
        this.dynamicTimeoutMs = newIntervalMs;
        return true;
    }

    @Override
    public boolean resetElectionTimeoutInterval(final int newIntervalMs) {
        this.electionTimeoutMs = newIntervalMs;
        return true;
    }

    @Override
    public boolean contains(final PeerId peer) {
        return this.replicatorMap.containsKey(peer);
    }

    @Override
    public boolean transferLeadershipTo(final PeerId peer, final long logIndex) {
        final ThreadId rid = this.replicatorMap.get(peer);
        return rid != null && Replicator.transferLeadership(rid, logIndex);
    }

    @Override
    public boolean stopTransferLeadership(final PeerId peer) {
        final ThreadId rid = this.replicatorMap.get(peer);
        return rid != null && Replicator.stopTransferLeadership(rid);
    }

    @Override
    public ThreadId stopAllAndFindTheNextCandidate(final ConfigurationEntry conf) {
        ThreadId candidate = null;
        final PeerId candidateId = this.findTheNextCandidate(conf);
        if (candidateId != null) {
            candidate = this.replicatorMap.get(candidateId);
        } else {
            LOG.info("Fail to find the next candidate.");
        }
        for (final ThreadId r : this.replicatorMap.values()) {
            if (r != candidate) {
                Replicator.stop(r);
            }
        }
        this.replicatorMap.clear();
        this.failureReplicators.clear();
        return candidate;
    }

    @Override
    public PeerId findTheNextCandidate(final ConfigurationEntry conf) {
        PeerId peerId = null;
        long maxIndex = -1L;
        for (final Map.Entry<PeerId, ThreadId> entry : this.replicatorMap.entrySet()) {
            if (!conf.contains(entry.getKey())) {
                continue;
            }
            final long nextIndex = Replicator.getNextIndex(entry.getValue());
            if (nextIndex > maxIndex) {
                maxIndex = nextIndex;
                peerId = entry.getKey();
            }
        }

        if (maxIndex == -1L) {
            return null;
        } else {
            return peerId;
        }
    }

    @Override
    public List<ThreadId> listReplicators() {
        return new ArrayList<>(this.replicatorMap.values());
    }

    @Override
    public void describe(final Printer out) {
        out.print("  replicators: ").println(this.replicatorMap.values());
        out.print("  failureReplicators: ").println(this.failureReplicators);
    }
}
