package com.alipay.sofa.jraft.example.pharos;

import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.TaskClosure;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.util.internal.ThrowUtil;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class PharosNode implements Lifecycle<PharosNodeOptions> {

    private static final Logger log = LoggerFactory.getLogger(PharosNode.class);

    private final ConcurrentMap<Long, Integer> DB = new ConcurrentHashMap<>();

    private final List<LeaderStateListener> listeners = new CopyOnWriteArrayList<>();
    private RaftGroupService raftGroupService;
    private Node node;
    private PharosStateMachine fsm;

    private boolean started;

    @Override
    public boolean init(final PharosNodeOptions opts) {
        if (this.started) {
            log.info("[ElectionNode: {}] already started.", opts.getServerAddress());
            return true;
        }

        // node options
        NodeOptions nodeOpts = opts.getNodeOptions();
        if (nodeOpts == null) {
            nodeOpts = new NodeOptions();
        }
        this.fsm = new PharosStateMachine(DB, this.listeners);
        nodeOpts.setFsm(this.fsm);
        final Configuration initialConf = new Configuration();
        if (!initialConf.parse(opts.getInitialServerAddressList())) {
            throw new IllegalArgumentException(
                    "Fail to parse initConf: " + opts.getInitialServerAddressList());
        }
        // Set the initial cluster configuration
        nodeOpts.setInitialConf(initialConf);
        final String dataPath = opts.getDataPath();
        try {
            FileUtils.forceMkdir(new File(dataPath));
        } catch (final IOException e) {
            log.error("Fail to make dir for dataPath {}.", dataPath);
            return false;
        }
        // Set the data path
        // Log, required
        nodeOpts.setLogUri(Paths.get(dataPath, "log").toString());
        // Metadata, required
        nodeOpts.setRaftMetaUri(Paths.get(dataPath, "meta").toString());
        nodeOpts.setSnapshotUri(Paths.get(dataPath, "snapshot").toString());

        final String groupId = opts.getGroupId();
        final PeerId serverId = new PeerId();
        if (!serverId.parse(opts.getServerAddress())) {
            throw new IllegalArgumentException("Fail to parse serverId: " + opts.getServerAddress());
        }
        final RpcServer rpcServer = RaftRpcServerFactory.createRaftRpcServer(serverId.getEndpoint());
        rpcServer.registerProcessor(new PharosRpcProcessor(this));
        this.raftGroupService = new RaftGroupService(groupId, serverId, nodeOpts, rpcServer);
        this.node = this.raftGroupService.start();
        if (this.node != null) {
            this.started = true;
        }
        return this.started;

    }

    public void put(long userId, int idcCode) {
        if (!node.isLeader()) {
            throw new IllegalStateException("not leader");
        }

        ByteBuffer buffer = ByteBuffer.allocate(12);
        buffer.putLong(userId);
        buffer.putInt(idcCode);
        buffer.flip();
        final Task task = new Task(buffer, new TaskClosure() {
            @Override
            public void onCommitted() {
                log.info("On committed, userId: {}, idcCode: {}", userId, idcCode);
            }

            @Override
            public void run(Status status) {
                if (status.isOk()) {
                    log.debug("Apply task success, userId: {}, idcCode: {}", userId, idcCode);
                } else {
                    log.error("Apply task error, reason: {}, userId: {}, idcCode: {}", status, userId, idcCode);
                }
            }
        });
        node.apply(task);
    }

    public void commit(boolean commit) {
        if (!node.isLeader()) {
            throw new IllegalStateException("not leader");
        }
        fsm.commit(commit);
    }

    @Override
    public void shutdown() {
        if (!this.started) {
            return;
        }
        if (this.raftGroupService != null) {
            this.raftGroupService.shutdown();
            try {
                this.raftGroupService.join();
            } catch (final InterruptedException e) {
                ThrowUtil.throwException(e);
            }
        }
        this.started = false;
        log.info("[PharosNode] shutdown successfully: {}.", this);
    }

    public Node getNode() {
        return node;
    }

    public PharosStateMachine getFsm() {
        return fsm;
    }

    public boolean isStarted() {
        return started;
    }

    public boolean isLeader() {
        return this.fsm.isLeader();
    }

    public void addLeaderStateListener(final LeaderStateListener listener) {
        this.listeners.add(listener);
    }

    public ConcurrentMap<Long, Integer> getDB() {
        return DB;
    }
}
