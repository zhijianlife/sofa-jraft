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

package com.alipay.sofa.jraft.storage;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.core.NodeImpl;
import com.alipay.sofa.jraft.option.SnapshotExecutorOptions;
import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.alipay.sofa.jraft.rpc.RpcRequests.InstallSnapshotRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.InstallSnapshotResponse;
import com.alipay.sofa.jraft.util.Describer;

/**
 * Executing Snapshot related stuff.
 *
 * 负责 Raft 状态机 Snapshot 存储、Leader 远程安装快照、复制镜像 Snapshot 文件，包括两大核心操作：
 * 状态机快照 doSnapshot(done) 和安装快照 installSnapshot(request, response, done)。
 *
 * StateMachine 快照 doSnapshot(done) 获取基于临时镜像 temp 文件路径的 Snapshot 存储快照编写器 LocalSnapshotWriter，
 * 加载 __raft_snapshot_meta 快照元数据文件初始化编写器；构建保存镜像回调 SaveSnapshotDone 提供 FSMCaller 调用 StateMachine
 * 的状态转换发布 SNAPSHOT_SAVE 类型任务事件到 Disruptor 队列，通过 Ring Buffer 方式触发申请任务处理器 ApplyTaskHandler 运行快照保存任务，
 * 调用 onSnapshotSave() 方法存储各种类型状态机快照。远程安装快照 installSnapshot(request, response, done) 按照安装镜像请求响应以及快照
 * 原信息创建并且注册快照下载作业 DownloadingSnapshot，加载快照下载 DownloadingSnapshot 获取当前快照拷贝器的阅读器 SnapshotReader，
 * 构建安装镜像回调 InstallSnapshotDone 分配 FSMCaller 调用 StateMachine 的状态转换发布 SNAPSHOT_LOAD 类型任务事件到 Disruptor 队列，
 * 也是通过 Ring Buffer 触发申请任务处理器 ApplyTaskHandler 执行快照安装任务，调用 onSnapshotLoad() 操作加载各种类型状态机快照。
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-22 2:27:02 PM
 */
public interface SnapshotExecutor extends Lifecycle<SnapshotExecutorOptions>, Describer {

    /**
     * Return the owner NodeImpl
     */
    NodeImpl getNode();

    /**
     * Start to snapshot StateMachine, and |done| is called after the
     * execution finishes or fails.
     *
     * @param done snapshot callback
     */
    void doSnapshot(final Closure done);

    /**
     * Install snapshot according to the very RPC from leader
     * After the installing succeeds (StateMachine is reset with the snapshot)
     * or fails, done will be called to respond
     * Errors:
     * - Term mismatches: which happens interrupt_downloading_snapshot was
     * called before install_snapshot, indicating that this RPC was issued by
     * the old leader.
     * - Interrupted: happens when interrupt_downloading_snapshot is called or
     * a new RPC with the same or newer snapshot arrives
     * - Busy: the state machine is saving or loading snapshot
     */
    void installSnapshot(final InstallSnapshotRequest request, final InstallSnapshotResponse.Builder response,
                         final RpcRequestClosure done);

    /**
     * Interrupt the downloading if possible.
     * This is called when the term of node increased to |new_term|, which
     * happens when receiving RPC from new peer. In this case, it's hard to
     * determine whether to keep downloading snapshot as the new leader
     * possibly contains the missing logs and is going to send AppendEntries. To
     * make things simplicity and leader changing during snapshot installing is
     * very rare. So we interrupt snapshot downloading when leader changes, and
     * let the new leader decide whether to install a new snapshot or continue
     * appending log entries.
     *
     * NOTE: we can't interrupt the snapshot installing which has finished
     * downloading and is reseting the State Machine.
     *
     * @param newTerm new term num
     */
    void interruptDownloadingSnapshots(final long newTerm);

    /**
     * Returns true if this is currently installing a snapshot, either
     * downloading or loading.
     */
    boolean isInstallingSnapshot();

    /**
     * Returns the backing snapshot storage
     */
    SnapshotStorage getSnapshotStorage();

    /**
     * Block the current thread until all the running job finishes (including failure)
     */
    void join() throws InterruptedException;
}
