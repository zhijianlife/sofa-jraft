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

import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.option.SnapshotCopierOptions;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotCopier;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;

/**
 * Snapshot storage.
 *
 * 用于存放用户的状态机 Snapshot 及元信息。
 *
 * 当 Raft 节点 Node 重启时，内存中状态机的状态数据丢失，触发启动过程重新存放日志存储 LogStorage 的所有日志重建整个状态机实例，此种场景会导致两个问题：
 *
 * 1. 如果任务提交比较频繁，例如消息中间件场景导致整个重建过程很长启动缓慢；
 * 2. 如果日志非常多并且节点需要存储所有的日志，对存储来说是资源占用不可持续；
 * 3. 如果增加 Node 节点，新节点需要从 Leader 获取所有的日志重新存放至状态机，对于 Leader 和网络带宽都是不小的负担。
 *
 * 因此通过引入 Snapshot 机制来解决此三个问题，所谓快照 Snapshot 即对数据当前值的记录，是为当前状态机的最新状态构建”镜像”单独保存，
 * 保存成功删除此时刻之前的日志减少日志存储占用；启动的时候直接加载最新的 Snapshot 镜像，然后重放在此之后的日志即可，如果 Snapshot 间隔合理，
 * 整个重放到状态机过程较快，加速启动过程。最后新节点的加入先从 Leader 拷贝最新的 Snapshot 安装到本地状态机，然后只要拷贝后续的日志即可，
 * 能够快速跟上整个 Raft Group 的进度。
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-08 3:30:05 PM
 */
public interface SnapshotStorage extends Lifecycle<Void>, Storage {

    /**
     * Set filterBeforeCopyRemote to be true. When true, it will filter the data before copy to remote.
     *
     * 设置 filterBeforeCopyRemote 设置为 true 复制到远程之前过滤数据；
     */
    boolean setFilterBeforeCopyRemote();

    /**
     * Create a snapshot writer.
     *
     * 创建快照编写器
     */
    SnapshotWriter create();

    /**
     * Open a snapshot reader.
     *
     * 打开快照阅读器
     */
    SnapshotReader open();

    /**
     * Copy data from remote uri.
     *
     * 从远程 Uri 复制数据
     *
     * @param uri remote uri
     * @param opts copy options
     * @return a SnapshotReader instance
     */
    SnapshotReader copyFrom(String uri, SnapshotCopierOptions opts);

    /**
     * Starts a copy job to copy data from remote uri.
     *
     * 启动从远程 Uri 复制数据的复制任务
     *
     * @param uri remote uri
     * @param opts copy options
     * @return a SnapshotCopier instance
     */
    SnapshotCopier startToCopyFrom(String uri, SnapshotCopierOptions opts);

    /**
     * Configure a SnapshotThrottle.
     *
     * @param snapshotThrottle throttle of snapshot
     */
    void setSnapshotThrottle(SnapshotThrottle snapshotThrottle);
}
