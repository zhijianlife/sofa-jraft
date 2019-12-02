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

package com.alipay.sofa.jraft.storage.snapshot;

import com.alipay.sofa.jraft.Status;
import com.google.protobuf.Message;

import java.util.Set;

/**
 * Represents a state machine snapshot.
 *
 * Snapshot 是快照，所谓快照就是对数据当前值的一个记录，Leader 生成快照有这么几个作用：
 *
 * 1. 当有新的 Node 加入集群的时候，不用只靠日志复制、回放去和 Leader 保持数据一致，而是通过安装 Leader 的快照来跳过早期大量日志的回放；
 * 2. Leader 用快照替代 Log 复制可以减少网络上的数据量；
 * 3. 用快照替代早期的 Log 可以节省存储空间。
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-07 10:17:52 AM
 */
public abstract class Snapshot extends Status {

    /**
     * Snapshot metadata file name.
     */
    public static final String JRAFT_SNAPSHOT_META_FILE = "__raft_snapshot_meta";
    /**
     * Snapshot file prefix.
     */
    public static final String JRAFT_SNAPSHOT_PREFIX = "snapshot_";
    /** Snapshot uri scheme for remote peer */
    public static final String REMOTE_SNAPSHOT_URI_SCHEME = "remote://";

    /**
     * Get the path of the Snapshot
     */
    public abstract String getPath();

    /**
     * List all the existing files in the Snapshot currently
     */
    public abstract Set<String> listFiles();

    /**
     * Get file meta by fileName.
     */
    public abstract Message getFileMeta(final String fileName);
}
