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

package com.alipay.sofa.jraft;

import com.alipay.sofa.jraft.core.CliServiceImpl;
import com.alipay.sofa.jraft.core.NodeImpl;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.option.NodeOptions;

/**
 * Service factory to create raft services, such as Node/CliService etc.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-May-03 11:06:02 AM
 */
public final class RaftServiceFactory {

    /**
     * Create a raft node with group id and it's serverId.
     *
     * 创建一个 Raft Node 对象
     */
    public static Node createRaftNode(String groupId, PeerId serverId) {
        return new NodeImpl(groupId, serverId);
    }

    /**
     * Create and initialize a raft node with node options.
     * Throw {@link IllegalStateException} when fail to initialize.
     *
     * 创建并初始化一个 Raft 节点
     */
    public static Node createAndInitRaftNode(String groupId, PeerId serverId, NodeOptions opts) {
        // 创建一个 Raft Node 对象
        final Node ret = createRaftNode(groupId, serverId);
        // 对 Raft Node 执行初始化
        if (!ret.init(opts)) {
            throw new IllegalStateException("Fail to init node, please see the logs to find the reason.");
        }
        return ret;
    }

    /**
     * Create a {@link CliService} instance.
     */
    public static CliService createCliService() {
        return new CliServiceImpl();
    }

    /**
     * Create and initialize a CliService instance.
     */
    public static CliService createAndInitCliService(CliOptions cliOptions) {
        final CliService ret = createCliService();
        if (!ret.init(cliOptions)) {
            throw new IllegalStateException("Fail to init CliService");
        }
        return ret;
    }
}
