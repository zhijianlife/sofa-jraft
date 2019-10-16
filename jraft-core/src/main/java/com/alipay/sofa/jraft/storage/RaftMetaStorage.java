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
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.RaftMetaStorageOptions;

/**
 * Raft metadata storage service.
 *
 * 元信息存储，用来存储记录 Raft 实现的内部状态，譬如当前任期 Term、投票给哪个 PeerId 节点等信息。
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-12 3:54:21 PM
 */
public interface RaftMetaStorage extends Lifecycle<RaftMetaStorageOptions>, Storage {

    /**
     * Set current term.
     *
     * 设置元数据的当前任期
     *
     * Raft 内部状态任期 Term 是在整个 Raft Group 里单调递增的 long 数字，用来表示一轮投票的编号，
     * 其中成功选举出来的 Leader 对应的 Term 称为 Leader Term，Leader 没有发生变更期间提交的日志都有相同的 Term 编号。
     */
    boolean setTerm(long term);

    /**
     * Get current term.
     *
     * 获取元数据的当前任期
     */
    long getTerm();

    /**
     * Set voted for information.
     *
     * 设置元信息的 PeerId 节点投票
     */
    boolean setVotedFor(PeerId peerId);

    /**
     * Get voted for information.
     *
     * 获取元信息的 PeerId 节点投票
     */
    PeerId getVotedFor();

    /**
     * Set term and voted for information.
     */
    boolean setTermAndVotedFor(long term, PeerId peerId);
}
