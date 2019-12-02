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
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.option.LogStorageOptions;

import java.util.List;

/**
 * Log entry storage service.
 *
 * 记录 Raft 配置变更和用户提交任务的日志，把日志从 Leader 复制到其他节点上面。
 *
 * Log Index 提交到 Raft Group 中的任务序列化为日志存储，每条日志一个编号，在整个 Raft Group 内单调递增并复制到每个 Raft 节点。
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-12 3:43:54 PM
 */
public interface LogStorage extends Lifecycle<LogStorageOptions>, Storage {

    /**
     * Returns first log index in log.
     *
     * 返回日志中的首个索引
     */
    long getFirstLogIndex();

    /**
     * Returns last log index in log.
     *
     * 返回日志中的最后一个索引
     */
    long getLastLogIndex();

    /**
     * Get logEntry by index.
     *
     * 按照日志索引获取 LogEntry
     */
    LogEntry getEntry(final long index);

    /**
     * Get logEntry's term by index.
     * This method is deprecated, you should use {@link #getEntry(long)} to get the log id's term.
     *
     * @deprecated
     */
    @Deprecated
    long getTerm(final long index);

    /**
     * Append entries to log.
     *
     * 追加单个 LogEntry 到日志中
     */
    boolean appendEntry(final LogEntry entry);

    /**
     * Append entries to log, return append success number.
     *
     * 追加批量 LogEntry 到日志中
     */
    int appendEntries(final List<LogEntry> entries);

    /**
     * Delete logs from storage's head, [first_log_index, first_index_kept) will be discarded.
     *
     * 从日志头部截断日志
     */
    boolean truncatePrefix(final long firstIndexKept);

    /**
     * Delete uncommitted logs from storage's tail, (last_index_kept, last_log_index] will be discarded.
     *
     * 从日志尾部截断日志
     */
    boolean truncateSuffix(final long lastIndexKept);

    /**
     * Drop all the existing logs and reset next log index to |next_log_index|.
     * This function is called after installing snapshot from leader.
     *
     * 删除所有日志，重置日志索引
     */
    boolean reset(final long nextLogIndex);
}
