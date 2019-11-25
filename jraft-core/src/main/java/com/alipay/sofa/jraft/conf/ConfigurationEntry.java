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

package com.alipay.sofa.jraft.conf;

import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.entity.PeerId;

import java.util.HashSet;
import java.util.Set;

/**
 * A configuration entry with current peers and old peers.
 *
 * 记录整个集群的节点信息
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-04 2:25:06 PM
 */
public class ConfigurationEntry {

    private LogId id = new LogId(0, 0);
    private Configuration conf = new Configuration();
    private Configuration oldConf = new Configuration();

    public LogId getId() {
        return this.id;
    }

    public void setId(LogId id) {
        this.id = id;
    }

    public Configuration getConf() {
        return this.conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public Configuration getOldConf() {
        return this.oldConf;
    }

    public void setOldConf(Configuration oldConf) {
        this.oldConf = oldConf;
    }

    public ConfigurationEntry() {
        super();
    }

    public ConfigurationEntry(LogId id, Configuration conf, Configuration oldConf) {
        super();
        this.id = id;
        this.conf = conf;
        this.oldConf = oldConf;
    }

    /**
     * 当前未进行配置更替
     *
     * @return
     */
    public boolean isStable() {
        return this.oldConf.isEmpty();
    }

    public boolean isEmpty() {
        return this.conf.isEmpty();
    }

    public Set<PeerId> listPeers() {
        final Set<PeerId> ret = new HashSet<>(this.conf.listPeers());
        ret.addAll(this.oldConf.listPeers());
        return ret;
    }

    public boolean contains(PeerId peer) {
        return this.conf.contains(peer) || this.oldConf.contains(peer);
    }

    @Override
    public String toString() {
        return "ConfigurationEntry [id=" + this.id + ", conf=" + this.conf + ", oldConf=" + this.oldConf + "]";
    }
}
