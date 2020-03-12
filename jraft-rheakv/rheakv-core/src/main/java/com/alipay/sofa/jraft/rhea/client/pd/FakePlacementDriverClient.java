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

package com.alipay.sofa.jraft.rhea.client.pd;

import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.metadata.Store;
import com.alipay.sofa.jraft.rhea.options.PlacementDriverOptions;
import com.alipay.sofa.jraft.rhea.options.RegionEngineOptions;
import com.alipay.sofa.jraft.rhea.options.StoreEngineOptions;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.util.Endpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Single raft group, no need for a real PD role.
 *
 * @author jiachun.fjc
 */
public class FakePlacementDriverClient extends AbstractPlacementDriverClient {

    private static final Logger LOG = LoggerFactory.getLogger(FakePlacementDriverClient.class);

    private boolean started;

    public FakePlacementDriverClient(long clusterId, String clusterName) {
        super(clusterId, clusterName);
    }

    @Override
    public synchronized boolean init(final PlacementDriverOptions opts) {
        if (this.started) {
            LOG.info("[FakePlacementDriverClient] already started.");
            return true;
        }
        super.init(opts);
        LOG.info("[FakePlacementDriverClient] start successfully, options: {}.", opts);
        return this.started = true;
    }

    @Override
    public synchronized void shutdown() {
        super.shutdown();
        LOG.info("[FakePlacementDriverClient] shutdown successfully.");
    }

    @Override
    protected void refreshRouteTable() {
        // NO-OP
    }

    @Override
    public Store getStoreMetadata(final StoreEngineOptions opts) {
        // 实例化一个 Store 对象
        final Store store = new Store();
        final List<RegionEngineOptions> rOptsList = opts.getRegionEngineOptionsList();
        final List<Region> regionList = Lists.newArrayListWithCapacity(rOptsList.size());
        store.setId(-1);
        store.setEndpoint(opts.getServerAddress());
        // 遍历为当前 Store 创建一系列的 Region
        for (final RegionEngineOptions rOpts : rOptsList) {
            regionList.add(this.getLocalRegionMetadata(rOpts));
        }
        // 一个 Store 下面有多个 Region
        store.setRegions(regionList);
        return store;
    }

    @Override
    public Endpoint getPdLeader(final boolean forceRefresh, final long timeoutMillis) {
        throw new UnsupportedOperationException();
    }
}
