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

package com.alipay.sofa.jraft.rhea.client;

import com.alipay.remoting.InvokeCallback;
import com.alipay.remoting.InvokeContext;
import com.alipay.remoting.rpc.RpcClient;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rhea.client.failover.FailoverClosure;
import com.alipay.sofa.jraft.rhea.client.pd.AbstractPlacementDriverClient;
import com.alipay.sofa.jraft.rhea.client.pd.PlacementDriverClient;
import com.alipay.sofa.jraft.rhea.cmd.store.BaseRequest;
import com.alipay.sofa.jraft.rhea.cmd.store.BaseResponse;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.errors.ErrorsHelper;
import com.alipay.sofa.jraft.rhea.options.RpcOptions;
import com.alipay.sofa.jraft.rhea.rpc.ExtSerializerSupports;
import com.alipay.sofa.jraft.rhea.util.concurrent.CallerRunsPolicyWithReport;
import com.alipay.sofa.jraft.rhea.util.concurrent.NamedThreadFactory;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.ExecutorServiceHelper;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.ThreadPoolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author jiachun.fjc
 */
public class DefaultRheaKVRpcService implements RheaKVRpcService {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultRheaKVRpcService.class);

    private final PlacementDriverClient pdClient;
    private final RpcClient rpcClient;
    private final Endpoint selfEndpoint;

    private ThreadPoolExecutor rpcCallbackExecutor;
    private int rpcTimeoutMillis;

    private boolean started;

    public DefaultRheaKVRpcService(PlacementDriverClient pdClient, Endpoint selfEndpoint) {
        this.pdClient = pdClient;
        this.rpcClient = ((AbstractPlacementDriverClient) pdClient).getRpcClient();
        this.selfEndpoint = selfEndpoint;
    }

    @Override
    public synchronized boolean init(final RpcOptions opts) {
        if (this.started) {
            LOG.info("[DefaultRheaKVRpcService] already started.");
            return true;
        }
        this.rpcCallbackExecutor = createRpcCallbackExecutor(opts);
        this.rpcTimeoutMillis = opts.getRpcTimeoutMillis();
        Requires.requireTrue(this.rpcTimeoutMillis > 0, "opts.rpcTimeoutMillis must > 0");
        LOG.info("[DefaultRheaKVRpcService] start successfully, options: {}.", opts);
        return this.started = true;
    }

    @Override
    public synchronized void shutdown() {
        ExecutorServiceHelper.shutdownAndAwaitTermination(this.rpcCallbackExecutor);
        this.started = false;
        LOG.info("[DefaultRheaKVRpcService] shutdown successfully.");
    }

    @Override
    public <V> CompletableFuture<V> callAsyncWithRpc(
            final BaseRequest request, final FailoverClosure<V> closure, final Errors lastCause) {
        return callAsyncWithRpc(request, closure, lastCause, true);
    }

    @Override
    public <V> CompletableFuture<V> callAsyncWithRpc(
            final BaseRequest request, final FailoverClosure<V> closure, final Errors lastCause, final boolean requireLeader) {
        final boolean forceRefresh = ErrorsHelper.isInvalidPeer(lastCause);
        // 获取目标请求节点
        final Endpoint endpoint = getRpcEndpoint(request.getRegionId(), forceRefresh, this.rpcTimeoutMillis, requireLeader);
        // 发送 RPC 请求
        internalCallAsyncWithRpc(endpoint, request, closure);
        return closure.future();
    }

    public Endpoint getLeader(final long regionId, final boolean forceRefresh, final long timeoutMillis) {
        return this.pdClient.getLeader(regionId, forceRefresh, timeoutMillis);
    }

    public Endpoint getLuckyPeer(final long regionId, final boolean forceRefresh, final long timeoutMillis) {
        return this.pdClient.getLuckyPeer(regionId, forceRefresh, timeoutMillis, this.selfEndpoint);
    }

    public Endpoint getRpcEndpoint(
            final long regionId, final boolean forceRefresh, final long timeoutMillis, final boolean requireLeader) {
        if (requireLeader) {
            // 获取 Leader 节点
            return getLeader(regionId, forceRefresh, timeoutMillis);
        } else {
            // 轮询节点
            return getLuckyPeer(regionId, forceRefresh, timeoutMillis);
        }
    }

    private <V> void internalCallAsyncWithRpc(final Endpoint endpoint, final BaseRequest request,
                                              final FailoverClosure<V> closure) {
        final String address = endpoint.toString();
        final InvokeContext invokeCtx = ExtSerializerSupports.getInvokeContext();
        final InvokeCallback invokeCallback = new InvokeCallback() {

            @Override
            public void onResponse(final Object result) {
                final BaseResponse<?> response = (BaseResponse<?>) result;
                if (response.isSuccess()) {
                    closure.setData(response.getValue());
                    closure.run(Status.OK());
                } else {
                    closure.setError(response.getError());
                    closure.run(new Status(-1, "RPC failed with address: %s, response: %s", address, response));
                }
            }

            @Override
            public void onException(final Throwable t) {
                closure.failure(t);
            }

            @Override
            public Executor getExecutor() {
                return rpcCallbackExecutor;
            }
        };
        try {
            this.rpcClient.invokeWithCallback(address, request, invokeCtx, invokeCallback, this.rpcTimeoutMillis);
        } catch (final Throwable t) {
            closure.failure(t);
        }
    }

    private ThreadPoolExecutor createRpcCallbackExecutor(final RpcOptions opts) {
        final int callbackExecutorCorePoolSize = opts.getCallbackExecutorCorePoolSize();
        final int callbackExecutorMaximumPoolSize = opts.getCallbackExecutorMaximumPoolSize();
        if (callbackExecutorCorePoolSize <= 0 || callbackExecutorMaximumPoolSize <= 0) {
            return null;
        }

        final String name = "rheakv-rpc-callback";
        return ThreadPoolUtil.newBuilder() //
                .poolName(name) //
                .enableMetric(true) //
                .coreThreads(callbackExecutorCorePoolSize) //
                .maximumThreads(callbackExecutorMaximumPoolSize) //
                .keepAliveSeconds(120L) //
                .workQueue(new ArrayBlockingQueue<>(opts.getCallbackExecutorQueueCapacity())) //
                .threadFactory(new NamedThreadFactory(name, true)) //
                .rejectedHandler(new CallerRunsPolicyWithReport(name)) //
                .build();
    }
}
