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

package com.alipay.sofa.jraft.example.counter.rpc;

import com.alipay.remoting.AsyncContext;
import com.alipay.remoting.BizContext;
import com.alipay.remoting.exception.CodecException;
import com.alipay.remoting.rpc.protocol.AsyncUserProcessor;
import com.alipay.remoting.serialization.SerializerManager;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.example.counter.CounterServer;
import com.alipay.sofa.jraft.example.counter.IncrementAndAddClosure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * IncrementAndGetRequest processor.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-09 5:43:57 PM
 */
public class IncrementAndGetRequestProcessor extends AsyncUserProcessor<IncrementAndGetRequest> {

    private static final Logger LOG = LoggerFactory.getLogger(IncrementAndGetRequestProcessor.class);

    private final CounterServer counterServer;

    public IncrementAndGetRequestProcessor(CounterServer counterServer) {
        super();
        this.counterServer = counterServer;
    }

    @Override
    public void handleRequest(final BizContext bizCtx, final AsyncContext asyncCtx, final IncrementAndGetRequest request) {
        // 如果不是 leader 节点，则将请求转交给 leader
        if (!this.counterServer.getFsm().isLeader()) {
            asyncCtx.sendResponse(this.counterServer.redirect());
            return;
        }

        // 构造响应对象
        final ValueResponse response = new ValueResponse();
        final IncrementAndAddClosure closure = new IncrementAndAddClosure(
                counterServer,
                request,
                response,
                status -> {
                    // 响应失败
                    if (!status.isOk()) {
                        response.setErrorMsg(status.getErrorMsg());
                        response.setSuccess(false);
                    }
                    asyncCtx.sendResponse(response);
                });

        try {
            final Task task = new Task();
            task.setDone(closure);
            // 序列化请求
            task.setData(ByteBuffer.wrap(SerializerManager.getSerializer(SerializerManager.Hessian2).serialize(request)));

            // 处理请求
            counterServer.getNode().apply(task);
        } catch (final CodecException e) {
            LOG.error("Fail to encode IncrementAndGetRequest", e);
            response.setSuccess(false);
            response.setErrorMsg(e.getMessage());
            asyncCtx.sendResponse(response);
        }
    }

    @Override
    public String interest() {
        return IncrementAndGetRequest.class.getName();
    }
}
