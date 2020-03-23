package com.alipay.sofa.jraft.rhea.serialization.impl.protostuff;

import com.alipay.sofa.jraft.rpc.RpcRequests;
import org.junit.Test;

/**
 * @author zhenchao.wang 2020-03-23 15:03
 * @version 1.0.0
 */
public class ProtoStuffSerializerTest {

    @Test
    public void test() {
        RpcRequests.AppendEntriesRequestHeader header =
                RpcRequests.AppendEntriesRequestHeader.newBuilder()
                        .setGroupId("test")
                        .setServerId("127.0.0.1:80")
                        .setPeerId("127.0.0.1:81")
                        .build();

        final ProtoStuffSerializer serializer = new ProtoStuffSerializer();
        final byte[] bytes = serializer.writeObject(header);
        final RpcRequests.AppendEntriesRequestHeader result = serializer.readObject(bytes, RpcRequests.AppendEntriesRequestHeader.class);
        System.out.println(result.toByteString().toStringUtf8());
    }
}