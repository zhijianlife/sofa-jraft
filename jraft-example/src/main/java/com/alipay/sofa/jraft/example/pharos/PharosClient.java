package com.alipay.sofa.jraft.example.pharos;

import com.alipay.remoting.exception.RemotingException;
import com.alipay.remoting.rpc.RpcClient;

/**
 * @author zhenchao.wang 2020-07-30 15:08
 * @version 1.0.0
 */
public class PharosClient {

    private static RpcClient rpcClient;

    public static void main(String[] args) throws Exception {
        rpcClient = new RpcClient();
        rpcClient.startup();
        try {
            put(124741485L, 312);
            get(124741485L);
        } finally {
            rpcClient.shutdown();
        }
    }

    private static void put(long userId, int idcCode) throws RemotingException, InterruptedException {
        System.out.println(rpcClient.invokeSync(getLeader(), "put:" + userId + ":" + idcCode, 1000));
    }

    private static void get(long userId) throws RemotingException, InterruptedException {
        for (int i = 0; i < 3; i++) {
            System.out.println(i + " -> " + rpcClient.invokeSync("127.0.0.1:808" + i, "get:" + userId, 1000));
        }
    }

    private static String getLeader() throws RemotingException, InterruptedException {
        final String leader = (String) rpcClient.invokeSync("127.0.0.1:8080", "leader", 1000);
        System.out.println("leader: " + leader);
        return leader;
    }

    private static void granted(boolean commit) throws RemotingException, InterruptedException {
        final String leader = getLeader();
        System.out.println(rpcClient.invokeSync(leader, "granted:" + commit, 1000));
    }

}
