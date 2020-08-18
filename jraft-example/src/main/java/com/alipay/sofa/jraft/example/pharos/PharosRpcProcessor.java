package com.alipay.sofa.jraft.example.pharos;

import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;

/**
 * @author zhenchao.wang 2020-07-30 14:50
 * @version 1.0.0
 */
public class PharosRpcProcessor implements RpcProcessor<String> {

    private final PharosNode pharosNode;

    public PharosRpcProcessor(PharosNode pharosNode) {
        this.pharosNode = pharosNode;
    }

    @Override
    public void handleRequest(RpcContext rpcCtx, String request) {
        final String[] items = request.split(":");
        final String type = items[0];
        if ("put".equalsIgnoreCase(type)) {
            final long userId = Long.parseLong(items[1]);
            final int idcCode = Integer.parseInt(items[2]);
            pharosNode.put(userId, idcCode);
            rpcCtx.sendResponse("handle request success, " +
                    "userId: " + userId + ", idcCode: " + pharosNode.getDB().get(userId));
        } else if ("get".equalsIgnoreCase(type)) {
            final long userId = Long.parseLong(items[1]);
            rpcCtx.sendResponse(
                    (pharosNode.isLeader() ? "L" : "F") + ", userId: " + userId + ", idcCode: " + pharosNode.getDB().get(userId));
        } else if ("leader".equalsIgnoreCase(type)) {
            final PeerId leaderId = pharosNode.getNode().getLeaderId();
            rpcCtx.sendResponse(leaderId.getIp() + ":" + leaderId.getPort());
        } else if ("granted".equalsIgnoreCase(type)) {
            boolean commit = Boolean.parseBoolean(items[1]);
            pharosNode.commit(commit);
            rpcCtx.sendResponse("commit: " + commit);
        } else {
            rpcCtx.sendResponse("invalid request: " + request);
        }
    }

    @Override
    public String interest() {
        return String.class.getName();
    }

}
