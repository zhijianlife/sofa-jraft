package com.alipay.sofa.jraft.example.pharos;

import com.alipay.sofa.jraft.entity.PeerId;

public class PharosServer1 {

    public static void main(final String[] args) {

        final String dataPath = "/home/zhenchao/workspace/data/sofa/jraft/s1";
        final String groupId = "pharos";
        final String serverIdStr = "127.0.0.1:8080";
        final String initialConfStr = "127.0.0.1:8080,127.0.0.1:8081,127.0.0.1:8082";

        final PharosNodeOptions options = new PharosNodeOptions();
        options.setDataPath(dataPath);
        options.setGroupId(groupId);
        options.setServerAddress(serverIdStr);
        options.setInitialServerAddressList(initialConfStr);

        PharosNode pharosNode = new PharosNode();
        pharosNode.addLeaderStateListener(new LeaderStateListener() {

            final PeerId serverId = PeerId.parsePeer(serverIdStr);
            final String ip = serverId.getIp();
            final int port = serverId.getPort();

            @Override
            public void onLeaderStart(long leaderTerm) {
                System.out.println("[Pharos Server 1] Leader's ip is: " + ip + ", port: " + port);
                System.out.println("[Pharos Server 1] Leader start on term: " + leaderTerm);
            }

            @Override
            public void onLeaderStop(long leaderTerm) {
                System.out.println("[Pharos Server 1] Leader stop on term: " + leaderTerm);
            }
        });
        pharosNode.init(options);
    }
}
