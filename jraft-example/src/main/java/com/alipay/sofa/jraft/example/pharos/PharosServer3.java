package com.alipay.sofa.jraft.example.pharos;

import com.alipay.sofa.jraft.entity.PeerId;

public class PharosServer3 {

    public static void main(final String[] args) {

        final String dataPath = "/home/zhenchao/workspace/data/sofa/jraft/s3";
        final String groupId = "pharos";
        final String serverIdStr = "127.0.0.1:8082";
        final String initialConfStr = "127.0.0.1:8080,127.0.0.1:8081,127.0.0.1:8082";

        final PharosNodeOptions options = new PharosNodeOptions();
        options.setDataPath(dataPath);
        options.setGroupId(groupId);
        options.setServerAddress(serverIdStr);
        options.setInitialServerAddressList(initialConfStr);

        PharosNode node = new PharosNode();
        node.addLeaderStateListener(new LeaderStateListener() {

            final PeerId serverId = PeerId.parsePeer(serverIdStr);
            final String ip = serverId.getIp();
            final int port = serverId.getPort();

            @Override
            public void onLeaderStart(long leaderTerm) {
                System.out.println("[Pharos Server 3] Leader's ip is: " + ip + ", port: " + port);
                System.out.println("[Pharos Server 3] Leader start on term: " + leaderTerm);
            }

            @Override
            public void onLeaderStop(long leaderTerm) {
                System.out.println("[Pharos Server 3] Leader stop on term: " + leaderTerm);
            }
        });
        node.init(options);
    }
}
