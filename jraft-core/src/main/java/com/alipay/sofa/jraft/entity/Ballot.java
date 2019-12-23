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

package com.alipay.sofa.jraft.entity;

import com.alipay.sofa.jraft.conf.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * A ballot to vote.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-15 2:29:11 PM
 */
public class Ballot {

    public static final class PosHint {
        int pos0 = -1; // position in current peers
        int pos1 = -1; // position in old peers
    }

    public static class UnfoundPeerId {
        PeerId peerId;
        boolean found;
        int index;

        public UnfoundPeerId(PeerId peerId, int index, boolean found) {
            super();
            this.peerId = peerId;
            this.index = index;
            this.found = found;
        }
    }

    private final List<UnfoundPeerId> peers = new ArrayList<>();
    private int quorum;
    private final List<UnfoundPeerId> oldPeers = new ArrayList<>();
    private int oldQuorum;

    /**
     * Init the ballot with current conf and old conf.
     *
     * @param conf current configuration
     * @param oldConf old configuration
     * @return true if init success
     */
    public boolean init(Configuration conf, Configuration oldConf) {
        this.peers.clear();
        this.oldPeers.clear();
        quorum = oldQuorum = 0;
        int index = 0;
        if (conf != null) {
            for (PeerId peer : conf) {
                this.peers.add(new UnfoundPeerId(peer, index++, false));
            }
        }

        this.quorum = this.peers.size() / 2 + 1;
        if (oldConf == null) {
            return true;
        }
        index = 0;
        for (PeerId peer : oldConf) {
            this.oldPeers.add(new UnfoundPeerId(peer, index++, false));
        }

        this.oldQuorum = this.oldPeers.size() / 2 + 1;
        return true;
    }

    private UnfoundPeerId findPeer(PeerId peerId, List<UnfoundPeerId> peers, int posHint) {
        if (posHint < 0 || posHint >= peers.size() || !peers.get(posHint).peerId.equals(peerId)) {
            for (UnfoundPeerId ufp : peers) {
                if (ufp.peerId.equals(peerId)) {
                    return ufp;
                }
            }
            return null;
        }

        return peers.get(posHint);
    }

    public PosHint grant(PeerId peerId, PosHint hint) {
        UnfoundPeerId peer = findPeer(peerId, peers, hint.pos0);
        if (peer != null) {
            if (!peer.found) {
                peer.found = true;
                this.quorum--;
            }
            hint.pos0 = peer.index;
        } else {
            hint.pos0 = -1;
        }
        if (oldPeers.isEmpty()) {
            hint.pos1 = -1;
            return hint;
        }
        peer = findPeer(peerId, oldPeers, hint.pos1);
        if (peer != null) {
            if (!peer.found) {
                peer.found = true;
                oldQuorum--;
            }
            hint.pos1 = peer.index;
        } else {
            hint.pos1 = -1;
        }

        return hint;
    }

    public void grant(PeerId peerId) {
        this.grant(peerId, new PosHint());
    }

    /**
     * Returns true when the ballot is granted.
     *
     * @return true if the ballot is granted
     */
    public boolean isGranted() {
        return this.quorum <= 0 && oldQuorum <= 0;
    }
}
