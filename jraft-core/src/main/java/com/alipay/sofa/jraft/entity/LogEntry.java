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

import com.alipay.sofa.jraft.entity.codec.LogEntryDecoder;
import com.alipay.sofa.jraft.entity.codec.LogEntryEncoder;
import com.alipay.sofa.jraft.entity.codec.v1.LogEntryV1CodecFactory;
import com.alipay.sofa.jraft.entity.codec.v1.V1Decoder;
import com.alipay.sofa.jraft.entity.codec.v1.V1Encoder;
import com.alipay.sofa.jraft.util.CrcUtil;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * A replica log entry.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-12 3:13:02 PM
 */
public class LogEntry implements Checksum {

    /** entry type */
    private EnumOutter.EntryType type;
    /** log id with index/term */
    private LogId id = new LogId(0, 0);
    /** log entry current peers */
    private List<PeerId> peers;
    /** log entry old peers */
    private List<PeerId> oldPeers;
    /** entry data */
    private ByteBuffer data;
    /** checksum for log entry */
    private long checksum;
    /** true when the log has checksum **/
    private boolean hasChecksum;

    public LogEntry() {
        super();
    }

    public LogEntry(final EnumOutter.EntryType type) {
        super();
        this.type = type;
    }

    @Override
    public long checksum() {
        long c = this.checksum(this.type.getNumber(), this.id.checksum());
        if (this.peers != null && !this.peers.isEmpty()) {
            for (PeerId peer : this.peers) {
                c = this.checksum(c, peer.checksum());
            }
        }
        if (this.oldPeers != null && !this.oldPeers.isEmpty()) {
            for (PeerId peer : this.oldPeers) {
                c = this.checksum(c, peer.checksum());
            }
        }
        if (this.data != null && this.data.hasRemaining()) {
            byte[] bs = new byte[this.data.remaining()];
            this.data.mark();
            this.data.get(bs);
            this.data.reset();
            c = this.checksum(c, CrcUtil.crc64(bs));
        }
        return c;
    }

    /**
     * Please use {@link LogEntryEncoder} instead.
     *
     * @return encoded byte array
     * @deprecated
     */
    @SuppressWarnings("DeprecatedIsStillUsed")
    @Deprecated
    public byte[] encode() {
        return V1Encoder.INSTANCE.encode(this);
    }

    /**
     * Please use {@link LogEntryDecoder} instead.
     *
     * @return whether success to decode
     * @deprecated
     */
    @SuppressWarnings("DeprecatedIsStillUsed")
    @Deprecated
    public boolean decode(final byte[] content) {
        if (content == null || content.length == 0) {
            return false;
        }
        if (content[0] != LogEntryV1CodecFactory.MAGIC) {
            // Corrupted log
            return false;
        }
        V1Decoder.INSTANCE.decode(this, content);
        return true;
    }

    /**
     * Returns whether the log entry has a checksum.
     *
     * @return true when the log entry has checksum, otherwise returns false.
     * @since 1.2.26
     */
    public boolean hasChecksum() {
        return this.hasChecksum;
    }

    /**
     * Returns true when the log entry is corrupted, it means that the checksum is mismatch.
     *
     * @return true when the log entry is corrupted, otherwise returns false
     * @since 1.2.6
     */
    public boolean isCorrupted() {
        return this.hasChecksum && this.checksum != this.checksum();
    }

    /**
     * Returns the checksum of the log entry. You should use {@link #hasChecksum} to check if
     * it has checksum.
     *
     * @return checksum value
     */
    public long getChecksum() {
        return this.checksum;
    }

    public void setChecksum(final long checksum) {
        this.checksum = checksum;
        this.hasChecksum = true;
    }

    public EnumOutter.EntryType getType() {
        return this.type;
    }

    public void setType(final EnumOutter.EntryType type) {
        this.type = type;
    }

    public LogId getId() {
        return this.id;
    }

    public void setId(final LogId id) {
        this.id = id;
    }

    public List<PeerId> getPeers() {
        return this.peers;
    }

    public void setPeers(final List<PeerId> peers) {
        this.peers = peers;
    }

    public List<PeerId> getOldPeers() {
        return this.oldPeers;
    }

    public void setOldPeers(final List<PeerId> oldPeers) {
        this.oldPeers = oldPeers;
    }

    public ByteBuffer getData() {
        return this.data;
    }

    public void setData(final ByteBuffer data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "LogEntry [type=" + this.type + ", id=" + this.id + ", peers=" + this.peers + ", oldPeers="
                + this.oldPeers + ", data=" + (this.data != null ? this.data.remaining() : 0) + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.data == null ? 0 : this.data.hashCode());
        result = prime * result + (this.id == null ? 0 : this.id.hashCode());
        result = prime * result + (this.oldPeers == null ? 0 : this.oldPeers.hashCode());
        result = prime * result + (this.peers == null ? 0 : this.peers.hashCode());
        result = prime * result + (this.type == null ? 0 : this.type.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        final LogEntry other = (LogEntry) obj;
        if (this.data == null) {
            if (other.data != null) {
                return false;
            }
        } else if (!this.data.equals(other.data)) {
            return false;
        }
        if (this.id == null) {
            if (other.id != null) {
                return false;
            }
        } else if (!this.id.equals(other.id)) {
            return false;
        }
        if (this.oldPeers == null) {
            if (other.oldPeers != null) {
                return false;
            }
        } else if (!this.oldPeers.equals(other.oldPeers)) {
            return false;
        }
        if (this.peers == null) {
            if (other.peers != null) {
                return false;
            }
        } else if (!this.peers.equals(other.peers)) {
            return false;
        }
        return this.type == other.type;
    }
}
