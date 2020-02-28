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

package com.alipay.sofa.jraft.storage.impl;

import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.conf.ConfigurationEntry;
import com.alipay.sofa.jraft.conf.ConfigurationManager;
import com.alipay.sofa.jraft.entity.EnumOutter.EntryType;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.entity.codec.LogEntryDecoder;
import com.alipay.sofa.jraft.entity.codec.LogEntryEncoder;
import com.alipay.sofa.jraft.option.LogStorageOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.LogStorage;
import com.alipay.sofa.jraft.util.Bits;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.StorageOptionsFactory;
import com.alipay.sofa.jraft.util.Utils;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.IndexType;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.StringAppendOperator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.rocksdb.util.SizeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Log storage based on rocksdb.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-06 7:27:47 AM
 */
public class RocksDBLogStorage implements LogStorage {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBLogStorage.class);

    static {
        RocksDB.loadLibrary();
    }

    /**
     * Write batch template.
     *
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2017-Nov-08 11:19:22 AM
     */
    private interface WriteBatchTemplate {

        void execute(WriteBatch batch) throws RocksDBException;
    }

    private final String path;
    private final boolean sync;
    private RocksDB db;
    private DBOptions dbOptions;
    private WriteOptions writeOptions;
    private final List<ColumnFamilyOptions> cfOptions = new ArrayList<>();
    /** 日志数据族 handle */
    private ColumnFamilyHandle defaultHandle;
    /** 配置信息族 handle */
    private ColumnFamilyHandle confHandle;
    private ReadOptions totalOrderReadOptions;
    private final ReadWriteLock lock = new ReentrantReadWriteLock(false);
    private final Lock readLock = this.lock.readLock();
    private final Lock writeLock = this.lock.writeLock();

    private volatile long firstLogIndex = 1;

    private volatile boolean hasLoadFirstLogIndex;

    private LogEntryEncoder logEntryEncoder;
    private LogEntryDecoder logEntryDecoder;

    public RocksDBLogStorage(final String path, final RaftOptions raftOptions) {
        super();
        this.path = path;
        this.sync = raftOptions.isSync();
    }

    private static BlockBasedTableConfig createTableConfig() {
        return new BlockBasedTableConfig(). //
                setIndexType(IndexType.kHashSearch). // use hash search(btree) for prefix scan.
                setBlockSize(4 * SizeUnit.KB).//
                setFilter(new BloomFilter(16, false)). //
                setCacheIndexAndFilterBlocks(true). //
                setBlockCacheSize(512 * SizeUnit.MB). //
                setCacheNumShardBits(8);
    }

    public static DBOptions createDBOptions() {
        return StorageOptionsFactory.getRocksDBOptions(RocksDBLogStorage.class);
    }

    public static ColumnFamilyOptions createColumnFamilyOptions() {
        final BlockBasedTableConfig tConfig = createTableConfig();
        final ColumnFamilyOptions options = StorageOptionsFactory
                .getRocksDBColumnFamilyOptions(RocksDBLogStorage.class);
        return options.useFixedLengthPrefixExtractor(8). //
                setTableFormatConfig(tConfig). //
                setMergeOperator(new StringAppendOperator());
    }

    @Override
    public boolean init(final LogStorageOptions opts) {
        Requires.requireNonNull(opts.getConfigurationManager(), "Null conf manager");
        Requires.requireNonNull(opts.getLogEntryCodecFactory(), "Null log entry codec factory");
        this.writeLock.lock();
        try {
            if (this.db != null) {
                LOG.warn("RocksDBLogStorage init() already.");
                return true;
            }
            this.logEntryDecoder = opts.getLogEntryCodecFactory().decoder();
            this.logEntryEncoder = opts.getLogEntryCodecFactory().encoder();
            Requires.requireNonNull(this.logEntryDecoder, "Null log entry decoder");
            Requires.requireNonNull(this.logEntryEncoder, "Null log entry encoder");
            this.dbOptions = createDBOptions();

            this.writeOptions = new WriteOptions();
            this.writeOptions.setSync(this.sync);
            this.totalOrderReadOptions = new ReadOptions();
            this.totalOrderReadOptions.setTotalOrderSeek(true);

            // 初始化 RocksDB，并记载 CONFIGURATION 类型的 LogEntry 和 firstLogIndex
            return this.initAndLoad(opts.getConfigurationManager());
        } catch (final RocksDBException e) {
            LOG.error("Fail to init RocksDBLogStorage, path={}", this.path, e);
            return false;
        } finally {
            this.writeLock.unlock();
        }

    }

    /**
     * 初始化日志数据存储
     *
     * @param confManager
     * @return
     * @throws RocksDBException
     */
    private boolean initAndLoad(final ConfigurationManager confManager) throws RocksDBException {
        this.hasLoadFirstLogIndex = false;
        this.firstLogIndex = 1; // 默认初始化为 1
        final List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        final ColumnFamilyOptions cfOption = createColumnFamilyOptions();
        this.cfOptions.add(cfOption);
        // Configuration 族用于存储配置信息
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor("Configuration".getBytes(), cfOption));
        // default 族用于存储日志数据
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOption));

        this.openDB(columnFamilyDescriptors);
        // 加载所有类型为 CONFIGURATION 的 LogEntry，以及 firstLogIndex
        this.load(confManager);
        return true;
    }

    /**
     * 加载所有类型为 CONFIGURATION 的 LogEntry，以及 firstLogIndex
     *
     * @param confManager
     */
    private void load(final ConfigurationManager confManager) {
        try (RocksIterator it = this.db.newIterator(this.confHandle, this.totalOrderReadOptions)) {
            // 指针移动到头部，从头开始遍历
            it.seekToFirst();
            while (it.isValid()) {
                final byte[] bs = it.value();
                final byte[] ks = it.key();

                // 长度为 8 的 key 对应的 value 是 LogEntry，key 就是 index
                if (ks.length == 8) {
                    final LogEntry entry = this.logEntryDecoder.decode(bs);
                    if (entry != null) {
                        // 发现类型为 CONFIGURATION 的 LogEntry
                        if (entry.getType() == EntryType.ENTRY_TYPE_CONFIGURATION) {
                            final ConfigurationEntry confEntry = new ConfigurationEntry();
                            confEntry.setId(new LogId(entry.getId().getIndex(), entry.getId().getTerm()));
                            confEntry.setConf(new Configuration(entry.getPeers()));
                            if (entry.getOldPeers() != null) {
                                confEntry.setOldConf(new Configuration(entry.getOldPeers()));
                            }
                            if (confManager != null) {
                                confManager.add(confEntry);
                            }
                        }
                        // 忽略其他类型的 LogEntry
                    } else {
                        LOG.warn("Fail to decode conf entry at index {}, the log data is: {}",
                                Bits.getLong(it.key(), 0), BytesUtil.toHex(bs));
                    }
                }
                // 非 LogEntry 项
                else {
                    // 发现 firstLogIndex
                    if (Arrays.equals(FIRST_LOG_IDX_KEY, ks)) {
                        // 设置 firstLogIndex
                        this.setFirstLogIndex(Bits.getLong(bs, 0));
                        // 截断 firstLogIndex 之前的数据
                        this.truncatePrefixInBackground(0L, this.firstLogIndex);
                    } else {
                        LOG.warn("Unknown entry in configuration storage key={}, value={}",
                                BytesUtil.toHex(ks), BytesUtil.toHex(bs));
                    }
                }
                it.next();
            }
        }
    }

    private void setFirstLogIndex(final long index) {
        this.firstLogIndex = index;
        this.hasLoadFirstLogIndex = true;
    }

    /**
     * First log inex and last log index key in configuration column family.
     */
    public static final byte[] FIRST_LOG_IDX_KEY = Utils.getBytes("meta/firstLogIndex");

    /**
     * Save the first log index into conf column family.
     */
    private boolean saveFirstLogIndex(final long firstLogIndex) {
        this.readLock.lock();
        try {
            final byte[] vs = new byte[8];
            Bits.putLong(vs, 0, firstLogIndex);
            this.db.put(this.confHandle, this.writeOptions, FIRST_LOG_IDX_KEY, vs);
            return true;
        } catch (final RocksDBException e) {
            LOG.error("Fail to save first log index {}", firstLogIndex, e);
            return false;
        } finally {
            this.readLock.unlock();
        }
    }

    private void openDB(final List<ColumnFamilyDescriptor> columnFamilyDescriptors) throws RocksDBException {
        final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

        final File dir = new File(this.path);
        if (dir.exists() && !dir.isDirectory()) {
            throw new IllegalStateException("Invalid log path, it's a regular file: " + this.path);
        }
        this.db = RocksDB.open(this.dbOptions, this.path, columnFamilyDescriptors, columnFamilyHandles);

        this.confHandle = columnFamilyHandles.get(0);
        this.defaultHandle = columnFamilyHandles.get(1);
    }

    /**
     * Execute write batch template.
     *
     * @param template write batch template
     */
    private boolean executeBatch(final WriteBatchTemplate template) {
        this.readLock.lock();
        try (final WriteBatch batch = new WriteBatch()) {
            template.execute(batch);
            this.db.write(this.writeOptions, batch);
        } catch (final RocksDBException e) {
            LOG.error("Execute rocksdb operation failed", e);
            return false;
        } finally {
            this.readLock.unlock();
        }
        return true;
    }

    @Override
    public void shutdown() {
        this.writeLock.lock();
        try {
            // The shutdown order is matter.
            // 1. close column family handles
            this.closeDB();
            // 2. close column family options.
            for (final ColumnFamilyOptions opt : this.cfOptions) {
                opt.close();
            }
            // 3. close options
            this.writeOptions.close();
            this.totalOrderReadOptions.close();
            // 4. help gc.
            this.cfOptions.clear();
            this.db = null;
            this.totalOrderReadOptions = null;
            this.writeOptions = null;
            this.defaultHandle = null;
            this.confHandle = null;
        } finally {
            this.writeLock.unlock();
        }
    }

    private void closeDB() {
        this.confHandle.close();
        this.defaultHandle.close();
        this.db.close();
    }

    @Override
    public long getFirstLogIndex() {
        this.readLock.lock();
        RocksIterator it = null;
        try {
            if (this.hasLoadFirstLogIndex) {
                return this.firstLogIndex;
            }
            it = this.db.newIterator(this.defaultHandle, this.totalOrderReadOptions);
            it.seekToFirst();
            if (it.isValid()) {
                final long ret = Bits.getLong(it.key(), 0);
                this.saveFirstLogIndex(ret);
                this.setFirstLogIndex(ret);
                return ret;
            }
            return 1L;
        } finally {
            if (it != null) {
                it.close();
            }
            this.readLock.unlock();
        }
    }

    @Override
    public long getLastLogIndex() {
        this.readLock.lock();
        try (final RocksIterator it = this.db.newIterator(this.defaultHandle, this.totalOrderReadOptions)) {
            // 移动到文件头部
            it.seekToLast();
            if (it.isValid()) {
                return Bits.getLong(it.key(), 0);
            }
            return 0L;
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public LogEntry getEntry(final long index) {
        this.readLock.lock();
        try {
            if (this.hasLoadFirstLogIndex && index < this.firstLogIndex) {
                return null;
            }
            final byte[] bs = this.db.get(this.defaultHandle, this.getKeyBytes(index));
            if (bs != null) {
                final LogEntry entry = this.logEntryDecoder.decode(bs);
                if (entry != null) {
                    return entry;
                } else {
                    LOG.error("Bad log entry format for index={}, the log data is: {}", index, BytesUtil.toHex(bs));
                    // invalid data remove? TODO
                    return null;
                }
            }
        } catch (final RocksDBException e) {
            LOG.error("Fail to get log entry at index {}", index, e);
            return null;
        } finally {
            this.readLock.unlock();
        }
        return null;
    }

    private byte[] getKeyBytes(final long index) {
        final byte[] ks = new byte[8];
        Bits.putLong(ks, 0, index);
        return ks;
    }

    @Override
    public long getTerm(final long index) {
        final LogEntry entry = this.getEntry(index);
        if (entry != null) {
            return entry.getId().getTerm();
        }
        return 0;
    }

    private void addConfBatch(final LogEntry entry, final WriteBatch batch) throws RocksDBException {
        final byte[] ks = this.getKeyBytes(entry.getId().getIndex());
        final byte[] content = this.logEntryEncoder.encode(entry);
        batch.put(this.defaultHandle, ks, content);
        batch.put(this.confHandle, ks, content);
    }

    private void addDataBatch(final LogEntry entry, final WriteBatch batch) throws RocksDBException {
        final byte[] ks = this.getKeyBytes(entry.getId().getIndex());
        final byte[] content = this.logEntryEncoder.encode(entry);
        batch.put(this.defaultHandle, ks, content);
    }

    @Override
    public boolean appendEntry(final LogEntry entry) {
        if (entry.getType() == EntryType.ENTRY_TYPE_CONFIGURATION) {
            return this.executeBatch(batch -> this.addConfBatch(entry, batch));

        } else {
            this.readLock.lock();
            try {
                this.db.put(this.defaultHandle, this.getKeyBytes(entry.getId().getIndex()),
                        this.logEntryEncoder.encode(entry));
                return true;
            } catch (final RocksDBException e) {
                LOG.error("Fail to append entry", e);
                return false;
            } finally {
                this.readLock.unlock();
            }
        }
    }

    @Override
    public int appendEntries(final List<LogEntry> entries) {
        if (entries == null || entries.isEmpty()) {
            return 0;
        }
        final int entriesCount = entries.size();
        final boolean ret = this.executeBatch(batch -> {
            for (int i = 0; i < entriesCount; i++) {
                final LogEntry entry = entries.get(i);
                if (entry.getType() == EntryType.ENTRY_TYPE_CONFIGURATION) {
                    this.addConfBatch(entry, batch);
                } else {
                    this.addDataBatch(entry, batch);
                }
            }
        });

        if (ret) {
            return entriesCount;
        } else {
            return 0;
        }

    }

    @Override
    public boolean truncatePrefix(final long firstIndexKept) {
        this.readLock.lock();
        try {
            final long startIndex = this.getFirstLogIndex();
            final boolean ret = this.saveFirstLogIndex(firstIndexKept);
            if (ret) {
                this.setFirstLogIndex(firstIndexKept);
            }
            this.truncatePrefixInBackground(startIndex, firstIndexKept);
            return ret;
        } finally {
            this.readLock.unlock();
        }

    }

    private void truncatePrefixInBackground(final long startIndex, final long firstIndexKept) {
        // delete logs in background.
        Utils.runInThread(() -> {
            this.readLock.lock();
            try {
                if (this.db == null) {
                    return;
                }
                this.db.deleteRange(this.defaultHandle, this.getKeyBytes(startIndex), this.getKeyBytes(firstIndexKept));
                this.db.deleteRange(this.confHandle, this.getKeyBytes(startIndex), this.getKeyBytes(firstIndexKept));
            } catch (final RocksDBException e) {
                LOG.error("Fail to truncatePrefix {}", firstIndexKept, e);
            } finally {
                this.readLock.unlock();
            }
        });
    }

    @Override
    public boolean truncateSuffix(final long lastIndexKept) {
        this.readLock.lock();
        try {
            this.db.deleteRange(this.defaultHandle, this.writeOptions, this.getKeyBytes(lastIndexKept + 1),
                    this.getKeyBytes(this.getLastLogIndex() + 1));
            this.db.deleteRange(this.confHandle, this.writeOptions, this.getKeyBytes(lastIndexKept + 1),
                    this.getKeyBytes(this.getLastLogIndex() + 1));
            return true;
        } catch (final RocksDBException e) {
            LOG.error("Fail to truncateSuffix {}", lastIndexKept, e);

        } finally {
            this.readLock.unlock();
        }
        return false;
    }

    @Override
    public boolean reset(final long nextLogIndex) {
        if (nextLogIndex <= 0) {
            throw new IllegalArgumentException("Invalid next log index.");
        }
        this.writeLock.lock();
        try (Options opt = new Options()) {
            LogEntry entry = this.getEntry(nextLogIndex);
            this.closeDB();
            try {
                RocksDB.destroyDB(this.path, opt);
                if (this.initAndLoad(null)) {
                    if (entry == null) {
                        entry = new LogEntry();
                        entry.setType(EntryType.ENTRY_TYPE_NO_OP);
                        entry.setId(new LogId(nextLogIndex, 0));
                        LOG.warn("Entry not found for nextLogIndex {} when reset", nextLogIndex);
                    }
                    return this.appendEntry(entry);
                } else {
                    return false;
                }
            } catch (final RocksDBException e) {
                LOG.error("Fail to reset next log index", e);
                return false;
            }
        } finally {
            this.writeLock.unlock();
        }
    }
}
