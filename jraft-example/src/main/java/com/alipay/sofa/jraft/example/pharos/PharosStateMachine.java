package com.alipay.sofa.jraft.example.pharos;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class PharosStateMachine extends StateMachineAdapter {

    private static final Logger log = LoggerFactory.getLogger(PharosStateMachine.class);

    private volatile CountDownLatch latch = new CountDownLatch(1);
    private volatile boolean commit = false;

    private final ConcurrentMap<Long, Integer> db;

    private final AtomicLong leaderTerm = new AtomicLong(-1L);
    private final List<LeaderStateListener> listeners;

    public PharosStateMachine(ConcurrentMap<Long, Integer> db, List<LeaderStateListener> listeners) {
        this.db = db;
        this.listeners = listeners;
    }

    @Override
    public void onApply(Iterator iter) {
        while (iter.hasNext()) {
            final long term = iter.getTerm();
            final long index = iter.getIndex();
            final Closure done = iter.done();
            log.info("Apply task, term: {}, index: {}", term, index);

            final ByteBuffer data = iter.getData();
            if (null == data) {
                log.warn("Null task data, and ignore it, term: {}, index: {}", term, index);
                if (null != done) {
                    done.run(new Status(-1, "null data"));
                }
            } else {
                try {
                    final long userId = data.getLong();
                    final int idcCode = data.getInt();
                    log.info("Store data to local db, userId: {}, idcCode: {}", userId, idcCode);
                    db.put(userId, idcCode);
                    if (null != done) {
                        done.run(Status.OK());
                    }
                } catch (Throwable t) {
                    log.error("Store data error, term: {}, index: {}", term, index, t);
                    if (null != done) {
                        done.run(new Status(-1, "store data error"));
                    }
                }
            }
            iter.next();
        }
    }

    @Override
    public void onLeaderStart(final long term) {
        super.onLeaderStart(term);
        this.leaderTerm.set(term);
        for (final LeaderStateListener listener : this.listeners) { // iterator the snapshot
            listener.onLeaderStart(term);
        }
    }

    @Override
    public void onLeaderStop(final Status status) {
        super.onLeaderStop(status);
        final long oldTerm = leaderTerm.get();
        this.leaderTerm.set(-1L);
        for (final LeaderStateListener listener : this.listeners) { // iterator the snapshot
            listener.onLeaderStop(oldTerm);
        }
    }

    public void commit(boolean commit) {
        this.commit = commit;
        latch.countDown();
    }

    public boolean isLeader() {
        return this.leaderTerm.get() > 0;
    }

    public void addLeaderStateListener(final LeaderStateListener listener) {
        this.listeners.add(listener);
    }

}
