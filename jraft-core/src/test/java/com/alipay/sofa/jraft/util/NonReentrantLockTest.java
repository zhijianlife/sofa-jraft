package com.alipay.sofa.jraft.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;

public class NonReentrantLockTest {

    private static final int SIZE = 10000;

    private static int counter = SIZE;

    @Test
    public void test() throws Exception {
        List<Callable<Boolean>> tasks = new ArrayList<>();
        Lock lock = new NonReentrantLock();
        for (int i = 0; i < SIZE; i++) {
            tasks.add(() -> {
                lock.lock();
                try {
                    System.out.println("thread " + Thread.currentThread().getId() + ", count: " + counter);
                    counter--;
                    return true;
                } finally {
                    lock.unlock();
                }
            });
        }

        ExecutorService es = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2 + 1);
        final List<Future<Boolean>> futures = es.invokeAll(tasks);
        for (final Future<Boolean> future : futures) {
            future.get();
        }
        Assert.assertEquals(0, counter);
        counter = SIZE;
    }

    @Test
    public void test2() throws Exception {
        for (int i = 0; i < 100; i++) {
            test();
        }
    }

}