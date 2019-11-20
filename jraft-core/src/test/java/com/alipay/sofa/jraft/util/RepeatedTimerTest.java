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

package com.alipay.sofa.jraft.util;

import org.junit.After;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class RepeatedTimerTest {

    private static class TestTimer extends RepeatedTimer {
        AtomicInteger counter = new AtomicInteger(0);
        AtomicInteger destroyed = new AtomicInteger(0);
        volatile int nextTimeout = -1;

        public TestTimer(String name, int timeoutMs) {
            super(name, timeoutMs);
        }

        @Override
        protected int adjustTimeout(int timeoutMs) {
            if (nextTimeout > 0) {
                return nextTimeout;
            } else {
                return timeoutMs;
            }
        }

        @Override
        protected void onDestroy() {
            destroyed.incrementAndGet();
        }

        @Override
        protected void onTrigger() {
            counter.incrementAndGet();
        }

    }

    private TestTimer timer;

    @Before
    public void setup() {
        this.timer = new TestTimer("test", 50);
    }

    @After
    public void teardown() {
        this.timer.destroy();
    }

    @Test
    public void testStartTrigger() throws Exception {
        assertEquals(0, timer.counter.get());
        this.timer.start();
        Thread.sleep(1000);
        assertEquals(20, timer.counter.get(), 3);
    }

    @Test
    public void testStopStart() throws Exception {
        assertEquals(0, timer.counter.get());
        this.timer.start();
        Thread.sleep(1000);
        assertEquals(20, timer.counter.get(), 5);
        this.timer.stop();
        Thread.sleep(1000);
        assertEquals(20, timer.counter.get(), 5);
        this.timer.start();
        Thread.sleep(1000);
        assertEquals(40, timer.counter.get(), 5);
    }

    @Test
    public void testRunOnce() throws Exception {
        assertEquals(0, timer.counter.get());
        this.timer.start();
        this.timer.runOnceNow();
        assertEquals(1, timer.counter.get());
        Thread.sleep(1000);
        assertEquals(20, timer.counter.get(), 3);
    }

    @Test
    public void testDestroy() throws Exception {
        this.timer.start();
        assertEquals(0, this.timer.destroyed.get());
        Thread.sleep(100);
        this.timer.destroy();
        assertEquals(1, this.timer.destroyed.get());
    }

    @Test
    public void testAdjustTimeout() throws Exception {
        this.timer.nextTimeout = 100;
        this.timer.start();
        Thread.sleep(1000);
        assertEquals(10, timer.counter.get(), 3);
    }

    @Test
    public void testReset() throws Exception {
        this.timer.start();
        assertEquals(50, timer.getTimeoutMs());
        for (int i = 0; i < 10; i++) {
            Thread.sleep(80);
            this.timer.reset();
        }
        assertEquals(10, timer.counter.get(), 3);
        this.timer.reset(100);
        for (int i = 0; i < 10; i++) {
            Thread.sleep(80);
            this.timer.reset();
        }
        assertEquals(10, timer.counter.get(), 3);
    }
}
