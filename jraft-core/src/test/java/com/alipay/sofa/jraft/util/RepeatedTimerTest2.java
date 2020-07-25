package com.alipay.sofa.jraft.util;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * @author zhenchao.wang 2020-07-23 16:18
 * @version 1.0.0
 */
public class RepeatedTimerTest2 {

    private static class TestRepeatedTimer extends RepeatedTimer {

        public TestRepeatedTimer(String name, int timeoutMs) {
            super(name, timeoutMs);
        }

        @Override
        protected void onTrigger() {
            System.out.println("on trigger");
        }

        @Override
        protected int adjustTimeout(int timeoutMs) {
            // 随机化计时周期
            return RandomUtils.nextInt(timeoutMs);
        }

    }

    @Test
    public void test() throws Exception {
        final TestRepeatedTimer timer = new TestRepeatedTimer("test", (int) TimeUnit.SECONDS.toMillis(1L));
        timer.start();

        Thread.sleep(10000);
    }
}