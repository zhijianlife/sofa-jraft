package com.alipay.sofa.jraft.util;

import org.junit.Test;

/**
 * @author zhenchao.wang 2020-01-07 15:31
 * @version 1.0.0
 */
public class ThreadHelperTest {

    @Test
    public void onSpinWait() {
        for (int i = 0; i < 10; i++) {
            System.out.println(i);
            ThreadHelper.onSpinWait();
        }
    }
}