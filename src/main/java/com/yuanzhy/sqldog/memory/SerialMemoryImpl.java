package com.yuanzhy.sqldog.memory;

import com.yuanzhy.sqldog.core.Serial;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/24
 */
public class SerialMemoryImpl implements Serial {

    private final int step;
    private final AtomicLong value;

    public SerialMemoryImpl(long initValue) {
        this(initValue, 1);
    }

    public SerialMemoryImpl(long initValue, int step) {
        this.value = new AtomicLong(initValue);
        this.step = step;
    }

    @Override
    public /*synchronized*/ long next() {
        return value.addAndGet(step);
    }
}
