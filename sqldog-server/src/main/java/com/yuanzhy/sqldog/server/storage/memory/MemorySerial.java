package com.yuanzhy.sqldog.server.storage.memory;

import com.yuanzhy.sqldog.server.core.Serial;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/24
 */
public class MemorySerial implements Serial {

    private final int step;
    private final AtomicLong value;

    public MemorySerial(long initValue) {
        this(initValue, 1);
    }

    public MemorySerial(long initValue, int step) {
        this.value = new AtomicLong(initValue);
        this.step = step;
    }

    @Override
    public /*synchronized*/ long next() {
        return value.addAndGet(step);
    }
}
