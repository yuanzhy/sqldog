package com.yuanzhy.sqldog.core.codec;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/17
 */
public interface Codec<T> {
    /**
     * 编码
     * @param obj obj
     * @return
     */
    byte[] encode(T obj);

    /**
     * 解码
     * @param bytes bytes
     * @return
     */
    T decode(byte[] bytes);
}
