package com.yuanzhy.sqldog.server.core;

/**
 * @author yuanzhy
 * @date 2022/3/30
 */
public interface Cipher {

    String encrypt(String data);

    String decrypt(String data);
}
