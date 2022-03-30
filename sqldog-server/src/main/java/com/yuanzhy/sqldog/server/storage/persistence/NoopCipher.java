package com.yuanzhy.sqldog.server.storage.persistence;

import com.yuanzhy.sqldog.server.core.Cipher;

/**
 * @author yuanzhy
 * @date 2022/3/30
 */
public class NoopCipher implements Cipher {
    @Override
    public String encrypt(String data) {
        return data;
    }

    @Override
    public String decrypt(String data) {
        return data;
    }
}
