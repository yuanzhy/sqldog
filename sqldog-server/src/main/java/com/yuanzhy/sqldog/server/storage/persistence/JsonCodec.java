package com.yuanzhy.sqldog.server.storage.persistence;

import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.yuanzhy.sqldog.core.exception.CodecException;
import com.yuanzhy.sqldog.server.core.Cipher;
import com.yuanzhy.sqldog.server.core.Codec;

/**
 * @author yuanzhy
 * @date 2022/3/30
 */
public class JsonCodec implements Codec {

    private final Cipher cipher;
    public JsonCodec(Cipher cipher) {
        this.cipher = cipher;
    }
    @Override
    public String encode(Map<String, Object> data) throws CodecException {
        return cipher.encrypt(JSON.toJSONString(data));
    }

    @Override
    public Map<String, Object> decode(String data) throws CodecException {
        return JSON.parseObject(cipher.decrypt(data));
    }
}
