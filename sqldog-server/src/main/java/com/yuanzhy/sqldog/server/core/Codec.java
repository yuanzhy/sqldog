package com.yuanzhy.sqldog.server.core;

import com.yuanzhy.sqldog.core.exception.CodecException;

import java.util.Map;

/**
 * @author yuanzhy
 * @date 2022/3/30
 */
public interface Codec {

    String encode(Map<String, Object> data) throws CodecException;

    Map<String, Object> decode(String data) throws CodecException;
}
