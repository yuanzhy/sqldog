package com.yuanzhy.sqldog.server.core;

import java.util.Map;

import com.yuanzhy.sqldog.core.exception.CodecException;

/**
 * @author yuanzhy
 * @date 2022/3/30
 */
public interface Codec {

    String encode(Map<String, Object> data) throws CodecException;

    Map<String, Object> decode(String data) throws CodecException;
}
