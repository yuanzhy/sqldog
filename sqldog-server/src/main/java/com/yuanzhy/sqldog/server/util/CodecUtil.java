package com.yuanzhy.sqldog.server.util;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yuanzhy.sqldog.server.common.StorageConst;

/**
 * @Author yuanzhy
 * @Date 2018/8/17
 */
public class CodecUtil {

    private static Logger log = LoggerFactory.getLogger(CodecUtil.class);

    public static String encode(String source) {
        try {
            return URLEncoder.encode(source, StorageConst.CHARSET);
        } catch (UnsupportedEncodingException e) {
            log.warn(e.getMessage(), e);
            return source;
        }
    }

    public static String decode(String source) {
        try {
            return URLDecoder.decode(source, StorageConst.CHARSET);
        } catch (UnsupportedEncodingException e) {
            log.warn(e.getMessage(), e);
            return source;
        }
    }
}
