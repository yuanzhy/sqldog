package com.yuanzhy.sqldog.server.util;

import com.yuanzhy.sqldog.server.common.StorageConst;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;

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
