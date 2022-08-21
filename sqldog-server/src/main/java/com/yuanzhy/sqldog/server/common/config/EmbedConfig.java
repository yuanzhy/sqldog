package com.yuanzhy.sqldog.server.common.config;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yuanzhy.sqldog.core.util.StringUtils;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/8/20
 */
public class EmbedConfig implements Config {

    private static final Logger log = LoggerFactory.getLogger(EmbedConfig.class);
    /**
     *
     */
    private Properties props = new Properties();

    public EmbedConfig(String filePath) {
        if (StringUtils.isEmpty(filePath)) {
            props.put("server.storage.mode", "memory");
        } else {
            props.put("server.storage.mode", "disk");
            props.put("server.storage.writeCache", "true");
            props.put("server.storage.path", filePath);
        }
        props.put("server.storage.codec", "json");
        props.put("server.storage.secret", "false");
        props.put("server.codec", "serialize");
        log.info("加载嵌入式配置");
    }

    @Override
    public String getProperty(String key) {
        return props.getProperty(key);
    }

    @Override
    public boolean isDisk() {
        return Config.super.isDisk();
    }
}
