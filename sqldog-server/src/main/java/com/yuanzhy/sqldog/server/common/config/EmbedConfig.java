package com.yuanzhy.sqldog.server.common.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
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
    private final Properties props = new Properties();

    public EmbedConfig(String filePath) {
        InputStream in = null;
        try {
            in = Config.class.getClassLoader().getResourceAsStream("META-INF/sqldog.properties");
            if (in == null) {
                log.info("META-INF/sqldog.properties not exists, load default");
                in = Config.class.getClassLoader().getResourceAsStream("sqldog.properties");
            }
            props.load(in);
        } catch (IOException e) {
            log.error("读取config配置文件失败", e);
        } finally {
            IOUtils.closeQuietly(in);
        }
        if (StringUtils.isEmpty(filePath)) {
            props.put("sqldog.storage.mode", "memory");
        } else {
            props.put("sqldog.storage.mode", "disk");
            props.putIfAbsent("sqldog.storage.writeCache", "true");
            props.put("sqldog.storage.path", filePath);
        }
        props.putIfAbsent("sqldog.storage.codec", "json");
        props.putIfAbsent("sqldog.storage.secret", "false");
        props.putIfAbsent("sqldog.codec", "serialize");
        log.info("Embedded configuration loaded");
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
