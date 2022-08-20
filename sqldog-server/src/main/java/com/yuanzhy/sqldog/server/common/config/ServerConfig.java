package com.yuanzhy.sqldog.server.common.config;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/8/20
 */
public class ServerConfig implements Config {

    private static final Logger log = LoggerFactory.getLogger(ServerConfig.class);
    /**
     *
     */
    private Properties props = new Properties();

    {
        // 先从平级目录找config.properties
        InputStream in = null;
        try {
            File configFile = new File(getJarPath().concat("/config.properties"));
            if (configFile.exists()) {
                log.info("Sqldog.jar同级目录下找到config.properties，读取此配置");
                in = new FileInputStream(configFile);
            } else {
                log.info("Sqldog.jar同级目录下没有config.properties，默认读取jar包中的配置");
                in = Config.class.getClassLoader().getResourceAsStream("config.properties");
            }
            props.load(in);
        } catch (IOException e) {
            log.error("读取config配置文件失败", e);
        } finally {
            IOUtils.closeQuietly(in);
        }
    }

    @Override
    public String getProperty(String key) {
        return props.getProperty(key);
    }
}
