package com.yuanzhy.sqldog.server.common.config;

import java.io.File;
import java.io.FileInputStream;
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
public class ServerConfig implements Config {

    private static final Logger log = LoggerFactory.getLogger(ServerConfig.class);
    /**
     *
     */
    protected final Properties props = new Properties();

    {
        String sqldogHome = System.getProperty("SQLDOG_HOME");
        if (StringUtils.isEmpty(sqldogHome)) {
            sqldogHome = new File(getJarPath()).getParent();
        }
        // 先从平级目录找sqldog.properties
        InputStream in = null;
        try {
            File configFile = new File(sqldogHome, "conf/sqldog.properties");
            if (configFile.exists()) {
                in = new FileInputStream(configFile);
            } else {
                log.info("SQLDOG_HOME/conf 目录下没有 sqldog.properties，默认读取jar包中的配置");
                in = Config.class.getClassLoader().getResourceAsStream("sqldog.properties");
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
