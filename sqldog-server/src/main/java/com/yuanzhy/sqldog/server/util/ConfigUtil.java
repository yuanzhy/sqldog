package com.yuanzhy.sqldog.server.util;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author yuanzhy
 * @date 2018/6/13
 */
public class ConfigUtil {

    private static Logger log = LoggerFactory.getLogger(ConfigUtil.class);
    /**
     *
     */
    private static Properties props = new Properties();

    private static final String DATA_PATH;

    static {
        // 先从平级目录找config.properties
        InputStream in = null;
        try {
            File configFile = new File(getJarPath().concat("/config.properties"));
            if (configFile.exists()) {
                log.info("Sqldog.jar同级目录下找到config.properties，读取此配置");
                in = new FileInputStream(configFile);
            } else {
                log.info("Sqldog.jar同级目录下没有config.properties，默认读取jar包中的配置");
                in = ConfigUtil.class.getClassLoader().getResourceAsStream("config.properties");
            }
            props.load(in);
        } catch (IOException e) {
            log.error("读取config配置文件失败", e);
        } finally {
            IOUtils.closeQuietly(in);
        }
        // dataPath
        String dataPath = getProperty("server.storage.path", "data");
        if (!dataPath.startsWith("/")) {
            dataPath = new File(getJarPath()).getParent() + "/" + dataPath;
            new File(dataPath).mkdirs();
        }
        DATA_PATH = dataPath;
    }

    public static String getProperty(String key) {
        return props.getProperty(key);
    }

    public static String getProperty(String key, String defaultValue) {
        String value = props.getProperty(key);
        return StringUtils.isEmpty(value) ? defaultValue : value;
    }

    public static int getIntProperty(String key) {
        String value = props.getProperty(key);
        return Integer.parseInt(value);
    }

    public static boolean getBoolProperty(String key) {
        String value = getProperty(key);
        return Boolean.parseBoolean(value);
    }

    public static String getJarPath() {
        String path = ConfigUtil.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        String result = new File(path).getParentFile().getAbsolutePath();
        return CodecUtil.decode(result).replace("\\", "/");
    }

    public static boolean isDisk() {
        return "disk".equals(getProperty("server.storage.mode"));
    }

    public static String getDataPath() {
        return DATA_PATH;
    }
}
