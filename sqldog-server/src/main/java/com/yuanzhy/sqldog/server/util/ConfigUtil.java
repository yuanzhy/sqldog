package com.yuanzhy.sqldog.server.util;

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
 * @date 2018/6/13
 */
@Deprecated
public class ConfigUtil {

    private static Logger log = LoggerFactory.getLogger(ConfigUtil.class);

    private static boolean disk = true;
    /**
     *
     */
    private static Properties props = new Properties();

    static {
        // 先从平级目录找sqldog.properties
        InputStream in = null;
        try {
            File configFile = new File(getJarPath().concat("/sqldog.properties"));
            if (configFile.exists()) {
                log.info("Sqldog.jar同级目录下找到sqldog.properties，读取此配置");
                in = new FileInputStream(configFile);
            } else {
                log.info("Sqldog.jar同级目录下没有sqldog.properties，默认读取jar包中的配置");
                in = ConfigUtil.class.getClassLoader().getResourceAsStream("sqldog.properties");
            }
            props.load(in);
            disk = "disk".equals(getProperty("server.storage.mode", "disk"));
        } catch (IOException e) {
            log.error("读取config配置文件失败", e);
        } finally {
            IOUtils.closeQuietly(in);
        }
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
        return disk;
    }

    public static boolean useWriteCache() {
        return getBoolProperty("sqldog.storage.writeCache");
    }

    public static boolean isMemory() {
        return "memory".equals(getProperty("sqldog.storage.mode"));
    }
}
