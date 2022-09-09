package com.yuanzhy.sqldog.server.common.config;

import java.io.File;

import com.yuanzhy.sqldog.server.util.CodecUtil;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/8/20
 */
public interface Config {

    String getProperty(String key);

    default int getIntProperty(String key) {
        String value = getProperty(key, null);
        return Integer.parseInt(value);
    }

    default int getIntProperty(String key, String defaultValue) {
        String value = getProperty(key, defaultValue);
        return Integer.parseInt(value);
    }

    default boolean getBoolProperty(String key) {
        String value = getProperty(key);
        return Boolean.parseBoolean(value);
    }

    default String getProperty(String key, String defaultValue) {
        String value = getProperty(key);
        return value == null || value.isEmpty() ? defaultValue : value;
    }

    default String getJarPath() {
        String path = Config.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        String result = new File(path).getParentFile().getAbsolutePath();
        return CodecUtil.decode(result).replace("\\", "/");
    }

    default boolean isDisk() {
        return "disk".equals(getProperty("sqldog.storage.mode", "disk"));
    }

    default boolean useWriteCache() {
        return getBoolProperty("sqldog.storage.writeCache");
    }

    default boolean isMemory() {
        return "memory".equals(getProperty("sqldog.storage.mode"));
    }
}
