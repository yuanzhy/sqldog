package com.yuanzhy.sqldog.server.common.config;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/8/20
 */
public class Configs {

    private static Config CONFIG;

    public static void init() {
        if (CONFIG == null) {
            CONFIG = new ServerConfig();
        }
    }

    public static void initEmbed(String filePath) {
        if (CONFIG == null) {
            CONFIG = new EmbedConfig(filePath);
        }
    }

    public static Config get() {
        return CONFIG;
    }
}
