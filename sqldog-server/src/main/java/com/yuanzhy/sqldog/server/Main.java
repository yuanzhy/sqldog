package com.yuanzhy.sqldog.server;

import com.yuanzhy.sqldog.server.common.config.Configs;
import com.yuanzhy.sqldog.server.io.BioServer;
import com.yuanzhy.sqldog.server.io.BioServer2;
import com.yuanzhy.sqldog.server.io.NioServer;
import com.yuanzhy.sqldog.server.io.RmiServer;

/**
 * mvn clean package -DskipTests
 *
 * With the property autoReleaseAfterClose set to false you can manually inspect the staging repository in the Nexus Repository Manager and trigger a release of the staging repository later with
 * mvn nexus-staging:release
 * If you find something went wrong you can drop the staging repository with
 * mvn nexus-staging:drop
 *
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/31
 */
public class Main {

    public static void main(String[] args) { // 用户可自行指定端口号
        Configs.init();
        String ioMode = Configs.get().getProperty("sqldog.io", "rmi");
        if ("rmi".equals(ioMode)) {
            new RmiServer().start();
        } else if ("bio2".equals(ioMode)) {
            new BioServer2().start();
        } else if ("bio".equals(ioMode)) {
            new BioServer().start();
        } else if ("nio".equals(ioMode)) {
            new NioServer().start();
        } else {
            throw new IllegalArgumentException(ioMode + " not supported");
        }
    }
}
