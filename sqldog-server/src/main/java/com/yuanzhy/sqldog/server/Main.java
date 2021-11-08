package com.yuanzhy.sqldog.server;

import com.yuanzhy.sqldog.server.io.BioServer;
import com.yuanzhy.sqldog.server.io.NioServer;
import com.yuanzhy.sqldog.server.util.ConfigUtil;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/31
 */
public class Main {

    public static void main(String[] args) { // 用户可自行指定端口号
        String ioMode = ConfigUtil.getProperty("server.io");
        if ("bio".equals(ioMode)) {
            new BioServer().start();
        } else if ("nio".equals(ioMode)) {
            new NioServer().start();
        } else {
            throw new IllegalArgumentException(ioMode + " not supported");
        }
    }
}