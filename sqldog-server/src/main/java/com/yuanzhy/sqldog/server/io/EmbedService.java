package com.yuanzhy.sqldog.server.io;

import com.yuanzhy.sqldog.core.service.Executor;
import com.yuanzhy.sqldog.server.common.config.Configs;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/8/20
 */
public class EmbedService implements com.yuanzhy.sqldog.core.service.EmbedService {

    private static volatile int count = 0;

    @Override
    public Executor connect(String filePath) {
        Configs.initEmbed(filePath);
        return new ExecutorImpl(++count);
    }

    private class ExecutorImpl extends AbstractExecutor implements Executor {
        ExecutorImpl(int serialNum) {
            super(serialNum);
            log.info("newConnection: {}", serialNum);
        }

        @Override
        public void close() {
            super.close();
            count--;
            log.info("closeConnection: {}", serialNum);
        }
    }
}
