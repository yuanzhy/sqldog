package com.yuanzhy.sqldog.r2dbc;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.time.Duration;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicBoolean;

import org.reactivestreams.Publisher;

import com.yuanzhy.sqldog.core.constant.Consts;
import com.yuanzhy.sqldog.core.service.EmbedService;
import com.yuanzhy.sqldog.core.service.Executor;
import com.yuanzhy.sqldog.core.service.Service;
import com.yuanzhy.sqldog.core.util.Asserts;
import com.yuanzhy.sqldog.core.util.StringUtils;

import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionMetadata;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.TransactionDefinition;
import io.r2dbc.spi.ValidationDepth;
import reactor.core.publisher.Mono;

/**
 * 由于使用rmi 没有实现非阻塞io, 后续自定义协议后需重写r2dbc实现
 * @author yuanzhy
 * @version 1.0
 * @date 2022/09/10
 */
public class SqldogConnection implements Connection {

    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    final Executor executor;
    final String schema;
    Duration statementTimeout = Duration.ofSeconds(30);
    SqldogConnection(SqldogConnectionConfiguration config) {
        if (config.isEmbed()) {
            ServiceLoader<EmbedService> sl = ServiceLoader.load(EmbedService.class);
            try {
                EmbedService service = sl.iterator().next();
                executor = service.connect(config.getFilePath());
            } catch (Exception e) {
                throw new R2dbcConnectionException(e);
            }
        } else {
            try {
                Registry registry = LocateRegistry.getRegistry(config.getHost(), config.getPort());
                Service service = (Service) registry.lookup(Consts.SERVER_NAME);
                executor = service.connect(config.getUsername(), config.getPassword());
            } catch (Exception e) {
                throw new R2dbcConnectionException(e);
            }
        }
        schema = StringUtils.isEmpty(config.getSchema()) ? "PUBLIC" : config.getSchema();
    }

    @Override
    public Publisher<Void> beginTransaction() {
        return Mono.empty();
    }

    @Override
    public Publisher<Void> beginTransaction(TransactionDefinition definition) {
        return Mono.empty();
    }

    @Override
    public Publisher<Void> close() {
        return Mono.fromRunnable(() -> {
            boolean connected = isConnected();
            if (this.isClosed.compareAndSet(false, true)) {
                if (connected) {
                    try {
                        executor.close();
                    } catch (RemoteException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });
    }

    @Override
    public Publisher<Void> commitTransaction() {
        return Mono.empty();
    }

    @Override
    public Batch createBatch() {
        return new SqldogBatch(this, schema);
    }

    @Override
    public Publisher<Void> createSavepoint(String name) {
        return Mono.empty();
    }

    @Override
    public Statement createStatement(String sql) {
        Asserts.hasText(sql, "sql must not be null");
        return new SqldogStatement(this, sql);
    }

    @Override
    public boolean isAutoCommit() {
        return true;
    }

    @Override
    public ConnectionMetadata getMetadata() {
        return SqldogConnectionMetadata.INSTANCE;
    }

    @Override
    public IsolationLevel getTransactionIsolationLevel() {
        return null;
    }

    @Override
    public Publisher<Void> releaseSavepoint(String name) {
        return Mono.empty();
    }

    @Override
    public Publisher<Void> rollbackTransaction() {
        return Mono.empty();
    }

    @Override
    public Publisher<Void> rollbackTransactionToSavepoint(String name) {
        return Mono.empty();
    }

    @Override
    public Publisher<Void> setAutoCommit(boolean autoCommit) {
        return Mono.empty();
    }

    @Override
    public Publisher<Void> setLockWaitTimeout(Duration timeout) {
        return Mono.empty();
    }

    @Override
    public Publisher<Void> setStatementTimeout(Duration timeout) {
        if (timeout == null) {
            return Mono.empty();
        }
        return Mono.fromRunnable(() -> this.statementTimeout = timeout);
    }

    @Override
    public Publisher<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
        return Mono.empty();
    }

    @Override
    public Publisher<Boolean> validate(ValidationDepth depth) {
        Asserts.notNull(depth, "depth must not be null");
        if (depth == ValidationDepth.LOCAL) {
            return Mono.fromSupplier(this::isConnected);
        }
        return Mono.defer(() -> {
            try {
                if (!this.isConnected()) {
                    return Mono.just(false);
                }
                executor.getVersion();
                return Mono.just(true);
            } catch (Exception e) {
                return Mono.just(false);
            }
        });
    }

    boolean isConnected() {
        if (this.isClosed.get()) {
            return false;
        }
        return true;
    }
}
