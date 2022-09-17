package com.yuanzhy.sqldog.r2dbc;

import org.reactivestreams.Publisher;

import com.yuanzhy.sqldog.core.constant.Consts;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.R2dbcException;
import reactor.core.publisher.Mono;

public class SqldogConnectionFactory implements ConnectionFactory {

    private final Mono<SqldogConnection> client;

    private SqldogConnectionFactory(Mono<SqldogConnection> client) {
        this.client = client;
    }

    @Override
    public Publisher<? extends Connection> create() {
        return client;
    }

    @Override
    public ConnectionFactoryMetadata getMetadata() {
        return SqldogConnectionFactoryMetadata.INSTANCE;
    }

    public static SqldogConnectionFactory from(ConnectionFactoryOptions options) {
        final SqldogConnectionConfiguration config = adapt(options);
        return new SqldogConnectionFactory(Mono.defer(() -> Mono.create(sink -> {
            try {
                sink.success(new SqldogConnection(config));
            } catch (R2dbcException e) {
                sink.error(e);
            }
        })));
    }

    private static SqldogConnectionConfiguration adapt(ConnectionFactoryOptions options) {
        String host = (String) options.getValue(ConnectionFactoryOptions.HOST);
        boolean embed = false;
        if ("mem".equals(host) || "file".equals(host)) {
            embed = true;
            host = null;
        }
        Integer port = (Integer) options.getValue(ConnectionFactoryOptions.PORT);
        if (port == null) {
            port = 2345;
        }
        SqldogConnectionConfiguration.Builder builder = SqldogConnectionConfiguration.builder();
        builder
                .embed(embed)
                .host(host)
                .filePath((String) options.getValue(SqldogConnectionConfiguration.FILE_PATH))
                .port(port)
                .username((String) options.getValue(ConnectionFactoryOptions.USER))
                .password((String) options.getValue(ConnectionFactoryOptions.PASSWORD))
                .database((String) options.getValue(ConnectionFactoryOptions.DATABASE))
                .schema((String) options.getValue(SqldogConnectionConfiguration.SCHEMA));
        return builder.build();
    }


    private static class SqldogConnectionFactoryMetadata implements ConnectionFactoryMetadata {

        static final SqldogConnectionFactoryMetadata INSTANCE = new SqldogConnectionFactoryMetadata();

        @Override
        public String getName() {
            return Consts.PRODUCT_NAME;
        }
    }
}
