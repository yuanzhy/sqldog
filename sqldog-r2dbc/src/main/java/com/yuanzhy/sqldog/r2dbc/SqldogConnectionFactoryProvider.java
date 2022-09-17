package com.yuanzhy.sqldog.r2dbc;

import com.yuanzhy.sqldog.core.constant.Consts;

import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.ConnectionFactoryProvider;

import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/09/01
 */
public class SqldogConnectionFactoryProvider implements ConnectionFactoryProvider {

    @Override
    public ConnectionFactory create(ConnectionFactoryOptions connectionFactoryOptions) {
        return SqldogConnectionFactory.from(connectionFactoryOptions);
    }

    @Override
    public boolean supports(ConnectionFactoryOptions connectionFactoryOptions) {
        return getDriver().equals(connectionFactoryOptions.getValue(DRIVER));
    }

    @Override
    public String getDriver() {
        return Consts.PRODUCT_NAME;
    }

//    static MySqlConnectionConfiguration setup(ConnectionFactoryOptions options) {
//        OptionMapper mapper = new OptionMapper(options);
//        MySqlConnectionConfiguration.Builder builder = MySqlConnectionConfiguration.builder();
//
//        mapper.requiredConsume(USER, builder::user);
//        // Notice for contributors: password is special, should keep it CharSequence,
//        // do NEVER use OptionMapper.from because it maybe convert password to String.
//        mapper.consume(PASSWORD, builder::password);
//
//        mapper.from(UNIX_SOCKET).asString()
//                .into(builder::unixSocket)
//                .otherwise(() -> setupHost(builder, mapper));
//        mapper.from(SERVER_ZONE_ID).asInstance(ZoneId.class, id -> ZoneId.of(id, ZoneId.SHORT_IDS))
//                .into(builder::serverZoneId);
//        mapper.from(TCP_KEEP_ALIVE).asBoolean()
//                .into(builder::tcpKeepAlive);
//        mapper.from(TCP_NO_DELAY).asBoolean()
//                .into(builder::tcpNoDelay);
//        mapper.from(ZERO_DATE).asInstance(ZeroDateOption.class, id -> ZeroDateOption.valueOf(id.toUpperCase()))
//                .into(builder::zeroDateOption);
//        mapper.from(USE_SERVER_PREPARE_STATEMENT).servePrepare(enable -> {
//            if (enable) {
//                builder.useServerPrepareStatement();
//            } else {
//                builder.useClientPrepareStatement();
//            }
//        }, builder::useServerPrepareStatement);
//        mapper.from(AUTODETECT_EXTENSIONS).asBoolean()
//                .into(builder::autodetectExtensions);
//        mapper.from(CONNECT_TIMEOUT).asInstance(Duration.class, Duration::parse)
//                .into(builder::connectTimeout);
//        mapper.from(DATABASE).asString()
//                .into(builder::database);
//
//        return builder.build();
//    }
//
//    /**
//     * Set builder of {@link MySqlConnectionConfiguration} for hostname-based address with SSL
//     * configurations.
//     * <p>
//     * Notice for contributors: SSL key password is special, should keep it {@link CharSequence},
//     * do NEVER use {@link OptionMapper#from} because it maybe convert password to {@link String}.
//     *
//     * @param builder the builder of {@link MySqlConnectionConfiguration}.
//     * @param mapper  the {@link OptionMapper} of {@code options}.
//     */
//    @SuppressWarnings("unchecked")
//    private static void setupHost(MySqlConnectionConfiguration.Builder builder, OptionMapper mapper) {
//        mapper.requiredConsume(HOST, builder::host);
//        mapper.from(PORT).asInt()
//                .into(builder::port);
//        mapper.from(SSL).asBoolean()
//                .into(isSsl -> builder.sslMode(isSsl ? SslMode.REQUIRED : SslMode.DISABLED));
//        mapper.from(SSL_MODE).asInstance(SslMode.class, id -> SslMode.valueOf(id.toUpperCase()))
//                .into(builder::sslMode);
//        mapper.from(TLS_VERSION).asStrings()
//                .into(builder::tlsVersion);
//        mapper.from(SSL_HOSTNAME_VERIFIER).asInstance(HostnameVerifier.class)
//                .into(builder::sslHostnameVerifier);
//        mapper.from(SSL_CERT).asString()
//                .into(builder::sslCert);
//        mapper.from(SSL_KEY).asString()
//                .into(builder::sslKey);
//        mapper.consume(SSL_KEY_PASSWORD, builder::sslKeyPassword);
//        mapper.from(SSL_CONTEXT_BUILDER_CUSTOMIZER).asInstance(Function.class)
//                .into(customizer -> builder.sslContextBuilderCustomizer((Function<SslContextBuilder, SslContextBuilder>) customizer));
//        mapper.from(SSL_CA).asString()
//                .into(builder::sslCa);
//    }
}
