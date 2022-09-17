package com.yuanzhy.sqldog.r2dbc;

import java.util.HashMap;
import java.util.Map;

import com.yuanzhy.sqldog.core.util.Asserts;
import com.yuanzhy.sqldog.core.util.StringUtils;

import io.r2dbc.spi.Option;

public class SqldogConnectionConfiguration {

    public static final Option<String> SCHEMA = Option.valueOf("schema");
    public static final Option<String> FILE_PATH = Option.valueOf("path");

    private final boolean embed;
    private final String filePath;
    private final String host;
    private final int port;
    private final String username;
    private final String password;
    private final String database;
    private final String schema;
    private final Map<String, String> options;

    private SqldogConnectionConfiguration(boolean embed, String filePath, String host, int port, String username, String password, String database, String schema, Map<String, String> options) {
        this.embed = embed;
        this.filePath = filePath;
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.database = database;
        this.schema = schema;
        this.options = options;
    }

    public boolean isEmbed() {
        return embed;
    }

    public String getFilePath() {
        return filePath;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getDatabase() {
        return database;
    }

    public String getSchema() {
        return schema;
    }

    public String getOption(String key) {
        return options.get(key);
    }

    public boolean hasOption(String key) {
        return options.containsKey(key);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private boolean embed;
        private String filePath;
        private String host;
        private int port;
        private String username;
        private String password;
        private String database;
        private String schema;

        private Map<String, String> options = new HashMap<>();
        private Builder() {
        }

        public Builder database(String database) {
            this.database = database;
            return this;
        }

        public Builder schema(String schema) {
            this.schema = schema;
            return this;
        }

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(int port) {
            Asserts.between(port, 0, 0xFFFF, "port must be between 0 and 65535");
            this.port = port;
            return this;
        }

        public Builder port(String port) {
            if (StringUtils.isNotBlank(port)) {
                this.port(Integer.parseInt(port.trim()));
            }
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder embed(boolean embed) {
            this.embed = embed;
            return this;
        }

        public Builder filePath(String filePath) {
            this.filePath = filePath;
            return this;
        }

        public Builder addOption(String key, String value) {
            options.put(key, value);
            return this;
        }

        public SqldogConnectionConfiguration build() {
            if (!embed) {
                Asserts.hasText(host, "host must not be null");
                Asserts.gteZero(port, "port must not be null");
                Asserts.hasText(username, "username must not be null");
                Asserts.hasText(password, "password must not be null");
            }
            return new SqldogConnectionConfiguration(embed, filePath, host, port, username, password, database, schema, options);
        }
    }
}
