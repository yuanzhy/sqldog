package com.yuanzhy.sqldog.jdbc;

import com.yuanzhy.sqldog.jdbc.impl.ConnectionImpl;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/16
 */
abstract class UnregisteredDriver implements Driver {

    public static final String USER_PROPERTY_KEY = "user";
    public static final String PASSWORD_PROPERTY_KEY = "password";

    public static final String HOST_PROPERTY_KEY = "HOST";
    public static final String PORT_PROPERTY_KEY = "PORT";
    public static final String SCHEMA_PROPERTY_KEY = "SCHEMA";

    public static final String VERSION = "1.0.0";
    public static final String NAME = "Sqldog Connector Java";

    private static final String DEFAULT_PORT = "2345";

    private static final Pattern URL_PTN = Pattern.compile("jdbc:sqldog://(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})(:\\d{1,5})?(/[\\w\\d-_]+)?");

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (url == null) {
            throw new SQLException("url can not be null");
        }
        Properties props;
        if ((props = parseURL(url, info)) == null) {
            return null;
        }
        String host = props.getProperty(HOST_PROPERTY_KEY);
        int port = Integer.parseInt(props.getProperty(PORT_PROPERTY_KEY));
        String schema = props.getProperty(SCHEMA_PROPERTY_KEY);
        return new ConnectionImpl(host, port, schema, info);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return parseURL(url, null) != null;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        if (info == null) {
            info = new Properties();
        }
        info = parseURL(url, info);
        if (info == null) {
            throw new SQLException("Illegal url");
        }

        DriverPropertyInfo hostProp = new DriverPropertyInfo(HOST_PROPERTY_KEY, info.getProperty(HOST_PROPERTY_KEY));
        hostProp.required = true;
        hostProp.description = "Hostname of Sqldog Server";

        DriverPropertyInfo portProp = new DriverPropertyInfo(PORT_PROPERTY_KEY, info.getProperty(PORT_PROPERTY_KEY, DEFAULT_PORT));
        portProp.required = false;
        portProp.description = "Port number of Sqldog Server";

        DriverPropertyInfo schemaProp = new DriverPropertyInfo(SCHEMA_PROPERTY_KEY, info.getProperty(SCHEMA_PROPERTY_KEY, ""));
        schemaProp.required = false;
        schemaProp.description = "Current Schema name";

        DriverPropertyInfo userProp = new DriverPropertyInfo(USER_PROPERTY_KEY, info.getProperty(USER_PROPERTY_KEY));
        userProp.required = true;
        userProp.description = "Username to authenticate as";

        DriverPropertyInfo passwordProp = new DriverPropertyInfo(PASSWORD_PROPERTY_KEY, info.getProperty(PASSWORD_PROPERTY_KEY));
        passwordProp.required = true;
        passwordProp.description = "Password to use for authentication";

        return new DriverPropertyInfo[]{hostProp, portProp, schemaProp, userProp, passwordProp};
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

    /**
     * // jdbc:sqldog://[192.168.1.11][:2345][/schema_name]
     * @param url jdbcUrl
     * @return
     */
    protected Properties parseURL(String url, Properties defaults) {
        Matcher m = URL_PTN.matcher(url);
        String host, port, schema;
        if (m.find()) {
            host = m.group(1);
            port = m.group(2).substring(1);
            schema = m.group(3);
        } else {
            return null;
        }
        if (port == null) {
            port = DEFAULT_PORT;
        }
        Properties props = new Properties(defaults);
        props.put(HOST_PROPERTY_KEY, host);
        props.put(PORT_PROPERTY_KEY, port);
        if (schema != null && schema.length() > 1) {
            props.put(SCHEMA_PROPERTY_KEY, schema.substring(1));
        }
        return props;
    }
}
