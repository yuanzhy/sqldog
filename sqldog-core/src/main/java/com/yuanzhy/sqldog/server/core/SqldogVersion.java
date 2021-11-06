package com.yuanzhy.sqldog.server.core;

import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author yuanzhy
 * @date 2021-11-05
 */
public class SqldogVersion {

    /**
     * constructor
     */
    private SqldogVersion() {}

    /**
     * 返回当前TAS版本号
     * @return the version of tas or {@code null}
     * @see Package#getImplementationVersion()
     */
    public static String getVersion() {
        Package pkg = SqldogVersion.class.getPackage();
        return (pkg != null ? pkg.getImplementationVersion() : null);
    }

    /**
     * 返回当前TAS版本号
     * @return the version of tas or {@code null}
     * @see Package#getImplementationVersion()
     */
    public static String getVersionOfEmpty() {
        Package pkg = SqldogVersion.class.getPackage();
        String version = (pkg != null ? pkg.getImplementationVersion() : null);
        return StringUtils.trimToEmpty(version);
    }
}
