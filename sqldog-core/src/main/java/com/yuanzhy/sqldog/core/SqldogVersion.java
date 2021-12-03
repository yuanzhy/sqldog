package com.yuanzhy.sqldog.core;

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
        String version = getVersion();
        return version == null ? "" : version;
    }
}
