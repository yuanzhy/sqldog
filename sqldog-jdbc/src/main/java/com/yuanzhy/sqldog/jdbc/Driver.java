package com.yuanzhy.sqldog.jdbc;

import java.sql.DriverManager;
import java.sql.SQLException;

/**
 *
 * @author yuanzhy
 * @date 2021-11-16
 */
public class Driver extends UnregisteredDriver {

    static {
        try {
            DriverManager.registerDriver(new Driver());
        } catch (SQLException e) {
            throw new RuntimeException("Can't register driver!");
        }
    }
}
