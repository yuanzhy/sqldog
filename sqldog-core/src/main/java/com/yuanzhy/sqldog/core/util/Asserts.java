package com.yuanzhy.sqldog.core.util;

import java.util.Collection;
import java.util.Map;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/24
 */
public class Asserts {

    public static void notNull(Object value, String msg) {
        if (value == null) {
            throw new IllegalArgumentException(msg);
        }
    }

    public static void hasText(String value, String msg) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(msg);
        }
    }

    public static void hasEle(Map<?, ?> value, String msg) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(msg);
        }
    }

    public static void hasEle(Collection<?> value, String msg) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(msg);
        }
    }

    public static void isTrue(boolean value, String msg) {
        if (!value) {
            throw new IllegalArgumentException(msg);
        }
    }

    public static void isFalse(boolean value, String msg) {
        isTrue(!value, msg);
    }

    public static void gteZero(int value, String msg) {
        isTrue(value >= 0, msg);
    }
}
