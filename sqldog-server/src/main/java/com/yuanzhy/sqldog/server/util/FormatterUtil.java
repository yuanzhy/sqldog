package com.yuanzhy.sqldog.server.util;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author yuanzhy
 * @date 2021-11-15
 */
public final class FormatterUtil {

    public static String joinByVLine(int maxLength, String... values) {
        return Arrays.stream(values).map(v -> " " + StringUtils.rightPad(StringUtils.trimToEmpty(v), maxLength)).collect(
                Collectors.joining("|"));
    }

    public static String genHLine(int maxLength, int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(StringUtils.repeat("-", maxLength));
            sb.append("-");
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }
}
