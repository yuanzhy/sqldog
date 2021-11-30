package com.yuanzhy.sqldog.cli.util;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 *
 * @author yuanzhy
 * @date 2021-11-15
 */
public final class FormatterUtil {

    public static String joinByVLine(int maxLength, String... values) {
        return Arrays.stream(values).map(v -> {
            int doubleBytes = 0;
            for (char ch : v.toCharArray()) {
                if (ch >= 0x80) {
                    doubleBytes++;
                }
            }
            String padValue = " ".concat(StringUtils.rightPad(StringUtils.trimToEmpty(v), maxLength - doubleBytes));
            if (!padValue.endsWith(" ")) {
                padValue = padValue.concat(" ");
            }
            return padValue;
        }).collect(Collectors.joining("|"));
    }

    public static String genHLine(int maxLength, int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(StringUtils.repeat("-", maxLength));
            sb.append("-+");
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    public static String join(String[] strings, String delimiter) {
        if (ArrayUtils.isEmpty(strings)) {
            return "";
        }
        if (strings.length == 1) {
            return strings[0];
        }
        return Arrays.stream(strings).collect(Collectors.joining(","));
    }
}
