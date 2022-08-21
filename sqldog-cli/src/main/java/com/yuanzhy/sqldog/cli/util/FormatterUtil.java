package com.yuanzhy.sqldog.cli.util;

import java.util.Arrays;
import java.util.stream.Collectors;

import com.yuanzhy.sqldog.core.util.ArrayUtils;
import com.yuanzhy.sqldog.core.util.StringUtils;

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
        if (sb.length() > 1) {
            sb.deleteCharAt(sb.length() - 1);
        }
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

    public static void translateLabel(String[] headers) {
        if (headers == null) {
            return;
        }
        for (int i = 0; i < headers.length; i++) {
            if (headers[i].equals("TABLE_CATALOG")) {
                headers[i] = "Database";
            } else if (headers[i].equals("TABLE_SCHEM")) {
                headers[i] = "Schema";
            } else if (headers[i].equals("TABLE_NAME")) {
                headers[i] = "Name";
            } else if (headers[i].equals("TABLE_TYPE")) {
                headers[i] = "Type";
            } else if (headers[i].equals("REMARKS")) {
                headers[i] = "Description";
            }
        }
    }
}
