package com.yuanzhy.sqldog.jdbc.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Statement;

/**
 *
 * @author yuanzhy
 * @date 2021-11-17
 */
class Util {

    /**
     * Determines whether or not the string 'searchIn' contains the string
     * 'searchFor', disregarding case and leading whitespace
     *
     * @param searchIn
     *            the string to search in
     * @param searchFor
     *            the string to search for
     *
     * @return true if the string starts with 'searchFor' ignoring whitespace
     */
    static boolean startsWithIgnoreCaseAndWs(String searchIn, String searchFor) {
        return startsWithIgnoreCaseAndWs(searchIn, searchFor, 0);
    }

    /**
     * Determines whether or not the string 'searchIn' contains the string
     * 'searchFor', disregarding case and leading whitespace
     *
     * @param searchIn
     *            the string to search in
     * @param searchFor
     *            the string to search for
     * @param beginPos
     *            where to start searching
     *
     * @return true if the string starts with 'searchFor' ignoring whitespace
     */

    static boolean startsWithIgnoreCaseAndWs(String searchIn, String searchFor, int beginPos) {
        if (searchIn == null) {
            return searchFor == null;
        }

        int inLength = searchIn.length();

        for (; beginPos < inLength; beginPos++) {
            if (!Character.isWhitespace(searchIn.charAt(beginPos))) {
                break;
            }
        }

        return startsWithIgnoreCase(searchIn, beginPos, searchFor);
    }

    /**
     * Determines whether or not the string 'searchIn' starts with one of the strings in 'searchFor', disregarding case
     * and leading whitespace
     *
     * @param searchIn
     *            the string to search in
     * @param searchFor
     *            the string array to search for
     *
     * @return the 'searchFor' array index that matched or -1 if none matches
     */
    static int startsWithIgnoreCaseAndWs(String searchIn, String[] searchFor) {
        for (int i = 0; i < searchFor.length; i++) {
            if (startsWithIgnoreCaseAndWs(searchIn, searchFor[i], 0)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Determines whether or not the string 'searchIn' contains the string
     * 'searchFor', dis-regarding case starting at 'startAt' Shorthand for a
     * String.regionMatch(...)
     *
     * @param searchIn
     *            the string to search in
     * @param startAt
     *            the position to start at
     * @param searchFor
     *            the string to search for
     *
     * @return whether searchIn starts with searchFor, ignoring case
     */
    static boolean startsWithIgnoreCase(String searchIn, int startAt, String searchFor) {
        return searchIn.regionMatches(true, startAt, searchFor, 0, searchFor.length());
    }

    /**
     * Determines whether or not the string 'searchIn' contains the string
     * 'searchFor', dis-regarding case. Shorthand for a String.regionMatch(...)
     *
     * @param searchIn
     *            the string to search in
     * @param searchFor
     *            the string to search for
     *
     * @return whether searchIn starts with searchFor, ignoring case
     */
    static boolean startsWithIgnoreCase(String searchIn, String searchFor) {
        return startsWithIgnoreCase(searchIn, 0, searchFor);
    }

    static int findStartOfStatement(String sql) {
        int statementStartPos = 0;

        if (startsWithIgnoreCaseAndWs(sql, "/*")) {
            statementStartPos = sql.indexOf("*/");

            if (statementStartPos == -1) {
                statementStartPos = 0;
            } else {
                statementStartPos += 2;
            }
        } else if (startsWithIgnoreCaseAndWs(sql, "--") || startsWithIgnoreCaseAndWs(sql, "#")) {
            statementStartPos = sql.indexOf('\n');

            if (statementStartPos == -1) {
                statementStartPos = sql.indexOf('\r');

                if (statementStartPos == -1) {
                    statementStartPos = 0;
                }
            }
        }

        return statementStartPos;
    }

    static char firstAlphaCharUc(String searchIn, int startAt) {
        if (searchIn == null) {
            return 0;
        }

        int length = searchIn.length();

        for (int i = startAt; i < length; i++) {
            char c = searchIn.charAt(i);

            if (Character.isLetter(c)) {
                return Character.toUpperCase(c);
            }
        }

        return 0;
    }

    static int autoGeneratedKey(int[] array) {
        return array != null && array.length > 0 ? Statement.RETURN_GENERATED_KEYS : Statement.NO_GENERATED_KEYS;
    }

    static int autoGeneratedKey(Object[] array) {
        return array != null && array.length > 0 ? Statement.RETURN_GENERATED_KEYS : Statement.NO_GENERATED_KEYS;
    }

    static boolean isEmpty(CharSequence cs) {
        return cs == null || cs.length() == 0;
    }

    static String substringBefore(String str, String separator) {
        if (!isEmpty(str) && separator != null) {
            if (separator.isEmpty()) {
                return "";
            } else {
                int pos = str.indexOf(separator);
                return pos == -1 ? str : str.substring(0, pos);
            }
        } else {
            return str;
        }
    }

    static String substringAfter(String str, String separator) {
        if (isEmpty(str)) {
            return str;
        } else if (separator == null) {
            return "";
        } else {
            int pos = str.indexOf(separator);
            return pos == -1 ? "" : str.substring(pos + separator.length());
        }
    }

    static byte[] toByteArray(InputStream is) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buf = new byte[4096];
        try (InputStream stream = is) {
            int n;
            while (-1 != (n = stream.read(buf))) {
                baos.write(buf, 0, n);
            }
            return baos.toByteArray();
        }
    }

    static byte[] toByteArray(InputStream is, int size) throws IOException {
        if (size < 0) {
            throw new IllegalArgumentException("Size must be equal or greater than zero: " + size);
        }
        if (size == 0) {
            return new byte[0];
        }
        final byte[] data = new byte[size];
        int offset = 0;
        int read;
        while (offset < size && (read = is.read(data, offset, size - offset)) != -1) {
            offset += read;
        }
        if (offset != size) {
            throw new IOException("Unexpected read size, current: " + offset + ", expected: " + size);
        }
        return data;
    }
}
