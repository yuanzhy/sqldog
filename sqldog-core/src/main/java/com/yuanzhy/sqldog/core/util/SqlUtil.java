package com.yuanzhy.sqldog.core.util;

import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Array;
import java.sql.Date;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/20
 */
public class SqlUtil {

    public static final String[] EMPTY_STRING_ARRAY = new String[0];

    public static String stripComments(String src, String stringOpens, String stringCloses, boolean slashStarComments, boolean slashSlashComments,
                                boolean hashComments, boolean dashDashComments) {
        if (src == null) {
            return null;
        }

        StringBuilder strBuilder = new StringBuilder(src.length());

        // It's just more natural to deal with this as a stream when parsing..This code is currently only called when parsing the kind of metadata that
        // developers are strongly recommended to cache anyways, so we're not worried about the _1_ extra object allocation if it cleans up the code

        StringReader sourceReader = new StringReader(src);

        int contextMarker = Character.MIN_VALUE;
        boolean escaped = false;
        int markerTypeFound = -1;

        int ind = 0;

        int currentChar = 0;

        try {
            while ((currentChar = sourceReader.read()) != -1) {

                if (markerTypeFound != -1 && currentChar == stringCloses.charAt(markerTypeFound) && !escaped) {
                    contextMarker = Character.MIN_VALUE;
                    markerTypeFound = -1;
                } else if ((ind = stringOpens.indexOf(currentChar)) != -1 && !escaped && contextMarker == Character.MIN_VALUE) {
                    markerTypeFound = ind;
                    contextMarker = currentChar;
                }

                if (contextMarker == Character.MIN_VALUE && currentChar == '/' && (slashSlashComments || slashStarComments)) {
                    currentChar = sourceReader.read();
                    if (currentChar == '*' && slashStarComments) {
                        int prevChar = 0;
                        while ((currentChar = sourceReader.read()) != '/' || prevChar != '*') {
                            if (currentChar == '\r') {

                                currentChar = sourceReader.read();
                                if (currentChar == '\n') {
                                    currentChar = sourceReader.read();
                                }
                            } else {
                                if (currentChar == '\n') {

                                    currentChar = sourceReader.read();
                                }
                            }
                            if (currentChar < 0) {
                                break;
                            }
                            prevChar = currentChar;
                        }
                        continue;
                    } else if (currentChar == '/' && slashSlashComments) {
                        while ((currentChar = sourceReader.read()) != '\n' && currentChar != '\r' && currentChar >= 0) {
                        }
                    }
                } else if (contextMarker == Character.MIN_VALUE && currentChar == '#' && hashComments) {
                    // Slurp up everything until the newline
                    while ((currentChar = sourceReader.read()) != '\n' && currentChar != '\r' && currentChar >= 0) {
                    }
                } else if (contextMarker == Character.MIN_VALUE && currentChar == '-' && dashDashComments) {
                    currentChar = sourceReader.read();

                    if (currentChar == -1 || currentChar != '-') {
                        strBuilder.append('-');

                        if (currentChar != -1) {
                            strBuilder.append((char) currentChar);
                        }

                        continue;
                    }

                    // Slurp up everything until the newline

                    while ((currentChar = sourceReader.read()) != '\n' && currentChar != '\r' && currentChar >= 0) {
                    }
                }

                if (currentChar != -1) {
                    strBuilder.append((char) currentChar);
                }
            }
        } catch (IOException ioEx) {
            // we'll never see this from a StringReader
        }

        return strBuilder.toString();
    }

    public static Array toArray(Object x) {
        if (x instanceof Array) {
            return (Array) x;
        }
        throw unsupportedCast(x.getClass(), Array.class);
    }

    public static BigDecimal toBigDecimal(Object x) {
        if (x instanceof BigDecimal) {
            return (BigDecimal) x;
        } else if (x instanceof BigInteger) {
            return new BigDecimal((BigInteger) x);
        } else if (x instanceof Number) {
            if (x instanceof Double || x instanceof Float) {
                return new BigDecimal(((Number) x).doubleValue());
            } else {
                return new BigDecimal(((Number) x).longValue());
            }
        } else if (x instanceof Boolean) {
            return (Boolean) x ? BigDecimal.ONE : BigDecimal.ZERO;
        } else if (x instanceof String) {
            return new BigDecimal((String) x);
        }
        throw unsupportedCast(x.getClass(), BigDecimal.class);
    }

    public static boolean toBoolean(Object x) {
        if (x instanceof Boolean) {
            return (Boolean) x;
        } else if (x instanceof Number) {
            return ((Number) x).intValue() != 0;
        } else if (x instanceof String) {
            String s = (String) x;
            if (s.equalsIgnoreCase("true") || s.equalsIgnoreCase("yes")) {
                return true;
            } else if (s.equalsIgnoreCase("false") || s.equalsIgnoreCase("no")) {
                return false;
            }
        }
        throw unsupportedCast(x.getClass(), Boolean.TYPE);
    }

    public static byte toByte(Object x) {
        if (x instanceof Number) {
            return ((Number) x).byteValue();
        } else if (x instanceof Boolean) {
            return (Boolean) x ? (byte) 1 : (byte) 0;
        } else if (x instanceof String) {
            return Byte.parseByte((String) x);
        } else {
            throw unsupportedCast(x.getClass(), Byte.TYPE);
        }
    }

    public static byte[] toBytes(Object x) {
        if (x instanceof byte[]) {
            return (byte[]) x;
        }
        if (x instanceof String) {
            return ((String) x).getBytes();
        }
//        if (x instanceof InputStream) {
//            return IOUtils.toByteArray((InputStream)x);
//        }
        throw unsupportedCast(x.getClass(), byte[].class);
    }

    public static Date toDate(Object x) {
        if (x instanceof Date) {
            return (Date) x;
        }
        if (x instanceof java.util.Date) {
            return new Date(((java.util.Date) x).getTime());
        }
        if (x instanceof String) {
            return DateUtil.parseSqlDate((String) x);
        }
        if (x instanceof Number) {
            return new Date(((Number)x).longValue());
        }
        throw unsupportedCast(x.getClass(), Date.class);
    }

    public static Time toTime(Object x) {
        if (x instanceof Time) {
            return (Time) x;
        }
        if (x instanceof String) {
            return DateUtil.parseSqlTime((String) x);
        }
        if (x instanceof Number) {
            return new Time(((Number)x).longValue());
        }
        throw unsupportedCast(x.getClass(), Time.class);
    }

    public static Timestamp toTimestamp(Object x) {
        if (x instanceof Timestamp) {
            return (Timestamp) x;
        }
        if (x instanceof java.util.Date) {
            return new Timestamp(((java.util.Date) x).getTime());
        }
        if (x instanceof String) {
            return DateUtil.parseTimestamp((String) x);
        }
        if (x instanceof Number) {
            return new Timestamp(((Number)x).longValue());
        }
        throw unsupportedCast(x.getClass(), Timestamp.class);
    }

    public static double toDouble(Object x) {
        if (x instanceof Number) {
            return ((Number) x).doubleValue();
        } else if (x instanceof Boolean) {
            return (Boolean) x ? 1D : 0D;
        } else if (x instanceof String) {
            return Double.parseDouble((String) x);
        } else {
            throw unsupportedCast(x.getClass(), Double.TYPE);
        }
    }

    public static float toFloat(Object x) {
        if (x instanceof Number) {
            return ((Number) x).floatValue();
        } else if (x instanceof Boolean) {
            return (Boolean) x ? 1F : 0F;
        } else if (x instanceof String) {
            return Float.parseFloat((String) x);
        } else {
            throw unsupportedCast(x.getClass(), Float.TYPE);
        }
    }

    public static int toInt(Object x) {
        if (x instanceof Number) {
            return ((Number) x).intValue();
        } else if (x instanceof Boolean) {
            return (Boolean) x ? 1 : 0;
        } else if (x instanceof String) {
            return Integer.parseInt((String) x);
        } else {
            throw unsupportedCast(x.getClass(), Integer.TYPE);
        }
    }

    public static long toLong(Object x) {
        if (x instanceof Number) {
            return ((Number) x).longValue();
        } else if (x instanceof Boolean) {
            return (Boolean) x ? 1L : 0L;
        } else if (x instanceof String) {
            return Long.parseLong((String) x);
        } else {
            throw unsupportedCast(x.getClass(), Long.TYPE);
        }
    }

    public static short toShort(Object x) {
        if (x instanceof Number) {
            return ((Number) x).shortValue();
        } else if (x instanceof Boolean) {
            return (Boolean) x ? (short) 1 : (short) 0;
        } else if (x instanceof String) {
            return Short.parseShort((String) x);
        } else {
            throw unsupportedCast(x.getClass(), Short.TYPE);
        }
    }

    public static String toString(Object x) {
        if (x instanceof String) {
            return (String) x;
        } else if (x instanceof Character
                || x instanceof Boolean) {
            return x.toString();
        }
        throw unsupportedCast(x.getClass(), String.class);
    }

    public static UnsupportedOperationException unsupportedCast(Class<?> from, Class<?> to) {
        return new UnsupportedOperationException("Cannot convert from "
                + from.getCanonicalName() + " to " + to.getCanonicalName());
    }

    public static SQLFeatureNotSupportedException notImplemented() {
        return new SQLFeatureNotSupportedException("not implemented");
    }

    private static final char DEFAULT_SEPARATOR = ',';
    private static final char DEFAULT_QUOTE = '\'';

    public static String[] parseLine(String line) {
        return parseLine(line, DEFAULT_SEPARATOR, DEFAULT_QUOTE);
    }

    public static String[] parseLine(String line, char separators) {
        return parseLine(line, separators, DEFAULT_QUOTE);
    }

    public static String[] parseLine(String line, char separators, char customQuote) {

        List<String> result = new ArrayList<>();

        //if empty, return!
        if (line == null && line.isEmpty()) {
            return EMPTY_STRING_ARRAY;
        }

        if (customQuote == ' ') {
            customQuote = DEFAULT_QUOTE;
        }

        if (separators == ' ') {
            separators = DEFAULT_SEPARATOR;
        }

        StringBuffer curVal = new StringBuffer();
        boolean inQuotes = false;
        boolean startCollectChar = false;
        boolean doubleQuotesInColumn = false;

        char[] chars = line.toCharArray();

        for (char ch : chars) {

            if (inQuotes) {
                startCollectChar = true;
                if (ch == customQuote) {
                    inQuotes = false;
                    doubleQuotesInColumn = false;
                } else {

                    //Fixed : allow "" in custom quote enclosed
                    if (ch == '\"') {
                        if (!doubleQuotesInColumn) {
                            curVal.append(ch);
                            doubleQuotesInColumn = true;
                        }
                    } else {
                        curVal.append(ch);
                    }

                }
            } else {
                if (ch == customQuote) {

                    inQuotes = true;

                    //Fixed : allow "" in empty quote enclosed
                    if (chars[0] != '"' && customQuote == '\"') {
                        curVal.append('"');
                    }

                    //double quotes in column will hit this!
                    if (startCollectChar) {
                        curVal.append('"');
                    }

                } else if (ch == separators) {

                    result.add(curVal.toString());

                    curVal = new StringBuffer();
                    startCollectChar = false;

                } else if (ch == '\r') {
                    //ignore LF characters
                    continue;
                } else if (ch == '\n') {
                    //the end, break!
                    break;
                } else {
                    curVal.append(ch);
                }
            }

        }

        result.add(curVal.toString());

        return result.toArray(new String[0]);
    }

    public static String escape(String keyword) {
        return keyword.replace("'", "\\'")
                .replace("%", "\\%")
//                .replace("_", "\\_")
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("`", "\\`").toUpperCase();
    }

}
