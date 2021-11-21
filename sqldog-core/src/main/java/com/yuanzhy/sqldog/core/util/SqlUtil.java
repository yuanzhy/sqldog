package com.yuanzhy.sqldog.core.util;

import com.yuanzhy.sqldog.core.constant.Consts;

import java.io.IOException;
import java.io.StringReader;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/20
 */
public class SqlUtil {

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

    public static String upperCaseIgnoreValue(String str) {
        StringBuilder sb = new StringBuilder();
        boolean valueToken = false;
        boolean escape = false;
        for (char c : str.toCharArray()) {
            // --- 转义处理 ---
            if (c == Consts.SQL_ESCAPE) {
                escape = true;
                sb.append(c);
                continue;
            }
            if (!escape && c == Consts.SQL_QUOTES) {
                valueToken = !valueToken;
                sb.append(c);
            } else {
                sb.append(valueToken ? c : Character.toUpperCase(c));
            }
            escape = false;
        }
        return sb.toString();
    }

    public static int countQuestionMark(String sql) {
        boolean valueToken = false;
        boolean escape = false;
        int count = 0;
        for (char c : sql.toCharArray()) {
            // --- 转义处理 ---
            if (c == Consts.SQL_ESCAPE) {
                escape = true;
                continue;
            }
            if (!escape && c == Consts.SQL_QUOTES) {
                valueToken = !valueToken;
            } else if (!valueToken && c == Consts.SQL_QUESTION_MARK) {
                count++;
            }
            escape = false;
        }
        return count;
    }
}
