package com.yuanzhy.sqldog.r2dbc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import com.yuanzhy.sqldog.core.util.StringUtils;

/**
 *
 * @author yuanzhy
 * @date 2021-11-17
 */
class Util {

    private static final String commentPrefix = "--";
    private static final String blockCommentStartDelimiter = "/*";
    private static final String blockCommentEndDelimiter = "*/";
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

    static boolean isEmpty(CharSequence cs) {
        return cs == null || cs.length() == 0;
    }

    static boolean isNotEmpty(CharSequence cs) {
        return !isEmpty(cs);
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

    /**
     * Read a script from the provided resource, using the supplied comment prefix
     * and statement separator, and build a {@code String} containing the lines.
     * <p>Lines <em>beginning</em> with the comment prefix are excluded from the
     * results; however, line comments anywhere else &mdash; for example, within
     * a statement &mdash; will be included in the results.
     * @param resource the {@code EncodedResource} containing the script
     * to be processed
     * @param commentPrefix the prefix that identifies comments in the SQL script
     * (typically "--")
     * @param separator the statement separator in the SQL script (typically ";")
     * @param blockCommentEndDelimiter the <em>end</em> block comment delimiter
     * @return a {@code String} containing the script lines
     * @throws IOException in case of I/O errors
     */
    private static String readScript(String resource, String commentPrefix, String separator,
                                     String blockCommentEndDelimiter) throws IOException {
        try (LineNumberReader lnr = new LineNumberReader(new StringReader(resource))) {
            return readScript(lnr, commentPrefix, separator, blockCommentEndDelimiter);
        }
    }

    /**
     * Read a script from the provided {@code LineNumberReader}, using the supplied
     * comment prefix and statement separator, and build a {@code String} containing
     * the lines.
     * <p>Lines <em>beginning</em> with the comment prefix are excluded from the
     * results; however, line comments anywhere else &mdash; for example, within
     * a statement &mdash; will be included in the results.
     * @param lineNumberReader the {@code LineNumberReader} containing the script
     * to be processed
     * @param lineCommentPrefix the prefix that identifies comments in the SQL script
     * (typically "--")
     * @param separator the statement separator in the SQL script (typically ";")
     * @param blockCommentEndDelimiter the <em>end</em> block comment delimiter
     * @return a {@code String} containing the script lines
     * @throws IOException in case of I/O errors
     */
    public static String readScript(LineNumberReader lineNumberReader, String lineCommentPrefix,
                                    String separator, String blockCommentEndDelimiter) throws IOException {

        String currentStatement = lineNumberReader.readLine();
        StringBuilder scriptBuilder = new StringBuilder();
        while (currentStatement != null) {
            if ((blockCommentEndDelimiter != null && currentStatement
                    .contains(blockCommentEndDelimiter)) || (lineCommentPrefix != null
                    && !currentStatement.startsWith(lineCommentPrefix))) {
                if (scriptBuilder.length() > 0) {
                    scriptBuilder.append('\n');
                }
                scriptBuilder.append(currentStatement);
            }
            currentStatement = lineNumberReader.readLine();
        }
        appendSeparatorToScriptIfNecessary(scriptBuilder, separator);
        return scriptBuilder.toString();
    }

    private static void appendSeparatorToScriptIfNecessary(StringBuilder scriptBuilder,
                                                           String separator) {
        if (separator == null) {
            return;
        }
        String trimmed = separator.trim();
        if (trimmed.length() == separator.length()) {
            return;
        }
        // separator ends in whitespace, so we might want to see if the script is trying
        // to end the same way
        if (scriptBuilder.lastIndexOf(trimmed) == scriptBuilder.length() - trimmed.length()) {
            scriptBuilder.append(separator.substring(trimmed.length()));
        }
    }

    /**
     * Split an SQL script into separate statements delimited by the provided
     * separator string. Each individual statement will be added to the provided
     * {@code List}.
     * <p>Within the script, the provided {@code commentPrefix} will be honored:
     * any text beginning with the comment prefix and extending to the end of the
     * line will be omitted from the output. Similarly, the provided
     * {@code blockCommentStartDelimiter} and {@code blockCommentEndDelimiter}
     * delimiters will be honored: any text enclosed in a block comment will be
     * omitted from the output. In addition, multiple adjacent whitespace characters
     * will be collapsed into a single space.
     * @param script the SQL script
     * @param separator text separating each statement
     * (typically a ';' or newline character)
     * @return statements the list that will contain the individual statements
     */
    static List<String> splitSqlScript(String script, String separator) {
        List<String> statements = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        boolean inEscape = false;

        for (int i = 0; i < script.length(); i++) {
            char c = script.charAt(i);
            if (inEscape) {
                inEscape = false;
                sb.append(c);
                continue;
            }
            // MySQL style escapes
            if (c == '\\') {
                inEscape = true;
                sb.append(c);
                continue;
            }
            if (!inDoubleQuote && (c == '\'')) {
                inSingleQuote = !inSingleQuote;
            } else if (!inSingleQuote && (c == '"')) {
                inDoubleQuote = !inDoubleQuote;
            }
            if (!inSingleQuote && !inDoubleQuote) {
                if (script.startsWith(separator, i)) {
                    // We've reached the end of the current statement
                    if (sb.length() > 0) {
                        statements.add(sb.toString());
                        sb = new StringBuilder();
                    }
                    i += separator.length() - 1;
                    continue;
                } else if (script.startsWith(commentPrefix, i)) {
                    // Skip over any content from the start of the comment to the EOL
                    int indexOfNextNewline = script.indexOf('\n', i);
                    if (indexOfNextNewline > i) {
                        i = indexOfNextNewline;
                        continue;
                    } else {
                        // If there's no EOL, we must be at the end of the script, so stop here.
                        break;
                    }
                } else if (script.startsWith(blockCommentStartDelimiter, i)) {
                    // Skip over any block comments
                    int indexOfCommentEnd = script.indexOf(blockCommentEndDelimiter, i);
                    if (indexOfCommentEnd > i) {
                        i = indexOfCommentEnd + blockCommentEndDelimiter.length() - 1;
                        continue;
                    } else {
                        throw new RuntimeException(
                                "Missing block comment end delimiter: " + blockCommentEndDelimiter
                                /*+ ", file: " + resource.getAbsolutePath()*/);
                    }
                } else if (c == ' ' || c == '\r' || c == '\n' || c == '\t') {
                    // Avoid multiple adjacent whitespace characters
                    if (sb.length() > 0 && sb.charAt(sb.length() - 1) != ' ') {
                        c = ' ';
                    } else {
                        continue;
                    }
                } else if (script.startsWith("begin", i)) { // 兼容 DaMeng的 "begin end" 语法
                    int indexOfBlockEnd = script.indexOf("end".concat(separator), i);
                    String block = script.substring(i, indexOfBlockEnd + separator.length() + 3);
                    statements.add(block);
                    sb = new StringBuilder();
                    i += block.length() - 1;
                    continue;
                }
            }
            sb.append(c);
        }

        String s = sb.toString();
        if (StringUtils.isNotBlank(s)) {
            statements.add(s);
        }
        return statements;
    }
}
