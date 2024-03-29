package com.yuanzhy.sqldog.core.util;

import java.nio.charset.StandardCharsets;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/4/5
 */
public class ByteUtil {

    private static final char[] DIGITS_LOWER = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd',
            'e', 'f' };

    public static byte[] toBytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] toBytes(short s) {
        byte[] bytes = new byte[2];
        bytes[0] = (byte) ((s & 0xff00) >> 8);
        bytes[1] = (byte) (s & 0xff);
        return bytes;
    }

    public static byte[] toBytes(int n) {
        byte[] b = new byte[4];
        b[0] = (byte) (n >> 24 & 0xff);
        b[1] = (byte) (n >> 16 & 0xff);
        b[2] = (byte) (n >> 8 & 0xff);
        b[3] = (byte) (n & 0xff);
        return b;
    }

    public static byte[] toBytes(long n) {
        byte[] b = new byte[8];
        b[0] = (byte) (n >> 56 & 0xff);
        b[1] = (byte) (n >> 48 & 0xff);
        b[2] = (byte) (n >> 40 & 0xff);
        b[3] = (byte) (n >> 32 & 0xff);
        b[4] = (byte) (n >> 24 & 0xff);
        b[5] = (byte) (n >> 16 & 0xff);
        b[6] = (byte) (n >> 8 & 0xff);
        b[7] = (byte) (n & 0xff);
        return b;
    }

    public static byte[] toBytes(float f) {
        int intBits = Float.floatToIntBits(f);
        return toBytes(intBits);
    }

    public static byte[] toBytes(double d) {
        long longBits = Double.doubleToLongBits(d);
        return toBytes(longBits);
    }

    public static float toFloat(byte[] bytes) {
        return toFloat(bytes, 0);
    }
    
    public static float toFloat(byte[] bytes, int offset) {
        return Float.intBitsToFloat(toInt(bytes, offset));
    }

    public static double toDouble(byte[] bytes) {
        return toDouble(bytes, 0);
    }

    public static double toDouble(byte[] bytes, int offset) {
        return Double.longBitsToDouble(toLong(bytes, offset));
    }

    public static short toShort(byte[] bytes) {
        return toShort(bytes, 0);
    }

    public static short toShort(byte[] bytes, int offset) {
        return (short) ((0xff00 & bytes[offset++] << 8) | (0xff & (bytes[offset++])));
    }

    public static int toInt(byte[] bytes){
        return toInt(bytes, 0);
    }

    public static int toInt(byte[] bytes, int offset) {
        return (0xff000000 & (bytes[offset++] << 24)) | (0xff0000 & (bytes[offset++] << 16)) | (0xff00 & (bytes[offset++] << 8)) | (0xff & bytes[offset++]);
    }

    public static long toLong(byte[] bytes) {
        return toLong(bytes, 0);
    }
    
    public static long toLong(byte[] bytes, int offset) {
        return (0xff00000000000000L & ((long) bytes[offset++] << 56)) | (0xff000000000000L & ((long) bytes[offset++] << 48))
                | (0xff0000000000L & ((long) bytes[offset++] << 40)) | (0xff00000000L & ((long) bytes[offset++] << 32))
                | (0xff000000L & ((long) bytes[offset++] << 24)) | (0xff0000L & ((long) bytes[offset++] << 16))
                | (0xff00L & ((long) bytes[offset++] << 8)) | (0xffL & (long) bytes[offset++])
                ;
    }

    public static String toString(byte[] bytes) {
        return toString(bytes, 0, bytes.length);
    }

    public static String toString(byte[] bytes, int offset, int len) {
        return new String(bytes, offset, len, StandardCharsets.UTF_8);
    }

    public static String toHexString(byte[] bytes) {
        final int l = bytes.length;
        final char[] out = new char[l << 1];
        encodeHex(bytes, l, out);
        return new String(out);
    }

    private static void encodeHex(final byte[] data, final int dataLen, final char[] out) {
        for (int i = 0, j = 0; i < dataLen; i++) {
            out[j++] = DIGITS_LOWER[(0xF0 & data[i]) >>> 4];
            out[j++] = DIGITS_LOWER[0x0F & data[i]];
        }
    }
}
