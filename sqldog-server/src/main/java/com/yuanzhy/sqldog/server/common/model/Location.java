package com.yuanzhy.sqldog.server.common.model;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/4/17
 */
public class Location {

    private final short offset;

    private final short length;

    public Location(short start, short length) {
        this.offset = start;
        this.length = length;
    }

    public short getOffset() {
        return offset;
    }

    public short getLength() {
        return length;
    }
}
