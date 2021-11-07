package com.yuanzhy.sqldog.server.memory;

import com.yuanzhy.sqldog.server.core.Base;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 *
 * @author yuanzhy
 * @date 2021-11-02
 */
public abstract class MemoryBase implements Base {

    protected final String name;
    protected String description;

    protected MemoryBase(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public void setDescription(String description) {
        this.description = description;
    }

    protected String joinByVLine(String... values) {
        return Arrays.stream(values).map(v -> " " + StringUtils.rightPad(StringUtils.trimToEmpty(v), 15)).collect(Collectors.joining("|"));
    }

    protected String genHLine(int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(StringUtils.repeat("-", 15));
            sb.append("-");
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }
}
