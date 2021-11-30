package com.yuanzhy.sqldog.server.sql.function.agg;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/28
 */
public class StringAggFunction implements AggFunction<StringAggFunction> {

    private final List<String> values = new ArrayList<>();
    private String delimiter = "";

    @Override
    public StringAggFunction init() {
        StringAggFunction fn = new StringAggFunction();
        return fn;
    }

    public StringAggFunction add(StringAggFunction fn, String val, String delimiter) {
        this.delimiter = delimiter;
        values.add(val);
        return fn;
    }

    @Override
    public String result(StringAggFunction fn) {
        return values.stream().collect(Collectors.joining(delimiter));
    }
}
