package com.yuanzhy.sqldog.r2dbc;

import java.util.Arrays;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import org.reactivestreams.Publisher;

import com.yuanzhy.sqldog.core.service.Response;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.core.util.Asserts;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/9/10
 */
class SqldogResult implements Result {

    private final Response response;
    private SqldogRowMetadata rowMetadata;
    SqldogResult(Response response) {
        this.response = response;
    }

    @Override
    public Publisher<Long> getRowsUpdated() {
        return Mono.just(Arrays.stream(response.getResults()).mapToLong(SqlResult::getRows).sum());
    }

    @Override
    public <T> Publisher<T> map(BiFunction<Row, RowMetadata, ? extends T> mappingFunction) {
        Asserts.notNull(mappingFunction, "mappingFunction must not be null");
        if (this.rowMetadata == null) {
            this.rowMetadata = new SqldogRowMetadata(response.getResult().getColumns());
        }
        return Flux.create(sink -> {
            for (Object[] data : response.getResult().getData()) {
                if (sink.isCancelled()) return;
                sink.next(mappingFunction.apply(new SqldogRow(rowMetadata, data), this.rowMetadata));
            }
            // TODO 内部分页 offset
//            nextInternal(sink);
            sink.complete();
        });
    }

    private <T> void nextInternal(FluxSink<T> sink) {
    }

    @Override
    public Result filter(Predicate<Segment> filter) {
        Asserts.notNull(filter, "filter must not be null");
        return SqldogSegmentResult.toResult(response).filter(filter);
    }

    @Override
    public <T> Publisher<T> flatMap(Function<Segment, ? extends Publisher<? extends T>> mappingFunction) {
        Asserts.notNull(mappingFunction, "mappingFunction must not be null");
        return SqldogSegmentResult.toResult(response).flatMap(mappingFunction);
    }
}
