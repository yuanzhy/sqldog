package com.yuanzhy.sqldog.r2dbc;

import java.util.Arrays;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import org.reactivestreams.Publisher;

import com.yuanzhy.sqldog.core.service.Response;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.core.util.Asserts;

import io.r2dbc.spi.R2dbcBadGrammarException;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/9/12
 */
class SqldogSegmentResult implements Result {

    private final Flux<Segment> segments;
    private SqldogRowMetadata rowMetadata;

    private SqldogSegmentResult(Flux<Segment> segments) {
        this.segments = segments;
    }

    SqldogSegmentResult(Response response) {
        Asserts.notNull(response, "response must not be null");
        if (rowMetadata == null) {
            rowMetadata = new SqldogRowMetadata(response.getResult().getColumns());
        }
        this.segments = Flux.create(sink -> {
            if (response.isSuccess()) {
                switch (response.getResult().getType()) {
                    case DML:
                        long rowCount = Arrays.stream(response.getResults()).mapToLong(SqlResult::getRows).sum();
                        sink.next(new SqldogUpdateCountSegment(rowCount));
                        break;
                    case DQL:
                        for (Object[] data : response.getResult().getData()) {
                            if (sink.isCancelled()) return;
                            sink.next(new SqldogRowSegment(new SqldogRow(rowMetadata, data)));
                        }
                        break;
                }
                sink.complete();
            } else {
                sink.next(new SqldogErrorSegment(response));
            }

        });
    }

    @Override
    public Mono<Long> getRowsUpdated() {
        return this.segments
                .<Integer>handle((segment, sink) -> {
                    if (segment instanceof SqldogErrorSegment) {
                        sink.error(((SqldogErrorSegment) segment).exception());
                        return;
                    }

                    if (segment instanceof UpdateCount) {
                        sink.next((int) (((UpdateCount) segment).value()));
                    }
                }).collectList().handle((list, sink) -> {
                    if (list.isEmpty()) {
                        return;
                    }
                    long sum = 0;
                    for (Integer integer : list) {
                        sum += integer;
                    }
                    sink.next(sum);
                });
    }

    @Override
    public <T> Flux<T> map(BiFunction<Row, RowMetadata, ? extends T> f) {
        Asserts.notNull(f, "f must not be null");
        return this.segments
                .handle((segment, sink) -> {
                    if (segment instanceof SqldogErrorSegment) {
                        sink.error(((SqldogErrorSegment) segment).exception());
                        return;
                    }

                    if (segment instanceof RowSegment) {
                        RowSegment row = (RowSegment) segment;
                        sink.next(f.apply(row.row(), row.row().getMetadata()));
                    }

                });
    }

    @Override
    public SqldogSegmentResult filter(Predicate<Segment> filter) {
        Asserts.notNull(filter, "filter must not be null");
        return new SqldogSegmentResult(this.segments.filter(it -> {
            boolean result = filter.test(it);
            return result;
        }));
    }

    @Override
    public <T> Publisher<T> flatMap(Function<Segment, ? extends Publisher<? extends T>> mappingFunction) {
        Asserts.notNull(mappingFunction, "mappingFunction must not be null");
        return this.segments.concatMap(segment -> {

            Publisher<? extends T> result = mappingFunction.apply(segment);

            if (result == null) {
                return Mono.error(new IllegalStateException("The mapper returned a null Publisher"));
            }

            // doAfterTerminate to not release resources before they had a chance to get emitted
            if (result instanceof Mono) {
                return result;
            }

            return Flux.from(result);
        });
    }


    @Override
    public String toString() {
        return "SqldogSegmentResult{" +
                "segments=" + this.segments +
                '}';
    }

    static SqldogSegmentResult toResult(Response response) {
        return new SqldogSegmentResult(response);
    }

    static class SqldogRowSegment implements Result.RowSegment {

        private final Row row;

        public SqldogRowSegment(Row row) {
            this.row = row;
        }

        @Override
        public Row row() {
            return this.row;
        }

    }

    static class SqldogUpdateCountSegment implements Result.UpdateCount {

        private final long value;

        public SqldogUpdateCountSegment(long value) {
            this.value = value;
        }

        @Override
        public long value() {
            return this.value;
        }

    }

    static class SqldogErrorSegment implements Result.Message {

        private final Response response;

        public SqldogErrorSegment(Response response) {
            this.response = response;
        }

        @Override
        public R2dbcException exception() {
            return new R2dbcBadGrammarException(response.getMessage());
        }

        @Override
        public int errorCode() {
            return 0;
        }

        @Override
        public String sqlState() {
            return response.getMessage();
        }

        @Override
        public String message() {
            return response.getMessage();
        }

    }
}
