package com.yuanzhy.sqldog.r2dbc;

import java.rmi.RemoteException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import com.yuanzhy.sqldog.core.constant.RequestType;
import com.yuanzhy.sqldog.core.service.Request;
import com.yuanzhy.sqldog.core.service.Response;
import com.yuanzhy.sqldog.core.service.impl.RequestBuilder;
import com.yuanzhy.sqldog.core.util.Asserts;

import io.r2dbc.spi.R2dbcBadGrammarException;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/09/10
 */
class SqldogStatement implements Statement {
    private final SqldogConnection connection;
    private final ParsedSql parsedSql;
    private final ArrayDeque<Binding> bindings;
    int fetchSize = 200;
    private String preparedId;
    private String[] generatedColumns;
    SqldogStatement(SqldogConnection sqldogConnection, String sql) {
        Asserts.hasText(sql, "sql must not be null");
        this.connection = sqldogConnection;
        this.parsedSql = SqldogSqlParser.parse(sql);
        this.bindings = new ArrayDeque<>();
    }

    @Override
    public Statement add() {
        Binding binding = this.bindings.peekLast();
        if (binding != null) {
            binding.validate();
        }
        this.bindings.add(new Binding(this.parsedSql.getParameterCount()));
        return this;
    }

    @Override
    public Statement bind(String identifier, Object value) {
        return bind(getIdentifierIndex(identifier), value);
    }

    @Override
    public Statement bind(int index, Object value) {
        Asserts.notNull(value, "value must not be null");
        getCurrentOrFirstBinding().add(index, value);
        return this;
    }

    @Override
    public Statement bindNull(String identifier, Class<?> type) {
        return bindNull(getIdentifierIndex(identifier), type);
    }

    @Override
    public Statement bindNull(int index, Class<?> type) {
        Asserts.notNull(type, "type must not be null");

        if (index >= this.parsedSql.getParameterCount()) {
            throw new UnsupportedOperationException(String.format("Cannot bind parameter %d, statement has %d parameters", index, this.parsedSql.getParameterCount()));
        }

        getCurrentOrFirstBinding().add(index, null);
        return this;
    }

    private Binding getCurrentOrFirstBinding() {
        Binding binding = this.bindings.peekLast();
        if (binding == null) {
            Binding newBinding = new Binding(this.parsedSql.getParameterCount());
            this.bindings.add(newBinding);
            return newBinding;
        } else {
            return binding;
        }
    }

    @Override
    public Flux<Result> execute() {
        final String sql = this.parsedSql.getSql();
        if (this.parsedSql.getParameterCount() != 0) {
            // Extended query protocol
            if (this.bindings.size() == 0) {
                throw new IllegalStateException("No parameters have been bound");
            }
            if (this.preparedId == null) {
                this.preparedId = UUID.randomUUID().toString();
            }
            this.bindings.forEach(Binding::validate);
            return Flux.defer(() -> {
                try {
                    prepare(sql);
                } catch (R2dbcException e) {
                    return Flux.error(e);
                }
                // possible optimization: fetch all when statement is already prepared or first statement to be prepared
                if (this.bindings.size() == 1) {
                    Binding binding = this.bindings.peekFirst();
                    try {
                        Response response = executePrepared(sql, binding);
                        return Flux.just(new SqldogResult(response));
                    } catch (R2dbcException e) {
                        return Flux.error(e);
                    }
                }

                Iterator<Binding> iterator = this.bindings.iterator();
                Sinks.Many<Binding> bindings = Sinks.many().unicast().onBackpressureBuffer();
                AtomicBoolean canceled = new AtomicBoolean();
                return bindings.asFlux()
                        .map(it -> {
                            return new SqldogResult(executePrepared(sql, it));
//                            Flux<Response> messages =
//                                    collectBindingParameters(it).flatMapMany(values -> ExtendedFlowDelegate.runQuery(this.resources, factory, sql, it, values, this.fetchSize)).doOnComplete(() -> tryNextBinding(iterator, bindings, canceled));
//                            return PostgresqlResult.toResult(this.resources, messages, factory);
                        })
                        .doOnCancel(() -> clearBindings(iterator, canceled))
                        .doOnError(e -> clearBindings(iterator, canceled))
                        .doOnNext(n -> tryNextBinding(iterator, bindings, canceled))
                        .doOnSubscribe(it -> bindings.emitNext(iterator.next(), Sinks.EmitFailureHandler.FAIL_FAST));
            }).subscribeOn(Schedulers.boundedElastic()).cast(Result.class);
        }

        // Simple Query protocol, 支持分号分割多条sql
        return Flux.defer(() -> {
            try {
                Response response = executeSimple(sql);
                return Flux.just(new SqldogResult(response));
            } catch (R2dbcException e) {
                return Flux.error(e);
            }
        }).subscribeOn(Schedulers.boundedElastic()).cast(Result.class);

//        Flux<Result> exchange;
//        if (this.fetchSize > 0) {
//
//            exchange = ExtendedFlowDelegate.runQuery(this.resources, factory, sql, Binding.EMPTY, Collections.emptyList(), this.fetchSize);
//        } else {
//            exchange = SimpleQueryMessageFlow.exchange(this.resources.getClient(), sql);
//        }
//
//        return exchange.windowUntil(WINDOW_UNTIL)
//                .doOnDiscard(ReferenceCounted.class, ReferenceCountUtil::release) // ensure release of rows within WindowPredicate
//                .map(messages -> PostgresqlResult.toResult(this.resources, messages, factory))
//                .as(Operators::discardOnCancel);
    }

    @Override
    public Statement returnGeneratedValues(String... columns) {
        Asserts.notNull(columns, "columns must not be null");

//        if (this.parsedSql.hasDefaultTokenValue("RETURNING")) {
//            throw new IllegalStateException("Statement already includes RETURNING clause");
//        }

        if (!this.parsedSql.hasDefaultTokenValue("DELETE", "INSERT", "UPDATE")) {
            throw new IllegalStateException("Statement is not a DELETE, INSERT, or UPDATE command");
        }

        this.generatedColumns = columns;
        return this;
    }

    @Override
    public Statement fetchSize(int rows) {
        Asserts.gteZero(rows, "fetch size must be greater or equal zero");
        this.fetchSize = rows;
        return this;
    }

    @Override
    public String toString() {
        return "Statement{" +
                "bindings=" + this.bindings +
                ", sql='" + this.parsedSql.getSql() + '\'' +
                ", generatedColumns=" + Arrays.toString(this.generatedColumns) +
                '}';
    }

    Binding getCurrentBinding() {
        return getCurrentOrFirstBinding();
    }

    private int getIdentifierIndex(String identifier) {
        Asserts.hasText(identifier, "identifier must not be null");
        if (!identifier.startsWith("$")) {
            throw new NoSuchElementException(String.format("\"%s\" is not a valid identifier", identifier));
        }
        try {
            return Integer.parseInt(identifier.substring(1)) - 1;
        } catch (NumberFormatException e) {
            throw new NoSuchElementException(String.format("\"%s\" is not a valid identifier", identifier));
        }
    }

    private Response prepare(final String sql) throws R2dbcException {
        Request queryRequest = new RequestBuilder(RequestType.PREPARED_QUERY).schema(connection.schema)
                .timeout((int)connection.statementTimeout.getSeconds()).fetchSize(fetchSize)
                .preparedId(preparedId).sqls(sql).returnValues(generatedColumns)
                .buildPrepared();
        // lou bi 写法，全同步
        return executeInternal(queryRequest);
    }

    private Response executePrepared(String sql, Binding binding) throws R2dbcException {
        List<Object> parameterList = binding.getParameterValues();
        Request paramRequest = new RequestBuilder(RequestType.PREPARED_PARAMETER).schema(connection.schema)
                .timeout((int)connection.statementTimeout.getSeconds()).fetchSize(fetchSize)//.offset(offset)
                .preparedId(preparedId).sqls(sql).returnValues(generatedColumns)
                .parameters(parameterList.toArray())
                .buildPrepared();
        return executeInternal(paramRequest);
    }

    private Response executeSimple(String sql) {
        String[] sqls = Util.splitSqlScript(sql, ";").toArray(new String[0]);
        Request request = new RequestBuilder(RequestType.SIMPLE_QUERY).schema(connection.schema)
                .timeout((int)connection.statementTimeout.getSeconds()).fetchSize(fetchSize)//.offset(offset)
                .sqls(sqls).returnValues(generatedColumns)
                .build();
        return executeInternal(request);
    }

    private Response executeInternal(Request request) {
        try {
            Response response = connection.executor.execute(request);
            if (!response.isSuccess()) {
                throw new R2dbcBadGrammarException(response.getMessage());
            }
            return response;
        } catch (RemoteException e) {
            throw SQLError.wrapEx(e);
        }
    }

    private static void tryNextBinding(Iterator<Binding> iterator, Sinks.Many<Binding> bindingSink, AtomicBoolean canceled) {

        if (canceled.get()) {
            return;
        }

        try {
            if (iterator.hasNext()) {
                bindingSink.emitNext(iterator.next(), Sinks.EmitFailureHandler.FAIL_FAST);
            } else {
                bindingSink.emitComplete(Sinks.EmitFailureHandler.FAIL_FAST);
            }
        } catch (Exception e) {
            bindingSink.emitError(e, Sinks.EmitFailureHandler.FAIL_FAST);
        }
    }

    private void clearBindings(Iterator<Binding> iterator, AtomicBoolean canceled) {

        canceled.set(true);

        while (iterator.hasNext()) {
            // exhaust iterator, ignore returned elements
            iterator.next();
        }

        this.bindings.forEach(Binding::clear);
    }
}
