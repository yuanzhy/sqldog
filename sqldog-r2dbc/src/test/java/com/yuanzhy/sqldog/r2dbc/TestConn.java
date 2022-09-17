package com.yuanzhy.sqldog.r2dbc;

import java.io.File;

import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Result;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Mono;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/9/13
 */
public class TestConn {

    private static final String FILE_PATH = "D:/test";
    private static final String URL = "r2dbc:sqldog://file?path="+FILE_PATH+"&schema=public";

    private Publisher<? extends Connection> connectionPublisher;

    @Before
    public void before() {
        ConnectionFactory connectionFactory = ConnectionFactories.get(URL);
        connectionPublisher = connectionFactory.create();
    }
    @Test
    public void create() {
        Mono.from(connectionPublisher).subscribe(connection -> {
            connection.createStatement("create table test(id int primary key, name varchar(10))").execute().subscribe(new BaseSubscriber<Result>() {
                @Override
                protected void hookOnSubscribe(Subscription subscription) {
                    super.hookOnSubscribe(subscription);
                }

                @Override
                protected void hookOnComplete() {
                    System.out.println("hookOnComplete");
                }

                @Override
                protected void hookOnNext(Result result) {
                    result.getRowsUpdated().subscribe(new BaseSubscriber<Object>() {
                        @Override
                        protected void hookOnNext(Object value) {
                            System.out.println(value);
                        }

                        @Override
                        protected void hookOnComplete() {
                            super.hookOnComplete();
                            System.exit(0);
                        }
                    });
                }
            });
        });
        await();
    }

    @Test
    public void insert() {
        Mono.from(connectionPublisher).subscribe(connection -> {
            connection.createBatch()
                    .add("insert into test values(1, 'aa')")
                    .add("insert into test values(2, 'bb')")
                    .add("insert into test values(3, 'cc')")
                    .execute().subscribe(new BaseSubscriber<Result>() {
                @Override
                protected void hookOnSubscribe(Subscription subscription) {
                    super.hookOnSubscribe(subscription);
                }

                @Override
                protected void hookOnComplete() {
                    System.out.println("hookOnComplete");
                }

                @Override
                protected void hookOnNext(Result result) {
                    result.getRowsUpdated().subscribe(new BaseSubscriber<Object>() {
                        @Override
                        protected void hookOnNext(Object value) {
                            System.out.println(value);
                        }

                        @Override
                        protected void hookOnComplete() {
                            super.hookOnComplete();
                            System.exit(0);
                        }
                    });
                }
            });
        });
        await();
    }

    @Test
    public void simpleQuery() {
        Mono.from(connectionPublisher).subscribe(connection -> {
            connection.createStatement("select * from test").execute().subscribe(new BaseSubscriber<Result>() {
                @Override
                protected void hookOnSubscribe(Subscription subscription) {
                    super.hookOnSubscribe(subscription);
                }

                @Override
                protected void hookOnComplete() {
                    System.out.println("hookOnComplete");
                }

                @Override
                protected void hookOnNext(Result result) {
                    result.map((row, rowMetadata) -> {
                        Object o1 = row.get(0);
                        Object o2 = row.get(1);
                        return o1 + ": " + o2;
                    }).subscribe(new BaseSubscriber<Object>() {
                        @Override
                        protected void hookOnNext(Object value) {
                            super.hookOnNext(value);
                            System.out.println(value);
                        }

                        @Override
                        protected void hookOnComplete() {
                            super.hookOnComplete();
                            System.exit(0);
                        }
                    });
                }
            });
        });
        await();
    }

    @Test
    public void prepareQuery() {
        Mono.from(connectionPublisher).subscribe(connection -> {
            connection.createStatement("select * from test where id > ?")
                    .bind(0, 1)
                    .execute().subscribe(new BaseSubscriber<Result>() {
                @Override
                protected void hookOnSubscribe(Subscription subscription) {
                    super.hookOnSubscribe(subscription);
                }

                @Override
                protected void hookOnComplete() {
                    System.out.println("hookOnComplete");
                }

                @Override
                protected void hookOnNext(Result result) {
                    result.map((row, rowMetadata) -> {
                        Object o1 = row.get(0);
                        Object o2 = row.get(1);
                        return o1 + ": " + o2;
                    }).subscribe(new BaseSubscriber<Object>() {
                        @Override
                        protected void hookOnNext(Object value) {
                            super.hookOnNext(value);
                            System.out.println(value);
                        }

                        @Override
                        protected void hookOnComplete() {
                            super.hookOnComplete();
                            System.exit(0);
                        }
                    });
                }
            });
        });
        await();
    }

    @Test
    public void after() {
        new File(FILE_PATH).delete();
    }

    private void await() {
        try {
            Thread.currentThread().join(3500);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
