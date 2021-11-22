package com.yuanzhy.sqldog.cli;

import com.yuanzhy.sqldog.core.constant.Consts;
import com.yuanzhy.sqldog.core.rmi.Executor;
import com.yuanzhy.sqldog.core.rmi.RMIServer;
import com.yuanzhy.sqldog.core.rmi.Response;
import org.junit.Before;
import org.junit.Test;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/21
 */
public class TestExecutorClient {

    private static final String H = "127.0.0.1";
    private static final int P = 2345;
    private static final String U = "root";
    private static final String PW = "123456";
    private Executor executor;
    @Before
    public void before() throws Exception {
        Registry registry = LocateRegistry.getRegistry(H, P);
        RMIServer rmiServer = (RMIServer) registry.lookup(Consts.SERVER_NAME);
        executor = rmiServer.connect(U, PW);
        System.out.println("Welcome to sqldog " + executor.getVersion());
        try {
            executor.execute("create schema test");
            executor.execute("create table test.tt (id int primary key, name varchar(20), birth date)");
        } catch (Exception e) {
            // ignore
        }
    }

    @Test
    public void dml() throws Exception {
        executor.execute("use test");
//        executor.execute("insert into test.tt values(2, 'cc', '2021-11-21')");
//        Response response = executor.execute("select * from test.tt");
        String sql = "insert into test.tt values(?,?,?)";
        Response response = executor.prepare(sql);
        executor.executePrepared(sql, new Object[]{1, "lisi", null});
        //executor.execute("insert into test.tt values(1,'2',null)");
        System.out.println(response);
    }

    @Test
    public void dql() throws Exception {
        executor.execute("use test");
        String sql = "select * from test.tt where id=? and name = ? and birth = ?";
        Response response = executor.prepare(sql);
        executor.executePrepared(sql, new Object[]{1, "lisi", "2021-11-21"});
        System.out.println(response);
    }

}
