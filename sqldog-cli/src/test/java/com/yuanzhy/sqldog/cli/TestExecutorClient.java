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
        executor.execute("create schema test");
        executor.execute("use test");
        executor.execute("create table tt (id int primary key, name varchar(20), birth date)");
    }

    @Test
    public void run() throws Exception {
//        executor.execute("insert into test.tt values(2, 'cc', '2021-11-21')");
//        Response response = executor.execute("select * from test.tt");
        Response response = executor.prepare("insert into test.tt values(?,?,?)");
        System.out.println(response);
    }

}
