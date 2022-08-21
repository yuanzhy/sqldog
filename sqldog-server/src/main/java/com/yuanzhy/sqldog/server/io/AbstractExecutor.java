package com.yuanzhy.sqldog.server.io;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yuanzhy.sqldog.core.SqldogVersion;
import com.yuanzhy.sqldog.core.constant.RequestType;
import com.yuanzhy.sqldog.core.service.Executor;
import com.yuanzhy.sqldog.core.service.PreparedRequest;
import com.yuanzhy.sqldog.core.service.Request;
import com.yuanzhy.sqldog.core.service.Response;
import com.yuanzhy.sqldog.core.service.impl.ResponseImpl;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.core.util.StringUtils;
import com.yuanzhy.sqldog.server.sql.PreparedSqlCommand;
import com.yuanzhy.sqldog.server.sql.SqlCommand;
import com.yuanzhy.sqldog.server.sql.SqlParser;
import com.yuanzhy.sqldog.server.sql.parser.DefaultSqlParser;
import com.yuanzhy.sqldog.server.sql.parser.PreparedSqlParser;
import com.yuanzhy.sqldog.server.util.LRUCache;
import com.yuanzhy.sqldog.server.util.RequestHolder;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/8/20
 */
class AbstractExecutor implements Executor {

    protected static final Logger log = LoggerFactory.getLogger(AbstractExecutor.class);

    protected final static SqlParser sqlParser = new DefaultSqlParser();
    protected final static PreparedSqlParser preparedSqlParser = new PreparedSqlParser();

    protected final int serialNum;
    protected final String version;
    protected final Map<String, PreparedSqlCommand> preparedSqlCache = new LRUCache<>(20);
    //        private String currentSchema = StorageConst.DEF_SCHEMA_NAME;
    protected long lastRequest = System.currentTimeMillis();
    AbstractExecutor(int serialNum) {
        this.serialNum = serialNum;
        String version = SqldogVersion.getVersion();
        this.version = version == null ? "1.0.0" : version;
    }

    @Override
    public String getVersion() {
        return this.version;
    }

    @Override
    public Response execute(Request request) throws RemoteException {
        RequestHolder.currRequest(request);
        lastRequest = System.currentTimeMillis();
        if (request.getType() == RequestType.SIMPLE_QUERY) {
            return this.simpleQuery(request);
        } else if (request.getType() == RequestType.PREPARED_QUERY) {
            return this.prepared((PreparedRequest)request);
        } else if (request.getType() == RequestType.PREPARED_PARAMETER) {
            return this.preparedQuery((PreparedRequest)request);
        }
        throw new IllegalArgumentException("Request Type Not Supported: " + request.getType());
    }

    private Response simpleQuery(Request request) {
        try {
            String[] sqls = request.getSql();
            List<SqlResult> results = new ArrayList<>();
            for (int i = 0; i < sqls.length; i++) {
                String[] arr = sqls[i].split("(;\\s+\n)");
                for (String sql : arr) {
                    if (StringUtils.isBlank(sql)) {
                        continue;
                    }
                    SqlCommand sqlCommand = sqlParser.parse(sql);
                    sqlCommand.defaultSchema(request.getSchema());
                    SqlResult result = sqlCommand.execute();
//                        if (sqlCommand instanceof SetCommand && result.getSchema() != null) {
//                            this.currentSchema = result.getSchema();
//                        }
                    results.add(result);
                }
            }
            return new ResponseImpl(true, results.toArray(new SqlResult[0]));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return new ResponseImpl(false, e.getMessage());
        }
    }

    private Response prepared(PreparedRequest request) {
        String preparedSql = request.getSql()[0];
        String preparedId = request.getPrepareId();
        try {
            PreparedSqlCommand sqlCommand = preparedSqlCache.computeIfAbsent(preparedId, key -> preparedSqlParser.parse(preparedSql));
            sqlCommand.defaultSchema(request.getSchema());
            SqlResult result = sqlCommand.execute();
            return new ResponseImpl(true, result);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return new ResponseImpl(false, e.getMessage());
        }
    }

    private Response preparedQuery(PreparedRequest request) {
        Object[][] parameters = request.getParameters();
        try {
            PreparedSqlCommand sqlCommand = preparedSqlCache.computeIfAbsent(request.getPrepareId(), key -> preparedSqlParser.parse(request.getSql()[0]));
            sqlCommand.defaultSchema(request.getSchema());
            SqlResult[] results = new SqlResult[parameters.length];
            for (int i = 0; i < parameters.length; i++) {
                results[i] = sqlCommand.execute(parameters[i]);
            }
            return new ResponseImpl(true, results);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return new ResponseImpl(false, e.getMessage());
        }
    }

    @Override
    public void close() {
        for (PreparedSqlCommand sqlCommand : preparedSqlCache.values()) {
            try {
                sqlCommand.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        preparedSqlCache.clear();
    }
}
