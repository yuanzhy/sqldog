package com.yuanzhy.sqldog.server.util;

import com.yuanzhy.sqldog.core.service.Request;
import com.yuanzhy.sqldog.core.util.Asserts;
import com.yuanzhy.sqldog.core.util.StringUtils;
import com.yuanzhy.sqldog.server.common.StorageConst;
import com.yuanzhy.sqldog.server.core.Schema;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/5/8
 */
public final class RequestHolder {

    private static final ThreadLocal<Request> REQ_TL = new ThreadLocal();

    public static Request currRequest() {
        Request req = REQ_TL.get();
        Asserts.notNull(req, "current request is null");
        return req;
    }

    public static void currRequest(Request request) {
        if (request == null) {
            REQ_TL.remove();
        } else {
            if (StringUtils.isNotEmpty(request.getSchema())) {
                Schema schema = Databases.getDatabase(StorageConst.DEF_DATABASE_NAME).getSchema(request.getSchema());
                Asserts.notNull(schema, "schema '" + request.getSchema() + "' not exists");
            }
            REQ_TL.set(request);
        }
    }
}
