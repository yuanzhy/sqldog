package com.yuanzhy.sqldog.sql.parser;

import com.yuanzhy.sqldog.core.SqlCommand;
import com.yuanzhy.sqldog.core.SqlParser;
import com.yuanzhy.sqldog.sql.command.AlterTableCommand;
import com.yuanzhy.sqldog.sql.command.CreateSchemaCommand;
import com.yuanzhy.sqldog.sql.command.CreateTableCommand;
import com.yuanzhy.sqldog.sql.command.DeleteCommand;
import com.yuanzhy.sqldog.sql.command.DropSchemaCommand;
import com.yuanzhy.sqldog.sql.command.DropTableCommand;
import com.yuanzhy.sqldog.sql.command.InsertCommand;
import com.yuanzhy.sqldog.sql.command.SelectCommand;
import com.yuanzhy.sqldog.sql.command.TruncateTableCommand;
import com.yuanzhy.sqldog.sql.command.UpdateCommand;
import org.apache.commons.lang3.StringUtils;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/31
 */
public class DefaultSqlParser implements SqlParser {

    @Override
    public SqlCommand parse(String sql) {
        sql = sql.toUpperCase().trim().replaceAll("[\n\r\t]", " ").replaceAll("\\s", " ");
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1);
        }
        String tmp = StringUtils.substringAfter(sql, " ").trim();
        if (sql.startsWith("CREATE")) {
            if (tmp.startsWith("DATABASE")) {
                throw new UnsupportedOperationException("create database is unsupported");
            } else if (tmp.startsWith("SCHEMA")) {
                return new CreateSchemaCommand(sql);
            } else if (tmp.startsWith("TABLE")) {
                return new CreateTableCommand(sql);
            }
        } else if (sql.startsWith("ALTER")) {
            if (tmp.startsWith("TABLE")) {
                return new AlterTableCommand(sql);
            } else if (tmp.startsWith("SCHEMA")) {
                throw new UnsupportedOperationException("alter schema is unsupported");
            }
        } else if (sql.startsWith("DROP")) {
            if (tmp.startsWith("TABLE")) {
                return new DropTableCommand(sql);
            } else if (tmp.startsWith("SCHEMA")) {
                return new DropSchemaCommand(sql);
            }
        } else if (sql.startsWith("TRUNCATE")) {
            if (tmp.startsWith("TABLE")) {
                return new TruncateTableCommand(sql);
            }
        } else if (sql.startsWith("INSERT")) {
            return new InsertCommand(sql);
        } else if (sql.startsWith("UPDATE")) {
            return new UpdateCommand(sql);
        } else if (sql.startsWith("DELETE")) {
            return new DeleteCommand(sql);
        } else if (sql.startsWith("SELECT")) {
            return new SelectCommand(sql);
        }
        throw new UnsupportedOperationException("operation not supported: " + sql);
    }
}
