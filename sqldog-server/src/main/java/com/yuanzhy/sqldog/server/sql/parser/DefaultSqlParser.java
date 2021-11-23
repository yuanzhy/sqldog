package com.yuanzhy.sqldog.server.sql.parser;

import com.yuanzhy.sqldog.server.sql.SqlCommand;
import com.yuanzhy.sqldog.server.sql.SqlParser;
import com.yuanzhy.sqldog.server.sql.command.AlterTableCommand;
import com.yuanzhy.sqldog.server.sql.command.CommentCommand;
import com.yuanzhy.sqldog.server.sql.command.CommitCommand;
import com.yuanzhy.sqldog.server.sql.command.CreateSchemaCommand;
import com.yuanzhy.sqldog.server.sql.command.CreateTableCommand;
import com.yuanzhy.sqldog.server.sql.command.DeleteCommand;
import com.yuanzhy.sqldog.server.sql.command.DescCommand;
import com.yuanzhy.sqldog.server.sql.command.DropSchemaCommand;
import com.yuanzhy.sqldog.server.sql.command.DropTableCommand;
import com.yuanzhy.sqldog.server.sql.command.InsertCommand;
import com.yuanzhy.sqldog.server.sql.command.SelectCommand;
import com.yuanzhy.sqldog.server.sql.command.SetCommand;
import com.yuanzhy.sqldog.server.sql.command.ShowCommand;
import com.yuanzhy.sqldog.server.sql.command.TruncateTableCommand;
import com.yuanzhy.sqldog.server.sql.command.UpdateCommand;
import org.apache.commons.lang3.StringUtils;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/31
 */
public class DefaultSqlParser implements SqlParser {

    @Override
    public SqlCommand parse(String rawSql) {
        String sql = pre(rawSql);
        String tmp = StringUtils.substringAfter(sql, " ").trim();
        if (sql.startsWith("SHOW")) {
            return new ShowCommand(sql);
        } else if (StringUtils.startsWithAny(sql, "SET", "USE")) { // SET search_path TO my_schema;   use my_schema;
            return new SetCommand(sql);
        } else if (StringUtils.startsWithAny(sql, "\\D", "DESC")) { // \d table_name;
            return new DescCommand(sql);
        } else if (sql.startsWith("CREATE")) {
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
        } else if (sql.startsWith("COMMENT")) {
            return new CommentCommand(sql);
        } else if (sql.startsWith("INSERT")) {
            return new InsertCommand(sql);
        } else if (sql.startsWith("UPDATE")) {
            return new UpdateCommand(sql);
        } else if (sql.startsWith("DELETE")) {
            return new DeleteCommand(sql);
        } else if (sql.startsWith("SELECT") /*|| sql.startsWith("WITH RECURSIVE")*/) {
            return new SelectCommand(sql);
        } else if (sql.equals("COMMIT")) {
            return new CommitCommand(sql);
        }
        throw new UnsupportedOperationException("operation not supported: " + sql);
    }
}
