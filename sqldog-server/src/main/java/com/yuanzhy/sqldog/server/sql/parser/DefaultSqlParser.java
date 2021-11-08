package com.yuanzhy.sqldog.server.sql.parser;

import org.apache.commons.lang3.StringUtils;

import com.yuanzhy.sqldog.server.core.SqlCommand;
import com.yuanzhy.sqldog.server.core.SqlParser;
import com.yuanzhy.sqldog.server.core.constant.Consts;
import com.yuanzhy.sqldog.server.sql.command.AlterTableCommand;
import com.yuanzhy.sqldog.server.sql.command.CommentCommand;
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

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/31
 */
public class DefaultSqlParser implements SqlParser {

    @Override
    public SqlCommand parse(String sql) {
        sql = this.upperCaseIgnoreValue(sql.trim());
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1);
        }
        sql = sql.replaceAll("[\n\r\t]", " ").replaceAll("\\s+", " ").trim();
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
        } else if (sql.startsWith("SELECT")) {
            return new SelectCommand(sql);
        }
        throw new UnsupportedOperationException("operation not supported: " + sql);
    }

    private String upperCaseIgnoreValue(String str) {
        StringBuilder sb = new StringBuilder();
        boolean valueToken = false;
        boolean escape = false;
        for (char c : str.toCharArray()) {
            if (c == Consts.SQL_ESCAPE) {
                escape = true;
                sb.append(c);
                continue;
            }
            if (escape) {
                sb.append(Character.toUpperCase(c));
                escape = false;
            } else {
                if (c == Consts.SQL_QUOTES) {
                    valueToken = !valueToken;
                    sb.append(c);
                } else if (valueToken) {
                    sb.append(c);
                } else {
                    sb.append(Character.toUpperCase(c));
                }
            }
        }
        return sb.toString();
    }
}