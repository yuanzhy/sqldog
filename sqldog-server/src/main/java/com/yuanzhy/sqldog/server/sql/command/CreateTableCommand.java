package com.yuanzhy.sqldog.server.sql.command;

import com.yuanzhy.sqldog.core.constant.StatementType;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.server.core.constant.ConstraintType;
import com.yuanzhy.sqldog.server.core.constant.DataType;
import com.yuanzhy.sqldog.server.memory.ColumnBuilder;
import com.yuanzhy.sqldog.server.memory.ConstraintBuilder;
import com.yuanzhy.sqldog.server.memory.TableBuilder;
import com.yuanzhy.sqldog.server.sql.SqlResultBuilder;
import org.apache.commons.lang3.StringUtils;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/31
 */
public class CreateTableCommand extends AbstractSqlCommand {

    public CreateTableCommand(String sql) {
        super(sql);
    }
    @Override
    public SqlResult execute() {
        // create table schema.table_name()
        String sqlSuffix = sql.substring("CREATE TABLE ".length());
        super.parseSchema(sqlSuffix);
        final String tableName = parseTableName(sqlSuffix);
        TableBuilder tb = new TableBuilder();
        tb.name(tableName);
        String main = StringUtils.substringAfter(sqlSuffix, "(").trim();
        main = main.substring(0, main.length() - 1);
        for (String rawColInfo : main.split(",")) {
            rawColInfo = rawColInfo.trim();
            if (rawColInfo.startsWith("PRIMARY KEY")) {
                this.handlePK(tb, rawColInfo, null);
            } else if (rawColInfo.startsWith("UNIQUE")) {
                this.handleUK(tb, rawColInfo, null);
            } else if (rawColInfo.startsWith("CONSTRAINT ")) {
                String consSuffix = rawColInfo.substring("CONSTRAINT ".length());
                String consName = StringUtils.substringBefore(consSuffix, " ");
                if (consSuffix.contains("PRIMARY KEY")) {
                    this.handlePK(tb, consSuffix, consName);
                } else if (consSuffix.startsWith("UNIQUE")) {
                    this.handleUK(tb, consSuffix, consName);
                } else {
                    throw new UnsupportedOperationException(consName + " not supported");
                }
            } else {
                ColumnBuilder cb = this.handleCol(tb, rawColInfo);
                tb.addColumn(cb.build());
            }
        }
        schema.addTable(tb.build());
        return new SqlResultBuilder(StatementType.DDL).schema(schema.getName()).table(tableName).build();
    }

    private ColumnBuilder handleCol(TableBuilder tb, String rawColInfo) {
        String colName = StringUtils.substringBefore(rawColInfo, " ");
        String dataTypeBegin = StringUtils.substringAfter(rawColInfo, " ");
        final String rawDataType = StringUtils.substringBefore(dataTypeBegin, " ").trim();
        DataType dataType = DataType.of(rawDataType);
        ColumnBuilder cb = new ColumnBuilder();
        cb.name(colName).dataType(dataType);
        if (dataType.isHasLength()) {
            super.parsePrecisionAndScale(rawDataType, cb);
        }
        if (rawColInfo.contains(" PRIMARY KEY")) {
            tb.addConstraint(new ConstraintBuilder().type(ConstraintType.PRIMARY_KEY).addColumnName(colName).build());
            cb.nullable(false);
            rawColInfo = rawColInfo.replace(" PRIMARY KEY", "");
        } else if (rawColInfo.contains(" UNIQUE")) {
            tb.addConstraint(new ConstraintBuilder().type(ConstraintType.UNIQUE).addColumnName(colName).build());
            cb.nullable(false);
            rawColInfo = rawColInfo.replace(" UNIQUE", "");
        }
        if (rawColInfo.contains(" NOT NULL")) {
            cb.nullable(false);
            rawColInfo = rawColInfo.replace(" NOT NULL", "").trim();
        } else if (rawColInfo.contains(" null")) {
            rawColInfo = rawColInfo.replace(" NULL", "").trim();
        }
        if (rawColInfo.contains(" DEFAULT ")) {
            String rawDefault = StringUtils.substringAfter(rawColInfo, " DEFAULT ");
            rawDefault = StringUtils.substringBefore(rawDefault, " ").trim();
            cb.defaultValue(dataType.parseRawValue(rawDefault));
        }
        return cb;
    }

    private void handleUK(TableBuilder tb, String rawColInfo, String consName) {
        String consColName = StringUtils.substringBetween(rawColInfo, "(", ")").trim();
        ConstraintBuilder cb = new ConstraintBuilder().type(ConstraintType.UNIQUE);
        if (consColName.contains(",")) {
            for (String cn : consColName.split(",")) {
                cb.addColumnName(cn.trim());
            }
        } else {
            cb.addColumnName(consColName);
        }
        tb.addConstraint(cb.build());
    }

    private void handlePK(TableBuilder tb, String rawColInfo, String consName) {
        String consColName = StringUtils.substringBetween(rawColInfo, "(", ")").trim();
        if (consColName.contains(",")) {
            throw new UnsupportedOperationException("暂不支持联合主键");
        }
        tb.addConstraint(new ConstraintBuilder().name(consName).type(ConstraintType.PRIMARY_KEY).addColumnName(consColName).build());
    }
}
