## sqldog：一款java开发的数据库

### 目前实现的功能
- 内存模式
- 磁盘模式
- DML SQL（simple）
- DDL SQL
- 命令行
- 单表查询
- 联表查询
- 聚合
- JDBC
- 常用pg函数

### 不支持功能
- 索引
- 外键约束
- 检查约束
- 事务

### 近期计划 2022-04
- 索引
- insert判重
- update + delete
- 查询优化 - calcite接口
- CLI重构为JDBC
- 协议重构，Java RMI -> PG
- 写入缓存（提升批量DML性能）
