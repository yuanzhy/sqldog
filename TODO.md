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
- 事务
- 序列
- 外键约束
- 检查约束

### 近期计划 2022-06
- 索引 --- 还没怎么测, 只支持唯一值
- update + delete
- 查询优化 - calcite接口 
  - 部分实现, 分组和排序还需要fullScan
  1. filterableTable
  2. fetchSize
  3. 不带条件的count(*)
- 协议重构，Java RMI -> PG

#### 05-16
- ~~根据主键删除和更新~~
- translatable