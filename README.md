= Sqldog

Sqldog 是一款 Java 开发的简易数据库，支持内存和硬盘存储模式（硬盘存储暂未实现），支持以下功能


* DML SQL（simple）
* DDL SQL
* CLI
* Simple Select
* Simple Agg


[[get-started]]
== Get started

#### install
- 解压到任意目录
- 将 bin 配置到环境变量
- cmd 中执行 start-server
- 默认只有一个数据库实例 default
- 支持多模式，默认模式 public

#### 命令行
- dsql --help

- 连接本机：
```shell
dsql -U xx -p xx
```
- 列出所有模式
```shell
show schemas
```
- 查看当前所在模式
```shell
show search_path
```
- 切换模式
```shell
use sche_name
# or
set search_path to sche_name
```
- 列出当前模式所有表
```shell
show tables
```
- 查看表信息
```shell
desc table_name
# or
\d table_name
```
- 退出命令行
```shell
quit
# or
exit
# or
\q
```

#### jdbc

- 将 jdbc jar 安装到本地仓库
```shell
mvn install:install-file -DgroupId=com.yuanzhy.sqldog -DartifactId=sqldog-jdbc -Dversion=1.0-SNAPSHOT -Dpackaging=jar -Dfile=./sqldog/jdbc/sqldog-jdbc-1.0-SNAPSHOT.jar
```
- 引入 maven 依赖
```xml
<dependency>
    <groupId>com.yuanzhy.sqldog</groupId>
    <artifactId>sqldog-jdbc</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```
- 配置jdbc
```properties
url=jdbc:sqldog://127.0.0.1[:2345][/schema_name]
user=root
password=123456
driver-class=com.yuanzhy.sqldog.jdbc.Driver
```