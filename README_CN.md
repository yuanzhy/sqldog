# sqldog

sqldog 是一款 Java 开发的简易关系型数据库，支持内存和硬盘存储模式，支持以下功能

* DML SQL（simple）
* DDL SQL
* CLI
* Simple Select
* Simple Agg

**注意**：仅用于开发和学习使用，切勿用于生产环境

## 安装

1. 安装JDK8+并配置环境变量
2. 将安装包解压到任意目录
3. 将 bin 目录配置到环境变量
4. 命令行中执行 dsql-server

## 使用说明

### 服务端

根据需要修改配置文件 server/config.properties，配置说明如下

- server.storage.mode：存储模式，可选项“disk”、“memory”
- server.storage.writeCache：是否开启写缓存，开启后可大幅提升写入速度，数据会延迟落盘，可靠性降低（仅disk模式生效）
- server.storage.path：数据存储路径，可配置相对路径或绝对路径，为相对路径则相对于sqldog安装目录（仅disk模式生效）
- server.storage.codec：元数据编码方式
- server.host：绑定IP
- server.port：绑定端口
- server.username：用户名
- server.password：密码
- server.max-connections：最大连接数

修改配置后启动服务
```shell
dql-server
```

### 命令行

- 查看帮助
```shell
dsql --help
```

- 连接本机
```shell
dsql -U [username] -p [password]
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
use [schema_name]
# or
set search_path to [schema_name]
```
- 列出当前模式所有表
```shell
show tables
```
- 查看表信息
```shell
desc [table_name]
# or
\d [table_name]
```
- 退出命令行
```shell
quit
# or
exit
# or
\q
```

### JDBC

- 将 jdbc jar 安装到本地仓库
```shell
mvn install:install-file -DgroupId=com.yuanzhy.sqldog -DartifactId=sqldog-core -Dversion=1.0-SNAPSHOT -Dpackaging=jar -Dfile=./sqldog/jdbc/sqldog-core-1.0-SNAPSHOT.jar
mvn install:install-file -DgroupId=com.yuanzhy.sqldog -DartifactId=sqldog-dialect -Dversion=1.0-SNAPSHOT -Dpackaging=jar -Dfile=./sqldog/jdbc/sqldog-dialect-1.0-SNAPSHOT.jar
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

## 维护者

[@yuanzhy](https://github.com/yuanzhy)

## 使用许可

[MIT](LICENSE) © yuanzhy