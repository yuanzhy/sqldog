# sqldog

#### [English](README.md) | [中文](README_CN.md)

sqldog 是一款 Java 开发的简易关系型数据库，支持内存和硬盘存储模式，支持以下功能

* DML SQL（simple）
* DDL SQL
* SELECT
* CLI
* JPA (Hibernate) dialect, Mybatis PageHelper, SpringData-JDBC 自动适配

**注意**：仅用于开发和学习使用，切勿用于生产环境

## 安装

### 独立版
1. 安装JDK8+并配置环境变量
2. 将安装包解压到任意目录
3. 将 bin 目录配置到环境变量

## 使用说明

### 嵌入版
- 引入 JDBC 和 server 依赖
1. maven
```xml
<dependency>
    <groupId>com.yuanzhy.sqldog</groupId>
    <artifactId>sqldog-jdbc</artifactId>
    <version>0.1-SNAPSHOT</version>
</dependency>
<dependency>
    <groupId>com.yuanzhy.sqldog</groupId>
    <artifactId>sqldog-server</artifactId>
    <version>0.1-SNAPSHOT</version>
</dependency>
```
2. gradle
```groovy
dependencies {
    implementation "com.yuanzhy.sqldog:sqldog-jdbc:0.1-SNAPSHOT"
    implementation "com.yuanzhy.sqldog:sqldog-server:0.1-SNAPSHOT"
}
```

- 配置jdbc
1. 内存模式（数据存储在jvm内存中）
```properties
url=jdbc:sqldog:mem
driver-class=com.yuanzhy.sqldog.jdbc.Driver
```
2. 文件模式（数据存储在配置的目录中）
```properties
url=jdbc:sqldog:file:/home/test/data
driver-class=com.yuanzhy.sqldog.jdbc.Driver
```
---

### 独立版

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

- 引入 JDBC 依赖
1. maven
```xml
<dependency>
    <groupId>com.yuanzhy.sqldog</groupId>
    <artifactId>sqldog-jdbc</artifactId>
    <version>0.1-SNAPSHOT</version>
</dependency>
```
2. gradle
```groovy
dependencies {
    implementation "com.yuanzhy.sqldog:sqldog-jdbc:0.1-SNAPSHOT"
}
```

- 配置jdbc
```properties
url=jdbc:sqldog://127.0.0.1[:2345][/schema_name]
user=root
password=123456
driver-class=com.yuanzhy.sqldog.jdbc.Driver
```

## 维护者

[@yuanzhy](https://gitee.com/yuanzhy)

## 使用许可

[MIT](LICENSE) © yuanzhy