# sqldog

#### [English](README.md) | [中文](README_CN.md)

Sqldog is a simple RDBMS developed in Java. It supports memory and disk storage mode and supports the following features

* DML SQL（simple）
* DDL SQL
* SELECT
* CLI
* JPA (Hibernate) dialect, Mybatis PageHelper, SpringData-JDBC automatic adaptation

**Note**: For development and learning use only, do not use in production environment

## Usage

### Embedded
- Import "JDBC" and "Server" dependencies
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

- Configure jdbc
1. In-memory mode (data is stored in JVM memory)
```properties
url=jdbc:sqldog:mem
driver-class=com.yuanzhy.sqldog.jdbc.Driver
```
2. File mode (data is stored in a configured directory)
```properties
url=jdbc:sqldog:file:/home/test/data
driver-class=com.yuanzhy.sqldog.jdbc.Driver
```

---

### Standalone

#### Install

1. Install JDK8+ and configure environment variables
2. Extracting the Release Package to any directory
3. Configure the "bin" directory to an environment variable
4. Modify the configuration file "server/sqldog.properties" if needed
configuration instruction as follows:

- server.storage.mode：Storage mode, Options are "disk", "memory"
- server.storage.writeCache: Whether to enable write cache. If write cache is enabled, the write speed is greatly increased, disk write is delayed, reduced reliability (only disk mode takes effect)
- server.storage.path: Data storage path, can be relative or absolute. Relative path is relative to the SQLDOG installation directory (only disk mode takes effect)
- server.storage.codec：Metadata encoding scheme
- server.host：Binding host
- server.port：Binding port
- server.username：DB username
- server.password：DB password
- server.max-connections：Maximum connections

Start the service after configuration
```shell
dql-server
```

#### CLI

- Show help
```shell
dsql --help
```

- Connecting to localhost server
```shell
dsql -U [username] -p [password]
```
- Lists all schema
```shell
show schemas
```
- Show current schema
```shell
show search_path
```
- Switch schema
```shell
use [schema_name]
# or
set search_path to [schema_name]
```
- Lists all tables in the current schema
```shell
show tables
```
- Show table information
```shell
desc [table_name]
# or
\d [table_name]
```
- Exit the command line
```shell
quit
# or
exit
# or
\q
```

#### JDBC

- Import JDBC dependency
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

- Configure jdbc
```properties
url=jdbc:sqldog://127.0.0.1[:2345][/schema_name]
user=root
password=123456
driver-class=com.yuanzhy.sqldog.jdbc.Driver
```

## Maintainers

[@yuanzhy](https://github.com/yuanzhy)

## License

[MIT](LICENSE) © yuanzhy