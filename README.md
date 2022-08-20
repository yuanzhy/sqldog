# sqldog

Sqldog is a simple RDBMS developed in Java. It supports memory and disk storage mode and supports the following features

* DML SQL（simple）
* DDL SQL
* CLI
* Simple Select
* Simple Agg

**Note**: For development and learning use only, do not use in production environment

## Install

1. Install JDK8+ and configure environment variables
2. Extracting the Release Package to any directory
3. Configure the bin directory to an environment variable

## Usage

### Server

Modify the configuration file "server/config.properties" if needed
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

### CLI

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

### JDBC

- Install "JDBC jar" into maven local repository
```shell
mvn install:install-file -DgroupId=com.yuanzhy.sqldog -DartifactId=sqldog-core -Dversion=1.0-SNAPSHOT -Dpackaging=jar -Dfile=./sqldog/jdbc/sqldog-core-1.0-SNAPSHOT.jar
mvn install:install-file -DgroupId=com.yuanzhy.sqldog -DartifactId=sqldog-dialect -Dversion=1.0-SNAPSHOT -Dpackaging=jar -Dfile=./sqldog/jdbc/sqldog-dialect-1.0-SNAPSHOT.jar
mvn install:install-file -DgroupId=com.yuanzhy.sqldog -DartifactId=sqldog-jdbc -Dversion=1.0-SNAPSHOT -Dpackaging=jar -Dfile=./sqldog/jdbc/sqldog-jdbc-1.0-SNAPSHOT.jar
```
- Add maven dependency
```xml
<dependency>
    <groupId>com.yuanzhy.sqldog</groupId>
    <artifactId>sqldog-jdbc</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```
- Configure JDBC
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