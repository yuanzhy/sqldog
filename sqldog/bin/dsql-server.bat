@echo off

set VERSION={VERSION}
set dir=%~dp0
if "%SQLDOG_HOME%"=="" (
    set SQLDOG_HOME=%dir:~0,-5%
)

echo Using JAVA_HOME:    %JAVA_HOME%
echo Using SQLDOG_HOME:  %SQLDOG_HOME%
echo Sqldog Version is %VERSION%

java -DSQLDOG_HOME=%SQLDOG_HOME% -Dlogback.configurationFile=%SQLDOG_HOME%/conf/logback.xml -jar %SQLDOG_HOME%\server\sqldog-server-%VERSION%.jar