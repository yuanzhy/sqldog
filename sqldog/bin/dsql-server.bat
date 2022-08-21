@echo off

set dir=%~dp0
if "%SQLDOG_HOME%"=="" (
    set SQLDOG_HOME=%dir:~0,-5%
)

echo Using JAVA_HOME:    %JAVA_HOME%
echo Using SQLDOG_HOME:  %SQLDOG_HOME%

java -DSQLDOG_HOME=%SQLDOG_HOME% -jar %SQLDOG_HOME%\server\sqldog-server.jar