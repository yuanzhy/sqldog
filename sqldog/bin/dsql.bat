@echo off

set VERSION={VERSION}

set dir=%~dp0

if "%SQLDOG_HOME%"=="" (
    set SQLDOG_HOME=%dir:~0,-5%
)

set allparam=

:param
set str=%1
if "%str%"=="" (
    goto end
)
set allparam=%allparam% %str%
shift /0
goto param

:end
if "%allparam%"=="" (
    goto eof
)

rem remove left right blank
:intercept_left
if "%allparam:~0,1%"==" " set "allparam=%allparam:~1%"&goto intercept_left

:intercept_right
if "%allparam:~-1%"==" " set "allparam=%allparam:~0,-1%"&goto intercept_right

:eof

java -jar %SQLDOG_HOME%\cli\sqldog-cli-%VERSION%.jar %allparam%