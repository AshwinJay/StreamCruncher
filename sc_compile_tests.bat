echo off
set basedir=.

del /Q .\demo_classes

mkdir .\demo_classes

set CP=%basedir%\streamcruncher.jar;%basedir%\lib\testng-5.1-jdk15.jar;%CLASSPATH%

@REM *** Set this to match your JDK directory ***

set JAVA_HOME=C:\Programs1\jdk1.6.0

set PATH=%JAVA_HOME%\bin;%PATH%

if not exist %JAVA_HOME%\bin\javac.exe goto err
dir /b /s .\demo_src\*.java > %basedir%\sources.txt

%JAVA_HOME%\bin\javac -target 1.5 -g -classpath %CP% -d .\demo_classes @%basedir%\sources.txt

goto :EOF

:err
echo on
echo "**** JAVA_HOME must be set correctly in this Batch file for compilation to work ***"
