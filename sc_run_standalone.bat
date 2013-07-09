set basedir=.

del /Q .\.artifacts\*.*

@REM set DB_DRIVER=C:\PROGRA~2\TimesTen\tt60\lib\classes15.jar
@REM set DB_DRIVER=C:\Programs1\MySQL\mysql-connector-java-3.1.12\mysql-connector-java-3.1.12-bin.jar
@REM set DB_DRIVER=C:\Progra~1\Firebird\jdbc\jaybird-full-2.0.1.jar
@REM set DB_DRIVER=C:\Programs1\jdk1.6.0\db\lib\derby.jar
@REM set DB_DRIVER=C:\Programs1\Solid\DatabaseEngine4.5\jdbc\SolidDriver2.0.jar
@REM set DB_DRIVER=C:\Programs1\antsdb\antsjdbc\antsjdbc.jar
@REM set DB_DRIVER=C:\Programs1\pointbase55\lib\pbembedded55ev.jar
@REM set DB_DRIVER=C:\oracle\product\10.2.0\db_1\jdbc\lib\ojdbc14.jar
set DB_DRIVER=%basedir%\lib\h2.jar

set CP=%basedir%\streamcruncher.jar;%basedir%\lib\antlr-2.7.6rc1.jar;%basedir%\lib\commons-collections-3.2.jar;%basedir%\lib\commons-dbcp-1.2.1.jar;%basedir%\lib\commons-pool-1.3.jar;%basedir%\lib\commons-math-1.1.jar;%basedir%\lib\testng-5.1-jdk15.jar;%basedir%\lib\ognl-2.7.jar;%basedir%\lib\javassist-3.4.ga.jar
set CP=%CP%;%DB_DRIVER%;.\demo_classes;%CLASSPATH%

@REM If using Java 1.6, then add -XX:+DoEscapeAnalysis
set tuning=-server -XX:+UseBiasedLocking -XX:CompileThreshold=5000 -XX:ThreadStackSize=256 -XX:+UseParallelGC -XX:ParallelGCThreads=2 -XX:MaxGCPauseMillis=100 -Xms768m -Xmx768m

set JAVA_HOME=C:\Programs1\jdk1.6.0
set PATH=%JAVA_HOME%\bin;%PATH%
if not exist %JAVA_HOME%\bin\java.exe goto err

%JAVA_HOME%\bin\java -cp %CP% %tuning% -Dsc.config.file=.\sc_config_h2.properties streamcruncher.test.StandAloneDemo

goto :EOF

:err
echo on
echo "**** JAVA_HOME must be set correctly to the JDK Home in this Batch file ***"

