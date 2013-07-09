/*
 * StreamCruncher:  Copyright (c) 2006-2008, Ashwin Jayaprakash. All Rights Reserved.
 * Contact:         ashwin {dot} jayaprakash {at} gmail {dot} com
 * Web:             http://www.StreamCruncher.com
 * 
 * This file is part of StreamCruncher.
 * 
 *     StreamCruncher is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 * 
 *     StreamCruncher is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 * 
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with StreamCruncher. If not, see <http://www.gnu.org/licenses/>.
 */
package streamcruncher.boot;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import streamcruncher.api.DBName;
import streamcruncher.api.StartupShutdownHook;
import streamcruncher.api.artifact.RowSpec;
import streamcruncher.api.artifact.RunningQuery;
import streamcruncher.innards.InnardsManager;
import streamcruncher.innards.core.EventBucket;
import streamcruncher.innards.core.filter.TableFilterClassLoaderManager;
import streamcruncher.innards.core.partition.aggregate.AggregatorManager;
import streamcruncher.innards.core.stream.InStream;
import streamcruncher.innards.db.DatabaseInterface;
import streamcruncher.innards.db.cache.CacheManager;
import streamcruncher.innards.db.cache.SchedulableCachedData;
import streamcruncher.innards.file.FileManager;
import streamcruncher.innards.file.FileManagerException;
import streamcruncher.innards.impl.AntsDatabaseInterface;
import streamcruncher.innards.impl.DerbyDatabaseInterface;
import streamcruncher.innards.impl.FirebirdDatabaseInterface;
import streamcruncher.innards.impl.H2DatabaseInterface;
import streamcruncher.innards.impl.MySQLDatabaseInterface;
import streamcruncher.innards.impl.OracleDatabaseInterface;
import streamcruncher.innards.impl.OracleTTDatabaseInterface;
import streamcruncher.innards.impl.PointBaseDatabaseInterface;
import streamcruncher.innards.impl.SolidDatabaseInterface;
import streamcruncher.kernel.InStreamAddRowEventManager;
import streamcruncher.kernel.JobExecutionManager;
import streamcruncher.kernel.PrioritizedSchedulableQuery;
import streamcruncher.kernel.QueryMaster;
import streamcruncher.kernel.RowDisposer;
import streamcruncher.util.LoggerManager;
import streamcruncher.util.TimeKeeper;
import streamcruncher.util.sysevent.SystemEventBus;

/*
 * Author: Ashwin Jayaprakash Date: Dec 31, 2005 Time: 9:11:34 PM
 */

public class Main {
    protected volatile StartupShutdownHook startupShutdownHook;

    /**
     * @param configFilePath
     * @throws Exception
     */
    public void start(String configFilePath) throws Exception {
        Properties properties = new Properties();
        File file = new File(configFilePath);
        FileInputStream fileInputStream = new FileInputStream(file);
        properties.load(fileInputStream);
        fileInputStream.close();

        // -----------

        initHousekeeping(properties);
        initInnards(properties);
        initKernel(properties);

        afterInit();
    }

    /**
     * Initialises the basic, housekeeping Components.
     * 
     * @param properties
     * @throws Exception
     */
    protected void initHousekeeping(Properties properties) throws Exception {
        Registry.init();
        Registry registry = Registry.getInstance();

        // -------------------

        LoggerManager loggerManager = new LoggerManager();
        loggerManager.start(properties);
        registry.setComponentImpl(LoggerManager.class, loggerManager);

        // -------------------

        TimeKeeper timeKeeper = new TimeKeeper();
        timeKeeper.start(properties);
        registry.setComponentImpl(TimeKeeper.class, timeKeeper);

        // -------------------

        SystemEventBus eventBus = new SystemEventBus();
        eventBus.start(properties);
        registry.setComponentImpl(SystemEventBus.class, eventBus);
    }

    protected DatabaseInterface fetchDBInterface(String dbName) throws Exception {
        if (dbName.equalsIgnoreCase(DBName.MySQL.name())) {
            return new MySQLDatabaseInterface();
        }
        else if (dbName.equalsIgnoreCase(DBName.Oracle.name())) {
            return new OracleDatabaseInterface();
        }
        else if (dbName.equalsIgnoreCase(DBName.OracleTT.name())) {
            return new OracleTTDatabaseInterface();
        }
        else if (dbName.equalsIgnoreCase(DBName.Firebird.name())) {
            return new FirebirdDatabaseInterface();
        }
        else if (dbName.equalsIgnoreCase(DBName.Derby.name())) {
            return new DerbyDatabaseInterface();
        }
        else if (dbName.equalsIgnoreCase(DBName.H2.name())) {
            return new H2DatabaseInterface();
        }
        else if (dbName.equalsIgnoreCase(DBName.Solid.name())) {
            return new SolidDatabaseInterface();
        }
        else if (dbName.equalsIgnoreCase(DBName.Ants.name())) {
            return new AntsDatabaseInterface();
        }
        else if (dbName.equalsIgnoreCase(DBName.PointBase.name())) {
            return new PointBaseDatabaseInterface();
        }

        String msg = "This Database: " + dbName + " is not supported.";
        msg = msg + " The Databases supported are: " + Arrays.asList(DBName.values());
        throw new Exception(msg);
    }

    /**
     * Initialises the basic, housekeeping Components.
     * 
     * @param properties
     * @throws Exception
     */
    protected void initInnards(Properties properties) throws Exception {
        Registry registry = Registry.getInstance();

        // -------------------

        String dbName = properties.getProperty(ConfigKeys.DB.NAME);
        DatabaseInterface databaseInterface = fetchDBInterface(dbName);
        databaseInterface.start(properties);

        registry.setComponentImpl(DatabaseInterface.class, databaseInterface);

        // -------------------

        FileManager artifactManager = new FileManager();
        artifactManager.start(properties);

        registry.setComponentImpl(FileManager.class, artifactManager);

        // -------------------

        InnardsManager manager = new InnardsManager();
        manager.start(properties);

        registry.setComponentImpl(InnardsManager.class, manager);

        // -------------------

        CacheManager cacheManager = new CacheManager();
        cacheManager.start(properties);

        registry.setComponentImpl(CacheManager.class, cacheManager);

        // -------------------

        TableFilterClassLoaderManager filterClassLoaderManager = new TableFilterClassLoaderManager();
        filterClassLoaderManager.start(properties);

        registry.setComponentImpl(TableFilterClassLoaderManager.class, filterClassLoaderManager);

        // -------------------

        AggregatorManager aggregatorManager = new AggregatorManager();
        aggregatorManager.start(properties);

        registry.setComponentImpl(AggregatorManager.class, aggregatorManager);

        // -------------------

        ProviderManager providerManager = new ProviderManager();
        providerManager.start(properties);

        registry.setComponentImpl(ProviderManager.class, providerManager);
    }

    /**
     * Scheduling, Query execution etc start here.
     * 
     * @param properties
     * @throws Exception
     */
    protected void initKernel(Properties properties) throws Exception {
        Registry registry = Registry.getInstance();

        // -------------------

        JobExecutionManager executionManager = new JobExecutionManager();
        executionManager.start(properties);
        registry.setComponentImpl(JobExecutionManager.class, executionManager);

        // -------------------

        InStreamAddRowEventManager eventProcManager = new InStreamAddRowEventManager();
        eventProcManager.start(properties);

        registry.setComponentImpl(InStreamAddRowEventManager.class, eventProcManager);

        // -------------------

        QueryMaster queryMaster = new QueryMaster();
        queryMaster.start(properties);

        registry.setComponentImpl(QueryMaster.class, queryMaster);

        // -------------------

        RowDisposer rowDisposer = new RowDisposer();
        rowDisposer.start(properties);

        registry.setComponentImpl(RowDisposer.class, rowDisposer);
    }

    /**
     * Loops through all Components registered in the Registry and stops them
     * one-by-one.
     * 
     * @param continueOnError
     *            If <code>false</code>, and a Component throws an exception,
     *            the loop stops and the method exits with that Exception.
     * @throws Exception
     */
    public void stop(boolean continueOnError) throws Exception {
        beforeShutdown();

        // -------------------

        List<Component> components = Registry.getInstance().getAllRegisteredComponents();
        for (Component component : components) {
            System.out.println("Stopping..." + component.getClass().getSimpleName());

            try {
                component.stop();
            }
            catch (Exception e) {
                if (continueOnError) {
                    e.printStackTrace(System.err);
                }
                else {
                    throw e;
                }
            }
        }

        Registry.discard();
    }

    // -----------

    public void keepRunning() throws Exception {
        String quitCommandName = "quit";
        String help = "Type '" + quitCommandName + "' to stop and exit the Program.";
        System.out.println(help);

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            String command = reader.readLine();
            if (/* Can be null when special chars are typed like Ctrl-Break. */
            command != null && command.equalsIgnoreCase(quitCommandName)) {
                System.out.println("Stopping...");
                stop(true);
                System.out.println("Stopped.");

                break;
            }

            System.out.println(help);
        }
    }

    // -----------

    protected void afterInit() throws Exception {
        StartupShutdownHook hook = getStartupShutdownHook();
        if (hook != null) {
            hook.afterBootup();
        }

        // ------------

        Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(Main.class.getName());
        FileManager artifactManager = Registry.getImplFor(FileManager.class);

        InStream[] inStreams = null;
        SchedulableCachedData[] cachedDatas = null;
        RunningQuery[] queries = null;
        try {
            inStreams = artifactManager.loadInStreams();
            cachedDatas = artifactManager.loadCachedData();
            queries = artifactManager.loadRunningQueries();
        }
        catch (FileManagerException e1) {
            throw new Exception(
                    "Error occurred while reading InStreams and Queries from previous run", e1);
        }

        // ------------

        for (InStream inStream : inStreams) {
            try {
                registerInStream(inStream.getName(), inStream.getRowSpec(),
                        inStream.getBlockSize(), true);

                logger.log(Level.INFO, "Registered InStream: " + inStream.getFQN());
            }
            catch (Exception e) {
                throw new Exception("Error occurred while loading InStream: " + inStream.getFQN()
                        + " from previous run", e);
            }
        }

        for (SchedulableCachedData data : cachedDatas) {
            try {
                registerCachedData(data);

                logger.log(Level.INFO, "Registered CachedData: " + data.getSql());
            }
            catch (Exception e) {
                throw new Exception("Error occurred while loading Cache: " + data.getSql()
                        + " from previous run", e);
            }
        }

        for (RunningQuery query : queries) {
            try {
                registerQuery(query, true);

                logger.log(Level.INFO, "Registered Query: " + query.getName());
            }
            catch (Exception e) {
                throw new Exception("Error occurred while loading Query: " + query.getName()
                        + " from previous run", e);
            }
        }
    }

    protected void beforeShutdown() {
        StartupShutdownHook hook = getStartupShutdownHook();
        if (hook != null) {
            hook.beforeShutdown();
        }

        // ------------

        Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(Main.class.getName());
        FileManager artifactManager = Registry.getImplFor(FileManager.class);

        CacheManager cacheManager = Registry.getImplFor(CacheManager.class);
        ConcurrentMap<String, SchedulableCachedData> allCaches = cacheManager.getAllCachedData();
        for (SchedulableCachedData data : allCaches.values()) {
            try {
                artifactManager.saveCachedData(data, true);
            }
            catch (FileManagerException e) {
                logger.log(Level.WARNING, "Could not save the latest configuration for Cache: "
                        + data.getSql());
            }
        }

        InnardsManager manager = Registry.getImplFor(InnardsManager.class);
        ConcurrentMap<String, RunningQuery> registeredQueries = manager.getAllRegisteredRQs();
        for (RunningQuery query : registeredQueries.values()) {
            try {
                artifactManager.saveRunningQuery(query, true);
            }
            catch (FileManagerException e) {
                logger.log(Level.WARNING, "Could not save the latest configuration for Query: "
                        + query.getName());
            }
        }
    }

    // ------------

    public void registerInStream(String name, RowSpec rowSpec, int blockSize, boolean startup)
            throws Exception {
        InnardsManager manager = Registry.getImplFor(InnardsManager.class);
        manager.registerInStream(name, rowSpec, blockSize, startup);
    }

    public void unregisterInStream(String name) throws Exception {
        InnardsManager manager = Registry.getImplFor(InnardsManager.class);
        manager.unregisterInStream(name);
    }

    public void registerCachedData(SchedulableCachedData cachedData) {
        CacheManager manager = Registry.getImplFor(CacheManager.class);
        manager.addCachedData(cachedData);
    }

    public void unregisterCachedData(SchedulableCachedData cachedData) {
        CacheManager manager = Registry.getImplFor(CacheManager.class);
        manager.addCachedData(cachedData);
    }

    public void registerQuery(RunningQuery runningQuery, boolean startup) throws Exception {
        InnardsManager manager = Registry.getImplFor(InnardsManager.class);
        boolean registered = false;

        try {
            EventBucket[] buckets = runningQuery.getEventBuckets();
            for (EventBucket bucket : buckets) {
                bucket.init();
            }

            manager.registerRunningQuery(runningQuery, startup);
            registered = true;

            // --------------------

            Set<String> cachedSubQueries = runningQuery.getCachedSubQueries();
            CacheManager cacheManager = Registry.getImplFor(CacheManager.class);
            for (String sql : cachedSubQueries) {
                cacheManager.registerCachedData(sql, runningQuery.getName());
            }

            // --------------------

            QueryMaster queryMaster = Registry.getImplFor(QueryMaster.class);
            queryMaster.registerQuery((PrioritizedSchedulableQuery) runningQuery);

            // --------------------

            InStreamAddRowEventManager rowEventManager = Registry
                    .getImplFor(InStreamAddRowEventManager.class);
            rowEventManager.refreshCachedInStreams();
        }
        catch (Exception e) {
            if (registered) {
                manager.unregisterRunningQuery(runningQuery.getName());
            }

            throw e;
        }
    }

    public void unregisterQuery(String name) {
        QueryMaster queryMaster = Registry.getImplFor(QueryMaster.class);
        queryMaster.unregisterQuery(name);

        // --------------------

        InnardsManager manager = Registry.getImplFor(InnardsManager.class);
        RunningQuery runningQuery = manager.getRegisteredRQs(name);
        if (runningQuery != null) {
            manager.unregisterRunningQuery(name);

            // --------------------

            Set<String> cachedSubQueries = runningQuery.getCachedSubQueries();
            CacheManager cacheManager = Registry.getImplFor(CacheManager.class);
            for (String sql : cachedSubQueries) {
                cacheManager.unregisterCachedData(sql, runningQuery.getName());
            }
        }

        // --------------------

        InStreamAddRowEventManager rowEventManager = Registry
                .getImplFor(InStreamAddRowEventManager.class);
        rowEventManager.refreshCachedInStreams();
    }

    // --------------------

    public StartupShutdownHook getStartupShutdownHook() {
        return startupShutdownHook;
    }

    public void setStartupShutdownHook(StartupShutdownHook startupShutdownHook) {
        this.startupShutdownHook = startupShutdownHook;
    }
}
