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
package streamcruncher.api;

import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;

import streamcruncher.api.aggregator.AbstractAggregatorHelper;
import streamcruncher.api.artifact.RowSpec;
import streamcruncher.api.artifact.RunningQuery;
import streamcruncher.boot.Main;
import streamcruncher.boot.ProviderManager;
import streamcruncher.boot.ProviderManagerException;
import streamcruncher.boot.Registry;
import streamcruncher.innards.core.partition.aggregate.AggregatorManager;
import streamcruncher.innards.core.partition.aggregate.AggregatorManagerException;
import streamcruncher.innards.db.DatabaseInterface;
import streamcruncher.innards.db.cache.CacheManager;
import streamcruncher.innards.db.cache.CachedData;
import streamcruncher.innards.query.Parser;
import streamcruncher.kernel.PrioritizedSchedulableQuery;
import streamcruncher.kernel.QueryMaster;
import streamcruncher.util.TimeKeeper;

/*
 * Author: Ashwin Jayaprakash Date: Jul 2, 2006 Time: 4:49:12 PM
 */
/**
 * The main class provided by the Kernel to - register/unregister Event Streams,
 * Queries, hooks etc. It also provides methods to start and stop the Kernel.
 * 
 * @author Ashwin Jayaprakash, Copyright 2005-2007. All Rights Reserved.
 */
public final class StreamCruncher {
    protected final Main main;

    /**
     * Any number of these instances can be created. However, the instances are
     * <b>not</b> Thread-safe.
     */
    public StreamCruncher() {
        main = new Main();
    }

    /**
     * Sets the lifecycle Listener. Must be invoked <b>before</b> invoking the
     * {@link #start(String)} method if the intention is to listen to Startup
     * events and/or <b>before</b> invoking the {@link #stop()} or before the
     * Kernel is issued a stop command via {@link #keepRunning()} if the
     * intention is to listen to Shutdown events.
     * 
     * @param hook
     */
    public void setStartupShutdownHook(StartupShutdownHook hook) {
        main.setStartupShutdownHook(hook);
    }

    /**
     * Removes the Listener from the Kernel.
     */
    public void clearStartupShutdownHook() {
        main.setStartupShutdownHook(null);
    }

    /**
     * @return The Listener currently attached to the Kernel.
     */
    public StartupShutdownHook getStartupShutdownHook() {
        return main.getStartupShutdownHook();
    }

    /**
     * Starts the Kernel. Successful return from this method invocation
     * indicates that the Kernel has started and is ready to register,
     * unregister artifacts etc. If the Kernel is being restarted, then any
     * Queries that were registered in a previous run, will start executing.
     * <p>
     * The Kernel must be <b>started first</b> using this method, before
     * invoking <b>any method on any of the API Classes</b>.
     * </p>
     * 
     * @param configFilePath
     *            The path, including the name of the Kernel configuration file.
     * @throws StreamCruncherException
     */
    public void start(String configFilePath) throws StreamCruncherException {
        try {
            main.start(configFilePath);
        }
        catch (Exception e) {
            throw new StreamCruncherException(e);
        }
    }

    /**
     * Once the Kernel has started, this method can be invoked, where the
     * <b>invoking Thread blocks and waits</b> for the stop-instruction to be
     * typed at the System Console. On receiving the correct instruction, the
     * Kernel will be stopped and the Thread will return from the method.
     * 
     * @throws StreamCruncherException
     */
    public void keepRunning() throws StreamCruncherException {
        try {
            main.keepRunning();
        }
        catch (Exception e) {
            throw new StreamCruncherException(e);
        }
    }

    /**
     * This is an alternative way of stopping the Kernel (also see
     * {@link #keepRunning()})
     * 
     * @throws StreamCruncherException
     */
    public void stop() throws StreamCruncherException {
        try {
            main.stop(true);
        }
        catch (Exception e) {
            throw new StreamCruncherException(e);
        }
    }

    // --------------------

    /**
     * Register an Input Event Stream as described by the {@link RowSpec}.
     * 
     * @param name
     * @param rowSpec
     * @param blockSize
     *            This number is used by the Kernel to allocate blocks in memory
     *            to accomodate the incoming Events. Input Streams with very
     *            high rates of arrivals must use larger numbers (multiples of
     *            <code>1024</code>).
     * @throws StreamCruncherException
     */
    public void registerInStream(String name, RowSpec rowSpec, int blockSize)
            throws StreamCruncherException {
        try {
            main.registerInStream(name, rowSpec, blockSize, false);
        }
        catch (Exception e) {
            throw new StreamCruncherException(e);
        }
    }

    /**
     * Register an Input Event Stream as described by the {@link RowSpec} and
     * default Block size of <code>1024</code>.
     * 
     * @param name
     * @param rowSpec
     * @throws StreamCruncherException
     * @see #registerInStream(String, RowSpec, int)
     */
    public void registerInStream(String name, RowSpec rowSpec) throws StreamCruncherException {
        registerInStream(name, rowSpec, 1024);
    }

    /**
     * Unregisters the Input Event Stream. This operation will succeed only if
     * all the Queries that were using this Stream have been unregistered.
     * 
     * @param name
     * @throws StreamCruncherException
     */
    public void unregisterInStream(String name) throws StreamCruncherException {
        try {
            main.unregisterInStream(name);
        }
        catch (Exception e) {
            throw new StreamCruncherException(e);
        }
    }

    // --------------------

    /**
     * The "Running Query" that will execute on the Event Stream has to be
     * parsed by the Kernel first.
     * 
     * @param parserParameters
     * @return The handle to the parsed "Running Query" as the output of
     *         successful parsing.
     * @throws StreamCruncherException
     */
    public ParsedQuery parseQuery(ParserParameters parserParameters) throws StreamCruncherException {
        DatabaseInterface databaseInterface = Registry.getImplFor(DatabaseInterface.class);
        Class<? extends Parser> parserClass = databaseInterface.getParser();

        RunningQuery runningQuery = null;

        try {
            Constructor<? extends Parser> constructor = parserClass
                    .getConstructor(new Class[] { ParserParameters.class });
            Parser parser = constructor.newInstance(new Object[] { parserParameters });
            runningQuery = parser.parse();

            PrioritizedSchedulableQuery psq = new PrioritizedSchedulableQuery(runningQuery);
            return new ParsedQuery(psq);
        }
        catch (Exception e) {
            throw new StreamCruncherException(e);
        }
    }

    /**
     * Registers the Query that was parsed using
     * {@link #parseQuery(ParserParameters)}. The Query execution will start
     * after this registration (based on the configurations provided using the
     * {@link QueryConfig}).
     * 
     * @param parsedQuery
     * @throws StreamCruncherException
     */
    public void registerQuery(ParsedQuery parsedQuery) throws StreamCruncherException {
        RunningQuery runningQuery = parsedQuery.getRunningQuery();

        try {
            main.registerQuery(runningQuery, false);
        }
        catch (Exception e) {
            throw new StreamCruncherException(e);
        }
    }

    /**
     * Each Query has a unique config-object and this method returns a handle to
     * the same. It can also be accessed using the
     * {@link ParsedQuery#getQueryConfig()} method. This object must be
     * retrieved (if needed) afresh after every Kernel restart.
     * 
     * @param queryName
     * @return <code>null</code> if there is no Query that has been registered
     *         with this name.
     */
    public QueryConfig getQueryConfig(String queryName) {
        QueryMaster queryMaster = Registry.getImplFor(QueryMaster.class);

        PrioritizedSchedulableQuery psq = queryMaster.getScheduledQuery(queryName);
        if (psq != null) {
            return psq.getQueryConfig();
        }

        return null;
    }

    /**
     * Stops and unregisters the Query that is running on the Kernel.
     * 
     * @param name
     *            As provided in {@link ParserParameters#getQueryName()}.
     */
    public void unregisterQuery(String name) {
        main.unregisterQuery(name);
    }

    // --------------------

    /**
     * @return A pooled Connection. Must be <b>closed explicitly</b> when not
     *         required anymore.
     * @throws SQLException
     */
    public Connection createConnection() throws SQLException {
        DatabaseInterface databaseInterface = Registry.getImplFor(DatabaseInterface.class);
        return databaseInterface.createConnection();
    }

    /**
     * @return The Database Schema which the Kernel has been configured to use.
     */
    public String getDBSchema() {
        DatabaseInterface databaseInterface = Registry.getImplFor(DatabaseInterface.class);
        return databaseInterface.getSchema();
    }

    // --------------------

    /**
     * @return All the SQL Sub-Queries that have been cached by the Kernel.
     */
    public Collection<String> getResultSetCacheConfigKeys() {
        CacheManager cacheManager = Registry.getImplFor(CacheManager.class);
        return new ArrayList<String>(cacheManager.getAllCachedData().keySet());
    }

    /**
     * @param cachedSql
     * @return The config-object for the Cached SQL Query provided as the
     *         parameter. If the Query that uses the SQL that is provided as the
     *         parameter, has not been registered with the Kernel yet, then the
     *         return value might be <code>null</code>. Since there could be
     *         multiple Queries that use the same SQL as a Sub-Query, the Kernel
     *         maintains only one cache that will be shared by all the
     *         referrers.
     */
    public ResultSetCacheConfig getResultSetCacheConfig(String cachedSql) {
        CacheManager cacheManager = Registry.getImplFor(CacheManager.class);
        CachedData cachedData = cacheManager.getCachedData(cachedSql);
        return cachedData.getCacheConfig();
    }

    // --------------------

    /**
     * Creates a helper object for the Input Event Stream.
     * 
     * @param name
     * @return
     * @throws StreamCruncherException
     */
    public InputSession createInputSession(String name) throws StreamCruncherException {
        return new InputSession(name);
    }

    /**
     * Creates a helper object for the Output Event Stream.
     * 
     * @param queryName
     * @return
     * @throws StreamCruncherException
     */
    public OutputSession createOutputSession(String queryName) throws StreamCruncherException {
        return new OutputSession(queryName);
    }

    // --------------------

    /**
     * Registers an Aggregator function. If a Query uses a custom Aggregator,
     * then that Aggregator must be registered before the Query is registered.
     * 
     * @param helper
     * @throws StreamCruncherException
     */
    public void registerAggregator(AbstractAggregatorHelper helper) throws StreamCruncherException {
        AggregatorManager manager = Registry.getImplFor(AggregatorManager.class);
        try {
            manager.registerHelper(helper);
        }
        catch (AggregatorManagerException e) {
            throw new StreamCruncherException(e);
        }
    }

    /**
     * An Aggregator must be unregistered <b>only after all</b> the Queries
     * that use it have been unregistered. This method will unregister the
     * Aggregator without checking if Queries are still using it.
     * 
     * @param functionName
     *            As provided in
     *            {@link AbstractAggregatorHelper#getFunctionName()}
     */
    public void unregisterAggregator(String functionName) {
        AggregatorManager manager = Registry.getImplFor(AggregatorManager.class);
        manager.unregisterHelper(functionName);
    }

    // --------------------

    /**
     * Registers a Provider. If a Query uses a custom Provider, then that
     * Provider must be registered before the Query is registered.
     * 
     * @param providerName
     * @param providerClass
     * @throws StreamCruncherException
     */
    public void registerProvider(String providerName, Class<? extends Provider> providerClass)
            throws StreamCruncherException {
        ProviderManager manager = Registry.getImplFor(ProviderManager.class);
        try {
            manager.registerProvider(providerName, providerClass);
        }
        catch (ProviderManagerException e) {
            throw new StreamCruncherException(e);
        }
    }

    /**
     * A Provider must be unregistered <b>only after all</b> the Queries that
     * use it have been unregistered. This method will unregister the Provider
     * without checking if Queries are still using it.
     * 
     * @param providerName
     *            As provided in {@link #registerProvider(String, Class)}
     */
    public void unregisterProvider(String providerName) {
        ProviderManager manager = Registry.getImplFor(ProviderManager.class);
        manager.unregisterProvider(providerName);
    }

    // --------------------

    /**
     * Forces the Kernel to add the number provided to the time it obtains from
     * the System clock. It affects Query execution times, Event expiry times,
     * Event generation timestamps etc. This operation <b>is Thread-safe</b>.
     * 
     * @param timeBiasMsecs
     *            Bias in milliseconds (+ve or -ve).
     */
    public void setTimeBiasMsecs(long timeBiasMsecs) {
        TimeKeeper keeper = Registry.getImplFor(TimeKeeper.class);
        keeper.setTimeBiasMsecs(timeBiasMsecs);
    }

    /**
     * @return The current bias (default is 0) being used by the Kernel.
     */
    public long getTimeBiasMsecs() {
        TimeKeeper keeper = Registry.getImplFor(TimeKeeper.class);
        return keeper.getTimeBiasMsecs();
    }
}
