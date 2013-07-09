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
package streamcruncher.innards.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.dbcp.BasicDataSource;

import streamcruncher.api.DBName;
import streamcruncher.api.artifact.IndexSpec;
import streamcruncher.api.artifact.MiscSpec;
import streamcruncher.api.artifact.RowSpec;
import streamcruncher.api.artifact.TableSpec;
import streamcruncher.boot.Component;
import streamcruncher.boot.ConfigKeys;
import streamcruncher.boot.Registry;
import streamcruncher.innards.core.partition.aggregate.AbstractAggregatedColumnDDLHelper;
import streamcruncher.innards.impl.query.DDLHelper;
import streamcruncher.innards.query.Parser;
import streamcruncher.innards.util.Helper;
import streamcruncher.util.AtomicX;
import streamcruncher.util.LoggerManager;
import streamcruncher.util.sysevent.SystemEvent;
import streamcruncher.util.sysevent.SystemEventBus;
import streamcruncher.util.sysevent.SystemEvent.Priority;

/*
 * Author: Ashwin Jayaprakash Date: Jan 2, 2006 Time: 9:40:59 AM
 */

public abstract class DatabaseInterface implements Component {
    protected Properties properties;

    protected BasicDataSource dataSource;

    protected String schema;

    protected boolean preservesArtifactsOnShutdown;

    protected boolean privateVolatileInstance;

    /**
     * Auto-commit is <code>false</code>.
     */
    protected Connection sentinelConnection;

    protected Timer sentinelConnectionKeeper;

    // ---------------------

    /**
     * {@inheritDoc} <code>params</code> requires the first parameter to be a
     * {@link java.util.Properties} object loaded with the necessary Database
     * properties.
     */
    public void start(Object... params) throws Exception {
        this.properties = (Properties) params[0];

        String driver = properties.getProperty(ConfigKeys.DB.DRIVER_CLASS_NAME);
        String url = properties.getProperty(ConfigKeys.DB.DRIVER_URL);
        String user = properties.getProperty(ConfigKeys.DB.USER);
        String password = properties.getProperty(ConfigKeys.DB.PASSWORD);

        Class.forName(driver);

        // ----------------------

        schema = properties.getProperty(ConfigKeys.DB.SCHEMA);
        if (schema != null && schema.length() == 0) {
            schema = null;
        }

        String preservesArtifactsStr = properties
                .getProperty(ConfigKeys.DB.PRESERVES_ARTIFACTS_ON_SHUTDOWN);
        preservesArtifactsOnShutdown = Boolean.parseBoolean(preservesArtifactsStr);

        String maxPoolSizeStr = properties.getProperty(ConfigKeys.DB.CONNECTION_POOL_MAX_SIZE);
        int maxPoolSize = Integer.parseInt(maxPoolSizeStr);

        String privateVolatileInstanceStr = properties
                .getProperty(ConfigKeys.DB.PRIVATE_VOLATILE_INSTANCE);
        privateVolatileInstance = Boolean.parseBoolean(privateVolatileInstanceStr);

        dataSource = new BasicDataSource();
        dataSource.setDriverClassName(driver);
        dataSource.setUrl(url);
        dataSource.setUsername(user);
        dataSource.setPassword(password);
        dataSource.setMaxActive(maxPoolSize);
        dataSource.setMaxIdle(maxPoolSize);
        dataSource.setTestOnBorrow(false);
        dataSource.setTestOnReturn(false);
        dataSource.setTimeBetweenEvictionRunsMillis(2 * 60 * 1000);
        dataSource.setMinEvictableIdleTimeMillis(30 * 1000);
        dataSource.setAccessToUnderlyingConnectionAllowed(true);
        dataSource.setPoolPreparedStatements(true);

        setupLastStandingConnection();

        // ----------------------

        Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                DatabaseInterface.class.getName());
        logger.log(Level.INFO, "Started");
    }

    private void setupLastStandingConnection() throws SQLException {
        if (privateVolatileInstance == true) {
            sentinelConnection = createConnection();
            sentinelConnection.setAutoCommit(false);

            sentinelConnectionKeeper = new Timer("SentinelConnectionKeeper", true);

            /*
             * Keep checking periodically and ensure that the sentinelConnection
             * does not timeout.
             */
            TimerTask timerTask = new TimerTask() {
                @Override
                public void run() {
                    boolean isClosed = false;
                    Throwable error = null;
                    try {
                        isClosed = DatabaseInterface.this.sentinelConnection.isClosed();
                    }
                    catch (SQLException e1) {
                        error = e1;
                    }

                    if (error != null || isClosed) {
                        Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                                DatabaseInterface.class.getName());
                        String msg = "The Private/Volatile Database instance may be at"
                                + " risk, because the sentinel Connection has been lost.";
                        if (error == null) {
                            logger.log(Level.SEVERE, msg);
                        }
                        else {
                            logger.log(Level.SEVERE, msg, error);
                        }

                        SystemEventBus bus = Registry.getImplFor(SystemEventBus.class);
                        SystemEvent event = new SystemEvent(DatabaseInterface.class.getName(),
                                "Sentinel Connection Lost", error, Priority.SEVERE);
                        bus.submit(event);

                        // -----------

                        // Try and create a new one.
                        try {
                            Timer oldTimer = DatabaseInterface.this.sentinelConnectionKeeper;

                            DatabaseInterface.this.setupLastStandingConnection();

                            // Cancel this old timer.
                            oldTimer.cancel();
                        }
                        catch (SQLException e) {
                            logger.log(Level.SEVERE, "An error occurred while the sentinel"
                                    + " Connection was being re-created.", e);

                            event = new SystemEvent(DatabaseInterface.class.getName(),
                                    "Sentinel Connection Re-creation Error", null, Priority.SEVERE);
                            bus.submit(event);

                            return;
                        }
                    }

                    // ------------

                    try {
                        DatabaseInterface.this.sentinelConnection.commit();
                    }
                    catch (SQLException e) {
                        Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                                DatabaseInterface.class.getName());
                        logger.log(Level.SEVERE, "An error occurred while the sentinel"
                                + " Connection was pinging the Database.", e);

                        SystemEventBus bus = Registry.getImplFor(SystemEventBus.class);
                        SystemEvent event = new SystemEvent(DatabaseInterface.class.getName(),
                                "Sentinel Connection Ping Error", null, Priority.SEVERE);
                        bus.submit(event);
                    }
                }
            };
            sentinelConnectionKeeper.scheduleAtFixedRate(timerTask, 10 * 1000, 30 * 1000);
        }
    }

    public void stop() throws Exception {
        if (privateVolatileInstance == true) {
            sentinelConnectionKeeper.cancel();

            /*
             * Hold on to this until the very end. Otherwise, the In-mem DBs
             * will shutdown.
             */
            Helper.closeConnection(sentinelConnection);
            sentinelConnection = null;
        }

        dataSource.close();
        dataSource = null;

        // ---------------------

        Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                DatabaseInterface.class.getName());
        logger.log(Level.INFO, "Stopped");
    }

    // ---------------------

    public Connection createConnection() throws SQLException {
        return dataSource.getConnection();
    }

    // ---------------------

    public abstract Class<? extends Parser> getParser();

    public abstract DBName getDBName();

    public String getSchema() {
        return schema;
    }

    public abstract AbstractAggregatedColumnDDLHelper getAggregatedColumnDDLHelper();

    public abstract DDLHelper getDDLHelper();

    public TableSpec createUnpartitionedTableSpec(String schema, String name, RowSpec rowSpec,
            IndexSpec[] indexSpecs, MiscSpec[] otherClauses) {
        return new TableSpec(schema, name, rowSpec, indexSpecs, otherClauses);
    }

    public IndexSpec createIndexSpec(String schema, String name, String tableName, boolean unique,
            String columnName, boolean ascending) {
        return new IndexSpec(schema, name, tableName, unique, columnName, ascending);
    }

    public IndexSpec createIndexSpec(String schema, String name, String tableName, boolean unique,
            String[] columnNames, boolean[] ascending) {
        return new IndexSpec(schema, name, tableName, unique, columnNames, ascending);
    }

    public boolean dbPreservesArtifactsOnShutdown() {
        return preservesArtifactsOnShutdown;
    }

    public AtomicX createRowIdGenerator() {
        return new AtomicX(new AtomicLong(Constants.DEFAULT_MONOTONIC_ID_VALUE));
    }

    public ResultSet wrapResultSet(ResultSet resultSet) throws SQLException {
        return resultSet;
    }
}