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
package streamcruncher.innards;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import streamcruncher.api.artifact.IndexSpec;
import streamcruncher.api.artifact.MiscSpec;
import streamcruncher.api.artifact.RowSpec;
import streamcruncher.api.artifact.RunningQuery;
import streamcruncher.api.artifact.TableFQN;
import streamcruncher.api.artifact.TableSpec;
import streamcruncher.boot.Component;
import streamcruncher.boot.Registry;
import streamcruncher.innards.core.InstreamNotificationRendezvous;
import streamcruncher.innards.core.stream.InStream;
import streamcruncher.innards.core.stream.OutStream;
import streamcruncher.innards.db.DatabaseInterface;
import streamcruncher.innards.file.FileManager;
import streamcruncher.innards.file.FileManagerException;
import streamcruncher.util.LoggerManager;
import streamcruncher.util.undo.Helper;

/*
 * Author: Ashwin Jayaprakash Date: Jan 2, 2006 Time: 9:59:59 AM
 */

public class InnardsManager implements Component {
    protected final ConcurrentMap<String, InStream> inStreams;

    protected final ConcurrentMap<String, RunningQuery> runningQueries;

    protected final ConcurrentMap<String, OutStream> outStreams;

    protected final InstreamNotificationRendezvous notificationRendezvous;

    protected FileManager artifactManager;

    public InnardsManager() {
        inStreams = new ConcurrentHashMap<String, InStream>();
        runningQueries = new ConcurrentHashMap<String, RunningQuery>();
        outStreams = new ConcurrentHashMap<String, OutStream>();
        notificationRendezvous = new InstreamNotificationRendezvous();
    }

    public void start(Object... params) throws Exception {
        Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                InnardsManager.class.getName());
        logger.log(Level.INFO, "Started");

        // -------------

        artifactManager = Registry.getImplFor(FileManager.class);
    }

    public void stop() throws Exception {
        artifactManager = null;

        // -------------

        Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                InnardsManager.class.getName());
        logger.log(Level.INFO, "Stopped");
    }

    // --------------

    /**
     * @param spec
     * @param autoPurge
     * @param blockSize
     * @param startup
     * @throws InnardsManagerException
     */
    public void registerInStream(String name, RowSpec rowSpec, int blockSize, boolean startup)
            throws InnardsManagerException {
        if (inStreams.containsKey(name) == false) {
            InStream inStream = new InStream(name, rowSpec, blockSize, notificationRendezvous);
            inStreams.put(name, inStream);

            if (startup == false) {
                try {
                    artifactManager.saveInStream(inStream, false);
                }
                catch (FileManagerException e) {
                    inStreams.remove(name);

                    throw new InnardsManagerException(e);
                }
            }
        }
        else {
            throw new InnardsManagerException("The InStream: " + name
                    + " has already been registered.");
        }
    }

    /**
     * @param schema
     * @param name
     * @return
     * @throws InnardsManagerException
     */
    public void unregisterInStream(String name) throws InnardsManagerException {
        InStream inStream = inStreams.get(name);
        if (inStream != null) {
            if (inStream.getListeners().isEmpty() == false) {
                throw new InnardsManagerException("The InStream: " + name
                        + ", could not be unregistered as there are"
                        + " some Queries listening on it.");
            }

            // --------------

            inStreams.remove(name);

            artifactManager.deleteInStream(inStream);
        }
    }

    /**
     * @param name
     *            {@link InStream#getName()}
     * @return
     * @see #getRegisteredInStream(String)
     */
    public InStream getRegisteredInStream(String name) {
        return inStreams.get(name);
    }

    public ConcurrentMap<String, InStream> getAllRegisteredInStreams() {
        return inStreams;
    }

    // --------------

    /**
     * Uses {@link RunningQuery#getName()} as the Key.
     * 
     * @param runningQuery
     * @param startup
     * @return
     * @throws InnardsManagerException
     */
    public void registerRunningQuery(final RunningQuery runningQuery, boolean startup)
            throws InnardsManagerException {
        Helper helper = new Helper();

        final String key = runningQuery.getName();
        if (runningQueries.containsKey(key) == false) {
            DatabaseInterface databaseInterface = Registry.getImplFor(DatabaseInterface.class);

            try {
                if (startup == false) {
                    artifactManager.saveRunningQuery(runningQuery, false);

                    helper.registerUndoEntry(new Helper.UndoRunner() {
                        public void undo() throws Exception {
                            artifactManager.deleteRunningQuery(runningQuery);
                        }
                    });
                }

                TableSpec resultTableSpec = null;
                TableFQN resultTableFQN = runningQuery.getResultTableFQN();

                Map<String, TableSpec> tableFQNAndSpecMap = runningQuery.getTableFQNAndSpecMap();
                Set<String> keys = tableFQNAndSpecMap.keySet();
                for (String fqn : keys) {
                    final TableSpec tableSpec = tableFQNAndSpecMap.get(fqn);

                    if (fqn.equals(resultTableFQN.getFQN())) {
                        resultTableSpec = tableSpec;
                    }

                    if (tableSpec.isVirtual()) {
                        continue;
                    }

                    if (startup && databaseInterface.dbPreservesArtifactsOnShutdown()) {
                        runDropDDL(tableSpec);
                    }

                    runCreateDDL(tableSpec);
                    helper.registerUndoEntry(new Helper.UndoRunner() {
                        public void undo() throws Exception {
                            InnardsManager.this.runDropDDL(tableSpec);
                        }
                    });
                }

                // -------------

                runningQueries.put(key, runningQuery);
                helper.registerUndoEntry(new Helper.UndoRunner() {
                    public void undo() throws Exception {
                        runningQueries.remove(key);
                    }
                });

                // -------------

                Collection<InStream> theInStreams = inStreams.values();
                for (final InStream stream : theInStreams) {
                    stream.afterRegisteringRQ(runningQuery);
                    helper.registerUndoEntry(new Helper.UndoRunner() {
                        public void undo() throws Exception {
                            stream.beforeUnregisteringRQ(runningQuery);
                        }
                    });
                }

                // -------------

                OutStream outStream = new OutStream(runningQuery.getName(), resultTableSpec);
                outStreams.put(runningQuery.getName(), outStream);
            }
            catch (Exception e) {
                helper.undo(true);

                throw new InnardsManagerException(e);
            }
        }
        else {
            throw new InnardsManagerException("The RunningQuery: " + runningQuery.getName()
                    + " has already been registered.");
        }
    }

    /**
     * @param name
     */
    public void unregisterRunningQuery(String name) {
        RunningQuery runningQuery = runningQueries.get(name);

        if (runningQuery == null) {
            return;
        }

        // -------------

        outStreams.remove(runningQuery.getName());

        // -------------

        Collection<InStream> theInStreams = inStreams.values();
        for (InStream stream : theInStreams) {
            stream.beforeUnregisteringRQ(runningQuery);
        }

        // -------------

        runningQueries.remove(name);

        // -------------

        Map<String, TableSpec> tableFQNAndSpecMap = runningQuery.getTableFQNAndSpecMap();
        Set<String> keys = tableFQNAndSpecMap.keySet();
        for (String fqn : keys) {
            TableSpec tableSpec = tableFQNAndSpecMap.get(fqn);
            if (tableSpec.isVirtual()) {
                continue;
            }
            try {
                runDropDDL(tableSpec);
            }
            catch (InnardsManagerException e) {
                Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                        InnardsManager.class.getName());
                logger.log(Level.WARNING, "", e);
            }
        }

        // -------------

        artifactManager.deleteRunningQuery(runningQuery);
    }

    public ConcurrentMap<String, RunningQuery> getAllRegisteredRQs() {
        return runningQueries;
    }

    public RunningQuery getRegisteredRQs(String name) {
        return runningQueries.get(name);
    }

    // --------------

    /**
     * @param queryName
     * @return
     */
    public OutStream getRegisteredOutStream(String queryName) {
        return outStreams.get(queryName);
    }

    public ConcurrentMap<String, OutStream> getAllRegisteredOutStreams() {
        return outStreams;
    }

    // --------------

    /**
     * @return Returns the notificationRendezvous.
     */
    public InstreamNotificationRendezvous getNotificationRendezvous() {
        return notificationRendezvous;
    }

    // --------------

    public void runCreateDDL(TableSpec spec) throws InnardsManagerException {
        LinkedList<String> commands = new LinkedList<String>();
        LinkedList<String> dropCommands = new LinkedList<String>();

        commands.add(spec.constructCreateCommand());
        dropCommands.add(spec.constructDropCommand());

        IndexSpec[] indexSpecs = spec.getIndexSpecs();
        for (IndexSpec spec2 : indexSpecs) {
            commands.add(spec2.constructCreateCommand());
            dropCommands.add(spec2.constructDropCommand());
        }

        MiscSpec[] others = spec.getOtherClauses();
        for (MiscSpec spec2 : others) {
            commands.add(spec2.constructCreateCommand());
            dropCommands.add(spec2.constructDropCommand());
        }

        // --------------

        DatabaseInterface databaseInterface = Registry.getImplFor(DatabaseInterface.class);
        Connection connection = null;
        Statement statement = null;

        int counter = 0;
        try {
            connection = databaseInterface.createConnection();
            statement = connection.createStatement();

            for (Iterator<String> iter = commands.iterator(); iter.hasNext();) {
                String ddl = iter.next();
                statement.execute(ddl);

                SQLWarning warning = statement.getWarnings();
                if (warning != null) {
                    throw warning;
                }

                counter++;
            }
        }
        catch (Throwable t) {
            Collections.reverse(dropCommands);

            // Remove commands that were not executed yet.
            int remove = dropCommands.size() - counter;
            while (remove > 0) {
                dropCommands.remove(0);
                remove--;
            }

            for (Iterator<String> iter = dropCommands.iterator(); iter.hasNext();) {
                String ddl = iter.next();
                try {
                    statement.execute(ddl);

                    SQLWarning warning = statement.getWarnings();
                    if (warning != null) {
                        logDDLError(spec, ddl, warning);
                    }
                }
                catch (Throwable t1) {
                    logDDLError(spec, ddl, t1);
                }
            }

            // ---------------

            throw new InnardsManagerException("Error occurred while executing DDL for: "
                    + spec.getFQN(), t);
        }
        finally {
            streamcruncher.innards.util.Helper.closeStatement(statement);
            streamcruncher.innards.util.Helper.closeConnection(connection);
        }
    }

    public void runDropDDL(TableSpec spec) throws InnardsManagerException {
        LinkedList<String> commands = new LinkedList<String>();

        MiscSpec[] others = spec.getOtherClauses();
        for (MiscSpec spec2 : others) {
            commands.add(spec2.constructDropCommand());
        }

        IndexSpec[] indexSpecs = spec.getIndexSpecs();
        for (IndexSpec spec2 : indexSpecs) {
            commands.add(spec2.constructDropCommand());
        }

        commands.add(spec.constructDropCommand());

        // --------------

        DatabaseInterface databaseInterface = Registry.getImplFor(DatabaseInterface.class);
        Connection connection = null;
        Statement statement = null;

        try {
            connection = databaseInterface.createConnection();
            statement = connection.createStatement();

            for (Iterator<String> iter = commands.iterator(); iter.hasNext();) {
                String ddl = iter.next();
                try {
                    statement.execute(ddl);

                    SQLWarning warning = statement.getWarnings();
                    if (warning != null) {
                        logDDLError(spec, ddl, warning);
                    }
                }
                catch (Throwable t1) {
                    logDDLError(spec, ddl, t1);
                }
            }
        }
        catch (SQLException e) {
            throw new InnardsManagerException("Error occurred while executing Drop-DDL for: "
                    + spec.getFQN(), e);
        }
        finally {
            streamcruncher.innards.util.Helper.closeStatement(statement);
            streamcruncher.innards.util.Helper.closeConnection(connection);
        }
    }

    private void logDDLError(TableSpec spec, String ddl, Throwable t1) {
        String msg = "Error/Warning occurred while executing Drop-DDL/clean-up for: "
                + spec.getFQN();
        msg = msg + System.getProperty("line.separator");
        msg = msg
                + "This command will have to be executed manually on the DB if the artifact exists: ";
        msg = msg + System.getProperty("line.separator") + "[" + ddl + "]";

        Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                InnardsManager.class.getName());
        logger.log(Level.WARNING, msg, t1);
    }
}
