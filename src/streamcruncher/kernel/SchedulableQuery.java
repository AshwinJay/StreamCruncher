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
package streamcruncher.kernel;

import java.io.ObjectStreamException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import streamcruncher.api.artifact.RunningQuery;
import streamcruncher.api.artifact.TableFQN;
import streamcruncher.api.artifact.TableSpec;
import streamcruncher.boot.Registry;
import streamcruncher.innards.core.EventBucket;
import streamcruncher.innards.core.FilterInfo;
import streamcruncher.innards.core.filter.FilteredTable;
import streamcruncher.innards.core.filter.TableFilter;
import streamcruncher.innards.core.filter.TableFilterClassLoaderManager;
import streamcruncher.innards.core.partition.RowStatus;
import streamcruncher.innards.core.partition.correlation.Correlator;
import streamcruncher.innards.core.partition.custom.CustomMatcher;
import streamcruncher.innards.core.partition.inmem.InMemMaster;
import streamcruncher.innards.db.Constants;
import streamcruncher.innards.util.Helper;
import streamcruncher.util.FixedKeyHashMap;
import streamcruncher.util.LoggerManager;
import streamcruncher.util.ReusableCountDownLatch;
import streamcruncher.util.RowStatusHelper;
import streamcruncher.util.SimpleJob;
import streamcruncher.util.SimpleJobBatchExecutionException;
import streamcruncher.util.SimpleJobFixedBatchExecutor;
import streamcruncher.util.TimeKeeper;

/*
 * Author: Ashwin Jayaprakash Date: Jan 7, 2006 Time: 8:06:30 AM
 */

public abstract class SchedulableQuery extends RunningQuery implements QueryConfigChangeListener {
    protected final QueryConfig queryConfig;

    protected final QueryContext queryContext;

    /**
     * The same order in which {@link RunningQuery#filteredTables} are stored.
     */
    protected transient final TableFilter[] filters;

    protected transient final SimpleJobFixedBatchExecutor batchExecutor;

    protected transient final FixedKeyHashMap<String, QueryContextAdaptor> contextAdaptors;

    protected transient final TimeKeeper timeKeeper;

    protected transient final Correlator correlator;

    protected transient final InMemMaster inMemMaster;

    protected transient final CustomMatcher customMatcher;

    // ---------------

    protected transient long lastRowIdInsertedNow;

    protected transient Connection connection;

    protected transient PreparedStatement thePreparedStatement;

    protected transient java.sql.Statement lastRowIdStmt;

    // ---------------

    /**
     * Create a new one with the details in the parameter provided.
     * 
     * @param query
     * @throws KernelException
     */
    protected SchedulableQuery(RunningQuery query) throws KernelException {
        this(query.getName(), query.getThePreparedStatementSQL(), query.getFilteredTables(), query
                .getProcessingSpec(), query.getStatusAndPositions(), query.getResultTableFQN(),
                query.getTableFQNAndSpecMap(), query.getLastRowIdInResultTableSQL(), query
                        .getCachedSubQueries(), null);
    }

    /**
     * Create a new one with the details in the parameter provided.
     * 
     * @param query
     * @throws KernelException
     */
    protected SchedulableQuery(SchedulableQuery query) throws KernelException {
        this(query.getName(), query.getThePreparedStatementSQL(), query.getFilteredTables(), query
                .getProcessingSpec(), query.getStatusAndPositions(), query.getResultTableFQN(),
                query.getTableFQNAndSpecMap(), query.getLastRowIdInResultTableSQL(), query
                        .getCachedSubQueries(), query.getQueryConfig());
    }

    /**
     * <p>
     * If this Query uses {@link TableFilter}s, then the
     * {@link TableFilterClassLoaderManager} must already have the mappings for
     * the Filters provided.
     * </p>
     * 
     * @param queryName
     * @param thePreparedStatementSQL
     * @param filteredTables
     * @param processingSpec
     * @param statusAndPositions
     * @param resultTableFQN
     * @param tableFQNAndSpecMap
     * @param lastRowIdInResultTableSQL
     * @param cachedSubQueries
     * @param qc
     *            Can be <code>null</code>.
     * @throws KernelException
     */
    @SuppressWarnings("unchecked")
    protected SchedulableQuery(String queryName, String thePreparedStatementSQL,
            FilteredTable[] filteredTables, Object processingSpec,
            EnumMap<RowStatus, ? extends List<Integer>> statusAndPositions,
            TableFQN resultTableFQN, Map<String, TableSpec> tableFQNAndSpecMap,
            String lastRowIdInResultTableSQL, Set<String> cachedSubQueries, QueryConfig qc)
            throws KernelException {
        super(queryName, thePreparedStatementSQL, filteredTables, processingSpec,
                statusAndPositions, resultTableFQN, tableFQNAndSpecMap, lastRowIdInResultTableSQL,
                cachedSubQueries);

        // ---------------

        try {
            TableFilterClassLoaderManager clManager = Registry
                    .getImplFor(TableFilterClassLoaderManager.class);

            filters = new TableFilter[filteredTables.length];
            if (filteredTables.length > 0) {
                for (int i = 0; i < filteredTables.length; i++) {
                    String className = filteredTables[i].getTableFilterClassName();

                    ClassLoader classLoader = clManager.getClassLoader(className);
                    if (classLoader == null) {
                        classLoader = this.getClass().getClassLoader();

                        Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                                SchedulableQuery.class.getName());
                        logger.log(Level.WARNING, "ClassLoader was not set using: "
                                + TableFilterClassLoaderManager.class.getSimpleName()
                                + ", for the Filter: " + className);
                    }

                    Class clazz = Class.forName(className, true, classLoader);
                    Class<TableFilter> theClazz = clazz.asSubclass(TableFilter.class);

                    filters[i] = theClazz.newInstance();
                    filters[i].init(queryName, filteredTables[i]);
                }
            }

            if (correlationSpec != null) {
                this.correlator = new Correlator(queryName, correlationSpec, filters);
            }
            else {
                this.correlator = null;
            }

            if (memSpec != null) {
                this.inMemMaster = new InMemMaster(queryName, memSpec, filters[0]);
            }
            else {
                this.inMemMaster = null;
            }

            if (customSpec != null) {
                this.customMatcher = new CustomMatcher(queryName, customSpec, filters);
            }
            else {
                this.customMatcher = null;
            }

            this.lastRowIdInsertedNow = Constants.DEFAULT_MONOTONIC_ID_VALUE;

            this.queryConfig = new QueryConfig(filteredTables, this);
            if (qc != null) {
                this.queryConfig.copyConfig(qc);
            }

            this.queryContext = new QueryContext(filteredTables, this.queryConfig);
            this.timeKeeper = Registry.getImplFor(TimeKeeper.class);

            // ---------------

            FilterJob[] filterJobs = new FilterJob[this.filteredTables.length];

            // ---------------

            Set<String> keys = new HashSet<String>();

            for (EventBucket bucket : eventBuckets) {
                String key = bucket.getSourceTableFQN().getFQN();
                keys.add(key);
            }

            contextAdaptors = new FixedKeyHashMap<String, QueryContextAdaptor>(keys, null);

            // ---------------

            ReusableCountDownLatch countDownLatch = new ReusableCountDownLatch();

            int i = 0;
            for (FilteredTable filteredTable : filteredTables) {
                String fqn = filteredTable.getSourceTableFQN().getFQN();

                QueryContextAdaptor adaptor = new QueryContextAdaptorTF(this.queryContext, fqn,
                        this.filters[i]);
                contextAdaptors.put(fqn, adaptor);

                filterJobs[i] = new FilterJob(filteredTable.getTargetTableFQN().getAliasOrFQN(),
                        adaptor, countDownLatch, filteredTable, this.filters[i]);

                i++;
            }

            batchExecutor = new SimpleJobFixedBatchExecutor(filterJobs, queryName + ": Jobs",
                    countDownLatch);
        }
        catch (Exception e) {
            throw new KernelException(e);
        }
    }

    /**
     * @return Clean new instance based on the minimum data that was persisted.
     * @throws ObjectStreamException
     */
    protected abstract Object readResolve() throws ObjectStreamException;

    /**
     * @return the queryConfig
     */
    public QueryConfig getQueryConfig() {
        return queryConfig;
    }

    /**
     * @return Returns the queryContext.
     */
    public QueryContext getQueryContext() {
        return queryContext;
    }

    public QueryContextAdaptor getContextAdaptor(String fqn) {
        return contextAdaptors.get(fqn);
    }

    public Set<String> getContextAdaptorKeys() {
        return contextAdaptors.getKeys();
    }

    // ---------------

    /**
     * @return
     * @throws SimpleJobBatchExecutionException
     */
    public void prepareToRun() throws SimpleJobBatchExecutionException {
        batchExecutor.setStuckJobInterruptionTimeMsecs(queryConfig
                .getStuckJobInterruptionTimeMsecs());
        batchExecutor.runJobs();
    }

    /**
     * <p>
     * Runs all the
     * {@link TableFilter#filter(streamcruncher.innards.core.QueryContext)} in
     * parallel. Readies the PreparedStatementWrapper using the Windows for
     * running.
     * </p>
     * <p>
     * Runs the Query.
     * </p>
     * 
     * @return {@link DBQueryOutput#getStartIdExclusive()} element could be
     *         <code>-1</code>. If it is <code>-1</code>, then it means
     *         that the results were the first in the BatchResult-Table.
     *         {@link DBQueryOutput#getEndIdInclusive()} indicates the <b>last</b>
     *         Row-Id of the results that were inserted. Or, <code>null</code>
     *         if no rows were inserted.
     * @throws Exception
     */
    public QueryOutput run() throws Exception {
        boolean success = false;
        QueryOutput output = null;

        if (correlator != null || inMemMaster != null || customMatcher != null) {
            List<Object[]> rows = inMemMaster == null ? (correlator == null ? customMatcher
                    .onCycleEnd() : correlator.onCycleEnd()) : inMemMaster.onCycleEnd();

            if (rows.isEmpty()) {
                success = true;
                return null;
            }

            output = new DirectQueryOutput(rows);
        }
        else {
            connection = queryContext.createConnection();

            try {
                connection.setAutoCommit(false);
                connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);

                thePreparedStatement = connection.prepareStatement(thePreparedStatementSQL);

                RowStatusHelper.setStatusValues(statusAndPositions, thePreparedStatement,
                        queryContext.getRunCount());

                // ---------------

                // Use the previous run's last Id.
                long newResultIdsAfter = lastRowIdInsertedNow;

                int rows = thePreparedStatement.executeUpdate();
                if (rows <= 0) {
                    success = true;
                    return null;
                }

                // ---------------

                lastRowIdInsertedNow = getLastRowIdInResultTable(newResultIdsAfter, rows);
                output = new DBQueryOutput(newResultIdsAfter, lastRowIdInsertedNow, rows,
                        timeKeeper.getTimeMsecs());

                success = true;
            }
            finally {
                if (connection != null) {
                    if (success) {
                        connection.commit();
                    }
                    else {
                        connection.rollback();
                    }
                }
            }
        }

        return output;
    }

    /**
     * @param startId
     * @param rows
     * @return {@link Constants#DEFAULT_MONOTONIC_ID_VALUE} if there were no
     *         Rows available.
     * @throws SQLException
     */
    protected long getLastRowIdInResultTable(long startId, int rows) throws SQLException {
        // Gets the precise value only every 50 runs.
        if (queryContext.getRunCount() % 50 != 0) {
            return startId < 0 ? rows : (startId + rows);
        }

        long lastRowId = Constants.DEFAULT_MONOTONIC_ID_VALUE;

        if (lastRowIdStmt == null) {
            lastRowIdStmt = connection.createStatement();
        }

        ResultSet resultSet = null;
        try {
            resultSet = lastRowIdStmt.executeQuery(lastRowIdInResultTableSQL);
            while (resultSet.next()) {
                lastRowId = resultSet.getLong(Constants.ID_COLUMN_POS + 1);
            }
        }
        finally {
            Helper.closeResultSet(resultSet);
        }

        return lastRowId;
    }

    /**
     * Perform any DB close and other clean-up operations etc after the Query
     * has been processed.
     * <p>
     * <b>Note:</b> This method must be invoked irrespective of whether the
     * {@link #run()} succeeded or not.
     * </p>
     */
    public void afterRun() {
        Helper.closeStatement(lastRowIdStmt);
        lastRowIdStmt = null;

        Helper.closeStatement(thePreparedStatement);
        thePreparedStatement = null;

        Helper.closeConnection(connection);
        connection = null;
    }

    // ---------------

    public void discard() {
        for (TableFilter<? extends FilterInfo> filter : filters) {
            filter.discard();
        }

        connection = null;
        thePreparedStatement = null;
        lastRowIdStmt = null;
        queryContext.discard();
    }

    // ---------------

    protected static class FilterJob extends SimpleJob {
        protected final FilteredTable filteredTable;

        protected final TableFilter filter;

        protected final streamcruncher.innards.core.QueryContext queryContext;

        protected FilterJob(String name, streamcruncher.innards.core.QueryContext queryContext,
                ReusableCountDownLatch countDownLatch, FilteredTable filteredTable,
                TableFilter filter) {
            super(name, countDownLatch);

            this.filteredTable = filteredTable;
            this.filter = filter;
            this.queryContext = queryContext;
        }

        /**
         * @return Returns the filter.
         */
        protected TableFilter getFilter() {
            return filter;
        }

        /**
         * @return Returns the filteredTable.
         */
        protected FilteredTable getFilteredTable() {
            return filteredTable;
        }

        // ---------------

        @Override
        public void doWork() throws Exception {
            filteredTable.forceFilter();
            filter.filter(queryContext);
        }
    }
}
