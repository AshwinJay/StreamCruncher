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

import java.awt.Window;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import streamcruncher.boot.Registry;
import streamcruncher.innards.core.filter.FilteredTable;
import streamcruncher.innards.core.filter.TableFilter;
import streamcruncher.innards.core.partition.Partitioner;
import streamcruncher.innards.db.DatabaseInterface;
import streamcruncher.util.FixedKeyHashMap;

/*
 * Author: Ashwin Jayaprakash Date: Aug 9, 2006 Time: 1:42:08 PM
 */
/**
 * There is one context, per Query. Once created, the <b>same context</b> will
 * be used throughout the Query's life.
 */
public class QueryContext implements Serializable {
    private static final long serialVersionUID = 1L;

    protected final FilteredTable[] filteredTables;

    /**
     * General purpose storage. Will not be serialized.
     */
    protected transient final ConcurrentMap map;

    protected transient final DatabaseInterface databaseInterface;

    protected final QueryConfig queryConfig;

    /**
     * Current Time in Milliseconds.
     */
    protected transient long currentTime;

    /**
     * Used for Partition's version-column, because
     * {@link System#currentTimeMillis()} does not change between fast,
     * successive runs on "MS Windows".
     */
    protected final AtomicLong runCount;

    protected final Set<String> keys;

    /**
     * The timestamps at which the Events in the Functions, will expire; in
     * sorted order.
     */
    protected transient final FixedKeyHashMap<String, SortedSet<Long>> eventExpirationTimes;

    /**
     * The total number of Rows that are still in the Buffers which, forces the
     * Query to be scheduled.
     */
    protected transient final FixedKeyHashMap<String, Integer> totalUnprocessedBufferedRows;

    /**
     * When the Query is about to be run, the
     * {@linkplain Partitioner Partitions} (TableFilters) are executed. Since
     * Partitions support pre-Filters, this Run could've been triggered by
     * Events that do not make it through the Filter and thus result in a
     * spurious Run. To avoid this, a Partition is allowed to Veto a scheduled
     * Run of the Query, provided the Windows or other Partitions do not
     * genuinely require a Run.
     */
    protected transient final ConcurrentMap<TableFilter, Boolean> tblFilterAndRequireQryRunFlg;

    protected transient final ConcurrentMap<Window, Boolean> windowAndRequireQryRunFlg;

    // ----------------------

    /**
     * @param filteredTables
     * @param queryConfig
     */
    public QueryContext(FilteredTable[] filteredTables, QueryConfig queryConfig) {
        this.filteredTables = filteredTables;
        this.queryConfig = queryConfig;

        this.map = new ConcurrentHashMap();
        this.databaseInterface = Registry.getImplFor(DatabaseInterface.class);

        this.keys = Collections.unmodifiableSet(queryConfig.getKeys());

        this.eventExpirationTimes = new FixedKeyHashMap<String, SortedSet<Long>>(keys, null);
        for (String key : keys) {
            this.eventExpirationTimes.put(key, new TreeSet<Long>());
        }

        this.totalUnprocessedBufferedRows = new FixedKeyHashMap<String, Integer>(keys, 0);

        this.runCount = new AtomicLong(0);

        this.tblFilterAndRequireQryRunFlg = new ConcurrentHashMap<TableFilter, Boolean>();
        this.windowAndRequireQryRunFlg = new ConcurrentHashMap<Window, Boolean>();
    }

    /**
     * @return Creates a new instance, so that references to other components
     *         like {@link #databaseInterface} etc are loaded afresh.
     * @throws ObjectStreamException
     */
    protected Object readResolve() throws ObjectStreamException {
        return new QueryContext(filteredTables, queryConfig);
    }

    // ----------------------

    public QueryConfig getQueryConfig() {
        return queryConfig;
    }

    public long getRunCount() {
        return runCount.get();
    }

    /**
     * @return
     */
    public long getCurrentTime() {
        return currentTime;
    }

    /**
     * @return Unmodifiable.
     */
    public Set<String> getKeys() {
        return keys;
    }

    /**
     * @param fqn
     *            {@link Window}'s or {@link FilteredTable}'s
     *            {@linkplain Window#getSourceTableFQN() FQN}.
     * @return Returns the eventExpirationTimes. Use
     *         {@link java.util.Set#isEmpty()} instead of
     *         <code>size() > 0</code>.
     */
    public SortedSet<Long> getEventExpirationTimes(String fqn) {
        return eventExpirationTimes.get(fqn);
    }

    public Set<String> getEventExpirationTimeKeys() {
        return eventExpirationTimes.getKeys();
    }

    /**
     * @param fqn
     * @return Returns the totalUnprocessedBufferedRows.
     */
    public int getTotalUnprocessedBufferedRows(String fqn) {
        return totalUnprocessedBufferedRows.get(fqn);
    }

    public Set<String> getTotalUnprocessedBufferedRowKeys() {
        return totalUnprocessedBufferedRows.getKeys();
    }

    /**
     * @param fqn
     * @param unprocessedBufferedRows
     */
    public void setTotalUnprocessedBufferedRows(String fqn, int unprocessedBufferedRows) {
        this.totalUnprocessedBufferedRows.put(fqn, unprocessedBufferedRows);
    }

    /**
     * @param currentTime
     */
    public void setCurrentTime(long currentTime) {
        this.currentTime = currentTime;
    }

    public void incrementRunCount() {
        this.runCount.incrementAndGet();
    }

    /**
     * @throws SQLException
     */
    public Connection createConnection() throws SQLException {
        return databaseInterface.createConnection();
    }

    public ConcurrentMap getMap() {
        return map;
    }

    // ----------------------

    public void discard() {
        map.clear();
        eventExpirationTimes.clear();
        totalUnprocessedBufferedRows.clear();
    }
}
