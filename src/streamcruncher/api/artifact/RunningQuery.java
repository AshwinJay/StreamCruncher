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
package streamcruncher.api.artifact;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import streamcruncher.innards.core.EventBucket;
import streamcruncher.innards.core.EventBucketClient;
import streamcruncher.innards.core.filter.FilteredTable;
import streamcruncher.innards.core.partition.RowStatus;
import streamcruncher.innards.core.partition.correlation.CorrelationSpec;
import streamcruncher.innards.core.partition.custom.CustomSpec;
import streamcruncher.innards.core.partition.inmem.InMemSpec;

/*
 * Author: Ashwin Jayaprakash Date: Feb 2, 2006 Time: 10:06:33 PM
 */

public class RunningQuery implements EventBucketClient, Serializable {
    private static final long serialVersionUID = 1L;

    protected final String name;

    protected final String thePreparedStatementSQL;

    protected final EventBucket[] eventBuckets;

    protected final FilteredTable[] filteredTables;

    protected final InMemSpec memSpec;

    protected final CorrelationSpec correlationSpec;

    protected final CustomSpec customSpec;

    protected final EnumMap<RowStatus, ArrayList<Integer>> statusAndPositions;

    protected final Set<String> cachedSubQueries;

    protected final TableFQN resultTableFQN;

    /**
     * Key is {@link TableFQN#getFQN() FQN}
     */
    protected final Map<String, TableSpec> tableFQNAndSpecMap;

    protected final String lastRowIdInResultTableSQL;

    /**
     * @param queryName
     * @param thePreparedStatementSQL
     *            <code>null</code> if <code>processingSpec</code> not
     *            <code>null</code>.
     * @param filteredTables
     *            Provide empty-array when there are no
     *            <code>FilteredTable</code>s.
     * @param processingSpec
     * @param statusAndPositions
     * @param resultTableFQN
     * @param tableFQNAndSpecMap
     * @param lastRowIdInResultTableSQL
     *            <code>null</code> if <code>processingSpec</code> is not
     *            <code>null</code>.
     * @param cachedSubQueries
     */
    public RunningQuery(String queryName, String thePreparedStatementSQL,
            FilteredTable[] filteredTables, Object processingSpec,
            EnumMap<RowStatus, ? extends List<Integer>> statusAndPositions,
            TableFQN resultTableFQN, Map<String, TableSpec> tableFQNAndSpecMap,
            String lastRowIdInResultTableSQL, Set<String> cachedSubQueries) {
        this.name = queryName;

        this.thePreparedStatementSQL = thePreparedStatementSQL;

        this.filteredTables = filteredTables;
        this.eventBuckets = new EventBucket[filteredTables.length];

        this.memSpec = processingSpec instanceof InMemSpec ? (InMemSpec) processingSpec : null;
        this.correlationSpec = processingSpec instanceof CorrelationSpec ? (CorrelationSpec) processingSpec
                : null;
        this.customSpec = processingSpec instanceof CustomSpec ? (CustomSpec) processingSpec : null;

        int counter = 0;
        for (FilteredTable table : filteredTables) {
            this.eventBuckets[counter++] = table;
        }

        this.statusAndPositions = new EnumMap<RowStatus, ArrayList<Integer>>(RowStatus.class);
        for (RowStatus rowStatus : RowStatus.values()) {
            List<Integer> list = statusAndPositions.get(rowStatus);

            ArrayList<Integer> newList = null;
            if (list == null) {
                newList = new ArrayList<Integer>(0);
            }
            else {
                newList = new ArrayList<Integer>(list.size());

                for (Integer i : list) {
                    newList.add(i);
                }
            }

            this.statusAndPositions.put(rowStatus, newList);
        }

        this.cachedSubQueries = cachedSubQueries;
        this.resultTableFQN = resultTableFQN;
        this.tableFQNAndSpecMap = tableFQNAndSpecMap;
        this.lastRowIdInResultTableSQL = lastRowIdInResultTableSQL;
    }

    /**
     * @return Returns the thePreparedStatementSQL.
     */
    public String getThePreparedStatementSQL() {
        return thePreparedStatementSQL;
    }

    /**
     * @return the tableFQNAndSpecMap
     */
    public Map<String, TableSpec> getTableFQNAndSpecMap() {
        return tableFQNAndSpecMap;
    }

    /**
     * @return Returns the resultTableFQN.
     */
    public TableFQN getResultTableFQN() {
        return resultTableFQN;
    }

    /**
     * @return Returns the lastRowIdInResultTableSQL.
     */
    public String getLastRowIdInResultTableSQL() {
        return lastRowIdInResultTableSQL;
    }

    /**
     * @return All the {@link #windows} and {@link #filteredTables}.
     */
    public EventBucket[] getEventBuckets() {
        return eventBuckets;
    }

    /**
     * @return Returns the allFilteredTables.
     */
    public FilteredTable[] getFilteredTables() {
        return filteredTables;
    }

    public Object getProcessingSpec() {
        return customSpec == null ? (memSpec == null ? correlationSpec : memSpec) : customSpec;
    }

    public CustomSpec getCustomSpec() {
        return customSpec;
    }

    public InMemSpec getMemSpec() {
        return memSpec;
    }

    public CorrelationSpec getCorrelationSpec() {
        return correlationSpec;
    }

    /**
     * @return Returns the statusAndPositions.
     */
    public EnumMap<RowStatus, ArrayList<Integer>> getStatusAndPositions() {
        return statusAndPositions;
    }

    /**
     * @return Returns the Sub-Queries in the Pre-Filters that are cached by the
     *         Kernel.
     */
    public Set<String> getCachedSubQueries() {
        return cachedSubQueries;
    }

    /**
     * @return Returns the name.
     */
    public String getName() {
        return name;
    }

    /**
     * <p>
     * <b>Note:</b> Over-ride this.
     * </p>
     * {@inheritDoc}
     */
    public void eventsArrived(EventBucket bucket, int numOfEvents) {
    }

    /**
     * <p>
     * <b>Note:</b> Over-ride this.
     * </p>
     * {@inheritDoc}
     */
    public float getEventWeight(EventBucket bucket) {
        return 1.0f;
    }

    /**
     * <p>
     * <b>Note:</b> Over-ride this.
     * </p>
     * {@inheritDoc}
     */
    public float getTotalCurrentEventWeight() {
        return 0.0f;
    }
}
