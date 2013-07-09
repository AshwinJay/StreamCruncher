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
package streamcruncher.innards.core.filter;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import streamcruncher.api.artifact.RowSpec;
import streamcruncher.api.artifact.TableFQN;
import streamcruncher.boot.Registry;
import streamcruncher.innards.core.EventBucketClient;
import streamcruncher.innards.core.FilterInfo;
import streamcruncher.innards.core.WhereClauseSpec;
import streamcruncher.innards.impl.expression.ExpressionEvaluationException;
import streamcruncher.innards.impl.expression.OgnlRowEvaluator;
import streamcruncher.kernel.JobExecutionManager;
import streamcruncher.util.LoggerManager;
import streamcruncher.util.PerpetualResultSet;
import streamcruncher.util.RowEvaluator;
import streamcruncher.util.TwoDAppendOnlyList;
import streamcruncher.util.TwoDAppendOnlyList.Reader;
import streamcruncher.util.sysevent.SystemEvent;
import streamcruncher.util.sysevent.SystemEventBus;
import streamcruncher.util.sysevent.SystemEvent.Priority;

/*
 * Author: Ashwin Jayaprakash Date: Jan 18, 2007 Time: 1:33:56 PM
 */

public abstract class PreFilter implements Runnable, FilterInfo, Serializable {
    private static final long serialVersionUID = 1L;

    protected final String queryName;

    protected final FilterSpec filterSpec;

    protected final AtomicBoolean busyFlag;

    // --------

    protected transient TwoDAppendOnlyList submittedEvents;

    protected transient Reader reader;

    protected transient RowEvaluator filter;

    protected transient EventBucketClient eventBucketClient;

    protected transient PerpetualResultSet perpetualResultSet;

    protected transient JobExecutionManager executionManager;

    /**
     * Invoke {@link #init()} to initialize this instance, before use.
     * 
     * @param queryName
     * @param filterSpec
     */
    public PreFilter(String queryName, FilterSpec filterSpec) {
        this.queryName = queryName;
        this.filterSpec = filterSpec;
        this.busyFlag = new AtomicBoolean(false);
    }

    protected abstract Object writeReplace() throws ObjectStreamException;

    public FilterSpec getFilterSpec() {
        return filterSpec;
    }

    public PerpetualResultSet getPerpetualResultSet() {
        return perpetualResultSet;
    }

    public EventBucketClient getPreFilterClient() {
        return eventBucketClient;
    }

    public boolean isBusy() {
        return busyFlag.get();
    }

    public boolean attemptSetBusy() {
        return busyFlag.compareAndSet(false, true);
    }

    public void setNotBusy() {
        busyFlag.set(false);
    }

    public abstract String getIdColumnName();

    public abstract TableFQN getSourceTableFQN();

    public abstract RowSpec getSourceTableRowSpec();

    public abstract TableFQN getTargetTableFQN();

    // ----------

    public void setStreamDataBuffer(TwoDAppendOnlyList list) {
        this.reader = list.createReader();
    }

    public void setEventBucketClient(EventBucketClient eventBucketClient) {
        this.eventBucketClient = eventBucketClient;
    }

    public void init() throws ExpressionEvaluationException {
        String[] columnNames = filterSpec.getColNameArrToSelectFromSrc();
        this.perpetualResultSet = new PerpetualResultSet(columnNames);

        WhereClauseSpec whereClauseSpec = filterSpec.getWhereClauseSpec();
        if (whereClauseSpec != null) {
            this.filter = new OgnlRowEvaluator(queryName, whereClauseSpec.getWhereClause(),
                    filterSpec.getSourceTableRowSpec(), whereClauseSpec.getContext(),
                    whereClauseSpec.getSubQueries());
        }
        else {
            this.filter = null;
        }

        this.executionManager = Registry.getImplFor(JobExecutionManager.class);
    }

    /**
     * Filter asynchronously.
     */
    public void eventsReceived() {
        if (attemptSetBusy()) {
            executionManager.submitJob(this);
        }
    }

    public void run() {
        try {
            int c = 0;

            // Keep looping for sometime. Then "yield".
            while (c <= 5) {
                int fetchedRows = 0;

                try {
                    fetchedRows = perpetualResultSet.pumpRows(reader, filter);
                }
                catch (ExpressionEvaluationException e) {
                    Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                            PreFilter.class.getName());
                    logger.log(Level.SEVERE,
                            "An error occurred while running the PreFilter for the Query: "
                                    + queryName, e);

                    SystemEventBus bus = Registry.getImplFor(SystemEventBus.class);
                    SystemEvent event = new SystemEvent(PreFilter.class.getName(), queryName, e,
                            Priority.SEVERE);
                    bus.submit(event);
                }

                if (fetchedRows > 0) {
                    /*
                     * Notify here. Not done for sync filtering because the
                     * pending-Events number would've got cleared by then. The
                     * number should not get added when the Query is running.
                     */
                    eventBucketClient.eventsArrived(this, fetchedRows);
                }

                c++;
                Thread.yield();
            }
        }
        finally {
            setNotBusy();
        }
    }

    /**
     * Filter synchronously.
     * 
     * @return The number of rows available after filteration.
     */
    public int forceFilter() {
        int fetched = perpetualResultSet.getSize();
        if (fetched == 0 && attemptSetBusy() == true) {
            try {
                try {
                    fetched = perpetualResultSet.pumpRows(reader, filter);
                }
                catch (ExpressionEvaluationException e) {
                    Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                            PreFilter.class.getName());
                    logger.log(Level.SEVERE,
                            "An error occurred while running the PreFilter for the Query: "
                                    + queryName, e);

                    SystemEventBus bus = Registry.getImplFor(SystemEventBus.class);
                    SystemEvent event = new SystemEvent(PreFilter.class.getName(), queryName, e,
                            Priority.SEVERE);
                    bus.submit(event);
                }
            }
            finally {
                setNotBusy();
            }
        }

        return fetched;
    }

    // ----------

    /**
     * @return The number of rows in the buffer after filteration.
     */
    public int getNumEventsInBucket() {
        return perpetualResultSet.getSize();
    }
}
