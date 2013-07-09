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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentMap;

import streamcruncher.innards.core.QueryContext;

/*
 * Author: Ashwin Jayaprakash Date: Aug 9, 2006 Time: 2:17:11 PM
 */

public abstract class QueryContextAdaptor implements QueryContext {
    protected final streamcruncher.kernel.QueryContext context;

    protected final QueryConfig config;

    protected final String fqn;

    protected final SortedSet<Long> expirationTimes;

    public QueryContextAdaptor(streamcruncher.kernel.QueryContext context, String fqn) {
        this.context = context;
        this.config = context.getQueryConfig();
        this.fqn = fqn;

        this.expirationTimes = context.getEventExpirationTimes(fqn);
    }

    public streamcruncher.kernel.QueryContext getContext() {
        return context;
    }

    public String getFqn() {
        return fqn;
    }

    // --------------

    public void addEventExpirationTime(long ts) {
        expirationTimes.add(ts);
    }

    public Connection createConnection() throws SQLException {
        return context.createConnection();
    }

    public long getCurrentTime() {
        return context.getCurrentTime();
    }

    public long getRunCount() {
        return context.getRunCount();
    }

    public ConcurrentMap getMap() {
        return context.getMap();
    }

    public int getTotalUnprocessedBufferedRows() {
        return context.getTotalUnprocessedBufferedRows(fqn);
    }

    public void setTotalUnprocessedBufferedRows(int totalUnprocessedBufferedRows) {
        context.setTotalUnprocessedBufferedRows(fqn, totalUnprocessedBufferedRows);
    }

    public int getPendingEventsAllowed() {
        return config.getAllowedPendingEvents(fqn);
    }
}
