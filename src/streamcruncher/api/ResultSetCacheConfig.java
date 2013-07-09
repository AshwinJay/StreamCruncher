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

import java.io.Serializable;

/*
 * Author: Ashwin Jayaprakash Date: Jul 12, 2007 Time: 7:47:20 PM
 */

/**
 * <p>
 * The Kernel performs many operations inside its own Process. There are also a
 * few operations that are performed inside the underlying Database. However, it
 * interfaces seamlessly and transparently with the Database, which is also why
 * the Query Language is an extension of SQL.
 * </p>
 * <p>
 * To improve performance and to reduce latency, the Kernel handles some of the
 * Pre-Filter Queries on its own instead of going to the Database. In cases
 * where the Pre-Filter references a Database Table - via the
 * <code>.. IN (SELECT .. FROM ..)</code> or the
 * <code>NOT IN (SELECT .. FROM ..)</code> clauses, the Kernel caches the
 * results of those <b>Sub-Queries</b>. This class is used to specify how often
 * the results of those Sub-Query fragments have to refreshed by the Kernel i.e
 * how often it should be fetched from the Database. This class <b>is
 * Thread-safe</b>.
 * </p>
 */
public class ResultSetCacheConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * {@value}
     */
    public static final long defaultRefreshIntervalMsecs = 1000 * 60 * 60;

    protected volatile long refreshIntervalMsecs;

    public ResultSetCacheConfig() {
        this.refreshIntervalMsecs = defaultRefreshIntervalMsecs;
    }

    public long getRefreshIntervalMsecs() {
        return refreshIntervalMsecs;
    }

    public void setRefreshIntervalMsecs(long refreshIntervalMsecs) {
        this.refreshIntervalMsecs = refreshIntervalMsecs;
    }
}
