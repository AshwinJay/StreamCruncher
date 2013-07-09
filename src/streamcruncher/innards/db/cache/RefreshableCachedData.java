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
package streamcruncher.innards.db.cache;

import java.io.ObjectStreamException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import streamcruncher.boot.Registry;
import streamcruncher.innards.db.DatabaseInterface;
import streamcruncher.innards.util.Helper;

/*
 * Author: Ashwin Jayaprakash Date: Jul 12, 2007 Time: 8:12:43 PM
 */

public class RefreshableCachedData extends CachedData {
    private static final long serialVersionUID = 1L;

    private transient final AtomicBoolean refreshBeforeAccess;

    private transient final ReentrantLock waitForRefreshLock;

    protected final ObservableCacheConfig cacheConfig;

    protected transient DatabaseInterface dbInterface;

    protected int resultColumnCount;

    public RefreshableCachedData(String sql) {
        this(sql, null);
    }

    protected RefreshableCachedData(String sql, ObservableCacheConfig cacheConfig) {
        super(sql);

        this.cacheConfig = cacheConfig == null ? new ObservableCacheConfig() : cacheConfig;
        this.refreshBeforeAccess = new AtomicBoolean(true);
        this.waitForRefreshLock = new ReentrantLock();

        this.dbInterface = Registry.getImplFor(DatabaseInterface.class);
    }

    @Override
    protected Object readResolve() throws ObjectStreamException {
        return new RefreshableCachedData(this.getSql(), this.getCacheConfig());
    }

    @Override
    public ObservableCacheConfig getCacheConfig() {
        return cacheConfig;
    }

    /**
     * Refreshes synchronously.
     */
    @Override
    public void forceRefresh() throws CacheException {
        refreshBeforeAccess.set(true);
        getData();
    }

    @Override
    public Object getData() throws CacheException {
        Object retVal = null;

        try {
            boolean tmpRefreshRequired = refreshBeforeAccess.get();
            Object tmpData = super.getData();

            if (tmpRefreshRequired == false) {
                if (tmpData == null) {
                    stats.miss();
                }
                else {
                    retVal = tmpData;
                }
            }

            while (tmpRefreshRequired == true || tmpData == null) {
                try {
                    waitForRefreshLock.lock();

                    /*
                     * Another Thread has already refreshed while this was
                     * waiting.
                     */
                    if (refreshBeforeAccess.get() == false) {
                        retVal = super.getData();

                        // Cache has expired or the GC cleared the data.
                        if (retVal == null) {
                            stats.miss();
                            continue;
                        }

                        break;
                    }

                    try {
                        retVal = fetchDataForCache();
                        setData(retVal);
                        refreshBeforeAccess.set(false);
                        break;
                    }
                    catch (SQLException e) {
                        throw new CacheException(e);
                    }
                }
                finally {
                    waitForRefreshLock.unlock();
                }
            }
        }
        finally {
            if (retVal != null) {
                stats.hit();
            }
            else {
                stats.miss();
            }
        }

        return retVal;
    }

    /**
     * @return If the {@link #getSql()} Query returns a Result-Set with only one
     *         columns per row, then the return value will be a {@link List} of
     *         Objects. If the Result-Set return multiple columns per row, then
     *         the return value is a {@link List} where each item is an array
     *         containing that row's columns.
     * @throws SQLException
     */
    protected Object fetchDataForCache() throws SQLException {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;

        ArrayList<Object> retVal = null;

        try {
            connection = dbInterface.createConnection();
            statement = connection.createStatement();
            resultSet = statement.executeQuery(sql);

            if (resultColumnCount == 0) {
                ResultSetMetaData metaData = resultSet.getMetaData();
                resultColumnCount = metaData.getColumnCount();
            }

            retVal = new ArrayList<Object>();
            while (resultSet.next()) {
                if (resultColumnCount > 1) {
                    Object[] columns = new Object[resultColumnCount];
                    for (int i = 0; i < columns.length; i++) {
                        columns[i] = resultSet.getObject(i + 1);
                    }
                    retVal.add(columns);
                }
                else {
                    Object obj = resultSet.getObject(1);
                    retVal.add(obj);
                }
            }
            retVal.trimToSize();
        }
        finally {
            Helper.closeResultSet(resultSet);
            Helper.closeStatement(statement);
            Helper.closeConnection(connection);
        }

        return retVal;
    }

    @Override
    public void discard() {
        super.discard();

        dbInterface = null;
    }
}
