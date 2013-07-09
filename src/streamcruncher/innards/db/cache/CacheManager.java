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

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import streamcruncher.boot.Component;
import streamcruncher.boot.ConfigKeys;
import streamcruncher.boot.Registry;
import streamcruncher.innards.db.DatabaseInterface;
import streamcruncher.innards.file.FileManager;
import streamcruncher.innards.file.FileManagerException;
import streamcruncher.util.LoggerManager;

/*
 * Author: Ashwin Jayaprakash Date: Jul 12, 2007 Time: 7:39:01 PM
 */

public class CacheManager implements Component {
    protected final ConcurrentMap<String, SchedulableCachedData> cache;

    protected RefreshMaster refreshMaster;

    protected DatabaseInterface dbInterface;

    public CacheManager() {
        this.cache = new ConcurrentHashMap<String, SchedulableCachedData>();
    }

    public void start(Object... params) throws Exception {
        Properties properties = (Properties) params[0];

        // ------------

        String numThreadsStr = properties.getProperty(ConfigKeys.ResultSetCacheRefresh.THREADS_NUM);
        int numThreads = Integer.parseInt(numThreadsStr);

        refreshMaster = new RefreshMaster(cache, numThreads);

        dbInterface = Registry.getImplFor(DatabaseInterface.class);

        // -------------

        Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                CacheManager.class.getName());
        logger.log(Level.INFO, "Started");
    }

    public void stop() throws Exception {
        refreshMaster.stop();

        dbInterface = null;

        // -------------

        Logger logger = Registry.getImplFor(LoggerManager.class).getLogger(
                CacheManager.class.getName());
        logger.log(Level.INFO, "Stopped");
    }

    // --------------

    public void addCachedData(SchedulableCachedData cachedData) {
        cachedData.init(refreshMaster);
        cache.put(cachedData.getSql(), cachedData);
    }

    public CachedData getCachedData(String query) {
        return cache.get(query);
    }

    public ConcurrentMap<String, SchedulableCachedData> getAllCachedData() {
        return cache;
    }

    /**
     * @param sql
     * @param queryName
     *            Can be <code>null</code>
     * @throws CacheException
     */
    public void registerCachedData(String sql, String queryName) throws CacheException {
        SchedulableCachedData cachedData = cache.get(sql);

        if (cachedData == null) {
            cachedData = new SchedulableCachedData(sql);

            FileManager fileManager = Registry.getImplFor(FileManager.class);
            try {
                fileManager.saveCachedData(cachedData, false);
            }
            catch (FileManagerException e) {
                throw new CacheException(e);
            }

            cache.put(sql, cachedData);
            cachedData.init(refreshMaster);
        }

        if (queryName != null) {
            cachedData.addListenerQueryName(queryName);
        }
    }

    public void unregisterCachedData(String sql, String queryName) {
        SchedulableCachedData cachedData = cache.get(sql);

        if (cachedData != null) {
            int remaining = cachedData.removeListenerQueryName(queryName);
            if (remaining == 0) {
                cache.remove(sql);

                FileManager fileManager = Registry.getImplFor(FileManager.class);
                fileManager.deleteCachedData(cachedData);
            }
        }
    }
}
