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

import streamcruncher.api.ResultSetCacheConfig;

/*
 * Author: Ashwin Jayaprakash Date: Jul 14, 2007 Time: 7:19:11 PM
 */

public class ObservableCacheConfig extends ResultSetCacheConfig {
    private static final long serialVersionUID = 1L;

    protected transient CacheConfigChangeListener changeListener;

    @Override
    public void setRefreshIntervalMsecs(long refreshIntervalMsecs) {
        long oldValue = getRefreshIntervalMsecs();
        super.setRefreshIntervalMsecs(refreshIntervalMsecs);

        if (changeListener != null) {
            changeListener.cacheRefreshIntervalChanged(oldValue, refreshIntervalMsecs);
        }
    }

    public CacheConfigChangeListener getChangeListener() {
        return changeListener;
    }

    public void setChangeListener(CacheConfigChangeListener changeListener) {
        this.changeListener = changeListener;
    }

    public interface CacheConfigChangeListener {
        public void cacheRefreshIntervalChanged(long oldRefreshIntervalMsecs,
                long newRefreshIntervalMsecs);
    }
}
