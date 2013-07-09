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

import java.util.concurrent.atomic.AtomicLong;

/*
 * Author: Ashwin Jayaprakash Date: Jul 13, 2007 Time: 8:28:38 PM
 */

public class CachedDataStats {
    protected final AtomicLong hitCount;

    protected final AtomicLong missCount;

    protected volatile long lastHitTime;

    protected volatile long lastMissTime;

    public CachedDataStats() {
        this.hitCount = new AtomicLong(0);
        this.missCount = new AtomicLong(0);
    }

    public AtomicLong getHitCount() {
        return hitCount;
    }

    public long getLastHitTime() {
        return lastHitTime;
    }

    public long getLastMissTime() {
        return lastMissTime;
    }

    public AtomicLong getMissCount() {
        return missCount;
    }

    public void hit() {
        hitCount.incrementAndGet();
        lastHitTime = System.currentTimeMillis();
    }

    public void miss() {
        missCount.incrementAndGet();
        lastMissTime = System.currentTimeMillis();
    }
}
