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

/*
 * Author: Ashwin Jayaprakash Date: Feb 11, 2006 Time: 11:23:46 AM
 */

public class DBQueryOutput implements QueryOutput {
    protected final long startIdExclusive;

    protected final long endIdInclusive;

    protected final int rows;

    protected final long createTime;

    /**
     * @param startIdExclusive
     * @param endIdInclusive
     * @param rows
     *            Number of rows between the first the start and end Ids.
     * @param createTime
     */
    public DBQueryOutput(long startIdExclusive, long endIdInclusive, int rows, long createTime) {
        this.startIdExclusive = startIdExclusive;
        this.endIdInclusive = endIdInclusive;
        this.rows = rows;
        this.createTime = createTime;
    }

    /**
     * @return Returns the endIdInclusive.
     */
    public long getEndIdInclusive() {
        return endIdInclusive;
    }

    /**
     * @return Returns the startIdExclusive.
     */
    public long getStartIdExclusive() {
        return startIdExclusive;
    }

    /**
     * @return Returns the createTime.
     */
    public long getCreateTime() {
        return createTime;
    }

    /**
     * @return Returns the rows.
     */
    public int getRows() {
        return rows;
    }
}