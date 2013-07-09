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
package streamcruncher.test.func;

import java.util.LinkedList;
import java.util.List;

/*
 * Author: Ashwin Jayaprakash Date: Sep 12, 2006 Time: 11:56:57 AM
 */

/**
 * All the Test cases use this Class to hold the results of the Query and then
 * verify the results against expected data.
 */
public class BatchResult {
    protected long timestamp;

    protected final List<Object[]> rows;

    public BatchResult() {
        this.timestamp = System.currentTimeMillis();
        this.rows = new LinkedList<Object[]>();
    }

    public List<Object[]> getRows() {
        return rows;
    }

    public void addRow(Object[] row) {
        rows.add(row);
    }

    public long getTimestamp() {
        return timestamp;
    }
}
