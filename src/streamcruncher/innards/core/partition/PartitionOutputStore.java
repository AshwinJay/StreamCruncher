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
package streamcruncher.innards.core.partition;

import java.util.List;

import streamcruncher.innards.core.QueryContext;
import streamcruncher.util.AppendOnlyPrimitiveLongList;

/*
 * Author: Ashwin Jayaprakash Date: Jun 23, 2007 Time: 7:39:32 PM
 */

public abstract class PartitionOutputStore {
    protected streamcruncher.innards.core.FilterInfo filterInfo;

    protected PartitionSpec spec;

    public PartitionOutputStore(streamcruncher.innards.core.FilterInfo filterInfo) {
        this.filterInfo = filterInfo;
        this.spec = (PartitionSpec) filterInfo.getFilterSpec();
    }

    public void startBatch(QueryContext context) throws Exception {
    }

    public abstract void insertNewRow(QueryContext context, List<Row> rows) throws Exception;

    public abstract void markRowsAsDead(QueryContext context, long markValue,
            AppendOnlyPrimitiveLongList idList) throws Exception;

    public abstract void deleteDeadRows(QueryContext context) throws Exception;

    public void endBatch(QueryContext context, boolean successfulEnd) throws Exception {
    }

    public abstract void discard();
}