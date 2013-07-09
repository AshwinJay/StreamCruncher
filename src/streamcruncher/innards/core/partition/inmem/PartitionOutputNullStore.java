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
package streamcruncher.innards.core.partition.inmem;

import java.util.List;

import streamcruncher.innards.core.QueryContext;
import streamcruncher.innards.core.partition.PartitionOutputStore;
import streamcruncher.innards.core.partition.Row;
import streamcruncher.innards.db.Constants;
import streamcruncher.util.AppendOnlyPrimitiveLongList;

/*
 * Author: Ashwin Jayaprakash Date: Jun 23, 2007 Time: 7:39:32 PM
 */

public class PartitionOutputNullStore extends PartitionOutputStore {
    protected final int rowIdPosition;

    protected final int versionIdPosition;

    protected List<Row> insertedRows;

    protected AppendOnlyPrimitiveLongList deletedRowIds;

    public PartitionOutputNullStore(streamcruncher.innards.core.FilterInfo filterInfo) {
        super(filterInfo);

        this.rowIdPosition = Constants.ID_COLUMN_POS;
        this.versionIdPosition = Constants.VERSION_COLUMN_POS;
    }

    @Override
    public void insertNewRow(QueryContext context, List<Row> rows) {
        insertedRows = rows;
    }

    @Override
    public void markRowsAsDead(QueryContext context, long markValue,
            AppendOnlyPrimitiveLongList idList) {
        deletedRowIds = idList;
    }

    @Override
    public void deleteDeadRows(QueryContext context) {
    }

    /**
     * @return <code>null</code> if there was nothing.
     */
    public List<Row> retrieveNewRowsInBatch() {
        List<Row> temp = insertedRows;
        insertedRows = null;
        return temp;
    }

    /**
     * @return <code>null</code> if there was nothing.
     */
    public AppendOnlyPrimitiveLongList retrieveDeadRowIdsInBatch() {
        AppendOnlyPrimitiveLongList temp = deletedRowIds;
        deletedRowIds = null;
        return temp;
    }

    public void discard() {
        insertedRows = null;
        deletedRowIds = null;
    }
}