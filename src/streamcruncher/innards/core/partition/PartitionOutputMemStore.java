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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import streamcruncher.innards.core.QueryContext;
import streamcruncher.innards.db.Constants;
import streamcruncher.util.AppendOnlyPrimitiveLongList;

/*
 * Author: Ashwin Jayaprakash Date: Jun 23, 2007 Time: 7:39:32 PM
 */

public class PartitionOutputMemStore extends PartitionOutputStore {
    protected final int rowIdPosition;

    protected final int versionIdPosition;

    protected final SortedMap<Long, Object[]> store;

    protected List<Object[]> newRowsInBatch;

    protected List<Object[]> dyingRowsInBatch;

    protected final AppendOnlyPrimitiveLongList deadRowIds;

    public PartitionOutputMemStore(streamcruncher.innards.core.FilterInfo filterInfo) {
        super(filterInfo);

        this.rowIdPosition = Constants.ID_COLUMN_POS;
        this.versionIdPosition = Constants.VERSION_COLUMN_POS;

        this.store = new TreeMap<Long, Object[]>();
        this.deadRowIds = new AppendOnlyPrimitiveLongList(20);
    }

    @Override
    public void insertNewRow(QueryContext context, List<Row> rows) {
        newRowsInBatch = (rows.size() > 0) ? new ArrayList<Object[]>(rows.size()) : null;

        for (Row row : rows) {
            Object[] data = row.getColumns();
            Long id = (Long) data[rowIdPosition];
            store.put(id, data);
            newRowsInBatch.add(data);
        }
        rows.clear();
    }

    @Override
    public void deleteDeadRows(QueryContext context) {
        int length = deadRowIds.getSize();
        for (int i = 0; i < length; i++) {
            long id = deadRowIds.remove();
            store.remove(id);
        }
    }

    @Override
    public void markRowsAsDead(QueryContext context, long markValue,
            AppendOnlyPrimitiveLongList idList) {
        int length = idList.getSize();
        dyingRowsInBatch = (length > 0) ? new ArrayList<Object[]>(length) : null;

        for (int i = 0; i < length; i++) {
            long id = idList.remove();
            Object[] row = store.get(id);
            row[versionIdPosition] = markValue;
            deadRowIds.add(id);

            dyingRowsInBatch.add(row);
        }
    }

    public Collection<Object[]> retrieveRows() {
        return store.values();
    }

    /**
     * @return <code>null</code> if there is nothing.
     */
    public Collection<Object[]> retrieveNewRowsInBatch() {
        Collection<Object[]> temp = newRowsInBatch;
        newRowsInBatch = null;
        return temp;
    }

    /**
     * @return <code>null</code> if there is nothing.
     */
    public Collection<Object[]> retrieveDeadRowsInBatch() {
        Collection<Object[]> temp = dyingRowsInBatch;
        dyingRowsInBatch = null;
        return temp;
    }

    public void discard() {
        while (deadRowIds.getSize() > 0) {
            deadRowIds.removeAvailable();
        }

        store.clear();
    }
}