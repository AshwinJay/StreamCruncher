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

import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;

import streamcruncher.api.artifact.RowSpec;
import streamcruncher.boot.Registry;
import streamcruncher.util.AtomicX;
import streamcruncher.util.TimeKeeper;

/*
 * Author: Ashwin Jayaprakash Date: Feb 18, 2006 Time: 12:37:55 PM
 */

/**
 * This class is <b>not</b> Thread-safe.
 */
public class RowBuffer {
    protected final AtomicX rowIdGenerator;

    protected final RowSpec rowSpec;

    protected final List<Row> rows;

    protected final int idColPos;

    protected final int timestampColPos;

    protected final int versionColPos;

    protected final TimeKeeper timeKeeper;

    /**
     * @param rowSpec
     * @param rowIdGenerator
     *            if <code>null</code>, then the Row-Ids will be using
     *            {@link System#nanoTime()}.
     */
    public RowBuffer(RowSpec rowSpec, AtomicX rowIdGenerator) {
        this.rowSpec = rowSpec;
        this.rowIdGenerator = rowIdGenerator;

        this.rows = new LinkedList<Row>();
        this.idColPos = rowSpec.getIdColumnPosition();
        this.timestampColPos = rowSpec.getTimestampColumnPosition();
        this.versionColPos = rowSpec.getVersionColumnPosition();

        this.timeKeeper = Registry.getImplFor(TimeKeeper.class);
    }

    /**
     * @return Returns the rows.
     */
    public List<Row> getRows() {
        return rows;
    }

    /**
     * @return Returns the finalTableRowSpec.
     */
    public RowSpec getRowSpec() {
        return rowSpec;
    }

    // --------------------

    /**
     * @param version
     * @return Adds an empty {@link Row} to the buffer and returns that row.
     *         However, the Id-column is populated based on the
     *         {@link #rowIdGenerator} {@link System#nanoTime()},
     *         Timestamp-column column is populated with
     *         {@link System#currentTimeMillis()} and the Version-column is
     *         populated with the "version" provided to the method.
     */
    public Row addNewRowWithAutoValues(long version) {
        Row row = addNewRow();

        Object[] cols = row.getColumns();

        if (rowIdGenerator == null) {
            cols[idColPos] = System.nanoTime();
        }
        else {
            cols[idColPos] = rowIdGenerator.incrementAndGet();
        }

        cols[timestampColPos] = new Timestamp(timeKeeper.getTimeMsecs());
        cols[versionColPos] = version;

        return row;
    }

    /**
     * @return Adds an empty {@link Row} to the buffer and returns that row.
     */
    public Row addNewRow() {
        Row row = new Row(rowSpec.getColumnNames().length);

        rows.add(row);

        return row;
    }
}
