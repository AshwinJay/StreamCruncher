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
package streamcruncher.util;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import streamcruncher.innards.impl.expression.ExpressionEvaluationException;
import streamcruncher.util.RowEvaluator.ContextHolder;

/*
 * Author: Ashwin Jayaprakash Date: Jan 18, 2007 Time: 3:30:41 PM
 */

/**
 * Uni-directional always-on ResultSet.
 */
public class PerpetualResultSet {
    protected final String[] columnNames;

    /**
     * Label and ResultSet position, not Array position.
     */
    protected final Map<String, Integer> columnLabelAndPos;

    protected final Queue<Object[]> rows;

    protected final AtomicInteger rowCounter;

    protected Object[] currRow;

    protected int sessionRowCounter;

    public PerpetualResultSet(String[] columnNames) {
        this.columnNames = columnNames;

        this.columnLabelAndPos = new HashMap<String, Integer>();
        for (int i = 0; i < columnNames.length; i++) {
            this.columnLabelAndPos.put(columnNames[i], (i + 1));
        }

        this.rows = new ConcurrentLinkedQueue<Object[]>();
        this.rowCounter = new AtomicInteger();
    }

    /**
     * @param columnLabel
     * @return The position in the ResultSet of the column - First column is at
     *         position <b>1</b>.
     */
    public int findColumn(String columnLabel) {
        return columnLabelAndPos.get(columnLabel) + 1;
    }

    /**
     * @param reader
     * @param filter
     *            Can be <code>null</code>
     * @return Number of elements that were added.
     * @throws ExpressionEvaluationException
     */
    public int pumpRows(streamcruncher.util.TwoDAppendOnlyList.Reader reader, RowEvaluator filter)
            throws ExpressionEvaluationException {
        int c = reader.readInto(rows, filter);
        rowCounter.addAndGet(c);
        return c;
    }

    /**
     * @param collection
     * @param filter
     *            Can be <code>null</code>
     * @return Number of elements that were added.
     * @throws ExpressionEvaluationException
     */
    public int pumpRows(Collection<Object[]> collection, RowEvaluator filter)
            throws ExpressionEvaluationException {
        int c = 0;

        if (filter != null) {
            filter.batchStart();
        }

        ContextHolder holder = null;
        for (Object[] row : collection) {
            if (filter != null) {
                holder = filter.rowStart(holder, row);
            }

            if (filter == null || ((Boolean) filter.evaluate(holder)).booleanValue() == true) {
                rows.add(row);

                c++;
                rowCounter.incrementAndGet();
            }

            if (filter != null) {
                filter.rowEnd();
                holder.clear();
            }
        }

        if (filter != null) {
            filter.batchEnd();
        }

        return c;
    }

    /**
     * Keeps changing over time.
     * 
     * @return
     */
    public int getSize() {
        return rowCounter.get();
    }

    // -----------

    /**
     * @param maxRows
     *            Max rows that should be picked up for this session.
     */
    public void startSession(int maxRows) {
        // Take a snapshot here.
        sessionRowCounter = Math.min(maxRows, rowCounter.get());
    }

    /**
     * @return <code>true</code> if there was a Row and the ResultSet moved
     *         forward.
     */
    public boolean next() {
        if (sessionRowCounter <= 0) {
            return false;
        }

        sessionRowCounter--;
        rowCounter.decrementAndGet();

        currRow = rows.poll();

        return true;
    }

    /**
     * @param columnLabel
     * @return
     */
    public Object getColumnValue(String columnLabel) {
        int pos = columnLabelAndPos.get(columnLabel);
        return getColumnValue(pos);
    }

    /**
     * @param columnIndex
     *            First column is at position <b>1</b>, not 0.
     * @return
     */
    public Object getColumnValue(int columnIndex) {
        return currRow[columnIndex - 1];
    }

    public void closeSession() {
        currRow = null;
        sessionRowCounter = 0;
    }
}
