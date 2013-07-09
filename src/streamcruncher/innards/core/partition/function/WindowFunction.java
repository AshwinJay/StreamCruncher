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
package streamcruncher.innards.core.partition.function;

import java.sql.ResultSet;
import java.util.List;

import streamcruncher.api.artifact.RowSpec;
import streamcruncher.innards.core.QueryContext;
import streamcruncher.innards.core.partition.Row;
import streamcruncher.util.AppendOnlyPrimitiveLongList;
import streamcruncher.util.AtomicX;
import streamcruncher.util.PerpetualResultSet;

/*
 * Author: Ashwin Jayaprakash Date: Feb 19, 2006 Time: 10:56:48 AM
 */

public abstract class WindowFunction extends Function {
    private final AppendOnlyPrimitiveLongList rowIdsInWindow;

    protected int free;

    /**
     * Since the new Rows cannot be seen until they are pushed to the
     * {@link Function}, unlike in a
     * {@link streamcruncher.innards.core.window.Window}, where the
     * {@link streamcruncher.innards.core.window.Window#getInputNotificationData()}
     * stores the incoming Rows, the value here will hold the "maximum" Rows
     * that can be consumed for the Cycle.
     */
    protected int maxRowsThatCanBeConsumed;

    protected final int[] sourceLocationForTargetCols;

    protected final int idColumnBufferPos;

    /**
     * <p>
     * If this function is not wrapped by an {@link AggregateFunction}, then
     * the number and order of columns in both the source and target tables will
     * be the same.
     * </p>
     * 
     * @param selectedRowSpec
     * @param newRowSpec
     *            Has the same order of columns as the selectedRowProc except
     *            for the additional Row-Id Column.
     * @param sourceLocationForTargetCols
     *            An array of numbers, one for each column in the
     *            target-rowspec, indicating the positions in the
     *            source-rowspec, from which to copy the values. A
     *            <code>-1</code> indicates that there was no match.
     * @param rowIdCacheSize
     * @see Function#Function(RowSpec, RowSpec)
     */
    public WindowFunction(RowSpec selectedRowSpec, RowSpec newRowSpec, AtomicX rowIdGenerator,
            int[] sourceLocationForTargetCols, int rowIdCacheSize) {
        super(selectedRowSpec, newRowSpec, rowIdGenerator);

        this.sourceLocationForTargetCols = sourceLocationForTargetCols;
        this.idColumnBufferPos = newRowSpec.getIdColumnPosition();
        this.rowIdsInWindow = new AppendOnlyPrimitiveLongList(rowIdCacheSize);
    }

    /**
     * @return Returns the sourceLocationForTargetCols.
     */
    public int[] getSourceLocationForTargetCols() {
        return sourceLocationForTargetCols;
    }

    // -----------------------

    /**
     * Invoked when {@link #process(ResultSet)} is invoked with a
     * <code>null</code> parameter, which in turn is invoked as described in
     * {@link #onCycleEnd()}.
     * 
     * @return <code>true</code> only if there are Rows in the
     *         {@link Function#unprocessedRowBuffer}.
     */
    protected boolean allowFreeingWhenRSIsNull() {
        int rowsToConsume = unprocessedRowBuffer.getRows().size();

        return (rowsToConsume > 0);
    }

    @Override
    /**
     * Uses the {@link #maxRowsThatCanBeConsumed} and {@link #free} values to
     * maxRowsThatCanBeConsumed/remove the Rows, respectively.
     * 
     * @param context
     * @param currRow
     *            Will be <code>null</code> when the
     *            {@link Function#unprocessedRowBuffer} needs to be processed or
     *            this Function never received a Row in the current simCycle,
     *            and so could not "free" the old Rows.
     * @throws Exception
     */
    protected void process(QueryContext context, PerpetualResultSet currRow) throws Exception {
        List<Row> unprocBuffer = unprocessedRowBuffer.getRows();
        String[] unprocColumnNames = realTableRowSpec.getColumnNames();

        // -------------------

        /*
         * Check if there are really any Rows to be consumed. currRow can be
         * null, only when this Function has not been invoked in this cycle.
         */
        boolean currRowConsumed = (currRow == null && unprocBuffer.size() == 0);

        while (free > 0 && (currRow != null || (currRow == null && allowFreeingWhenRSIsNull()))) {
            removeFirstXIds(1);
            free--;
        }

        // -------------------

        while (maxRowsThatCanBeConsumed > 0 &&
        /*
         * If we've consumed the currRow, then it means that the unprocessed
         * buffer has been emptied, and then the currRow after that. So, there
         * is no more data to maxRowsThatCanBeConsumed.
         */
        currRowConsumed == false) {
            Row newRow = processedRowBuffer.addNewRowWithAutoValues(context.getRunCount());
            Object[] columns = newRow.getColumns();

            // Buffer is empty, so, use the ResultSet row directly.
            if (unprocBuffer.size() == 0) {
                for (int i = 0; i < columns.length; i++) {
                    int position = sourceLocationForTargetCols[i];
                    if (position >= 0) {
                        columns[i] = currRow.getColumnValue(unprocColumnNames[position]);
                    }
                }

                currRowConsumed = true;
            }
            else {
                Row bufferedRow = unprocBuffer.remove(0);
                Object[] oldColumns = bufferedRow.getColumns();

                for (int i = 0; i < columns.length; i++) {
                    int position = sourceLocationForTargetCols[i];
                    if (position >= 0) {
                        columns[i] = oldColumns[position];
                    }
                }
            }

            afterRowProcess(columns);
            maxRowsThatCanBeConsumed--;

            // -------------------

            if (currRow == null && unprocBuffer.size() == 0) {
                currRowConsumed = true;
            }
        }

        if (currRowConsumed == false && currRow != null) {
            Row toBuffer = unprocessedRowBuffer.addNewRow();
            Object[] columns = toBuffer.getColumns();

            for (int i = 0; i < unprocColumnNames.length; i++) {
                columns[i] = currRow.getColumnValue(unprocColumnNames[i]);
            }
        }
    }

    /**
     * Adds the Event into the Window.
     * 
     * @param processedRow
     */
    protected void afterRowProcess(Object[] processedRow) {
        long rowId = ((Number) processedRow[idColumnBufferPos]).longValue();
        rowIdsInWindow.add(rowId);
    }

    /**
     * Removes the first few Event Ids from the Window and adds it to
     * {@link #getOustedRowIds()}.
     * 
     * @param howMany
     */
    protected void removeFirstXIds(int howMany) {
        while (howMany > 0 && rowIdsInWindow.getSize() > 0) {
            Long id = rowIdsInWindow.remove();
            oustedRowIds.add(id);
            howMany--;
        }
    }

    /**
     * <p>
     * Optional implementation.
     * </p>
     * <p>
     * Removes the first few Event Ids from the Window. Does not add them to
     * {@link #getOustedRowIds()}.
     * </p>
     * 
     * @param howMany
     */
    protected void discardFirstXIds(int howMany) {
        while (howMany > 0 && rowIdsInWindow.getSize() > 0) {
            rowIdsInWindow.remove();
            howMany--;
        }
    }

    /**
     * <p>
     * Optional implementation.
     * </p>
     * <p>
     * Adds the Event Id to the Window.
     * </p>
     * 
     * @param id
     */
    protected void addId(long id) {
        rowIdsInWindow.add(id);
    }

    /**
     * @return The number of Event Ids in the Window.
     */
    protected int getNumOfIds() {
        return rowIdsInWindow.getSize();
    }

    @Override
    /**
     * <p>
     * {@inheritDoc}
     * </p>
     * <b>Always</b> invokes {@link #process(ResultSet)} once, with
     * <code>null</code> as the parameter if this Function was never invoked
     * with a <code>non-null</code> <code>ResultSet</code> in the <b>current
     * cycle</b>.
     * 
     * @return <code>true</code> if {@link #rowIdsInWindow} and
     *         {@link #unprocessedRowBuffer} are both empty.
     */
    protected boolean onCycleEnd(QueryContext context) throws Exception {
        if (wasProcessInvokedAtleastOnce() == false) {
            handleRow(context, null);
        }

        List<Row> unprocRows = unprocessedRowBuffer.getRows();
        if (getNumOfIds() == 0 && unprocRows.size() == 0) {
            return true;
        }

        return false;
    }
}
