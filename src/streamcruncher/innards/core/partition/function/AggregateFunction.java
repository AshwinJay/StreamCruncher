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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

import streamcruncher.api.aggregator.AbstractAggregator;
import streamcruncher.api.artifact.RowSpec;
import streamcruncher.innards.core.QueryContext;
import streamcruncher.innards.core.partition.CalculateTSFunctionPair;
import streamcruncher.innards.core.partition.Row;
import streamcruncher.innards.core.partition.RowBuffer;
import streamcruncher.innards.util.AggregatorHolder;
import streamcruncher.innards.util.SourceToTargetMapper;
import streamcruncher.util.AppendOnlyPrimitiveLongList;
import streamcruncher.util.AtomicX;
import streamcruncher.util.PerpetualResultSet;

/*
 * Author: Ashwin Jayaprakash Date: Feb 26, 2006 Time: 9:43:16 PM
 */
public class AggregateFunction extends Function {
    protected final Function innerFunction;

    protected final int[] sourceLocationForTargetCols;

    protected final AggregatorHolder[] holders;

    protected final HashMap<Long, Object[]> rowIdAndAggregatedColumns;

    protected final int sourceIdColumnBufferPos;

    protected final int finalIdColumnBufferPos;

    protected final int finalTimestampColumnBufferPos;

    protected final int finalVersionColumnBufferPos;

    protected Long currentRowId;

    protected Object[] currentRowValues;

    /**
     * @param realTableRowSpec
     * @param finalTableRowSpec
     * @param rowIdGenerator
     * @param triples
     * @param innerFunction
     * @param pinned
     */
    public AggregateFunction(RowSpec realTableRowSpec, RowSpec finalTableRowSpec,
            AtomicX rowIdGenerator, AggregatorHolder[] triples, Function innerFunction,
            boolean pinned) {
        super(realTableRowSpec, finalTableRowSpec, rowIdGenerator);

        this.innerFunction = innerFunction;
        this.innerFunction.setHomeFunction(this);
        this.innerFunction.setPinned(pinned);

        this.sourceLocationForTargetCols = new SourceToTargetMapper(realTableRowSpec,
                finalTableRowSpec).map();
        this.holders = triples;

        this.rowIdAndAggregatedColumns = new HashMap<Long, Object[]>();

        this.sourceIdColumnBufferPos = realTableRowSpec.getIdColumnPosition();
        this.finalIdColumnBufferPos = finalTableRowSpec.getIdColumnPosition();
        this.finalVersionColumnBufferPos = finalTableRowSpec.getVersionColumnPosition();
        this.finalTimestampColumnBufferPos = finalTableRowSpec.getTimestampColumnPosition();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setDirtyFunctions(Set<Function> dirtyFunctions) {
        super.setDirtyFunctions(dirtyFunctions);

        innerFunction.setDirtyFunctions(dirtyFunctions);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setUnprocessedDataFunctions(Set<Function> unprocessedFunctions) {
        super.setUnprocessedDataFunctions(unprocessedFunctions);

        innerFunction.setUnprocessedDataFunctions(unprocessedFunctions);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setCalculateTSFunctionPairs(
            PriorityQueue<CalculateTSFunctionPair> calculateTSFunctionPairs) {
        super.setCalculateTSFunctionPairs(calculateTSFunctionPairs);

        innerFunction.setCalculateTSFunctionPairs(calculateTSFunctionPairs);
    }

    /**
     * @return Returns the innerFunction.
     */
    public Function getInnerFunction() {
        return innerFunction;
    }

    // --------------------

    @Override
    public void onCalculate(QueryContext context) {
        innerFunction.calculate(context);
    }

    @Override
    protected void onCycleStart(QueryContext context) throws Exception {
        innerFunction.cycleStart(context);
    }

    @Override
    protected void process(QueryContext context, PerpetualResultSet currRow) throws Exception {
        innerFunction.process(context, currRow);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected boolean onCycleEnd(QueryContext context) throws Exception {
        boolean noActionRequired = innerFunction.cycleEnd(context);

        // --------------------

        AppendOnlyPrimitiveLongList longList = innerFunction.getOustedRowIds();
        List<Object[]> removedValues = (longList.getSize() > 0) ? new ArrayList<Object[]>(longList
                .getSize()) : null;

        while (longList.getSize() > 0) {
            Object[] removedColValues = rowIdAndAggregatedColumns.remove(longList.remove());
            removedValues.add(removedColValues);
        }

        // --------------------

        RowBuffer buffer = innerFunction.getProcessedRowBuffer();
        List<Row> processed = buffer.getRows();
        List<Object[]> addedValues = (processed.size() > 0) ? new ArrayList<Object[]>(processed
                .size()) : null;

        /*
         * Add only if we are not going to be discarded. Otherwise, don't
         * bother.
         */
        if (noActionRequired == false) {
            for (Row row : processed) {
                Object[] sourceColumnValues = row.getColumns();

                // Source Row's Id.
                Long rowId = ((Number) sourceColumnValues[sourceIdColumnBufferPos]).longValue();
                rowIdAndAggregatedColumns.put(rowId, sourceColumnValues);
                addedValues.add(sourceColumnValues);

                // Copy the unchanged columns.
                if (currentRowValues == null) {
                    String[] columnNames = finalTableRowSpec.getColumnNames();
                    Object[] columns = new Object[columnNames.length];

                    // Copy the values. Aggregates will be produced later.
                    for (int i = 1; i < sourceLocationForTargetCols.length; i++) {
                        if (sourceLocationForTargetCols[i] > -1) {
                            columns[i] = sourceColumnValues[sourceLocationForTargetCols[i]];
                        }
                    }

                    currentRowValues = columns;
                    currentRowValues[finalIdColumnBufferPos] = 0L;
                    currentRowValues[finalTableRowSpec.getTimestampColumnPosition()] = 0L;
                    currentRowValues[finalTableRowSpec.getVersionColumnPosition()] = 0L;
                }
            }
        }
        processed.clear();

        // --------------------

        if (removedValues != null || addedValues != null) {
            if (noActionRequired == false) {
                boolean someAggregateChanged = false;
                Object[] newAggregates = new Object[holders.length];
                for (int i = 0; i < holders.length; i++) {
                    AbstractAggregator aggregator = holders[i].getAggregator();

                    newAggregates[i] = aggregator.aggregate(removedValues, addedValues);

                    int pos = holders[i].getTargetColumnPos();
                    if (currentRowValues[pos] == null || newAggregates[i] == null
                            || newAggregates[i].equals(currentRowValues[pos]) == false) {
                        someAggregateChanged = true;
                    }
                }

                /*
                 * If the aggregate value does not change, then it should not be
                 * re-written.
                 */
                if (someAggregateChanged) {
                    if (currentRowId != null) {
                        oustedRowIds.add(currentRowId);
                        currentRowId = null;
                    }

                    Row newRow = processedRowBuffer.addNewRowWithAutoValues(context.getRunCount());
                    Object[] columns = newRow.getColumns();

                    /*
                     * Number could be Integer or Long, from AtomicX.
                     */
                    currentRowId = ((Number) columns[finalIdColumnBufferPos]).longValue();

                    for (int i = 0; i < currentRowValues.length; i++) {
                        // Don't overwrite auto-values.
                        if (i == finalIdColumnBufferPos || i == finalTimestampColumnBufferPos
                                || i == finalVersionColumnBufferPos) {
                            continue;
                        }

                        columns[i] = currentRowValues[i];
                    }

                    for (int i = 0; i < holders.length; i++) {
                        int pos = holders[i].getTargetColumnPos();

                        currentRowValues[pos] = newAggregates[i];
                        columns[pos] = newAggregates[i];
                    }
                }
            }
            else {
                if (currentRowId != null) {
                    oustedRowIds.add(currentRowId);
                    currentRowId = null;
                }
            }
        }

        // --------------------

        return noActionRequired;
    }
}