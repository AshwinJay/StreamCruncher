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

import java.lang.ref.SoftReference;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

import streamcruncher.api.artifact.RowSpec;
import streamcruncher.innards.core.FilterInfo;
import streamcruncher.innards.core.QueryContext;
import streamcruncher.innards.core.filter.TableFilter;
import streamcruncher.innards.core.partition.function.Function;
import streamcruncher.util.AppendOnlyPrimitiveLongList;

/*
 * Author: Ashwin Jayaprakash Date: Mar 29, 2006 Time: 11:32:49 PM
 */

public abstract class Partitioner<F extends FilterInfo> implements TableFilter<F> {
    protected F filterInfo;

    protected FirstPartitionLevel firstLevel;

    // -------------

    protected PartitionDescender partitionDescender;

    // -------------

    protected PartitionOutputStore storage;

    /**
     * PQ instead of a TreeSet, because TreeSet does not allow duplicate
     * entries. Add the {@link Function#getHomeFunction()}, but remove the
     * function directly.
     */
    protected PriorityQueue<CalculateTSFunctionPair> calculateTSFunctionPairs;

    /**
     * Add the {@link Function#getHomeFunction()}, but remove the function
     * directly.
     */
    protected Set<Function> dirtyFunctions;

    /**
     * Add the {@link Function#getHomeFunction()}, but remove the function
     * directly.
     */
    protected Set<Function> unprocessedDataFunctions;

    // -------------

    protected int freedSinceCleanup;

    protected int consumedSinceCleanup;

    protected SoftReference gcRequiredIndicator;

    // -------------

    /**
     * @param queryName
     * @param filterInfo
     */
    public void init(String queryName, F filterInfo) throws Exception {
        this.filterInfo = filterInfo;

        // -------------

        dirtyFunctions = new HashSet<Function>();
        unprocessedDataFunctions = new HashSet<Function>();
        calculateTSFunctionPairs = new PriorityQueue<CalculateTSFunctionPair>();
        gcRequiredIndicator = new SoftReference(new byte[32]);

        PartitionSpec spec = (PartitionSpec) filterInfo.getFilterSpec();
        buildLevels(spec);

        partitionDescender = new PartitionDescender(firstLevel, dirtyFunctions,
                unprocessedDataFunctions, calculateTSFunctionPairs);
    }

    protected void initStorage(PartitionOutputStore store) {
        storage = store;
    }

    // ----------------

    protected void buildLevels(PartitionSpec spec) {
        PartitionLevel prevLevel = null;

        String[] columnNames = spec.getPartitionColumnNames();
        for (int i = columnNames.length - 1; i >= 0; i--) {
            if (i == 0) {
                if (prevLevel == null) {
                    firstLevel = new FirstPartitionLevel(columnNames[i], spec.getFunctionBuilder());
                }
                else {
                    firstLevel = new FirstPartitionLevel(columnNames[i], prevLevel);
                }
            }
            else {
                if (prevLevel == null) {
                    prevLevel = new PartitionLevel(columnNames[i], spec.getFunctionBuilder());
                }
                else {
                    prevLevel = new PartitionLevel(columnNames[i], prevLevel);
                }
            }
        }

        if (firstLevel == null) {
            firstLevel = new FirstPartitionLevel(spec.getFunctionBuilder());
        }
    }

    // -------------

    public void filter(QueryContext context) throws Exception {
        AppendOnlyPrimitiveLongList allOustedIds = new AppendOnlyPrimitiveLongList(20);
        LinkedList<Row> newRows = new LinkedList<Row>();
        RowSpec rowSpec = null;

        // -------------

        while (true) {
            CalculateTSFunctionPair calculateTSFunctionPair = calculateTSFunctionPairs.peek();

            if (calculateTSFunctionPair == null) {
                break;
            }

            long ts = calculateTSFunctionPair.getTimestamp();
            if (context.getCurrentTime() >= ts) {
                // Remove it.
                calculateTSFunctionPairs.poll();

                Function function = calculateTSFunctionPair.getFunction();
                // Add the Function explicitly to complete its cycle.
                dirtyFunctions.add(function);
                function.cycleStart(context);
            }
            else {
                break;
            }
        }

        for (Iterator<Function> iter = unprocessedDataFunctions.iterator(); iter.hasNext();) {
            Function function = iter.next();
            function.cycleStart(context);
            iter.remove();

            /**
             * Add the Function explicitly, because the unproc-buffer gets
             * consumed only in {@link Function#cycleEnd(Context)}, if it did
             * not receive any fresh rows.
             */
            dirtyFunctions.add(function);
        }

        // -------------

        int rowsCopied = 0;
        int rowsInserted = 0;
        int rowsOusted = 0;

        rowsCopied = copyAndDescend(context);

        // -------------

        for (Iterator<Function> iter = dirtyFunctions.iterator(); iter.hasNext();) {
            Function function = iter.next();
            iter.remove();
            boolean canDiscard = function.cycleEnd(context);

            AppendOnlyPrimitiveLongList oustedIds = function.getOustedRowIds();

            for (int i = oustedIds.getSize() - 1; i >= 0; i--) {
                allOustedIds.add(oustedIds.remove());
            }

            RowBuffer rowBuffer = function.getProcessedRowBuffer();
            if (rowSpec == null) {
                rowSpec = function.getFinalTableRowSpec();
            }
            List<Row> rows = rowBuffer.getRows();
            newRows.addAll(rows);
            rows.clear();

            if (canDiscard) {
                // Release the Strong-Reference.
                firstLevel.removeFunction(function);
            }
        }

        // -------------

        boolean storeSuccess = false;

        storage.startBatch(context);
        try {
            deleteMarkedRows(context);

            rowsOusted = allOustedIds.getSize();
            if (rowsOusted > 0) {
                markRowsForDeletion(context, allOustedIds);
            }

            rowsInserted = newRows.size();
            if (rowsInserted > 0) {
                insertRows(context, newRows);
            }

            storeSuccess = true;
        }
        catch (Exception e) {
            throw e;
        }
        finally {
            storage.endBatch(context, storeSuccess);
        }

        // -------------

        freedSinceCleanup = freedSinceCleanup + rowsOusted;
        consumedSinceCleanup = consumedSinceCleanup + rowsInserted;

        // -------------

        postProcess(context, rowsCopied, rowsOusted, rowsInserted);

        boolean cleanup = (gcRequiredIndicator.get() == null);
        if (cleanup) {
            gcRequiredIndicator = new SoftReference(new byte[32]);
        }

        cleanup = cleanup || (freedSinceCleanup > (0.75 * consumedSinceCleanup));
        if (cleanup) {
            partitionDescender.attemptCleanup();

            freedSinceCleanup = 0;
            consumedSinceCleanup = 0;
        }
    }

    protected void postProcess(QueryContext context, int rowsCopied, int rowsOusted,
            int rowsInserted) {
    }

    /**
     * Mark Rows so that they can be deleted in the next Cycle.
     * 
     * @param context
     * @param allOustedIds
     */
    protected void markRowsForDeletion(QueryContext context,
            AppendOnlyPrimitiveLongList allOustedIds) throws Exception {
        long markValue = -1 * context.getRunCount();
        storage.markRowsAsDead(context, markValue, allOustedIds);
    }

    /**
     * Delete the Rows that were marked for deletion in the <b>previous</b>
     * cycle. Then,
     * {@linkplain #markRowsForDeletion(QueryContext, AppendOnlyPrimitiveLongList) mark}
     * the Rows from the <b>current</b> cycle.
     * 
     * @param context
     */
    protected void deleteMarkedRows(QueryContext context) throws Exception {
        storage.deleteDeadRows(context);
    }

    protected void insertRows(QueryContext context, List<Row> newRows) throws Exception {
        storage.insertNewRow(context, newRows);
    }

    /**
     * @param context
     * @return Rows copied.
     * @throws Exception
     */
    protected abstract int copyAndDescend(QueryContext context) throws Exception;

    // -------------

    public void discard() {
        firstLevel = null;
        filterInfo = null;
        partitionDescender = null;

        storage.discard();
        dirtyFunctions.clear();
        dirtyFunctions = null;
        unprocessedDataFunctions.clear();
        unprocessedDataFunctions = null;
    }
}