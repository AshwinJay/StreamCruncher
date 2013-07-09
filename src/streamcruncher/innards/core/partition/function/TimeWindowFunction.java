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

import java.sql.Timestamp;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import streamcruncher.api.artifact.RowSpec;
import streamcruncher.innards.core.QueryContext;
import streamcruncher.innards.core.partition.CalculateTSFunctionPair;
import streamcruncher.util.AppendOnlyPrimitiveLongList;
import streamcruncher.util.AtomicX;
import streamcruncher.util.PerpetualResultSet;

/*
 * Author: Ashwin Jayaprakash Date: Feb 19, 2006 Time: 11:08:45 AM
 */

public class TimeWindowFunction extends WindowFunction {
    protected final long windowSizeMilliseconds;

    protected final int maxWindowSize;

    protected final int timeColumnBufferPos;

    /**
     * The Value can either be a single Id or a List Of Ids.
     */
    protected final SortedMap<Long, Object> timeStampsAndIds;

    protected final SortedSet<Long> idsAddedInCycle;

    protected int totalRowIdsInWindow;

    protected int watchAndFree;

    /**
     * @param selectedRowSpec
     * @param newRowSpec
     * @param rowIdGenerator
     * @param sourceLocationForTargetCols
     * @param windowSizeMilliseconds
     * @param maxWindowSize
     */
    public TimeWindowFunction(RowSpec selectedRowSpec, RowSpec newRowSpec, AtomicX rowIdGenerator,
            int[] sourceLocationForTargetCols, long windowSizeMilliseconds, int maxWindowSize) {
        super(selectedRowSpec, newRowSpec, rowIdGenerator, sourceLocationForTargetCols, Math.min(
                maxWindowSize, AppendOnlyPrimitiveLongList.FRAGMENT_SIZE));

        this.windowSizeMilliseconds = windowSizeMilliseconds;
        this.maxWindowSize = maxWindowSize;

        this.timeColumnBufferPos = newRowSpec.getTimestampColumnPosition();
        this.timeStampsAndIds = new TreeMap<Long, Object>();
        this.idsAddedInCycle = new TreeSet<Long>();
    }

    /**
     * @return Returns the maxWindowSize.
     */
    public int getMaxWindowSize() {
        return maxWindowSize;
    }

    /**
     * @return Returns the windowSizeMilliseconds.
     */
    public long getWindowSizeMilliseconds() {
        return windowSizeMilliseconds;
    }

    // -----------------------

    @Override
    /**
     * Unlike other "calculate" methods, this is <b>not</b> Idempotent.
     * 
     * @param context
     */
    public void onCalculate(QueryContext context) {
        long currTime = context.getCurrentTime();

        while (timeStampsAndIds.isEmpty() == false) {
            long ts = timeStampsAndIds.firstKey();

            if ((currTime - ts) >= windowSizeMilliseconds) {
                /**
                 * We are freeing the Rows that have expired, here itself.
                 * Therefore, the
                 * {@link WindowFunction#allowFreeingWhenRSIsNull()} is not
                 * over-ridden.
                 */
                Object ids = timeStampsAndIds.get(ts);
                int howMany = 1;
                if (ids instanceof List) {
                    List<Long> list = (List<Long>) ids;
                    howMany = list.size();
                }
                removeFirstXIds(howMany);
            }
            else {
                break;
            }
        }

        /*
         * Only the Rows that were participated in the previous cycle are
         * allowed to be slid out. Not the ones that come in the current cycle.
         */
        watchAndFree = totalRowIdsInWindow;
        free = 0;

        // If Rows are available, then the ones in the Window can be slid out.
        maxRowsThatCanBeConsumed = maxWindowSize;
    }

    @Override
    protected void process(QueryContext context, PerpetualResultSet currRow) throws Exception {
        /*
         * Whatever is remaining in the Window now, can be freed if there are
         * Rows that can slide them out.
         */
        if (watchAndFree > 0
        /*
         * But, slide out only those Events that have been processed and stayed
         * for at least one cycle.
         */
        && totalRowIdsInWindow == maxWindowSize) {
            if (currRow != null || (currRow == null && allowFreeingWhenRSIsNull())) {
                int maxReplacements = Math.max(1 /* when currRow != null */, unprocessedRowBuffer
                        .getRows().size());
                int clean = Math.min(watchAndFree, maxReplacements);
                removeFirstXIds(clean);
                watchAndFree = watchAndFree - clean;
            }
        }

        super.process(context, currRow);
    }

    // todo Use this same method of List/Id in Map for Aggregate Functions.
    protected void addIdAndTimestamp(Long timestampKey, Long id) {
        Object ids = timeStampsAndIds.get(timestampKey);
        if (ids != null) {
            if (ids instanceof List) {
                List<Long> list = (List<Long>) ids;
                list.add(id);
            }
            else {
                List<Long> list = new LinkedList<Long>();
                list.add((Long) ids);
                list.add(id);
                timeStampsAndIds.put(timestampKey, list);
            }
        }
        else {
            timeStampsAndIds.put(timestampKey, id);
        }

        idsAddedInCycle.add(id);
        totalRowIdsInWindow++;
    }

    @Override
    protected void removeFirstXIds(int howMany) {
        for (int i = howMany; i > 0;) {
            Long timestampKey = timeStampsAndIds.firstKey();
            Object ids = timeStampsAndIds.get(timestampKey);

            if (ids instanceof List) {
                List<Long> list = (List<Long>) ids;
                for (Iterator<Long> iter = list.iterator(); iter.hasNext();) {
                    Long id = iter.next();

                    /*
                     * This Event arrived late and cannot be removed
                     * immediately. It has to stay for at least 1 cycle.
                     */
                    if (idsAddedInCycle.contains(id)) {
                        continue;
                    }

                    oustedRowIds.add(id);

                    iter.remove();
                    totalRowIdsInWindow--;
                    i--;
                    if (i == 0) {
                        break;
                    }
                }

                if (list.isEmpty()) {
                    timeStampsAndIds.remove(timestampKey);
                }
            }
            else {
                Long id = (Long) ids;

                /*
                 * Remove only if not added in the current Cycle.
                 */
                if (idsAddedInCycle.contains(id) == false) {
                    oustedRowIds.add(id);
                    i--;

                    timeStampsAndIds.remove(timestampKey);

                    totalRowIdsInWindow--;
                }
            }
        }
    }

    @Override
    protected void afterRowProcess(Object[] processedRow) {
        Long rowId = ((Number) processedRow[idColumnBufferPos]).longValue();
        Timestamp ts = (Timestamp) processedRow[timeColumnBufferPos];
        addIdAndTimestamp(ts.getTime(), rowId);
    }

    @Override
    protected int getNumOfIds() {
        return totalRowIdsInWindow;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean onCycleEnd(QueryContext context) throws Exception {
        boolean val = super.onCycleEnd(context);

        if (timeStampsAndIds.isEmpty() == false) {
            Long nextTS = timeStampsAndIds.firstKey();
            // Expires after the configured time.
            Long expiryAt = nextTS + windowSizeMilliseconds;

            context.addEventExpirationTime(expiryAt);
            calculateTSFunctionPairs.add(new CalculateTSFunctionPair(expiryAt, getHomeFunction()));
        }

        idsAddedInCycle.clear();

        return val;
    }
}
