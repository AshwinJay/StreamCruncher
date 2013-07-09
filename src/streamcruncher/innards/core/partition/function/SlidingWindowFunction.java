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

import java.util.List;

import streamcruncher.api.artifact.RowSpec;
import streamcruncher.innards.core.QueryContext;
import streamcruncher.innards.core.partition.Row;
import streamcruncher.util.AtomicX;

/*
 * Author: Ashwin Jayaprakash Date: Feb 18, 2006 Time: 6:53:20 PM
 */

public class SlidingWindowFunction extends WindowFunction {
    protected final int windowSize;

    /**
     * @param pinned
     * @param selectedRowSpec
     * @param newRowSpec
     * @param rowIdGenerator
     * @param sourceLocationForTargetCols
     * @param windowSize
     */
    public SlidingWindowFunction(RowSpec selectedRowSpec, RowSpec newRowSpec,
            AtomicX rowIdGenerator, int[] sourceLocationForTargetCols, int windowSize) {
        super(selectedRowSpec, newRowSpec, rowIdGenerator, sourceLocationForTargetCols, windowSize);

        this.windowSize = windowSize;
    }

    /**
     * @return Returns the windowSize.
     */
    public int getWindowSize() {
        return windowSize;
    }

    // -------------------

    @Override
    /**
     * @param context
     */
    public void onCalculate(QueryContext context) {
        free = 0;
        maxRowsThatCanBeConsumed = 0;

        /*
         * Once the Window reaches its full capacity, it can only move 1 Row at
         * a time, because it's a Sliding Window. If a Row cannot be slid in,
         * then an old Row cannot be slid out. Also, for each simCycle, only 1
         * Row can be slid out/in.
         */
        if (getNumOfIds() == windowSize) {
            maxRowsThatCanBeConsumed = 1;
            free = 1;
        }
        /* Otherwise, allow the Window to fill up. */
        else {
            maxRowsThatCanBeConsumed = windowSize - getNumOfIds();
        }

        // -------------------

        final List<Row> unprocessedRows = unprocessedRowBuffer.getRows();
        final int pendingEventsAllowed = context.getPendingEventsAllowed();
        final int newEvents = unprocessedRows.size() - pendingEventsAllowed;

        /**
         * <pre>
         *      Max-Window-Size                |
         *    ....................             |
         *                                     |
         *    .................................|............
         *    |XXXXXXXXXXXX|YYYYY|             |XXXXXXXXXXX|
         *    `..................:.............|...........'
         *     Curr-Window                     | Allowed-Pending
         *                                     |
         *                                     |
         * </pre>
         */
        if ((getNumOfIds() + newEvents) - windowSize > 1) {
            free = 0;

            int idsInWindow = getNumOfIds();
            while (idsInWindow > 0 && (idsInWindow + newEvents) > windowSize) {
                free++;
                idsInWindow--;
            }
            maxRowsThatCanBeConsumed = windowSize - idsInWindow;

            /*
             * Drop some rows so that only 'windowSize' number of Rows remain.
             */
            int extraEvents = idsInWindow + newEvents;
            while (extraEvents > windowSize) {
                unprocessedRows.remove(0);
                extraEvents--;
            }
        }
    }
}
