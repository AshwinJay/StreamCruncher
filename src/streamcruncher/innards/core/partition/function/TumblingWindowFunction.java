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

import streamcruncher.api.artifact.RowSpec;
import streamcruncher.innards.core.QueryContext;
import streamcruncher.innards.core.partition.CalculateTSFunctionPair;
import streamcruncher.util.AtomicX;

/*
 * Author: Ashwin Jayaprakash Date: Feb 19, 2006 Time: 10:21:19 AM
 */

public class TumblingWindowFunction extends WindowFunction {
    protected final int windowSize;

    /**
     * @param selectedRowSpec
     * @param newRowSpec
     * @param rowIdGenerator
     * @param sourceLocationForTargetCols
     * @param windowSize
     */
    public TumblingWindowFunction(RowSpec selectedRowSpec, RowSpec newRowSpec,
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
        free = getNumOfIds();
        maxRowsThatCanBeConsumed = windowSize;
    }

    @Override
    /**
     * return <code>true</code>. Allow freeing even if there are no Rows to
     * be consumed.
     */
    protected boolean allowFreeingWhenRSIsNull() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean onCycleEnd(QueryContext context) throws Exception {
        boolean val = super.onCycleEnd(context);

        if (getNumOfIds() > 0) {
            // Expires in the next cycle.
            Long expiryAt = context.getCurrentTime() + 1;

            /*
             * Don't add to the Context, just add to the list of Functions that
             * need to be invoked in the next cycle. If, it is added to the
             * context, then it will trigger the Query. We want it these Events
             * to expire only when some other Stream triggers the Query.
             */
            calculateTSFunctionPairs.add(new CalculateTSFunctionPair(expiryAt, getHomeFunction()));
        }

        return val;
    }
}
