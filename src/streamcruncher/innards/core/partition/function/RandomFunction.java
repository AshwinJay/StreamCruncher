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

import java.util.Random;

import streamcruncher.api.artifact.RowSpec;
import streamcruncher.innards.core.QueryContext;
import streamcruncher.util.AtomicX;
import streamcruncher.util.PerpetualResultSet;

/*
 * Author: Ashwin Jayaprakash Date: Feb 19, 2006 Time: 4:26:05 PM
 */

public class RandomFunction extends SlidingWindowFunction {
    protected final Random random;

    /**
     * @param selectedRowSpec
     * @param newRowSpec
     * @param rowIdGenerator
     * @param sourceLocationForTargetCols
     * @param windowSize
     */
    public RandomFunction(RowSpec selectedRowSpec, RowSpec newRowSpec, AtomicX rowIdGenerator,
            int[] sourceLocationForTargetCols, int windowSize) {
        super(selectedRowSpec, newRowSpec, rowIdGenerator, sourceLocationForTargetCols, windowSize);

        this.random = new Random();
    }

    @Override
    protected void process(QueryContext context, PerpetualResultSet currRow) throws Exception {
        if (random.nextBoolean() == true) {
            super.process(context, currRow);
        }
    }
}
