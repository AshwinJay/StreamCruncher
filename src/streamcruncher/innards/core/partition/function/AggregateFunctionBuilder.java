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

import streamcruncher.api.aggregator.AbstractAggregator;
import streamcruncher.api.artifact.RowSpec;
import streamcruncher.innards.core.partition.aggregate.AggregatorBuilder;
import streamcruncher.innards.util.AggregatorHolder;

/*
 * Author: Ashwin Jayaprakash Date: Feb 19, 2006 Time: 2:10:12 PM
 */

public class AggregateFunctionBuilder extends FunctionBuilder {
    private static final long serialVersionUID = 1L;

    protected final FunctionBuilder innerFunctionBuilder;

    protected final boolean pinned;

    protected final AggregatorBuilder[] aggregatorBuilders;

    protected final int[] aggregatorTargetPositions;

    /**
     * @param innerFunctionBuilder
     *            The source-table for the Aggregate Function is the "imaginary"
     *            Processed Buffer of the inner-function, which does not have a
     *            matching Table in the DB.
     * @param pinned
     * @param aggregatorBuilders
     * @param aggregatorTargetPositions
     * @param finalTableRowSpec
     */
    public AggregateFunctionBuilder(FunctionBuilder innerFunctionBuilder, boolean pinned,
            AggregatorBuilder[] aggregatorBuilders, int[] aggregatorTargetPositions,
            RowSpec finalTableRowSpec) {
        super(innerFunctionBuilder.getFinalTableRowSpec(), finalTableRowSpec);

        this.innerFunctionBuilder = innerFunctionBuilder;
        this.pinned = pinned;
        this.aggregatorBuilders = aggregatorBuilders;
        this.aggregatorTargetPositions = aggregatorTargetPositions;
    }

    /**
     * @return Returns the innerFunctionBuilder.
     */
    public FunctionBuilder getInnerFunctionBuilder() {
        return innerFunctionBuilder;
    }

    public int[] getAggregatorTargetPositions() {
        return aggregatorTargetPositions;
    }

    @Override
    public Function build(Object[] levelValues) throws Exception {
        Function inner = innerFunctionBuilder.build(levelValues);

        AggregatorHolder[] holders = new AggregatorHolder[aggregatorTargetPositions.length];

        for (int i = 0; i < aggregatorBuilders.length; i++) {
            AbstractAggregator aggregator = aggregatorBuilders[i].build();

            AggregatorHolder holder = new AggregatorHolder(aggregatorTargetPositions[i]);
            holder.setAggregator(aggregator);
            holders[i] = holder;
        }

        return new AggregateFunction(realTableRowSpec, finalTableRowSpec, rowIdGenerator, holders,
                inner, pinned);
    }
}
