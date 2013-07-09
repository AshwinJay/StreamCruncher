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

import java.io.Serializable;

import streamcruncher.api.artifact.RowSpec;
import streamcruncher.innards.core.WhereClauseSpec;
import streamcruncher.innards.core.filter.FilterSpec;
import streamcruncher.innards.core.partition.function.AggregateFunctionBuilder;
import streamcruncher.innards.core.partition.function.FunctionBuilder;
import streamcruncher.innards.core.partition.function.TimeWindowFunction;

/*
 * Author: Ashwin Jayaprakash Date: Feb 12, 2006 Time: 2:29:58 PM
 */

public class PartitionSpec extends FilterSpec implements Serializable {
    private static final long serialVersionUID = 1L;

    protected final String[] partitionColumnNames;

    protected final FunctionBuilder functionBuilder;

    /**
     * @param partitionColumnNames
     * @param whereClauseSpec
     *            <code>null</code> allowed.
     * @param functionBuilder
     */
    public PartitionSpec(String[] partitionColumnNames, WhereClauseSpec whereClauseSpec,
            FunctionBuilder functionBuilder) {
        super(whereClauseSpec, null);

        this.partitionColumnNames = partitionColumnNames;
        this.functionBuilder = functionBuilder;
    }

    /**
     * @return Returns the functionBuilder.
     */
    public FunctionBuilder getFunctionBuilder() {
        return functionBuilder;
    }

    /**
     * @return Returns the partitionColumnNames.
     */
    public String[] getPartitionColumnNames() {
        return partitionColumnNames;
    }

    // ---------

    protected FunctionBuilder getCoreFB() {
        FunctionBuilder fb = functionBuilder;

        if (fb instanceof AggregateFunctionBuilder) {
            fb = ((AggregateFunctionBuilder) fb).getInnerFunctionBuilder();
        }

        return fb;
    }

    @Override
    public RowSpec getSourceTableRowSpec() {
        FunctionBuilder fb = getCoreFB();
        return fb.getRealTableRowSpec();
    }

    @Override
    public RowSpec getTargetTableRowSpec() {
        FunctionBuilder fb = getCoreFB();
        return fb.getFinalTableRowSpec();
    }

    /**
     * @return Only those columns specified in the
     *         {@link PartitionSpec#getFunctionBuilder()}'s (or, if the builder
     *         is an {@link AggregateFunctionBuilder}, then the
     *         {@link AggregateFunctionBuilder#getInnerFunctionBuilder()} is
     *         used)
     *         {@link streamcruncher.innards.core.partition.function.FunctionBuilder#getRealTableRowSpec()}.
     *         If the Inner-function is a {@link TimeWindowFunction}, then the
     *         {@link TimeWindowFunction#getTimeColumnName()} is also included
     */
    public String[] getColNameArrToSelectFromSrc() {
        FunctionBuilder fb = getCoreFB();

        RowSpec rowSpec = fb.getRealTableRowSpec();
        return rowSpec.getColumnNames();
    }
}
