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
package streamcruncher.innards.core.partition.aggregate;

import java.util.LinkedHashMap;

import streamcruncher.api.aggregator.AbstractAggregator;
import streamcruncher.api.aggregator.AbstractAggregatorHelper;
import streamcruncher.api.aggregator.AbstractAggregator.AggregationStage;
import streamcruncher.innards.core.partition.function.FunctionSupportException;

/*
 * Author: Ashwin Jayaprakash Date: Nov 20, 2006 Time: 11:24:02 PM
 */

public class BuilderCreator {
    protected final AbstractAggregatorHelper helper;

    public BuilderCreator(final AbstractAggregatorHelper helper) {
        this.helper = helper;
    }

    @SuppressWarnings("unchecked")
    public AggregatorBuilder createBuilder(String[] params,
            LinkedHashMap<String, String> columnNamesAndTypes, AggregationStage aggregationStage)
            throws FunctionSupportException {
        Class<? extends AbstractAggregator> class1 = helper.getAggregatorClass();

        return new AggregatorBuilder(class1, params, columnNamesAndTypes, aggregationStage);
    }
}
