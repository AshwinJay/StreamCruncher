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

import java.io.Serializable;
import java.util.LinkedHashMap;

import streamcruncher.api.aggregator.AbstractAggregator;
import streamcruncher.api.aggregator.AbstractAggregator.AggregationStage;
import streamcruncher.innards.core.partition.function.FunctionSupportException;

/*
 * Author: Ashwin Jayaprakash Date: Sep 21, 2006 Time: 11:01:42 PM
 */

public class AggregatorBuilder implements Serializable {
    private static final long serialVersionUID = 1L;

    protected final Class<? extends AbstractAggregator> aggregatorClass;

    protected final String[] params;

    protected final LinkedHashMap<String, String> columnNamesAndTypes;

    protected final AggregationStage aggregationStage;

    private final String message;

    /**
     * @param aggregatorClass
     * @param params
     * @param columnNamesAndTypes
     * @param aggregationStage
     * @throws FunctionSupportException
     */
    public AggregatorBuilder(Class<? extends AbstractAggregator> aggregatorClass, String[] params,
            LinkedHashMap<String, String> columnNamesAndTypes, AggregationStage aggregationStage)
            throws FunctionSupportException {
        this.aggregatorClass = aggregatorClass;
        this.params = params;
        this.columnNamesAndTypes = columnNamesAndTypes;
        this.aggregationStage = aggregationStage;

        this.message = "Could not create an instance of " + aggregatorClass.getClass().getName()
                + " Class";

        try {
            // Test dynamic loading. Fail fast.
            aggregatorClass.newInstance();
        }
        catch (Exception e) {
            throw new FunctionSupportException(message, e);
        }
    }

    public Class<? extends AbstractAggregator> getAggregatorClass() {
        return aggregatorClass;
    }

    public LinkedHashMap<String, String> getColumnNamesAndTypes() {
        return columnNamesAndTypes;
    }

    public String[] getParams() {
        return params;
    }

    // ------------

    public AbstractAggregator build() throws FunctionSupportException {
        AbstractAggregator aggregator = null;

        try {
            aggregator = aggregatorClass.newInstance();
            aggregator.init(params, columnNamesAndTypes, aggregationStage);
        }
        catch (Exception e) {
            /*
             * Instantiation was already tested before. An exception again is
             * quite improbable.
             */
            throw new FunctionSupportException(message, e);
        }

        return aggregator;
    }
}
