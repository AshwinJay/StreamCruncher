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
package streamcruncher.api.aggregator;

import java.util.LinkedHashMap;

import streamcruncher.api.DBName;
import streamcruncher.api.StartupShutdownHook;

/*
 * Author: Ashwin Jayaprakash Date: Sep 23, 2006 Time: 12:26:18 PM
 */

/**
 * <p>
 * An {@linkplain AbstractAggregator Aggregator} must be accompanied by a
 * Sub-class of this Helper, which provides all the details regarding the
 * Aggregator being registered.
 * </p>
 * <p>
 * When the Kernel stops and restarts, the Aggregates must be registered again,
 * before the Queries start executing. See {@link StartupShutdownHook}.
 * </p>
 */
public abstract class AbstractAggregatorHelper {
    protected final String functionName;

    protected final Class<? extends AbstractAggregator> aggregatorClass;

    /**
     * @param functionName
     *            The name of the function that will be used in the Query, which
     *            indicates that the Aggregate (being registered) must be
     *            invoked.
     * @param aggregatorClass
     *            The Aggregator Class that is being represented/registered by
     *            this Helper.
     * @see #AbstractAggregatorHelper(String, Class)
     */
    public AbstractAggregatorHelper(String functionName,
            Class<? extends AbstractAggregator> aggregatorClass) {
        this.functionName = functionName;
        this.aggregatorClass = aggregatorClass;
    }

    public String getFunctionName() {
        return functionName;
    }

    public Class<? extends AbstractAggregator> getAggregatorClass() {
        return aggregatorClass;
    }

    // ------------

    /**
     * @param name
     * @param params
     *            That parameters that were provided in the Query to the
     *            Function. Ex: A definition such as
     *            <code>with custom(test_fn, order_id, J) as test_fn_val)</code>
     *            will produce <code>String[]{"order_id", "J"}</code>
     * @param columnNamesAndTypes
     *            The names and SQL Native Types in the Source Table.
     * @return "varchar2(10)" or "integer".
     * @throws Exception
     *             If aggregation is not possible on the columns chosen.
     */
    public abstract String getAggregatedColumnDDLFragment(DBName name, String[] params,
            LinkedHashMap<String, String> columnNamesAndTypes) throws Exception;
}
