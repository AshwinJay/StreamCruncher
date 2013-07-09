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
import java.util.List;

/*
 * Author: Ashwin Jayaprakash Date: Sep 21, 2006 Time: 10:59:37 PM
 */
/**
 * <p>
 * Custom Aggregator functions can be plugged into the Kernel before the Query
 * is registered. The APIs provide a means to register and unregister these
 * custom functions. Such custom Aggregators must extend this Class. The
 * Sub-classes must have a <b>no argument constructor</b>.
 * </p>
 * <p>
 * When the Kernel creates a Window in a Partition that uses the function name
 * against which this Aggregate is registered, an instance of the registered
 * Class is created and used. When the Window expires, the corresponding
 * Aggregator instance is also discarded.
 * </p>
 */
public abstract class AbstractAggregator {
    private String[] params;

    private LinkedHashMap<String, String> columnNamesAndTypes;

    private AggregationStage aggregationStage;

    /**
     * <b>Note:</b> This class' method must be invoked even if it is
     * over-ridden (i.e <code>super.init(...)</code>).
     * 
     * @param params
     *            Parameters that were supplied to the function in the "Running
     *            Query". Ex: A definition such as
     *            <code>with custom(test_fn, order_id, J) as test_fn_val</code>
     *            will produce <code>String[]{"order_id", "J"}</code>
     * @param columnNamesAndTypes
     *            The same order in which the columns are placed in Lists in
     *            {@link #aggregate(List, List)}.
     * @param aggregationStage
     */
    public void init(String[] params, LinkedHashMap<String, String> columnNamesAndTypes,
            AggregationStage aggregationStage) {
        this.params = params;
        this.columnNamesAndTypes = columnNamesAndTypes;
        this.aggregationStage = aggregationStage;
    }

    public LinkedHashMap<String, String> getColumnNamesAndTypes() {
        return columnNamesAndTypes;
    }

    public String[] getParams() {
        return params;
    }

    public AggregationStage getAggregationStage() {
        return aggregationStage;
    }

    // -------------

    /**
     * <p>
     * At the end of each Query execution, this method will be called to
     * aggregate the Events in the Window over which this Aggregate is created.
     * </p>
     * <p>
     * One of the parameters (added/removed) can be <code>null</code>. But,
     * never both.
     * </p>
     * 
     * @param removedValues
     *            List of rows that were removed in the current cycle. Each
     *            array in the list is a group of columns in that Row.
     * @param addedValues
     *            List of rows added in the current cycle. Each array in the
     *            list is a group of columns in that Row.
     * @return The aggregated value or <code>null</code> for some cases (Ex:
     *         In-built functions return a <code>null</code> if the aggregate
     *         calculation yields a NaN).
     * @see #columnNamesAndTypes for the names and types of the data/columns.
     */
    public abstract Object aggregate(List<Object[]> removedValues, List<Object[]> addedValues);

    // -------------

    /**
     * <p>
     * This feature is to provide a choice for the User to decide whether an
     * Event's entrance into a Window only or both entrance and exit should
     * affect the aggregate.
     * </p>
     * <p>
     * The default is {@link AggregationStage#BOTH}.
     * </p>
     * 
     * @since 1.03 Beta
     */
    public static enum AggregationStage {
        ENTRANCE, BOTH;
    }
}
