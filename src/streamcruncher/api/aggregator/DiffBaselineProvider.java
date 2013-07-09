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

import streamcruncher.api.Provider;

/*
 * Author: Ashwin Jayaprakash Date: Mar 27, 2007 Time: 8:54:41 PM
 */

/**
 * <p>
 * This Provider Class allows the customization of those Partitions with
 * Aggregates that have been declared with the <code>$diff</code> keyword.
 * This Class is used to compute the Baseline for performing the <b>"Diff: New
 * Value - Baseline Value"</b>. By default, the Value from the previous cycle
 * (absolute value, not the diff) is used as the Baseline.
 * </p>
 * <p>
 * Note: Sub-classes must return the Type appropriate for the Aggregate Function
 * on which it is configured.
 * </p>
 */
public class DiffBaselineProvider<T extends Comparable> implements Provider {
    public static final String name = "DiffBaseline/Default";

    /**
     * @param oldValue
     *            Aggregate from previous cycle
     * @param newValue
     *            Aggregate from current cycle
     * @return Baseline value.
     */
    public T getBaseline(T oldValue, T newValue) {
        return oldValue;
    }

    /**
     * @return {@value #name}.
     */
    public static String getName() {
        return name;
    }
}
