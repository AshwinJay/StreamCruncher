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

import streamcruncher.innards.core.partition.function.Function;

/*
 * Author: Ashwin Jayaprakash Date: Jun 25, 2006 Time: 12:59:23 PM
 */

public class CalculateTSFunctionPair implements Comparable<CalculateTSFunctionPair> {
    protected final long timestamp;

    protected final Function function;

    /**
     * @param timestamp
     * @param function
     *            {@link Function#getHomeFunction()}.
     */
    public CalculateTSFunctionPair(long timestamp, Function function) {
        this.timestamp = timestamp;
        this.function = function;
    }

    /**
     * @return Returns the function.
     */
    public Function getFunction() {
        return function;
    }

    /**
     * @return Returns the timestamp.
     */
    public long getTimestamp() {
        return timestamp;
    }

    // -----------

    public int compareTo(CalculateTSFunctionPair that) {
        return (int) (this.timestamp - that.timestamp);
    }
}
