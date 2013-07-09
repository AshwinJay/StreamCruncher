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
package streamcruncher.innards.util;

import streamcruncher.api.aggregator.AbstractAggregator;

/*
 * Author: Ashwin Jayaprakash Date: Sep 23, 2006 Time: 8:05:00 PM
 */

public class AggregatorHolder {
    protected final int targetColumnPos;

    protected AbstractAggregator aggregator;

    public AggregatorHolder(int targetColumnPos) {
        this.targetColumnPos = targetColumnPos;
    }

    public int getTargetColumnPos() {
        return targetColumnPos;
    }

    public AbstractAggregator getAggregator() {
        return aggregator;
    }

    public void setAggregator(AbstractAggregator aggregator) {
        this.aggregator = aggregator;
    }
}
