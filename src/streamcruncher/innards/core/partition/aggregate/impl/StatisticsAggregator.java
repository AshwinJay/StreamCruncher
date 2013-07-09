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
package streamcruncher.innards.core.partition.aggregate.impl;

import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;

import streamcruncher.innards.core.partition.aggregate.GenericSingleSrcColumnAggregator;

/*
 * Author: Ashwin Jayaprakash Date: Oct 9, 2006 Time: 10:22:25 PM
 */

public abstract class StatisticsAggregator extends GenericSingleSrcColumnAggregator<Comparable> {
    private static final long NEGATIVE_INFINITY_BITS = Double
            .doubleToLongBits(Double.NEGATIVE_INFINITY);

    private static final long POSITIVE_INFINITY_BITS = Double
            .doubleToLongBits(Double.POSITIVE_INFINITY);

    private static final long NaN_BITS = Double.doubleToLongBits(Double.NaN);

    protected DescriptiveStatistics stats;

    @Override
    public void init(String[] params, LinkedHashMap<String, String> columnNamesAndTypes,
            AggregationStage aggregationStage) {
        super.init(params, columnNamesAndTypes, aggregationStage);

        stats = DescriptiveStatistics.newInstance();
    }

    /**
     * {@inheritDoc}
     * 
     * @return <code>null</code> when result is
     *         {@link Double#NEGATIVE_INFINITY}, {@link Double#NaN} or
     *         {@link Double#POSITIVE_INFINITY}.
     */
    @Override
    protected Double fetchAggregatedValue() {
        stats.clear();

        for (Comparable c : valuesAndCounts.keySet()) {
            if (c instanceof Number) {
                AtomicInteger count = valuesAndCounts.get(c);
                Number num = (Number) c;
                double d = num.doubleValue();

                for (int i = count.get(); i > 0; i--) {
                    // Add the same item 'x' times.
                    stats.addValue(d);
                }
            }
        }

        Double d = getStats();
        if (d != null) {
            /*
             * Double's "==" does not work for Infinity and NaN.
             */
            long bits = Double.doubleToLongBits(d);

            if (bits == NEGATIVE_INFINITY_BITS || bits == POSITIVE_INFINITY_BITS
                    || bits == NaN_BITS) {
                d = null;
            }
        }

        return d;
    }

    /**
     * @return Use {@link stats}.
     */
    protected abstract Double getStats();

    @Override
    protected Comparable doDiff(Comparable oldValue, Comparable newValue) {
        double oldVal = 0.0d;
        if (oldValue != null && oldValue instanceof Number) {
            oldVal = ((Number) oldValue).doubleValue();
        }

        double newVal = 0.0d;
        if (newValue != null && newValue instanceof Number) {
            newVal = ((Number) newValue).doubleValue();
        }

        return new Double(newVal - oldVal);
    }
}
