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
package streamcruncher.innards.impl.query;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

import streamcruncher.innards.core.partition.aggregate.AbstractAggregatedColumnDDLHelper;
import streamcruncher.innards.query.Aggregator;

/*
 * Author: Ashwin Jayaprakash Date: Sep 26, 2006 Time: 10:35:50 PM
 */

public class DerbyDDLHelper extends AbstractAggregatedColumnDDLHelper implements DDLHelper {
    protected static final Map<String, String> NATIVE_TO_JAVA_TYPES = new HashMap<String, String>() {
        {
            put("integer", java.lang.Integer.class.getName());
            put("double", java.lang.Double.class.getName());
            put("bigint", java.lang.Long.class.getName());
            put("varchar", java.lang.String.class.getName());
            put("timestamp", java.sql.Timestamp.class.getName());
        }
    };

    protected static final Map<String, String> JAVA_TO_NATIVE_TYPES = new HashMap<String, String>() {
        {
            put(java.lang.Integer.class.getName(), "integer");
            put(java.lang.Float.class.getName(), "double");
            put(java.lang.Double.class.getName(), "double");
            put(java.lang.Long.class.getName(), "bigint");
            put(java.lang.String.class.getName(), "varchar");
            put(java.sql.Timestamp.class.getName(), "timestamp");
        }
    };

    protected static final EnumMap<Aggregator, Pair> MAPPING = new EnumMap<Aggregator, Pair>(
            Aggregator.class) {
        {
            final Pair statsPair = new Pair(new String[] { java.lang.Integer.class.getName(),
                    java.lang.Long.class.getName(), java.lang.Double.class.getName() },
                    java.lang.Double.class.getName());

            put(Aggregator.AVG, statsPair);
            put(Aggregator.COUNT, new Pair(new String[] {}, java.lang.Integer.class.getName()));
            put(Aggregator.GEOMEAN, statsPair);
            put(Aggregator.KURTOSIS, statsPair);
            put(Aggregator.MAX, new Pair(new String[] {}, null));
            put(Aggregator.MEDIAN, statsPair);
            put(Aggregator.MIN, new Pair(new String[] {}, null));
            put(Aggregator.SKEWNESS, statsPair);
            put(Aggregator.STDDEV, statsPair);
            put(Aggregator.SUM, statsPair);
            put(Aggregator.SUMSQ, statsPair);
            put(Aggregator.VARIANCE, statsPair);
        }
    };

    public String getJavaType(String nativeType) {
        return NATIVE_TO_JAVA_TYPES.get(nativeType);
    }

    public String[] getJavaTypes() {
        return JAVA_TO_NATIVE_TYPES.keySet().toArray(new String[JAVA_TO_NATIVE_TYPES.size()]);
    }

    public String getNativeType(String javaType) {
        return JAVA_TO_NATIVE_TYPES.get(javaType);
    }

    public String[] getNativeTypes() {
        return NATIVE_TO_JAVA_TYPES.keySet().toArray(new String[NATIVE_TO_JAVA_TYPES.size()]);
    }

    @Override
    protected Pair getPair(Aggregator aggregator) {
        return MAPPING.get(aggregator);
    }
}
