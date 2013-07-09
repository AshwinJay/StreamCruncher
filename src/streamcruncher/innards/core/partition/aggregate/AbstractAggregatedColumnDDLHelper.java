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

import streamcruncher.api.DBName;
import streamcruncher.innards.query.Aggregator;
import streamcruncher.innards.query.QueryParseException;

/*
 * Author: Ashwin Jayaprakash Date: Sep 26, 2006 Time: 10:35:50 PM
 */

public abstract class AbstractAggregatedColumnDDLHelper {
    public String getDDLFragment(Aggregator aggregator, DBName dbName, String[] params,
            LinkedHashMap<String, String> columnNamesAndTypes) throws QueryParseException {
        // All these functions support only 1 parameter/column name.
        final String givenParamName = params[0];
        String givenParamType = null;
        for (String column : columnNamesAndTypes.keySet()) {
            if (givenParamName.equalsIgnoreCase(column)) {
                givenParamType = columnNamesAndTypes.get(column);
                break;
            }
        }

        if (givenParamType == null) {
            throw new QueryParseException("A valid Column name must be provided for aggregation.");
        }

        // ----------

        Pair pair = getPair(aggregator);
        if (pair == null) {
            throw new QueryParseException("Result column datatype information of "
                    + aggregator.name() + ", not found for: " + dbName.name());
        }

        // ----------

        String[] types = pair.getSupportedTypes();
        String returnType = null;

        for (String type : types) {
            if (type.equals(givenParamType)) {
                returnType = pair.getAggregatedType();
                break;
            }
        }

        if (returnType == null) {
            if (types.length == 0) {
                // Ex: Count function supports any column/type.
                returnType = pair.getAggregatedType();

                // Ex: Max/Min, where return-type is same as given type.
                if (returnType == null) {
                    returnType = givenParamType;
                }
            }
            else {
                throw new QueryParseException("Aggregation on " + givenParamType
                        + " not supported for: " + aggregator.name() + " - " + dbName);
            }
        }

        return returnType;
    }

    protected abstract Pair getPair(Aggregator aggregator);

    // ----------

    protected static class Pair {
        protected final String[] supportedTypes;

        protected final String aggregatedType;

        public Pair(String[] supportedTypes, String aggregatedType) {
            this.supportedTypes = supportedTypes;
            this.aggregatedType = aggregatedType;
        }

        public String getAggregatedType() {
            return aggregatedType;
        }

        public String[] getSupportedTypes() {
            return supportedTypes;
        }
    }
}
