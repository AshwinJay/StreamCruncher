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

import streamcruncher.api.DBName;
import streamcruncher.api.aggregator.AbstractAggregatorHelper;
import streamcruncher.boot.Registry;
import streamcruncher.innards.core.partition.aggregate.AbstractAggregatedColumnDDLHelper;
import streamcruncher.innards.db.DatabaseInterface;
import streamcruncher.innards.query.Aggregator;
import streamcruncher.innards.query.QueryParseException;

/*
 * Author: Ashwin Jayaprakash Date: Sep 26, 2006 Time: 10:17:50 PM
 */

public class AvgAggregatorHelper extends AbstractAggregatorHelper {
    public AvgAggregatorHelper() {
        super(Aggregator.AVG.name(), AvgAggregator.class);
    }

    @Override
    public String getAggregatedColumnDDLFragment(DBName dbName, String[] params,
            LinkedHashMap<String, String> columnNamesAndTypes) throws QueryParseException {
        DatabaseInterface databaseInterface = Registry.getImplFor(DatabaseInterface.class);
        AbstractAggregatedColumnDDLHelper columnDDLHelper = databaseInterface
                .getAggregatedColumnDDLHelper();

        return columnDDLHelper.getDDLFragment(Aggregator.AVG, dbName, params, columnNamesAndTypes);
    }
}
