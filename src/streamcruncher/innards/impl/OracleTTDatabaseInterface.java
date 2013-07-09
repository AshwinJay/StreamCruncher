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
package streamcruncher.innards.impl;

import streamcruncher.api.DBName;
import streamcruncher.innards.core.partition.aggregate.AbstractAggregatedColumnDDLHelper;
import streamcruncher.innards.db.DatabaseInterface;
import streamcruncher.innards.impl.query.DDLHelper;
import streamcruncher.innards.impl.query.OracleTTDDLHelper;
import streamcruncher.innards.impl.query.OracleTTParser;
import streamcruncher.innards.query.Parser;

/*
 * Author: Ashwin Jayaprakash Date: Jul 22, 2006 Time: 6:52:50 PM
 */

public class OracleTTDatabaseInterface extends DatabaseInterface {
    @Override
    public Class<? extends Parser> getParser() {
        return OracleTTParser.class;
    }

    @Override
    public DBName getDBName() {
        return DBName.OracleTT;
    }

    @Override
    public AbstractAggregatedColumnDDLHelper getAggregatedColumnDDLHelper() {
        return new OracleTTDDLHelper();
    }

    @Override
    public DDLHelper getDDLHelper() {
        return new OracleTTDDLHelper();
    }

}
