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

import java.util.Properties;

import streamcruncher.api.DBName;
import streamcruncher.innards.core.partition.aggregate.AbstractAggregatedColumnDDLHelper;
import streamcruncher.innards.db.DatabaseInterface;
import streamcruncher.innards.impl.query.DDLHelper;
import streamcruncher.innards.impl.query.DerbyDDLHelper;
import streamcruncher.innards.impl.query.DerbyParser;
import streamcruncher.innards.query.Parser;

/*
 * Author: Ashwin Jayaprakash Date: Dec 31, 2005 Time: 9:21:28 PM
 */

public class DerbyDatabaseInterface extends DatabaseInterface {
    @Override
    public void start(Object... params) throws Exception {
        Properties props = (Properties) params[0];
        Properties sysProps = System.getProperties();
        sysProps.put("org.apache.derby.system.home", props.getProperty("db.derby.home"));

        super.start(params);
    }

    // ---------------------

    @Override
    public Class<? extends Parser> getParser() {
        return DerbyParser.class;
    }

    @Override
    public DBName getDBName() {
        return DBName.Derby;
    }

    @Override
    public AbstractAggregatedColumnDDLHelper getAggregatedColumnDDLHelper() {
        return new DerbyDDLHelper();
    }

    @Override
    public DDLHelper getDDLHelper() {
        return new DerbyDDLHelper();
    }
}
