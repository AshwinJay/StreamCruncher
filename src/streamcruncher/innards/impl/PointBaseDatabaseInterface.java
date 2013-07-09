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
import streamcruncher.api.artifact.IndexSpec;
import streamcruncher.innards.core.partition.aggregate.AbstractAggregatedColumnDDLHelper;
import streamcruncher.innards.db.DatabaseInterface;
import streamcruncher.innards.impl.artifact.PointBaseIndexSpec;
import streamcruncher.innards.impl.query.DDLHelper;
import streamcruncher.innards.impl.query.PointBaseDDLHelper;
import streamcruncher.innards.impl.query.PointBaseParser;
import streamcruncher.innards.query.Parser;

/*
 * Author: Ashwin Jayaprakash Date: Jul 22, 2006 Time: 6:52:50 PM
 */

public class PointBaseDatabaseInterface extends DatabaseInterface {
    @Override
    public void start(Object... params) throws Exception {
        Properties props = (Properties) params[0];
        Properties sysProps = System.getProperties();
        sysProps.put("pointbase.ini", props.getProperty("db.pointbase.ini"));

        super.start(params);
    }

    // ---------------------

    @Override
    public Class<? extends Parser> getParser() {
        return PointBaseParser.class;
    }

    @Override
    public DBName getDBName() {
        return DBName.PointBase;
    }

    @Override
    public AbstractAggregatedColumnDDLHelper getAggregatedColumnDDLHelper() {
        return new PointBaseDDLHelper();
    }

    @Override
    public DDLHelper getDDLHelper() {
        return new PointBaseDDLHelper();
    }

    @Override
    public IndexSpec createIndexSpec(String schema, String name, String tableName, boolean unique,
            String columnName, boolean ascending) {
        return new PointBaseIndexSpec(schema, name, tableName, unique, columnName, ascending);
    }

    @Override
    public IndexSpec createIndexSpec(String schema, String name, String tableName, boolean unique,
            String[] columnNames, boolean[] ascending) {
        return new PointBaseIndexSpec(schema, name, tableName, unique, columnNames, ascending);
    }
}
