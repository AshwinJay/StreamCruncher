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
import streamcruncher.api.artifact.IndexSpec;
import streamcruncher.api.artifact.MiscSpec;
import streamcruncher.api.artifact.RowSpec;
import streamcruncher.api.artifact.TableSpec;
import streamcruncher.innards.core.partition.aggregate.AbstractAggregatedColumnDDLHelper;
import streamcruncher.innards.db.DatabaseInterface;
import streamcruncher.innards.impl.artifact.H2IndexSpec;
import streamcruncher.innards.impl.query.DDLHelper;
import streamcruncher.innards.impl.query.H2DDLHelper;
import streamcruncher.innards.impl.query.H2Parser;
import streamcruncher.innards.query.Parser;

/*
 * Author: Ashwin Jayaprakash Date: Jul 22, 2006 Time: 6:52:50 PM
 */

public class H2DatabaseInterface extends DatabaseInterface {
    @Override
    public Class<? extends Parser> getParser() {
        return H2Parser.class;
    }

    @Override
    public DBName getDBName() {
        return DBName.H2;
    }

    @Override
    public AbstractAggregatedColumnDDLHelper getAggregatedColumnDDLHelper() {
        return new H2DDLHelper();
    }

    @Override
    public DDLHelper getDDLHelper() {
        return new H2DDLHelper();
    }

    @Override
    public TableSpec createUnpartitionedTableSpec(String schema, String name, RowSpec rowSpec,
            IndexSpec[] indexSpecs, MiscSpec[] otherClauses) {
        return new TableSpec(schema, name, rowSpec, indexSpecs, otherClauses);
    }

    @Override
    public IndexSpec createIndexSpec(String schema, String name, String tableName, boolean unique,
            String columnName, boolean ascending) {
        return new H2IndexSpec(schema, name, tableName, unique, columnName, ascending);
    }

    @Override
    public IndexSpec createIndexSpec(String schema, String name, String tableName, boolean unique,
            String[] columnNames, boolean[] ascending) {
        return new H2IndexSpec(schema, name, tableName, unique, columnNames, ascending);
    }
}
