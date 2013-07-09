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

import streamcruncher.api.DBName;
import streamcruncher.api.ParserParameters;
import streamcruncher.api.artifact.IndexSpec;
import streamcruncher.api.artifact.MiscSpec;
import streamcruncher.api.artifact.RowSpec;
import streamcruncher.api.artifact.TableSpec;
import streamcruncher.innards.db.Constants;
import streamcruncher.innards.impl.artifact.H2IndexSpec;
import streamcruncher.innards.query.QueryParseException;

/*
 * Author: Ashwin Jayaprakash Date: May 28, 2006 Time: 11:48:17 AM
 */

public class H2Parser extends AbstractParser {
    public H2Parser(ParserParameters parserParameters) throws QueryParseException {
        super(parserParameters);
    }

    @Override
    protected TableSpec customizeResultTableSpec(RowSpec rowSpec, IndexSpec[] indexSpecs) {
        rowSpec.getColumnNativeTypes()[Constants.ID_COLUMN_POS] = "bigint not null auto_increment primary key";

        TableSpec newTableSpec = new TableSpec(resultTable.getSchema(), resultTable.getName(),
                rowSpec, indexSpecs, null);

        return newTableSpec;
    }

    @Override
    protected boolean isUniqueIndexOnResultTableReqd() {
        // Primary key clause automatically creates an Index.
        return false;
    }

    @Override
    protected String getIdColumnType() {
        return java.lang.Long.class.getName();
    }

    @Override
    protected String getTimestampColumnType() {
        return java.sql.Timestamp.class.getName();
    }

    @Override
    protected String getVersionColumnType() {
        return java.lang.Long.class.getName();
    }

    @Override
    protected TableSpec createTableSpec(String schema, String name, RowSpec rowSpec,
            IndexSpec[] indexSpecs, MiscSpec[] otherClauses, boolean partitioned, boolean virtual) {
        return new TableSpec(schema, name, rowSpec, indexSpecs, otherClauses, partitioned, virtual);
    }

    @Override
    protected TableSpec createUnpartitionedTableSpec(String schema, String name, RowSpec rowSpec,
            IndexSpec[] indexSpecs, MiscSpec[] otherClauses) {
        return new TableSpec(schema, name, rowSpec, indexSpecs, otherClauses);
    }

    @Override
    protected IndexSpec createIndexSpec(String schema, String name, String tableFQN,
            boolean unique, String columnName, boolean ascending) {
        return new H2IndexSpec(schema, name, tableFQN, unique, columnName, ascending);
    }

    @Override
    protected IndexSpec createIndexSpec(String schema, String name, String tableFQN,
            boolean unique, String[] columnNames, boolean[] ascending) {
        return new H2IndexSpec(schema, name, tableFQN, unique, columnNames, ascending);
    }

    @Override
    protected DBName getDBName() {
        return DBName.H2;
    }

    @Override
    protected DDLHelper getDDLHelper() {
        return new H2DDLHelper();
    }

    @Override
    protected String[] getInsertIntoColumns(String[] resultTableColumns) {
        return resultTableColumns;
    }

    @Override
    protected String getFirstIdColumnInResultSQL() {
        return "null";
    }

    @Override
    protected boolean asSupportedInAlias() {
        return true;
    }
}