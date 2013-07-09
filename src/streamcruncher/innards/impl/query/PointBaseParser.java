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
import streamcruncher.innards.impl.artifact.PointBaseIndexSpec;
import streamcruncher.innards.query.QueryParseException;

/*
 * Author: Ashwin Jayaprakash Date: May 28, 2006 Time: 11:43:08 PM
 */

public class PointBaseParser extends AbstractParser {
    public PointBaseParser(ParserParameters parserParameters) throws QueryParseException {
        super(parserParameters);
    }

    @Override
    protected TableSpec customizeResultTableSpec(RowSpec rowSpec, IndexSpec[] indexSpecs) {
        rowSpec.getColumnNativeTypes()[Constants.ID_COLUMN_POS] = getResultTableIdColumnType()
                + " primary key";

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
        return "bigint not null";
    }

    @Override
    protected String getResultTableIdColumnType() {
        return "decimal(18,0) identity not null";
    }

    @Override
    protected String getTimestampColumnType() {
        return "timestamp";
    }

    @Override
    protected String getVersionColumnType() {
        return "bigint";
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
        return new PointBaseIndexSpec(schema, name, tableFQN, unique, columnName, ascending);
    }

    @Override
    protected IndexSpec createIndexSpec(String schema, String name, String tableFQN,
            boolean unique, String[] columnNames, boolean[] ascending) {
        return new PointBaseIndexSpec(schema, name, tableFQN, unique, columnNames, ascending);
    }

    @Override
    protected DBName getDBName() {
        return DBName.PointBase;
    }

    @Override
    protected DDLHelper getDDLHelper() {
        return new PointBaseDDLHelper();
    }

    @Override
    protected String[] getInsertIntoColumns(String[] resultTableColumns) {
        String[] newArray = new String[resultTableColumns.length - 1];
        for (int i = 1; i < resultTableColumns.length; i++) {
            newArray[i - 1] = resultTableColumns[i];
        }

        return newArray;
    }

    @Override
    protected String getFirstIdColumnInResultSQL() {
        return "";
    }

    @Override
    protected boolean asSupportedInAlias() {
        return true;
    }
}
