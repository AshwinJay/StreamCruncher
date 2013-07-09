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
import streamcruncher.innards.impl.artifact.SequenceSpec;
import streamcruncher.innards.query.QueryParseException;

/*
 * Author: Ashwin Jayaprakash Date: May 28, 2006 Time: 11:48:17 AM
 */

public class OracleParser extends AbstractParser {
    public OracleParser(ParserParameters parserParameters) throws QueryParseException {
        super(parserParameters);
    }

    @Override
    protected TableSpec customizeResultTableSpec(RowSpec rowSpec, IndexSpec[] indexSpecs) {
        MiscSpec[] others = { new SequenceSpec(resultTable.getSchema(), resultTable.getName()
                + "_seq", resultTable.getFQN(), null) };

        TableSpec newTableSpec = new TableSpec(resultTable.getSchema(), resultTable.getName(),
                rowSpec, indexSpecs, others);

        return newTableSpec;
    }

    @Override
    protected String getIdColumnType() {
        return "number not null";
    }

    @Override
    protected String getTimestampColumnType() {
        return "timestamp";
    }

    @Override
    protected String getVersionColumnType() {
        return "number";
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
        return new IndexSpec(schema, name, tableFQN, unique, columnName, ascending);
    }

    @Override
    protected IndexSpec createIndexSpec(String schema, String name, String tableFQN,
            boolean unique, String[] columnNames, boolean[] ascending) {
        return new IndexSpec(schema, name, tableFQN, unique, columnNames, ascending);
    }

    @Override
    protected DBName getDBName() {
        return DBName.Oracle;
    }

    @Override
    protected DDLHelper getDDLHelper() {
        return new OracleDDLHelper();
    }

    @Override
    protected String[] getInsertIntoColumns(String[] resultTableColumns) {
        return resultTableColumns;
    }

    @Override
    protected String getFirstIdColumnInResultSQL() {
        return resultTable.getFQN() + "_seq.nextval";
    }

    @Override
    protected boolean asSupportedInAlias() {
        return false;
    }
}