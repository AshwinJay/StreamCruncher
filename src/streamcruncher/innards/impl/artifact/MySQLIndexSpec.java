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
package streamcruncher.innards.impl.artifact;

import streamcruncher.api.artifact.IndexSpec;

/*
 * Author: Ashwin Jayaprakash Date: Nov 9, 2006 Time: 12:21:58 PM
 */

public class MySQLIndexSpec extends IndexSpec {
    private static final long serialVersionUID = 1L;

    /**
     * @param schema
     * @param name
     * @param tableFQN
     * @param unique
     * @param columnName
     * @param ascending
     */
    public MySQLIndexSpec(String schema, String name, String tableName, boolean unique,
            String columnName, boolean ascending) {
        super(schema, name, tableName, unique, columnName, ascending);
    }

    /**
     * @param schema
     * @param name
     * @param tableFQN
     * @param unique
     * @param columnNames
     * @param ascending
     */
    public MySQLIndexSpec(String schema, String name, String tableName, boolean unique,
            String[] columnNames, boolean[] ascending) {
        super(schema, name, tableName, unique, columnNames, ascending);
    }

    @Override
    public String constructCreateCommand() {
        String type = (unique ? "hash" : "btree");

        String ddl = "create " + (unique ? "unique" : "") + " index " + getFQN() + " using " + type
                + " on " + getTableFQN() + "(";

        for (int i = 0; i < columnNames.length; i++) {
            ddl = ddl + columnNames[i] + " " + (ascending[i] ? "asc" : "desc");

            if (i < columnNames.length - 1) {
                ddl = ddl + ",";
            }
        }

        ddl = ddl + ")";

        return ddl;
    }

    @Override
    public String constructDropCommand() {
        return "drop index " + getFQN() + " on " + getTableFQN();
    }
}
