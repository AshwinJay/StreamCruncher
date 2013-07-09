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
package streamcruncher.api.artifact;

import java.util.Arrays;

/*
 * Author: Ashwin Jayaprakash Date: Apr 2, 2006 Time: 10:56:52 AM
 */

/**
 * <p>
 * A definition of an Index on one or more columns of an Input or Output Event
 * Stream ({@link TableSpec}).
 * </p>
 * These Classes must <b>not be instantiated directly</b>, but must be created
 * through the API.
 */
public class IndexSpec extends Spec {
    private static final long serialVersionUID = 1L;

    protected final String tableName;

    protected final boolean unique;

    protected final String[] columnNames;

    protected final boolean[] ascending;

    // ------------------------

    private final int hashCode;

    private final String str;

    // ------------------------

    /**
     * @param schema
     *            Schema of this Index and the Table this is related to.
     *            <code>null</code> allowed.
     * @param name
     *            Name of this Index
     * @param tableName
     *            Name of the Table to which this Index is related.
     * @param unique
     * @param columnName
     * @param ascending
     */
    public IndexSpec(String schema, String name, String tableName, boolean unique,
            String columnName, boolean ascending) {
        this(schema, name, tableName, unique, new String[] { columnName },
                new boolean[] { ascending });
    }

    /**
     * @param schema
     *            Schema of this Index and the Table this is related to.
     *            <code>null</code> allowed.
     * @param name
     *            Name of this Index
     * @param tableName
     *            Name of the Table to which this Index is related.
     * @param unique
     * @param columnNames
     * @param ascending
     */
    public IndexSpec(String schema, String name, String tableName, boolean unique,
            String[] columnNames, boolean[] ascending) {
        super(schema, name);

        this.tableName = tableName;
        this.unique = unique;
        this.columnNames = columnNames;
        this.ascending = ascending;

        // For Arrays.asList() to work.
        Boolean[] boolObjArr = new Boolean[ascending.length];
        for (int i = 0; i < ascending.length; i++) {
            boolObjArr[i] = ascending[i];
        }

        this.str = schema + "." + name + ", " + tableName + ", " + unique + ", "
                + Arrays.asList(columnNames) + ", " + Arrays.asList(boolObjArr);
        this.hashCode = this.str.hashCode();
    }

    /**
     * @return Returns the ascending.
     */
    public boolean[] getAscending() {
        return ascending;
    }

    /**
     * @return Returns the columnNames.
     */
    public String[] getColumnNames() {
        return columnNames;
    }

    /**
     * @return Returns the tableName.
     */
    public String getTableName() {
        return tableName;
    }

    public String getTableFQN() {
        if (hasSchema()) {
            return schema + "." + tableName;
        }

        return tableName;
    }

    /**
     * @return Returns the unique.
     */
    public boolean isUnique() {
        return unique;
    }

    // ------------------------

    public String constructCreateCommand() {
        String ddl = "create " + (unique ? "unique" : "") + " index " + getFQN() + " on "
                + getTableFQN() + "(";

        for (int i = 0; i < columnNames.length; i++) {
            ddl = ddl + columnNames[i] + " " + (ascending[i] ? "asc" : "desc");

            if (i < columnNames.length - 1) {
                ddl = ddl + ",";
            }
        }

        ddl = ddl + ")";

        return ddl;
    }

    public String constructDropCommand() {
        return "drop index " + getFQN();
    }

    // ------------------------

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof IndexSpec) {
            IndexSpec that = (IndexSpec) obj;

            String thisStr = toString();
            String thatStr = that.toString();

            return thisStr.equals(thatStr);
        }

        return false;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public String toString() {
        return str;
    }
}