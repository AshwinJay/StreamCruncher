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
 * Author: Ashwin Jayaprakash Date: Feb 25, 2006 Time: 9:53:22 AM
 */

public class TableSpec extends Spec {
    private static final long serialVersionUID = 1L;

    protected final RowSpec rowSpec;

    protected final IndexSpec[] indexSpecs;

    protected final MiscSpec[] otherClauses;

    protected final boolean partitioned;

    protected final boolean virtual;

    // ------------------------

    private final int hashCode;

    private final String str;

    // ------------------------

    /**
     * <i>Internal use.</i>
     * 
     * @param schema
     * @param name
     * @param rowSpec
     * @param indexSpecs
     * @param otherClauses
     * @param partitioned
     * @see #TableSpec(String, String, RowSpec, IndexSpec[], MiscSpec[])
     */
    public TableSpec(String schema, String name, RowSpec rowSpec, IndexSpec[] indexSpecs,
            MiscSpec[] otherClauses, boolean partitioned, boolean virtual) {
        super(schema, name);

        this.rowSpec = rowSpec;
        this.indexSpecs = (indexSpecs == null) ? new IndexSpec[] {} : indexSpecs;
        this.otherClauses = (otherClauses == null) ? new MiscSpec[] {} : otherClauses;
        this.partitioned = partitioned;
        this.virtual = virtual;

        this.str = schema + "." + name + ", " + rowSpec.toString() + ", "
                + Arrays.asList(this.indexSpecs) + ", " + Arrays.asList(this.otherClauses) + ", "
                + partitioned + ", " + virtual;
        this.hashCode = this.str.hashCode();
    }

    /**
     * Non-virtual, Non-Partitioned Table.
     * 
     * @param schema
     * @param name
     * @param rowSpec
     * @param indexSpecs
     * @param otherClauses
     */
    public TableSpec(String schema, String name, RowSpec rowSpec, IndexSpec[] indexSpecs,
            MiscSpec[] otherClauses) {
        this(schema, name, rowSpec, indexSpecs, otherClauses, false, false);
    }

    /**
     * Non-virtual, Non-Partitioned Table.
     * 
     * @param schema
     * @param name
     * @param rowSpec
     */
    public TableSpec(String schema, String name, RowSpec rowSpec) {
        this(schema, name, rowSpec, null, null, false, false);
    }

    /**
     * @return Returns the rowSpec.
     */
    public RowSpec getRowSpec() {
        return rowSpec;
    }

    /**
     * @return Returns the indexSpecs.
     */
    public IndexSpec[] getIndexSpecs() {
        return indexSpecs;
    }

    /**
     * @return Returns the otherClauses.
     */
    public MiscSpec[] getOtherClauses() {
        return otherClauses;
    }

    /**
     * @return Returns the partitioned.
     */
    public boolean isPartitioned() {
        return partitioned;
    }

    public boolean isVirtual() {
        return virtual;
    }

    // ------------------------

    public String constructCreateCommand() {
        String ddl = "create table " + getFQN() + "(";

        String[] columnNames = rowSpec.getColumnNames();
        String[] columnTypes = rowSpec.getColumnNativeTypes();
        for (int i = 0; i < columnTypes.length; i++) {
            ddl = ddl + columnNames[i] + " " + columnTypes[i];

            if (i < columnNames.length - 1) {
                ddl = ddl + ",";
            }
        }

        ddl = ddl + ")";

        return ddl;
    }

    public String constructDropCommand() {
        return "drop table " + getFQN();
    }

    // ------------------------

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TableSpec) {
            TableSpec that = (TableSpec) obj;

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
