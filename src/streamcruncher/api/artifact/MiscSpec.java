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
 * Author: Ashwin Jayaprakash Date: Jul 24, 2006 Time: 11:14:04 PM
 */
/**
 * Any custom artifacts that need to be packaged with the {@link TableSpec} and
 * whose lifecycle must be tied to the main {@link TableSpec} should extend this
 * Class. Example: Sequences or Id Generators.
 */
public abstract class MiscSpec extends Spec {
    private static final long serialVersionUID = 1L;

    protected final String tableName;

    protected final Object[] parameters;

    // ------------------------

    private final int hashCode;

    private final String str;

    // ------------------------

    /**
     * @param schema
     *            Schema of this artifact and the Table this is related to.
     *            <code>null</code> allowed.
     * @param name
     *            Name of this artifact
     * @param tableName
     *            Name of the Table to which this artifact is related.
     * @param parameters
     *            Additional parameters that may be needed.
     */
    public MiscSpec(String schema, String name, String tableName, Object... parameters) {
        super(schema, name);

        this.tableName = tableName;
        this.parameters = (parameters == null) ? new Object[] {} : parameters;

        this.str = schema + "." + name + ", " + tableName + ", " + Arrays.asList(this.parameters);
        this.hashCode = this.str.hashCode();
    }

    public String getTableName() {
        return tableName;
    }

    public String getTableFQN() {
        if (hasSchema()) {
            return schema + "." + tableName;
        }

        return tableName;
    }

    public Object[] getParameters() {
        return parameters;
    }

    // ------------------------

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof MiscSpec) {
            MiscSpec that = (MiscSpec) obj;

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
