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

import java.io.Serializable;

/*
 * Author: Ashwin Jayaprakash Date: Jan 14, 2006 Time: 2:44:23 PM
 */

/**
 * A type-safe way to describe the Fully-Qualified-Name of an Event Stream
 * definition.
 */
public class TableFQN implements Serializable {
    private static final long serialVersionUID = 1L;

    protected final String schema;

    protected final String name;

    protected final String alias;

    // ------------------------

    protected final int hashCode;

    protected final String fqn;

    protected final String str;

    // ------------------------

    /**
     * Creates an FQN with a <code>null</code> Schema.
     * 
     * @param name
     * @see #TableFQN(String, String)
     */
    public TableFQN(String name) {
        this(null, name, null);
    }

    /**
     * @param schema
     * @param name
     */
    public TableFQN(String schema, String name) {
        this(schema, name, null);
    }

    /**
     * <i>Internal use.</i>
     * 
     * @param schema
     * @param name
     * @param alias
     */
    public TableFQN(String schema, String name, String alias) {
        this.schema = schema;
        this.name = name;
        this.alias = alias;

        // ------------------------

        int hash = (schema + ".").hashCode();
        hash = hash + (37 * (name + " ").hashCode());
        hash = hash + (37 * (alias + "").hashCode());
        this.hashCode = hash;

        this.str = schema + "." + name + " " + alias;
        this.fqn = this.schema != null ? (schema + "." + name) : name;
    }

    /**
     * @return Returns the alias.
     */
    public String getAlias() {
        return alias;
    }

    /**
     * @return Returns the name.
     */
    public String getName() {
        return name;
    }

    /**
     * @return Returns the schema.
     */
    public String getSchema() {
        return schema;
    }

    // ------------------------

    /**
     * Whether this table has an alias assigned.
     */
    public boolean hasAlias() {
        return alias != null;
    }

    public boolean hasSchema() {
        return schema != null;
    }

    public String getFQN() {
        return fqn;
    }

    public String getAliasOrFQN() {
        if (hasAlias()) {
            return alias;
        }

        return getFQN();
    }

    // ------------------------

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TableFQN) {
            TableFQN that = (TableFQN) obj;

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

    public boolean checkEquivalence(Object obj) {
        if (obj instanceof TableFQN) {
            TableFQN that = (TableFQN) obj;

            String thisStr = schema + "." + name;
            String thatStr = that.schema + "." + that.name;

            return thisStr.equals(thatStr);
        }

        return false;
    }

    public int equivalenceCode() {
        return (schema + "." + name).hashCode();
    }
}
