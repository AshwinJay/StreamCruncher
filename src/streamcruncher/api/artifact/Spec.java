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

/*
 * Author: Ashwin Jayaprakash Date: Jul 24, 2006 Time: 11:45:00 PM
 */
public abstract class Spec implements DDLCommand {
    private static final long serialVersionUID = 1L;

    protected final String schema;

    protected final String name;

    public Spec(String schema, String name) {
        this.schema = schema;
        this.name = name;
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

    public boolean hasSchema() {
        return schema != null;
    }

    public String getFQN() {
        if (hasSchema()) {
            return schema + "." + name;
        }

        return name;
    }
}
