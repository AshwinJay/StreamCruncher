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
import streamcruncher.api.artifact.MiscSpec;
import streamcruncher.api.artifact.RowSpec;
import streamcruncher.api.artifact.TableSpec;

/*
 * Author: Ashwin Jayaprakash Date: Nov 9, 2006 Time: 12:30:17 PM
 */

public class MySQLTableSpec extends TableSpec {
    private static final long serialVersionUID = 1L;

    public MySQLTableSpec(String schema, String name, RowSpec rowSpec, IndexSpec[] indexSpecs,
            MiscSpec[] otherClauses, boolean partitioned, boolean virtual) {
        super(schema, name, rowSpec, indexSpecs, otherClauses, partitioned, virtual);
    }

    public MySQLTableSpec(String schema, String name, RowSpec rowSpec, IndexSpec[] indexSpecs,
            MiscSpec[] otherClauses) {
        super(schema, name, rowSpec, indexSpecs, otherClauses);
    }

    @Override
    public String constructCreateCommand() {
        return super.constructCreateCommand() + " engine = MEMORY";
    }
}
