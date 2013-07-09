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
package streamcruncher.innards.core.partition.function;

import java.io.Serializable;

import streamcruncher.api.artifact.RowSpec;
import streamcruncher.boot.Registry;
import streamcruncher.innards.db.DatabaseInterface;
import streamcruncher.util.AtomicX;

/*
 * Author: Ashwin Jayaprakash Date: Feb 16, 2006 Time: 8:39:11 PM
 */

public abstract class FunctionBuilder implements Serializable {
    protected final RowSpec realTableRowSpec;

    protected final RowSpec finalTableRowSpec;

    protected final AtomicX rowIdGenerator;

    /**
     * @param realTableRowSpec
     * @param finalTableRowSpec
     */
    public FunctionBuilder(RowSpec realTableRowSpec, RowSpec finalTableRowSpec) {
        this.realTableRowSpec = realTableRowSpec;
        this.finalTableRowSpec = finalTableRowSpec;

        DatabaseInterface databaseInterface = Registry.getImplFor(DatabaseInterface.class);
        this.rowIdGenerator = databaseInterface.createRowIdGenerator();
    }

    /**
     * @return Returns the finalTableRowSpec.
     */
    public RowSpec getFinalTableRowSpec() {
        return finalTableRowSpec;
    }

    /**
     * @return Returns the realTableRowSpec.
     */
    public RowSpec getRealTableRowSpec() {
        return realTableRowSpec;
    }

    public AtomicX getRowIdGenerator() {
        return rowIdGenerator;
    }

    // ----------------------

    public abstract Function build(Object[] levelValues) throws Exception;
}
