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
package streamcruncher.innards.core.filter;

import java.io.Serializable;

import streamcruncher.api.artifact.RowSpec;
import streamcruncher.innards.core.WhereClauseSpec;

/*
 * Author: Ashwin Jayaprakash Date: Jan 20, 2007 Time: 9:41:57 AM
 */

public abstract class FilterSpec implements Serializable {
    private static final long serialVersionUID = 1L;

    protected final WhereClauseSpec whereClauseSpec;

    protected final Serializable parameters;

    /**
     * @param whereClauseSpec
     *            <code>null</code> allowed.
     * @param parameters
     *            <code>null</code> allowed.
     */
    public FilterSpec(WhereClauseSpec whereClauseSpec, Serializable parameters) {
        this.whereClauseSpec = whereClauseSpec;
        this.parameters = parameters;
    }

    public Serializable getParameters() {
        return parameters;
    }

    /**
     * @return Returns the whereClauseSpec.
     */
    public WhereClauseSpec getWhereClauseSpec() {
        return whereClauseSpec;
    }

    // ---------

    public abstract RowSpec getSourceTableRowSpec();

    public abstract RowSpec getTargetTableRowSpec();

    public abstract String[] getColNameArrToSelectFromSrc();
}