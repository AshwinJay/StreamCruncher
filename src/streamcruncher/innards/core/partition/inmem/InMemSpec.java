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
package streamcruncher.innards.core.partition.inmem;

import java.io.Serializable;

import streamcruncher.api.artifact.TableSpec;
import streamcruncher.innards.core.WhereClauseSpec;
import streamcruncher.innards.expression.Statement;

/*
 * Author: Ashwin Jayaprakash Date: Aug 28, 2007 Time: 11:20:24 PM
 */

public class InMemSpec implements Serializable {
    private static final long serialVersionUID = 1L;

    protected final TableSpec partitionTableSpec;

    protected WhereClauseSpec whereClauseSpec;

    protected Statement statement;

    public InMemSpec(TableSpec partitionTableSpec) {
        this.partitionTableSpec = partitionTableSpec;
    }

    public TableSpec getPartitionTableSpec() {
        return partitionTableSpec;
    }

    public WhereClauseSpec getWhereClauseSpec() {
        return whereClauseSpec;
    }

    public void setWhereClauseSpec(WhereClauseSpec whereClauseSpec) {
        this.whereClauseSpec = whereClauseSpec;
    }

    public void setStatement(Statement statement) {
        this.statement = statement;
    }

    public Statement getStatement() {
        return statement;
    }
}
