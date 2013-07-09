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
package streamcruncher.innards.core.partition.custom;

import java.io.Serializable;
import java.util.Map;

import streamcruncher.api.artifact.RowSpec;
import streamcruncher.innards.expression.Statement;

/*
 * Author: Ashwin Jayaprakash Date: Sep 29, 2007 Time: 11:03:19 AM
 */

public class CustomSpec implements Serializable {
    private static final long serialVersionUID = 1L;

    protected final String customStoreClassFQN;

    protected Map<String, RowSpec> sourceTblAliasAndRowSpec;

    protected Statement statement;

    protected String whereClause;

    public CustomSpec(String customStoreClassFQN) {
        this.customStoreClassFQN = customStoreClassFQN;
    }

    public String getCustomStoreClassFQN() {
        return customStoreClassFQN;
    }

    public Map<String, RowSpec> getSourceTblAliasAndRowSpec() {
        return sourceTblAliasAndRowSpec;
    }

    public void setSourceTblAliasAndRowSpec(Map<String, RowSpec> sourceTblAliasAndRowSpec) {
        this.sourceTblAliasAndRowSpec = sourceTblAliasAndRowSpec;
    }

    public Statement getStatement() {
        return statement;
    }

    public void setStatement(Statement statement) {
        this.statement = statement;
    }

    public String getWhereClause() {
        return whereClause;
    }

    public void setWhereClause(String whereClause) {
        this.whereClause = whereClause;
    }
}
