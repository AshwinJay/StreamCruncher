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
package streamcruncher.innards.expression;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import streamcruncher.innards.core.WhereClauseSpec;

/*
 * Author: Ashwin Jayaprakash Date: Aug 15, 2007 Time: 8:28:07 PM
 */

public class Statement implements Serializable {
    private static final long serialVersionUID = 1L;

    protected final Integer firstX;

    protected final Integer offsetX;

    protected final boolean distinct;

    protected final List<WhereClauseSpec> columnExpressions;

    // todo Add Post-Where clause specs.

    public Statement(Integer firstX, Integer offsetX, boolean distinct,
            List<WhereClauseSpec> columnExpressions) {
        this.firstX = firstX;
        this.offsetX = offsetX;
        this.distinct = distinct;
        this.columnExpressions = new ArrayList<WhereClauseSpec>(columnExpressions);
    }

    public List<WhereClauseSpec> getColumnExpressions() {
        return columnExpressions;
    }

    public boolean isDistinct() {
        return distinct;
    }

    public Integer getFirstX() {
        return firstX;
    }

    public Integer getOffsetX() {
        return offsetX;
    }
}
