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
package streamcruncher.innards.core;

import java.io.Serializable;
import java.util.Map;

/*
 * Author: Ashwin Jayaprakash Date: Apr 2, 2006 Time: 7:08:58 PM
 */

/**
 * Does not have 'where' keyword in the beginning.
 */
public class WhereClauseSpec implements Serializable {
    private static final long serialVersionUID = 1L;

    protected final Map<String, Object> context;

    protected final Map<String, String> subQueries;

    protected final String whereClause;

    /**
     * @param whereClause
     * @param context
     * @param subQueries
     *            Can be <code>null</code>.
     */
    public WhereClauseSpec(String whereClause, Map<String, Object> context,
            Map<String, String> subQueries) {
        this.whereClause = whereClause;
        this.context = context;
        this.subQueries = subQueries;
    }

    public Map<String, Object> getContext() {
        return context;
    }

    /**
     * @return Can be <code>null</code>.
     */
    public Map<String, String> getSubQueries() {
        return subQueries;
    }

    /**
     * @return Returns the whereClause. Can be <code>null</code>.
     */
    public String getWhereClause() {
        return whereClause;
    }
}
