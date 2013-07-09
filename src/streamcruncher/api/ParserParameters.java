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
package streamcruncher.api;

/*
 * Author: Ashwin Jayaprakash Date: Jul 16, 2006 Time: 5:01:22 PM
 */

/**
 * <p>
 * This class should be used to wrap the "Running Query" along with the name and
 * Output Event Stream's Table definition, which should then be submitted to the
 * Kernel for parsing.
 * </p>
 * <p>
 * The names and Database types of the columns that will be selected/generated
 * by the Query must be listed in the correct order. In addition to these, the
 * Kernel will add an Id column to this list automatically as part of the
 * parsing exercise.
 * </p>
 */
public class ParserParameters {
    protected String query;

    protected String queryName;

    protected String[] resultColumnTypes;

    /**
     * @return the queryName
     */
    public String getQueryName() {
        return queryName;
    }

    /**
     * @param queryName
     *            the queryName to set
     */
    public void setQueryName(String queryName) {
        this.queryName = queryName;
    }

    /**
     * @return the resultColumnTypes
     */
    public String[] getResultColumnTypes() {
        return resultColumnTypes;
    }

    /**
     * @param resultColumnTypes
     *            the resultColumnTypes to set
     */
    public void setResultColumnTypes(String[] resultColumnTypes) {
        this.resultColumnTypes = resultColumnTypes;
    }

    /**
     * @return the query
     */
    public String getQuery() {
        return query;
    }

    /**
     * @param rql
     *            the query to set
     */
    public void setQuery(String rql) {
        this.query = rql;
    }
}
