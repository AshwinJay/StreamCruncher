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

/*
 * Author: Ashwin Jayaprakash Date: Jun 19, 2007 Time: 7:41:12 PM
 */

public interface Constants {
    /**
     * OGNL variable prefix to set into the Context.
     */
    public static final String VARIABLE_REFERENCE_PREFIX = "$";

    /**
     * OGNL Context variable.
     */
    public static final String VARIABLE_REWRITE_PREFIX = "#" + VARIABLE_REFERENCE_PREFIX;

    public static final String DATA_ROW_MARKER_START = "[";

    public static final String KEYWORD_DATA_ROW_NAME = "column_$";

    public static final String DATA_ROW_MARKER_END = "]";

    public static final String KEYWORD_CURRENT_TIMESTAMP = "current_timestamp";

    public static final String VARIABLE_CURRENT_TIMESTAMP = VARIABLE_REFERENCE_PREFIX
            + KEYWORD_CURRENT_TIMESTAMP;
}
