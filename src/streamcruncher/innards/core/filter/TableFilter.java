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

import streamcruncher.innards.core.FilterInfo;
import streamcruncher.innards.core.QueryContext;

/*
 * Author: Ashwin Jayaprakash Date: Feb 1, 2006 Time: 11:04:36 PM
 */

public interface TableFilter<F extends FilterInfo> {
    /**
     * @param queryName
     * @param filterInfo
     * @throws Exception
     */
    public void init(String queryName, F filterInfo) throws Exception;

    /**
     * Will be called everytime, by the Kernel, before the Query is executed.
     * 
     * @param context
     */
    public void filter(QueryContext context) throws Exception;

    public void discard();
}
