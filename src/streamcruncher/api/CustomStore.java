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

import java.util.List;
import java.util.Map;

import streamcruncher.api.artifact.RowSpec;

/*
 * Author: Ashwin Jayaprakash Date: Sep 29, 2007 Time: 1:18:31 PM
 */

public interface CustomStore {
    public void init(String queryName, Map<String, RowSpec> sourceTblAliasAndRowSpec,
            String whereClause);

    public void startBatch();

    public void added(String alias, Long id, Object[] data);

    public void removed(String alias, Long id, Object[] data);

    public List<Object[]> endBatch();

    public void destroy();
}
