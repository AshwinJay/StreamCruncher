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

import java.util.List;

import streamcruncher.api.artifact.TableFQN;
import streamcruncher.innards.core.partition.Row;
import streamcruncher.util.AppendOnlyPrimitiveLongList;

/*
 * Author: Ashwin Jayaprakash Date: Feb 23, 2007 Time: 9:00:12 PM
 */

public interface InMemPartitionDataProducer {
    public TableFQN getTargetTableFQN();

    /**
     * @return <code>null</code> if there was nothing.
     */
    public List<Row> retrieveNewRowsInBatch();

    /**
     * @return <code>null</code> if there was nothing.
     */
    public AppendOnlyPrimitiveLongList retrieveDeadRowIdsInBatch();
}
