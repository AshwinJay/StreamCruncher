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
import streamcruncher.innards.core.partition.PartitionOutputStore;
import streamcruncher.innards.core.partition.PartitionedTable;
import streamcruncher.innards.core.partition.Row;
import streamcruncher.innards.core.partition.StreamReaderPartitioner;
import streamcruncher.util.AppendOnlyPrimitiveLongList;

/*
 * Author: Ashwin Jayaprakash Date: Feb 23, 2007 Time: 8:43:53 PM
 */

public class InMemPartitioner<P extends PartitionedTable> extends StreamReaderPartitioner<P>
        implements InMemPartitionDataProducer {
    protected PartitionOutputNullStore nullStore;

    @Override
    protected PartitionOutputStore createStore() {
        nullStore = new PartitionOutputNullStore(filterInfo);
        return nullStore;
    }

    public TableFQN getTargetTableFQN() {
        return partitionedTable.getTargetTableFQN();
    }

    public List<Row> retrieveNewRowsInBatch() {
        return nullStore.retrieveNewRowsInBatch();
    }

    public AppendOnlyPrimitiveLongList retrieveDeadRowIdsInBatch() {
        return nullStore.retrieveDeadRowIdsInBatch();
    }
}
