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

import java.io.ObjectStreamException;

import streamcruncher.api.artifact.RowSpec;
import streamcruncher.api.artifact.TableFQN;
import streamcruncher.innards.core.partition.PartitionSpec;
import streamcruncher.innards.core.partition.PartitionedTable;

/*
 * Author: Ashwin Jayaprakash Date: Feb 23, 2007 Time: 8:29:40 PM
 */

public class InMemPartitionedTable extends PartitionedTable {
    private static final long serialVersionUID = 1L;

    public InMemPartitionedTable(String queryName, TableFQN realTableFQN, RowSpec realTableRowSpec,
            TableFQN finalTableFQN, PartitionSpec partitionSpec) {
        super(queryName, realTableFQN, realTableRowSpec, finalTableFQN, InMemPartitioner.class
                .getName(), partitionSpec);
    }

    @Override
    protected Object writeReplace() throws ObjectStreamException {
        return new InMemPartitionedTable(queryName, sourceTableFQN, sourceTableRowSpec,
                finalTableFQN, (PartitionSpec) filterSpec);
    }

}
