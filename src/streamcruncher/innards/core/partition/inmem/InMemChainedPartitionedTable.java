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
import streamcruncher.innards.core.partition.ChainedPartitionedTable;
import streamcruncher.innards.core.partition.PartitionSpec;

/*
 * Author: Ashwin Jayaprakash Date: Feb 23, 2007 Time: 9:20:23 PM
 */

public class InMemChainedPartitionedTable extends ChainedPartitionedTable {
    private static final long serialVersionUID = 1L;

    public InMemChainedPartitionedTable(TableFQN sourceTableFQN, RowSpec sourceTableRowSpec,
            TableFQN finalTableFQN, PartitionSpec partitionSpec) {
        super(sourceTableFQN, sourceTableRowSpec, finalTableFQN, partitionSpec);
    }

    protected Object writeReplace() throws ObjectStreamException {
        return new InMemChainedPartitionedTable(sourceTableFQN, sourceTableRowSpec, finalTableFQN,
                partitionSpec);
    }
}
