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
package streamcruncher.innards.core.partition;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import streamcruncher.innards.core.partition.function.Function;
import streamcruncher.innards.core.partition.function.FunctionBuilder;

/*
 * Author: Ashwin Jayaprakash Date: Feb 16, 2006 Time: 9:25:55 PM
 */

public class FirstPartitionLevel extends PartitionLevel {
    protected final Map columnValueAndData = new HashMap();

    /**
     * The only place where Strong references to Functions are held - as long as
     * a Function is active i.e {@link Function#canDiscard()} keeps returning
     * <code>false</code>.
     */
    protected final Set<Function> functions = new HashSet<Function>();

    // ------------------

    /**
     * @param columnName
     * @param nextLevel
     * @see PartitionLevel#PartitionLevel(String, PartitionLevel)
     */
    public FirstPartitionLevel(String columnName, PartitionLevel nextLevel) {
        super(columnName, nextLevel);
    }

    /**
     * @param columnName
     * @param functionBuilder
     */
    public FirstPartitionLevel(String columnName, FunctionBuilder functionBuilder) {
        super(columnName, functionBuilder);
    }

    /**
     * @param builder
     * @see PartitionLevel#PartitionLevel(FunctionBuilder)
     */
    public FirstPartitionLevel(FunctionBuilder builder) {
        super(builder);
    }

    /**
     * @return Returns the columnValueAndData.
     */
    public Map getColumnValueAndData() {
        return columnValueAndData;
    }

    /**
     * @return Returns the functions.
     */
    public Set<Function> getFunctions() {
        return functions;
    }

    // ------------------

    /**
     * @param function
     *            Always the {@link Function#getHomeFunction()}.
     */
    public void addFunction(Function function) {
        functions.add(function);
    }

    /**
     * @param function
     */
    public boolean containsFunction(Function function) {
        return functions.contains(function);
    }

    /**
     * @param function
     */
    public void removeFunction(Function function) {
        functions.remove(function);
    }
}
