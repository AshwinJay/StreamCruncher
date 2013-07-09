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

import streamcruncher.innards.core.partition.function.FunctionBuilder;

/*
 * Author: Ashwin Jayaprakash Date: Feb 16, 2006 Time: 9:06:55 PM
 */

/**
 * <p>
 * <b>Example 1:</b> If the Partition has 3 columns - "partition by a, b, store
 * last 5 c", then there are 2 levels and the second level terminates at the
 * {@link streamcruncher.innards.core.partition.function.FunctionBuilder}.
 * </p>
 * <p>
 * <b>Example 2:</b> If the Partition is "partition by store last 10 d", then
 * there are 0 levels. However, there will always be a "Dummy level" which will
 * terminate at a the
 * {@link streamcruncher.innards.core.partition.function.FunctionBuilder}
 * </p>
 */
public class PartitionLevel {
    protected final String columnName;

    /**
     * <code>null</code> if this is the last level.
     */
    protected final PartitionLevel nextLevel;

    /**
     * <code>null</code> if this is <b>not</b> the last level.
     */
    protected final FunctionBuilder functionBuilder;

    /**
     * @param columnName
     * @param nextLevel
     */
    public PartitionLevel(String columnName, PartitionLevel nextLevel) {
        this(columnName, nextLevel, null);
    }

    /**
     * Last Level
     * 
     * @param columnName
     * @param functionBuilder
     */
    public PartitionLevel(String columnName, FunctionBuilder functionBuilder) {
        this(columnName, null, functionBuilder);
    }

    /**
     * Dummy Level - where there are really no Levels, but just the
     * {@link FunctionBuilder}.
     * 
     * @param functionBuilder
     */
    protected PartitionLevel(FunctionBuilder functionBuilder) {
        this(null, null, functionBuilder);
    }

    /**
     * @param columnName
     * @param nextLevel
     * @param functionBuilder
     */
    protected PartitionLevel(String columnName, PartitionLevel nextLevel,
            FunctionBuilder functionBuilder) {
        this.columnName = columnName;
        this.nextLevel = nextLevel;
        this.functionBuilder = functionBuilder;
    }

    /**
     * @return Returns the columnName.
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * @return Returns the nextLevel.
     */
    public PartitionLevel getNextLevel() {
        return nextLevel;
    }

    /**
     * @return Returns the functionBuilder.
     */
    public FunctionBuilder getFunctionBuilder() {
        return functionBuilder;
    }

    // --------------------

    public boolean isDummyLevel() {
        return (columnName == null) && isLastLevel();
    }

    public boolean isLastLevel() {
        return (functionBuilder != null);
    }
}
