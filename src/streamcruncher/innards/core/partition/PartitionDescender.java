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

import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import streamcruncher.innards.core.QueryContext;
import streamcruncher.innards.core.partition.function.Function;
import streamcruncher.innards.core.partition.function.FunctionBuilder;
import streamcruncher.util.PerpetualResultSet;

/*
 * Author: Ashwin Jayaprakash Date: Feb 18, 2006 Time: 8:53:32 AM
 */

public class PartitionDescender {
    protected final FirstPartitionLevel firstPartitionLevel;

    protected final Set<Function> dirtyFunctions;

    protected final Set<Function> unprocessedFunctions;

    protected final PriorityQueue<CalculateTSFunctionPair> calculateTSFunctionPairs;

    protected final Object[] levelValueHolder;

    public PartitionDescender(FirstPartitionLevel firstPartitionLevel,
            Set<Function> dirtyFunctions, Set<Function> unprocessedFunctions,
            PriorityQueue<CalculateTSFunctionPair> calculateTSFunctionPairs) {
        this.firstPartitionLevel = firstPartitionLevel;
        this.dirtyFunctions = dirtyFunctions;
        this.unprocessedFunctions = unprocessedFunctions;
        this.calculateTSFunctionPairs = calculateTSFunctionPairs;

        // ---------

        int levelCount = 0;
        PartitionLevel level = firstPartitionLevel;
        while (level != null) {
            if (level.isDummyLevel() == false) {
                levelCount++;
            }

            level = level.getNextLevel();
        }
        this.levelValueHolder = new Object[levelCount];
    }

    /**
     * Constructs the whole Tree starting from the {@link FirstPartitionLevel}.
     * 
     * @param context
     * @param resultSet
     * @return Rows in the ResultSet
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    public int descendAndAddRows(QueryContext context, PerpetualResultSet resultSet)
            throws Exception {
        int counter = 0;

        while (resultSet.next()) {
            Map map = firstPartitionLevel.getColumnValueAndData();
            PartitionLevel level = firstPartitionLevel;
            Function function = null;

            /*
             * Descend to the last Level for the current Row and get the
             * Function.
             */
            int levelNum = 0;
            for (;;) {
                Object columnValue = null;
                if (level.isDummyLevel() == false) {
                    columnValue = resultSet.getColumnValue(level.getColumnName());

                    levelValueHolder[levelNum++] = columnValue;
                }
                else {
                    // Use null as the key.
                }

                Object data = map.get(columnValue);
                boolean weakRefNeedsRefresh = (level.isLastLevel() && data != null);
                if (weakRefNeedsRefresh) {
                    WeakReference<Function> ref = (WeakReference<Function>) data;
                    /*
                     * WeakRef wrapped Object has been reclaimed, or the
                     * TableParitioner released the StrongRef, but the GC hasn't
                     * reclaimed yet.
                     */
                    if (ref.get() == null || ref.get().canDiscard()) {
                        weakRefNeedsRefresh = true;
                        ref.clear();
                    }
                    else {
                        weakRefNeedsRefresh = false;
                    }
                }

                if (data == null || weakRefNeedsRefresh) {
                    if (level.isLastLevel()) {
                        FunctionBuilder builder = level.getFunctionBuilder();
                        Function tmpFn = builder.build(levelValueHolder);
                        // First, add a Strong-Reference.
                        firstPartitionLevel.addFunction(tmpFn.getHomeFunction());

                        tmpFn.setDirtyFunctions(dirtyFunctions);
                        tmpFn.setUnprocessedDataFunctions(unprocessedFunctions);
                        tmpFn.setCalculateTSFunctionPairs(calculateTSFunctionPairs);
                        tmpFn.cycleStart(context);

                        data = new WeakReference<Function>(tmpFn.getHomeFunction());
                    }
                    else {
                        data = new HashMap();
                    }

                    map.put(columnValue, data);
                }

                // -----------------

                if (level.isLastLevel()) {
                    WeakReference<Function> ref = (WeakReference<Function>) data;
                    function = ref.get();

                    break;
                }

                level = level.getNextLevel();
                map = (HashMap) data;
            }

            function.handleRow(context, resultSet);
            counter++;
        }

        return counter;
    }

    public void attemptCleanup() {
        Map map = firstPartitionLevel.getColumnValueAndData();
        cleanMap(map);
    }

    @SuppressWarnings("unchecked")
    protected void cleanMap(Map map) {
        for (Iterator iter = map.keySet().iterator(); iter.hasNext();) {
            Object key = iter.next();
            Object value = map.get(key);

            if (value == null) {
                iter.remove();
                continue;
            }

            if (value instanceof Map) {
                Map innerMap = (Map) value;
                cleanMap(innerMap);

                if (innerMap.size() == 0) {
                    iter.remove();
                }
            }
            else {
                WeakReference<Function> ref = (WeakReference<Function>) value;
                Function function = ref.get();

                if (firstPartitionLevel.containsFunction(function) == false) {
                    iter.remove();
                }
            }
        }
    }
}