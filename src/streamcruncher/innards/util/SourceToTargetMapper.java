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
package streamcruncher.innards.util;

import java.util.HashMap;

import streamcruncher.api.artifact.RowSpec;

/*
 * Author: Ashwin Jayaprakash Date: Apr 22, 2006 Time: 2:18:07 PM
 */

public class SourceToTargetMapper {
    protected RowSpec source;

    protected RowSpec target;

    public SourceToTargetMapper(RowSpec source, RowSpec target) {
        this.source = source;
        this.target = target;
    }

    /**
     * @return An array of same size as {@link #target} columns. If a mapping
     *         for that position was found, then the value in the array will be
     *         tbe position of the Source column from which the value must be
     *         copied.
     */
    public int[] map() {
        String[] sourceColumns = source.getColumnNames();
        HashMap<String, Integer> sourceColNameAndPos = new HashMap<String, Integer>();
        int i = 0;
        for (String string : sourceColumns) {
            sourceColNameAndPos.put(string, i);
            i++;
        }

        String[] targetColumns = target.getColumnNames();
        int[] sourceLocationForTargetCols = new int[targetColumns.length];
        for (int j = 0; j < targetColumns.length; j++) {
            Integer position = sourceColNameAndPos.get(targetColumns[j]);

            // See if the column could be found.
            if (position == null) {
                sourceLocationForTargetCols[j] = -1;
            }
            else {
                sourceLocationForTargetCols[j] = position;
            }
        }

        if (source.getIdColumnPosition() > -1) {
            sourceLocationForTargetCols[target.getIdColumnPosition()] = source
                    .getIdColumnPosition();
        }
        if (source.getTimestampColumnPosition() > -1) {
            sourceLocationForTargetCols[target.getTimestampColumnPosition()] = source
                    .getTimestampColumnPosition();
        }

        return sourceLocationForTargetCols;
    }
}
