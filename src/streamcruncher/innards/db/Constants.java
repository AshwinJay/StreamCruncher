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
package streamcruncher.innards.db;

import streamcruncher.api.InputSession;
import streamcruncher.innards.impl.AntsDatabaseInterface;

/*
 * Author: Ashwin Jayaprakash Date: Dec 31, 2005 Time: 3:47:43 PM
 */

public interface Constants {
    /**
     * The Id column number of the Partitioned Table. Zero based.
     */
    public static final int ID_COLUMN_POS = 0;

    /**
     * The Timestamp column of the Partitioned Table. Zero based.
     */
    public static final int TIMESTAMP_COLUMN_POS = 1;

    /**
     * The Version number column of the Partitioned Table. Zero based.
     */
    public static final int VERSION_COLUMN_POS = 2;

    /**
     * <p>
     * This value must never be used for any Row in the Tables. This is supposed
     * to be a non-existant Id.
     * </p>
     * <p>
     * Long's Min value will cause problems in ANTs DB. See
     * {@link AntsDatabaseInterface.AntsPreparedStatementAdapter}.
     * </p>
     * <p>
     * See {@link InputSession#submitEventId(long)}
     * </p>
     */
    public static final long DEFAULT_MONOTONIC_ID_VALUE = Integer.MIN_VALUE;
}
