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

/*
 * Author: Ashwin Jayaprakash Date: Feb 6, 2007 Time: 11:04:08 AM
 */

/**
 * <p>
 * This Provider Class enables Window sizes in Partitions to be customized at
 * Run-time. The default value is the one provided in the Query as part of the
 * Partition definition.
 * </p>
 * <p>
 * If the Partition definition is
 * <code>.. from test (partition by country, state, city store latest 10) .. .</code>,
 * then the {@link #size} is <code>10</code>.
 * </p>
 * <p>
 * <b>Note:</b> This object is <b>not</b> Thread-safe.
 * </p>
 * 
 * @see #provideSize(Object[])
 */

public class WindowSizeProvider implements Provider {
    public static final String name = "WindowSize/Default";

    protected int size;

    /**
     * @return {@value #name}.
     */
    public static String getName() {
        return name;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    /**
     * @param levelValues
     *            The Partition values at each level for that Partition.
     *            <p>
     *            Ex: For
     *            <code>.. from test (partition by country, state, city store latest 10) ..</code>,
     *            where there are 2 Events -
     *            <code>("US", "California", "San Jose", "warp-drive", .. more properties)</code>
     *            and
     *            <code>("India", "Karnataka", "Bangalore", "force-field" .. other props)</code>
     *            the parameter will contain
     *            <code>["US", "California", "San Jose"]</code> and
     *            <code>["India", "Karnataka", "Bangalore"]</code>
     *            respectively.
     *            </p>
     *            <p>
     *            This method is invoked before creating a Window for that
     *            Partition (level values). Also, if the Partition gets
     *            destroyed - in Time based Windows and Tumbling Windows, and
     *            gets re-created later, then this method gets invoked for that
     *            Partition.
     *            </p>
     *            <p>
     *            Once the Window gets created with the specified size (default
     *            or otherwise), this method will not get invoked unless it gets
     *            re-created.
     *            </p>
     *            If the Partition is anonymous
     *            <code>(.. partition by store latest 10.. )</code>, then
     *            this parameter will be a zero length Array.
     * @return {@link #size}.
     */
    public int provideSize(Object[] levelValues) {
        return size;
    }
}
