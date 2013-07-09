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
 * Author: Ashwin Jayaprakash Date: Feb 6, 2007 Time: 12:31:59 PM
 */
/**
 * <p>
 * Window Size Provider for Time based Windows. Instances of this class are used
 * to provide the Window size in milliseconds using
 * {@link #provideSizeMillis(Object[])}. If the Time based Window is configured
 * to use the "Max Events" constraint, then the {@link #provideSize(Object[])}
 * returns the appropriate size.
 * </p>
 * <p>
 * <b>Note:</b> This object is <b>not</b> Thread-safe.
 * </p>
 */
public class TimeWindowSizeProvider extends WindowSizeProvider {
    public static final String name2 = "TimeWindowSize/Default";

    protected long sizeMillis;

    /**
     * @return {@value #name2}.
     */
    public static String getName() {
        return name2;
    }

    public Long getSizeMillis() {
        return sizeMillis;
    }

    public void setSizeMillis(Long sizeMillis) {
        this.sizeMillis = sizeMillis;
    }

    /**
     * @param levelValues
     * @return {@link #sizeMillis}
     * @see #provideSize(Object[])
     */
    public long provideSizeMillis(Object[] levelValues) {
        return sizeMillis;
    }
}
