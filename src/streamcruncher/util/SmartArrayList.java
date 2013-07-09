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
package streamcruncher.util;

import java.util.ArrayList;
import java.util.Collection;

/*
 * Author: Ashwin Jayaprakash Date: Apr 14, 2006 Time: 12:10:00 AM
 */

/**
 * Simple wrapper on {@link java.util.ArrayList}, but resizes the internal
 * backing array when required.
 */
public class SmartArrayList<T> extends ArrayList<T> {
    private static final long serialVersionUID = 1L;

    /**
     * @param initialCapacity
     */
    public SmartArrayList(int initialCapacity) {
        super(initialCapacity);
    }

    public SmartArrayList() {
        super();
    }

    /**
     * @param c
     */
    public SmartArrayList(Collection<? extends T> c) {
        super(c);
    }

    // --------------

    /**
     * Clears the original contents and adds the collection provided. Trims the
     * list, when required.
     * 
     * @param c
     */
    public void replace(Collection<? extends T> c) {
        int origSize = size();
        clear();

        addAll(c);
        // New size is less than half of original size.
        if ((2 * size()) <= origSize) {
            trimToSize();
        }
    }
}
