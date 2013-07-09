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
package streamcruncher.util.sysevent;

/*
 * Author: Ashwin Jayaprakash Date: Oct 28, 2006 Time: 10:03:09 AM
 */

public class SystemEvent {
    protected final String srcComponentName;

    protected final String header;

    protected final Object payload;

    protected final Priority priority;

    public SystemEvent(String srcComponentName, String header, Object payload, Priority priority) {
        this.srcComponentName = srcComponentName;
        this.header = header;
        this.payload = payload;
        this.priority = priority;
    }

    public String getHeader() {
        return header;
    }

    public Object getPayload() {
        return payload;
    }

    public Priority getPriority() {
        return priority;
    }

    public String getSrcComponentName() {
        return srcComponentName;
    }

    // ------------

    public static enum Priority {
        INFO, WARNING, SEVERE;
    }
}
