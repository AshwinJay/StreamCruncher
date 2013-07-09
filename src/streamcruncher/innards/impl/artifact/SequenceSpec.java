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
package streamcruncher.innards.impl.artifact;

import streamcruncher.api.artifact.MiscSpec;

/*
 * Author: Ashwin Jayaprakash Date: Jul 25, 2006 Time: 10:56:52 PM
 */

public class SequenceSpec extends MiscSpec {
    public SequenceSpec(String schema, String name, String tableFQN, Object[] parameters) {
        super(schema, name, tableFQN, parameters);
    }

    public String constructCreateCommand() {
        return "create sequence " + getFQN();
    }

    public String constructDropCommand() {
        return "drop sequence " + getFQN();
    }
}
