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
package streamcruncher.test.misc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashSet;

import org.testng.Assert;
import org.testng.annotations.Test;

import streamcruncher.test.TestGroupNames;
import streamcruncher.util.FixedKeyHashMap;

/*
 * Author: Ashwin Jayaprakash Date: Oct 30, 2006 Time: 10:14:12 PM
 */

public class FixedKeyHashMapSerTest {
    @Test(groups = { TestGroupNames.SC_TEST })
    public void test() {
        byte[] ser = null;

        try {
            HashSet<String> set = new HashSet<String>();
            set.add("a");
            set.add("b");
            FixedKeyHashMap<String, Integer> map = new FixedKeyHashMap<String, Integer>(set,
                    new Integer(0));

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream so = new ObjectOutputStream(baos);
            so.writeObject(map);
            so.flush();

            ser = baos.toByteArray();
        }
        catch (Exception e) {
            Assert.fail(e.getMessage(), e);
        }

        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(ser);
            ObjectInputStream si = new ObjectInputStream(bais);
            FixedKeyHashMap<String, Integer> map = (FixedKeyHashMap<String, Integer>) si
                    .readObject();
        }
        catch (Exception e) {
            Assert.fail(e.getMessage(), e);
        }
    }
}
