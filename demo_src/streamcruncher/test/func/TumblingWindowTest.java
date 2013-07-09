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
package streamcruncher.test.func;

import java.util.HashSet;
import java.util.List;

import org.testng.Assert;

/*
 * Author: Ashwin Jayaprakash Date: Sep 10, 2006 Time: 4:18:54 PM
 */
/**
 * Simple Test driver for "Latest Rows Window" feature tests.
 */
public abstract class TumblingWindowTest extends TrafficGenerator {
    @Override
    protected void verify(List<BatchResult> results) {
        HashSet<Long> receivedIds = new HashSet<Long>();

        System.out.println("--Results--");
        for (BatchResult result : results) {
            System.out.println("Batch created at: " + result.getTimestamp() + ". Rows: "
                    + result.getRows().size());

            List<Object[]> rows = result.getRows();

            for (Object[] objects : rows) {
                /* "event_id", "vehicle_id", "speed" */
                Long id = ((Number) objects[0]).longValue();

                Assert.assertFalse(receivedIds.contains(id), "Id: " + id
                        + " had appeared in a cycle before");

                receivedIds.add(id);
            }

            System.out.println("  Batch results");
            for (Object[] objects : rows) {
                System.out.print("  ");
                for (Object object : objects) {
                    System.out.print(object + " ");
                }
                System.out.println();
            }
        }

        Assert.assertEquals(receivedIds.size(), generatedEvents.size(),
                "Number of Submitted Events and Received Events don't match");
    }
}
