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

import java.sql.Timestamp;
import java.util.List;

import org.testng.Assert;

/*
 * Author: Ashwin Jayaprakash Date: Sep 10, 2006 Time: 4:18:54 PM
 */
/**
 * Special Test Case for the "Sliding Window" feature.
 */
public abstract class SlidingWindowTest extends TrafficGenerator {
    @Override
    protected void verify(List<BatchResult> results) {
        int counter = 0;

        System.out.println("--Results--");
        for (BatchResult result : results) {
            System.out.println("Batch created at: " + new Timestamp(result.getTimestamp())
                    + ". Rows: " + result.getRows().size());

            List<Object[]> rows = result.getRows();

            counter++;

            if (counter >= (results.size() - 2)) {
                Assert.assertEquals(rows.size(), 10, "Expected 10 rows in batch-result");
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
    }
}
