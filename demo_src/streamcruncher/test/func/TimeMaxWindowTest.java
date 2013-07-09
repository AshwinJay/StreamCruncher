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
import java.util.HashSet;
import java.util.List;

import org.testng.Assert;

/*
 * Author: Ashwin Jayaprakash Date: Sep 10, 2006 Time: 4:18:54 PM
 */
/**
 * <p>
 * Special Test Case for "Time based Windows" with the "Max" clause.
 * </p>
 * <p>
 * <b>Note:</b> Some of the "Time based Window" Tests may fail occassionally,
 * because the expiry times of Events and hence the expected results are
 * dependent on the Hardware's performance. A slight delay in scheduling the
 * Query might cause the Test case to fail. In such cases, try running the Test
 * again or correct the expected behaviour.
 * </p>
 */
public abstract class TimeMaxWindowTest extends TimeWindowTest {
    @Override
    protected void verify(List<BatchResult> results) {
        HashSet<Long> ids = new HashSet<Long>();

        System.out.println("--Results--");
        for (BatchResult result : results) {
            System.out.println("Batch created at: " + new Timestamp(result.getTimestamp())
                    + ". Rows: " + result.getRows().size());

            List<Object[]> rows = result.getRows();

            System.out.println("  Batch results");
            for (Object[] objects : rows) {
                System.out.print("  ");
                for (Object object : objects) {
                    System.out.print(object + " ");
                }
                System.out.println();

                /*
                 * "event_id", "vehicle_id", "speed", "curr_timestamp".
                 */
                Long id = ((Number) objects[0]).longValue();

                Object[] origEvent = generatedEvents.get(id);

                /*
                 * "event_id", "event_time", "vehicle_id", "seg", "speed".
                 */
                Timestamp ts = (Timestamp) origEvent[1];
                Timestamp ts2 = (Timestamp) objects[3];

                long diff = ts2.getTime() - ts.getTime();
                long configuredTime = getWindowSizeSeconds() * 1000 +
                /*
                 * Allow delays.
                 */1500;

                Assert.assertTrue((rows.size() <= getWindowSizeSeconds()),
                        " Window size is greater than allowed Max size of: "
                                + getWindowSizeSeconds());

                Assert
                        .assertTrue((diff <= configuredTime),
                                "Event was allowed to stay in the Window for longer than required: "
                                        + diff);

                ids.add(id);
            }
        }

        Assert.assertEquals(ids.size(), getMaxDataRows(), "Some events were missed");
    }
}
