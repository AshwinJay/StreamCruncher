package streamcruncher.test.func.generic;

import java.sql.Timestamp;
import java.util.List;
import java.util.TreeSet;

import org.testng.Assert;

import streamcruncher.api.artifact.RowSpec;
import streamcruncher.api.artifact.RowSpec.Info;
import streamcruncher.test.func.BatchResult;

/*
 * Author: Ashwin Jayaprakash Date: Sep 10, 2006 Time: 4:49:51 PM
 */
/**
 * <p>
 * Performance test. Unfinished, need a better way to load and measure Kernel
 * perf. DB and JVM must be tuned.
 * </p>
 * <p>
 * Sliding Window is not actually a right candidate for Perf testing as the
 * Window is forced to move forward one row at a time.
 * </p>
 * <p>
 * todo Make Latency and Capacity graph.
 * </p>
 */
public abstract class SlidingNewWindowFPerfTest extends SlidingNewWindowFTest {
    protected String[] getResultColumnNames() {
        return new String[] { "event_id", "vehicle_id", "speed", "event_time" };
    }

    @Override
    protected String[] getColumnTypes() {
        return new String[] { java.lang.Long.class.getName(), java.sql.Timestamp.class.getName(),
                RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 12),
                java.lang.Integer.class.getName(), java.lang.Double.class.getName() };
    }

    @Override
    protected String[] getResultColumnTypes() {
        return new String[] { java.lang.Long.class.getName(),
                RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 12),
                java.lang.Double.class.getName(), java.sql.Timestamp.class.getName() };
    }

    @Override
    protected String getRQL() {
        return "select event_id, vehicle_id, speed, event_time from test (partition store last 10) as testStr"
                + " where testStr.$row_status is new;";
    }

    protected int getMaxDataRows() {
        return 1000;
    }

    protected int getBatchSize() {
        return 250;
    }

    @Override
    protected void afterEvent(int counter) {
        if (counter > 0 && counter < getMaxDataRows() && counter % getBatchSize() == 0) {
            try {
                // Wait for the Query to complete and pump the next batch.
                Thread.sleep(2000);
            }
            catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
        }
    }

    @Override
    protected void verify(List<BatchResult> results) {
        TreeSet<Long> ids = new TreeSet<Long>();

        System.out.println("--Results--");

        long diffs = 0;

        int k = 0;
        for (BatchResult result : results) {
            Timestamp batchTS = new Timestamp(result.getTimestamp());

            System.out.println("Batch created at: " + new Timestamp(result.getTimestamp())
                    + ". Rows: " + result.getRows().size());

            List<Object[]> rows = result.getRows();

            k++;

            System.out.println("  Batch results");
            for (Object[] objects : rows) {
                /*
                 * "event_id", "vehicle_id", "speed", "timestamp".
                 */
                Long id = (Long) objects[0];
                Timestamp ts = (Timestamp) objects[3];

                Assert.assertFalse(ids.contains(id), "Id must not have occurred before");

                ids.add(id);

                diffs = diffs + (batchTS.getTime() - ts.getTime());
            }
        }

        Assert.assertEquals(ids.size(), getMaxDataRows(),
                "Total events received does not match expectations");

        double avg = diffs / ids.size();

        System.out.println("==============================");
        System.out.println("Total events published: " + ids.size() + ". Each batch was of size:"
                + getBatchSize() + ". Avg time to publish each event (Latency in Msecs): " + avg);
        System.out.println("==============================");
    }
}
