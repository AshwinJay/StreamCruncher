package streamcruncher.test.func.generic;

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.List;

import streamcruncher.api.artifact.RowSpec;
import streamcruncher.api.artifact.RowSpec.Info;
import streamcruncher.test.func.BatchResult;

/*
 * Author: Ashwin Jayaprakash Date: Nov 12, 2006 Time: 1:10:22 PM
 */

/**
 * <p>
 * Performance test. Unfinished, need a better way to load and measure Kernel
 * perf. DB and JVM must be tuned.
 * </p>
 * <p>
 * todo Make Latency and Capacity graph.
 * </p>
 */
public abstract class TimeWindowFPerfTest extends TimeWindowFTest {
    protected HashSet<Long> batchStartAndEndIds = new HashSet<Long>();

    @Override
    public int getMaxDataRows() {
        return 400000;
    }

    protected int getBatchSize() {
        return 25000;
    }

    @Override
    protected int getInStreamBlockSize() {
        return getBatchSize();
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
                java.lang.Double.class.getName(), java.sql.Timestamp.class.getName(),
                java.sql.Timestamp.class.getName() };
    }

    @Override
    protected void afterEvent(int counter) {
        if (counter > 0 && counter % getBatchSize() == 0 && counter < getMaxDataRows()) {
            Long batchBorderId = generatedEvents.lastKey();
            batchStartAndEndIds.add(batchBorderId);

            try {
                System.err.println("Sleeping..");
                Thread.sleep((getWindowSizeSeconds() * 1000) + 4000);
            }
            catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
        }
        else if ((counter - 1) % getBatchSize() == 0 || counter == getMaxDataRows()) {
            Long batchBorderId = generatedEvents.lastKey();
            batchStartAndEndIds.add(batchBorderId);
        }
    }

    protected String getCurrentTimestampKeyword() {
        return "current_timestamp";
    }

    @Override
    protected String getRQL() {
        return "select event_id, vehicle_id, speed, " + getCurrentTimestampKeyword()
                + ",event_time from test (partition store last " + getWindowSizeSeconds()
                + " seconds) as testStr where testStr.$row_status is new;";
    }

    @Override
    protected void verify(List<BatchResult> results) {
        super.verify(results);

        // ---------------

        long perEventTimes = 0;
        int count = 0;

        Long startInsertionTime = null;
        Long endKernelTime = null;
        long batchTimes = 0;
        int batchCount = 0;

        for (BatchResult result : results) {
            List<Object[]> rows = result.getRows();

            for (Object[] objects : rows) {
                /*
                 * "event_id", "vehicle_id", "speed", "curr_timestamp",
                 * "event_time".
                 */
                Long id = (Long) objects[0];
                Timestamp outputEvtTime = (Timestamp) objects[3];
                Timestamp inputEvtTime = (Timestamp) objects[4];

                perEventTimes = perEventTimes + (outputEvtTime.getTime() - inputEvtTime.getTime());
                count++;

                if (batchStartAndEndIds.remove(id)) {
                    if (startInsertionTime == null) {
                        startInsertionTime = inputEvtTime.getTime();
                    }
                    else {
                        endKernelTime = outputEvtTime.getTime();

                        batchTimes = batchTimes + (endKernelTime - startInsertionTime);

                        batchCount++;

                        startInsertionTime = null;
                        endKernelTime = null;
                    }
                }
            }
        }

        double avg = perEventTimes / count;
        double avgBatchTime = batchTimes / batchCount;

        System.out.println("==============================");
        System.out.println("Total events published: " + count + ". Each batch was of size:"
                + getBatchSize() + ". Avg time to publish each event (Latency in Msecs): " + avg);
        System.out.println("Avg time (in Msecs) to process " + getBatchSize()
                + " Events by the Kernel: " + avgBatchTime);
        System.out.println("==============================");
    }
}
