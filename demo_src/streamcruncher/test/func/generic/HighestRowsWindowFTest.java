package streamcruncher.test.func.generic;

import java.sql.Timestamp;
import java.util.List;
import java.util.TreeSet;

import org.testng.Assert;

import streamcruncher.api.artifact.RowSpec;
import streamcruncher.api.artifact.RowSpec.Info;
import streamcruncher.test.func.BatchResult;
import streamcruncher.test.func.TrafficGenerator;

/*
 * Author: Ashwin Jayaprakash Date: Sep 10, 2006 Time: 4:49:51 PM
 */

/**
 * Test for the "Highest X" in a Partition Window clause.
 */
public abstract class HighestRowsWindowFTest extends TrafficGenerator {
    protected static final int windowSize = 5;

    protected int getMaxDataRows() {
        return 30;
    }

    protected void afterEvent(int counter) {
        if (counter % 10 == 0) {
            try {
                Thread.sleep(5000);
            }
            catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
        }
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
                java.lang.Double.class.getName() };
    }

    @Override
    protected String getRQL() {
        return "select event_id, vehicle_id, speed from test (partition store highest "
                + windowSize + " using event_id) as testStr "
                + "where testStr.$row_status is not dead order by event_id;";
    }

    protected String getResultFetcherSQLOrderByClause() {
        return "order by event_id";
    }

    @Override
    protected void verify(List<BatchResult> results) {
        TreeSet<Long> previousIds = new TreeSet<Long>();

        Long lastIdInBatch = null;

        System.out.println("--Results--");
        for (BatchResult result : results) {
            System.out.println("Batch created at: " + new Timestamp(result.getTimestamp())
                    + ". Rows: " + result.getRows().size());

            List<Object[]> rows = result.getRows();

            // ------------

            System.out.println("  Batch results");
            for (Object[] objects : rows) {
                System.out.print("  ");
                for (Object object : objects) {
                    System.out.print(object + " ");
                }
                System.out.println();

                /*
                 * "event_id", "vehicle_id", "speed".
                 */

                lastIdInBatch = ((Number) objects[0]).longValue();
            }

            Assert.assertFalse(previousIds.contains(lastIdInBatch),
                    "Highest Id has already occurred in previous run, but appeared again");

            if (previousIds.isEmpty() == false) {
                Long prevCycId = previousIds.last();
                Assert.assertTrue((lastIdInBatch > prevCycId),
                        "Highest Id is not greater than previously seen Highest Ids");
            }

            previousIds.add(lastIdInBatch);
        }

        Assert.assertEquals(lastIdInBatch.longValue(), getMaxDataRows(),
                "Last Highest Id returned was not expected");
    }
}
