package streamcruncher.test.func.generic;

import java.sql.Timestamp;
import java.util.List;

import org.testng.Assert;

import streamcruncher.api.artifact.RowSpec;
import streamcruncher.api.artifact.RowSpec.Info;
import streamcruncher.test.func.BatchResult;
import streamcruncher.test.func.SlidingWindowTest;

/*
 * Author: Ashwin Jayaprakash Date: Sep 10, 2006 Time: 4:49:51 PM
 */
/**
 * Test Case to show the "$row_status is dead" feature.
 */
public abstract class SlidingDeadWindowFTest extends SlidingWindowTest {
    @Override
    protected String getRQL() {
        return "select event_id, vehicle_id, speed from test (partition store last 10) as testStr"
                + " where testStr.$row_status is dead;";
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
    protected void verify(List<BatchResult> results) {
        Assert.assertEquals(results.size(), 2, "Expected 2 batch-result sets");

        int[] resultSizes = { 1, 1 };

        System.out.println("--Results--");

        int k = 0;
        for (BatchResult result : results) {
            System.out.println("Batch created at: " + new Timestamp(result.getTimestamp())
                    + ". Rows: " + result.getRows().size());

            List<Object[]> rows = result.getRows();

            Assert.assertEquals(rows.size(), resultSizes[k], "Expected " + resultSizes[k]
                    + " rows in batch-result");

            k++;

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
