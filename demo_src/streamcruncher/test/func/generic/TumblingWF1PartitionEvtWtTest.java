package streamcruncher.test.func.generic;

import java.sql.Timestamp;
import java.util.List;

import org.testng.Assert;

import streamcruncher.api.QueryConfig;
import streamcruncher.api.artifact.RowSpec;
import streamcruncher.api.artifact.RowSpec.Info;
import streamcruncher.test.func.BatchResult;

/*
 * Author: Ashwin Jayaprakash Date: Oct 3, 2006 Time: 10:55:32 PM
 */
/**
 * This Test demonstrates the "Event Weights" feature. Each Event from the
 * Stream is assigned a weight of "0.1". So, only after 10 events or more have
 * accumulated will the Query run. The Query's schedule-time is very large
 * (almost infinite). Therefore, only a total Event weight of 1.0 or more can
 * fire the Query.
 */
public abstract class TumblingWF1PartitionEvtWtTest extends OrderGeneratorTest {
    @Override
    protected void modifyQueryConfig(QueryConfig config) {
        config.setUnprocessedEventWeight("test", 0.1f);
    }

    @Override
    protected String[] getColumnTypes() {
        /*
         * "country", "state", "city", "item_sku", "item_qty", "order_time",
         * "order_id"
         */
        return new String[] { RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 15),
                RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 15),
                RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 15),
                RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 15),
                java.lang.Integer.class.getName(), java.sql.Timestamp.class.getName(),
                java.lang.Long.class.getName() };
    }

    @Override
    protected String[] getResultColumnTypes() {
        return new String[] { RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 15),
                RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 15),
                RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 15),
                RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 15),
                java.lang.Integer.class.getName(), java.sql.Timestamp.class.getName(),
                java.lang.Long.class.getName() };
    }

    @Override
    protected String getRQL() {
        String csv = getRQLColumnsCSV();

        return "select " + csv + " from test (partition store latest 15) as testStr"
                + " where testStr.$row_status is new;";
    }

    @Override
    protected void afterEvent(int counter) {
        long sleepTime = 0;
        if (counter > 0 && counter % 5 == 0) {
            sleepTime = 5000;
        }
        if (sleepTime > 0) {
            try {
                Thread.sleep(sleepTime);
            }
            catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
        }
    }

    @Override
    protected void verify(List<BatchResult> results) {
        System.out.println("--Results--");

        Assert.assertEquals(results.size(), 3, "Batch count does not match expected count.");

        for (BatchResult result : results) {
            System.out.println("Batch created at: " + new Timestamp(result.getTimestamp())
                    + ". Rows: " + result.getRows().size());

            List<Object[]> rows = result.getRows();

            System.out.println(" Batch results");
            for (Object[] objects : rows) {
                System.out.print(" ");
                for (Object object : objects) {
                    System.out.print(object + " ");
                }

                /*
                 * "country", "state", "city", "item_sku", "item_qty",
                 * "order_time", "order_id"
                 */

                Assert.assertEquals(rows.size(), 10, "Each batch size should've been the same.");

                System.out.println();
            }
        }
    }
}
