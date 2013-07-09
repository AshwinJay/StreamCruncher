package streamcruncher.test.func.generic;

import java.sql.Timestamp;
import java.util.Date;
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
 * This Test shows that the Query can be paused and resumed at will.
 */
public abstract class TumblingWF1PartitionPauseTest extends OrderGeneratorTest {
    protected static final int windowSize = 15;

    protected static final int minWindowSize = 5;

    protected QueryConfig config;

    @Override
    protected int getMaxDataRows() {
        return 60;
    }

    @Override
    protected void modifyQueryConfig(QueryConfig config) {
        this.config = config;
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

        return "select " + csv + " from test (partition store latest " + windowSize
                + ") as testStr where testStr.$row_status is new;";
    }

    @Override
    protected void afterEvent(int counter) {
        if (counter == 10 || counter == 31) {
            config.pauseQuery();

            System.out.println("Paused: " + new Date(System.currentTimeMillis()));
        }
        else if (counter == 30 || counter == getMaxDataRows()) {
            config.resumeQuery();

            System.out.println("Resumed: " + new Date(System.currentTimeMillis()));
        }

        if (counter % minWindowSize == 0 && counter >= 10 && counter < getMaxDataRows()) {
            try {
                System.out.println("Sleeping...");

                if (counter == 30) {
                    Thread.sleep(10000);
                }
                else {
                    Thread.sleep(5000);
                }
            }
            catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
        }
    }

    @Override
    protected void verify(List<BatchResult> results) {
        System.out.println("--Results--");

        int totalEvents = 0;

        int totalSmallBatches = 0;

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

                totalEvents++;

                System.out.println();
            }

            boolean value = rows.size() <= windowSize;
            Assert.assertTrue(value, "Each batch size should not exceed: " + windowSize);

            if (rows.size() < minWindowSize) {
                totalSmallBatches++;
            }
        }

        Assert.assertEquals(totalEvents, getMaxDataRows(),
                "Total events received don't match expected value.");

        System.out.println("Total small batches!!! - " + totalSmallBatches);
    }
}
