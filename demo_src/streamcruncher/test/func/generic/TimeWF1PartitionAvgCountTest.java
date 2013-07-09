package streamcruncher.test.func.generic;

import java.sql.Timestamp;
import java.util.List;

import org.testng.Assert;

import streamcruncher.api.artifact.RowSpec;
import streamcruncher.api.artifact.RowSpec.Info;
import streamcruncher.test.func.BatchResult;

/*
 * Author: Ashwin Jayaprakash Date: Oct 3, 2006 Time: 10:55:32 PM
 */
/**
 * A demonstration of a Partition with Aggregates.
 */
public abstract class TimeWF1PartitionAvgCountTest extends OrderGeneratorTest {
    protected static final int windowSize = 5;

    @Override
    protected String[] getResultColumnNames() {
        return new String[] { "num_skus", "avg_qty" };
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
        return new String[] { java.lang.Integer.class.getName(), java.lang.Double.class.getName() };
    }

    @Override
    protected String getRQL() {
        String csv = getRQLColumnsCSV();

        return "select " + csv + " from test (partition store last " + windowSize
                + " seconds with count(item_sku) as num_skus, avg(item_qty) as avg_qty) as testStr"
                + " where testStr.$row_status is new;";
    }

    @Override
    protected void afterEvent(int counter) {
        if (counter > 0 && counter < getMaxDataRows() && counter % 10 == 0) {
            try {
                System.out.println("Sleeping...");
                Thread.sleep((windowSize * 1000) + 3000);
            }
            catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
        }
    }

    @Override
    protected void verify(List<BatchResult> results) {
        /*
         * Exact verification cannot be done for this test.
         */

        System.out.println("--Results--");
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
                 * "num_skus", "avg_qty"
                 */
                int count = ((Number) objects[0]).intValue();
                double avg = ((Number) objects[1]).doubleValue();

                Assert.assertTrue((count <= getMaxDataRows()),
                        "Aggregate-value was greater than expected max-value");

                System.out.println();
            }
        }
    }
}
