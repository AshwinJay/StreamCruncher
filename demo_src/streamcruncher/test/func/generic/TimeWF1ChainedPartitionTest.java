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
 * <p>
 * This Test demonstrates the "Chained Partition" feature, where a series of
 * Partitions can be tied together like a pipeline, with the "to" clause.
 * </p>
 * <p>
 * The first Partition creates a 5 second Window with aggregates on it. This is
 * then fed to a second Partition, which picks up only the aggregated Events
 * whose values are between 10 and 30 and holds them for 5 seconds. The second
 * Partition only picks up the aggregates that replace the old aggregates.
 * </p>
 */
public abstract class TimeWF1ChainedPartitionTest extends OrderGeneratorTest {
    @Override
    protected String[] getResultColumnNames() {
        return new String[] { "country", "num_skus", "avg_qty" };
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
                java.lang.Integer.class.getName(), java.lang.Double.class.getName() };
    }

    @Override
    protected String getRQL() {
        String csv = getRQLColumnsCSV();

        String stageOne = "(partition by country store last 5 seconds"
                + " with count(item_sku) as num_skus, avg(item_qty) as avg_qty)";
        String stageTwo = "(partition store last 5 seconds where $row_status is new and (num_skus >= 10 and num_skus <= 30))";

        return "select " + csv + " from test " + stageOne + " to " + stageTwo
                + " as testStr where testStr.$row_status is new;";
    }

    @Override
    protected void verify(List<BatchResult> results) {
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
                 * "country", "num_skus", "avg_qty"
                 */
                int count = ((Number) objects[1]).intValue();
                Assert.assertTrue((count >= 10 && count <= 30), "Count exceeds limit.");

                System.out.println();
            }
        }
    }
}
