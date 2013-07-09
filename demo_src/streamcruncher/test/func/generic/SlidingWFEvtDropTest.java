package streamcruncher.test.func.generic;

import java.sql.Timestamp;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.testng.Assert;

import streamcruncher.api.QueryConfig;
import streamcruncher.api.artifact.RowSpec;
import streamcruncher.api.artifact.RowSpec.Info;
import streamcruncher.test.func.BatchResult;

/*
 * Author: Ashwin Jayaprakash Date: Oct 14, 2006 Time: 2:45:44 PM
 */
/**
 * <p>
 * This Test demonstrates the Sliding Window feature where a policy has been
 * defined to prevent the Window and hence the Query from lagging behind the
 * Stream, which tends to produce bursts of Events.
 * </p>
 */
public abstract class SlidingWFEvtDropTest extends OrderGeneratorTest {
    protected static final int windowSize = 15;

    protected QueryConfig config;

    @Override
    protected int getMaxDataRows() {
        return 60;
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
    protected void modifyQueryConfig(QueryConfig config) {
        this.config = config;

        config.setAllowedPendingEvents("test", 5);
        config.pauseQuery();
        System.out.println("Paused: " + new Date(System.currentTimeMillis()));
    }

    @Override
    protected String getRQL() {
        String csv = getRQLColumnsCSV();

        return "select " + csv + " from test (partition store last " + windowSize
                + ") as testStr where testStr.$row_status is not dead order by order_id;";
    }

    @Override
    protected void afterEvent(int counter) {
        if (counter == 30 || counter == getMaxDataRows()) {
            config.resumeQuery();
            System.out.println("Resumed: " + new Date(System.currentTimeMillis()));

            try {
                System.out.println("Sleeping...");
                Thread.sleep(12000);
            }
            catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }

            if (counter < getMaxDataRows()) {
                config.pauseQuery();
                System.out.println("Paused: " + new Date(System.currentTimeMillis()));
            }
        }
    }

    protected int[] getFirstOrderIds() {
        return new int[] { 1, 11, 12, 13, 14, 15, 16, 17, 41, 42, 43, 44, 45, 46 };
    }

    @Override
    protected void verify(List<BatchResult> results) {
        System.out.println("--Results--");

        LinkedList<Long> firstOrderIds = new LinkedList<Long>();

        int[] expectedIds = getFirstOrderIds();

        Assert.assertEquals(results.size(), expectedIds.length,
                "Total batches received don't match");

        for (BatchResult result : results) {
            System.out.println("Batch created at: " + new Timestamp(result.getTimestamp())
                    + ". Rows: " + result.getRows().size());

            List<Object[]> rows = result.getRows();

            int rowCount = 0;
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
                if (rowCount == 0) {
                    Long orderId = ((Number) objects[6]).longValue();
                    firstOrderIds.add(orderId);
                }

                rowCount++;

                System.out.println();
            }

            Assert
                    .assertEquals(rows.size(), windowSize,
                            "Batch size does not match expected count");
        }

        System.out.println("First Order Ids: " + firstOrderIds);

        Assert.assertEquals(firstOrderIds.size(), expectedIds.length,
                "Order of First Ids doesn't match");
        for (int l : expectedIds) {
            Assert.assertEquals(firstOrderIds.removeFirst().longValue(), l,
                    "Order of First Ids in the Batch doesn't match");
        }
    }
}
