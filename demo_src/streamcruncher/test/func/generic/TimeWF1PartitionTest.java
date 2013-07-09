package streamcruncher.test.func.generic;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.testng.Assert;

import streamcruncher.api.artifact.RowSpec;
import streamcruncher.api.artifact.RowSpec.Info;
import streamcruncher.test.func.BatchResult;

/*
 * Author: Ashwin Jayaprakash Date: Sep 10, 2006 Time: 4:49:51 PM
 */
/**
 * Time based Window in a Partition. One Window is created for each item-sku.
 * These Windows exist only as long as the Events in them are alive. The Windows
 * here also have a "Max" clause where new Events can force older Events out
 * even if they have not expired, but only when the Window is full.
 */
public abstract class TimeWF1PartitionTest extends OrderGeneratorTest {
    protected static final int windowSize = 5;

    @Override
    protected String getRQL() {
        String csv = getRQLColumnsCSV();

        return "select " + csv + " from test (partition by item_sku store last " + windowSize
                + " seconds max " + windowSize + ") as testStr"
                + " where testStr.$row_status is not dead order by item_sku, order_id;";
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
    protected void afterEvent(int counter) {
        long sleepTime = 0;
        if (counter == 5) {
            sleepTime = 6500;
        }
        else if (counter == 8) {
            sleepTime = 2500;
        }
        else if (counter == 10) {
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
        HashSet<Long> orderIds = new HashSet<Long>();

        System.out.println("--Results--");
        for (BatchResult result : results) {
            System.out.println("Batch created at: " + new Timestamp(result.getTimestamp())
                    + ". Rows: " + result.getRows().size());

            List<Object[]> rows = result.getRows();

            Map<String, List<Long>> partitionedData = new HashMap<String, List<Long>>();

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
                List<Long> itemSKUWindow = partitionedData.get(objects[3]);
                if (itemSKUWindow == null) {
                    itemSKUWindow = new LinkedList<Long>();
                    partitionedData.put((String) objects[3], itemSKUWindow);
                }

                Long l = ((Number) objects[6]).longValue();
                itemSKUWindow.add(l);

                if (orderIds.contains(l) == false) {
                    System.out.print(" (Adding/Sliding in)");
                    orderIds.add(l);
                }

                System.out.println();
            }

            int currBatchCount = 0;
            for (List<Long> list : partitionedData.values()) {
                currBatchCount = currBatchCount + list.size();

                Assert.assertTrue((list.size() <= windowSize), "Set: " + list
                        + " size is greater than expected Window-size of: " + windowSize);
            }

            Assert.assertEquals(currBatchCount, result.getRows().size(),
                    "Batch content does not match expected number of Rows.");
        }

        Assert.assertEquals(orderIds.size(), getMaxDataRows(),
                "Expected rows received don't match the number of inserted rows.");
    }
}
