package streamcruncher.test.func.generic;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.testng.Assert;

import streamcruncher.api.artifact.RowSpec;
import streamcruncher.api.artifact.RowSpec.Info;
import streamcruncher.test.func.BatchResult;
import streamcruncher.test.func.OrderGenenerator;

/*
 * Author: Ashwin Jayaprakash Date: Dec 23, 2006 Time: 8:42:03 AM
 */
/**
 * <p>
 * This example along with {@link TopSellingItemsUpdateGroupTest} shows how
 * Chained Partitions and the "Highest X elements Window" can be used to keep
 * track of the top selling items in a Store.
 * <p>
 * <p>
 * Here, Orders are placed and the total quantities are stored at "Country >
 * State > City > Item SKU" level. The sum is a rolling-sum over a 30 day
 * period. Everytime the Sum changes, it is fed to the Partition down the chain,
 * which stores the top 3 products per Country/State/City, whose total
 * quantities exceed all other products.
 * </p>
 */
public abstract class TopSellingItemsTest extends OrderGenenerator {
    protected static final int windowSize = 3;

    private ArrayList<Object[]> data = initData();

    private ArrayList<Object[]> initData() {
        ArrayList<Object[]> list = new ArrayList<Object[]>();

        list.add(new Object[] { "US", "California", "San Jose", "warp-drive", 10, null, 1L });
        list.add(new Object[] { "US", "California", "San Jose", "force-field", 10, null, 2L });
        list.add(new Object[] { "US", "California", "San Jose", "warp-drive", 3, null, 3L });
        list.add(new Object[] { "US", "California", "San Jose", "warp-drive", 11, null, 4L });
        list.add(new Object[] { "US", "California", "San Jose", "warp-drive", 1, null, 5L });
        list.add(new Object[] { "US", "California", "San Jose", "warp-drive", 4, null, 6L });
        list.add(new Object[] { "US", "California", "San Jose", "force-field", 50, null, 7L });
        list.add(new Object[] { "US", "California", "San Jose", "warp-drive", 20, null, 8L });
        list.add(new Object[] { "US", "California", "San Jose", "warp-drive", 3, null, 9L });
        list.add(new Object[] { "US", "California", "San Jose", "warp-drive", 7, null, 10L });
        list.add(new Object[] { "US", "California", "San Jose", "warp-drive", 8, null, 11L });
        list.add(new Object[] { "US", "California", "San Jose", "warp-drive", 9, null, 12L });
        list.add(new Object[] { "US", "California", "San Jose", "warp-drive", 2, null, 13L });
        list.add(new Object[] { "US", "California", "San Jose", "warp-drive", 3, null, 14L });
        list.add(new Object[] { "US", "California", "San Jose", "warp-drive", 1, null, 15L });
        list.add(new Object[] { "US", "California", "San Jose", "force-field", 90, null, 16L });
        list.add(new Object[] { "US", "California", "San Jose", "force-field", 48, null, 17L });

        list.add(new Object[] { "US", "California", "San Jose", "ansible", 4800, null, 19L });
        list.add(new Object[] { "US", "California", "San Jose", "nano-mech", 90000, null, 20L });
        list.add(new Object[] { "US", "California", "San Jose", "reentry-tile", 4800, null, 21L });
        list.add(new Object[] { "US", "California", "San Jose", "niling-dsink", 4800, null, 22L });

        list.add(new Object[] { "US", "California", "San Jose", "warp-drive", 10, null, 23L });
        list.add(new Object[] { "US", "California", "San Jose", "niling-dsink", 1200, null, 24L });

        return list;
    }

    @Override
    protected int getMaxDataRows() {
        return data.size();
    }

    @Override
    protected String[] getResultColumnNames() {
        return new String[] { "country", "state", "city", "item_sku", "sum_item_qty" };
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
                java.lang.Double.class.getName() };
    }

    @Override
    protected void afterEvent(int counter) {
        try {
            Thread.sleep(1000);
        }
        catch (InterruptedException e) {
            e.printStackTrace(System.err);
        }
    }

    protected Iterator<Object[]> getData() {
        Iterator<Object[]> iter = new Iterator<Object[]>() {
            private int counter = 1;

            public boolean hasNext() {
                return counter <= getMaxDataRows();
            }

            public Object[] next() {
                /*
                 * "country", "state", "city", "item_sku", "item_qty",
                 * "order_time", "order_id"
                 */
                Object[] event = data.get(counter - 1);
                event[5] = new Timestamp(TopSellingItemsTest.this.getEventTimeStamp(counter));

                System.out.println(Arrays.asList(event));

                counter++;

                return event;
            }

            /**
             * @throws UnsupportedOperationException
             */
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };

        return iter;
    }

    @Override
    protected String getRQL() {
        String csv = getRQLColumnsCSV();

        return "select "
                + csv
                + " from test"
                + " (partition by country, state, city, item_sku store last 30 days with sum(item_qty) as sum_item_qty)"
                + " to" + " (partition by country, state, city store highest " + windowSize
                + " using sum_item_qty where $row_status is new) as test_str"
                + " where test_str.$row_status is not dead;";
    }

    @Override
    protected void verify(List<BatchResult> results) {
        System.out.println("--Results--");

        for (BatchResult result : results) {
            System.out.println("Batch created at: " + new Timestamp(result.getTimestamp())
                    + ". Rows: " + result.getRows().size());

            List<Object[]> rows = result.getRows();

            Assert.assertTrue((rows.size() <= windowSize),
                    "Window size does not match expected count");

            System.out.println(" Batch results");
            for (Object[] objects : rows) {
                System.out.print(" ");
                for (Object object : objects) {
                    System.out.print(object + " ");
                }

                System.out.println();
            }
        }

        BatchResult lastResult = results.get(results.size() - 1);
        List<Object[]> rows = lastResult.getRows();

        Assert.assertEquals(rows.size(), windowSize, "Top X items do not match expected count");

        HashMap<String, Object> values = new HashMap<String, Object>();
        for (Object[] objects : rows) {
            String itemSKU = (String) objects[3];
            Double sum = (Double) objects[4];

            Object oldVal = values.get(itemSKU);
            if (oldVal == null) {
                values.put(itemSKU, sum);
            }
            else {
                values.put(itemSKU, new Object[] { oldVal, sum });
            }
        }

        Assert.assertEquals(values.get("nano-mech"), 90000.0D,
                "Nano-Mech sum not present or not matching");

        Object dsink = values.get("niling-dsink");
        Assert.assertTrue(dsink != null, "Niling-DSinks sums not present");

        Object[] dsinks = (Object[]) dsink;
        Double d1 = (Double) dsinks[0];
        Double d2 = (Double) dsinks[1];

        Assert.assertTrue((d1 != d2), "Niling-DSink values should not be equal");
        Assert.assertTrue((d1 + d2 == 10800), "Niling-DSink sum does not match expected value");
    }
}
