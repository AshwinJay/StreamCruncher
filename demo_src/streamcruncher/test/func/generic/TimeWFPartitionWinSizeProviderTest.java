package streamcruncher.test.func.generic;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.testng.Assert;

import streamcruncher.api.StreamCruncherException;
import streamcruncher.api.TimeWindowSizeProvider;
import streamcruncher.api.WindowSizeProvider;
import streamcruncher.api.artifact.RowSpec;
import streamcruncher.api.artifact.RowSpec.Info;
import streamcruncher.test.func.BatchResult;

/*
 * Author: Ashwin Jayaprakash Date: Feb 6, 2007 Time: 4:54:02 PM
 */

/**
 * <p>
 * This Test demonstrates the use of the {@link WindowSizeProvider} and the
 * {@link TimeWindowSizeProvider}, which can be used to modify the default size
 * of the Window for a particular sub-Window in a Partition.
 * </p>
 * <p>
 * Here, the Partition is created at the Item-SKU level and the
 * {@link CustomWindowSizeProvider} is used to alter the Time Window duration
 * and the maximum size for the "force-field" sub-Windows. "force-field" Events
 * are put into sub-Windows in the Partition where they do not expire for the
 * duration of the Test, while the other Events like "warp-drive" SKUs use the
 * default values.
 * </p>
 */
public abstract class TimeWFPartitionWinSizeProviderTest extends TimeWF1PartitionTest {
    private ArrayList<Object[]> data = initData();

    private ArrayList<Object[]> initData() {
        ArrayList<Object[]> list = new ArrayList<Object[]>();

        list.add(new Object[] { "US", "California", "San Jose", "force-field", 10, null, 1L });
        list.add(new Object[] { "India", "Karnataka", "Bangalore", "force-field", 10, null, 2L });
        list.add(new Object[] { "India", "Karnataka", "Bangalore", "force-field", 3, null, 3L });
        list.add(new Object[] { "India", "Karnataka", "Bangalore", "force-field", 11, null, 4L });
        list.add(new Object[] { "India", "Karnataka", "Bangalore", "force-field", 1, null, 5L });
        list.add(new Object[] { "US", "California", "San Jose", "force-field", 4, null, 6L });
        list.add(new Object[] { "US", "California", "San Jose", "force-field", 50, null, 7L });
        list.add(new Object[] { "India", "Karnataka", "Bangalore", "warp-drive", 20, null, 8L });
        list.add(new Object[] { "India", "Karnataka", "Bangalore", "warp-drive", 3, null, 9L });
        list.add(new Object[] { "India", "Karnataka", "Bangalore", "warp-drive", 7, null, 10L });
        list.add(new Object[] { "US", "California", "San Jose", "warp-drive", 8, null, 11L });
        list.add(new Object[] { "US", "California", "San Jose", "warp-drive", 9, null, 12L });
        list.add(new Object[] { "India", "Karnataka", "Bangalore", "warp-drive", 2, null, 13L });
        list.add(new Object[] { "US", "California", "San Jose", "warp-drive", 3, null, 14L });
        list.add(new Object[] { "India", "Karnataka", "Bangalore", "warp-drive", 1, null, 15L });
        list.add(new Object[] { "India", "Karnataka", "Bangalore", "force-field", 90, null, 16L });
        list.add(new Object[] { "US", "California", "San Jose", "force-field", 48, null, 17L });

        list.add(new Object[] { "India", "Karnataka", "Bangalore", "warp-drive", 4800, null, 19L });
        list.add(new Object[] { "US", "California", "San Jose", "warp-drive", 90000, null, 20L });
        list.add(new Object[] { "India", "Karnataka", "Bangalore", "warp-drive", 4800, null, 21L });
        list.add(new Object[] { "India", "Karnataka", "Bangalore", "warp-drive", 4800, null, 22L });

        list.add(new Object[] { "India", "Karnataka", "Bangalore", "warp-drive", 10, null, 23L });
        list.add(new Object[] { "US", "California", "San Jose", "force-field", 1200, null, 24L });

        return list;
    }

    @Override
    protected int getMaxDataRows() {
        return data.size();
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
                event[5] = new Timestamp(TimeWFPartitionWinSizeProviderTest.this
                        .getEventTimeStamp(counter));

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
    protected void beforeQueryParse() {
        super.beforeQueryParse();

        try {
            cruncher.registerProvider("TimeWindowSize/Mine", CustomWindowSizeProvider.class);
        }
        catch (StreamCruncherException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected String getRQL() {
        String csv = getRQLColumnsCSV();

        return "select " + csv + " from test (partition by item_sku store last " + windowSize
                + " seconds max " + windowSize + " 'TimeWindowSize/Mine') as testStr"
                + " where testStr.$row_status is not dead;";
    }

    @Override
    protected void verify(List<BatchResult> results) {
        HashSet<Long> orderIds = new HashSet<Long>();

        System.out.println("--Results--");
        for (BatchResult result : results) {
            System.out.println("Batch created at: " + new Timestamp(result.getTimestamp())
                    + ". Rows: " + result.getRows().size());

            List<Object[]> rows = result.getRows();

            Map<String, SortedSet<Long>> partitionedData = new HashMap<String, SortedSet<Long>>();

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
                SortedSet<Long> itemSKUWindow = partitionedData.get(objects[3]);
                if (itemSKUWindow == null) {
                    itemSKUWindow = new TreeSet<Long>();
                    partitionedData.put((String) objects[3], itemSKUWindow);
                }

                itemSKUWindow.add((Long) objects[6]);

                if (orderIds.contains(objects[6]) == false) {
                    System.out.print(" (Adding/Sliding in)");
                    orderIds.add((Long) objects[6]);
                }

                System.out.println();
            }

            int currBatchCount = 0;

            for (String itemSKU : partitionedData.keySet()) {
                SortedSet<Long> set = partitionedData.get(itemSKU);
                currBatchCount = currBatchCount + set.size();

                int expectedSize = itemSKU.equals("force-field") ? 20 : windowSize;

                Assert.assertTrue((set.size() <= expectedSize), "Set: " + set
                        + " size is greater than expected Window-size of: " + expectedSize);
            }

            Assert.assertEquals(currBatchCount, result.getRows().size(),
                    "Batch content does not match expected number of Rows.");
        }

        Assert.assertEquals(orderIds.size(), getMaxDataRows(),
                "Expected rows received don't match the number of inserted rows.");
    }

    // ------------

    public static class CustomWindowSizeProvider extends TimeWindowSizeProvider {
        /**
         * @param levelValues
         *            Will be item_sku value.
         */
        @Override
        public long provideSizeMillis(Object[] levelValues) {
            if (levelValues[0].equals("force-field")) {
                // 50 second Window.
                return 50 * 1000;
            }

            // Default for the others.
            return super.provideSizeMillis(levelValues);
        }

        /**
         * @param levelValues
         *            Will be item_sku value.
         */
        @Override
        public int provideSize(Object[] levelValues) {
            if (levelValues[0].equals("force-field")) {
                // 20 size Window.
                return 20;
            }

            // Default for the others.
            return super.provideSize(levelValues);
        }
    }
}
