package streamcruncher.test.func.generic;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.testng.Assert;

import streamcruncher.api.artifact.RowSpec;
import streamcruncher.api.artifact.RowSpec.Info;
import streamcruncher.test.func.BatchResult;

/*
 * Author: Ashwin Jayaprakash Date: Sep 10, 2006 Time: 4:49:51 PM
 */
/**
 * Sliding Window Test with Partitions at "Country > Item-SKU" level and a
 * pre-filter on Country.
 */
public abstract class SlidingWF3PartitionWhereTest extends OrderGeneratorTest {
    protected static final int windowSize = 5;

    protected static final String country = "China";

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

        return "select " + csv + " from test (partition by country, item_sku store last "
                + windowSize + " where country != '" + country + "') as testStr"
                + " where testStr.$row_status is not dead order by country, item_sku, order_id;";
    }

    @Override
    protected void verify(List<BatchResult> results) {
        Map<String, Map<String, SortedSet<Number>>> partitionedData = new HashMap<String, Map<String, SortedSet<Number>>>();
        TreeSet<Number> deletedOrders = new TreeSet<Number>();

        int totalRowCount = 0;
        int currBatchCount = 0;

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
                 * "country", "state", "city", "item_sku", "item_qty",
                 * "order_time", "order_id"
                 */
                String c = (String) objects[0];
                Assert.assertFalse((c.equals(country)), "Filter on Country: " + country
                        + " not working");

                Map<String, SortedSet<Number>> countryData = partitionedData.get(c);
                if (countryData == null) {
                    countryData = new HashMap<String, SortedSet<Number>>();
                    partitionedData.put((String) objects[0], countryData);
                }

                SortedSet<Number> itemSKUWindow = countryData.get(objects[3]);
                if (itemSKUWindow == null) {
                    itemSKUWindow = new TreeSet<Number>();
                    countryData.put((String) objects[3], itemSKUWindow);
                }

                if (deletedOrders.contains(objects[6])) {
                    Assert.fail("Deleted Order: " + objects[6] + " still remains in the Window");
                }

                if (itemSKUWindow.contains(objects[6]) == false) {
                    System.out.print(" (Adding/Sliding in)");

                    itemSKUWindow.add((Number) objects[6]);
                    totalRowCount++;
                    currBatchCount++;

                    while (itemSKUWindow.size() > windowSize) {
                        Number removed = itemSKUWindow.first();
                        itemSKUWindow.remove(removed);
                        currBatchCount--;

                        deletedOrders.add(removed);
                        System.out.print(" (Sliding out: " + removed + ")");
                    }
                }

                System.out.println();
            }

            Assert.assertEquals(currBatchCount, result.getRows().size(),
                    "Batch content does not match expected number of Rows.");
        }
    }
}
