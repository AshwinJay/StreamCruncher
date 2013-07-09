package streamcruncher.test.func.generic;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;

import org.testng.Assert;

import streamcruncher.api.artifact.RowSpec;
import streamcruncher.api.artifact.RowSpec.Info;
import streamcruncher.test.func.BatchResult;

/*
 * Author: Ashwin Jayaprakash Date: Dec 23, 2006 Time: 8:42:03 AM
 */
/**
 * <p>
 * This is the correct version of the Query as against
 * {@link TopSellingItemsTest}. The "Highest X Window" clause provides a
 * facility to specify a group of columns which will be used to identify if a
 * previous entry exists in the Window for that group. If such an entry exists
 * in the Window, then the updated value for the group will replace the older
 * on. If the clause were no used, then the Window will treat the 2 values as 2
 * unrelated entries and might end up retain both of them in the Window.
 * </p>
 * <p>
 * Pay close attention to the different results the Query fetches with and
 * without the clause. Observe the last 4 events that get pumped in.
 * </p>
 */
public abstract class TopSellingItemsUpdateGroupTest extends TopSellingItemsTest {
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
    protected String getRQL() {
        String csv = getRQLColumnsCSV();

        return "select "
                + csv
                + " from test"
                + " (partition by country, state, city, item_sku store last 30 days with sum(item_qty) as sum_item_qty)"
                + " to"
                + " (partition by country, state, city store highest "
                + windowSize
                + " using sum_item_qty with update group country, state, city, item_sku where $row_status is new) as test_str"
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

        HashMap<String, Double> values = new HashMap<String, Double>();
        for (Object[] objects : rows) {
            String itemSKU = (String) objects[3];
            Double sum = (Double) objects[4];

            values.put(itemSKU, sum);
        }

        Assert.assertEquals(values.get("nano-mech"), 90000.0D,
                "Nano-Mech sum not present or not matching");
        Assert.assertEquals(values.get("reentry-tile"), 4800.0D,
                "Reentry-Tile sum not present or not matching");
        Assert.assertEquals(values.get("niling-dsink"), 6000.0D,
                "Niling-DSink sum not present or not matching");
    }
}
