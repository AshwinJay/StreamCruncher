package streamcruncher.test.func.generic;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

import org.testng.Assert;

import streamcruncher.api.DBName;
import streamcruncher.api.StreamCruncherException;
import streamcruncher.api.aggregator.AbstractAggregator;
import streamcruncher.api.aggregator.AbstractAggregatorHelper;
import streamcruncher.api.artifact.RowSpec;
import streamcruncher.api.artifact.RowSpec.Info;
import streamcruncher.test.func.BatchResult;

/*
 * Author: Ashwin Jayaprakash Date: Oct 3, 2006 Time: 10:55:32 PM
 */
/**
 * This Test lists all the Aggregate Functions that are available. It also shows
 * how the create and plug in a custom Aggregate Function.
 */
public abstract class TimeWF2PartitionAggTest extends OrderGeneratorTest {
    @Override
    protected void beforeQueryParse() {
        try {
            cruncher.registerAggregator(new TestAggregatorHelper());
        }
        catch (StreamCruncherException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected String[] getResultColumnNames() {
        return new String[] { "country", "avg_qty", "num_skus", "geo_qty", "kurt_qty", "max_skus",
                "median_qty", "min_skus", "skew_qty", "std_qty", "sum_qty", "sumsq_qty", "var_qty",
                "test_fn_val" };
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
                java.lang.Double.class.getName(), java.lang.Integer.class.getName(),
                java.lang.Double.class.getName(), java.lang.Double.class.getName(),
                RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 15),
                java.lang.Double.class.getName(),
                RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 15),
                java.lang.Double.class.getName(), java.lang.Double.class.getName(),
                java.lang.Double.class.getName(), java.lang.Double.class.getName(),
                java.lang.Double.class.getName(),
                RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 15), };
    }

    @Override
    protected String getRQL() {
        String csv = getRQLColumnsCSV();

        return "select "
                + csv
                + " from test (partition by country store last 5 seconds"
                + " with avg(item_qty) as avg_qty, count(item_sku) as num_skus"
                + ", geomean(item_qty) as geo_qty, kurtosis(item_qty) as kurt_qty, max(item_sku) as max_skus"
                + ", median(item_qty) as median_qty, min(item_sku) as min_skus, skewness(item_qty) as skew_qty"
                + ", stddev(item_qty) as std_qty, sum(item_qty) as sum_qty"
                + ", sumsq(item_qty) as sumsq_qty, variance(item_qty) as var_qty"
                + ", custom(test_fn, order_id, J) as test_fn_val)"
                + " as testStr where testStr.$row_status is new order by country;";
    }

    @Override
    protected void verify(List<BatchResult> results) {
        HashMap<String, Integer> countryVsCount = new HashMap<String, Integer>();
        HashMap<String, Double> countryVsAvg = new HashMap<String, Double>();

        System.out.println("--Results--");
        for (BatchResult result : results) {
            System.out.println("Batch created at: " + new Timestamp(result.getTimestamp())
                    + ". Rows: " + result.getRows().size());

            List<Object[]> rows = result.getRows();

            System.out.println(" Batch results");
            for (Object[] objects : rows) {
                System.out.print(" ");

                int i = 0;
                String[] colNames = getResultColumnNames();

                for (Object object : objects) {
                    if (i > 0) {
                        System.out.print("(" + colNames[i - 1] + ") ");
                    }

                    System.out.print(object + " ");
                    i++;
                }

                /*
                 * "country", "avg_qty", "num_skus", "geo_qty", "kurt_qty",
                 * "max_skus", "median_qty", "min_skus", "skew_qty", "std_qty",
                 * "sum_qty", "sumsq_qty", "var_qty", "test_fn_val".
                 */
                String country = (String) objects[0];
                double avg = ((Number) objects[1]).doubleValue();
                int count = ((Number) objects[2]).intValue();
                double sum = ((Number) objects[10]).doubleValue();

                Integer oldCount = countryVsCount.get(country);
                if (oldCount != null && oldCount.intValue() == count) {
                    Assert.fail("For Country: " + country + ", new count: " + count
                            + " is still the same as old: " + oldCount);
                }
                countryVsCount.put(country, count);

                Double oldAvg = countryVsAvg.get(country);
                if (oldAvg != null && oldAvg.doubleValue() == avg) {
                    Assert.fail("For Country: " + country + ", new avg: " + avg
                            + " is still the same as old: " + oldAvg);
                }
                countryVsAvg.put(country, oldAvg);

                double derivedSum = sum / (1.0 * count);
                Assert.assertEquals(derivedSum, avg, "Average and Sum/Count are not the same.");

                System.out.println();
            }

            int totalCount = 0;
            for (Integer count : countryVsCount.values()) {
                totalCount = totalCount + count;
            }
            if (totalCount > getMaxDataRows()) {
                Assert.fail("Total count:" + totalCount + " cannot be greated than the max rows: "
                        + getMaxDataRows());
            }
        }
    }

    // ----------

    public static class TestAggregatorHelper extends AbstractAggregatorHelper {
        public TestAggregatorHelper() {
            super("test_fn", TestAggregator.class);
        }

        @Override
        public String getAggregatedColumnDDLFragment(DBName dbName, String[] params,
                LinkedHashMap<String, String> columnNamesAndTypes) throws Exception {
            return RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 2);
        }
    }

    /**
     * Adds up the int-values of the Order-Id's in the Window and then converts
     * the sum into a char.
     */
    public static class TestAggregator extends AbstractAggregator {
        private int columnPosition;

        private int sumOfOrderIds;

        private char suffix;

        @Override
        public void init(String[] params, LinkedHashMap<String, String> columnNamesAndTypes,
                AggregationStage aggregationStage) {
            super.init(params, columnNamesAndTypes, aggregationStage);

            for (String column : columnNamesAndTypes.keySet()) {
                if (params[0].equalsIgnoreCase(column)) {
                    break;
                }

                columnPosition++;
            }

            suffix = params[1].charAt(0);
        }

        @Override
        public String aggregate(List<Object[]> removedValues, List<Object[]> addedValues) {
            if (removedValues != null && getAggregationStage() != AggregationStage.ENTRANCE) {
                for (Object[] objects : removedValues) {
                    Object object = objects[columnPosition];

                    // Consider only non-nulls.
                    if (object != null && object instanceof Long) {
                        Long l = (Long) object;
                        sumOfOrderIds = sumOfOrderIds - l.intValue();
                    }
                }
            }

            if (addedValues != null) {
                for (Object[] objects : addedValues) {
                    Object object = objects[columnPosition];

                    // Consider only non-nulls.
                    if (object != null && object instanceof Long) {
                        Long l = (Long) object;
                        sumOfOrderIds = sumOfOrderIds + l.intValue();
                    }
                }
            }

            int i = (sumOfOrderIds % 26) + 65;
            char c = (char) i;
            return new String(new char[] { c, suffix });
        }
    }
}
