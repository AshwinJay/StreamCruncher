package streamcruncher.test.func.generic;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
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
 * Another example to show how normal Tables containing Master-Data can be used
 * to drive the behaviour of the Query.
 * </p>
 * <p>
 * This example receives Events containing the Orders placed by Customers which
 * deplete the Stock and Events containing the quantity of Stock that was
 * replenished. There is a normal DB Table, which store the minimum quantity of
 * each Product, that the Store must have in stock. When the stock at "Country >
 * State > City > Item SKU" level dips below the specified limit, an Alert is
 * sounded.
 * </p>
 * <p>
 * The Product quantity is stored in a "Latest Rows/Events Window". However, the
 * Events expire in the next cycle in such Windows. When the Window is empty, it
 * is destroyed and the Aggregates are also lost. To avoid this, the Windows are
 * <code>pinned</code> in the Memory and stay there even if the Window is
 * empty. But the Aggregate values will be stored safely to maintain continuity.
 * </p>
 * <p>
 * Since this Stream brings in both Consumption Events and Re-stock Events, with
 * +/- values, the sum total of all Events that enter the Window must be
 * calculated and pinned. However, in a normal Window when an Event expires, its
 * contribution to the Aggregate will be recalled and the Aggregate will be
 * recalculated. In this case we want to maintain the total and not the
 * moving-sum. So, only the Event entrances are allowed to affect the Aggregate
 * by using the <code>entrance only</code> clause.
 * </p>
 */
public abstract class ReStockAlertTest extends OrderGenenerator {
    private ArrayList<Object[]> data = initData();

    private ArrayList<Object[]> initData() {
        ArrayList<Object[]> list = new ArrayList<Object[]>();

        /*
         * Stock replenish Events. Ideally, these Events shouldn't be carrying
         * Order-Ids, because this is not a regular Customer Order. We can treat
         * them as Transaction Ids instead. Starts with twice the minimum Stock
         * levels.
         */
        list.add(new Object[] { "US", "California", "San Jose", "warp-drive", 200, null, 1L });
        list.add(new Object[] { "US", "California", "San Jose", "force-field", 50, null, 2L });
        list.add(new Object[] { "US", "California", "San Jose", "eva-suit", 600, null, 3L });
        list.add(new Object[] { "US", "California", "San Jose", "ansible", 100, null, 4L });
        list.add(new Object[] { "US", "California", "San Jose", "nano-mech", 8400, null, 5L });
        list.add(new Object[] { "US", "California", "San Jose", "reentry-tile", 400, null, 6L });
        list.add(new Object[] { "US", "California", "San Jose", "niling-dsink", 10, null, 7L });
        list.add(new Object[] { "India", "Karnataka", "Bangalore", "force-field", 50, null, 8L });

        /*
         * Customer orders. The Quantity is negative, indicating that the stock
         * depletes on Orders.
         */
        list.add(new Object[] { "US", "California", "San Jose", "warp-drive", -20, null, 9L });
        list.add(new Object[] { "US", "California", "San Jose", "force-field", -30, null, 10L });
        list.add(new Object[] { "US", "California", "San Jose", "warp-drive", -20, null, 11L });
        list.add(new Object[] { "US", "California", "San Jose", "eva-suit", -100, null, 12L });
        list.add(new Object[] { "US", "California", "San Jose", "ansible", -10, null, 13L });

        // Restock.
        list.add(new Object[] { "US", "California", "San Jose", "force-field", 40, null, 14L });

        list.add(new Object[] { "US", "California", "San Jose", "warp-drive", -20, null, 15L });
        list.add(new Object[] { "US", "California", "San Jose", "nano-mech", -8000, null, 16L });
        list.add(new Object[] { "US", "California", "San Jose", "niling-dsink", -2, null, 17L });
        list.add(new Object[] { "US", "California", "San Jose", "force-field", -50, null, 18L });
        list.add(new Object[] { "India", "Karnataka", "Bangalore", "force-field", -35, null, 19L });
        list.add(new Object[] { "US", "California", "San Jose", "ansible", -65, null, 20L });

        // Restock.
        list.add(new Object[] { "US", "California", "San Jose", "nano-mech", 9000, null, 21L });

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
                java.lang.Long.class.getName(), java.lang.Long.class.getName() };
    }

    @Override
    protected String[] getResultColumnTypes() {
        return new String[] { RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 15),
                RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 15),
                RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 15),
                RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 15),
                java.lang.Double.class.getName(), java.lang.Long.class.getName() };
    }

    @Override
    protected String[] getResultColumnNames() {
        return new String[] { "country", "state", "city", "item_sku", "sum_item_qty",
                "stock_min_level" };
    }

    protected String getIdColumnType() {
        return "bigint";
    }

    @Override
    protected void beforeQueryParse() {
        try {
            Connection conn = cruncher.createConnection();
            Statement statement = conn.createStatement();
            statement
                    .execute("create table stock_level(stock_item_sku varchar(15) not null primary key, stock_min_level "
                            + getIdColumnType() + ")");
            statement.execute("insert into stock_level values('warp-drive', 100)");
            statement.execute("insert into stock_level values('force-field', 25)");
            statement.execute("insert into stock_level values('eva-suit', 300)");
            statement.execute("insert into stock_level values('ansible', 50)");
            statement.execute("insert into stock_level values('nano-mech', 4200)");
            statement.execute("insert into stock_level values('reentry-tile', 200)");
            statement.execute("insert into stock_level values('niling-dsink', 5)");

            statement.close();
            conn.close();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
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

    @Override
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
                event[5] = new Timestamp(ReStockAlertTest.this.getEventTimeStamp(counter));

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
                + " (partition by country, state, city, item_sku store latest 500 with pinned sum(item_qty) entrance only as sum_item_qty)"
                + " as test_str, stock_level where test_str.$row_status is new and test_str.item_sku = stock_item_sku"
                + " and test_str.sum_item_qty <= stock_min_level;";
    }

    @Override
    protected void verify(List<BatchResult> results) {
        try {
            Connection conn = cruncher.createConnection();
            Statement statement = conn.createStatement();
            statement.execute("drop table stock_level");

            statement.close();
            conn.close();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }

        // --------------

        Object[][] expectedResults = { { "force-field", new Double(20) },
                { "nano-mech", new Double(400) }, { "force-field", new Double(10) },
                { "force-field", new Double(15) }, { "ansible", new Double(25) } };

        System.out.println("--Results--");

        Assert.assertEquals(results.size(), expectedResults.length,
                "Total result sets do not match expected count");
        int counter = 0;

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

                String sku = (String) objects[3];
                Double currentStock = ((Number) objects[4]).doubleValue();

                Assert.assertEquals(sku, expectedResults[counter][0],
                        "Item SKU does not match expected value");
                Assert.assertEquals(currentStock, expectedResults[counter][1],
                        "Item stock does not match expected value");

                counter++;

                System.out.println();
            }
        }
    }
}
