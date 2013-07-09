package streamcruncher.test.func.generic;

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
 * This example demonstrates the Store-And-Forward mechanism that can be
 * simulated using Time Windows. It maintains 30 second Windows at "Country >
 * State > City" level. It holds shipment Orders, in their corresponding Windows
 * based on their Shipment destination for 30 seconds.
 * </p>
 * <p>
 * Or, if there are more than 5 Orders placed to that destination, then the
 * first X Orders are expelled from the Window even before they reach their 30
 * second limit. This is done to accumulate Orders for a while and then ship
 * them if they are in sufficient numbers, in order to avail any Bulk-shipment
 * offers.
 * </p>
 */
public abstract class ShipmentAggregatorTest extends OrderGenenerator {
    private ArrayList<Object[]> data = initData();

    private ArrayList<Object[]> initData() {
        ArrayList<Object[]> list = new ArrayList<Object[]>();

        /*
         * Orders. Assume the "Country > State > City" are the Shipping
         * destinations for the Order.
         */
        list.add(new Object[] { "India", "Karnataka", "Bangalore", "warp-drive", 200, null, 1L });
        list.add(new Object[] { "US", "California", "San Jose", "force-field", 50, null, 2L });
        list.add(new Object[] { "India", "Karnataka", "Bangalore", "eva-suit", 600, null, 3L });
        list.add(new Object[] { "India", "Karnataka", "Bangalore", "ansible", 100, null, 4L });
        list.add(new Object[] { "India", "Karnataka", "Bangalore", "nano-mech", 8400, null, 5L });
        list.add(new Object[] { "US", "California", "San Jose", "reentry-tile", 400, null, 6L });
        list.add(new Object[] { "India", "Karnataka", "Bangalore", "niling-dsink", 10, null, 7L });

        list.add(new Object[] { "India", "Karnataka", "Bangalore", "force-field", 50, null, 8L });
        list.add(new Object[] { "US", "California", "San Jose", "nano-mech", 430, null, 9L });
        list.add(new Object[] { "US", "California", "San Jose", "reentry-tile", 360, null, 10L });
        list.add(new Object[] { "US", "California", "San Jose", "niling-dsink", 70, null, 11L });
        list.add(new Object[] { "India", "Karnataka", "Bangalore", "nano-mech", 7300, null, 12L });
        list.add(new Object[] { "India", "Karnataka", "Bangalore", "force-field", 35, null, 13L });
        list.add(new Object[] { "India", "Karnataka", "Bangalore", "ansible", 60, null, 14L });

        list.add(new Object[] { "India", "Karnataka", "Bangalore", "force-field", 25, null, 15L });
        list.add(new Object[] { "India", "Karnataka", "Bangalore", "force-field", 10, null, 19L });

        return list;
    }

    @Override
    protected int getMaxDataRows() {
        return data.size();
    }

    @Override
    protected String[] getResultColumnNames() {
        return new String[] { "country", "state", "city", "item_sku", "item_qty", "order_time",
                "order_id" };
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
        if (counter % 7 == 0) {
            try {
                Thread.sleep(2000);
            }
            catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
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
                event[5] = new Timestamp(ShipmentAggregatorTest.this.getEventTimeStamp(counter));

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

        return "select " + csv + " from test"
                + " (partition by country, state, city store last 30 seconds max 5)"
                + " as test_str where test_str.$row_status is dead;";
    }

    @Override
    protected void waitForMoreResults(List<BatchResult> resultsSoFar) throws InterruptedException {
        int total = 0;

        for (BatchResult result : resultsSoFar) {
            total = total + result.getRows().size();
        }

        if (total == data.size()) {
            throw new InterruptedException("Timed out!");
        }
    }

    @Override
    protected void verify(List<BatchResult> results) {
        System.out.println("--Results--");

        long[] firstFewDispatchedOrders = { 1, 3, 4, 5, 7, 8, 2, 6, 9 };

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

                long orderId = (Long) objects[6];

                if (counter < firstFewDispatchedOrders.length) {
                    Assert.assertEquals(orderId, firstFewDispatchedOrders[counter],
                            "Dispatch order does not match expected sequence");
                }

                counter++;

                System.out.println();
            }
        }

        Assert.assertEquals(counter, data.size(), "Total Events do not match expected number");
    }
}
