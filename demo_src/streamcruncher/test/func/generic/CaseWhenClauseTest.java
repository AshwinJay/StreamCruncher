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

/*
 * Author: Ashwin Jayaprakash Date: Jan 22, 2007 Time: 12:28:12 PM
 */

/**
 * This Test demonstrates the use of the SQL "Case..When..End" clause.
 */
public abstract class CaseWhenClauseTest extends OrderGeneratorTest {
    private ArrayList<Object[]> data = initData();

    private ArrayList<Object[]> initData() {
        ArrayList<Object[]> list = new ArrayList<Object[]>();

        list.add(new Object[] { "US", "California", "San Jose", "warp-drive", 10, null, 1L });
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
        list.add(new Object[] { "India", "TamilNadu", "Madras", "warp-drive", 2, null, 13L });
        list.add(new Object[] { "US", "California", "San Jose", "warp-drive", 3, null, 14L });
        list.add(new Object[] { "India", "TamilNadu", "Madras", "warp-drive", 1, null, 15L });
        list.add(new Object[] { "India", "TamilNadu", "Madras", "force-field", 90, null, 16L });
        list.add(new Object[] { "US", "California", "San Jose", "force-field", 48, null, 17L });

        list.add(new Object[] { "India", "TamilNadu", "Madras", "ansible", 4800, null, 19L });
        list.add(new Object[] { "US", "California", "San Jose", "nano-mech", 90000, null, 20L });
        list.add(new Object[] { "India", "TamilNadu", "Madras", "reentry-tile", 4800, null, 21L });
        list.add(new Object[] { "India", "TamilNadu", "Madras", "niling-dsink", 4800, null, 22L });

        list.add(new Object[] { "India", "TamilNadu", "Madras", "warp-drive", 10, null, 23L });
        list.add(new Object[] { "US", "California", "San Jose", "niling-dsink", 1200, null, 24L });

        return list;
    }

    @Override
    protected int getMaxDataRows() {
        return data.size();
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
                event[5] = new Timestamp(CaseWhenClauseTest.this.getEventTimeStamp(counter));

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
    protected String[] getResultColumnNames() {
        return new String[] { "country", "state", "new_city", "item_sku", "item_qty", "order_time",
                "order_id" };
    }

    @Override
    protected String getRQL() {
        String columns = "country, state,"
                + " case when city = 'Bangalore' then 'Bengaluru' when city = 'Madras' then 'Chennai' else 'Somewhere' end as new_city"
                + ", item_sku, item_qty, order_time, order_id";

        return "select " + columns
                + " from test (partition by country, state, city store latest 10) as testStr"
                + " where testStr.$row_status is new;";
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
                 * "country", "state", "new_city", "item_sku", "item_qty",
                 * "order_time", "order_id", "customer_id".
                 */
                String city = (String) objects[2];
                Assert.assertTrue("Bengaluru".equals(city) || "Chennai".equals(city)
                        || "Somewhere".equals(city), "City name: " + city
                        + " did not get converted");

                System.out.println();
            }
        }
    }
}