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

import streamcruncher.api.QueryConfig;
import streamcruncher.api.artifact.RowSpec;
import streamcruncher.api.artifact.RowSpec.Info;
import streamcruncher.test.func.BatchResult;

/*
 * Author: Ashwin Jayaprakash Date: Jan 22, 2007 Time: 12:28:12 PM
 */

/**
 * <p>
 * This tests and demonstrates the re-architected Partition clause with
 * Pre-filters, where the Query is triggered only by the filtered Events. The
 * Event Weight specified for the Stream is the Weight of the filtered Events.
 * If the Event weight is 0.1 and there are 10 Events, out of which only 5 make
 * it through the Filter, then the new accumulated-weight of 0.5 will not
 * trigger the Query. Even though the total unfiltered weight is 1.0 (0.1 * 10).
 * </p>
 * 
 * @since 1.05. Before this, the unfiltered Event would trigger the Query and
 *        then get filtered. So, if that Event did not make it through the
 *        Filter, the Query would still've got triggered (spurious) and the
 *        Latest Rows Window (if any) would've spilled out its contents.
 */
public abstract class PreFilterTriggerTest extends OrderGeneratorTest {
    protected static final String priorityItem = "warp-drive";

    private ArrayList<Object[]> data = initData();

    private ArrayList<Object[]> initData() {
        ArrayList<Object[]> list = new ArrayList<Object[]>();

        list.add(new Object[] { "US", "California", "San Jose", "warp-drive", 10, null, 1L });
        list.add(new Object[] { "US", "California", "San Jose", "force-field", 10, null, 2L });
        list.add(new Object[] { "US", "California", "San Jose", "force-field", 3, null, 3L });
        list.add(new Object[] { "US", "California", "San Jose", "force-field", 11, null, 4L });
        list.add(new Object[] { "US", "California", "San Jose", "force-field", 1, null, 5L });
        list.add(new Object[] { "US", "California", "San Jose", "force-field", 4, null, 6L });
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
    protected void modifyQueryConfig(QueryConfig config) {
        config.setUnprocessedEventWeight("test", 0.2f);
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

    @Override
    protected void beforeQueryParse() {
        try {
            Connection conn = cruncher.createConnection();
            Statement statement = conn.createStatement();
            statement.execute("create table priority_item(item_sku varchar(15))");
            statement.execute("insert into priority_item values('" + priorityItem + "')");

            statement.close();
            conn.close();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void afterEvent(int counter) {
        if (counter == 11) {
            try {
                Thread.sleep(3000);
            }
            catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
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
                event[5] = new Timestamp(PreFilterTriggerTest.this.getEventTimeStamp(counter));

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

        return "select " + csv + " from test (partition store latest 10"
                + " where item_sku in (select item_sku from priority_item)) as testStr;";
    }

    @Override
    protected void verify(List<BatchResult> results) {
        Long[][] batches = { { 1L, 8L, 9L, 10L, 11L },
                { 1L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 23L } };

        try {
            Connection conn = cruncher.createConnection();
            Statement statement = conn.createStatement();
            statement.execute("drop table priority_item");

            statement.close();
            conn.close();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }

        System.out.println("--Results--");

        Assert.assertEquals(batches.length, results.size(), "There are more Batches than expected");

        int batch = 0;

        for (BatchResult result : results) {
            System.out.println("Batch created at: " + new Timestamp(result.getTimestamp())
                    + ". Rows: " + result.getRows().size());

            List<Object[]> rows = result.getRows();

            int row = 0;

            System.out.println(" Batch results");
            for (Object[] objects : rows) {
                System.out.print(" ");
                for (Object object : objects) {
                    System.out.print(object + " ");
                }

                /*
                 * "country", "state", "city", "item_sku", "item_qty",
                 * "order_time", "order_id", "customer_id".
                 */
                String sku = (String) objects[3];
                Assert.assertEquals(sku, priorityItem,
                        "Non-priority items have also been picked up");

                Long id = ((Number) objects[6]).longValue();
                Assert.assertEquals(id, batches[batch][row],
                        "Expected Ids not present in the Batch");
                row++;

                System.out.println();
            }

            batch++;
        }
    }
}