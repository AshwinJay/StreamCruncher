package streamcruncher.test.func.generic;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import streamcruncher.test.func.BatchResult;

/*
 * Author: Ashwin Jayaprakash Date: Dec 24, 2006 Time: 12:02:07 AM
 */
/**
 * todo Not sure how to measure the performance. Latency might be a better
 * measure. Performance test. Unfinished, need a better way to load and measure
 * Kernel perf. DB and JVM must be tuned.
 */
public abstract class ReStockAlertPerfTest extends ReStockAlertTest {
    @Override
    protected int getMaxDataRows() {
        return 1000;
    }

    protected int getBatchSize() {
        return 250;
    }

    @Override
    protected void afterEvent(int counter) {
        try {
            if (counter % getBatchSize() == 0) {
                System.err.println("Sleeping..");
                Thread.sleep(1000);
            }
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
                Object[] event = null;

                if (counter % 60 == 0) {
                    event = new Object[] { "US", "California", "San Jose", "ansible", 60,
                            new Timestamp(ReStockAlertPerfTest.this.getEventTimeStamp(counter)),
                            new Long(counter) };
                }
                else {
                    event = new Object[] { "US", "California", "San Jose", "ansible", -1,
                            new Timestamp(ReStockAlertPerfTest.this.getEventTimeStamp(counter)),
                            new Long(counter) };
                }

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

        System.out.println("--Results--");

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
                Double currentStock = (Double) objects[4];

                counter++;

                System.out.println();
            }
        }
    }
}
