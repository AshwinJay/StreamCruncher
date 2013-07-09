package streamcruncher.test.func.generic;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.List;

import org.testng.Assert;

import streamcruncher.api.artifact.RowSpec;
import streamcruncher.api.artifact.RowSpec.Info;
import streamcruncher.test.func.BatchResult;

/*
 * Author: Ashwin Jayaprakash Date: Oct 24, 2006 Time: 2:46:12 PM
 */
/**
 * This Test demonstrates that some part of the behaviour of the Query can be
 * made dynamically configurable. The Query uses a Sub-query (in the form of a
 * Partition Filter) to process Orders only with high priority Items. These
 * Items are specified in a regular Database Table, obviously, into which Items
 * can be added/removed while the Query is running (ACID properties of DBs).
 */
public abstract class OAConfigurableFTest extends OrderAndFulfillmentGeneratorTest {
    protected static final String priorityItem = "warp-drive";

    @Override
    protected void beforeQueryParse() {
        beforeQueryParse(null);
    }

    protected void beforeQueryParse(Connection connection) {
        try {
            Connection conn = connection;

            if (conn == null) {
                conn = cruncher.createConnection();
            }
            Statement statement = conn.createStatement();
            statement.execute("create table priority_item(item_sku varchar(15))");
            statement.execute("insert into priority_item values('" + priorityItem + "')");
            statement.execute("insert into priority_item values('junk')");

            statement.close();

            if (connection == null) {
                conn.close();
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected String[] getOrderColumnTypes() {
        /*
         * "country", "state", "city", "item_sku", "item_qty", "order_time",
         * "order_id", "customer_id"
         */
        return new String[] { RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 15),
                RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 15),
                RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 15),
                RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 15),
                java.lang.Integer.class.getName(), java.sql.Timestamp.class.getName(),
                java.lang.Long.class.getName(), java.lang.Long.class.getName() };
    }

    @Override
    protected String[] getFulfillmentColumnTypes() {
        /* "fulfillment_time", "fulfillment_id", "order_id" */
        return new String[] { java.sql.Timestamp.class.getName(), java.lang.Long.class.getName(),
                java.lang.Long.class.getName() };
    }

    @Override
    protected String[] getResultColumnTypes() {
        return new String[] { RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 15),
                RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 15),
                RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 15),
                RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 15),
                java.lang.Integer.class.getName(), java.sql.Timestamp.class.getName(),
                java.lang.Long.class.getName(), java.lang.Long.class.getName(),
                java.sql.Timestamp.class.getName(), java.lang.Long.class.getName() };
    }

    @Override
    protected String getRQLColumnsCSV() {
        String[] columns = getResultColumnNames();
        String csv = "";
        for (int i = 0; i < columns.length; i++) {
            if (csv != "") {
                csv = csv + ", ";
            }
            csv = csv + (columns[i].equals("order_id") ? "order_events.order_id" : columns[i]);
        }

        return csv;
    }

    @Override
    protected String getRQL() {
        String csv = getRQLColumnsCSV();

        String overlyComplexPreFilter = "where 300.0 = 3000.0/10 and 6 in(6,12,36) and"
                + " item_sku in (select item_sku from priority_item where 20/10 = 2 and"
                + " item_sku in(select item_sku from priority_item where item_sku != 'junk') and 100 in (100, -100, 570))";

        return "select " + csv + " from " + "cust_order (partition store last 10 seconds "
                + overlyComplexPreFilter + ") as order_events"
                + ", fulfillment (partition store latest 15) as fulfillment_events"
                + " where fulfillment_events.$row_status is not dead"
                + " and order_events.$row_status is not dead"
                + " and fulfillment_events.order_id = order_events.order_id;";
    }

    @Override
    protected void verify(List<BatchResult> results) {
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
                 * "order_time", "order_id", "customer_id", "fulfillment_time",
                 * "fulfillment_id"
                 */
                String sku = (String) objects[3];
                Assert.assertEquals(sku, priorityItem,
                        "Non-priority items have also been picked up");

                System.out.println();
            }
        }
    }
}
