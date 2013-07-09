package streamcruncher.test.func.generic;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.List;

import org.testng.Assert;

import streamcruncher.api.artifact.RowSpec;
import streamcruncher.api.artifact.RowSpec.Info;
import streamcruncher.test.func.BatchResult;

/*
 * Author: Ashwin Jayaprakash Date: Oct 24, 2006 Time: 2:46:12 PM
 */
/**
 * <p>
 * This example demonstrates the use of a Co-related Sub-Query and Filters in a
 * Partition (a rather complex example). The idea is to retrieve the
 * Priority-item Orders that are not Fulfilled within 10 seconds of placing one.
 * </p>
 * <p>
 * Only half of the Priority-items are fulfilled by the data feeder on purpose.
 * This example shows how easy it is to combine a Kernel artifact with a normal
 * Database Table. The Database Table holds the configirable data, which gets
 * updated in the background, outside the boundary of the Kernel.
 * </p>
 */
public abstract class SLAAlertTest extends OrderAndFulfillmentGeneratorTest {
    protected static final String priorityItem = "warp-drive";

    protected final LinkedHashMap<Long, Object[]> priorityFulfillments;

    protected int partialFulfillments;

    public SLAAlertTest() {
        priorityFulfillments = new LinkedHashMap<Long, Object[]>();
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
                java.lang.Long.class.getName(), java.lang.Long.class.getName() };
    }

    @Override
    protected void beforeQueryParse() {
        try {
            Connection conn = cruncher.createConnection();
            Statement statement = conn.createStatement();
            statement
                    .execute("create table priority_item(item_sku varchar(15) not null primary key)");
            statement.execute("insert into priority_item values('" + priorityItem + "')");

            statement.close();
            conn.close();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected String[] getResultColumnNames() {
        return new String[] { "country", "state", "city", "item_sku", "item_qty", "order_time",
                "order_id", "customer_id" };
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

    protected boolean insertOrder(Object[] data) {
        /*
         * "country", "state", "city", "item_sku", "item_qty", "order_time",
         * "order_id", "customer_id"
         */
        Long orderId = (Long) data[6];
        String sku = (String) data[3];

        if (priorityItem.equals(sku)) {
            priorityFulfillments.put(orderId, null);
        }

        return true;
    }

    @Override
    protected boolean insertFulfillment(Object[] data) {
        /* "fulfillment_time", "fulfillment_id", "order_id" */
        Long orderId = (Long) data[2];
        if (priorityFulfillments.containsKey(orderId)) {
            // Fulfil only half of the Priority-item Orders.
            if (partialFulfillments < (priorityFulfillments.size() / 2)) {
                priorityFulfillments.put(orderId, data);
                partialFulfillments++;
            }
            else {
                return false;
            }
        }

        return true;
    }

    @Override
    protected String getRQL() {
        String csv = getRQLColumnsCSV();

        return "select "
                + csv
                + " from cust_order (partition store last 10 seconds where item_sku in (select item_sku from priority_item)) as order_events"
                + " where order_events.$row_status is dead"
                + " and not exists (select order_id from fulfillment (partition store last 10 seconds) as fulfillment_events"
                + " where order_events.order_id = fulfillment_events.order_id and not(fulfillment_events.$row_status is new));";
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

        // --------------

        int totalUnfulfilled = 0;
        System.out.println("--Fulfillments--");
        for (Long orderId : priorityFulfillments.keySet()) {
            Object[] data = priorityFulfillments.get(orderId);

            System.out.print(" " + orderId + ": ");
            if (data != null) {
                for (Object object : data) {
                    System.out.print(object + " ");
                }
            }
            else {
                System.out.println("<Unfulfilled>");
                totalUnfulfilled++;
            }
            System.out.println();
        }

        // --------------

        int unfulfilledReport = 0;

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
                 * "order_time", "order_id", "customer_id"
                 */
                String sku = (String) objects[3];
                Long unfulfilledOrderId = ((Number) objects[6]).longValue();

                Assert.assertEquals(sku, priorityItem,
                        "Non Priority-Item also in the list of unfulfilled Orders");

                // Unfulfilled will not have any data, just the key.
                Object[] fulfillmentData = priorityFulfillments.get(unfulfilledOrderId);
                Assert.assertTrue((fulfillmentData == null),
                        "Fulfilled Order is also in the Query result");

                unfulfilledReport++;

                System.out.println();
            }
        }

        Assert.assertEquals(unfulfilledReport, totalUnfulfilled, "Unfulfilled Orders do not match");
    }
}
