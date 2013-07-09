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
 * Author: Ashwin Jayaprakash Date: Feb 13, 2007 Time: 9:16:54 AM
 */

public abstract class SLAAlertCorrelationTest extends SLAAlertTest {
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
        return new String[] { java.lang.Long.class.getName(), java.sql.Timestamp.class.getName(),
                RowSpec.addInfo(java.lang.String.class.getName(), Info.SIZE, 15) };
    }

    @Override
    protected String[] getResultColumnNames() {
        return new String[] { "unfulfl_order_id", "unfulfl_order_time", "unfulfl_item_sku" };
    }

    @Override
    protected String getRQLColumnsCSV() {
        String[] columns = getResultColumnNames();
        String csv = "";
        for (int i = 0; i < columns.length; i++) {
            if (csv != "") {
                csv = csv + ", ";
            }
            csv = csv + columns[i];
        }

        return csv;
    }

    @Override
    protected String getRQL() {
        String csv = getRQLColumnsCSV();

        return "select "
                + csv
                + " from"
                + " alert order_events.order_id as unfulfl_order_id, "
                + "order_events.order_time as unfulfl_order_time, "
                + "order_events.item_sku as unfulfl_item_sku"
                + " using cust_order (partition store "
                + "last 10 seconds where item_sku "
                + "in (select item_sku from priority_item)) as order_events correlate on order_id,"
                + " fulfillment (partition store last 10 seconds) as fulfillment_events correlate on order_id"
                + " when present(order_events and not fulfillment_events);";
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
                 * "unfulfl_order_id", "unfulfl_order_time", "unfulfl_item_sku"
                 */
                String sku = (String) objects[2];
                Long unfulfilledOrderId = ((Number) objects[0]).longValue();

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
