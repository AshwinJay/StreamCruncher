package streamcruncher.test.func.generic;

import java.sql.Timestamp;
import java.util.List;

import org.testng.Assert;

import streamcruncher.api.artifact.RowSpec;
import streamcruncher.api.artifact.RowSpec.Info;
import streamcruncher.test.func.BatchResult;

/*
 * Author: Ashwin Jayaprakash Date: Oct 15, 2006 Time: 2:34:17 PM
 */
/**
 * Demonstrates a simple SQL-Join on 2 Event Streams. A Stream can also be
 * joined with a regular Table.
 */
public abstract class OAFJoinTest extends OrderAndFulfillmentGeneratorTest {
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

        return "select " + csv + " from "
                + "cust_order (partition store last 10 seconds) as order_events"
                + ", fulfillment (partition store latest 15) as fulfillment_events"
                + " where fulfillment_events.$row_status is new"
                + " and order_events.$row_status is not dead"
                + " and fulfillment_events.order_id = order_events.order_id;";
    }

    @Override
    protected void verify(List<BatchResult> results) {
        System.out.println("--Results--");

        int totalRows = 0;

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
                 * "fulfillment_id", "order_id"
                 */
                totalRows++;

                System.out.println();
            }
        }

        int missed = (getMaxOrderDataRows() - totalRows);

        System.out.println();
        System.out.println();
        System.out.println("Total not fulfilled: " + missed);
        System.out.println();
        System.out.println();

        Assert.assertTrue((missed <= 5), "More than 5 Orders were not fulfilled.");
    }
}
