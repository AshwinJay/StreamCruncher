package streamcruncher.test.func.generic;

import streamcruncher.test.func.OrderAndFulfillmentGenerator;

/*
 * Author: Ashwin Jayaprakash Date: Oct 15, 2006 Time: 2:16:15 PM
 */

public abstract class OrderAndFulfillmentGeneratorTest extends OrderAndFulfillmentGenerator {
    @Override
    protected String[] getResultColumnNames() {
        return new String[] { "country", "state", "city", "item_sku", "item_qty", "order_time",
                "order_id", "customer_id", "fulfillment_time", "fulfillment_id" };
    }
}
