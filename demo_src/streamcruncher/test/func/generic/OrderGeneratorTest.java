package streamcruncher.test.func.generic;

import streamcruncher.test.func.OrderGenenerator;

/*
 * Author: Ashwin Jayaprakash Date: Sep 10, 2006 Time: 4:49:51 PM
 */

public abstract class OrderGeneratorTest extends OrderGenenerator {
    @Override
    protected String[] getResultColumnNames() {
        return new String[] { "country", "state", "city", "item_sku", "item_qty", "order_time",
                "order_id" };
    }
}
