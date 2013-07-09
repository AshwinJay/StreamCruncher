package streamcruncher.test.func.h2;

import java.util.List;

import org.testng.annotations.AfterGroups;
import org.testng.annotations.BeforeGroups;
import org.testng.annotations.Test;

import streamcruncher.test.TestGroupNames;
import streamcruncher.test.func.BatchResult;
import streamcruncher.test.func.generic.ReStockAlertPerfTest;

/*
 * Author: Ashwin Jayaprakash Date: Oct 3, 2006 Time: 10:55:32 PM
 */

public class H2ReStockAlertPerfTest extends ReStockAlertPerfTest {
    @Override
    @BeforeGroups(dependsOnGroups = { TestGroupNames.SC_INIT_REQUIRED }, value = { TestGroupNames.SC_TEST_H2 }, groups = { TestGroupNames.SC_TEST_H2 })
    public void init() throws Exception {
        super.init();
    }

    @Override
    protected String[] getColumnTypes() {
        /*
         * "country", "state", "city", "item_sku", "item_qty", "order_time",
         * "order_id"
         */
        return new String[] { "varchar(15)", "varchar(15)", "varchar(15)", "varchar(15)",
                "integer", "timestamp", "bigint not null" };
    }

    @Override
    protected String[] getResultColumnTypes() {
        return new String[] { "varchar(15)", "varchar(15)", "varchar(15)", "varchar(15)", "double",
                "bigint" };
    }

    @Test(dependsOnGroups = { TestGroupNames.SC_INIT_REQUIRED }, groups = { TestGroupNames.SC_TEST_H2 })
    protected void performTest() throws Exception {
        List<BatchResult> results = test();
    }

    @Override
    @AfterGroups(dependsOnGroups = { TestGroupNames.SC_INIT_REQUIRED }, value = { TestGroupNames.SC_TEST_H2 }, groups = { TestGroupNames.SC_TEST_H2 })
    public void discard() {
        super.discard();
    }
}
