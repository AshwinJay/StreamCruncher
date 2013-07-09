package streamcruncher.test.func.oracle;

import java.util.List;

import org.testng.annotations.AfterGroups;
import org.testng.annotations.BeforeGroups;
import org.testng.annotations.Test;

import streamcruncher.test.TestGroupNames;
import streamcruncher.test.func.BatchResult;
import streamcruncher.test.func.generic.ClusterHealthTest;

/*
 * Author: Ashwin Jayaprakash Date: Mar 15, 2007 Time: 7:09:29 PM
 */

public class OracleClusterHealthTest extends ClusterHealthTest {
    @Override
    @BeforeGroups(dependsOnGroups = { TestGroupNames.SC_INIT_REQUIRED }, value = { TestGroupNames.SC_TEST_ORACLE }, groups = { TestGroupNames.SC_TEST_ORACLE })
    public void init() throws Exception {
        super.init();
    }

    @Test(dependsOnGroups = { TestGroupNames.SC_INIT_REQUIRED }, groups = { TestGroupNames.SC_TEST_ORACLE })
    protected void performTest() throws Exception {
        List<BatchResult> results = test();
    }

    @Override
    @AfterGroups(dependsOnGroups = { TestGroupNames.SC_INIT_REQUIRED }, value = { TestGroupNames.SC_TEST_ORACLE }, groups = { TestGroupNames.SC_TEST_ORACLE })
    public void discard() {
        super.discard();
    }
}
