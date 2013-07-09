package streamcruncher.test.func.firebird;

import java.util.List;

import org.testng.annotations.AfterGroups;
import org.testng.annotations.BeforeGroups;
import org.testng.annotations.Test;

import streamcruncher.test.TestGroupNames;
import streamcruncher.test.func.BatchResult;
import streamcruncher.test.func.generic.TimeWF1ChainedPartitionTest;

/*
 * Author: Ashwin Jayaprakash Date: Oct 3, 2006 Time: 10:55:32 PM
 */

public class FirebirdTimeWF1ChainedPartitionTest extends TimeWF1ChainedPartitionTest {
    @Override
    @BeforeGroups(dependsOnGroups = { TestGroupNames.SC_INIT_REQUIRED }, value = { TestGroupNames.SC_TEST_FIREBIRD }, groups = { TestGroupNames.SC_TEST_FIREBIRD })
    public void init() throws Exception {
        super.init();
    }

    @Test(dependsOnGroups = { TestGroupNames.SC_INIT_REQUIRED }, groups = { TestGroupNames.SC_TEST_FIREBIRD })
    protected void performTest() throws Exception {
        List<BatchResult> results = test();
    }

    @Override
    @AfterGroups(dependsOnGroups = { TestGroupNames.SC_INIT_REQUIRED }, value = { TestGroupNames.SC_TEST_FIREBIRD }, groups = { TestGroupNames.SC_TEST_FIREBIRD })
    public void discard() {
        super.discard();
    }
}
