package streamcruncher.test.func.pointbase;

import java.util.List;

import org.testng.annotations.AfterGroups;
import org.testng.annotations.BeforeGroups;
import org.testng.annotations.Test;

import streamcruncher.test.TestGroupNames;
import streamcruncher.test.func.BatchResult;
import streamcruncher.test.func.generic.TimeWindowFPerfTest;

/*
 * Author: Ashwin Jayaprakash Date: Nov 12, 2006 Time: 1:26:09 PM
 */

public class PointBaseTimeWindowFPerfTest extends TimeWindowFPerfTest {
    @Override
    @BeforeGroups(dependsOnGroups = { TestGroupNames.SC_INIT_REQUIRED }, value = { TestGroupNames.SC_TEST_POINTBASE }, groups = { TestGroupNames.SC_TEST_POINTBASE })
    public void init() throws Exception {
        super.init();
    }

    @Test(dependsOnGroups = { TestGroupNames.SC_INIT_REQUIRED }, groups = { TestGroupNames.SC_TEST_POINTBASE })
    protected void performTest() throws Exception {
        List<BatchResult> results = test();
    }

    @Override
    @AfterGroups(dependsOnGroups = { TestGroupNames.SC_INIT_REQUIRED }, value = { TestGroupNames.SC_TEST_POINTBASE }, groups = { TestGroupNames.SC_TEST_POINTBASE })
    public void discard() {
        super.discard();
    }
}
