package streamcruncher.test.nonfunc.h2;

import java.util.List;

import org.testng.annotations.Test;

import streamcruncher.api.OutputSession;
import streamcruncher.test.Suite;
import streamcruncher.test.TestGroupNames;
import streamcruncher.test.func.BatchResult;
import streamcruncher.test.func.h2.H2MultiStreamEventGeneratorChainTest;

/*
 * Author: Ashwin Jayaprakash Date: Oct 29, 2006 Time: 11:54:29 AM
 */

public class H2StartupShutdown2Test extends H2MultiStreamEventGeneratorChainTest {
    @Test(dependsOnGroups = { TestGroupNames.SC_INIT_REQUIRED }, groups = { TestGroupNames.SC_TEST_H2 })
    protected void performTest() throws Exception {
        List<BatchResult> results = test();

        // -----------

        Suite suite = new Suite();
        suite.end();
        System.err.println("Stopped...");

        Thread.sleep(5000);
        System.err.println("Restarting...");
        suite.start();

        // -----------

        OutputSession outputSession = cruncher.createOutputSession("event_correlation_rql");
        outputSession.start();
        outputSession.close();

        // Test again.
        results = test();
    }
}
