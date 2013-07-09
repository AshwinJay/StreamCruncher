package streamcruncher.test.nonfunc.h2;

import java.sql.Connection;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import streamcruncher.api.OutputSession;
import streamcruncher.api.QueryConfig;
import streamcruncher.api.ResultSetCacheConfig;
import streamcruncher.api.StreamCruncher;
import streamcruncher.test.Suite;
import streamcruncher.test.TestGroupNames;
import streamcruncher.test.func.BatchResult;
import streamcruncher.test.func.h2.H2OAConfigurableFTest;

/*
 * Author: Ashwin Jayaprakash Date: Oct 29, 2006 Time: 11:54:29 AM
 */

public class H2StartupShutdown3Test extends H2OAConfigurableFTest {
    @Override
    protected void modifyQueryConfig(QueryConfig config) {
        super.modifyQueryConfig(config);

        config.setStuckJobInterruptionTimeMsecs(4700);
        config.setResumeCheckTimeMsecs(2600);
    }

    @Test(dependsOnGroups = { TestGroupNames.SC_INIT_REQUIRED }, groups = { TestGroupNames.SC_TEST_H2 })
    protected void performTest() throws Exception {
        List<BatchResult> results = test();

        // -----------

        String queryName = "order_fulfillment_rql";
        QueryConfig config = cruncher.getQueryConfig(queryName);
        config.pauseQuery();

        for (String sql : parsedQuery.getCachedSubQueries()) {
            ResultSetCacheConfig cacheConfig = cruncher.getResultSetCacheConfig(sql);
            cacheConfig.setRefreshIntervalMsecs(8320);
        }

        // -----------

        Suite suite = new Suite();
        suite.end();
        System.err.println("Stopped...");

        Thread.sleep(5000);
        System.err.println("Restarting...");

        // -----------

        suite.start();

        // -----------

        config = cruncher.getQueryConfig(queryName);
        Assert
                .assertTrue(config.isQueryPaused(),
                        "Query state should've been Paused after restart");

        for (String sql : parsedQuery.getCachedSubQueries()) {
            ResultSetCacheConfig cacheConfig = cruncher.getResultSetCacheConfig(sql);
            Assert.assertEquals(cacheConfig.getRefreshIntervalMsecs(), 8320,
                    "Cache Config values did not get restored: " + sql);
        }

        StreamCruncher streamCruncher = new StreamCruncher();
        Connection connection = streamCruncher.createConnection();
        beforeQueryParse(connection);
        connection.close();

        config.resumeQuery();

        // -----------

        config = cruncher.getQueryConfig(queryName);
        Assert.assertEquals(config.getStuckJobInterruptionTimeMsecs(), 4700,
                "Config values did not get restored: StuckJobInterruptionTimeMsecs");
        Assert.assertEquals(config.getResumeCheckTimeMsecs(), 2600,
                "Config values did not get restored: ResumeCheckTimeMsecs");

        OutputSession outputSession = cruncher.createOutputSession(queryName);
        outputSession.start();
        outputSession.close();

        // Test again.
        results = test();
    }
}
