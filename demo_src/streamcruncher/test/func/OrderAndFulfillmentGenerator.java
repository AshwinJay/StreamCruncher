/*
 * StreamCruncher:  Copyright (c) 2006-2008, Ashwin Jayaprakash. All Rights Reserved.
 * Contact:         ashwin {dot} jayaprakash {at} gmail {dot} com
 * Web:             http://www.StreamCruncher.com
 * 
 * This file is part of StreamCruncher.
 * 
 *     StreamCruncher is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 * 
 *     StreamCruncher is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 * 
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with StreamCruncher. If not, see <http://www.gnu.org/licenses/>.
 */
package streamcruncher.test.func;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import streamcruncher.api.InputSession;
import streamcruncher.api.OutputSession;
import streamcruncher.api.ParsedQuery;
import streamcruncher.api.ParserParameters;
import streamcruncher.api.QueryConfig;
import streamcruncher.api.StreamCruncher;
import streamcruncher.api.StreamCruncherException;
import streamcruncher.api.QueryConfig.QuerySchedulePolicy;
import streamcruncher.api.artifact.RowSpec;

/*
 * Author: Ashwin Jayaprakash Date: Oct 15, 2006 Time: 11:35:36 AM
 */
/**
 * <p>
 * Parent Class for all Order-Fulfillment Co-relation Test Cases. Creates 2
 * Streams - one for Orders and another for Fulfillments. It also produces some
 * test data using random values.
 * </p>
 * <p>
 * <b>Note:</b> Some of the "Time based Window" Tests may fail occassionally,
 * because the expiry times of Events and hence the expected results are
 * dependent on the Hardware's performance. A slight delay in scheduling the
 * Query might cause the Test case to fail. In such cases, try running the Test
 * again or correct the expected behaviour.
 * </p>
 */
public abstract class OrderAndFulfillmentGenerator {
    protected StreamCruncher cruncher;

    protected ParsedQuery parsedQuery;

    protected LinkedBlockingQueue<Long> orderIds;

    protected OrderAndFulfillmentGenerator() {
        cruncher = new StreamCruncher();
    }

    protected void init() throws Exception {
        String[] columnNames = { "country", "state", "city", "item_sku", "item_qty", "order_time",
                "order_id", "customer_id" };
        String[] columnTypes = getOrderColumnTypes();
        RowSpec rowSpec = new RowSpec(columnNames, columnTypes, 6, 5);
        cruncher.registerInStream("cust_order", rowSpec, 4096);

        // ---------

        columnNames = new String[] { "fulfillment_time", "fulfillment_id", "order_id" };
        columnTypes = getFulfillmentColumnTypes();
        rowSpec = new RowSpec(columnNames, columnTypes, 1, 0);
        cruncher.registerInStream("fulfillment", rowSpec, 4096);

        // ---------

        beforeQueryParse();
        orderIds = new LinkedBlockingQueue<Long>();

        // ---------

        String rql = getRQL();

        columnNames = getResultColumnNames();
        columnTypes = getResultColumnTypes();

        String queryName = "order_fulfillment_rql";

        ParserParameters parameters = new ParserParameters();
        parameters.setQuery(rql);
        parameters.setQueryName(queryName);
        parameters.setResultColumnTypes(columnTypes);

        parsedQuery = cruncher.parseQuery(parameters);

        QueryConfig config = parsedQuery.getQueryConfig();
        config.setQuerySchedulePolicy(new QueryConfig.QuerySchedulePolicyValue(
                QuerySchedulePolicy.ATLEAST_OR_SOONER, Integer.MAX_VALUE));

        modifyQueryConfig(config);

        cruncher.registerQuery(parsedQuery);
    }

    protected void beforeQueryParse() {
    }

    protected void modifyQueryConfig(QueryConfig config) {
    }

    protected abstract String[] getOrderColumnTypes();

    protected abstract String[] getFulfillmentColumnTypes();

    protected abstract String getRQL();

    protected abstract String[] getResultColumnNames();

    protected abstract String[] getResultColumnTypes();

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

    protected int getMaxOrderDataRows() {
        return 30;
    }

    protected int getMaxFulfillmentDataRows() {
        return 30;
    }

    protected static class Node {
        protected final String name;

        protected Node[] children;

        protected final boolean leaf;

        public Node(String name, Node[] children, boolean leaf) {
            this.name = name;
            this.children = children;
            this.leaf = leaf;
        }

        public String getName() {
            return name;
        }

        public void setChildren(Node[] children) {
            this.children = children;
        }

        public Node[] getChildren() {
            return children;
        }

        public boolean isLeaf() {
            return leaf;
        }
    }

    protected Iterator<Object[]> getOrderData() {
        Iterator<Object[]> iter = new Iterator<Object[]>() {
            private final Random random = new Random();

            private final Node[] countries = init();

            private final String[] itemSKUs = { "warp-drive", "force-field", "eva-suit" };

            private int counter = 1;

            public boolean hasNext() {
                return counter <= getMaxOrderDataRows();
            }

            private Node[] init() {
                Node country1 = new Node("US", null, false);
                Node state1 = new Node("California", null, false);
                Node city1 = new Node("San Jose", null, true);
                Node city2 = new Node("San Francisco", null, true);
                Node city3 = new Node("San Diego", null, true);
                state1.setChildren(new Node[] { city1, city2, city3 });
                Node state2 = new Node("New York", null, false);
                city1 = new Node("Brooklyn", null, true);
                city2 = new Node("New York City", null, true);
                city3 = new Node("Buffalo", null, true);
                state2.setChildren(new Node[] { city1, city2, city3 });
                Node state3 = new Node("Florida", null, false);
                city1 = new Node("Jacksonville", null, true);
                city2 = new Node("Miami", null, true);
                city3 = new Node("Orlando", null, true);
                state3.setChildren(new Node[] { city1, city2, city3 });
                country1.setChildren(new Node[] { state1, state2, state3 });

                Node country2 = new Node("India", null, false);
                state1 = new Node("Karnataka", null, false);
                city1 = new Node("Bangalore", null, true);
                city2 = new Node("Mangalore", null, true);
                city3 = new Node("Mysore", null, true);
                state1.setChildren(new Node[] { city1, city2, city3 });
                state2 = new Node("Maharashtra", null, false);
                city1 = new Node("Mumbai", null, true);
                city2 = new Node("Pune", null, true);
                state2.setChildren(new Node[] { city1, city2 });
                state3 = new Node("Delhi", null, false);
                city1 = new Node("Delhi", null, true);
                state3.setChildren(new Node[] { city1 });
                country2.setChildren(new Node[] { state1, state2, state3 });

                Node country3 = new Node("China", null, false);
                state1 = new Node("Shanghai", null, false);
                city1 = new Node("Shanghai", null, true);
                state1.setChildren(new Node[] { city1 });
                state2 = new Node("Tibet", null, false);
                city1 = new Node("Lhasa", null, true);
                state2.setChildren(new Node[] { city1 });
                state3 = new Node("Hong Kong", null, false);
                city1 = new Node("Hong Kong", null, true);
                state3.setChildren(new Node[] { city1 });
                country3.setChildren(new Node[] { state1, state2, state3 });

                return new Node[] { country1, country2, country3 };
            }

            public Object[] next() {
                /*
                 * "country", "state", "city", "item_sku", "item_qty",
                 * "order_time", "order_id", "customer_id"
                 */
                Object[] data = new Object[8];

                Node node = countries[random.nextInt(countries.length)];
                data[0] = node.getName();
                Node[] children = node.getChildren();
                node = children[random.nextInt(children.length)];
                data[1] = node.getName();
                children = node.getChildren();
                node = children[random.nextInt(children.length)];
                data[2] = node.getName();

                data[3] = itemSKUs[random.nextInt(itemSKUs.length)];

                data[4] = random.nextInt(50) + 1;

                data[5] = new Timestamp(System.currentTimeMillis());
                data[6] = new Long(counter);

                data[7] = new Long((counter % 5));

                System.out.println("Order: " + Arrays.asList(data));

                counter++;

                return data;
            }

            /**
             * @throws UnsupportedOperationException
             */
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };

        return iter;
    }

    protected Iterator<Object[]> getFulfillmentData() {
        Iterator<Object[]> iter = new Iterator<Object[]>() {
            private int counter = 1;

            public boolean hasNext() {
                return counter <= getMaxFulfillmentDataRows();
            }

            public Object[] next() {
                Long orderId;
                try {
                    orderId = orderIds.take();
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                /*
                 * "fulfillment_time", "fulfillment_id", "order_id"
                 */
                Object[] data = new Object[3];

                data[0] = new Timestamp(System.currentTimeMillis());
                data[1] = new Long(counter);
                data[2] = orderId;

                System.out.println("Fulfillment: " + Arrays.asList(data));

                counter++;

                return data;
            }

            /**
             * @throws UnsupportedOperationException
             */
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };

        return iter;
    }

    protected List<BatchResult> test() throws Exception {
        final InputSession inputSession1 = cruncher.createInputSession("cust_order");
        inputSession1.start();
        Thread orderThread = new Thread() {
            @Override
            public void run() {
                try {
                    generateOrderEvents(inputSession1);
                }
                catch (Exception e) {
                    e.printStackTrace(System.err);
                }
            }
        };

        final InputSession inputSession2 = cruncher.createInputSession("fulfillment");
        inputSession2.start();
        Thread fulfillmentThread = new Thread() {
            @Override
            public void run() {
                try {
                    generateFulfillmentEvents(inputSession2);
                }
                catch (Exception e) {
                    e.printStackTrace(System.err);
                }
            }
        };

        orderThread.start();

        beforeFulfillment();

        fulfillmentThread.start();

        orderThread.join();
        fulfillmentThread.join();

        OutputSession outputSession = cruncher.createOutputSession("order_fulfillment_rql");
        List<BatchResult> results = fetchResults(outputSession);

        // --------------

        verify(results);

        return results;
    }

    protected void beforeFulfillment() {
        try {
            Thread.sleep(5000);
        }
        catch (InterruptedException e) {
            e.printStackTrace(System.err);
        }
    }

    protected void generateOrderEvents(InputSession inputSession) throws StreamCruncherException {
        Iterator<Object[]> iter = getOrderData();
        for (int counter = 1; iter.hasNext();) {
            Object[] data = iter.next();
            if (insertOrder(data) == false) {
                continue;
            }

            long orderId = ((Long) data[6]).longValue();
            inputSession.submitEvent(data);

            afterOrderEvent(counter, orderId);
            counter++;
        }

        inputSession.close();
    }

    protected boolean insertOrder(Object[] data) {
        return true;
    }

    protected void afterOrderEvent(int counter, long orderId) {
        if (counter % 5 == 0) {
            try {
                Thread.sleep(5000);
            }
            catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
        }

        orderIds.offer(orderId);
    }

    protected void generateFulfillmentEvents(InputSession inputSession)
            throws StreamCruncherException {
        Iterator<Object[]> iter = getFulfillmentData();
        for (int counter = 1; iter.hasNext();) {
            Object[] data = iter.next();
            if (insertFulfillment(data) == false) {
                continue;
            }

            long fulfillmentId = ((Long) data[2]).longValue();
            inputSession.submitEvent(data);

            afterFulfillmentEvent(counter, fulfillmentId);
            counter++;
        }

        inputSession.close();
    }

    protected boolean insertFulfillment(Object[] data) {
        return true;
    }

    protected void afterFulfillmentEvent(int counter, long afterFulfillmentEvent) {
        if (counter % 10 == 0) {
            try {
                Thread.sleep(8000);
            }
            catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
        }
    }

    protected String getResultFetcherSQLOrderByClause() {
        return "";
    }

    protected List<BatchResult> fetchResults(OutputSession outputSession) throws Exception {
        outputSession.start();

        int pollTimeSec = 10;
        List<BatchResult> results = new LinkedList<BatchResult>();
        while (true) {
            try {
                List<Object[]> events = outputSession.readEvents(pollTimeSec, TimeUnit.SECONDS);
                if (events.size() == 0) {
                    if (results.size() > 0) {
                        throw new InterruptedException("Timed out!");
                    }

                    System.err.println("Retrying..");
                    continue;
                }

                BatchResult batchResult = new BatchResult();
                for (Object[] objects : events) {
                    batchResult.addRow(objects);
                }

                results.add(batchResult);
                System.err.println("Got result batch.");
            }
            catch (InterruptedException e) {
                if (pollTimeSec == 10) {
                    pollTimeSec = 5;
                }
                else if (pollTimeSec > 1) {
                    pollTimeSec--;
                }
                else {
                    break;
                }

                System.err.println("Retrying for more results...");
            }
        }

        outputSession.close();

        return results;
    }

    protected abstract void verify(List<BatchResult> results);

    protected void discard() {
        try {
            cruncher.unregisterQuery("order_fulfillment_rql");
        }
        catch (RuntimeException e) {
            e.printStackTrace(System.err);
        }

        try {
            cruncher.unregisterInStream("cust_order");
        }
        catch (StreamCruncherException e) {
            e.printStackTrace(System.err);
        }

        try {
            cruncher.unregisterInStream("fulfillment");
        }
        catch (StreamCruncherException e) {
            e.printStackTrace(System.err);
        }

        cruncher = null;
    }
}
