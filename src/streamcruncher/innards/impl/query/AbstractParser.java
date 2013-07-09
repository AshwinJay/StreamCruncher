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
package streamcruncher.innards.impl.query;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;

import streamcruncher.api.DBName;
import streamcruncher.api.ParserParameters;
import streamcruncher.api.TimeWindowSizeProvider;
import streamcruncher.api.WindowSizeProvider;
import streamcruncher.api.aggregator.AbstractAggregatorHelper;
import streamcruncher.api.aggregator.AbstractAggregator.AggregationStage;
import streamcruncher.api.artifact.IndexSpec;
import streamcruncher.api.artifact.MiscSpec;
import streamcruncher.api.artifact.RowSpec;
import streamcruncher.api.artifact.RunningQuery;
import streamcruncher.api.artifact.TableFQN;
import streamcruncher.api.artifact.TableSpec;
import streamcruncher.boot.ConfigKeys;
import streamcruncher.boot.ProviderManager;
import streamcruncher.boot.ProviderManagerException;
import streamcruncher.boot.Registry;
import streamcruncher.innards.core.WhereClauseSpec;
import streamcruncher.innards.core.filter.FilteredTable;
import streamcruncher.innards.core.partition.ChainedPartitionedTable;
import streamcruncher.innards.core.partition.EncapsulatedPartitionedTable;
import streamcruncher.innards.core.partition.PartitionSpec;
import streamcruncher.innards.core.partition.PartitionedTable;
import streamcruncher.innards.core.partition.RowStatus;
import streamcruncher.innards.core.partition.aggregate.AggregatorBuilder;
import streamcruncher.innards.core.partition.aggregate.AggregatorManager;
import streamcruncher.innards.core.partition.aggregate.BuilderCreator;
import streamcruncher.innards.core.partition.correlation.CorrelationSpec;
import streamcruncher.innards.core.partition.correlation.CorrelationSpec.MatchSpec;
import streamcruncher.innards.core.partition.custom.CustomSpec;
import streamcruncher.innards.core.partition.function.AggregateFunctionBuilder;
import streamcruncher.innards.core.partition.function.FunctionBuilder;
import streamcruncher.innards.core.partition.function.MiscFunctionBuilder;
import streamcruncher.innards.core.partition.function.WindowFunctionBuilder;
import streamcruncher.innards.core.partition.inmem.InMemChainedPartitionedTable;
import streamcruncher.innards.core.partition.inmem.InMemPartitionedTable;
import streamcruncher.innards.core.partition.inmem.InMemSpec;
import streamcruncher.innards.db.Constants;
import streamcruncher.innards.db.DatabaseInterface;
import streamcruncher.innards.expression.Statement;
import streamcruncher.innards.file.FileManager;
import streamcruncher.innards.file.FileManagerException;
import streamcruncher.innards.query.Aggregator;
import streamcruncher.innards.query.MiscFunction;
import streamcruncher.innards.query.Parser;
import streamcruncher.innards.query.QueryParseException;
import streamcruncher.innards.query.TimeUnit;
import streamcruncher.innards.query.WindowFunction;
import antlr.collections.AST;

/*
 * Author: Ashwin Jayaprakash Date: Jan 31, 2006 Time: 12:00:41 PM
 */

/*
 * todo Separate Query creation and deployment.
 */
/*
 * doc If an alias is used in 2 different parts of a query separated by a union,
 * then the row_status validation logic will get fooled if the alias is used in
 * the second part even though the second part does not have a Partition.
 */
public abstract class AbstractParser extends Parser {
    protected final HashMap<String, TableSpec> tableFQNAndSpecMap;

    protected final List<AST> tableRefListNodes;

    protected final HashMap<String, FilteredTable> aliasAndFilteredTables;

    protected final HashMap<String, String> cloneAliasesAndPartitionAliases;

    protected final EnumMap<RowStatus, List<Integer>> statusAndPositions;

    protected final HashSet<String> subQueriesToCache;

    protected final Stack<Context> currContext;

    protected final AtomicInteger positionInPrepStmt;

    protected final Random random;

    protected final HashSet<String> usedUpRandomStrings;

    // -------------

    protected TableFQN resultTable;

    protected CustomSpec customSpec;

    protected InMemSpec memSpec;

    protected CorrelationSpec correlationSpec;

    protected int windowsAndFiltersB4Correlation;

    protected RunningQuery runningQuery;

    protected AST topSelectExpression;

    protected AST prevNode;

    protected String topWhereClauseString;

    protected StringBuffer newSQL;

    // -------------

    protected static class Context {
        protected final StringWriter newSQL;

        protected final PrintWriter writer;

        protected boolean whereClauseCreated;

        protected Context() {
            this.newSQL = new StringWriter();
            this.writer = new PrintWriter(newSQL);
            this.whereClauseCreated = false;
        }
    }

    // -------------

    public AbstractParser(ParserParameters parserParameters) throws QueryParseException {
        super(parserParameters);

        this.tableFQNAndSpecMap = new HashMap<String, TableSpec>();

        this.tableRefListNodes = new LinkedList<AST>();
        this.aliasAndFilteredTables = new HashMap<String, FilteredTable>();
        this.cloneAliasesAndPartitionAliases = new HashMap<String, String>();
        this.statusAndPositions = new EnumMap<RowStatus, List<Integer>>(RowStatus.class);
        this.subQueriesToCache = new HashSet<String>();

        this.currContext = new Stack<Context>();
        this.positionInPrepStmt = new AtomicInteger(0);

        this.random = new Random();
        this.usedUpRandomStrings = new HashSet<String>();

        this.prevNode = null;
    }

    /**
     * @return The Parsed Queries, in the same sequence as they occurred in the
     *         Input String.
     * @throws QueryParseException
     */
    @Override
    public RunningQuery parse() throws QueryParseException {
        AST ast = parser.getAST();
        try {
            visit(ast);
        }
        catch (FileManagerException e) {
            throw new QueryParseException(e);
        }

        return runningQuery;
    }

    // -------------

    /**
     * @return the tableFQNAndSpecMap
     */
    public Map<String, TableSpec> getTableFQNAndSpecMap() {
        return tableFQNAndSpecMap;
    }

    // -------------

    protected void visit(AST theNode) throws FileManagerException, QueryParseException {
        boolean visitAlias = true;

        for (AST node = theNode; node != null && this.runningQuery == null; node = node
                .getNextSibling()) {
            boolean visitChildren = true;

            final int type = node.getType();

            switch (type) {
                case RQLTokenTypes.SQL_STATEMENT: {
                    break;
                }

                case RQLTokenTypes.SELECT_EXPRESSION: {
                    this.currContext.push(new Context());

                    if (this.currContext.size() == 1) {
                        topSelectExpression = node;
                        lookAhead(node);
                        afterLookAhead();
                    }

                    break;
                }

                case RQLTokenTypes.SELECT_LIST: {
                    if (this.currContext.size() == 1) {
                        String firstIdColumnInResult = getFirstIdColumnInResultSQL();

                        this.currContext.peek().writer.print(" ");

                        // Automatic Id-Generation.
                        if (firstIdColumnInResult != null && firstIdColumnInResult.length() > 0) {
                            this.currContext.peek().writer.print(firstIdColumnInResult);
                            this.currContext.peek().writer.print(", ");
                        }
                    }
                    break;
                }

                case RQLTokenTypes.TABLE_REFERENCE_LIST:
                    break;

                case RQLTokenTypes.DISPLAYED_COLUMN:
                    break;

                case RQLTokenTypes.EXP_SIMPLE:
                    break;

                case RQLTokenTypes.CASE_EXPRESSION:
                    break;

                case RQLTokenTypes.MONITOR_EXPRESSION: {
                    TableFQN table = handleMonitorExpression(node);
                    this.currContext.peek().writer.print(" ");
                    this.currContext.peek().writer.print(table.getFQN());
                    this.currContext.peek().writer.print(" ");

                    visitChildren = false;
                    break;
                }

                case RQLTokenTypes.SELECTED_TABLE:
                    break;

                case RQLTokenTypes.FIRST_CLAUSE:
                    break;

                case RQLTokenTypes.LIMIT_CLAUSE:
                    break;

                case RQLTokenTypes.TABLE_SPEC:
                    break;

                // todo Remove FilterSpec.
                case RQLTokenTypes.FILTER_SPEC: {
                    TableFQN table = createFilter(node);
                    this.currContext.peek().writer.print(" ");
                    this.currContext.peek().writer.print(table.getFQN());
                    this.currContext.peek().writer.print((asSupportedInAlias() ? " as " : " "));
                    this.currContext.peek().writer.print(table.getAlias());
                    this.currContext.peek().writer.print(" ");

                    visitChildren = false;
                    visitAlias = false;
                    break;
                }

                case RQLTokenTypes.PARTITION_SPEC_LIST: {
                    boolean inMem = isInMemCandidate();
                    boolean isCustomStore = isCustomCandidate();
                    if (isCustomStore) {
                        inMem = true;
                    }

                    TableFQN table = handlePartitionList(node, inMem);
                    this.currContext.peek().writer.print(" ");
                    this.currContext.peek().writer.print(table.getFQN());
                    this.currContext.peek().writer.print((asSupportedInAlias() ? " as " : " "));
                    this.currContext.peek().writer.print(table.getAlias());
                    this.currContext.peek().writer.print(" ");

                    // This really is inMem.
                    if (isCustomStore == false && inMem) {
                        TableSpec partitionTableSpec = tableFQNAndSpecMap.get(table.getFQN());
                        this.memSpec = new InMemSpec(partitionTableSpec);
                    }

                    visitChildren = false;
                    visitAlias = false;
                    break;
                }

                case RQLTokenTypes.CLONE_PARTITION_CLAUSE: {
                    TableFQN table = handleClonePartitionClause(node);
                    this.currContext.peek().writer.print(" ");
                    this.currContext.peek().writer.print(table.getFQN());
                    this.currContext.peek().writer.print((asSupportedInAlias() ? " as " : " "));
                    this.currContext.peek().writer.print(table.getAlias());
                    this.currContext.peek().writer.print(" ");

                    visitChildren = false;
                    visitAlias = false;
                    break;
                }

                case RQLTokenTypes.ALIAS:
                    visitChildren = visitAlias;
                    break;

                case RQLTokenTypes.WHERE_CONDITION: {
                    if (correlationSpec != null) {
                        TableSpec correlationResultTable = correlationSpec.getOutputTableSpec();
                        TableFQN tableFQN = new TableFQN(correlationResultTable.getSchema(),
                                correlationResultTable.getName());
                        WhereClauseSpec whereClauseSpec = createPartitionWhereClauseSpec(node,
                                tableFQN);
                        correlationSpec.setWhereClauseSpec(whereClauseSpec);

                        visitChildren = false;
                        visitAlias = false;
                    }
                    else if (memSpec != null) {
                        TableSpec partitionTable = memSpec.getPartitionTableSpec();
                        TableFQN tableFQN = new TableFQN(partitionTable.getSchema(), partitionTable
                                .getName());
                        WhereClauseSpec whereClauseSpec = createPartitionWhereClauseSpec(node,
                                tableFQN);
                        memSpec.setWhereClauseSpec(whereClauseSpec);

                        visitChildren = false;
                        visitAlias = false;
                    }

                    this.currContext.peek().whereClauseCreated = true;
                    this.currContext.push(new Context());
                    break;
                }

                case RQLTokenTypes.ROW_STATUS_CLAUSE: {
                    String whereCondition = handleRowStatusClause(node, null, statusAndPositions,
                            positionInPrepStmt);

                    this.currContext.peek().writer.print(" ");
                    this.currContext.peek().writer.print(whereCondition);

                    visitChildren = false;
                    visitAlias = false;
                    break;
                }

                case RQLTokenTypes.POST_WHERE_CLAUSES: {
                    AST postWhereClauseChild = node.getFirstChild();
                    if (correlationSpec != null && postWhereClauseChild != null
                            && postWhereClauseChild.getType() != RQLTokenTypes.LIMIT_CLAUSE) {
                        throw new QueryParseException(
                                createErrorPosString(postWhereClauseChild)
                                        + "Only the Limit clause is allowed after the Where clause in a Monitor-Expression.");
                    }

                    if (this.currContext.peek().whereClauseCreated == false) {
                        this.currContext.peek().whereClauseCreated = true;
                    }

                    break;
                }

                case RQLTokenTypes.SEMI: {
                    handleQueryEnd();
                    return;
                }

                default: {
                    /*
                     * Can be null due to Blank lines between queries or last
                     * trailing lines.
                     */
                    if (this.currContext.peek().writer != null) {
                        if (prevNode != null) {
                            if (prevNode.getType() == RQLTokenTypes.IDENTIFIER
                                    && node.getType() == RQLTokenTypes.DOT) {
                            }
                            else if (prevNode.getType() == RQLTokenTypes.DOT
                                    && node.getType() == RQLTokenTypes.IDENTIFIER) {
                            }
                            else {
                                this.currContext.peek().writer.print(" ");
                            }
                        }

                        this.currContext.peek().writer.print(node.getText());
                    }
                }
            }

            // -------------

            this.prevNode = node;

            if (visitChildren && node.getFirstChild() != null) {
                visit(node.getFirstChild());
            }

            // -------------

            switch (type) {
                case RQLTokenTypes.SELECT_EXPRESSION: {
                    Context root = this.currContext.pop();
                    root.writer.flush();
                    this.newSQL = root.newSQL.getBuffer();

                    break;
                }

                case RQLTokenTypes.WHERE_CONDITION: {
                    Context whereCtx = this.currContext.pop();
                    whereCtx.writer.flush();
                    StringBuffer whereClause = whereCtx.newSQL.getBuffer();

                    if (this.currContext.size() == 1) {
                        topWhereClauseString = whereClause.toString();
                    }

                    // Put into the parent Context.
                    this.currContext.peek().writer.append(whereClause);

                    break;
                }

                default: {
                    break;
                }
            }
        }
    }

    protected void tabs() {
        this.currContext.peek().writer.print("   ");
    }

    protected void lookAhead(AST theNode) {
        for (AST node = theNode; node != null; node = node.getNextSibling()) {
            if (node.getType() == RQLTokenTypes.TABLE_REFERENCE_LIST) {
                /*
                 * todo This method just blindly adds all Table references to
                 * the list. It does not distinguish between IN-clause referring
                 * to Partition or an IN-clause referring to a DB Table, which
                 * can be handled by the RowFilter.
                 */
                this.tableRefListNodes.add(node);
            }

            if (node.getFirstChild() != null) {
                lookAhead(node.getFirstChild());
            }
        }
    }

    protected void afterLookAhead() {
        if (isInMemCandidate() == false) {
            String s = System.getProperty(ConfigKeys.Custom.CUSTOM_STORE_NAME);
            if (s != null) {
                customSpec = new CustomSpec(s);
            }
        }
    }

    protected boolean isCustomCandidate() {
        return customSpec != null;
    }

    protected boolean isInMemCandidate() {
        return (tableRefListNodes.size() == 1 && tableRefListNodes.get(0).getNumberOfChildren() == 1);
    }

    protected void handleQueryEnd() throws FileManagerException, QueryParseException {
        int possibleNewAdditions = aliasAndFilteredTables.size();
        if (correlationSpec != null && possibleNewAdditions != windowsAndFiltersB4Correlation) {
            /*
             * Where Clause in the Correlation Spec has Windows/Filters, which
             * is not allowed.
             */
            throw new QueryParseException("Where clause in the Monitor-Expression cannot have"
                    + " Window and/or Partition definitions.");
        }

        // -------------

        createResultTableSpec();

        String finalQuery = null;
        String lastRowIdInResultTableSQL = null;

        if (correlationSpec != null) {
            TableSpec correlationResultTable = correlationSpec.getOutputTableSpec();
            TableFQN tableFQN = new TableFQN(correlationResultTable.getSchema(),
                    correlationResultTable.getName());
            replaceTableSpecWithVirtual(tableFQN);

            replaceTableSpecWithVirtual(this.resultTable);

            Statement statement = createSelectExpression(correlationSpec.getOutputTableSpec());
            this.correlationSpec.setStatement(statement);
        }
        else if (memSpec != null) {
            TableSpec partitionTable = memSpec.getPartitionTableSpec();
            TableFQN tableFQN = new TableFQN(partitionTable.getSchema(), partitionTable.getName());
            replaceTableSpecWithVirtual(tableFQN);

            replaceTableSpecWithVirtual(this.resultTable);

            Statement statement = createSelectExpression(memSpec.getPartitionTableSpec());
            this.memSpec.setStatement(statement);
        }
        else if (customSpec != null) {
            Map<String, TableSpec> filteredTblAliasAndTableSpec = new HashMap<String, TableSpec>();
            Map<String, RowSpec> filteredTblAliasAndRowSpec = new HashMap<String, RowSpec>();

            for (String key : aliasAndFilteredTables.keySet()) {
                FilteredTable filteredTable = aliasAndFilteredTables.get(key);
                TableFQN tableFQN = filteredTable.getTargetTableFQN();
                replaceTableSpecWithVirtual(tableFQN);

                TableSpec tableSpec = load(tableFQN);
                filteredTblAliasAndTableSpec.put(key, tableSpec);
                filteredTblAliasAndRowSpec.put(key, tableSpec.getRowSpec());
            }

            replaceTableSpecWithVirtual(this.resultTable);

            customSpec.setSourceTblAliasAndRowSpec(filteredTblAliasAndRowSpec);
            Statement statement = createSelectExpression(filteredTblAliasAndTableSpec);
            customSpec.setStatement(statement);
            customSpec.setWhereClause(topWhereClauseString);
        }
        else {
            TableSpec resultTableSpec = load(this.resultTable);
            RowSpec rowSpec = resultTableSpec.getRowSpec();
            IndexSpec[] indexSpecs = resultTableSpec.getIndexSpecs();

            String[] resultColumns = rowSpec.getColumnNames();
            String[] insertIntoColumns = getInsertIntoColumns(resultColumns);

            finalQuery = "insert into " + this.resultTable.getFQN() + " (";
            for (int i = 0; i < insertIntoColumns.length; i++) {
                finalQuery = finalQuery + insertIntoColumns[i];

                if (i < insertIntoColumns.length - 1) {
                    finalQuery = finalQuery + ", ";
                }
            }
            finalQuery = finalQuery + ") " + this.newSQL;

            // -------------

            String idColumnName = getIdColumnName(this.resultTable);
            lastRowIdInResultTableSQL = "select max(" + idColumnName + ")"
                    + (asSupportedInAlias() ? " as " : " ") + idColumnName + " from "
                    + this.resultTable.getFQN();

            // -------------

            for (String fqn : this.tableFQNAndSpecMap.keySet()) {
                TableSpec spec = this.tableFQNAndSpecMap.get(fqn);

                if (spec.isVirtual() == false) {
                    convertRowSpecToNative(spec.getRowSpec());
                    save(spec);
                }
            }

            TableSpec newTableSpec = customizeResultTableSpec(rowSpec, indexSpecs);
            save(newTableSpec);
        }

        Collection<FilteredTable> collection = this.aliasAndFilteredTables.values();
        FilteredTable[] theFilteredTables = collection
                .toArray(new FilteredTable[this.aliasAndFilteredTables.size()]);

        Object processorSpec = this.memSpec == null ? (this.correlationSpec == null ? (this.customSpec == null ? null
                : this.customSpec)
                : this.correlationSpec)
                : this.memSpec;
        this.runningQuery = new RunningQuery(this.queryName, finalQuery, theFilteredTables,
                processorSpec, this.statusAndPositions, this.resultTable, this.tableFQNAndSpecMap,
                lastRowIdInResultTableSQL, subQueriesToCache);
    }

    protected Statement createSelectExpression(TableSpec outputSpec) throws QueryParseException,
            FileManagerException {
        HashMap<String, TableSpec> map = new HashMap<String, TableSpec>();
        map.put(null, outputSpec);

        return createSelectExpression(map);
    }

    protected Statement createSelectExpression(Map<String, TableSpec> aliasAndTableSpecs)
            throws QueryParseException, FileManagerException {
        Integer firstX = null;
        Integer offsetX = null;
        boolean distinct = false;
        AST selectListNode = null;

        // select.
        AST node = topSelectExpression.getFirstChild();

        node = node.getNextSibling();
        while (node != null) {
            if (node.getType() == RQLTokenTypes.FIRST_CLAUSE) {
                // first.
                AST n = node.getFirstChild();
                // number.
                n = n.getNextSibling();
                firstX = Integer.parseInt(n.getText());
            }
            else if (node.getType() == RQLTokenTypes.SELECT_LIST) {
                selectListNode = node;
            }
            else if (node.getType() == RQLTokenTypes.POST_WHERE_CLAUSES) {
                AST n = node.getFirstChild();
                while (n != null) {
                    if (n.getType() == RQLTokenTypes.LIMIT_CLAUSE) {
                        // limit.
                        AST x = n.getFirstChild();
                        // number.
                        x = x.getNextSibling();
                        firstX = Integer.parseInt(x.getText());

                        // offset.
                        x = x.getNextSibling();
                        if (x != null) {
                            // number.
                            x = x.getNextSibling();
                            offsetX = Integer.parseInt(x.getText());
                        }

                        break;
                    }
                    // todo Implement Group by, having, order by
                    n = n.getNextSibling();
                }
            }
            else if (node.getType() == RQLTokenTypes.LITERAL_distinct) {
                distinct = true;
            }

            node = node.getNextSibling();
        }

        // todo Group clause.
        List<WhereClauseSpec> groupExpressions = convertGroupList(null);
        List<WhereClauseSpec> columnExpressions = convertSelectList(selectListNode,
                aliasAndTableSpecs);

        Statement statement = new Statement(firstX, offsetX, distinct, columnExpressions);
        return statement;
    }

    protected List<WhereClauseSpec> convertGroupList(AST groupList) {
        // todo Implement Group-by clause expressions.
        return null;
    }

    protected List<WhereClauseSpec> convertSelectList(AST selectListNode,
            Map<String, TableSpec> aliasAndTableSpecs) throws QueryParseException,
            FileManagerException {
        List<WhereClauseSpec> columnExpressions = new LinkedList<WhereClauseSpec>();

        for (String alias : aliasAndTableSpecs.keySet()) {
            TableSpec tableSpec = aliasAndTableSpecs.get(alias);
            TableFQN tableFQN = new TableFQN(tableSpec.getSchema(), tableSpec.getName());
            //todo Must handle columns form Multiple Schemas. 
        }

        AST node = selectListNode.getFirstChild();
        if (node.getType() != RQLTokenTypes.ASTERISK) {
            while (node != null) {
                if (node.getType() == RQLTokenTypes.DISPLAYED_COLUMN) {
                    AST contentNode = node.getFirstChild();

                    if (contentNode.getType() == RQLTokenTypes.EXP_SIMPLE) {
                        WhereClauseSpec spec = createWhereClauseSpec(contentNode.getFirstChild(),
                                aliasAndTableSpecs);
                        columnExpressions.add(spec);
                    }
                    else if (contentNode.getType() == RQLTokenTypes.CASE_EXPRESSION) {
                        // todo Implement Case-Expression.
                    }
                    else {
                        // schema.table.*
                        throw new QueryParseException(createErrorPosString(node)
                                + "Select clause in Monitor Expressions can"
                                + " only refer to the fields projected by the Alert clause.");
                    }
                }

                node = node.getNextSibling();
            }
        }

        return columnExpressions;
    }

    protected void createResultTableSpec() {
        String[] resultColumnNames = new String[resultColumnTypes.length];

        char c = 65;
        for (int i = 0; i < resultColumnNames.length; i++) {
            resultColumnNames[i] = new Character(c) + "" + i;

            c++;
            c = (c > 90) ? 65 : c;
        }

        // -------------

        String schema = getSchema();
        String name = getRandomizedUniqueString(queryName);
        resultTable = new TableFQN(schema, name);

        // -------------

        String[] columns = new String[resultColumnNames.length + 1];
        for (int i = 1; i < columns.length; i++) {
            columns[i] = resultColumnNames[i - 1];
        }
        columns[0] = createIdColumnName(resultTable.getName());

        String[] columnTypes = new String[resultColumnTypes.length + 1];
        for (int i = 1; i < columnTypes.length; i++) {
            columnTypes[i] = resultColumnTypes[i - 1];
        }
        columnTypes[0] = getResultTableIdColumnType();

        IndexSpec uniqueIndex = createIndexSpec(resultTable.getSchema(),
                createIndexName(columns[0]), resultTable.getFQN(), true, columns[0], true);

        IndexSpec[] indexSpecs = {};
        if (isUniqueIndexOnResultTableReqd()) {
            indexSpecs = new IndexSpec[] { uniqueIndex };
        }

        RowSpec rowSpec = new RowSpec(columns, columnTypes, 0, -1, -1);

        // Create an un-customized version first.
        TableSpec newTableSpec = new TableSpec(resultTable.getSchema(), resultTable.getName(),
                rowSpec, indexSpecs, null);
        save(newTableSpec);
    }

    protected boolean isUniqueIndexOnResultTableReqd() {
        return true;
    }

    protected TableSpec customizeResultTableSpec(RowSpec rowSpec, IndexSpec[] indexSpecs) {
        TableSpec newTableSpec = createUnpartitionedTableSpec(resultTable.getSchema(), resultTable
                .getName(), rowSpec, indexSpecs, null);

        return newTableSpec;
    }

    /**
     * @param resultTableColumns
     * @return The columns that must be included in the results, based on the
     *         ones provided in the parameter.
     */
    protected abstract String[] getInsertIntoColumns(String[] resultTableColumns);

    /**
     * @return "null" or "sequence_XYZ.nextval" or just "" etc.
     */
    protected abstract String getFirstIdColumnInResultSQL();

    protected long convertUsingTimeUnitToMillis(long input, String timeUnitName)
            throws QueryParseException {
        if (timeUnitName.equalsIgnoreCase(TimeUnit.SECONDS.name())) {
            input = input * 1000;
        }
        else if (timeUnitName.equalsIgnoreCase(TimeUnit.MINUTES.name())) {
            input = input * 60 * 1000;
        }
        else if (timeUnitName.equalsIgnoreCase(TimeUnit.HOURS.name())) {
            input = input * 60 * 60 * 1000;
        }
        else if (timeUnitName.equalsIgnoreCase(TimeUnit.DAYS.name())) {
            input = input * 24 * 60 * 60 * 1000;
        }
        else {
            throw new QueryParseException("The Time unit: " + timeUnitName
                    + " provided is not defined.");
        }

        return input;
    }

    protected TableFQN createTableFQN(AST tableSpecNode, AST aliasNode) {
        String schema = null;
        String table = null;

        // For Ex: "schema.name" or just "name".
        AST tmpNode = tableSpecNode.getFirstChild();
        table = tmpNode.getText();

        tmpNode = tmpNode.getNextSibling();
        if (tmpNode != null && tmpNode.getText().equals(".")) {
            schema = table;

            tmpNode = tmpNode.getNextSibling();
            table = tmpNode.getText();
        }

        // -------------

        String alias = extractAlias(aliasNode);

        return new TableFQN(schema, table, alias);
    }

    protected abstract boolean asSupportedInAlias();

    /**
     * Parses, creates and add the {@link FilteredTable} to
     * {@link #aliasAndFilteredTables}.
     * 
     * @param filterSpecNode
     * @return The newly created {@link TableFQN}.
     * @throws QueryParseException
     */
    protected TableFQN createFilter(AST filterSpecNode) throws QueryParseException {
        // For Ex: "tablespec<> filter using 'xxxxxx'".
        AST tableSpecNode = filterSpecNode.getFirstChild();
        AST aliasNode = filterSpecNode.getNextSibling();
        TableFQN selectedTable = createTableFQN(tableSpecNode, aliasNode);

        // -------------

        // For Ex: "(filter using 'xxxx'".
        AST tmpNode = tableSpecNode.getNextSibling();
        tmpNode = tmpNode.getNextSibling();
        tmpNode = tmpNode.getNextSibling();

        AST quotedNode = tmpNode.getNextSibling();
        String quotedStr = quotedNode.getText();
        String filterClassName = quotedStr.substring(1, quotedStr.length() - 1);

        // -------------

        TableFQN newTable = getFilterTableFQN(selectedTable);
        try {
            createFilterTable(selectedTable, newTable);

            RowSpec selectedTableRowSpec = getRowSpec(selectedTable);

            FilteredTable filteredTable = new FilteredTable(queryName, selectedTable,
                    selectedTableRowSpec, newTable, filterClassName, null
            /*
             * todo Add parameters for Filter
             */);
            aliasAndFilteredTables.put(newTable.getAlias(), filteredTable);
        }
        catch (FileManagerException e) {
            throw new QueryParseException(createErrorPosString(tableSpecNode)
                    + "An error occurred while creating/accessing the Filtered-Table: " + newTable
                    + " for: " + selectedTable, e);
        }

        return newTable;
    }

    protected TableFQN handlePartitionList(AST partitionSpecListNode, boolean inMemoryStore)
            throws QueryParseException, FileManagerException {
        AST tableSpecNode = partitionSpecListNode.getFirstChild();
        AST aliasNode = partitionSpecListNode.getNextSibling();

        // -------------

        LinkedList<AST> partitionSpecNodes = new LinkedList<AST>();

        AST partitionSpecNode = tableSpecNode.getNextSibling();
        while (partitionSpecNode != null) {
            // "to" separate the nodes.
            if (partitionSpecNode.getType() == RQLTokenTypes.PARTITION_SPEC) {
                partitionSpecNodes.add(partitionSpecNode);
            }

            partitionSpecNode = partitionSpecNode.getNextSibling();
        }

        TableFQN selectedTable = createTableFQN(tableSpecNode, aliasNode);
        TableFQN resultFQN = createPartition(partitionSpecNodes, selectedTable, inMemoryStore);
        return resultFQN;
    }

    /**
     * Parses, creates and adds the {@link TableFQN} to the
     * {@link Context#tableAndWindow}.
     * 
     * @param partitionSpecNodes
     * @param selectedTable
     * @param inMemoryStore
     * @return
     * @throws QueryParseException
     * @throws FileManagerException
     */
    protected TableFQN createPartition(LinkedList<AST> partitionSpecNodes, TableFQN selectedTable,
            boolean inMemoryStore) throws QueryParseException, FileManagerException {
        PartitionedTable partitionedTable = null;
        ChainedPartitionedTable cpt = null;

        TableFQN sourceFQN = selectedTable;
        TableFQN newTableFQN = null;

        int count = 0;
        for (AST partitionSpecNode : partitionSpecNodes) {
            // For Ex: "(partition [by] xxxxx".
            AST tmpNode = partitionSpecNode.getFirstChild();
            tmpNode = tmpNode.getNextSibling();

            newTableFQN = getFilterTableFQN(sourceFQN);

            LinkedHashSet<String> partitionColumnNames = new LinkedHashSet<String>();
            AST rowPickSpecNode = null;
            AST whereClauseNode = null;
            while ((tmpNode = tmpNode.getNextSibling()) != null) {
                int type = tmpNode.getType();
                switch (type) {
                    case RQLTokenTypes.IDENTIFIER:
                        String name = tmpNode.getText();
                        if (partitionColumnNames.contains(name) == false) {
                            partitionColumnNames.add(name);
                        }
                        else {
                            throw new QueryParseException(createErrorPosString(tmpNode)
                                    + "The Partition column: " + name
                                    + " appears more than once, which is not allowed.");
                        }
                        break;
                    case RQLTokenTypes.ROW_PICK_SPEC:
                        rowPickSpecNode = tmpNode;
                        break;
                    case RQLTokenTypes.WHERE_CONDITION:
                        whereClauseNode = tmpNode;
                        break;
                    default:
                        break;
                }
            }

            // -------------

            PartitionSpec rowPickSpec = createRowPickSpec(rowPickSpecNode, partitionColumnNames,
                    whereClauseNode, sourceFQN, newTableFQN);

            RowSpec sourceTableRowSpec = null;
            try {
                sourceTableRowSpec = getRowSpec(sourceFQN);
            }
            catch (FileManagerException e) {
                throw new QueryParseException(
                        "An error occurred while preparing the Partitioned-Table: " + newTableFQN
                                + " generated for: " + sourceFQN, e);
            }

            // -------------

            if (partitionedTable == null) {
                if (partitionSpecNodes.size() == 1) {
                    if (inMemoryStore) {
                        replaceTableSpecWithVirtual(newTableFQN);

                        partitionedTable = new InMemPartitionedTable(queryName, sourceFQN,
                                sourceTableRowSpec, newTableFQN, rowPickSpec);
                    }
                    else {
                        partitionedTable = new PartitionedTable(queryName, sourceFQN,
                                sourceTableRowSpec, newTableFQN, rowPickSpec);
                    }
                }
                else {
                    replaceTableSpecWithVirtual(newTableFQN);

                    partitionedTable = new EncapsulatedPartitionedTable(queryName, selectedTable,
                            sourceTableRowSpec, newTableFQN, rowPickSpec);
                }
            }
            else {
                WhereClauseSpec whereClauseSpec = rowPickSpec.getWhereClauseSpec();
                if (whereClauseSpec == null) {
                    throw new QueryParseException(
                            "Chained Partitions should always have a Where-clause.");
                }

                Map<String, Object> context = whereClauseSpec.getContext();
                if (context.containsKey(RowStatus.class.getName()) == false) {
                    /*
                     * For Chained Partitions: If the Where clause exists, then
                     * it must start with RowStatus (New or Dead only) and then
                     * AND and the rest of the expression.
                     */
                    throw new QueryParseException(
                            "The Where-clause for Chained Partitions must always start with the 'row_status' ("
                                    + RowStatus.NEW.name() + "/" + RowStatus.DEAD.name()
                                    + ") clause. The remaining part of the "
                                    + "expression (if any), must be followed an 'and' literal.");
                }

                ChainedPartitionedTable newCPT = null;

                // Last in the chain and this is Correlated.
                if (inMemoryStore && (count + 1) == partitionSpecNodes.size()) {
                    replaceTableSpecWithVirtual(newTableFQN);

                    newCPT = new InMemChainedPartitionedTable(sourceFQN, sourceTableRowSpec,
                            newTableFQN, rowPickSpec);
                }
                else {
                    // Not the last in the Chain.
                    if ((count + 1) < partitionSpecNodes.size()) {
                        replaceTableSpecWithVirtual(newTableFQN);
                    }

                    newCPT = new ChainedPartitionedTable(sourceFQN, sourceTableRowSpec,
                            newTableFQN, rowPickSpec);
                }

                if (count == 1) {
                    ((EncapsulatedPartitionedTable) partitionedTable).setNextCPT(newCPT);
                }
                else {
                    cpt.setNextCPT(newCPT);
                }

                cpt = newCPT;
            }

            count++;
            sourceFQN = newTableFQN;
        }

        aliasAndFilteredTables.put(newTableFQN.getAlias(), partitionedTable);

        return sourceFQN;
    }

    /**
     * @param rowPickSpecNode
     * @param partitionColumnNames
     * @param whereClauseNode
     *            <code>null</code> allowed.
     * @param selectedTable
     * @param newTable
     * @return
     * @throws QueryParseException
     * @throws FileManagerException
     */
    protected PartitionSpec createRowPickSpec(AST rowPickSpecNode,
            LinkedHashSet<String> partitionColumnNames, AST whereClauseNode,
            TableFQN selectedTable, TableFQN newTable) throws QueryParseException,
            FileManagerException {
        AST grpFunctionNode = null;
        AST windowFunctionNode = null;
        AST miscFunctionNode = null;

        AST childNode = rowPickSpecNode.getFirstChild();
        do {
            int type = childNode.getType();
            switch (type) {
                case RQLTokenTypes.MISC_FUNCTION:
                    miscFunctionNode = childNode;
                    break;
                case RQLTokenTypes.WINDOW_FUNCTION:
                    windowFunctionNode = childNode;
                    break;
                case RQLTokenTypes.AGGREGATE_FUNCTION:
                    grpFunctionNode = childNode;
                    break;
            }

            childNode = childNode.getNextSibling();
        } while (childNode != null);

        // ----------------

        FunctionBuilder functionBuilder = createFunctionBuilder(grpFunctionNode,
                windowFunctionNode, miscFunctionNode, partitionColumnNames, selectedTable, newTable);

        WhereClauseSpec whereClauseSpec = null;
        if (whereClauseNode != null) {
            whereClauseSpec = createPartitionWhereClauseSpec(whereClauseNode, selectedTable);
        }

        String[] columnNames = partitionColumnNames
                .toArray(new String[partitionColumnNames.size()]);
        PartitionSpec partitionSpec = new PartitionSpec(columnNames, whereClauseSpec,
                functionBuilder);
        return partitionSpec;
    }

    protected FunctionBuilder createFunctionBuilder(AST grpFunctionNode, AST windowFunctionNode,
            AST miscFunctionNode, LinkedHashSet<String> partitionColumnNames,
            TableFQN selectedTable, TableFQN newTable) throws QueryParseException {
        FunctionBuilder retVal = null;

        try {
            /*
             * Even if Aggregator Function is not null, this Table is required
             * for the Window/Misc FunctionBuilders to initialize. If the
             * Aggregator exists, then it will overwrite the Filter-Table with
             * the aggregated column types later.
             */
            createFilterTable(selectedTable, newTable);

            // --------------

            if (windowFunctionNode != null) {
                retVal = createWindowFunctionBuilder(windowFunctionNode, selectedTable, newTable);
            }
            else if (miscFunctionNode != null) {
                retVal = createMiscFunctionBuilder(miscFunctionNode, selectedTable, newTable);
            }

            if (grpFunctionNode != null) {
                retVal = createGroupFunctionBuilder(grpFunctionNode, partitionColumnNames, retVal,
                        newTable);
            }
        }
        catch (FileManagerException exception) {
            throw new QueryParseException(
                    "An error occurred while creating the Partitioned-Table: " + newTable
                            + " for: " + selectedTable, exception);
        }

        return retVal;
    }

    protected FunctionBuilder createGroupFunctionBuilder(AST partitionGroupFunc,
            LinkedHashSet<String> partitionColNames, FunctionBuilder innerFunction,
            TableFQN newTable) throws QueryParseException, FileManagerException {
        RowSpec tempTargetSpec = getRowSpec(newTable);

        String[] origColNames = tempTargetSpec.getColumnNames();
        String[] origColTypes = tempTargetSpec.getColumnNativeTypes();
        LinkedHashMap<String, String> oldNamesAndTypes = new LinkedHashMap<String, String>();
        for (int i = 0; i < origColNames.length; i++) {
            oldNamesAndTypes.put(origColNames[i], origColTypes[i]);
        }

        /*
         * Contains only the columns in the Partition and the "aggregated"
         * columns.
         */
        LinkedList<String> modifiedSpecColNames = new LinkedList<String>();
        LinkedList<String> modifiedSpecColTypes = new LinkedList<String>();

        for (String name : partitionColNames) {
            String type = oldNamesAndTypes.get(name);

            modifiedSpecColNames.add(name);
            modifiedSpecColTypes.add(type);
        }

        /*
         * Next position starts after 3 Internal columns and the partition
         * columns.
         */
        int nextColPosInTargetRowSpec = modifiedSpecColNames.size() + 3;

        // --------------

        // "with"
        AST tmpNode = partitionGroupFunc.getFirstChild();

        boolean pinned = false;

        ArrayList<AST> funcSpecNodes = new ArrayList<AST>();
        while ((tmpNode = tmpNode.getNextSibling()) != null) {
            if (tmpNode.getType() == RQLTokenTypes.AGGREGATE_FUNCTION_SPEC) {
                funcSpecNodes.add(tmpNode);
            }
            else if (tmpNode.getType() == RQLTokenTypes.LITERAL_pinned) {
                pinned = true;
            }
        }

        // --------------

        AggregatorManager manager = Registry.getImplFor(AggregatorManager.class);
        ArrayList<AggregatorBuilder> builders = new ArrayList<AggregatorBuilder>();
        ArrayList<Integer> aggregatorTargetPositions = new ArrayList<Integer>();

        for (AST ast : funcSpecNodes) {
            ast = ast.getFirstChild();
            String nodeText = ast.getText();

            Aggregator selectedAggregator = null;
            for (Aggregator aggregator : Aggregator.values()) {
                if (nodeText.equalsIgnoreCase(aggregator.name())) {
                    selectedAggregator = aggregator;
                    break;
                }
            }

            if (selectedAggregator == null) {
                throw new QueryParseException(createErrorPosString(ast)
                        + "Unrecognized Aggregator: " + nodeText);
            }

            ArrayList<String> params = new ArrayList<String>();
            String alias = null;
            AST node = ast.getNextSibling();
            AggregationStage aggregationStage = AggregationStage.BOTH;
            do {
                if (node.getType() == RQLTokenTypes.IDENTIFIER
                        || node.getType() == RQLTokenTypes.QUOTED_STRING) {
                    nodeText = node.getText();
                    params.add(nodeText);
                }
                else if (node.getType() == RQLTokenTypes.LITERAL_diff) {
                    nodeText = node.getText();
                    params.add(nodeText);
                }
                else if (node.getType() == RQLTokenTypes.LITERAL_entrance) {
                    if (AggregationStage.ENTRANCE.name().equalsIgnoreCase(node.getText())) {
                        aggregationStage = AggregationStage.ENTRANCE;
                    }
                }
                else if (node.getType() == RQLTokenTypes.ALIAS) {
                    // "as abcd".
                    alias = extractAlias(node);
                }
            } while ((node = node.getNextSibling()) != null);

            String aggregatorName = null;
            if (selectedAggregator == Aggregator.CUSTOM) {
                aggregatorName = params.remove(0);
            }
            else {
                aggregatorName = selectedAggregator.name();
            }

            AbstractAggregatorHelper helper = manager.getHelper(aggregatorName);
            if (helper == null) {
                throw new QueryParseException("Aggregator: " + aggregatorName
                        + " does not have a registered "
                        + AbstractAggregatorHelper.class.getSimpleName());
            }

            // --------------

            String[] paramArr = params.toArray(new String[params.size()]);

            try {
                String type = helper.getAggregatedColumnDDLFragment(getDBName(), paramArr,
                        oldNamesAndTypes);

                if (modifiedSpecColNames.contains(alias)) {
                    throw new QueryParseException("The Alias: " + alias
                            + " clashes with a Partition column name or"
                            + " with another Aggregate alias.");
                }

                modifiedSpecColNames.add(alias);
                modifiedSpecColTypes.add(type);

                BuilderCreator builderCreator = new BuilderCreator(helper);
                AggregatorBuilder builder = builderCreator.createBuilder(paramArr,
                        oldNamesAndTypes, aggregationStage);
                builders.add(builder);
            }
            catch (Exception e) {
                throw new QueryParseException(e);
            }

            aggregatorTargetPositions.add(nextColPosInTargetRowSpec);

            nextColPosInTargetRowSpec++;
        }

        AggregatorBuilder[] builderArr = builders.toArray(new AggregatorBuilder[builders.size()]);

        int[] aggregatorTargetPositionsArr = new int[aggregatorTargetPositions.size()];
        int k = 0;
        for (Iterator<Integer> iter = aggregatorTargetPositions.iterator(); iter.hasNext(); k++) {
            aggregatorTargetPositionsArr[k] = iter.next();
        }

        String[] newColNames = modifiedSpecColNames
                .toArray(new String[modifiedSpecColNames.size()]);
        String[] newColTypes = modifiedSpecColTypes
                .toArray(new String[modifiedSpecColTypes.size()]);

        RowSpec finalRowSpec = new RowSpec(newColNames, newColTypes);
        createTableSpecUsingRowSpecForPartition(newTable, finalRowSpec);
        // Get the complete/modified Spec.
        finalRowSpec = getRowSpec(newTable);

        return new AggregateFunctionBuilder(innerFunction, pinned, builderArr,
                aggregatorTargetPositionsArr, finalRowSpec);
    }

    /**
     * @param windowFunc
     * @param selectedTable
     * @param newTable
     * @return
     * @throws FileManagerException
     * @throws QueryParseException
     */
    protected WindowFunctionBuilder createWindowFunctionBuilder(AST windowFunc,
            TableFQN selectedTable, TableFQN newTable) throws FileManagerException,
            QueryParseException {
        AST tmpNode = windowFunc.getFirstChild();
        String nodeText = tmpNode.getText();

        // Just validate Id-column type.
        getIdColumnName(selectedTable);

        RowSpec sourceRowSpec = getRowSpec(selectedTable);

        // ----------------

        String providerName = WindowSizeProvider.getName();

        if (nodeText.equalsIgnoreCase(WindowFunction.LAST.name())) {
            tmpNode = tmpNode.getNextSibling();
            nodeText = tmpNode.getText();

            long windowSize = Long.parseLong(nodeText);
            Integer maxSize = null;

            tmpNode = tmpNode.getNextSibling();
            if (tmpNode != null) {
                if (tmpNode.getType() == RQLTokenTypes.TIME_UNIT_SPEC) {
                    AST timeNode = tmpNode.getFirstChild();
                    String timeUnitName = timeNode.getText();

                    windowSize = convertUsingTimeUnitToMillis(windowSize, timeUnitName);

                    int tsPos = sourceRowSpec.getTimestampColumnPosition();
                    if (tsPos < 0) {
                        throw new QueryParseException(createErrorPosString(tmpNode)
                                + "The Time Window defined" + " on the source Table: "
                                + selectedTable.getFQN()
                                + " does not have a Timestamp-column in the RowSpec");
                    }

                    providerName = TimeWindowSizeProvider.getName();

                    tmpNode = tmpNode.getNextSibling();
                    // "max"
                    if (tmpNode != null && tmpNode.getType() == RQLTokenTypes.LITERAL_max) {
                        // number
                        tmpNode = tmpNode.getNextSibling();
                        nodeText = tmpNode.getText();
                        maxSize = Integer.parseInt(nodeText);
                    }
                    else if (tmpNode != null && tmpNode.getType() == RQLTokenTypes.QUOTED_STRING) {
                        providerName = getProviderName(providerName, tmpNode, false);
                    }
                    else {
                        maxSize = Integer.MAX_VALUE;
                    }

                    providerName = getProviderName(providerName, tmpNode, true);
                }
            }

            if (maxSize == null) {
                checkProviderExists(providerName, WindowSizeProvider.class);

                try {
                    return new WindowFunctionBuilder.SlidingWindowFunctionBuilder(sourceRowSpec,
                            getRowSpec(newTable), (int) windowSize, providerName);
                }
                catch (ProviderManagerException e) {
                    throw new QueryParseException(e);
                }
            }

            checkProviderExists(providerName, TimeWindowSizeProvider.class);

            try {
                return new WindowFunctionBuilder.TimeWindowFunctionBuilder(sourceRowSpec,
                        getRowSpec(newTable), maxSize, windowSize, providerName);
            }
            catch (ProviderManagerException e) {
                throw new QueryParseException(e);
            }
        }
        else if (nodeText.equalsIgnoreCase(WindowFunction.LATEST.name())) {
            tmpNode = tmpNode.getNextSibling();
            nodeText = tmpNode.getText();

            int windowSize = Integer.parseInt(nodeText);

            providerName = getProviderName(providerName, tmpNode, true);

            try {
                return new WindowFunctionBuilder.TumblingWindowFunctionBuilder(sourceRowSpec,
                        getRowSpec(newTable), windowSize, providerName);
            }
            catch (ProviderManagerException e) {
                throw new QueryParseException(e);
            }
        }

        throw new QueryParseException("Unrecognized Window Function: " + nodeText);
    }

    /**
     * @param miscFunc
     * @param selectedTable
     * @param newTable
     * @return
     * @throws FileManagerException
     * @throws QueryParseException
     */
    protected MiscFunctionBuilder createMiscFunctionBuilder(AST miscFunc, TableFQN selectedTable,
            TableFQN newTable) throws FileManagerException, QueryParseException {
        AST tmpNode = miscFunc.getFirstChild();
        String nodeText = tmpNode.getText();

        // Just validate Id-column type.
        getIdColumnName(selectedTable);

        RowSpec sourceRowSpec = getRowSpec(selectedTable);

        // ----------------

        String providerName = WindowSizeProvider.getName();

        if (nodeText.equalsIgnoreCase(MiscFunction.RANDOM.name())) {
            tmpNode = tmpNode.getNextSibling();
            nodeText = tmpNode.getText();

            providerName = getProviderName(providerName, tmpNode, true);

            int windowSize = Integer.parseInt(nodeText);

            checkProviderExists(providerName, WindowSizeProvider.class);

            try {
                return new MiscFunctionBuilder.RandomFunctionBuilder(sourceRowSpec,
                        getRowSpec(newTable), windowSize, providerName);
            }
            catch (ProviderManagerException e) {
                throw new QueryParseException(e);
            }
        }
        else if (nodeText.equalsIgnoreCase(MiscFunction.HIGHEST.name())
                || nodeText.equalsIgnoreCase(MiscFunction.LOWEST.name())) {
            String function = nodeText;

            tmpNode = tmpNode.getNextSibling();
            nodeText = tmpNode.getText();

            int windowSize = Integer.parseInt(nodeText);

            // using.
            tmpNode = tmpNode.getNextSibling();

            // column-name.
            tmpNode = tmpNode.getNextSibling();
            String columnName = tmpNode.getText();

            if (checkColumnExists(sourceRowSpec, columnName) == false) {
                throw new QueryParseException(createErrorPosString(tmpNode) + "The Column: "
                        + columnName + " on which the Window is "
                        + "defined does not exist in the source Table: " + selectedTable.getFQN());
            }

            LinkedList<String> groupColumnNames = new LinkedList<String>();
            // with.
            tmpNode = tmpNode.getNextSibling();
            if (tmpNode != null && tmpNode.getType() != RQLTokenTypes.QUOTED_STRING) {
                // update.
                tmpNode = tmpNode.getNextSibling();

                // group.
                tmpNode = tmpNode.getNextSibling();

                // Columns.
                while ((tmpNode = tmpNode.getNextSibling()) != null) {
                    if (tmpNode.getType() == RQLTokenTypes.IDENTIFIER) {
                        String groupColName = tmpNode.getText();

                        if (checkColumnExists(sourceRowSpec, groupColName) == false) {
                            throw new QueryParseException(createErrorPosString(tmpNode)
                                    + "The Column: " + groupColName
                                    + " on which the Window's Group is "
                                    + "defined does not exist in the source Table: "
                                    + selectedTable.getFQN());
                        }

                        groupColumnNames.add(groupColName);
                    }
                    else if (tmpNode.getType() == RQLTokenTypes.QUOTED_STRING) {
                        providerName = tmpNode.getText();
                        providerName = providerName.replaceAll("\'", "");
                    }
                }
            }

            providerName = getProviderName(providerName, tmpNode, false);

            String[] groupColNameArray = new String[groupColumnNames.size()];
            for (int i = 0; i < groupColNameArray.length; i++) {
                groupColNameArray[i] = groupColumnNames.get(i);
            }

            checkProviderExists(providerName, WindowSizeProvider.class);

            if (function.equalsIgnoreCase(MiscFunction.HIGHEST.name())) {
                try {
                    return new MiscFunctionBuilder.HighestFunctionBuilder(sourceRowSpec,
                            getRowSpec(newTable), windowSize, providerName, columnName,
                            groupColNameArray);
                }
                catch (ProviderManagerException e) {
                    throw new QueryParseException(e);
                }
            }
            else if (function.equalsIgnoreCase(MiscFunction.LOWEST.name())) {
                try {
                    return new MiscFunctionBuilder.LowestFunctionBuilder(sourceRowSpec,
                            getRowSpec(newTable), windowSize, providerName, columnName,
                            groupColNameArray);
                }
                catch (ProviderManagerException e) {
                    throw new QueryParseException(e);
                }
            }
        }

        throw new QueryParseException("Unrecognized Misc Function: " + nodeText);
    }

    protected TableFQN handleClonePartitionClause(AST cloneNode) throws FileManagerException {
        // self
        AST node = cloneNode.getFirstChild();
        // #
        node = node.getNextSibling();
        // identifier
        node = node.getNextSibling();
        String realPartitionAlias = node.getText();

        // alias
        node = node.getNextSibling();
        String cloneAlias = extractAlias(node);

        if (aliasAndFilteredTables.containsKey(realPartitionAlias)) {
            FilteredTable filteredTable = aliasAndFilteredTables.get(realPartitionAlias);

            TableFQN t = extractTargetTableFQN(filteredTable);

            cloneAliasesAndPartitionAliases.put(cloneAlias, realPartitionAlias);
            return new TableFQN(t.getSchema(), t.getName(), cloneAlias);
        }

        throw new FileManagerException(createErrorPosString(cloneNode) + "The Partition alias: "
                + realPartitionAlias + " on which the clone alias: " + cloneAlias
                + " has been defined does not exist.");
    }

    protected String getProviderName(String defaultName, AST node, boolean checkSibling) {
        String providerName = defaultName;

        if (checkSibling && node != null) {
            node = node.getNextSibling();
        }

        if (node != null && node.getType() == RQLTokenTypes.QUOTED_STRING) {
            providerName = node.getText();
            providerName = providerName.replaceAll("\'", "");
        }

        return providerName;
    }

    protected void checkProviderExists(String providerName, Class type) throws QueryParseException {
        ProviderManager manager = Registry.getImplFor(ProviderManager.class);
        if (manager.doesProviderExist(providerName) == false) {
            throw new QueryParseException("A suitable " + type.getSimpleName()
                    + " has not been registered for the name: " + providerName);
        }
    }

    protected boolean checkColumnExists(RowSpec sourceRowSpec, String columnName) {
        String[] colNames = sourceRowSpec.getColumnNames();
        for (int i = 0; i < colNames.length; i++) {
            if (colNames[i].equalsIgnoreCase(columnName)) {
                return true;
            }
        }

        return false;
    }

    protected RowSpec getRowSpec(TableFQN table) throws FileManagerException {
        TableSpec tableSpec = load(table);

        RowSpec originalRowSpec = tableSpec.getRowSpec();
        return originalRowSpec;
    }

    protected static final String prevNodeHolderKey = "@_prevNodeHolder_";

    protected static final String rowStatusKey = "@_rowStatus_";

    protected static final String unClosedBracesKey = "@_unClosedBraces_";

    protected static final String selectSubQryListKey = "@_selectSubQryList_";

    protected WhereClauseSpec createPartitionWhereClauseSpec(AST whereConditionNode,
            TableFQN defaultPartitionedTable) throws FileManagerException, QueryParseException {
        TableSpec spec = load(defaultPartitionedTable);
        Map<String, TableSpec> aliasAndSpec = new HashMap<String, TableSpec>();
        // No alias.
        aliasAndSpec.put(null, spec);
        return createWhereClauseSpec(whereConditionNode.getFirstChild(), aliasAndSpec);
    }

    /**
     * Originally written for Partition Where Clauses, now it can handle any
     * Expression.
     * 
     * @param whereConditionList
     * @param aliasAndTableSpecs
     * @return
     * @throws FileManagerException
     * @throws QueryParseException
     */
    protected WhereClauseSpec createWhereClauseSpec(AST whereConditionList,
            Map<String, TableSpec> aliasAndTableSpecs) throws FileManagerException,
            QueryParseException {
        StringBuilder whereCondition = new StringBuilder();
        Map<String, Object> context = new HashMap<String, Object>();

        Map<String, LinkedHashMap<String, String>> aliasAndColumnNameAndTypes = new HashMap<String, LinkedHashMap<String, String>>();
        for (String alias : aliasAndTableSpecs.keySet()) {
            LinkedHashMap<String, String> columnNameAndTypes = new LinkedHashMap<String, String>();
            aliasAndColumnNameAndTypes.put(alias, columnNameAndTypes);

            TableSpec tableSpec = aliasAndTableSpecs.get(alias);
            RowSpec rowSpec = tableSpec.getRowSpec();
            String[] columns = rowSpec.getColumnNames();
            String[] columnTypes = rowSpec.getColumnNativeTypes();
            for (int i = 0; i < columns.length; i++) {
                columnNameAndTypes.put(columns[i], columnTypes[i]);
            }
        }

        visitWhereClause(whereCondition, aliasAndTableSpecs, aliasAndColumnNameAndTypes,
                whereConditionList, context, false);

        context.remove(unClosedBracesKey);
        context.remove(prevNodeHolderKey);
        RowStatus rowStatus = (RowStatus) context.remove(rowStatusKey);
        if (rowStatus != null) {
            context.put(RowStatus.class.getName(), rowStatus);
        }

        Map<String, String> subQueries = (Map<String, String>) context.remove(selectSubQryListKey);
        if (subQueries != null) {
            for (String subQuery : subQueries.values()) {
                subQueriesToCache.add(subQuery);
            }
        }

        WhereClauseSpec whereClauseSpec = new WhereClauseSpec(whereCondition.toString(), context,
                subQueries);
        return whereClauseSpec;
    }

    protected void visitWhereClause(StringBuilder whereCondition,
            Map<String, TableSpec> aliasAndTableSpecs,
            Map<String, LinkedHashMap<String, String>> aliasAndColumnNameAndTypes, AST node,
            Map<String, Object> context, boolean isSelectSubQuery) throws FileManagerException,
            QueryParseException {
        boolean visitAlias = true;
        for (; node != null; node = node.getNextSibling()) {
            boolean visitChildren = true;

            final int type = node.getType();
            switch (type) {
                case RQLTokenTypes.SQL_STATEMENT:
                    break;

                case RQLTokenTypes.SELECT_EXPRESSION: {
                    if (isSelectSubQuery == false) {
                        HashMap<String, String> map = (HashMap<String, String>) context
                                .get(selectSubQryListKey);
                        if (map == null) {
                            map = new HashMap<String, String>();
                            context.put(selectSubQryListKey, map);
                        }

                        StringBuilder privateWhereBuilder = new StringBuilder();
                        /*
                         * Build the SubQuery without any
                         * expansion/interpretation.
                         */
                        visitWhereClause(privateWhereBuilder, aliasAndTableSpecs,
                                aliasAndColumnNameAndTypes, node, new HashMap<String, Object>(),
                                true);

                        String subSelect = privateWhereBuilder.toString();
                        // Remove trailing ")".
                        subSelect = subSelect.substring(0, subSelect.length() - 1);
                        String uniqueName = getRandomizedUniqueString(null);

                        String a = streamcruncher.innards.expression.Constants.VARIABLE_REFERENCE_PREFIX
                                + uniqueName;
                        map.put(a, subSelect);

                        String s = streamcruncher.innards.expression.Constants.VARIABLE_REWRITE_PREFIX
                                + uniqueName;

                        whereCondition.append(" ");
                        whereCondition.append(s);

                        visitChildren = false;
                        visitAlias = false;
                        break;
                    }
                }

                case RQLTokenTypes.SELECT_LIST:
                case RQLTokenTypes.TABLE_REFERENCE_LIST:
                case RQLTokenTypes.DISPLAYED_COLUMN:
                case RQLTokenTypes.EXP_SIMPLE:
                case RQLTokenTypes.CASE_EXPRESSION:
                case RQLTokenTypes.MONITOR_EXPRESSION:
                case RQLTokenTypes.SELECTED_TABLE:
                case RQLTokenTypes.FIRST_CLAUSE:
                case RQLTokenTypes.LIMIT_CLAUSE:
                case RQLTokenTypes.TABLE_SPEC:
                    break;

                case RQLTokenTypes.FILTER_SPEC:
                case RQLTokenTypes.PARTITION_SPEC_LIST:
                case RQLTokenTypes.CLONE_PARTITION_CLAUSE:
                    throw new QueryParseException(
                            "The Where-clause in a Partition cannot define Partitions");

                case RQLTokenTypes.ALIAS:
                    visitChildren = visitAlias;
                    break;

                case RQLTokenTypes.WHERE_CONDITION:
                    break;

                case RQLTokenTypes.ROW_STATUS_CLAUSE: {
                    // todo Need to clean this validation and messages since
                    // Where is used for all Queries - Correlation, InMemMaster

                    if (context.containsKey(rowStatusKey)) {
                        throw new QueryParseException(
                                "The Where-clause in a Partition cannot have more than one 'row_status' clause");
                    }

                    AST prevNodeHolder = (AST) context.get(prevNodeHolderKey);
                    if (prevNodeHolder != null) {
                        throw new QueryParseException(
                                createErrorPosString(prevNodeHolder)
                                        + "The Where-clause in a Partition must have as the first condition, the 'row_status' ("
                                        + RowStatus.NEW.name() + "/" + RowStatus.DEAD.name()
                                        + ") clause");
                    }

                    EnumMap<RowStatus, List<Integer>> dummyMap = new EnumMap<RowStatus, List<Integer>>(
                            RowStatus.class);
                    AtomicInteger dummyCounter = new AtomicInteger();

                    TableFQN fqn = null;
                    // This is only if there is one TableSpec.
                    if (aliasAndColumnNameAndTypes.size() == 1) {
                        TableSpec spec = aliasAndTableSpecs.entrySet().iterator().next().getValue();
                        fqn = new TableFQN(spec.getSchema(), spec.getName());
                    }
                    handleRowStatusClause(node, fqn, dummyMap, dummyCounter);
                    for (RowStatus rowStatus : dummyMap.keySet()) {
                        context.put(rowStatusKey, rowStatus);
                        break;
                    }

                    AST nextSibling = node.getNextSibling();
                    if (nextSibling != null) {
                        if (RQLTokenTypes.LITERAL_and != nextSibling.getType()) {
                            throw new QueryParseException(
                                    createErrorPosString(nextSibling)
                                            + "The Where-clause in a Partition must always start with the 'row_status' ("
                                            + RowStatus.NEW.name()
                                            + "/"
                                            + RowStatus.DEAD.name()
                                            + ") clause. The remaining part of the "
                                            + "expression (if any), must be followed an 'and' literal.");
                        }

                        // Skip the 'and' literal.
                        node = nextSibling;
                    }

                    visitChildren = false;
                    visitAlias = false;
                    break;
                }

                case RQLTokenTypes.POST_WHERE_CLAUSES:
                    break;

                case RQLTokenTypes.SEMI:
                    return;

                default: {
                    AST prevNodeHolder = (AST) context.get(prevNodeHolderKey);
                    if (prevNodeHolder != null) {
                        String msg = createErrorPosString(prevNodeHolder)
                                + "Column names in a Partition Where-clause must not be prefixed with the Schema name, except in Sub-Queries.";
                        if ((prevNodeHolder.getType() == RQLTokenTypes.IDENTIFIER && node.getType() == RQLTokenTypes.DOT)
                                || (prevNodeHolder.getType() == RQLTokenTypes.DOT && node.getType() == RQLTokenTypes.IDENTIFIER)) {
                            if (isSelectSubQuery == false) {
                                throw new QueryParseException(msg);
                            }
                        }
                        else {
                            whereCondition.append(" ");
                        }
                    }

                    String s = node.getText();
                    if (isSelectSubQuery == false) {
                        s = rewriteWhereClauseKeywords(node, aliasAndTableSpecs,
                                aliasAndColumnNameAndTypes, context);
                    }
                    whereCondition.append(s);
                }
            }

            context.put(prevNodeHolderKey, node);

            if (visitChildren && node.getFirstChild() != null) {
                visitWhereClause(whereCondition, aliasAndTableSpecs, aliasAndColumnNameAndTypes,
                        node.getFirstChild(), context, isSelectSubQuery);
            }
        }
    }

    /**
     * <p>
     * doc Performs OGNL specific conversions.
     * </p>
     * 
     * @param node
     * @param tableSpec
     * @param context
     * @return
     * @throws QueryParseException
     */
    protected String rewriteWhereClauseKeywords(AST node,
            Map<String, TableSpec> aliasAndTableSpecs,
            Map<String, LinkedHashMap<String, String>> aliasAndColumnNameAndTypes,
            Map<String, Object> context) throws QueryParseException {
        int type = node.getType();
        String text = node.getText();

        switch (type) {
            case RQLTokenTypes.ASTERISK:
            case RQLTokenTypes.DIVIDE:
            case RQLTokenTypes.MINUS:
            case RQLTokenTypes.MODULO:
            case RQLTokenTypes.PLUS: {
                break;
            }
            case RQLTokenTypes.EQ: {
                return "==";
            }
            case RQLTokenTypes.GE:
            case RQLTokenTypes.GT:
            case RQLTokenTypes.LE:
            case RQLTokenTypes.LT: {
                break;
            }
            case RQLTokenTypes.NOT_EQ: {
                return "!=";
            }
            case RQLTokenTypes.LITERAL_and: {
                return "&&";
            }
            case RQLTokenTypes.LITERAL_between: {
                // todo Handle in, exists, upper(), lower(), trim() etc.
                throw new QueryParseException(createErrorPosString(node)
                        + "The Between-clause is not supported.");
            }
            case RQLTokenTypes.LITERAL_exists: {
                throw new QueryParseException(createErrorPosString(node)
                        + "The Exists-clause is not supported in a Partition Where-clause.");
            }
            case RQLTokenTypes.LITERAL_in: {
                break;
            }
            case RQLTokenTypes.LITERAL_is: {
                /*
                 * Don't do anything now. Let the next node handle it. Ex: "is
                 * null" or "is not null".
                 */
                return "";
            }
            case RQLTokenTypes.LITERAL_like: {
                /*
                 * todo Support this. ALSO DOCUMENT limited/no functions support -
                 * trim etc
                 */
                throw new QueryParseException(createErrorPosString(node)
                        + "The Like-clause is not supported.");
            }
            case RQLTokenTypes.LITERAL_not: {
                AST previousNode = (AST) context.get(prevNodeHolderKey);
                if (previousNode != null && previousNode.getType() == RQLTokenTypes.LITERAL_is) {
                    return "!=";
                }

                // Ex: "not in".
                break;
            }
            case RQLTokenTypes.LITERAL_null: {
                AST previousNode = (AST) context.get(prevNodeHolderKey);
                if (previousNode != null && previousNode.getType() == RQLTokenTypes.LITERAL_is) {
                    return "== null";
                }

                return "null";
            }
            case RQLTokenTypes.LITERAL_or: {
                return "||";
            }
            case RQLTokenTypes.OPEN_PAREN: {
                AST previousNode = (AST) context.get(prevNodeHolderKey);
                if (previousNode != null && previousNode.getType() == RQLTokenTypes.LITERAL_in) {
                    if (node.getNextSibling() != null
                            && node.getNextSibling().getType() == RQLTokenTypes.SELECT_EXPRESSION) {
                        return "";
                    }

                    Stack<AtomicInteger> counter = (Stack<AtomicInteger>) context
                            .get(unClosedBracesKey);
                    if (counter == null) {
                        counter = new Stack<AtomicInteger>();
                        context.put(unClosedBracesKey, counter);
                    }
                    counter.push(new AtomicInteger(1));

                    return "{";
                }

                Stack<AtomicInteger> counter = (Stack<AtomicInteger>) context
                        .get(unClosedBracesKey);
                if (counter != null && counter.isEmpty() == false) {
                    /*
                     * Brace opens at a non-in clause, inside the context of an
                     * in-clause.
                     */
                    counter.peek().incrementAndGet();
                }

                break;
            }
            case RQLTokenTypes.CLOSE_PAREN: {
                AST previousNode = (AST) context.get(prevNodeHolderKey);
                if (previousNode != null
                        && previousNode.getType() == RQLTokenTypes.SELECT_EXPRESSION) {
                    return "";
                }

                Stack<AtomicInteger> counter = (Stack<AtomicInteger>) context
                        .get(unClosedBracesKey);
                if (counter != null && counter.isEmpty() == false) {
                    AtomicInteger ai = counter.peek();
                    ai.decrementAndGet();

                    if (ai.get() == 0) {
                        counter.pop();
                        return "}";
                    }
                }

                break;
            }
            case RQLTokenTypes.DOT: {
                AST nextNode = node.getNextSibling();
                if (nextNode != null && nextNode.getType() == RQLTokenTypes.IDENTIFIER) {
                    // Handled in Identifier.
                    return "";
                }
            }
            case RQLTokenTypes.LITERAL_current_timestamp:
            case RQLTokenTypes.IDENTIFIER: {
                String originalText = text;

                if (type == RQLTokenTypes.LITERAL_current_timestamp) {
                    text = streamcruncher.innards.expression.Constants.KEYWORD_CURRENT_TIMESTAMP;
                }

                // -----------

                String originalColumnText = null;

                AST nextNode = node.getNextSibling();
                if (nextNode != null && nextNode.getType() == RQLTokenTypes.DOT) {
                    nextNode = nextNode.getNextSibling();
                    if (nextNode != null && nextNode.getType() == RQLTokenTypes.IDENTIFIER) {
                        originalColumnText = nextNode.getText();
                    }
                }

                AST previousNode = (AST) context.get(prevNodeHolderKey);
                if (previousNode != null && previousNode.getType() == RQLTokenTypes.DOT) {
                    // Already handled.
                    return "";
                }

                // ------------

                String alias = aliasAndColumnNameAndTypes.containsKey(originalText) ? originalText
                        : null;
                if (alias == null && originalColumnText != null) {
                    // Alias.Column pattern, but the Alias is not valid.
                    throw new QueryParseException(createErrorPosString(node) + "The Alias: "
                            + originalText + " is not valid.");
                }

                String columnType = null;
                int columnPos = -1;

                if (alias != null) {
                    Map<String, String> nameAndTypes = aliasAndColumnNameAndTypes.get(alias);

                    columnType = nameAndTypes.get(originalColumnText);
                    if (columnType == null) {
                        throw new QueryParseException(createErrorPosString(nextNode)
                                + "The Column: " + originalColumnText + " is not valid.");
                    }

                    for (String key : nameAndTypes.keySet()) {
                        columnPos++;
                        if (key.equals(originalColumnText)) {
                            break;
                        }
                    }
                }
                else {
                    for (String key : aliasAndColumnNameAndTypes.keySet()) {
                        Map<String, String> nameAndTypes = aliasAndColumnNameAndTypes.get(key);

                        boolean present = nameAndTypes.containsKey(originalText);
                        if (columnType != null && present) {
                            throw new QueryParseException(createErrorPosString(node)
                                    + "The Column: " + originalText
                                    + " appears is more than one Partition. An alias "
                                    + "must be used to disambiguate the reference.");
                        }
                        if (present) {
                            columnType = nameAndTypes.get(originalText);

                            for (String s : nameAndTypes.keySet()) {
                                columnPos++;
                                if (s.equals(originalText)) {
                                    break;
                                }
                            }
                        }
                    }
                }

                if (columnType != null) {
                    text = createColumnVariableRef(alias, columnType, columnPos);

                    context.put(originalText, text);
                    return text;
                }

                // -------------

                String a = streamcruncher.innards.expression.Constants.VARIABLE_REFERENCE_PREFIX
                        + text;
                context.put(originalText, a);

                String s = streamcruncher.innards.expression.Constants.VARIABLE_REWRITE_PREFIX
                        + text;
                return s;
            }
            case RQLTokenTypes.QUOTED_STRING: {
                String s = "\"" + text.substring(1, text.length());
                s = s.substring(0, s.length() - 1) + "\"";
                return s;
            }
        }

        return text;
    }

    /**
     * @param alias
     *            Can be <code>null</code>
     * @param columnType
     * @param i
     * @return
     */
    protected String createColumnVariableRef(String alias, String columnType, int i) {
        String text = streamcruncher.innards.expression.Constants.KEYWORD_DATA_ROW_NAME;

        if (java.lang.Integer.class.getName().equals(columnType)) {
            text = text + java.lang.Integer.class.getSimpleName();
        }
        else if (java.lang.Long.class.getName().equals(columnType)) {
            text = text + java.lang.Long.class.getSimpleName();
        }
        else if (java.lang.Float.class.getName().equals(columnType)) {
            text = text + java.lang.Float.class.getSimpleName();
        }
        else if (java.lang.Double.class.getName().equals(columnType)) {
            text = text + java.lang.Double.class.getSimpleName();
        }
        else if (columnType.startsWith(java.lang.String.class.getName())) {
            text = text + java.lang.String.class.getSimpleName();
        }
        else if (java.sql.Timestamp.class.getName().equals(columnType)) {
            text = text + java.sql.Timestamp.class.getSimpleName();
        }

        text = text + streamcruncher.innards.expression.Constants.DATA_ROW_MARKER_START + i
                + streamcruncher.innards.expression.Constants.DATA_ROW_MARKER_END;

        return text;
    }

    /**
     * Converts the {@link RQLTokenTypes#ROW_STATUS_CONDITION} to an SQL
     * Fragment.
     * 
     * @param rowStatusClauseNode
     * @param defaultTable
     *            <code>null</code> if an Alias or FQN <b>has</b> to be
     *            provided - Query's main body.
     * @param map
     * @param counter
     * @return
     * @throws QueryParseException
     */
    protected String handleRowStatusClause(AST rowStatusClauseNode, TableFQN defaultTable,
            EnumMap<RowStatus, List<Integer>> map, AtomicInteger counter)
            throws QueryParseException {
        String schema = null;
        String tableOrAlias = null;

        AST innerNode = rowStatusClauseNode.getFirstChild();
        while (innerNode.getType() != RQLTokenTypes.ROW_STATUS_CONDITION) {
            if (innerNode.getType() != RQLTokenTypes.DOT) {
                if (schema == null) {
                    schema = innerNode.getText();
                }
                else {
                    tableOrAlias = innerNode.getText();
                }
            }

            innerNode = innerNode.getNextSibling();
        }

        // --------------

        String versionColumnName = null;

        if (schema != null && tableOrAlias == null) {
            tableOrAlias = schema;
            schema = null;

            TableFQN theTable = null;
            if (aliasAndFilteredTables.containsKey(tableOrAlias)
                    || cloneAliasesAndPartitionAliases.containsKey(tableOrAlias)) {
                FilteredTable filteredTable = aliasAndFilteredTables.get(tableOrAlias);
                if (filteredTable == null) {
                    String realPartitionAlias = cloneAliasesAndPartitionAliases.get(tableOrAlias);
                    filteredTable = aliasAndFilteredTables.get(realPartitionAlias);
                }

                theTable = extractTargetTableFQN(filteredTable);
            }
            else {
                theTable = new TableFQN(schema, tableOrAlias, null);
            }

            try {
                versionColumnName = getVersionColumnName(theTable);
            }
            catch (Exception e) {
                throw new QueryParseException(
                        createErrorPosString(rowStatusClauseNode)
                                + "'row_status' clause is supported only for Partitioned-Tables "
                                + "and they must be prefixed by an Alias, when used in the Query's main body.");
            }
        }

        boolean thisIsInChainedPartition = false;

        if (schema == null && tableOrAlias == null) {
            if (defaultTable == null) {
                throw new QueryParseException(createErrorPosString(rowStatusClauseNode)
                        + "All references to 'row_status' columns must be prefixed "
                        + "by an Alias, when used in the Query's main body.");
            }

            schema = defaultTable.getSchema();
            tableOrAlias = defaultTable.getName();

            /*
             * Then this row_status is now being used in a ChainedPartition, by
             * another Partition down the Chain. Not-Dead is not allowed,
             * because the Not-Dead rows from the Source Partition will cause
             * Index-Violations in the Destination Partition.
             */
            thisIsInChainedPartition = true;

            try {
                versionColumnName = getVersionColumnName(defaultTable);
            }
            catch (FileManagerException e) {
                throw new QueryParseException(e);
            }
        }

        String column = (schema == null) ? tableOrAlias : (schema + "." + tableOrAlias);
        column = column + "." + versionColumnName;

        // --------------

        // $
        innerNode = innerNode.getFirstChild();
        // row_status
        innerNode = innerNode.getNextSibling();
        // is
        innerNode = innerNode.getNextSibling();
        // new | (not)? dead
        innerNode = innerNode.getNextSibling();

        String condition = null;

        String status = innerNode.getText();
        RowStatus theType = null;
        if (RowStatus.NEW.name().equalsIgnoreCase(status)) {
            theType = RowStatus.NEW;

            condition = column + " = ?";
        }
        else if (RowStatus.DEAD.name().equalsIgnoreCase(status)) {
            theType = RowStatus.DEAD;

            condition = column + " = ?";
        }
        else {
            theType = RowStatus.NOT_DEAD;

            condition = "(" + column + " > 0 and " + column + " <= ?)";

            if (thisIsInChainedPartition) {
                throw new QueryParseException(
                        createErrorPosString(rowStatusClauseNode)
                                + "Only 'new' or 'dead' statuses are allowed for 'row_status' when used in a Chained-Partition.");
            }
        }

        List<Integer> position = map.get(theType);
        if (position == null) {
            position = new LinkedList<Integer>();
            map.put(theType, position);
        }
        position.add(counter.incrementAndGet());

        return condition;
    }

    protected TableFQN extractTargetTableFQN(FilteredTable filteredTable) {
        TableFQN theTable = null;

        if (filteredTable instanceof EncapsulatedPartitionedTable) {
            EncapsulatedPartitionedTable ept = (EncapsulatedPartitionedTable) filteredTable;
            ChainedPartitionedTable cpt = ept.getNextCPT();
            while (cpt != null) {
                theTable = cpt.getTargetTableFQN();
                cpt = cpt.getNextCPT();
            }
        }
        else {
            theTable = filteredTable.getTargetTableFQN();
        }

        return theTable;
    }

    /**
     * Copies the Alias of the source Table provided.
     * 
     * @param selectedTable
     * @return
     */
    protected TableFQN getFilterTableFQN(TableFQN selectedTable) {
        String origTableName = selectedTable.getName();
        String newTableName = getRandomizedUniqueString(origTableName);

        TableFQN newTable = new TableFQN(getSchema(), newTableName, selectedTable.getAlias());

        return newTable;
    }

    protected TableFQN handleMonitorExpression(AST monitorNode) throws QueryParseException,
            FileManagerException {
        // "alert"
        AST tmpNode = monitorNode.getFirstChild();

        // -----------

        // partition_column_name_list
        tmpNode = tmpNode.getNextSibling();

        HashMap<String, AlertColumnDef> aliasAndSrcColumns = new HashMap<String, AlertColumnDef>();
        LinkedHashMap<String, AlertColumnDef> alertColumnDefs = new LinkedHashMap<String, AlertColumnDef>();
        AST partitionColumnListNode = tmpNode.getFirstChild();
        while (partitionColumnListNode != null) {
            if (partitionColumnListNode.getType() == RQLTokenTypes.COMMA) {
                partitionColumnListNode = partitionColumnListNode.getNextSibling();
            }

            String partitionAlias = partitionColumnListNode.getText();

            // DOT
            partitionColumnListNode = partitionColumnListNode.getNextSibling();

            partitionColumnListNode = partitionColumnListNode.getNextSibling();
            String columnName = partitionColumnListNode.getText();

            // "as"
            partitionColumnListNode = partitionColumnListNode.getNextSibling();

            partitionColumnListNode = partitionColumnListNode.getNextSibling();
            String resultColumnName = partitionColumnListNode.getText();

            // -----------

            AlertColumnDef alertColumnDef = aliasAndSrcColumns.get(partitionAlias + "."
                    + columnName);
            if (alertColumnDef != null) {
                throw new QueryParseException("The source Column: " + partitionAlias + "."
                        + columnName + ", has already been defined before.");
            }

            alertColumnDef = alertColumnDefs.get(resultColumnName);
            if (alertColumnDef != null) {
                throw new QueryParseException("The result Column: " + resultColumnName
                        + " has already" + " been defined before. Only unique names are allowed.");
            }

            alertColumnDef = new AlertColumnDef(partitionAlias, columnName, resultColumnName);
            aliasAndSrcColumns.put(partitionAlias + "." + columnName, alertColumnDef);
            alertColumnDefs.put(resultColumnName, alertColumnDef);

            partitionColumnListNode = partitionColumnListNode.getNextSibling();
        }

        // -----------

        // using_list
        tmpNode = tmpNode.getNextSibling();

        // "using"
        AST usingListNode = tmpNode.getFirstChild();
        LinkedHashMap<String, PartitionResultDef> aliasAndPartitionDefs = new LinkedHashMap<String, PartitionResultDef>();
        usingListNode = usingListNode.getNextSibling();
        while (usingListNode != null) {
            if (usingListNode.getType() == RQLTokenTypes.USING_SPEC) {
                // partition_spec_list
                AST usingSpecNode = usingListNode.getFirstChild();
                TableFQN partitionedResult = handlePartitionList(usingSpecNode, true);

                // alias
                usingSpecNode = usingSpecNode.getNextSibling();

                // "correlate"
                usingSpecNode = usingSpecNode.getNextSibling();

                // "on"
                usingSpecNode = usingSpecNode.getNextSibling();

                usingSpecNode = usingSpecNode.getNextSibling();
                String correlationColumn = usingSpecNode.getText();

                PartitionResultDef resultDef = aliasAndPartitionDefs.get(partitionedResult
                        .getAlias());
                if (resultDef != null) {
                    throw new QueryParseException(createErrorPosString(usingSpecNode)
                            + "The Alias: " + partitionedResult.getAlias()
                            + " has already been defined.");
                }

                aliasAndPartitionDefs.put(partitionedResult.getAlias(), new PartitionResultDef(
                        partitionedResult, correlationColumn));
            }

            usingListNode = usingListNode.getNextSibling();
        }

        // -----------

        // present_list
        tmpNode = tmpNode.getNextSibling();

        // "when"
        AST presentListNode = tmpNode.getFirstChild();

        LinkedList<PresentSpec> presentSpecs = new LinkedList<PresentSpec>();

        presentListNode = presentListNode.getNextSibling();
        while (presentListNode != null) {
            if (presentListNode.getType() == RQLTokenTypes.PRESENT_SPEC) {
                // "present"
                AST presentSpecNode = presentListNode.getFirstChild();

                // "("
                presentSpecNode = presentSpecNode.getNextSibling();

                PresentSpec presentSpec = new PresentSpec();
                presentSpecNode = presentSpecNode.getNextSibling();
                AST prevSpecNode = null;
                while (presentSpecNode != null) {
                    if (presentSpecNode.getType() == RQLTokenTypes.IDENTIFIER) {
                        PresentSpecCondition condition = PresentSpecCondition.IN;
                        if (prevSpecNode != null
                                && prevSpecNode.getType() == RQLTokenTypes.LITERAL_not) {
                            condition = PresentSpecCondition.NOT_IN;
                        }

                        if (presentSpec.partitionAliasAndConditions.containsKey(presentSpecNode
                                .getText())) {
                            throw new QueryParseException(createErrorPosString(presentSpecNode)
                                    + "The Alias: " + presentSpecNode.getText()
                                    + " has already been defined.");
                        }
                        presentSpec.addCondition(presentSpecNode.getText(), condition);
                    }

                    prevSpecNode = presentSpecNode;
                    presentSpecNode = presentSpecNode.getNextSibling();
                }

                presentSpecs.add(presentSpec);
            }

            presentListNode = presentListNode.getNextSibling();
        }

        // -----------

        TableFQN tableFQN = createMonitorTable(alertColumnDefs.values(), aliasAndPartitionDefs
                .values(), presentSpecs);
        return tableFQN;
    }

    protected TableFQN createMonitorTable(Collection<AlertColumnDef> alertColumnDefs,
            Collection<PartitionResultDef> partitionDefs, Collection<PresentSpec> presentSpecs)
            throws QueryParseException {
        Map<String, Integer> sourceTblAndCorrIdPosition = new HashMap<String, Integer>();
        Map<String, Integer> sourceTblAndRowIdPosition = new HashMap<String, Integer>();

        HashMap<String, LinkedHashMap<String, String>> partitionAliasAndColumnSpec = new HashMap<String, LinkedHashMap<String, String>>();
        for (PartitionResultDef partitionResultDef : partitionDefs) {
            String alias = partitionResultDef.partitionResult.getAlias();

            LinkedHashMap<String, String> tableDef = new LinkedHashMap<String, String>();
            partitionAliasAndColumnSpec.put(alias, tableDef);

            RowSpec targetRowSpec = null;
            try {
                targetRowSpec = getRowSpec(partitionResultDef.partitionResult);
            }
            catch (FileManagerException e) {
                throw new QueryParseException(
                        "An error occurred while accessing the Partitioned-Table: "
                                + partitionResultDef.partitionResult + " in the Monitor-Expression",
                        e);
            }

            String[] colNames = targetRowSpec.getColumnNames();
            String[] colTypes = targetRowSpec.getColumnNativeTypes();
            for (int i = 0; i < colTypes.length; i++) {
                tableDef.put(colNames[i], colTypes[i]);

                if (partitionResultDef.correlationColumn.equals(colNames[i])) {
                    sourceTblAndCorrIdPosition.put(alias, i);
                }
            }

            sourceTblAndRowIdPosition.put(alias, targetRowSpec.getIdColumnPosition());
        }

        // --------

        Map<String, Integer[][]> sourceTblAndDestPosition = new HashMap<String, Integer[][]>();

        String[] alertTblColumnNames = new String[alertColumnDefs.size()];
        String[] alertTblColumnTypes = new String[alertTblColumnNames.length];
        int c = 0;
        for (Iterator<AlertColumnDef> iter = alertColumnDefs.iterator(); iter.hasNext();) {
            AlertColumnDef columnDef = iter.next();

            alertTblColumnNames[c] = columnDef.resultColumn;
            LinkedHashMap<String, String> colSpecs = partitionAliasAndColumnSpec
                    .get(columnDef.partitionAlias);
            if (colSpecs == null) {
                throw new QueryParseException("The source Table for the Alert column: "
                        + columnDef.resultColumn + ", does not exist: " + columnDef.partitionAlias);
            }

            alertTblColumnTypes[c] = colSpecs.get(columnDef.partitionSrcColumn);
            if (alertTblColumnTypes[c] == null) {
                throw new QueryParseException("The source column for the Alert column: "
                        + columnDef.resultColumn + ", does not exist: " + columnDef.partitionAlias
                        + "." + columnDef.partitionSrcColumn);
            }

            int j = 0;
            for (String srcCol : colSpecs.keySet()) {
                if (srcCol.equals(columnDef.partitionSrcColumn)) {
                    Integer[][] map = sourceTblAndDestPosition.get(columnDef.partitionAlias);
                    if (map == null) {
                        map = new Integer[0][2];
                    }

                    // In-efficient, but works OK for just a few Rows.
                    Integer[][] resizedMap = new Integer[map.length + 1][2];
                    int q = 0;
                    for (; q < map.length; q++) {
                        resizedMap[q][0] = map[q][0];
                        resizedMap[q][1] = map[q][1];
                    }
                    resizedMap[q][0] = j;
                    resizedMap[q][1] = c;

                    sourceTblAndDestPosition.put(columnDef.partitionAlias, resizedMap);
                    break;
                }

                j++;
            }

            c++;
        }

        // --------

        String correlationColumnType = null;
        for (PartitionResultDef resultDef : partitionDefs) {
            LinkedHashMap<String, String> colSpecs = partitionAliasAndColumnSpec
                    .get(resultDef.partitionResult.getAlias());

            if (colSpecs.containsKey(resultDef.correlationColumn) == false) {
                throw new QueryParseException("The Correlation column: "
                        + resultDef.correlationColumn + ", does not exist in: "
                        + resultDef.partitionResult.getAlias());
            }

            if (correlationColumnType == null) {
                correlationColumnType = colSpecs.get(resultDef.correlationColumn);
            }
            else if (correlationColumnType.equalsIgnoreCase(colSpecs
                    .get(resultDef.correlationColumn)) == false) {
                throw new QueryParseException("The Correlation column: "
                        + resultDef.correlationColumn + " on: "
                        + resultDef.partitionResult.getAlias()
                        + " has to be of the same type as the other Correlation columns");
            }
        }

        // --------

        String newTableName = getRandomizedUniqueString("al");
        String newSchemaName = getSchema();

        TableFQN newTableFQN = new TableFQN(newSchemaName, newTableName, newTableName);
        RowSpec newRowSpec = new RowSpec(alertTblColumnNames, alertTblColumnTypes);

        TableSpec newTableSpec = createTableSpec(newSchemaName, newTableName, newRowSpec, null,
                null, false, false);
        save(newTableSpec);

        // --------

        MatchSpec[] matchSpecs = new MatchSpec[presentSpecs.size()];

        int m = 0;
        for (PresentSpec spec : presentSpecs) {
            LinkedList<String> present = new LinkedList<String>();
            LinkedList<String> notPresent = new LinkedList<String>();

            for (String alias : spec.partitionAliasAndConditions.keySet()) {
                if (spec.partitionAliasAndConditions.get(alias) == PresentSpecCondition.IN) {
                    present.add(alias);
                }
                else {
                    notPresent.add(alias);
                }
            }

            String[] p = present.toArray(new String[present.size()]);
            String[] np = notPresent.toArray(new String[notPresent.size()]);

            matchSpecs[m] = new MatchSpec(p, np);
            m++;
        }

        // --------

        correlationSpec = new CorrelationSpec(sourceTblAndDestPosition, sourceTblAndCorrIdPosition,
                sourceTblAndRowIdPosition, newTableSpec, matchSpecs);

        windowsAndFiltersB4Correlation = aliasAndFilteredTables.size();

        // --------

        return newTableFQN;
    }

    protected static class AlertColumnDef {
        protected final String partitionAlias;

        protected final String partitionSrcColumn;

        protected final String resultColumn;

        public AlertColumnDef(String partitionAlias, String partitionSrcColumn, String resultColumn) {
            this.partitionAlias = partitionAlias;
            this.partitionSrcColumn = partitionSrcColumn;
            this.resultColumn = resultColumn;
        }
    }

    protected static class PartitionResultDef {
        protected final TableFQN partitionResult;

        protected final String correlationColumn;

        public PartitionResultDef(TableFQN partitionResult, String correlationColumn) {
            this.partitionResult = partitionResult;
            this.correlationColumn = correlationColumn;
        }
    }

    protected static enum PresentSpecCondition {
        IN, NOT_IN;
    }

    protected static class PresentSpec {
        protected final LinkedHashMap<String, PresentSpecCondition> partitionAliasAndConditions;

        public PresentSpec() {
            this.partitionAliasAndConditions = new LinkedHashMap<String, PresentSpecCondition>();
        }

        public void addCondition(String partitionAlias, PresentSpecCondition condition) {
            partitionAliasAndConditions.put(partitionAlias, condition);
        }
    }

    /**
     * @param selectedTable
     *            Should exist at the {@link ProviderManager}.
     * @param newTable
     *            Copy of the first Table except for creating a
     *            partitioned-table and also, see:
     *            {@link #createTableSpecUsingRowSpecForPartition(ProviderManager, TableFQN, RowSpec)}.
     * @throws FileManagerException
     */
    protected void createFilterTable(TableFQN selectedTable, TableFQN newTable)
            throws FileManagerException {
        TableSpec tableSpec = load(selectedTable);

        RowSpec rowSpec = tableSpec.getRowSpec();
        createTableSpecUsingRowSpecForPartition(newTable, rowSpec);
    }

    /**
     * Copy of the columns provided in the rowspec, except all the columns are
     * moved two places right with a new Id-Column at the first position,
     * Version-column at the second position and Timestamp-column at the third.
     * 
     * @param newTable
     * @param rowSpec
     * @throws FileManagerException
     */
    protected void createTableSpecUsingRowSpecForPartition(TableFQN newTable, RowSpec rowSpec) {
        String[] columns = rowSpec.getColumnNames();
        String[] columnTypes = rowSpec.getColumnNativeTypes();

        // -------------

        String[] newColumns = new String[columns.length + 3];
        newColumns[Constants.ID_COLUMN_POS] = createIdColumnName(newTable.getName());
        newColumns[Constants.TIMESTAMP_COLUMN_POS] = createTimestampColumnName(newTable.getName());
        newColumns[Constants.VERSION_COLUMN_POS] = createVersionColumnName(newTable.getName());

        for (int i = 3; i < newColumns.length; i++) {
            newColumns[i] = columns[i - 3];
        }

        String[] newColumnTypes = new String[columns.length + 3];
        newColumnTypes[Constants.ID_COLUMN_POS] = getIdColumnType();
        newColumnTypes[Constants.TIMESTAMP_COLUMN_POS] = getTimestampColumnType();
        newColumnTypes[Constants.VERSION_COLUMN_POS] = getVersionColumnType();

        for (int i = 3; i < newColumnTypes.length; i++) {
            newColumnTypes[i] = columnTypes[i - 3];
        }

        // -------------

        IndexSpec[] indexSpecs = null;

        rowSpec = new RowSpec(newColumns, newColumnTypes, Constants.ID_COLUMN_POS,
                Constants.TIMESTAMP_COLUMN_POS, Constants.VERSION_COLUMN_POS);

        IndexSpec uniqueIndex = createIndexSpec(newTable.getSchema(),
                createIndexName(newColumns[Constants.ID_COLUMN_POS]), newTable.getFQN(), true,
                newColumns[Constants.ID_COLUMN_POS], true);

        IndexSpec index = createIndexSpec(newTable.getSchema(),
                createIndexName(newColumns[Constants.VERSION_COLUMN_POS]), newTable.getFQN(),
                false, newColumns[Constants.VERSION_COLUMN_POS], true);

        indexSpecs = new IndexSpec[] { uniqueIndex, index };

        TableSpec newTableSpec = createTableSpec(newTable.getSchema(), newTable.getName(), rowSpec,
                indexSpecs, null, true, false);
        save(newTableSpec);
    }

    protected void replaceTableSpecWithVirtual(TableFQN tableFQN) throws FileManagerException {
        TableSpec tableSpec = load(tableFQN);

        RowSpec rowSpec = tableSpec.getRowSpec();
        TableSpec virtualSpec = createTableSpec(tableSpec.getSchema(), tableSpec.getName(),
                rowSpec, null, null, tableSpec.isPartitioned(), true);

        save(virtualSpec);
    }

    protected void convertRowSpecToNative(RowSpec rowSpec) throws QueryParseException {
        DDLHelper helper = getDDLHelper();

        String[] types = rowSpec.getColumnNativeTypes();
        for (int i = 0; i < types.length; i++) {
            String[] parts = types[i].split(RowSpec.INFO_SEPARATOR);

            String nativeType = helper.getNativeType(parts[0]);
            if (nativeType == null) {
                throw new QueryParseException("The Data type: " + types[i]
                        + ", is not supported. Supported Types: "
                        + Arrays.asList(helper.getJavaTypes()));
            }
            types[i] = nativeType;

            if (parts.length > 1) {
                String[] nameVal = parts[1].split(RowSpec.INFO_NAME_VALUE_SEPARATOR);
                if (nameVal.length > 1 && nameVal[0].equals(RowSpec.Info.SIZE.name())) {
                    types[i] = types[i] + "(" + nameVal[1] + ")";
                }
            }
        }
    }

    protected String getIdColumnName(TableFQN tableFQN) throws FileManagerException,
            QueryParseException {
        TableSpec tableSpec = load(tableFQN);

        RowSpec rowSpec = tableSpec.getRowSpec();
        String[] columns = rowSpec.getColumnNames();
        String[] columnTypes = rowSpec.getColumnNativeTypes();

        int pos = rowSpec.getIdColumnPosition();
        if (pos < 0) {
            throw new FileManagerException("The Table: " + tableFQN.getFQN()
                    + " does not have an Id-column.");
        }

        boolean found = false;
        String[] allowedTypes = new String[] { getIdColumnType(), getResultTableIdColumnType() };
        for (String type : allowedTypes) {
            if (columnTypes[pos].contains(type)) {
                found = true;
                break;
            }
        }

        if (found == false) {
            throw new QueryParseException("The Column: " + columns[pos]
                    + ", defined as the Id-column must be of one of these Types: "
                    + Arrays.asList(allowedTypes));
        }

        return columns[pos];
    }

    protected String getTimestampColumnName(TableFQN tableFQN) throws FileManagerException,
            QueryParseException {
        TableSpec tableSpec = load(tableFQN);

        RowSpec rowSpec = tableSpec.getRowSpec();
        String[] columns = rowSpec.getColumnNames();
        String[] columnTypes = rowSpec.getColumnNativeTypes();

        int pos = rowSpec.getTimestampColumnPosition();
        if (pos < 0) {
            throw new FileManagerException("The Table: " + tableFQN.getFQN()
                    + " does not have a Timestamp-column.");
        }

        if (columnTypes[pos].contains(getTimestampColumnType()) == false) {
            throw new QueryParseException("The Column: " + columns[pos]
                    + ", defined as the Timestamp-column must be of Type: "
                    + getTimestampColumnType());
        }

        return columns[pos];
    }

    protected String getVersionColumnName(TableFQN tableFQN) throws FileManagerException {
        TableSpec tableSpec = load(tableFQN);

        if (tableSpec.isPartitioned() == false) {
            throw new FileManagerException("The Table: " + tableFQN.getFQN()
                    + " is not a Partitioned Table and so has no 'row_status' column.");
        }

        RowSpec rowSpec = tableSpec.getRowSpec();
        String[] columns = rowSpec.getColumnNames();

        int pos = rowSpec.getVersionColumnPosition();
        if (pos < 0) {
            throw new FileManagerException("The Table: " + tableFQN.getFQN()
                    + " does not have a Version-column.");
        }

        return columns[pos];
    }

    protected abstract String getIdColumnType();

    protected String getResultTableIdColumnType() {
        return getIdColumnType();
    }

    protected abstract String getTimestampColumnType();

    protected abstract String getVersionColumnType();

    protected abstract TableSpec createTableSpec(String schema, String name, RowSpec rowSpec,
            IndexSpec[] indexSpecs, MiscSpec[] otherClauses, boolean partitioned, boolean virtual);

    protected abstract TableSpec createUnpartitionedTableSpec(String schema, String name,
            RowSpec rowSpec, IndexSpec[] indexSpecs, MiscSpec[] otherClauses);

    protected abstract IndexSpec createIndexSpec(String schema, String name, String tableFQN,
            boolean unique, String columnName, boolean ascending);

    protected abstract IndexSpec createIndexSpec(String schema, String name, String tableFQN,
            boolean unique, String[] columnNames, boolean[] ascending);

    protected String createIdColumnName(String tableName) {
        return "xx_" + getRandomizedUniqueString(tableName);
    }

    protected String getSchema() {
        DatabaseInterface dbInterface = Registry.getImplFor(DatabaseInterface.class);
        return dbInterface.getSchema();
    }

    protected abstract DBName getDBName();

    protected abstract DDLHelper getDDLHelper();

    protected String createTimestampColumnName(String tableName) {
        return "yy_" + getRandomizedUniqueString(tableName);
    }

    protected String createVersionColumnName(String tableName) {
        return "xx_" + getRandomizedUniqueString(tableName);
    }

    protected String createIndexName(String columnName) {
        return getRandomizedUniqueString(columnName) + "_ii";
    }

    protected String getFirstXChars(String name) {
        int len = Math.min(5, name.length());
        return name.substring(0, len);
    }

    protected String getRandomizedUniqueString(String prefix) {
        String str = "sc_";
        if (prefix != null) {
            str = getFirstXChars(prefix) + "_";
        }

        for (;;) {
            long l1 = Math.abs(System.nanoTime());
            long l2 = Math.abs(random.nextLong());
            long l3 = l1 ^ l2;
            String s = str + l3;

            if (usedUpRandomStrings.contains(s) == false) {
                str = s;

                usedUpRandomStrings.add(str);
                break;
            }
        }

        return str;
    }

    protected String extractAlias(AST aliasNode) {
        AST node = aliasNode.getFirstChild();

        // "as xyz" or "xyz" directly.
        if (node.getNextSibling() != null) {
            node = node.getNextSibling();
        }

        return node.getText();
    }

    protected String createErrorPosString(AST node) {
        return "Line: " + node.getLine() + ", Col: " + node.getColumn() + " ";
    }

    protected void save(TableSpec spec) {
        tableFQNAndSpecMap.put(spec.getFQN(), spec);
    }

    protected TableSpec loadCached(String fqn) throws FileManagerException {
        return tableFQNAndSpecMap.get(fqn);
    }

    protected TableSpec load(TableFQN tableFQN) throws FileManagerException {
        TableSpec spec = loadCached(tableFQN.getFQN());
        if (spec == null) {
            FileManager artifactManager = Registry.getImplFor(FileManager.class);
            spec = artifactManager.loadTableSpec(tableFQN.getSchema(), tableFQN.getName());
        }

        return spec;
    }
}
