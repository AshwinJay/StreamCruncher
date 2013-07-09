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

import antlr.ASTFactory;
import antlr.ASTPair;
import antlr.NoViableAltException;
import antlr.ParserSharedInputState;
import antlr.RecognitionException;
import antlr.Token;
import antlr.TokenBuffer;
import antlr.TokenStream;
import antlr.TokenStreamException;
import antlr.collections.AST;
import antlr.collections.impl.ASTArray;
import antlr.collections.impl.BitSet;

/**
 * Original file:
 * <p>
 * Oracle 7 SQL grammar (http://antlr.org/grammar/ORACLE-7-SQL) The BNF notation
 * found at http://cui.unige.ch/db-research/Enseignement/analyseinfo/BNFweb.html
 * may prove to be useful as a reference. Recent updates by bwiese@metasolv.com
 * ToDo: - Acknowledge all 7 parser ambiguities. - Proper support of floating
 * point numbers in lexer. A floating point number like '.9' will not be handled
 * correctly. I believe the way java.g handles this should be more than
 * sufficient, just has not been an issue yet. - Confirm that the lexer rules
 * NOT_EQ and GT work and are even necessary. - Deal with 3 lexer ambiguities,
 * unless they were taken care of when resolving the last to items. Changes:
 * 5/17/2002 - Start version using above mentioned BNF as base 5/22/2002 -
 * Changes include (any number is the line from pb.sql): - Allow 'not' after
 * 'or', not just at beginning of select. - (2500) Added concat char_function. -
 * (1796) Moved "user" from other_function to new rule pseudo_column and added
 * pseudo_column to variable. - (2624) Renamed 'constante_nonsigne' to literal,
 * and in this rule changed 'N' to NUMBER, where number was moved to the lexer
 * since 1e9 was being split into a number '1' and a literal 'e9', which was no
 * good. Also changed the "E" in number to lowercase for case insensitivity. The
 * NUMBER in the lexer needs work, see comment. - (3327) The unary PLUS & MINUS
 * is messed up, why would that be mixed up with the binary addition and
 * subtraction? The BNF above was helpful for the most part, but definitely
 * leaves a lot to be desired. It would be nice to send them all the fixes if I
 * could manage it, but with all the changes I am not sure that is pratical. -
 * (3414) Multiple "prior" conditions are allowed in a connect by, so I just
 * moved the optional "prior" qualifier to the first alt of logical_factor. -
 * (3419) Add support for a list as an expression, needed to support list
 * comparisons. - (3864) Add a bunch of keywords to identifiers, since "length"
 * is the name of a column and also a char_function. - (4849) Allow in
 * expression in exp_set, the 'simple_value' is not sufficient. - (5171) Make
 * precedence of logical_not to be just lower then parenthesises. - (5221) Allow
 * for an option "escape" followed by a QUOTED_STRING after a like and
 * match_string. - (5252) Need to allow complex expression for match_string when
 * used with the "like" comparison operator. - (5364) Make columns optional on
 * update_clause. - (2499) Allow for arbitrary parenthesises around select
 * statements by updating the select_statement rule to refer back to the
 * select_command rule, rather than going directly to the select_expression
 * rule. The way it was only allow for an optional set of parenthesises, not any
 * number.
 * </p>
 */
public class RQLParser extends antlr.LLkParser implements RQLTokenTypes {

    protected RQLParser(TokenBuffer tokenBuf, int k) {
        super(tokenBuf, k);
        tokenNames = _tokenNames;
        buildTokenTypeASTClassMap();
        astFactory = new ASTFactory(getTokenTypeToASTClassMap());
    }

    public RQLParser(TokenBuffer tokenBuf) {
        this(tokenBuf, 4);
    }

    protected RQLParser(TokenStream lexer, int k) {
        super(lexer, k);
        tokenNames = _tokenNames;
        buildTokenTypeASTClassMap();
        astFactory = new ASTFactory(getTokenTypeToASTClassMap());
    }

    public RQLParser(TokenStream lexer) {
        this(lexer, 4);
    }

    public RQLParser(ParserSharedInputState state) {
        super(state, 4);
        tokenNames = _tokenNames;
        buildTokenTypeASTClassMap();
        astFactory = new ASTFactory(getTokenTypeToASTClassMap());
    }

    public final void start_rule() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST start_rule_AST = null;

        sql_statement();
        astFactory.addASTChild(currentAST, returnAST);
        AST tmp1_AST = null;
        tmp1_AST = astFactory.create(LT(1));
        astFactory.addASTChild(currentAST, tmp1_AST);
        match(Token.EOF_TYPE);
        start_rule_AST = (AST) currentAST.root;
        returnAST = start_rule_AST;
    }

    public final void sql_statement() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST sql_statement_AST = null;

        select_expression();
        astFactory.addASTChild(currentAST, returnAST);
        AST tmp2_AST = null;
        tmp2_AST = astFactory.create(LT(1));
        astFactory.addASTChild(currentAST, tmp2_AST);
        match(SEMI);
        if (inputState.guessing == 0) {
            sql_statement_AST = (AST) currentAST.root;
            sql_statement_AST = (AST) astFactory.make((new ASTArray(2)).add(
                    astFactory.create(SQL_STATEMENT, "sql_statement")).add(sql_statement_AST));
            currentAST.root = sql_statement_AST;
            currentAST.child = sql_statement_AST != null
                    && sql_statement_AST.getFirstChild() != null ? sql_statement_AST
                    .getFirstChild() : sql_statement_AST;
            currentAST.advanceChildToEnd();
        }
        sql_statement_AST = (AST) currentAST.root;
        returnAST = sql_statement_AST;
    }

    public final void select_expression() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST select_expression_AST = null;

        AST tmp3_AST = null;
        tmp3_AST = astFactory.create(LT(1));
        astFactory.addASTChild(currentAST, tmp3_AST);
        match(LITERAL_select);
        {
            switch (LA(1)) {
                case LITERAL_first: {
                    first_clause();
                    astFactory.addASTChild(currentAST, returnAST);
                    break;
                }
                case LITERAL_all:
                case LITERAL_distinct:
                case ASTERISK:
                case LITERAL_case:
                case LITERAL_self:
                case PLUS:
                case MINUS:
                case OPEN_PAREN:
                case NUMBER:
                case QUOTED_STRING:
                case LITERAL_null:
                case LITERAL_avg:
                case LITERAL_count:
                case LITERAL_max:
                case LITERAL_min:
                case LITERAL_stddev:
                case LITERAL_sum:
                case LITERAL_variance:
                case LITERAL_filter:
                case LITERAL_using:
                case LITERAL_partition:
                case LITERAL_by:
                case IDENTIFIER:
                case LITERAL_store:
                case LITERAL_random:
                case LITERAL_lowest:
                case LITERAL_highest:
                case LITERAL_with:
                case LITERAL_group:
                case LITERAL_pinned:
                case LITERAL_geomean:
                case LITERAL_kurtosis:
                case LITERAL_median:
                case LITERAL_skewness:
                case LITERAL_sumsq:
                case LITERAL_diff:
                case LITERAL_custom:
                case LITERAL_entrance:
                case LITERAL_only:
                case LITERAL_last:
                case LITERAL_latest:
                case LITERAL_milliseconds:
                case LITERAL_seconds:
                case LITERAL_minutes:
                case LITERAL_hours:
                case LITERAL_days:
                case LITERAL_alert:
                case LITERAL_correlate:
                case LITERAL_present:
                case LITERAL_is:
                case LITERAL_row_status:
                case LITERAL_new:
                case LITERAL_dead:
                case LITERAL_union:
                case LITERAL_minus:
                case LITERAL_except:
                case LITERAL_intersect:
                case LITERAL_limit:
                case LITERAL_offset:
                case LITERAL_current_timestamp:
                case LITERAL_of: {
                    break;
                }
                default: {
                    throw new NoViableAltException(LT(1), getFilename());
                }
            }
        }
        {
            if ((LA(1) == LITERAL_all) && (_tokenSet_0.member(LA(2)))
                    && (_tokenSet_1.member(LA(3))) && (_tokenSet_2.member(LA(4)))) {
                AST tmp4_AST = null;
                tmp4_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp4_AST);
                match(LITERAL_all);
            }
            else if ((LA(1) == LITERAL_distinct)) {
                AST tmp5_AST = null;
                tmp5_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp5_AST);
                match(LITERAL_distinct);
            }
            else if ((_tokenSet_0.member(LA(1))) && (_tokenSet_1.member(LA(2)))
                    && (_tokenSet_2.member(LA(3))) && (_tokenSet_3.member(LA(4)))) {
            }
            else {
                throw new NoViableAltException(LT(1), getFilename());
            }

        }
        select_list();
        astFactory.addASTChild(currentAST, returnAST);
        AST tmp6_AST = null;
        tmp6_AST = astFactory.create(LT(1));
        astFactory.addASTChild(currentAST, tmp6_AST);
        match(LITERAL_from);
        data_source();
        astFactory.addASTChild(currentAST, returnAST);
        {
            switch (LA(1)) {
                case LITERAL_where: {
                    AST tmp7_AST = null;
                    tmp7_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp7_AST);
                    match(LITERAL_where);
                    where_condition();
                    astFactory.addASTChild(currentAST, returnAST);
                    break;
                }
                case SEMI:
                case CLOSE_PAREN:
                case LITERAL_group:
                case LITERAL_union:
                case LITERAL_minus:
                case LITERAL_except:
                case LITERAL_intersect:
                case LITERAL_order:
                case LITERAL_limit: {
                    break;
                }
                default: {
                    throw new NoViableAltException(LT(1), getFilename());
                }
            }
        }
        post_where_clauses();
        astFactory.addASTChild(currentAST, returnAST);
        if (inputState.guessing == 0) {
            select_expression_AST = (AST) currentAST.root;
            select_expression_AST = (AST) astFactory.make((new ASTArray(2)).add(
                    astFactory.create(SELECT_EXPRESSION, "select_expression")).add(
                    select_expression_AST));
            currentAST.root = select_expression_AST;
            currentAST.child = select_expression_AST != null
                    && select_expression_AST.getFirstChild() != null ? select_expression_AST
                    .getFirstChild() : select_expression_AST;
            currentAST.advanceChildToEnd();
        }
        select_expression_AST = (AST) currentAST.root;
        returnAST = select_expression_AST;
    }

    public final void first_clause() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST first_clause_AST = null;

        {
            AST tmp8_AST = null;
            tmp8_AST = astFactory.create(LT(1));
            astFactory.addASTChild(currentAST, tmp8_AST);
            match(LITERAL_first);
            AST tmp9_AST = null;
            tmp9_AST = astFactory.create(LT(1));
            astFactory.addASTChild(currentAST, tmp9_AST);
            match(NUMBER);
        }
        if (inputState.guessing == 0) {
            first_clause_AST = (AST) currentAST.root;
            first_clause_AST = (AST) astFactory.make((new ASTArray(2)).add(
                    astFactory.create(FIRST_CLAUSE, "first_clause")).add(first_clause_AST));
            currentAST.root = first_clause_AST;
            currentAST.child = first_clause_AST != null && first_clause_AST.getFirstChild() != null ? first_clause_AST
                    .getFirstChild()
                    : first_clause_AST;
            currentAST.advanceChildToEnd();
        }
        first_clause_AST = (AST) currentAST.root;
        returnAST = first_clause_AST;
    }

    public final void select_list() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST select_list_AST = null;

        {
            switch (LA(1)) {
                case LITERAL_all:
                case LITERAL_case:
                case LITERAL_self:
                case PLUS:
                case MINUS:
                case OPEN_PAREN:
                case NUMBER:
                case QUOTED_STRING:
                case LITERAL_null:
                case LITERAL_avg:
                case LITERAL_count:
                case LITERAL_max:
                case LITERAL_min:
                case LITERAL_stddev:
                case LITERAL_sum:
                case LITERAL_variance:
                case LITERAL_filter:
                case LITERAL_using:
                case LITERAL_partition:
                case LITERAL_by:
                case IDENTIFIER:
                case LITERAL_store:
                case LITERAL_random:
                case LITERAL_lowest:
                case LITERAL_highest:
                case LITERAL_with:
                case LITERAL_group:
                case LITERAL_pinned:
                case LITERAL_geomean:
                case LITERAL_kurtosis:
                case LITERAL_median:
                case LITERAL_skewness:
                case LITERAL_sumsq:
                case LITERAL_diff:
                case LITERAL_custom:
                case LITERAL_entrance:
                case LITERAL_only:
                case LITERAL_last:
                case LITERAL_latest:
                case LITERAL_milliseconds:
                case LITERAL_seconds:
                case LITERAL_minutes:
                case LITERAL_hours:
                case LITERAL_days:
                case LITERAL_alert:
                case LITERAL_correlate:
                case LITERAL_present:
                case LITERAL_is:
                case LITERAL_row_status:
                case LITERAL_new:
                case LITERAL_dead:
                case LITERAL_union:
                case LITERAL_minus:
                case LITERAL_except:
                case LITERAL_intersect:
                case LITERAL_limit:
                case LITERAL_offset:
                case LITERAL_current_timestamp:
                case LITERAL_of: {
                    displayed_column();
                    astFactory.addASTChild(currentAST, returnAST);
                    {
                        _loop18: do {
                            if ((LA(1) == COMMA)) {
                                AST tmp10_AST = null;
                                tmp10_AST = astFactory.create(LT(1));
                                astFactory.addASTChild(currentAST, tmp10_AST);
                                match(COMMA);
                                displayed_column();
                                astFactory.addASTChild(currentAST, returnAST);
                            }
                            else {
                                break _loop18;
                            }

                        } while (true);
                    }
                    break;
                }
                case ASTERISK: {
                    AST tmp11_AST = null;
                    tmp11_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp11_AST);
                    match(ASTERISK);
                    break;
                }
                default: {
                    throw new NoViableAltException(LT(1), getFilename());
                }
            }
        }
        if (inputState.guessing == 0) {
            select_list_AST = (AST) currentAST.root;
            select_list_AST = (AST) astFactory.make((new ASTArray(2)).add(
                    astFactory.create(SELECT_LIST, "select_list")).add(select_list_AST));
            currentAST.root = select_list_AST;
            currentAST.child = select_list_AST != null && select_list_AST.getFirstChild() != null ? select_list_AST
                    .getFirstChild()
                    : select_list_AST;
            currentAST.advanceChildToEnd();
        }
        select_list_AST = (AST) currentAST.root;
        returnAST = select_list_AST;
    }

    public final void data_source() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST data_source_AST = null;

        {
            boolean synPredMatched10 = false;
            if (((LA(1) == LITERAL_alert) && (LA(2) == IDENTIFIER) && (LA(3) == DOT))) {
                int _m10 = mark();
                synPredMatched10 = true;
                inputState.guessing++;
                try {
                    {
                        monitor_expression();
                    }
                }
                catch (RecognitionException pe) {
                    synPredMatched10 = false;
                }
                rewind(_m10);
                inputState.guessing--;
            }
            if (synPredMatched10) {
                monitor_expression();
                astFactory.addASTChild(currentAST, returnAST);
            }
            else {
                boolean synPredMatched12 = false;
                if (((_tokenSet_4.member(LA(1))) && (_tokenSet_5.member(LA(2))) && (_tokenSet_6
                        .member(LA(3))))) {
                    int _m12 = mark();
                    synPredMatched12 = true;
                    inputState.guessing++;
                    try {
                        {
                            table_reference_list();
                        }
                    }
                    catch (RecognitionException pe) {
                        synPredMatched12 = false;
                    }
                    rewind(_m12);
                    inputState.guessing--;
                }
                if (synPredMatched12) {
                    table_reference_list();
                    astFactory.addASTChild(currentAST, returnAST);
                }
                else {
                    throw new NoViableAltException(LT(1), getFilename());
                }
            }
        }
        data_source_AST = (AST) currentAST.root;
        returnAST = data_source_AST;
    }

    public final void where_condition() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST where_condition_AST = null;

        condition();
        astFactory.addASTChild(currentAST, returnAST);
        if (inputState.guessing == 0) {
            where_condition_AST = (AST) currentAST.root;
            where_condition_AST = (AST) astFactory
                    .make((new ASTArray(2)).add(
                            astFactory.create(WHERE_CONDITION, "where_condition")).add(
                            where_condition_AST));
            currentAST.root = where_condition_AST;
            currentAST.child = where_condition_AST != null
                    && where_condition_AST.getFirstChild() != null ? where_condition_AST
                    .getFirstChild() : where_condition_AST;
            currentAST.advanceChildToEnd();
        }
        where_condition_AST = (AST) currentAST.root;
        returnAST = where_condition_AST;
    }

    public final void post_where_clauses() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST post_where_clauses_AST = null;

        {
            switch (LA(1)) {
                case LITERAL_group: {
                    group_clause();
                    astFactory.addASTChild(currentAST, returnAST);
                    break;
                }
                case SEMI:
                case CLOSE_PAREN:
                case LITERAL_union:
                case LITERAL_minus:
                case LITERAL_except:
                case LITERAL_intersect:
                case LITERAL_order:
                case LITERAL_limit: {
                    break;
                }
                default: {
                    throw new NoViableAltException(LT(1), getFilename());
                }
            }
        }
        {
            switch (LA(1)) {
                case LITERAL_union:
                case LITERAL_minus:
                case LITERAL_except:
                case LITERAL_intersect: {
                    combine_clause();
                    astFactory.addASTChild(currentAST, returnAST);
                    break;
                }
                case SEMI:
                case CLOSE_PAREN:
                case LITERAL_order:
                case LITERAL_limit: {
                    break;
                }
                default: {
                    throw new NoViableAltException(LT(1), getFilename());
                }
            }
        }
        {
            boolean synPredMatched43 = false;
            if (((LA(1) == LITERAL_order) && (LA(2) == LITERAL_by) && (_tokenSet_7.member(LA(3))) && (_tokenSet_8
                    .member(LA(4))))) {
                int _m43 = mark();
                synPredMatched43 = true;
                inputState.guessing++;
                try {
                    {
                        order_clause();
                    }
                }
                catch (RecognitionException pe) {
                    synPredMatched43 = false;
                }
                rewind(_m43);
                inputState.guessing--;
            }
            if (synPredMatched43) {
                order_clause();
                astFactory.addASTChild(currentAST, returnAST);
            }
            else if ((_tokenSet_9.member(LA(1))) && (_tokenSet_10.member(LA(2)))
                    && (_tokenSet_11.member(LA(3))) && (_tokenSet_12.member(LA(4)))) {
            }
            else {
                throw new NoViableAltException(LT(1), getFilename());
            }

        }
        {
            boolean synPredMatched46 = false;
            if (((LA(1) == LITERAL_limit) && (LA(2) == NUMBER) && (_tokenSet_13.member(LA(3))) && (_tokenSet_10
                    .member(LA(4))))) {
                int _m46 = mark();
                synPredMatched46 = true;
                inputState.guessing++;
                try {
                    {
                        limit_clause();
                    }
                }
                catch (RecognitionException pe) {
                    synPredMatched46 = false;
                }
                rewind(_m46);
                inputState.guessing--;
            }
            if (synPredMatched46) {
                limit_clause();
                astFactory.addASTChild(currentAST, returnAST);
            }
            else if ((_tokenSet_9.member(LA(1))) && (_tokenSet_10.member(LA(2)))
                    && (_tokenSet_11.member(LA(3))) && (_tokenSet_12.member(LA(4)))) {
            }
            else {
                throw new NoViableAltException(LT(1), getFilename());
            }

        }
        if (inputState.guessing == 0) {
            post_where_clauses_AST = (AST) currentAST.root;
            post_where_clauses_AST = (AST) astFactory.make((new ASTArray(2)).add(
                    astFactory.create(POST_WHERE_CLAUSES, "post_where_clauses")).add(
                    post_where_clauses_AST));
            currentAST.root = post_where_clauses_AST;
            currentAST.child = post_where_clauses_AST != null
                    && post_where_clauses_AST.getFirstChild() != null ? post_where_clauses_AST
                    .getFirstChild() : post_where_clauses_AST;
            currentAST.advanceChildToEnd();
        }
        post_where_clauses_AST = (AST) currentAST.root;
        returnAST = post_where_clauses_AST;
    }

    public final void monitor_expression() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST monitor_expression_AST = null;

        AST tmp12_AST = null;
        tmp12_AST = astFactory.create(LT(1));
        astFactory.addASTChild(currentAST, tmp12_AST);
        match(LITERAL_alert);
        partition_column_name_list();
        astFactory.addASTChild(currentAST, returnAST);
        using_list();
        astFactory.addASTChild(currentAST, returnAST);
        present_list();
        astFactory.addASTChild(currentAST, returnAST);
        if (inputState.guessing == 0) {
            monitor_expression_AST = (AST) currentAST.root;
            monitor_expression_AST = (AST) astFactory.make((new ASTArray(2)).add(
                    astFactory.create(MONITOR_EXPRESSION, "monitor_expression")).add(
                    monitor_expression_AST));
            currentAST.root = monitor_expression_AST;
            currentAST.child = monitor_expression_AST != null
                    && monitor_expression_AST.getFirstChild() != null ? monitor_expression_AST
                    .getFirstChild() : monitor_expression_AST;
            currentAST.advanceChildToEnd();
        }
        monitor_expression_AST = (AST) currentAST.root;
        returnAST = monitor_expression_AST;
    }

    public final void table_reference_list() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST table_reference_list_AST = null;

        selected_table();
        astFactory.addASTChild(currentAST, returnAST);
        {
            _loop21: do {
                if ((LA(1) == COMMA)) {
                    AST tmp13_AST = null;
                    tmp13_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp13_AST);
                    match(COMMA);
                    selected_table();
                    astFactory.addASTChild(currentAST, returnAST);
                }
                else {
                    break _loop21;
                }

            } while (true);
        }
        {
            switch (LA(1)) {
                case LITERAL_left:
                case LITERAL_right:
                case LITERAL_inner:
                case LITERAL_join: {
                    join_expression();
                    astFactory.addASTChild(currentAST, returnAST);
                    break;
                }
                case SEMI:
                case LITERAL_where:
                case CLOSE_PAREN:
                case LITERAL_group:
                case LITERAL_union:
                case LITERAL_minus:
                case LITERAL_except:
                case LITERAL_intersect:
                case LITERAL_order:
                case LITERAL_limit: {
                    break;
                }
                default: {
                    throw new NoViableAltException(LT(1), getFilename());
                }
            }
        }
        if (inputState.guessing == 0) {
            table_reference_list_AST = (AST) currentAST.root;
            table_reference_list_AST = (AST) astFactory.make((new ASTArray(2)).add(
                    astFactory.create(TABLE_REFERENCE_LIST, "table_reference_list")).add(
                    table_reference_list_AST));
            currentAST.root = table_reference_list_AST;
            currentAST.child = table_reference_list_AST != null
                    && table_reference_list_AST.getFirstChild() != null ? table_reference_list_AST
                    .getFirstChild() : table_reference_list_AST;
            currentAST.advanceChildToEnd();
        }
        table_reference_list_AST = (AST) currentAST.root;
        returnAST = table_reference_list_AST;
    }

    public final void displayed_column() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST displayed_column_AST = null;

        {
            boolean synPredMatched51 = false;
            if (((_tokenSet_14.member(LA(1))) && (LA(2) == DOT) && (_tokenSet_15.member(LA(3))) && (LA(4) == LITERAL_from
                    || LA(4) == COMMA || LA(4) == DOT))) {
                int _m51 = mark();
                synPredMatched51 = true;
                inputState.guessing++;
                try {
                    {
                        {
                            if ((_tokenSet_14.member(LA(1))) && (LA(2) == DOT)
                                    && (_tokenSet_14.member(LA(3)))) {
                                schema_name();
                                match(DOT);
                            }
                            else if ((_tokenSet_14.member(LA(1))) && (LA(2) == DOT)
                                    && (LA(3) == ASTERISK)) {
                            }
                            else {
                                throw new NoViableAltException(LT(1), getFilename());
                            }

                        }
                        table_name();
                        match(DOT);
                        match(ASTERISK);
                    }
                }
                catch (RecognitionException pe) {
                    synPredMatched51 = false;
                }
                rewind(_m51);
                inputState.guessing--;
            }
            if (synPredMatched51) {
                {
                    {
                        if ((_tokenSet_14.member(LA(1))) && (LA(2) == DOT)
                                && (_tokenSet_14.member(LA(3)))) {
                            schema_name();
                            astFactory.addASTChild(currentAST, returnAST);
                            AST tmp14_AST = null;
                            tmp14_AST = astFactory.create(LT(1));
                            astFactory.addASTChild(currentAST, tmp14_AST);
                            match(DOT);
                        }
                        else if ((_tokenSet_14.member(LA(1))) && (LA(2) == DOT)
                                && (LA(3) == ASTERISK)) {
                        }
                        else {
                            throw new NoViableAltException(LT(1), getFilename());
                        }

                    }
                    table_name();
                    astFactory.addASTChild(currentAST, returnAST);
                    AST tmp15_AST = null;
                    tmp15_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp15_AST);
                    match(DOT);
                    AST tmp16_AST = null;
                    tmp16_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp16_AST);
                    match(ASTERISK);
                }
            }
            else if ((_tokenSet_7.member(LA(1))) && (_tokenSet_16.member(LA(2)))
                    && (_tokenSet_17.member(LA(3))) && (_tokenSet_18.member(LA(4)))) {
                {
                    exp_simple();
                    astFactory.addASTChild(currentAST, returnAST);
                    {
                        switch (LA(1)) {
                            case LITERAL_all:
                            case LITERAL_self:
                            case LITERAL_as:
                            case QUOTED_STRING:
                            case LITERAL_avg:
                            case LITERAL_count:
                            case LITERAL_max:
                            case LITERAL_min:
                            case LITERAL_stddev:
                            case LITERAL_sum:
                            case LITERAL_variance:
                            case LITERAL_filter:
                            case LITERAL_using:
                            case LITERAL_partition:
                            case LITERAL_by:
                            case IDENTIFIER:
                            case LITERAL_store:
                            case LITERAL_random:
                            case LITERAL_lowest:
                            case LITERAL_highest:
                            case LITERAL_with:
                            case LITERAL_group:
                            case LITERAL_pinned:
                            case LITERAL_geomean:
                            case LITERAL_kurtosis:
                            case LITERAL_median:
                            case LITERAL_skewness:
                            case LITERAL_sumsq:
                            case LITERAL_diff:
                            case LITERAL_custom:
                            case LITERAL_entrance:
                            case LITERAL_only:
                            case LITERAL_last:
                            case LITERAL_latest:
                            case LITERAL_milliseconds:
                            case LITERAL_seconds:
                            case LITERAL_minutes:
                            case LITERAL_hours:
                            case LITERAL_days:
                            case LITERAL_alert:
                            case LITERAL_correlate:
                            case LITERAL_present:
                            case LITERAL_is:
                            case LITERAL_row_status:
                            case LITERAL_new:
                            case LITERAL_dead:
                            case LITERAL_union:
                            case LITERAL_minus:
                            case LITERAL_except:
                            case LITERAL_intersect:
                            case LITERAL_limit:
                            case LITERAL_offset:
                            case LITERAL_current_timestamp:
                            case LITERAL_of: {
                                alias();
                                astFactory.addASTChild(currentAST, returnAST);
                                break;
                            }
                            case LITERAL_from:
                            case COMMA: {
                                break;
                            }
                            default: {
                                throw new NoViableAltException(LT(1), getFilename());
                            }
                        }
                    }
                }
            }
            else if ((LA(1) == LITERAL_case)) {
                {
                    case_expression();
                    astFactory.addASTChild(currentAST, returnAST);
                    {
                        switch (LA(1)) {
                            case LITERAL_all:
                            case LITERAL_self:
                            case LITERAL_as:
                            case QUOTED_STRING:
                            case LITERAL_avg:
                            case LITERAL_count:
                            case LITERAL_max:
                            case LITERAL_min:
                            case LITERAL_stddev:
                            case LITERAL_sum:
                            case LITERAL_variance:
                            case LITERAL_filter:
                            case LITERAL_using:
                            case LITERAL_partition:
                            case LITERAL_by:
                            case IDENTIFIER:
                            case LITERAL_store:
                            case LITERAL_random:
                            case LITERAL_lowest:
                            case LITERAL_highest:
                            case LITERAL_with:
                            case LITERAL_group:
                            case LITERAL_pinned:
                            case LITERAL_geomean:
                            case LITERAL_kurtosis:
                            case LITERAL_median:
                            case LITERAL_skewness:
                            case LITERAL_sumsq:
                            case LITERAL_diff:
                            case LITERAL_custom:
                            case LITERAL_entrance:
                            case LITERAL_only:
                            case LITERAL_last:
                            case LITERAL_latest:
                            case LITERAL_milliseconds:
                            case LITERAL_seconds:
                            case LITERAL_minutes:
                            case LITERAL_hours:
                            case LITERAL_days:
                            case LITERAL_alert:
                            case LITERAL_correlate:
                            case LITERAL_present:
                            case LITERAL_is:
                            case LITERAL_row_status:
                            case LITERAL_new:
                            case LITERAL_dead:
                            case LITERAL_union:
                            case LITERAL_minus:
                            case LITERAL_except:
                            case LITERAL_intersect:
                            case LITERAL_limit:
                            case LITERAL_offset:
                            case LITERAL_current_timestamp:
                            case LITERAL_of: {
                                alias();
                                astFactory.addASTChild(currentAST, returnAST);
                                break;
                            }
                            case LITERAL_from:
                            case COMMA: {
                                break;
                            }
                            default: {
                                throw new NoViableAltException(LT(1), getFilename());
                            }
                        }
                    }
                }
            }
            else {
                throw new NoViableAltException(LT(1), getFilename());
            }

        }
        if (inputState.guessing == 0) {
            displayed_column_AST = (AST) currentAST.root;
            displayed_column_AST = (AST) astFactory.make((new ASTArray(2)).add(
                    astFactory.create(DISPLAYED_COLUMN, "displayed_column")).add(
                    displayed_column_AST));
            currentAST.root = displayed_column_AST;
            currentAST.child = displayed_column_AST != null
                    && displayed_column_AST.getFirstChild() != null ? displayed_column_AST
                    .getFirstChild() : displayed_column_AST;
            currentAST.advanceChildToEnd();
        }
        displayed_column_AST = (AST) currentAST.root;
        returnAST = displayed_column_AST;
    }

    public final void selected_table() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST selected_table_AST = null;

        if ((_tokenSet_14.member(LA(1))) && (LA(2) == DOT || LA(2) == OPEN_PAREN)
                && (_tokenSet_14.member(LA(3))) && (LA(4) == OPEN_PAREN || LA(4) == LITERAL_using)) {
            {
                filter_spec();
                astFactory.addASTChild(currentAST, returnAST);
                alias();
                astFactory.addASTChild(currentAST, returnAST);
            }
            if (inputState.guessing == 0) {
                selected_table_AST = (AST) currentAST.root;
                selected_table_AST = (AST) astFactory.make((new ASTArray(2)).add(
                        astFactory.create(SELECTED_TABLE, "selected_table"))
                        .add(selected_table_AST));
                currentAST.root = selected_table_AST;
                currentAST.child = selected_table_AST != null
                        && selected_table_AST.getFirstChild() != null ? selected_table_AST
                        .getFirstChild() : selected_table_AST;
                currentAST.advanceChildToEnd();
            }
            selected_table_AST = (AST) currentAST.root;
        }
        else if ((_tokenSet_14.member(LA(1))) && (LA(2) == DOT || LA(2) == OPEN_PAREN)
                && (_tokenSet_14.member(LA(3)))
                && (LA(4) == OPEN_PAREN || LA(4) == LITERAL_by || LA(4) == LITERAL_store)) {
            {
                partition_spec_list();
                astFactory.addASTChild(currentAST, returnAST);
                alias();
                astFactory.addASTChild(currentAST, returnAST);
            }
            if (inputState.guessing == 0) {
                selected_table_AST = (AST) currentAST.root;
                selected_table_AST = (AST) astFactory.make((new ASTArray(2)).add(
                        astFactory.create(SELECTED_TABLE, "selected_table"))
                        .add(selected_table_AST));
                currentAST.root = selected_table_AST;
                currentAST.child = selected_table_AST != null
                        && selected_table_AST.getFirstChild() != null ? selected_table_AST
                        .getFirstChild() : selected_table_AST;
                currentAST.advanceChildToEnd();
            }
            selected_table_AST = (AST) currentAST.root;
        }
        else if ((_tokenSet_4.member(LA(1))) && (_tokenSet_19.member(LA(2)))
                && (_tokenSet_6.member(LA(3))) && (_tokenSet_20.member(LA(4)))) {
            {
                {
                    switch (LA(1)) {
                        case LITERAL_all:
                        case LITERAL_self:
                        case QUOTED_STRING:
                        case LITERAL_avg:
                        case LITERAL_count:
                        case LITERAL_max:
                        case LITERAL_min:
                        case LITERAL_stddev:
                        case LITERAL_sum:
                        case LITERAL_variance:
                        case LITERAL_filter:
                        case LITERAL_using:
                        case LITERAL_partition:
                        case LITERAL_by:
                        case IDENTIFIER:
                        case LITERAL_store:
                        case LITERAL_random:
                        case LITERAL_lowest:
                        case LITERAL_highest:
                        case LITERAL_with:
                        case LITERAL_group:
                        case LITERAL_pinned:
                        case LITERAL_geomean:
                        case LITERAL_kurtosis:
                        case LITERAL_median:
                        case LITERAL_skewness:
                        case LITERAL_sumsq:
                        case LITERAL_diff:
                        case LITERAL_custom:
                        case LITERAL_entrance:
                        case LITERAL_only:
                        case LITERAL_last:
                        case LITERAL_latest:
                        case LITERAL_milliseconds:
                        case LITERAL_seconds:
                        case LITERAL_minutes:
                        case LITERAL_hours:
                        case LITERAL_days:
                        case LITERAL_alert:
                        case LITERAL_correlate:
                        case LITERAL_present:
                        case LITERAL_is:
                        case LITERAL_row_status:
                        case LITERAL_new:
                        case LITERAL_dead:
                        case LITERAL_union:
                        case LITERAL_minus:
                        case LITERAL_except:
                        case LITERAL_intersect:
                        case LITERAL_limit:
                        case LITERAL_offset:
                        case LITERAL_current_timestamp:
                        case LITERAL_of: {
                            table_spec();
                            astFactory.addASTChild(currentAST, returnAST);
                            break;
                        }
                        case OPEN_PAREN: {
                            subquery();
                            astFactory.addASTChild(currentAST, returnAST);
                            break;
                        }
                        default: {
                            throw new NoViableAltException(LT(1), getFilename());
                        }
                    }
                }
                {
                    if ((_tokenSet_21.member(LA(1))) && (_tokenSet_22.member(LA(2)))
                            && (_tokenSet_23.member(LA(3))) && (_tokenSet_24.member(LA(4)))) {
                        alias();
                        astFactory.addASTChild(currentAST, returnAST);
                    }
                    else if ((_tokenSet_25.member(LA(1))) && (_tokenSet_23.member(LA(2)))
                            && (_tokenSet_24.member(LA(3))) && (_tokenSet_26.member(LA(4)))) {
                    }
                    else {
                        throw new NoViableAltException(LT(1), getFilename());
                    }

                }
            }
            if (inputState.guessing == 0) {
                selected_table_AST = (AST) currentAST.root;
                selected_table_AST = (AST) astFactory.make((new ASTArray(2)).add(
                        astFactory.create(SELECTED_TABLE, "selected_table"))
                        .add(selected_table_AST));
                currentAST.root = selected_table_AST;
                currentAST.child = selected_table_AST != null
                        && selected_table_AST.getFirstChild() != null ? selected_table_AST
                        .getFirstChild() : selected_table_AST;
                currentAST.advanceChildToEnd();
            }
            selected_table_AST = (AST) currentAST.root;
        }
        else if ((LA(1) == LITERAL_self) && (LA(2) == POUND)) {
            {
                clone_partition_clause();
                astFactory.addASTChild(currentAST, returnAST);
            }
            if (inputState.guessing == 0) {
                selected_table_AST = (AST) currentAST.root;
                selected_table_AST = (AST) astFactory.make((new ASTArray(2)).add(
                        astFactory.create(SELECTED_TABLE, "selected_table"))
                        .add(selected_table_AST));
                currentAST.root = selected_table_AST;
                currentAST.child = selected_table_AST != null
                        && selected_table_AST.getFirstChild() != null ? selected_table_AST
                        .getFirstChild() : selected_table_AST;
                currentAST.advanceChildToEnd();
            }
            selected_table_AST = (AST) currentAST.root;
        }
        else {
            throw new NoViableAltException(LT(1), getFilename());
        }

        returnAST = selected_table_AST;
    }

    public final void join_expression() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST join_expression_AST = null;

        {
            switch (LA(1)) {
                case LITERAL_left:
                case LITERAL_right: {
                    {
                        {
                            switch (LA(1)) {
                                case LITERAL_left: {
                                    AST tmp17_AST = null;
                                    tmp17_AST = astFactory.create(LT(1));
                                    astFactory.addASTChild(currentAST, tmp17_AST);
                                    match(LITERAL_left);
                                    break;
                                }
                                case LITERAL_right: {
                                    AST tmp18_AST = null;
                                    tmp18_AST = astFactory.create(LT(1));
                                    astFactory.addASTChild(currentAST, tmp18_AST);
                                    match(LITERAL_right);
                                    break;
                                }
                                default: {
                                    throw new NoViableAltException(LT(1), getFilename());
                                }
                            }
                        }
                        AST tmp19_AST = null;
                        tmp19_AST = astFactory.create(LT(1));
                        astFactory.addASTChild(currentAST, tmp19_AST);
                        match(LITERAL_outer);
                    }
                    break;
                }
                case LITERAL_inner: {
                    AST tmp20_AST = null;
                    tmp20_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp20_AST);
                    match(LITERAL_inner);
                    break;
                }
                case LITERAL_join: {
                    break;
                }
                default: {
                    throw new NoViableAltException(LT(1), getFilename());
                }
            }
        }
        AST tmp21_AST = null;
        tmp21_AST = astFactory.create(LT(1));
        astFactory.addASTChild(currentAST, tmp21_AST);
        match(LITERAL_join);
        selected_table();
        astFactory.addASTChild(currentAST, returnAST);
        AST tmp22_AST = null;
        tmp22_AST = astFactory.create(LT(1));
        astFactory.addASTChild(currentAST, tmp22_AST);
        match(LITERAL_on);
        condition();
        astFactory.addASTChild(currentAST, returnAST);
        join_expression_AST = (AST) currentAST.root;
        returnAST = join_expression_AST;
    }

    public final void case_expression() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST case_expression_AST = null;

        {
            AST tmp23_AST = null;
            tmp23_AST = astFactory.create(LT(1));
            astFactory.addASTChild(currentAST, tmp23_AST);
            match(LITERAL_case);
            {
                switch (LA(1)) {
                    case LITERAL_all:
                    case LITERAL_self:
                    case PLUS:
                    case MINUS:
                    case OPEN_PAREN:
                    case NUMBER:
                    case QUOTED_STRING:
                    case LITERAL_null:
                    case LITERAL_avg:
                    case LITERAL_count:
                    case LITERAL_max:
                    case LITERAL_min:
                    case LITERAL_stddev:
                    case LITERAL_sum:
                    case LITERAL_variance:
                    case LITERAL_filter:
                    case LITERAL_using:
                    case LITERAL_partition:
                    case LITERAL_by:
                    case IDENTIFIER:
                    case LITERAL_store:
                    case LITERAL_random:
                    case LITERAL_lowest:
                    case LITERAL_highest:
                    case LITERAL_with:
                    case LITERAL_group:
                    case LITERAL_pinned:
                    case LITERAL_geomean:
                    case LITERAL_kurtosis:
                    case LITERAL_median:
                    case LITERAL_skewness:
                    case LITERAL_sumsq:
                    case LITERAL_diff:
                    case LITERAL_custom:
                    case LITERAL_entrance:
                    case LITERAL_only:
                    case LITERAL_last:
                    case LITERAL_latest:
                    case LITERAL_milliseconds:
                    case LITERAL_seconds:
                    case LITERAL_minutes:
                    case LITERAL_hours:
                    case LITERAL_days:
                    case LITERAL_alert:
                    case LITERAL_correlate:
                    case LITERAL_present:
                    case LITERAL_is:
                    case LITERAL_row_status:
                    case LITERAL_new:
                    case LITERAL_dead:
                    case LITERAL_union:
                    case LITERAL_minus:
                    case LITERAL_except:
                    case LITERAL_intersect:
                    case LITERAL_limit:
                    case LITERAL_offset:
                    case LITERAL_current_timestamp:
                    case LITERAL_of: {
                        expression();
                        astFactory.addASTChild(currentAST, returnAST);
                        break;
                    }
                    case LITERAL_when: {
                        break;
                    }
                    default: {
                        throw new NoViableAltException(LT(1), getFilename());
                    }
                }
            }
            {
                int _cnt27 = 0;
                _loop27: do {
                    if ((LA(1) == LITERAL_when)) {
                        case_when_fragment();
                        astFactory.addASTChild(currentAST, returnAST);
                    }
                    else {
                        if (_cnt27 >= 1) {
                            break _loop27;
                        }
                        else {
                            throw new NoViableAltException(LT(1), getFilename());
                        }
                    }

                    _cnt27++;
                } while (true);
            }
            {
                switch (LA(1)) {
                    case LITERAL_else: {
                        AST tmp24_AST = null;
                        tmp24_AST = astFactory.create(LT(1));
                        astFactory.addASTChild(currentAST, tmp24_AST);
                        match(LITERAL_else);
                        expression();
                        astFactory.addASTChild(currentAST, returnAST);
                        break;
                    }
                    case LITERAL_end: {
                        break;
                    }
                    default: {
                        throw new NoViableAltException(LT(1), getFilename());
                    }
                }
            }
            AST tmp25_AST = null;
            tmp25_AST = astFactory.create(LT(1));
            astFactory.addASTChild(currentAST, tmp25_AST);
            match(LITERAL_end);
        }
        if (inputState.guessing == 0) {
            case_expression_AST = (AST) currentAST.root;
            case_expression_AST = (AST) astFactory
                    .make((new ASTArray(2)).add(
                            astFactory.create(CASE_EXPRESSION, "case_expression")).add(
                            case_expression_AST));
            currentAST.root = case_expression_AST;
            currentAST.child = case_expression_AST != null
                    && case_expression_AST.getFirstChild() != null ? case_expression_AST
                    .getFirstChild() : case_expression_AST;
            currentAST.advanceChildToEnd();
        }
        case_expression_AST = (AST) currentAST.root;
        returnAST = case_expression_AST;
    }

    public final void expression() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST expression_AST = null;

        term();
        astFactory.addASTChild(currentAST, returnAST);
        {
            _loop68: do {
                if ((LA(1) == PLUS || LA(1) == MINUS) && (_tokenSet_7.member(LA(2)))
                        && (_tokenSet_27.member(LA(3))) && (_tokenSet_28.member(LA(4)))) {
                    {
                        switch (LA(1)) {
                            case PLUS: {
                                AST tmp26_AST = null;
                                tmp26_AST = astFactory.create(LT(1));
                                astFactory.addASTChild(currentAST, tmp26_AST);
                                match(PLUS);
                                break;
                            }
                            case MINUS: {
                                AST tmp27_AST = null;
                                tmp27_AST = astFactory.create(LT(1));
                                astFactory.addASTChild(currentAST, tmp27_AST);
                                match(MINUS);
                                break;
                            }
                            default: {
                                throw new NoViableAltException(LT(1), getFilename());
                            }
                        }
                    }
                    term();
                    astFactory.addASTChild(currentAST, returnAST);
                }
                else {
                    break _loop68;
                }

            } while (true);
        }
        expression_AST = (AST) currentAST.root;
        returnAST = expression_AST;
    }

    public final void case_when_fragment() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST case_when_fragment_AST = null;

        {
            AST tmp28_AST = null;
            tmp28_AST = astFactory.create(LT(1));
            astFactory.addASTChild(currentAST, tmp28_AST);
            match(LITERAL_when);
            condition();
            astFactory.addASTChild(currentAST, returnAST);
            AST tmp29_AST = null;
            tmp29_AST = astFactory.create(LT(1));
            astFactory.addASTChild(currentAST, tmp29_AST);
            match(LITERAL_then);
            expression();
            astFactory.addASTChild(currentAST, returnAST);
        }
        case_when_fragment_AST = (AST) currentAST.root;
        returnAST = case_when_fragment_AST;
    }

    public final void condition() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST condition_AST = null;

        logical_term();
        astFactory.addASTChild(currentAST, returnAST);
        {
            _loop213: do {
                if ((LA(1) == LITERAL_or) && (_tokenSet_29.member(LA(2)))
                        && (_tokenSet_30.member(LA(3))) && (_tokenSet_31.member(LA(4)))) {
                    AST tmp30_AST = null;
                    tmp30_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp30_AST);
                    match(LITERAL_or);
                    logical_term();
                    astFactory.addASTChild(currentAST, returnAST);
                }
                else {
                    break _loop213;
                }

            } while (true);
        }
        condition_AST = (AST) currentAST.root;
        returnAST = condition_AST;
    }

    public final void group_clause() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST group_clause_AST = null;

        AST tmp31_AST = null;
        tmp31_AST = astFactory.create(LT(1));
        astFactory.addASTChild(currentAST, tmp31_AST);
        match(LITERAL_group);
        AST tmp32_AST = null;
        tmp32_AST = astFactory.create(LT(1));
        astFactory.addASTChild(currentAST, tmp32_AST);
        match(LITERAL_by);
        expression();
        astFactory.addASTChild(currentAST, returnAST);
        {
            _loop275: do {
                if ((LA(1) == COMMA)) {
                    AST tmp33_AST = null;
                    tmp33_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp33_AST);
                    match(COMMA);
                    expression();
                    astFactory.addASTChild(currentAST, returnAST);
                }
                else {
                    break _loop275;
                }

            } while (true);
        }
        {
            switch (LA(1)) {
                case LITERAL_having: {
                    AST tmp34_AST = null;
                    tmp34_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp34_AST);
                    match(LITERAL_having);
                    condition();
                    astFactory.addASTChild(currentAST, returnAST);
                    break;
                }
                case SEMI:
                case CLOSE_PAREN:
                case LITERAL_union:
                case LITERAL_minus:
                case LITERAL_except:
                case LITERAL_intersect:
                case LITERAL_order:
                case LITERAL_limit: {
                    break;
                }
                default: {
                    throw new NoViableAltException(LT(1), getFilename());
                }
            }
        }
        group_clause_AST = (AST) currentAST.root;
        returnAST = group_clause_AST;
    }

    public final void combine_clause() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST combine_clause_AST = null;

        {
            switch (LA(1)) {
                case LITERAL_union: {
                    {
                        AST tmp35_AST = null;
                        tmp35_AST = astFactory.create(LT(1));
                        astFactory.addASTChild(currentAST, tmp35_AST);
                        match(LITERAL_union);
                        {
                            switch (LA(1)) {
                                case LITERAL_all: {
                                    AST tmp36_AST = null;
                                    tmp36_AST = astFactory.create(LT(1));
                                    astFactory.addASTChild(currentAST, tmp36_AST);
                                    match(LITERAL_all);
                                    break;
                                }
                                case LITERAL_select: {
                                    break;
                                }
                                default: {
                                    throw new NoViableAltException(LT(1), getFilename());
                                }
                            }
                        }
                    }
                    break;
                }
                case LITERAL_minus: {
                    AST tmp37_AST = null;
                    tmp37_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp37_AST);
                    match(LITERAL_minus);
                    break;
                }
                case LITERAL_except: {
                    AST tmp38_AST = null;
                    tmp38_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp38_AST);
                    match(LITERAL_except);
                    break;
                }
                case LITERAL_intersect: {
                    AST tmp39_AST = null;
                    tmp39_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp39_AST);
                    match(LITERAL_intersect);
                    break;
                }
                default: {
                    throw new NoViableAltException(LT(1), getFilename());
                }
            }
        }
        select_expression();
        astFactory.addASTChild(currentAST, returnAST);
        combine_clause_AST = (AST) currentAST.root;
        returnAST = combine_clause_AST;
    }

    public final void order_clause() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST order_clause_AST = null;

        AST tmp40_AST = null;
        tmp40_AST = astFactory.create(LT(1));
        astFactory.addASTChild(currentAST, tmp40_AST);
        match(LITERAL_order);
        AST tmp41_AST = null;
        tmp41_AST = astFactory.create(LT(1));
        astFactory.addASTChild(currentAST, tmp41_AST);
        match(LITERAL_by);
        sorted_def();
        astFactory.addASTChild(currentAST, returnAST);
        {
            _loop283: do {
                if ((LA(1) == COMMA)) {
                    AST tmp42_AST = null;
                    tmp42_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp42_AST);
                    match(COMMA);
                    sorted_def();
                    astFactory.addASTChild(currentAST, returnAST);
                }
                else {
                    break _loop283;
                }

            } while (true);
        }
        order_clause_AST = (AST) currentAST.root;
        returnAST = order_clause_AST;
    }

    public final void limit_clause() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST limit_clause_AST = null;

        {
            AST tmp43_AST = null;
            tmp43_AST = astFactory.create(LT(1));
            astFactory.addASTChild(currentAST, tmp43_AST);
            match(LITERAL_limit);
            AST tmp44_AST = null;
            tmp44_AST = astFactory.create(LT(1));
            astFactory.addASTChild(currentAST, tmp44_AST);
            match(NUMBER);
            {
                switch (LA(1)) {
                    case LITERAL_offset: {
                        AST tmp45_AST = null;
                        tmp45_AST = astFactory.create(LT(1));
                        astFactory.addASTChild(currentAST, tmp45_AST);
                        match(LITERAL_offset);
                        AST tmp46_AST = null;
                        tmp46_AST = astFactory.create(LT(1));
                        astFactory.addASTChild(currentAST, tmp46_AST);
                        match(NUMBER);
                        break;
                    }
                    case SEMI:
                    case CLOSE_PAREN:
                    case LITERAL_order:
                    case LITERAL_limit: {
                        break;
                    }
                    default: {
                        throw new NoViableAltException(LT(1), getFilename());
                    }
                }
            }
        }
        if (inputState.guessing == 0) {
            limit_clause_AST = (AST) currentAST.root;
            limit_clause_AST = (AST) astFactory.make((new ASTArray(2)).add(
                    astFactory.create(LIMIT_CLAUSE, "limit_clause")).add(limit_clause_AST));
            currentAST.root = limit_clause_AST;
            currentAST.child = limit_clause_AST != null && limit_clause_AST.getFirstChild() != null ? limit_clause_AST
                    .getFirstChild()
                    : limit_clause_AST;
            currentAST.advanceChildToEnd();
        }
        limit_clause_AST = (AST) currentAST.root;
        returnAST = limit_clause_AST;
    }

    public final void schema_name() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST schema_name_AST = null;

        identifier();
        astFactory.addASTChild(currentAST, returnAST);
        schema_name_AST = (AST) currentAST.root;
        returnAST = schema_name_AST;
    }

    public final void table_name() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST table_name_AST = null;

        identifier();
        astFactory.addASTChild(currentAST, returnAST);
        table_name_AST = (AST) currentAST.root;
        returnAST = table_name_AST;
    }

    public final void exp_simple() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST exp_simple_AST = null;

        expression();
        astFactory.addASTChild(currentAST, returnAST);
        if (inputState.guessing == 0) {
            exp_simple_AST = (AST) currentAST.root;
            exp_simple_AST = (AST) astFactory.make((new ASTArray(2)).add(
                    astFactory.create(EXP_SIMPLE, "exp_simple")).add(exp_simple_AST));
            currentAST.root = exp_simple_AST;
            currentAST.child = exp_simple_AST != null && exp_simple_AST.getFirstChild() != null ? exp_simple_AST
                    .getFirstChild()
                    : exp_simple_AST;
            currentAST.advanceChildToEnd();
        }
        exp_simple_AST = (AST) currentAST.root;
        returnAST = exp_simple_AST;
    }

    public final void alias() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST alias_AST = null;

        {
            switch (LA(1)) {
                case LITERAL_as: {
                    AST tmp47_AST = null;
                    tmp47_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp47_AST);
                    match(LITERAL_as);
                    break;
                }
                case LITERAL_all:
                case LITERAL_self:
                case QUOTED_STRING:
                case LITERAL_avg:
                case LITERAL_count:
                case LITERAL_max:
                case LITERAL_min:
                case LITERAL_stddev:
                case LITERAL_sum:
                case LITERAL_variance:
                case LITERAL_filter:
                case LITERAL_using:
                case LITERAL_partition:
                case LITERAL_by:
                case IDENTIFIER:
                case LITERAL_store:
                case LITERAL_random:
                case LITERAL_lowest:
                case LITERAL_highest:
                case LITERAL_with:
                case LITERAL_group:
                case LITERAL_pinned:
                case LITERAL_geomean:
                case LITERAL_kurtosis:
                case LITERAL_median:
                case LITERAL_skewness:
                case LITERAL_sumsq:
                case LITERAL_diff:
                case LITERAL_custom:
                case LITERAL_entrance:
                case LITERAL_only:
                case LITERAL_last:
                case LITERAL_latest:
                case LITERAL_milliseconds:
                case LITERAL_seconds:
                case LITERAL_minutes:
                case LITERAL_hours:
                case LITERAL_days:
                case LITERAL_alert:
                case LITERAL_correlate:
                case LITERAL_present:
                case LITERAL_is:
                case LITERAL_row_status:
                case LITERAL_new:
                case LITERAL_dead:
                case LITERAL_union:
                case LITERAL_minus:
                case LITERAL_except:
                case LITERAL_intersect:
                case LITERAL_limit:
                case LITERAL_offset:
                case LITERAL_current_timestamp:
                case LITERAL_of: {
                    break;
                }
                default: {
                    throw new NoViableAltException(LT(1), getFilename());
                }
            }
        }
        identifier();
        astFactory.addASTChild(currentAST, returnAST);
        if (inputState.guessing == 0) {
            alias_AST = (AST) currentAST.root;
            alias_AST = (AST) astFactory.make((new ASTArray(2)).add(
                    astFactory.create(ALIAS, "alias")).add(alias_AST));
            currentAST.root = alias_AST;
            currentAST.child = alias_AST != null && alias_AST.getFirstChild() != null ? alias_AST
                    .getFirstChild() : alias_AST;
            currentAST.advanceChildToEnd();
        }
        alias_AST = (AST) currentAST.root;
        returnAST = alias_AST;
    }

    public final void clone_partition_clause() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST clone_partition_clause_AST = null;

        {
            AST tmp48_AST = null;
            tmp48_AST = astFactory.create(LT(1));
            astFactory.addASTChild(currentAST, tmp48_AST);
            match(LITERAL_self);
            AST tmp49_AST = null;
            tmp49_AST = astFactory.create(LT(1));
            astFactory.addASTChild(currentAST, tmp49_AST);
            match(POUND);
            identifier();
            astFactory.addASTChild(currentAST, returnAST);
            alias();
            astFactory.addASTChild(currentAST, returnAST);
        }
        if (inputState.guessing == 0) {
            clone_partition_clause_AST = (AST) currentAST.root;
            clone_partition_clause_AST = (AST) astFactory.make((new ASTArray(2)).add(
                    astFactory.create(CLONE_PARTITION_CLAUSE, "clone_partition_clause")).add(
                    clone_partition_clause_AST));
            currentAST.root = clone_partition_clause_AST;
            currentAST.child = clone_partition_clause_AST != null
                    && clone_partition_clause_AST.getFirstChild() != null ? clone_partition_clause_AST
                    .getFirstChild()
                    : clone_partition_clause_AST;
            currentAST.advanceChildToEnd();
        }
        clone_partition_clause_AST = (AST) currentAST.root;
        returnAST = clone_partition_clause_AST;
    }

    public final void identifier() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST identifier_AST = null;

        {
            switch (LA(1)) {
                case IDENTIFIER: {
                    AST tmp50_AST = null;
                    tmp50_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp50_AST);
                    match(IDENTIFIER);
                    break;
                }
                case QUOTED_STRING: {
                    AST tmp51_AST = null;
                    tmp51_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp51_AST);
                    match(QUOTED_STRING);
                    break;
                }
                case LITERAL_all:
                case LITERAL_self:
                case LITERAL_avg:
                case LITERAL_count:
                case LITERAL_max:
                case LITERAL_min:
                case LITERAL_stddev:
                case LITERAL_sum:
                case LITERAL_variance:
                case LITERAL_filter:
                case LITERAL_using:
                case LITERAL_partition:
                case LITERAL_by:
                case LITERAL_store:
                case LITERAL_random:
                case LITERAL_lowest:
                case LITERAL_highest:
                case LITERAL_with:
                case LITERAL_group:
                case LITERAL_pinned:
                case LITERAL_geomean:
                case LITERAL_kurtosis:
                case LITERAL_median:
                case LITERAL_skewness:
                case LITERAL_sumsq:
                case LITERAL_diff:
                case LITERAL_custom:
                case LITERAL_entrance:
                case LITERAL_only:
                case LITERAL_last:
                case LITERAL_latest:
                case LITERAL_milliseconds:
                case LITERAL_seconds:
                case LITERAL_minutes:
                case LITERAL_hours:
                case LITERAL_days:
                case LITERAL_alert:
                case LITERAL_correlate:
                case LITERAL_present:
                case LITERAL_is:
                case LITERAL_row_status:
                case LITERAL_new:
                case LITERAL_dead:
                case LITERAL_union:
                case LITERAL_minus:
                case LITERAL_except:
                case LITERAL_intersect:
                case LITERAL_limit:
                case LITERAL_offset:
                case LITERAL_current_timestamp:
                case LITERAL_of: {
                    keyword();
                    astFactory.addASTChild(currentAST, returnAST);
                    break;
                }
                default: {
                    throw new NoViableAltException(LT(1), getFilename());
                }
            }
        }
        identifier_AST = (AST) currentAST.root;
        returnAST = identifier_AST;
    }

    public final void term() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST term_AST = null;

        factor();
        astFactory.addASTChild(currentAST, returnAST);
        {
            _loop74: do {
                if ((LA(1) == ASTERISK || LA(1) == DIVIDE || LA(1) == MODULO)
                        && (_tokenSet_7.member(LA(2))) && (_tokenSet_27.member(LA(3)))
                        && (_tokenSet_28.member(LA(4)))) {
                    {
                        switch (LA(1)) {
                            case ASTERISK: {
                                multiply();
                                astFactory.addASTChild(currentAST, returnAST);
                                break;
                            }
                            case DIVIDE: {
                                AST tmp52_AST = null;
                                tmp52_AST = astFactory.create(LT(1));
                                astFactory.addASTChild(currentAST, tmp52_AST);
                                match(DIVIDE);
                                break;
                            }
                            case MODULO: {
                                AST tmp53_AST = null;
                                tmp53_AST = astFactory.create(LT(1));
                                astFactory.addASTChild(currentAST, tmp53_AST);
                                match(MODULO);
                                break;
                            }
                            default: {
                                throw new NoViableAltException(LT(1), getFilename());
                            }
                        }
                    }
                    factor();
                    astFactory.addASTChild(currentAST, returnAST);
                }
                else {
                    break _loop74;
                }

            } while (true);
        }
        term_AST = (AST) currentAST.root;
        returnAST = term_AST;
    }

    public final void factor() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST factor_AST = null;

        factor2();
        astFactory.addASTChild(currentAST, returnAST);
        {
            _loop78: do {
                if ((LA(1) == VERTBAR) && (LA(2) == VERTBAR) && (_tokenSet_7.member(LA(3)))
                        && (_tokenSet_27.member(LA(4)))) {
                    AST tmp54_AST = null;
                    tmp54_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp54_AST);
                    match(VERTBAR);
                    AST tmp55_AST = null;
                    tmp55_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp55_AST);
                    match(VERTBAR);
                    factor2();
                    astFactory.addASTChild(currentAST, returnAST);
                }
                else {
                    break _loop78;
                }

            } while (true);
        }
        factor_AST = (AST) currentAST.root;
        returnAST = factor_AST;
    }

    public final void multiply() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST multiply_AST = null;

        AST tmp56_AST = null;
        tmp56_AST = astFactory.create(LT(1));
        astFactory.addASTChild(currentAST, tmp56_AST);
        match(ASTERISK);
        multiply_AST = (AST) currentAST.root;
        returnAST = multiply_AST;
    }

    public final void factor2() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST factor2_AST = null;

        boolean synPredMatched81 = false;
        if ((((LA(1) >= NUMBER && LA(1) <= LITERAL_null)) && (_tokenSet_32.member(LA(2)))
                && (_tokenSet_33.member(LA(3))) && (_tokenSet_34.member(LA(4))))) {
            int _m81 = mark();
            synPredMatched81 = true;
            inputState.guessing++;
            try {
                {
                    sql_literal();
                }
            }
            catch (RecognitionException pe) {
                synPredMatched81 = false;
            }
            rewind(_m81);
            inputState.guessing--;
        }
        if (synPredMatched81) {
            sql_literal();
            astFactory.addASTChild(currentAST, returnAST);
            factor2_AST = (AST) currentAST.root;
        }
        else {
            boolean synPredMatched84 = false;
            if (((LA(1) == PLUS || LA(1) == MINUS))) {
                int _m84 = mark();
                synPredMatched84 = true;
                inputState.guessing++;
                try {
                    {
                        {
                            switch (LA(1)) {
                                case PLUS: {
                                    match(PLUS);
                                    break;
                                }
                                case MINUS: {
                                    match(MINUS);
                                    break;
                                }
                                default: {
                                    throw new NoViableAltException(LT(1), getFilename());
                                }
                            }
                        }
                        expression();
                    }
                }
                catch (RecognitionException pe) {
                    synPredMatched84 = false;
                }
                rewind(_m84);
                inputState.guessing--;
            }
            if (synPredMatched84) {
                {
                    switch (LA(1)) {
                        case PLUS: {
                            AST tmp57_AST = null;
                            tmp57_AST = astFactory.create(LT(1));
                            astFactory.addASTChild(currentAST, tmp57_AST);
                            match(PLUS);
                            break;
                        }
                        case MINUS: {
                            AST tmp58_AST = null;
                            tmp58_AST = astFactory.create(LT(1));
                            astFactory.addASTChild(currentAST, tmp58_AST);
                            match(MINUS);
                            break;
                        }
                        default: {
                            throw new NoViableAltException(LT(1), getFilename());
                        }
                    }
                }
                expression();
                astFactory.addASTChild(currentAST, returnAST);
                factor2_AST = (AST) currentAST.root;
            }
            else {
                boolean synPredMatched90 = false;
                if (((_tokenSet_14.member(LA(1))) && (LA(2) == DOT || LA(2) == OPEN_PAREN)
                        && (_tokenSet_7.member(LA(3))) && (_tokenSet_35.member(LA(4))))) {
                    int _m90 = mark();
                    synPredMatched90 = true;
                    inputState.guessing++;
                    try {
                        {
                            function();
                            {
                                match(OPEN_PAREN);
                                expression();
                                {
                                    _loop89: do {
                                        if ((LA(1) == COMMA)) {
                                            match(COMMA);
                                            expression();
                                        }
                                        else {
                                            break _loop89;
                                        }

                                    } while (true);
                                }
                                match(CLOSE_PAREN);
                            }
                        }
                    }
                    catch (RecognitionException pe) {
                        synPredMatched90 = false;
                    }
                    rewind(_m90);
                    inputState.guessing--;
                }
                if (synPredMatched90) {
                    function();
                    astFactory.addASTChild(currentAST, returnAST);
                    {
                        AST tmp59_AST = null;
                        tmp59_AST = astFactory.create(LT(1));
                        astFactory.addASTChild(currentAST, tmp59_AST);
                        match(OPEN_PAREN);
                        expression();
                        astFactory.addASTChild(currentAST, returnAST);
                        {
                            _loop93: do {
                                if ((LA(1) == COMMA)) {
                                    AST tmp60_AST = null;
                                    tmp60_AST = astFactory.create(LT(1));
                                    astFactory.addASTChild(currentAST, tmp60_AST);
                                    match(COMMA);
                                    expression();
                                    astFactory.addASTChild(currentAST, returnAST);
                                }
                                else {
                                    break _loop93;
                                }

                            } while (true);
                        }
                        AST tmp61_AST = null;
                        tmp61_AST = astFactory.create(LT(1));
                        astFactory.addASTChild(currentAST, tmp61_AST);
                        match(CLOSE_PAREN);
                    }
                    factor2_AST = (AST) currentAST.root;
                }
                else {
                    boolean synPredMatched97 = false;
                    if ((((LA(1) >= LITERAL_avg && LA(1) <= LITERAL_variance))
                            && (LA(2) == OPEN_PAREN) && (_tokenSet_36.member(LA(3))) && (_tokenSet_27
                            .member(LA(4))))) {
                        int _m97 = mark();
                        synPredMatched97 = true;
                        inputState.guessing++;
                        try {
                            {
                                group_function();
                                match(OPEN_PAREN);
                                {
                                    switch (LA(1)) {
                                        case ASTERISK: {
                                            match(ASTERISK);
                                            break;
                                        }
                                        case LITERAL_distinct: {
                                            match(LITERAL_distinct);
                                            break;
                                        }
                                        default:
                                            if ((LA(1) == LITERAL_all)
                                                    && (_tokenSet_37.member(LA(2))) && (true)
                                                    && (true)) {
                                                match(LITERAL_all);
                                            }
                                            else if ((_tokenSet_37.member(LA(1))) && (true) && (true) && (true)) {
                                            }
                                            else {
                                                throw new NoViableAltException(LT(1), getFilename());
                                            }
                                    }
                                }
                                {
                                    switch (LA(1)) {
                                        case LITERAL_all:
                                        case LITERAL_self:
                                        case PLUS:
                                        case MINUS:
                                        case OPEN_PAREN:
                                        case NUMBER:
                                        case QUOTED_STRING:
                                        case LITERAL_null:
                                        case LITERAL_avg:
                                        case LITERAL_count:
                                        case LITERAL_max:
                                        case LITERAL_min:
                                        case LITERAL_stddev:
                                        case LITERAL_sum:
                                        case LITERAL_variance:
                                        case LITERAL_filter:
                                        case LITERAL_using:
                                        case LITERAL_partition:
                                        case LITERAL_by:
                                        case IDENTIFIER:
                                        case LITERAL_store:
                                        case LITERAL_random:
                                        case LITERAL_lowest:
                                        case LITERAL_highest:
                                        case LITERAL_with:
                                        case LITERAL_group:
                                        case LITERAL_pinned:
                                        case LITERAL_geomean:
                                        case LITERAL_kurtosis:
                                        case LITERAL_median:
                                        case LITERAL_skewness:
                                        case LITERAL_sumsq:
                                        case LITERAL_diff:
                                        case LITERAL_custom:
                                        case LITERAL_entrance:
                                        case LITERAL_only:
                                        case LITERAL_last:
                                        case LITERAL_latest:
                                        case LITERAL_milliseconds:
                                        case LITERAL_seconds:
                                        case LITERAL_minutes:
                                        case LITERAL_hours:
                                        case LITERAL_days:
                                        case LITERAL_alert:
                                        case LITERAL_correlate:
                                        case LITERAL_present:
                                        case LITERAL_is:
                                        case LITERAL_row_status:
                                        case LITERAL_new:
                                        case LITERAL_dead:
                                        case LITERAL_union:
                                        case LITERAL_minus:
                                        case LITERAL_except:
                                        case LITERAL_intersect:
                                        case LITERAL_limit:
                                        case LITERAL_offset:
                                        case LITERAL_current_timestamp:
                                        case LITERAL_of: {
                                            expression();
                                            break;
                                        }
                                        case CLOSE_PAREN: {
                                            break;
                                        }
                                        default: {
                                            throw new NoViableAltException(LT(1), getFilename());
                                        }
                                    }
                                }
                                match(CLOSE_PAREN);
                            }
                        }
                        catch (RecognitionException pe) {
                            synPredMatched97 = false;
                        }
                        rewind(_m97);
                        inputState.guessing--;
                    }
                    if (synPredMatched97) {
                        group_function();
                        astFactory.addASTChild(currentAST, returnAST);
                        AST tmp62_AST = null;
                        tmp62_AST = astFactory.create(LT(1));
                        astFactory.addASTChild(currentAST, tmp62_AST);
                        match(OPEN_PAREN);
                        {
                            switch (LA(1)) {
                                case ASTERISK: {
                                    AST tmp63_AST = null;
                                    tmp63_AST = astFactory.create(LT(1));
                                    astFactory.addASTChild(currentAST, tmp63_AST);
                                    match(ASTERISK);
                                    break;
                                }
                                case LITERAL_distinct: {
                                    AST tmp64_AST = null;
                                    tmp64_AST = astFactory.create(LT(1));
                                    astFactory.addASTChild(currentAST, tmp64_AST);
                                    match(LITERAL_distinct);
                                    break;
                                }
                                default:
                                    if ((LA(1) == LITERAL_all) && (_tokenSet_37.member(LA(2)))
                                            && (_tokenSet_27.member(LA(3)))
                                            && (_tokenSet_28.member(LA(4)))) {
                                        AST tmp65_AST = null;
                                        tmp65_AST = astFactory.create(LT(1));
                                        astFactory.addASTChild(currentAST, tmp65_AST);
                                        match(LITERAL_all);
                                    }
                                    else if ((_tokenSet_37.member(LA(1)))
                                            && (_tokenSet_27.member(LA(2)))
                                            && (_tokenSet_28.member(LA(3)))
                                            && (_tokenSet_34.member(LA(4)))) {
                                    }
                                    else {
                                        throw new NoViableAltException(LT(1), getFilename());
                                    }
                            }
                        }
                        {
                            switch (LA(1)) {
                                case LITERAL_all:
                                case LITERAL_self:
                                case PLUS:
                                case MINUS:
                                case OPEN_PAREN:
                                case NUMBER:
                                case QUOTED_STRING:
                                case LITERAL_null:
                                case LITERAL_avg:
                                case LITERAL_count:
                                case LITERAL_max:
                                case LITERAL_min:
                                case LITERAL_stddev:
                                case LITERAL_sum:
                                case LITERAL_variance:
                                case LITERAL_filter:
                                case LITERAL_using:
                                case LITERAL_partition:
                                case LITERAL_by:
                                case IDENTIFIER:
                                case LITERAL_store:
                                case LITERAL_random:
                                case LITERAL_lowest:
                                case LITERAL_highest:
                                case LITERAL_with:
                                case LITERAL_group:
                                case LITERAL_pinned:
                                case LITERAL_geomean:
                                case LITERAL_kurtosis:
                                case LITERAL_median:
                                case LITERAL_skewness:
                                case LITERAL_sumsq:
                                case LITERAL_diff:
                                case LITERAL_custom:
                                case LITERAL_entrance:
                                case LITERAL_only:
                                case LITERAL_last:
                                case LITERAL_latest:
                                case LITERAL_milliseconds:
                                case LITERAL_seconds:
                                case LITERAL_minutes:
                                case LITERAL_hours:
                                case LITERAL_days:
                                case LITERAL_alert:
                                case LITERAL_correlate:
                                case LITERAL_present:
                                case LITERAL_is:
                                case LITERAL_row_status:
                                case LITERAL_new:
                                case LITERAL_dead:
                                case LITERAL_union:
                                case LITERAL_minus:
                                case LITERAL_except:
                                case LITERAL_intersect:
                                case LITERAL_limit:
                                case LITERAL_offset:
                                case LITERAL_current_timestamp:
                                case LITERAL_of: {
                                    expression();
                                    astFactory.addASTChild(currentAST, returnAST);
                                    break;
                                }
                                case CLOSE_PAREN: {
                                    break;
                                }
                                default: {
                                    throw new NoViableAltException(LT(1), getFilename());
                                }
                            }
                        }
                        AST tmp66_AST = null;
                        tmp66_AST = astFactory.create(LT(1));
                        astFactory.addASTChild(currentAST, tmp66_AST);
                        match(CLOSE_PAREN);
                        factor2_AST = (AST) currentAST.root;
                    }
                    else {
                        boolean synPredMatched101 = false;
                        if (((LA(1) == OPEN_PAREN) && (_tokenSet_7.member(LA(2)))
                                && (_tokenSet_38.member(LA(3))) && (_tokenSet_39.member(LA(4))))) {
                            int _m101 = mark();
                            synPredMatched101 = true;
                            inputState.guessing++;
                            try {
                                {
                                    match(OPEN_PAREN);
                                    expression();
                                    match(CLOSE_PAREN);
                                }
                            }
                            catch (RecognitionException pe) {
                                synPredMatched101 = false;
                            }
                            rewind(_m101);
                            inputState.guessing--;
                        }
                        if (synPredMatched101) {
                            AST tmp67_AST = null;
                            tmp67_AST = astFactory.create(LT(1));
                            astFactory.addASTChild(currentAST, tmp67_AST);
                            match(OPEN_PAREN);
                            expression();
                            astFactory.addASTChild(currentAST, returnAST);
                            AST tmp68_AST = null;
                            tmp68_AST = astFactory.create(LT(1));
                            astFactory.addASTChild(currentAST, tmp68_AST);
                            match(CLOSE_PAREN);
                            factor2_AST = (AST) currentAST.root;
                        }
                        else {
                            boolean synPredMatched103 = false;
                            if (((_tokenSet_14.member(LA(1))) && (_tokenSet_40.member(LA(2)))
                                    && (_tokenSet_33.member(LA(3))) && (_tokenSet_34.member(LA(4))))) {
                                int _m103 = mark();
                                synPredMatched103 = true;
                                inputState.guessing++;
                                try {
                                    {
                                        variable();
                                    }
                                }
                                catch (RecognitionException pe) {
                                    synPredMatched103 = false;
                                }
                                rewind(_m103);
                                inputState.guessing--;
                            }
                            if (synPredMatched103) {
                                variable();
                                astFactory.addASTChild(currentAST, returnAST);
                                factor2_AST = (AST) currentAST.root;
                            }
                            else if ((LA(1) == OPEN_PAREN) && (_tokenSet_7.member(LA(2)))
                                    && (_tokenSet_41.member(LA(3))) && (_tokenSet_42.member(LA(4)))) {
                                expression_list();
                                astFactory.addASTChild(currentAST, returnAST);
                                factor2_AST = (AST) currentAST.root;
                            }
                            else {
                                throw new NoViableAltException(LT(1), getFilename());
                            }
                        }
                    }
                }
            }
        }
        returnAST = factor2_AST;
    }

    public final void sql_literal() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST sql_literal_AST = null;

        {
            switch (LA(1)) {
                case NUMBER: {
                    AST tmp69_AST = null;
                    tmp69_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp69_AST);
                    match(NUMBER);
                    break;
                }
                case QUOTED_STRING: {
                    AST tmp70_AST = null;
                    tmp70_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp70_AST);
                    match(QUOTED_STRING);
                    break;
                }
                case LITERAL_null: {
                    AST tmp71_AST = null;
                    tmp71_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp71_AST);
                    match(LITERAL_null);
                    break;
                }
                default: {
                    throw new NoViableAltException(LT(1), getFilename());
                }
            }
        }
        sql_literal_AST = (AST) currentAST.root;
        returnAST = sql_literal_AST;
    }

    public final void function() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST function_AST = null;

        if ((_tokenSet_14.member(LA(1))) && (LA(2) == DOT || LA(2) == OPEN_PAREN)
                && (_tokenSet_7.member(LA(3))) && (_tokenSet_35.member(LA(4)))) {
            any_function();
            astFactory.addASTChild(currentAST, returnAST);
            function_AST = (AST) currentAST.root;
        }
        else if (((LA(1) >= LITERAL_avg && LA(1) <= LITERAL_variance)) && (LA(2) == OPEN_PAREN)
                && (_tokenSet_7.member(LA(3))) && (_tokenSet_35.member(LA(4)))) {
            group_function();
            astFactory.addASTChild(currentAST, returnAST);
            function_AST = (AST) currentAST.root;
        }
        else {
            throw new NoViableAltException(LT(1), getFilename());
        }

        returnAST = function_AST;
    }

    public final void group_function() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST group_function_AST = null;

        {
            switch (LA(1)) {
                case LITERAL_avg: {
                    AST tmp72_AST = null;
                    tmp72_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp72_AST);
                    match(LITERAL_avg);
                    break;
                }
                case LITERAL_count: {
                    AST tmp73_AST = null;
                    tmp73_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp73_AST);
                    match(LITERAL_count);
                    break;
                }
                case LITERAL_max: {
                    AST tmp74_AST = null;
                    tmp74_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp74_AST);
                    match(LITERAL_max);
                    break;
                }
                case LITERAL_min: {
                    AST tmp75_AST = null;
                    tmp75_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp75_AST);
                    match(LITERAL_min);
                    break;
                }
                case LITERAL_stddev: {
                    AST tmp76_AST = null;
                    tmp76_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp76_AST);
                    match(LITERAL_stddev);
                    break;
                }
                case LITERAL_sum: {
                    AST tmp77_AST = null;
                    tmp77_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp77_AST);
                    match(LITERAL_sum);
                    break;
                }
                case LITERAL_variance: {
                    AST tmp78_AST = null;
                    tmp78_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp78_AST);
                    match(LITERAL_variance);
                    break;
                }
                default: {
                    throw new NoViableAltException(LT(1), getFilename());
                }
            }
        }
        if (inputState.guessing == 0) {
            group_function_AST = (AST) currentAST.root;
            group_function_AST = (AST) astFactory.make((new ASTArray(2)).add(
                    astFactory.create(GROUP_FUNCTION, "group_function")).add(group_function_AST));
            currentAST.root = group_function_AST;
            currentAST.child = group_function_AST != null
                    && group_function_AST.getFirstChild() != null ? group_function_AST
                    .getFirstChild() : group_function_AST;
            currentAST.advanceChildToEnd();
        }
        group_function_AST = (AST) currentAST.root;
        returnAST = group_function_AST;
    }

    public final void variable() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST variable_AST = null;

        boolean synPredMatched112 = false;
        if (((_tokenSet_14.member(LA(1))) && (LA(2) == DOT || LA(2) == OPEN_PAREN)
                && (_tokenSet_43.member(LA(3))) && (LA(4) == DOT || LA(4) == OPEN_PAREN || LA(4) == CLOSE_PAREN))) {
            int _m112 = mark();
            synPredMatched112 = true;
            inputState.guessing++;
            try {
                {
                    column_spec();
                    {
                        match(OPEN_PAREN);
                        match(PLUS);
                        match(CLOSE_PAREN);
                    }
                }
            }
            catch (RecognitionException pe) {
                synPredMatched112 = false;
            }
            rewind(_m112);
            inputState.guessing--;
        }
        if (synPredMatched112) {
            column_spec();
            astFactory.addASTChild(currentAST, returnAST);
            {
                AST tmp79_AST = null;
                tmp79_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp79_AST);
                match(OPEN_PAREN);
                AST tmp80_AST = null;
                tmp80_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp80_AST);
                match(PLUS);
                AST tmp81_AST = null;
                tmp81_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp81_AST);
                match(CLOSE_PAREN);
            }
            variable_AST = (AST) currentAST.root;
        }
        else if ((_tokenSet_14.member(LA(1))) && (_tokenSet_44.member(LA(2)))
                && (_tokenSet_33.member(LA(3))) && (_tokenSet_34.member(LA(4)))) {
            column_spec();
            astFactory.addASTChild(currentAST, returnAST);
            variable_AST = (AST) currentAST.root;
        }
        else {
            throw new NoViableAltException(LT(1), getFilename());
        }

        returnAST = variable_AST;
    }

    public final void expression_list() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST expression_list_AST = null;

        AST tmp82_AST = null;
        tmp82_AST = astFactory.create(LT(1));
        astFactory.addASTChild(currentAST, tmp82_AST);
        match(OPEN_PAREN);
        expression();
        astFactory.addASTChild(currentAST, returnAST);
        {
            int _cnt106 = 0;
            _loop106: do {
                if ((LA(1) == COMMA)) {
                    AST tmp83_AST = null;
                    tmp83_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp83_AST);
                    match(COMMA);
                    expression();
                    astFactory.addASTChild(currentAST, returnAST);
                }
                else {
                    if (_cnt106 >= 1) {
                        break _loop106;
                    }
                    else {
                        throw new NoViableAltException(LT(1), getFilename());
                    }
                }

                _cnt106++;
            } while (true);
        }
        AST tmp84_AST = null;
        tmp84_AST = astFactory.create(LT(1));
        astFactory.addASTChild(currentAST, tmp84_AST);
        match(CLOSE_PAREN);
        expression_list_AST = (AST) currentAST.root;
        returnAST = expression_list_AST;
    }

    public final void column_spec() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST column_spec_AST = null;

        {
            if ((_tokenSet_14.member(LA(1))) && (LA(2) == DOT)) {
                {
                    if ((_tokenSet_14.member(LA(1))) && (LA(2) == DOT)
                            && (_tokenSet_14.member(LA(3))) && (LA(4) == DOT)) {
                        schema_name();
                        astFactory.addASTChild(currentAST, returnAST);
                        AST tmp85_AST = null;
                        tmp85_AST = astFactory.create(LT(1));
                        astFactory.addASTChild(currentAST, tmp85_AST);
                        match(DOT);
                    }
                    else if ((_tokenSet_14.member(LA(1))) && (LA(2) == DOT)
                            && (_tokenSet_14.member(LA(3))) && (_tokenSet_45.member(LA(4)))) {
                    }
                    else {
                        throw new NoViableAltException(LT(1), getFilename());
                    }

                }
                table_name();
                astFactory.addASTChild(currentAST, returnAST);
                AST tmp86_AST = null;
                tmp86_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp86_AST);
                match(DOT);
            }
            else if ((_tokenSet_14.member(LA(1))) && (_tokenSet_45.member(LA(2)))) {
            }
            else {
                throw new NoViableAltException(LT(1), getFilename());
            }

        }
        column_name();
        astFactory.addASTChild(currentAST, returnAST);
        column_spec_AST = (AST) currentAST.root;
        returnAST = column_spec_AST;
    }

    public final void column_name() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST column_name_AST = null;

        identifier();
        astFactory.addASTChild(currentAST, returnAST);
        column_name_AST = (AST) currentAST.root;
        returnAST = column_name_AST;
    }

    public final void package_name() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST package_name_AST = null;

        identifier();
        astFactory.addASTChild(currentAST, returnAST);
        package_name_AST = (AST) currentAST.root;
        returnAST = package_name_AST;
    }

    public final void any_function() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST any_function_AST = null;

        {
            if ((_tokenSet_14.member(LA(1))) && (LA(2) == DOT)) {
                {
                    if ((_tokenSet_14.member(LA(1))) && (LA(2) == DOT)
                            && (_tokenSet_14.member(LA(3))) && (LA(4) == DOT)) {
                        schema_name();
                        astFactory.addASTChild(currentAST, returnAST);
                        AST tmp87_AST = null;
                        tmp87_AST = astFactory.create(LT(1));
                        astFactory.addASTChild(currentAST, tmp87_AST);
                        match(DOT);
                    }
                    else if ((_tokenSet_14.member(LA(1))) && (LA(2) == DOT)
                            && (_tokenSet_14.member(LA(3))) && (LA(4) == OPEN_PAREN)) {
                    }
                    else {
                        throw new NoViableAltException(LT(1), getFilename());
                    }

                }
                package_name();
                astFactory.addASTChild(currentAST, returnAST);
                AST tmp88_AST = null;
                tmp88_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp88_AST);
                match(DOT);
            }
            else if ((_tokenSet_14.member(LA(1))) && (LA(2) == OPEN_PAREN)) {
            }
            else {
                throw new NoViableAltException(LT(1), getFilename());
            }

        }
        identifier();
        astFactory.addASTChild(currentAST, returnAST);
        any_function_AST = (AST) currentAST.root;
        returnAST = any_function_AST;
    }

    public final void filter_spec() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST filter_spec_AST = null;

        table_spec();
        astFactory.addASTChild(currentAST, returnAST);
        AST tmp89_AST = null;
        tmp89_AST = astFactory.create(LT(1));
        astFactory.addASTChild(currentAST, tmp89_AST);
        match(OPEN_PAREN);
        AST tmp90_AST = null;
        tmp90_AST = astFactory.create(LT(1));
        astFactory.addASTChild(currentAST, tmp90_AST);
        match(LITERAL_filter);
        AST tmp91_AST = null;
        tmp91_AST = astFactory.create(LT(1));
        astFactory.addASTChild(currentAST, tmp91_AST);
        match(LITERAL_using);
        AST tmp92_AST = null;
        tmp92_AST = astFactory.create(LT(1));
        astFactory.addASTChild(currentAST, tmp92_AST);
        match(QUOTED_STRING);
        AST tmp93_AST = null;
        tmp93_AST = astFactory.create(LT(1));
        astFactory.addASTChild(currentAST, tmp93_AST);
        match(CLOSE_PAREN);
        if (inputState.guessing == 0) {
            filter_spec_AST = (AST) currentAST.root;
            filter_spec_AST = (AST) astFactory.make((new ASTArray(2)).add(
                    astFactory.create(FILTER_SPEC, "filter_spec")).add(filter_spec_AST));
            currentAST.root = filter_spec_AST;
            currentAST.child = filter_spec_AST != null && filter_spec_AST.getFirstChild() != null ? filter_spec_AST
                    .getFirstChild()
                    : filter_spec_AST;
            currentAST.advanceChildToEnd();
        }
        filter_spec_AST = (AST) currentAST.root;
        returnAST = filter_spec_AST;
    }

    public final void partition_spec_list() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST partition_spec_list_AST = null;

        table_spec();
        astFactory.addASTChild(currentAST, returnAST);
        partition_spec();
        astFactory.addASTChild(currentAST, returnAST);
        {
            _loop141: do {
                if ((LA(1) == LITERAL_to)) {
                    AST tmp94_AST = null;
                    tmp94_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp94_AST);
                    match(LITERAL_to);
                    partition_spec();
                    astFactory.addASTChild(currentAST, returnAST);
                }
                else {
                    break _loop141;
                }

            } while (true);
        }
        if (inputState.guessing == 0) {
            partition_spec_list_AST = (AST) currentAST.root;
            partition_spec_list_AST = (AST) astFactory.make((new ASTArray(2)).add(
                    astFactory.create(PARTITION_SPEC_LIST, "partition_spec_list")).add(
                    partition_spec_list_AST));
            currentAST.root = partition_spec_list_AST;
            currentAST.child = partition_spec_list_AST != null
                    && partition_spec_list_AST.getFirstChild() != null ? partition_spec_list_AST
                    .getFirstChild() : partition_spec_list_AST;
            currentAST.advanceChildToEnd();
        }
        partition_spec_list_AST = (AST) currentAST.root;
        returnAST = partition_spec_list_AST;
    }

    public final void table_spec() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST table_spec_AST = null;

        {
            if ((_tokenSet_14.member(LA(1))) && (LA(2) == DOT)) {
                schema_name();
                astFactory.addASTChild(currentAST, returnAST);
                AST tmp95_AST = null;
                tmp95_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp95_AST);
                match(DOT);
            }
            else if ((_tokenSet_14.member(LA(1))) && (_tokenSet_46.member(LA(2)))) {
            }
            else {
                throw new NoViableAltException(LT(1), getFilename());
            }

        }
        table_name();
        astFactory.addASTChild(currentAST, returnAST);
        if (inputState.guessing == 0) {
            table_spec_AST = (AST) currentAST.root;
            table_spec_AST = (AST) astFactory.make((new ASTArray(2)).add(
                    astFactory.create(TABLE_SPEC, "table_spec")).add(table_spec_AST));
            currentAST.root = table_spec_AST;
            currentAST.child = table_spec_AST != null && table_spec_AST.getFirstChild() != null ? table_spec_AST
                    .getFirstChild()
                    : table_spec_AST;
            currentAST.advanceChildToEnd();
        }
        table_spec_AST = (AST) currentAST.root;
        returnAST = table_spec_AST;
    }

    public final void subquery() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST subquery_AST = null;

        AST tmp96_AST = null;
        tmp96_AST = astFactory.create(LT(1));
        astFactory.addASTChild(currentAST, tmp96_AST);
        match(OPEN_PAREN);
        select_expression();
        astFactory.addASTChild(currentAST, returnAST);
        AST tmp97_AST = null;
        tmp97_AST = astFactory.create(LT(1));
        astFactory.addASTChild(currentAST, tmp97_AST);
        match(CLOSE_PAREN);
        subquery_AST = (AST) currentAST.root;
        returnAST = subquery_AST;
    }

    public final void partition_spec() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST partition_spec_AST = null;

        AST tmp98_AST = null;
        tmp98_AST = astFactory.create(LT(1));
        astFactory.addASTChild(currentAST, tmp98_AST);
        match(OPEN_PAREN);
        AST tmp99_AST = null;
        tmp99_AST = astFactory.create(LT(1));
        astFactory.addASTChild(currentAST, tmp99_AST);
        match(LITERAL_partition);
        {
            switch (LA(1)) {
                case LITERAL_by: {
                    AST tmp100_AST = null;
                    tmp100_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp100_AST);
                    match(LITERAL_by);
                    AST tmp101_AST = null;
                    tmp101_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp101_AST);
                    match(IDENTIFIER);
                    {
                        _loop147: do {
                            if ((LA(1) == COMMA)) {
                                AST tmp102_AST = null;
                                tmp102_AST = astFactory.create(LT(1));
                                astFactory.addASTChild(currentAST, tmp102_AST);
                                match(COMMA);
                                AST tmp103_AST = null;
                                tmp103_AST = astFactory.create(LT(1));
                                astFactory.addASTChild(currentAST, tmp103_AST);
                                match(IDENTIFIER);
                            }
                            else {
                                break _loop147;
                            }

                        } while (true);
                    }
                    break;
                }
                case LITERAL_store: {
                    break;
                }
                default: {
                    throw new NoViableAltException(LT(1), getFilename());
                }
            }
        }
        AST tmp104_AST = null;
        tmp104_AST = astFactory.create(LT(1));
        astFactory.addASTChild(currentAST, tmp104_AST);
        match(LITERAL_store);
        row_pick_spec();
        astFactory.addASTChild(currentAST, returnAST);
        {
            switch (LA(1)) {
                case LITERAL_where: {
                    AST tmp105_AST = null;
                    tmp105_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp105_AST);
                    match(LITERAL_where);
                    where_condition();
                    astFactory.addASTChild(currentAST, returnAST);
                    break;
                }
                case CLOSE_PAREN: {
                    break;
                }
                default: {
                    throw new NoViableAltException(LT(1), getFilename());
                }
            }
        }
        AST tmp106_AST = null;
        tmp106_AST = astFactory.create(LT(1));
        astFactory.addASTChild(currentAST, tmp106_AST);
        match(CLOSE_PAREN);
        if (inputState.guessing == 0) {
            partition_spec_AST = (AST) currentAST.root;
            partition_spec_AST = (AST) astFactory.make((new ASTArray(2)).add(
                    astFactory.create(PARTITION_SPEC, "partition_spec")).add(partition_spec_AST));
            currentAST.root = partition_spec_AST;
            currentAST.child = partition_spec_AST != null
                    && partition_spec_AST.getFirstChild() != null ? partition_spec_AST
                    .getFirstChild() : partition_spec_AST;
            currentAST.advanceChildToEnd();
        }
        partition_spec_AST = (AST) currentAST.root;
        returnAST = partition_spec_AST;
    }

    public final void row_pick_spec() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST row_pick_spec_AST = null;

        {
            {
                switch (LA(1)) {
                    case LITERAL_random:
                    case LITERAL_lowest:
                    case LITERAL_highest: {
                        misc_function();
                        astFactory.addASTChild(currentAST, returnAST);
                        break;
                    }
                    case LITERAL_last:
                    case LITERAL_latest: {
                        window_function();
                        astFactory.addASTChild(currentAST, returnAST);
                        break;
                    }
                    default: {
                        throw new NoViableAltException(LT(1), getFilename());
                    }
                }
            }
            {
                switch (LA(1)) {
                    case LITERAL_with: {
                        aggregate_function();
                        astFactory.addASTChild(currentAST, returnAST);
                        break;
                    }
                    case LITERAL_where:
                    case CLOSE_PAREN: {
                        break;
                    }
                    default: {
                        throw new NoViableAltException(LT(1), getFilename());
                    }
                }
            }
        }
        if (inputState.guessing == 0) {
            row_pick_spec_AST = (AST) currentAST.root;
            row_pick_spec_AST = (AST) astFactory.make((new ASTArray(2)).add(
                    astFactory.create(ROW_PICK_SPEC, "row_pick_spec")).add(row_pick_spec_AST));
            currentAST.root = row_pick_spec_AST;
            currentAST.child = row_pick_spec_AST != null
                    && row_pick_spec_AST.getFirstChild() != null ? row_pick_spec_AST
                    .getFirstChild() : row_pick_spec_AST;
            currentAST.advanceChildToEnd();
        }
        row_pick_spec_AST = (AST) currentAST.root;
        returnAST = row_pick_spec_AST;
    }

    public final void misc_function() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST misc_function_AST = null;

        {
            {
                switch (LA(1)) {
                    case LITERAL_random: {
                        {
                            AST tmp107_AST = null;
                            tmp107_AST = astFactory.create(LT(1));
                            astFactory.addASTChild(currentAST, tmp107_AST);
                            match(LITERAL_random);
                            AST tmp108_AST = null;
                            tmp108_AST = astFactory.create(LT(1));
                            astFactory.addASTChild(currentAST, tmp108_AST);
                            match(NUMBER);
                        }
                        break;
                    }
                    case LITERAL_lowest:
                    case LITERAL_highest: {
                        {
                            {
                                switch (LA(1)) {
                                    case LITERAL_lowest: {
                                        AST tmp109_AST = null;
                                        tmp109_AST = astFactory.create(LT(1));
                                        astFactory.addASTChild(currentAST, tmp109_AST);
                                        match(LITERAL_lowest);
                                        break;
                                    }
                                    case LITERAL_highest: {
                                        AST tmp110_AST = null;
                                        tmp110_AST = astFactory.create(LT(1));
                                        astFactory.addASTChild(currentAST, tmp110_AST);
                                        match(LITERAL_highest);
                                        break;
                                    }
                                    default: {
                                        throw new NoViableAltException(LT(1), getFilename());
                                    }
                                }
                            }
                            AST tmp111_AST = null;
                            tmp111_AST = astFactory.create(LT(1));
                            astFactory.addASTChild(currentAST, tmp111_AST);
                            match(NUMBER);
                            AST tmp112_AST = null;
                            tmp112_AST = astFactory.create(LT(1));
                            astFactory.addASTChild(currentAST, tmp112_AST);
                            match(LITERAL_using);
                            AST tmp113_AST = null;
                            tmp113_AST = astFactory.create(LT(1));
                            astFactory.addASTChild(currentAST, tmp113_AST);
                            match(IDENTIFIER);
                            {
                                _loop162: do {
                                    if ((LA(1) == LITERAL_with) && (LA(2) == LITERAL_update)) {
                                        AST tmp114_AST = null;
                                        tmp114_AST = astFactory.create(LT(1));
                                        astFactory.addASTChild(currentAST, tmp114_AST);
                                        match(LITERAL_with);
                                        AST tmp115_AST = null;
                                        tmp115_AST = astFactory.create(LT(1));
                                        astFactory.addASTChild(currentAST, tmp115_AST);
                                        match(LITERAL_update);
                                        AST tmp116_AST = null;
                                        tmp116_AST = astFactory.create(LT(1));
                                        astFactory.addASTChild(currentAST, tmp116_AST);
                                        match(LITERAL_group);
                                        AST tmp117_AST = null;
                                        tmp117_AST = astFactory.create(LT(1));
                                        astFactory.addASTChild(currentAST, tmp117_AST);
                                        match(IDENTIFIER);
                                        {
                                            _loop161: do {
                                                if ((LA(1) == COMMA)) {
                                                    AST tmp118_AST = null;
                                                    tmp118_AST = astFactory.create(LT(1));
                                                    astFactory.addASTChild(currentAST, tmp118_AST);
                                                    match(COMMA);
                                                    AST tmp119_AST = null;
                                                    tmp119_AST = astFactory.create(LT(1));
                                                    astFactory.addASTChild(currentAST, tmp119_AST);
                                                    match(IDENTIFIER);
                                                }
                                                else {
                                                    break _loop161;
                                                }

                                            } while (true);
                                        }
                                    }
                                    else {
                                        break _loop162;
                                    }

                                } while (true);
                            }
                        }
                        break;
                    }
                    default: {
                        throw new NoViableAltException(LT(1), getFilename());
                    }
                }
            }
            {
                switch (LA(1)) {
                    case QUOTED_STRING: {
                        AST tmp120_AST = null;
                        tmp120_AST = astFactory.create(LT(1));
                        astFactory.addASTChild(currentAST, tmp120_AST);
                        match(QUOTED_STRING);
                        break;
                    }
                    case LITERAL_where:
                    case CLOSE_PAREN:
                    case LITERAL_with: {
                        break;
                    }
                    default: {
                        throw new NoViableAltException(LT(1), getFilename());
                    }
                }
            }
        }
        if (inputState.guessing == 0) {
            misc_function_AST = (AST) currentAST.root;
            misc_function_AST = (AST) astFactory.make((new ASTArray(2)).add(
                    astFactory.create(MISC_FUNCTION, "misc_function")).add(misc_function_AST));
            currentAST.root = misc_function_AST;
            currentAST.child = misc_function_AST != null
                    && misc_function_AST.getFirstChild() != null ? misc_function_AST
                    .getFirstChild() : misc_function_AST;
            currentAST.advanceChildToEnd();
        }
        misc_function_AST = (AST) currentAST.root;
        returnAST = misc_function_AST;
    }

    public final void window_function() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST window_function_AST = null;

        {
            {
                switch (LA(1)) {
                    case LITERAL_last: {
                        {
                            AST tmp121_AST = null;
                            tmp121_AST = astFactory.create(LT(1));
                            astFactory.addASTChild(currentAST, tmp121_AST);
                            match(LITERAL_last);
                            AST tmp122_AST = null;
                            tmp122_AST = astFactory.create(LT(1));
                            astFactory.addASTChild(currentAST, tmp122_AST);
                            match(NUMBER);
                            {
                                switch (LA(1)) {
                                    case LITERAL_milliseconds:
                                    case LITERAL_seconds:
                                    case LITERAL_minutes:
                                    case LITERAL_hours:
                                    case LITERAL_days: {
                                        time_unit_spec();
                                        astFactory.addASTChild(currentAST, returnAST);
                                        {
                                            switch (LA(1)) {
                                                case LITERAL_max: {
                                                    AST tmp123_AST = null;
                                                    tmp123_AST = astFactory.create(LT(1));
                                                    astFactory.addASTChild(currentAST, tmp123_AST);
                                                    match(LITERAL_max);
                                                    AST tmp124_AST = null;
                                                    tmp124_AST = astFactory.create(LT(1));
                                                    astFactory.addASTChild(currentAST, tmp124_AST);
                                                    match(NUMBER);
                                                    break;
                                                }
                                                case LITERAL_where:
                                                case CLOSE_PAREN:
                                                case QUOTED_STRING:
                                                case LITERAL_with: {
                                                    break;
                                                }
                                                default: {
                                                    throw new NoViableAltException(LT(1),
                                                            getFilename());
                                                }
                                            }
                                        }
                                        break;
                                    }
                                    case LITERAL_where:
                                    case CLOSE_PAREN:
                                    case QUOTED_STRING:
                                    case LITERAL_with: {
                                        break;
                                    }
                                    default: {
                                        throw new NoViableAltException(LT(1), getFilename());
                                    }
                                }
                            }
                            {
                                if ((LA(1) == QUOTED_STRING) && (_tokenSet_47.member(LA(2)))
                                        && (_tokenSet_48.member(LA(3)))
                                        && (_tokenSet_49.member(LA(4)))) {
                                    AST tmp125_AST = null;
                                    tmp125_AST = astFactory.create(LT(1));
                                    astFactory.addASTChild(currentAST, tmp125_AST);
                                    match(QUOTED_STRING);
                                }
                                else if ((_tokenSet_47.member(LA(1)))
                                        && (_tokenSet_48.member(LA(2)))
                                        && (_tokenSet_49.member(LA(3)))
                                        && (_tokenSet_50.member(LA(4)))) {
                                }
                                else {
                                    throw new NoViableAltException(LT(1), getFilename());
                                }

                            }
                        }
                        break;
                    }
                    case LITERAL_latest: {
                        {
                            AST tmp126_AST = null;
                            tmp126_AST = astFactory.create(LT(1));
                            astFactory.addASTChild(currentAST, tmp126_AST);
                            match(LITERAL_latest);
                            AST tmp127_AST = null;
                            tmp127_AST = astFactory.create(LT(1));
                            astFactory.addASTChild(currentAST, tmp127_AST);
                            match(NUMBER);
                        }
                        break;
                    }
                    default: {
                        throw new NoViableAltException(LT(1), getFilename());
                    }
                }
            }
            {
                switch (LA(1)) {
                    case QUOTED_STRING: {
                        AST tmp128_AST = null;
                        tmp128_AST = astFactory.create(LT(1));
                        astFactory.addASTChild(currentAST, tmp128_AST);
                        match(QUOTED_STRING);
                        break;
                    }
                    case LITERAL_where:
                    case CLOSE_PAREN:
                    case LITERAL_with: {
                        break;
                    }
                    default: {
                        throw new NoViableAltException(LT(1), getFilename());
                    }
                }
            }
        }
        if (inputState.guessing == 0) {
            window_function_AST = (AST) currentAST.root;
            window_function_AST = (AST) astFactory
                    .make((new ASTArray(2)).add(
                            astFactory.create(WINDOW_FUNCTION, "window_function")).add(
                            window_function_AST));
            currentAST.root = window_function_AST;
            currentAST.child = window_function_AST != null
                    && window_function_AST.getFirstChild() != null ? window_function_AST
                    .getFirstChild() : window_function_AST;
            currentAST.advanceChildToEnd();
        }
        window_function_AST = (AST) currentAST.root;
        returnAST = window_function_AST;
    }

    public final void aggregate_function() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST aggregate_function_AST = null;

        {
            AST tmp129_AST = null;
            tmp129_AST = astFactory.create(LT(1));
            astFactory.addASTChild(currentAST, tmp129_AST);
            match(LITERAL_with);
            {
                switch (LA(1)) {
                    case LITERAL_pinned: {
                        AST tmp130_AST = null;
                        tmp130_AST = astFactory.create(LT(1));
                        astFactory.addASTChild(currentAST, tmp130_AST);
                        match(LITERAL_pinned);
                        break;
                    }
                    case LITERAL_avg:
                    case LITERAL_count:
                    case LITERAL_max:
                    case LITERAL_min:
                    case LITERAL_stddev:
                    case LITERAL_sum:
                    case LITERAL_variance:
                    case LITERAL_geomean:
                    case LITERAL_kurtosis:
                    case LITERAL_median:
                    case LITERAL_skewness:
                    case LITERAL_sumsq:
                    case LITERAL_custom: {
                        break;
                    }
                    default: {
                        throw new NoViableAltException(LT(1), getFilename());
                    }
                }
            }
            aggregate_function_spec();
            astFactory.addASTChild(currentAST, returnAST);
            {
                _loop168: do {
                    if ((LA(1) == COMMA)) {
                        AST tmp131_AST = null;
                        tmp131_AST = astFactory.create(LT(1));
                        astFactory.addASTChild(currentAST, tmp131_AST);
                        match(COMMA);
                        aggregate_function_spec();
                        astFactory.addASTChild(currentAST, returnAST);
                    }
                    else {
                        break _loop168;
                    }

                } while (true);
            }
        }
        if (inputState.guessing == 0) {
            aggregate_function_AST = (AST) currentAST.root;
            aggregate_function_AST = (AST) astFactory.make((new ASTArray(2)).add(
                    astFactory.create(AGGREGATE_FUNCTION, "aggregate_function")).add(
                    aggregate_function_AST));
            currentAST.root = aggregate_function_AST;
            currentAST.child = aggregate_function_AST != null
                    && aggregate_function_AST.getFirstChild() != null ? aggregate_function_AST
                    .getFirstChild() : aggregate_function_AST;
            currentAST.advanceChildToEnd();
        }
        aggregate_function_AST = (AST) currentAST.root;
        returnAST = aggregate_function_AST;
    }

    public final void aggregate_function_spec() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST aggregate_function_spec_AST = null;

        {
            {
                switch (LA(1)) {
                    case LITERAL_avg:
                    case LITERAL_count:
                    case LITERAL_max:
                    case LITERAL_min:
                    case LITERAL_stddev:
                    case LITERAL_sum:
                    case LITERAL_variance:
                    case LITERAL_geomean:
                    case LITERAL_kurtosis:
                    case LITERAL_median:
                    case LITERAL_skewness:
                    case LITERAL_sumsq: {
                        {
                            {
                                switch (LA(1)) {
                                    case LITERAL_avg: {
                                        AST tmp132_AST = null;
                                        tmp132_AST = astFactory.create(LT(1));
                                        astFactory.addASTChild(currentAST, tmp132_AST);
                                        match(LITERAL_avg);
                                        break;
                                    }
                                    case LITERAL_count: {
                                        AST tmp133_AST = null;
                                        tmp133_AST = astFactory.create(LT(1));
                                        astFactory.addASTChild(currentAST, tmp133_AST);
                                        match(LITERAL_count);
                                        break;
                                    }
                                    case LITERAL_geomean: {
                                        AST tmp134_AST = null;
                                        tmp134_AST = astFactory.create(LT(1));
                                        astFactory.addASTChild(currentAST, tmp134_AST);
                                        match(LITERAL_geomean);
                                        break;
                                    }
                                    case LITERAL_kurtosis: {
                                        AST tmp135_AST = null;
                                        tmp135_AST = astFactory.create(LT(1));
                                        astFactory.addASTChild(currentAST, tmp135_AST);
                                        match(LITERAL_kurtosis);
                                        break;
                                    }
                                    case LITERAL_max: {
                                        AST tmp136_AST = null;
                                        tmp136_AST = astFactory.create(LT(1));
                                        astFactory.addASTChild(currentAST, tmp136_AST);
                                        match(LITERAL_max);
                                        break;
                                    }
                                    case LITERAL_median: {
                                        AST tmp137_AST = null;
                                        tmp137_AST = astFactory.create(LT(1));
                                        astFactory.addASTChild(currentAST, tmp137_AST);
                                        match(LITERAL_median);
                                        break;
                                    }
                                    case LITERAL_min: {
                                        AST tmp138_AST = null;
                                        tmp138_AST = astFactory.create(LT(1));
                                        astFactory.addASTChild(currentAST, tmp138_AST);
                                        match(LITERAL_min);
                                        break;
                                    }
                                    case LITERAL_skewness: {
                                        AST tmp139_AST = null;
                                        tmp139_AST = astFactory.create(LT(1));
                                        astFactory.addASTChild(currentAST, tmp139_AST);
                                        match(LITERAL_skewness);
                                        break;
                                    }
                                    case LITERAL_stddev: {
                                        AST tmp140_AST = null;
                                        tmp140_AST = astFactory.create(LT(1));
                                        astFactory.addASTChild(currentAST, tmp140_AST);
                                        match(LITERAL_stddev);
                                        break;
                                    }
                                    case LITERAL_sum: {
                                        AST tmp141_AST = null;
                                        tmp141_AST = astFactory.create(LT(1));
                                        astFactory.addASTChild(currentAST, tmp141_AST);
                                        match(LITERAL_sum);
                                        break;
                                    }
                                    case LITERAL_sumsq: {
                                        AST tmp142_AST = null;
                                        tmp142_AST = astFactory.create(LT(1));
                                        astFactory.addASTChild(currentAST, tmp142_AST);
                                        match(LITERAL_sumsq);
                                        break;
                                    }
                                    case LITERAL_variance: {
                                        AST tmp143_AST = null;
                                        tmp143_AST = astFactory.create(LT(1));
                                        astFactory.addASTChild(currentAST, tmp143_AST);
                                        match(LITERAL_variance);
                                        break;
                                    }
                                    default: {
                                        throw new NoViableAltException(LT(1), getFilename());
                                    }
                                }
                            }
                            AST tmp144_AST = null;
                            tmp144_AST = astFactory.create(LT(1));
                            astFactory.addASTChild(currentAST, tmp144_AST);
                            match(OPEN_PAREN);
                            AST tmp145_AST = null;
                            tmp145_AST = astFactory.create(LT(1));
                            astFactory.addASTChild(currentAST, tmp145_AST);
                            match(IDENTIFIER);
                            {
                                switch (LA(1)) {
                                    case DOLLAR: {
                                        AST tmp146_AST = null;
                                        tmp146_AST = astFactory.create(LT(1));
                                        astFactory.addASTChild(currentAST, tmp146_AST);
                                        match(DOLLAR);
                                        AST tmp147_AST = null;
                                        tmp147_AST = astFactory.create(LT(1));
                                        astFactory.addASTChild(currentAST, tmp147_AST);
                                        match(LITERAL_diff);
                                        {
                                            switch (LA(1)) {
                                                case QUOTED_STRING: {
                                                    AST tmp148_AST = null;
                                                    tmp148_AST = astFactory.create(LT(1));
                                                    astFactory.addASTChild(currentAST, tmp148_AST);
                                                    match(QUOTED_STRING);
                                                    break;
                                                }
                                                case CLOSE_PAREN: {
                                                    break;
                                                }
                                                default: {
                                                    throw new NoViableAltException(LT(1),
                                                            getFilename());
                                                }
                                            }
                                        }
                                        break;
                                    }
                                    case CLOSE_PAREN: {
                                        break;
                                    }
                                    default: {
                                        throw new NoViableAltException(LT(1), getFilename());
                                    }
                                }
                            }
                            AST tmp149_AST = null;
                            tmp149_AST = astFactory.create(LT(1));
                            astFactory.addASTChild(currentAST, tmp149_AST);
                            match(CLOSE_PAREN);
                        }
                        break;
                    }
                    case LITERAL_custom: {
                        {
                            AST tmp150_AST = null;
                            tmp150_AST = astFactory.create(LT(1));
                            astFactory.addASTChild(currentAST, tmp150_AST);
                            match(LITERAL_custom);
                            AST tmp151_AST = null;
                            tmp151_AST = astFactory.create(LT(1));
                            astFactory.addASTChild(currentAST, tmp151_AST);
                            match(OPEN_PAREN);
                            AST tmp152_AST = null;
                            tmp152_AST = astFactory.create(LT(1));
                            astFactory.addASTChild(currentAST, tmp152_AST);
                            match(IDENTIFIER);
                            {
                                _loop179: do {
                                    if ((LA(1) == COMMA)) {
                                        AST tmp153_AST = null;
                                        tmp153_AST = astFactory.create(LT(1));
                                        astFactory.addASTChild(currentAST, tmp153_AST);
                                        match(COMMA);
                                        {
                                            switch (LA(1)) {
                                                case IDENTIFIER: {
                                                    AST tmp154_AST = null;
                                                    tmp154_AST = astFactory.create(LT(1));
                                                    astFactory.addASTChild(currentAST, tmp154_AST);
                                                    match(IDENTIFIER);
                                                    break;
                                                }
                                                case QUOTED_STRING: {
                                                    AST tmp155_AST = null;
                                                    tmp155_AST = astFactory.create(LT(1));
                                                    astFactory.addASTChild(currentAST, tmp155_AST);
                                                    match(QUOTED_STRING);
                                                    break;
                                                }
                                                default: {
                                                    throw new NoViableAltException(LT(1),
                                                            getFilename());
                                                }
                                            }
                                        }
                                    }
                                    else {
                                        break _loop179;
                                    }

                                } while (true);
                            }
                            AST tmp156_AST = null;
                            tmp156_AST = astFactory.create(LT(1));
                            astFactory.addASTChild(currentAST, tmp156_AST);
                            match(CLOSE_PAREN);
                        }
                        break;
                    }
                    default: {
                        throw new NoViableAltException(LT(1), getFilename());
                    }
                }
            }
            {
                if ((LA(1) == LITERAL_entrance) && (LA(2) == LITERAL_only)
                        && (_tokenSet_21.member(LA(3))) && (_tokenSet_51.member(LA(4)))) {
                    AST tmp157_AST = null;
                    tmp157_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp157_AST);
                    match(LITERAL_entrance);
                    AST tmp158_AST = null;
                    tmp158_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp158_AST);
                    match(LITERAL_only);
                }
                else if ((_tokenSet_21.member(LA(1))) && (_tokenSet_51.member(LA(2)))
                        && (_tokenSet_52.member(LA(3))) && (_tokenSet_49.member(LA(4)))) {
                }
                else {
                    throw new NoViableAltException(LT(1), getFilename());
                }

            }
            alias();
            astFactory.addASTChild(currentAST, returnAST);
        }
        if (inputState.guessing == 0) {
            aggregate_function_spec_AST = (AST) currentAST.root;
            aggregate_function_spec_AST = (AST) astFactory.make((new ASTArray(2)).add(
                    astFactory.create(AGGREGATE_FUNCTION_SPEC, "aggregate_function_spec")).add(
                    aggregate_function_spec_AST));
            currentAST.root = aggregate_function_spec_AST;
            currentAST.child = aggregate_function_spec_AST != null
                    && aggregate_function_spec_AST.getFirstChild() != null ? aggregate_function_spec_AST
                    .getFirstChild()
                    : aggregate_function_spec_AST;
            currentAST.advanceChildToEnd();
        }
        aggregate_function_spec_AST = (AST) currentAST.root;
        returnAST = aggregate_function_spec_AST;
    }

    public final void time_unit_spec() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST time_unit_spec_AST = null;

        {
            switch (LA(1)) {
                case LITERAL_milliseconds: {
                    AST tmp159_AST = null;
                    tmp159_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp159_AST);
                    match(LITERAL_milliseconds);
                    break;
                }
                case LITERAL_seconds: {
                    AST tmp160_AST = null;
                    tmp160_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp160_AST);
                    match(LITERAL_seconds);
                    break;
                }
                case LITERAL_minutes: {
                    AST tmp161_AST = null;
                    tmp161_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp161_AST);
                    match(LITERAL_minutes);
                    break;
                }
                case LITERAL_hours: {
                    AST tmp162_AST = null;
                    tmp162_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp162_AST);
                    match(LITERAL_hours);
                    break;
                }
                case LITERAL_days: {
                    AST tmp163_AST = null;
                    tmp163_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp163_AST);
                    match(LITERAL_days);
                    break;
                }
                default: {
                    throw new NoViableAltException(LT(1), getFilename());
                }
            }
        }
        if (inputState.guessing == 0) {
            time_unit_spec_AST = (AST) currentAST.root;
            time_unit_spec_AST = (AST) astFactory.make((new ASTArray(2)).add(
                    astFactory.create(TIME_UNIT_SPEC, "time_unit_spec")).add(time_unit_spec_AST));
            currentAST.root = time_unit_spec_AST;
            currentAST.child = time_unit_spec_AST != null
                    && time_unit_spec_AST.getFirstChild() != null ? time_unit_spec_AST
                    .getFirstChild() : time_unit_spec_AST;
            currentAST.advanceChildToEnd();
        }
        time_unit_spec_AST = (AST) currentAST.root;
        returnAST = time_unit_spec_AST;
    }

    public final void table_alias() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST table_alias_AST = null;

        {
            if ((_tokenSet_14.member(LA(1))) && (LA(2) == DOT)) {
                schema_name();
                astFactory.addASTChild(currentAST, returnAST);
                AST tmp164_AST = null;
                tmp164_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp164_AST);
                match(DOT);
            }
            else if ((_tokenSet_14.member(LA(1))) && (_tokenSet_53.member(LA(2)))) {
            }
            else {
                throw new NoViableAltException(LT(1), getFilename());
            }

        }
        table_name();
        astFactory.addASTChild(currentAST, returnAST);
        {
            switch (LA(1)) {
                case LITERAL_all:
                case LITERAL_self:
                case LITERAL_as:
                case QUOTED_STRING:
                case LITERAL_avg:
                case LITERAL_count:
                case LITERAL_max:
                case LITERAL_min:
                case LITERAL_stddev:
                case LITERAL_sum:
                case LITERAL_variance:
                case LITERAL_filter:
                case LITERAL_using:
                case LITERAL_partition:
                case LITERAL_by:
                case IDENTIFIER:
                case LITERAL_store:
                case LITERAL_random:
                case LITERAL_lowest:
                case LITERAL_highest:
                case LITERAL_with:
                case LITERAL_group:
                case LITERAL_pinned:
                case LITERAL_geomean:
                case LITERAL_kurtosis:
                case LITERAL_median:
                case LITERAL_skewness:
                case LITERAL_sumsq:
                case LITERAL_diff:
                case LITERAL_custom:
                case LITERAL_entrance:
                case LITERAL_only:
                case LITERAL_last:
                case LITERAL_latest:
                case LITERAL_milliseconds:
                case LITERAL_seconds:
                case LITERAL_minutes:
                case LITERAL_hours:
                case LITERAL_days:
                case LITERAL_alert:
                case LITERAL_correlate:
                case LITERAL_present:
                case LITERAL_is:
                case LITERAL_row_status:
                case LITERAL_new:
                case LITERAL_dead:
                case LITERAL_union:
                case LITERAL_minus:
                case LITERAL_except:
                case LITERAL_intersect:
                case LITERAL_limit:
                case LITERAL_offset:
                case LITERAL_current_timestamp:
                case LITERAL_of: {
                    alias();
                    astFactory.addASTChild(currentAST, returnAST);
                    break;
                }
                case EOF: {
                    break;
                }
                default: {
                    throw new NoViableAltException(LT(1), getFilename());
                }
            }
        }
        table_alias_AST = (AST) currentAST.root;
        returnAST = table_alias_AST;
    }

    public final void partition_column_name_list() throws RecognitionException,
            TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST partition_column_name_list_AST = null;

        AST tmp165_AST = null;
        tmp165_AST = astFactory.create(LT(1));
        astFactory.addASTChild(currentAST, tmp165_AST);
        match(IDENTIFIER);
        AST tmp166_AST = null;
        tmp166_AST = astFactory.create(LT(1));
        astFactory.addASTChild(currentAST, tmp166_AST);
        match(DOT);
        AST tmp167_AST = null;
        tmp167_AST = astFactory.create(LT(1));
        astFactory.addASTChild(currentAST, tmp167_AST);
        match(IDENTIFIER);
        AST tmp168_AST = null;
        tmp168_AST = astFactory.create(LT(1));
        astFactory.addASTChild(currentAST, tmp168_AST);
        match(LITERAL_as);
        AST tmp169_AST = null;
        tmp169_AST = astFactory.create(LT(1));
        astFactory.addASTChild(currentAST, tmp169_AST);
        match(IDENTIFIER);
        {
            _loop198: do {
                if ((LA(1) == COMMA)) {
                    AST tmp170_AST = null;
                    tmp170_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp170_AST);
                    match(COMMA);
                    AST tmp171_AST = null;
                    tmp171_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp171_AST);
                    match(IDENTIFIER);
                    AST tmp172_AST = null;
                    tmp172_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp172_AST);
                    match(DOT);
                    AST tmp173_AST = null;
                    tmp173_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp173_AST);
                    match(IDENTIFIER);
                    AST tmp174_AST = null;
                    tmp174_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp174_AST);
                    match(LITERAL_as);
                    AST tmp175_AST = null;
                    tmp175_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp175_AST);
                    match(IDENTIFIER);
                }
                else {
                    break _loop198;
                }

            } while (true);
        }
        if (inputState.guessing == 0) {
            partition_column_name_list_AST = (AST) currentAST.root;
            partition_column_name_list_AST = (AST) astFactory.make((new ASTArray(2)).add(
                    astFactory.create(PARTITION_COLUMN_NAME_LIST, "partition_column_name_list"))
                    .add(partition_column_name_list_AST));
            currentAST.root = partition_column_name_list_AST;
            currentAST.child = partition_column_name_list_AST != null
                    && partition_column_name_list_AST.getFirstChild() != null ? partition_column_name_list_AST
                    .getFirstChild()
                    : partition_column_name_list_AST;
            currentAST.advanceChildToEnd();
        }
        partition_column_name_list_AST = (AST) currentAST.root;
        returnAST = partition_column_name_list_AST;
    }

    public final void using_list() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST using_list_AST = null;

        AST tmp176_AST = null;
        tmp176_AST = astFactory.create(LT(1));
        astFactory.addASTChild(currentAST, tmp176_AST);
        match(LITERAL_using);
        {
            using_spec();
            astFactory.addASTChild(currentAST, returnAST);
            {
                int _cnt202 = 0;
                _loop202: do {
                    if ((LA(1) == COMMA)) {
                        AST tmp177_AST = null;
                        tmp177_AST = astFactory.create(LT(1));
                        astFactory.addASTChild(currentAST, tmp177_AST);
                        match(COMMA);
                        using_spec();
                        astFactory.addASTChild(currentAST, returnAST);
                    }
                    else {
                        if (_cnt202 >= 1) {
                            break _loop202;
                        }
                        else {
                            throw new NoViableAltException(LT(1), getFilename());
                        }
                    }

                    _cnt202++;
                } while (true);
            }
        }
        if (inputState.guessing == 0) {
            using_list_AST = (AST) currentAST.root;
            using_list_AST = (AST) astFactory.make((new ASTArray(2)).add(
                    astFactory.create(USING_LIST, "using_list")).add(using_list_AST));
            currentAST.root = using_list_AST;
            currentAST.child = using_list_AST != null && using_list_AST.getFirstChild() != null ? using_list_AST
                    .getFirstChild()
                    : using_list_AST;
            currentAST.advanceChildToEnd();
        }
        using_list_AST = (AST) currentAST.root;
        returnAST = using_list_AST;
    }

    public final void present_list() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST present_list_AST = null;

        AST tmp178_AST = null;
        tmp178_AST = astFactory.create(LT(1));
        astFactory.addASTChild(currentAST, tmp178_AST);
        match(LITERAL_when);
        present_spec();
        astFactory.addASTChild(currentAST, returnAST);
        {
            _loop206: do {
                if ((LA(1) == LITERAL_or)) {
                    AST tmp179_AST = null;
                    tmp179_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp179_AST);
                    match(LITERAL_or);
                    present_spec();
                    astFactory.addASTChild(currentAST, returnAST);
                }
                else {
                    break _loop206;
                }

            } while (true);
        }
        if (inputState.guessing == 0) {
            present_list_AST = (AST) currentAST.root;
            present_list_AST = (AST) astFactory.make((new ASTArray(2)).add(
                    astFactory.create(PRESENT_LIST, "present_list")).add(present_list_AST));
            currentAST.root = present_list_AST;
            currentAST.child = present_list_AST != null && present_list_AST.getFirstChild() != null ? present_list_AST
                    .getFirstChild()
                    : present_list_AST;
            currentAST.advanceChildToEnd();
        }
        present_list_AST = (AST) currentAST.root;
        returnAST = present_list_AST;
    }

    public final void using_spec() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST using_spec_AST = null;

        partition_spec_list();
        astFactory.addASTChild(currentAST, returnAST);
        alias();
        astFactory.addASTChild(currentAST, returnAST);
        AST tmp180_AST = null;
        tmp180_AST = astFactory.create(LT(1));
        astFactory.addASTChild(currentAST, tmp180_AST);
        match(LITERAL_correlate);
        AST tmp181_AST = null;
        tmp181_AST = astFactory.create(LT(1));
        astFactory.addASTChild(currentAST, tmp181_AST);
        match(LITERAL_on);
        identifier();
        astFactory.addASTChild(currentAST, returnAST);
        if (inputState.guessing == 0) {
            using_spec_AST = (AST) currentAST.root;
            using_spec_AST = (AST) astFactory.make((new ASTArray(2)).add(
                    astFactory.create(USING_SPEC, "using_spec")).add(using_spec_AST));
            currentAST.root = using_spec_AST;
            currentAST.child = using_spec_AST != null && using_spec_AST.getFirstChild() != null ? using_spec_AST
                    .getFirstChild()
                    : using_spec_AST;
            currentAST.advanceChildToEnd();
        }
        using_spec_AST = (AST) currentAST.root;
        returnAST = using_spec_AST;
    }

    public final void present_spec() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST present_spec_AST = null;

        AST tmp182_AST = null;
        tmp182_AST = astFactory.create(LT(1));
        astFactory.addASTChild(currentAST, tmp182_AST);
        match(LITERAL_present);
        AST tmp183_AST = null;
        tmp183_AST = astFactory.create(LT(1));
        astFactory.addASTChild(currentAST, tmp183_AST);
        match(OPEN_PAREN);
        AST tmp184_AST = null;
        tmp184_AST = astFactory.create(LT(1));
        astFactory.addASTChild(currentAST, tmp184_AST);
        match(IDENTIFIER);
        {
            int _cnt210 = 0;
            _loop210: do {
                if ((LA(1) == LITERAL_and)) {
                    AST tmp185_AST = null;
                    tmp185_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp185_AST);
                    match(LITERAL_and);
                    {
                        switch (LA(1)) {
                            case LITERAL_not: {
                                AST tmp186_AST = null;
                                tmp186_AST = astFactory.create(LT(1));
                                astFactory.addASTChild(currentAST, tmp186_AST);
                                match(LITERAL_not);
                                break;
                            }
                            case IDENTIFIER: {
                                break;
                            }
                            default: {
                                throw new NoViableAltException(LT(1), getFilename());
                            }
                        }
                    }
                    AST tmp187_AST = null;
                    tmp187_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp187_AST);
                    match(IDENTIFIER);
                }
                else {
                    if (_cnt210 >= 1) {
                        break _loop210;
                    }
                    else {
                        throw new NoViableAltException(LT(1), getFilename());
                    }
                }

                _cnt210++;
            } while (true);
        }
        AST tmp188_AST = null;
        tmp188_AST = astFactory.create(LT(1));
        astFactory.addASTChild(currentAST, tmp188_AST);
        match(CLOSE_PAREN);
        if (inputState.guessing == 0) {
            present_spec_AST = (AST) currentAST.root;
            present_spec_AST = (AST) astFactory.make((new ASTArray(2)).add(
                    astFactory.create(PRESENT_SPEC, "present_spec")).add(present_spec_AST));
            currentAST.root = present_spec_AST;
            currentAST.child = present_spec_AST != null && present_spec_AST.getFirstChild() != null ? present_spec_AST
                    .getFirstChild()
                    : present_spec_AST;
            currentAST.advanceChildToEnd();
        }
        present_spec_AST = (AST) currentAST.root;
        returnAST = present_spec_AST;
    }

    public final void logical_term() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST logical_term_AST = null;

        logical_factor();
        astFactory.addASTChild(currentAST, returnAST);
        {
            _loop216: do {
                if ((LA(1) == LITERAL_and) && (_tokenSet_29.member(LA(2)))
                        && (_tokenSet_30.member(LA(3))) && (_tokenSet_31.member(LA(4)))) {
                    AST tmp189_AST = null;
                    tmp189_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp189_AST);
                    match(LITERAL_and);
                    logical_factor();
                    astFactory.addASTChild(currentAST, returnAST);
                }
                else {
                    break _loop216;
                }

            } while (true);
        }
        logical_term_AST = (AST) currentAST.root;
        returnAST = logical_term_AST;
    }

    public final void logical_factor() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST logical_factor_AST = null;

        boolean synPredMatched219 = false;
        if (((_tokenSet_7.member(LA(1))) && (_tokenSet_54.member(LA(2)))
                && (_tokenSet_55.member(LA(3))) && (_tokenSet_56.member(LA(4))))) {
            int _m219 = mark();
            synPredMatched219 = true;
            inputState.guessing++;
            try {
                {
                    exp_simple();
                    comparison_op();
                    exp_simple();
                }
            }
            catch (RecognitionException pe) {
                synPredMatched219 = false;
            }
            rewind(_m219);
            inputState.guessing--;
        }
        if (synPredMatched219) {
            {
                exp_simple();
                astFactory.addASTChild(currentAST, returnAST);
                comparison_op();
                astFactory.addASTChild(currentAST, returnAST);
                exp_simple();
                astFactory.addASTChild(currentAST, returnAST);
            }
            logical_factor_AST = (AST) currentAST.root;
        }
        else {
            boolean synPredMatched223 = false;
            if (((_tokenSet_7.member(LA(1))) && (_tokenSet_57.member(LA(2)))
                    && (_tokenSet_58.member(LA(3))) && (_tokenSet_59.member(LA(4))))) {
                int _m223 = mark();
                synPredMatched223 = true;
                inputState.guessing++;
                try {
                    {
                        exp_simple();
                        {
                            switch (LA(1)) {
                                case LITERAL_not: {
                                    match(LITERAL_not);
                                    break;
                                }
                                case LITERAL_in: {
                                    break;
                                }
                                default: {
                                    throw new NoViableAltException(LT(1), getFilename());
                                }
                            }
                        }
                        match(LITERAL_in);
                    }
                }
                catch (RecognitionException pe) {
                    synPredMatched223 = false;
                }
                rewind(_m223);
                inputState.guessing--;
            }
            if (synPredMatched223) {
                exp_simple();
                astFactory.addASTChild(currentAST, returnAST);
                {
                    switch (LA(1)) {
                        case LITERAL_not: {
                            AST tmp190_AST = null;
                            tmp190_AST = astFactory.create(LT(1));
                            astFactory.addASTChild(currentAST, tmp190_AST);
                            match(LITERAL_not);
                            break;
                        }
                        case LITERAL_in: {
                            break;
                        }
                        default: {
                            throw new NoViableAltException(LT(1), getFilename());
                        }
                    }
                }
                AST tmp191_AST = null;
                tmp191_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp191_AST);
                match(LITERAL_in);
                exp_set();
                astFactory.addASTChild(currentAST, returnAST);
                logical_factor_AST = (AST) currentAST.root;
            }
            else {
                boolean synPredMatched227 = false;
                if (((_tokenSet_7.member(LA(1))) && (_tokenSet_60.member(LA(2)))
                        && (_tokenSet_61.member(LA(3))) && (_tokenSet_62.member(LA(4))))) {
                    int _m227 = mark();
                    synPredMatched227 = true;
                    inputState.guessing++;
                    try {
                        {
                            exp_simple();
                            {
                                switch (LA(1)) {
                                    case LITERAL_not: {
                                        match(LITERAL_not);
                                        break;
                                    }
                                    case LITERAL_like: {
                                        break;
                                    }
                                    default: {
                                        throw new NoViableAltException(LT(1), getFilename());
                                    }
                                }
                            }
                            match(LITERAL_like);
                        }
                    }
                    catch (RecognitionException pe) {
                        synPredMatched227 = false;
                    }
                    rewind(_m227);
                    inputState.guessing--;
                }
                if (synPredMatched227) {
                    exp_simple();
                    astFactory.addASTChild(currentAST, returnAST);
                    {
                        switch (LA(1)) {
                            case LITERAL_not: {
                                AST tmp192_AST = null;
                                tmp192_AST = astFactory.create(LT(1));
                                astFactory.addASTChild(currentAST, tmp192_AST);
                                match(LITERAL_not);
                                break;
                            }
                            case LITERAL_like: {
                                break;
                            }
                            default: {
                                throw new NoViableAltException(LT(1), getFilename());
                            }
                        }
                    }
                    AST tmp193_AST = null;
                    tmp193_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp193_AST);
                    match(LITERAL_like);
                    expression();
                    astFactory.addASTChild(currentAST, returnAST);
                    {
                        switch (LA(1)) {
                            case LITERAL_escape: {
                                AST tmp194_AST = null;
                                tmp194_AST = astFactory.create(LT(1));
                                astFactory.addASTChild(currentAST, tmp194_AST);
                                match(LITERAL_escape);
                                AST tmp195_AST = null;
                                tmp195_AST = astFactory.create(LT(1));
                                astFactory.addASTChild(currentAST, tmp195_AST);
                                match(QUOTED_STRING);
                                break;
                            }
                            case SEMI:
                            case LITERAL_where:
                            case LITERAL_then:
                            case CLOSE_PAREN:
                            case LITERAL_group:
                            case LITERAL_or:
                            case LITERAL_and:
                            case LITERAL_union:
                            case LITERAL_minus:
                            case LITERAL_except:
                            case LITERAL_intersect:
                            case LITERAL_order:
                            case LITERAL_limit: {
                                break;
                            }
                            default: {
                                throw new NoViableAltException(LT(1), getFilename());
                            }
                        }
                    }
                    logical_factor_AST = (AST) currentAST.root;
                }
                else {
                    boolean synPredMatched232 = false;
                    if (((_tokenSet_7.member(LA(1))) && (_tokenSet_63.member(LA(2)))
                            && (_tokenSet_64.member(LA(3))) && (_tokenSet_65.member(LA(4))))) {
                        int _m232 = mark();
                        synPredMatched232 = true;
                        inputState.guessing++;
                        try {
                            {
                                exp_simple();
                                {
                                    switch (LA(1)) {
                                        case LITERAL_not: {
                                            match(LITERAL_not);
                                            break;
                                        }
                                        case LITERAL_between: {
                                            break;
                                        }
                                        default: {
                                            throw new NoViableAltException(LT(1), getFilename());
                                        }
                                    }
                                }
                                match(LITERAL_between);
                            }
                        }
                        catch (RecognitionException pe) {
                            synPredMatched232 = false;
                        }
                        rewind(_m232);
                        inputState.guessing--;
                    }
                    if (synPredMatched232) {
                        exp_simple();
                        astFactory.addASTChild(currentAST, returnAST);
                        {
                            switch (LA(1)) {
                                case LITERAL_not: {
                                    AST tmp196_AST = null;
                                    tmp196_AST = astFactory.create(LT(1));
                                    astFactory.addASTChild(currentAST, tmp196_AST);
                                    match(LITERAL_not);
                                    break;
                                }
                                case LITERAL_between: {
                                    break;
                                }
                                default: {
                                    throw new NoViableAltException(LT(1), getFilename());
                                }
                            }
                        }
                        AST tmp197_AST = null;
                        tmp197_AST = astFactory.create(LT(1));
                        astFactory.addASTChild(currentAST, tmp197_AST);
                        match(LITERAL_between);
                        exp_simple();
                        astFactory.addASTChild(currentAST, returnAST);
                        AST tmp198_AST = null;
                        tmp198_AST = astFactory.create(LT(1));
                        astFactory.addASTChild(currentAST, tmp198_AST);
                        match(LITERAL_and);
                        exp_simple();
                        astFactory.addASTChild(currentAST, returnAST);
                        logical_factor_AST = (AST) currentAST.root;
                    }
                    else {
                        boolean synPredMatched236 = false;
                        if (((_tokenSet_7.member(LA(1))) && (_tokenSet_66.member(LA(2)))
                                && (_tokenSet_67.member(LA(3))) && (_tokenSet_68.member(LA(4))))) {
                            int _m236 = mark();
                            synPredMatched236 = true;
                            inputState.guessing++;
                            try {
                                {
                                    exp_simple();
                                    match(LITERAL_is);
                                    {
                                        switch (LA(1)) {
                                            case LITERAL_not: {
                                                match(LITERAL_not);
                                                break;
                                            }
                                            case LITERAL_null: {
                                                break;
                                            }
                                            default: {
                                                throw new NoViableAltException(LT(1), getFilename());
                                            }
                                        }
                                    }
                                    match(LITERAL_null);
                                }
                            }
                            catch (RecognitionException pe) {
                                synPredMatched236 = false;
                            }
                            rewind(_m236);
                            inputState.guessing--;
                        }
                        if (synPredMatched236) {
                            exp_simple();
                            astFactory.addASTChild(currentAST, returnAST);
                            AST tmp199_AST = null;
                            tmp199_AST = astFactory.create(LT(1));
                            astFactory.addASTChild(currentAST, tmp199_AST);
                            match(LITERAL_is);
                            {
                                switch (LA(1)) {
                                    case LITERAL_not: {
                                        AST tmp200_AST = null;
                                        tmp200_AST = astFactory.create(LT(1));
                                        astFactory.addASTChild(currentAST, tmp200_AST);
                                        match(LITERAL_not);
                                        break;
                                    }
                                    case LITERAL_null: {
                                        break;
                                    }
                                    default: {
                                        throw new NoViableAltException(LT(1), getFilename());
                                    }
                                }
                            }
                            AST tmp201_AST = null;
                            tmp201_AST = astFactory.create(LT(1));
                            astFactory.addASTChild(currentAST, tmp201_AST);
                            match(LITERAL_null);
                            logical_factor_AST = (AST) currentAST.root;
                        }
                        else {
                            boolean synPredMatched239 = false;
                            if (((_tokenSet_69.member(LA(1))) && (_tokenSet_70.member(LA(2)))
                                    && (_tokenSet_71.member(LA(3))) && (_tokenSet_72.member(LA(4))))) {
                                int _m239 = mark();
                                synPredMatched239 = true;
                                inputState.guessing++;
                                try {
                                    {
                                        quantified_factor();
                                    }
                                }
                                catch (RecognitionException pe) {
                                    synPredMatched239 = false;
                                }
                                rewind(_m239);
                                inputState.guessing--;
                            }
                            if (synPredMatched239) {
                                quantified_factor();
                                astFactory.addASTChild(currentAST, returnAST);
                                logical_factor_AST = (AST) currentAST.root;
                            }
                            else {
                                boolean synPredMatched241 = false;
                                if (((LA(1) == LITERAL_not) && (_tokenSet_29.member(LA(2)))
                                        && (_tokenSet_30.member(LA(3))) && (_tokenSet_31
                                        .member(LA(4))))) {
                                    int _m241 = mark();
                                    synPredMatched241 = true;
                                    inputState.guessing++;
                                    try {
                                        {
                                            match(LITERAL_not);
                                            condition();
                                        }
                                    }
                                    catch (RecognitionException pe) {
                                        synPredMatched241 = false;
                                    }
                                    rewind(_m241);
                                    inputState.guessing--;
                                }
                                if (synPredMatched241) {
                                    AST tmp202_AST = null;
                                    tmp202_AST = astFactory.create(LT(1));
                                    astFactory.addASTChild(currentAST, tmp202_AST);
                                    match(LITERAL_not);
                                    condition();
                                    astFactory.addASTChild(currentAST, returnAST);
                                    logical_factor_AST = (AST) currentAST.root;
                                }
                                else if ((LA(1) == OPEN_PAREN) && (_tokenSet_29.member(LA(2)))
                                        && (_tokenSet_30.member(LA(3)))
                                        && (_tokenSet_31.member(LA(4)))) {
                                    {
                                        AST tmp203_AST = null;
                                        tmp203_AST = astFactory.create(LT(1));
                                        astFactory.addASTChild(currentAST, tmp203_AST);
                                        match(OPEN_PAREN);
                                        condition();
                                        astFactory.addASTChild(currentAST, returnAST);
                                        AST tmp204_AST = null;
                                        tmp204_AST = astFactory.create(LT(1));
                                        astFactory.addASTChild(currentAST, tmp204_AST);
                                        match(CLOSE_PAREN);
                                    }
                                    logical_factor_AST = (AST) currentAST.root;
                                }
                                else if ((_tokenSet_73.member(LA(1)))
                                        && (LA(2) == DOT || LA(2) == LITERAL_row_status)
                                        && (_tokenSet_73.member(LA(3)))
                                        && (_tokenSet_74.member(LA(4)))) {
                                    row_status_clause();
                                    astFactory.addASTChild(currentAST, returnAST);
                                    logical_factor_AST = (AST) currentAST.root;
                                }
                                else {
                                    throw new NoViableAltException(LT(1), getFilename());
                                }
                            }
                        }
                    }
                }
            }
        }
        returnAST = logical_factor_AST;
    }

    public final void comparison_op() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST comparison_op_AST = null;

        switch (LA(1)) {
            case EQ: {
                AST tmp205_AST = null;
                tmp205_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp205_AST);
                match(EQ);
                comparison_op_AST = (AST) currentAST.root;
                break;
            }
            case LT: {
                AST tmp206_AST = null;
                tmp206_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp206_AST);
                match(LT);
                comparison_op_AST = (AST) currentAST.root;
                break;
            }
            case GT: {
                AST tmp207_AST = null;
                tmp207_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp207_AST);
                match(GT);
                comparison_op_AST = (AST) currentAST.root;
                break;
            }
            case NOT_EQ: {
                AST tmp208_AST = null;
                tmp208_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp208_AST);
                match(NOT_EQ);
                comparison_op_AST = (AST) currentAST.root;
                break;
            }
            case LE: {
                AST tmp209_AST = null;
                tmp209_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp209_AST);
                match(LE);
                comparison_op_AST = (AST) currentAST.root;
                break;
            }
            case GE: {
                AST tmp210_AST = null;
                tmp210_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp210_AST);
                match(GE);
                comparison_op_AST = (AST) currentAST.root;
                break;
            }
            default: {
                throw new NoViableAltException(LT(1), getFilename());
            }
        }
        returnAST = comparison_op_AST;
    }

    public final void exp_set() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST exp_set_AST = null;

        boolean synPredMatched269 = false;
        if (((_tokenSet_7.member(LA(1))) && (_tokenSet_75.member(LA(2))))) {
            int _m269 = mark();
            synPredMatched269 = true;
            inputState.guessing++;
            try {
                {
                    exp_simple();
                }
            }
            catch (RecognitionException pe) {
                synPredMatched269 = false;
            }
            rewind(_m269);
            inputState.guessing--;
        }
        if (synPredMatched269) {
            exp_simple();
            astFactory.addASTChild(currentAST, returnAST);
            exp_set_AST = (AST) currentAST.root;
        }
        else if ((LA(1) == OPEN_PAREN) && (LA(2) == LITERAL_select)) {
            subquery();
            astFactory.addASTChild(currentAST, returnAST);
            exp_set_AST = (AST) currentAST.root;
        }
        else {
            throw new NoViableAltException(LT(1), getFilename());
        }

        returnAST = exp_set_AST;
    }

    public final void quantified_factor() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST quantified_factor_AST = null;

        boolean synPredMatched260 = false;
        if (((_tokenSet_7.member(LA(1))) && (_tokenSet_54.member(LA(2))))) {
            int _m260 = mark();
            synPredMatched260 = true;
            inputState.guessing++;
            try {
                {
                    exp_simple();
                    comparison_op();
                    {
                        switch (LA(1)) {
                            case LITERAL_all: {
                                match(LITERAL_all);
                                break;
                            }
                            case LITERAL_any: {
                                match(LITERAL_any);
                                break;
                            }
                            case OPEN_PAREN: {
                                break;
                            }
                            default: {
                                throw new NoViableAltException(LT(1), getFilename());
                            }
                        }
                    }
                    subquery();
                }
            }
            catch (RecognitionException pe) {
                synPredMatched260 = false;
            }
            rewind(_m260);
            inputState.guessing--;
        }
        if (synPredMatched260) {
            exp_simple();
            astFactory.addASTChild(currentAST, returnAST);
            comparison_op();
            astFactory.addASTChild(currentAST, returnAST);
            {
                switch (LA(1)) {
                    case LITERAL_all: {
                        AST tmp211_AST = null;
                        tmp211_AST = astFactory.create(LT(1));
                        astFactory.addASTChild(currentAST, tmp211_AST);
                        match(LITERAL_all);
                        break;
                    }
                    case LITERAL_any: {
                        AST tmp212_AST = null;
                        tmp212_AST = astFactory.create(LT(1));
                        astFactory.addASTChild(currentAST, tmp212_AST);
                        match(LITERAL_any);
                        break;
                    }
                    case OPEN_PAREN: {
                        break;
                    }
                    default: {
                        throw new NoViableAltException(LT(1), getFilename());
                    }
                }
            }
            subquery();
            astFactory.addASTChild(currentAST, returnAST);
            quantified_factor_AST = (AST) currentAST.root;
        }
        else {
            boolean synPredMatched264 = false;
            if (((LA(1) == LITERAL_not || LA(1) == LITERAL_exists))) {
                int _m264 = mark();
                synPredMatched264 = true;
                inputState.guessing++;
                try {
                    {
                        {
                            switch (LA(1)) {
                                case LITERAL_not: {
                                    match(LITERAL_not);
                                    break;
                                }
                                case LITERAL_exists: {
                                    break;
                                }
                                default: {
                                    throw new NoViableAltException(LT(1), getFilename());
                                }
                            }
                        }
                        match(LITERAL_exists);
                        subquery();
                    }
                }
                catch (RecognitionException pe) {
                    synPredMatched264 = false;
                }
                rewind(_m264);
                inputState.guessing--;
            }
            if (synPredMatched264) {
                {
                    switch (LA(1)) {
                        case LITERAL_not: {
                            AST tmp213_AST = null;
                            tmp213_AST = astFactory.create(LT(1));
                            astFactory.addASTChild(currentAST, tmp213_AST);
                            match(LITERAL_not);
                            break;
                        }
                        case LITERAL_exists: {
                            break;
                        }
                        default: {
                            throw new NoViableAltException(LT(1), getFilename());
                        }
                    }
                }
                AST tmp214_AST = null;
                tmp214_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp214_AST);
                match(LITERAL_exists);
                subquery();
                astFactory.addASTChild(currentAST, returnAST);
                quantified_factor_AST = (AST) currentAST.root;
            }
            else if ((LA(1) == OPEN_PAREN) && (LA(2) == LITERAL_select)) {
                subquery();
                astFactory.addASTChild(currentAST, returnAST);
                quantified_factor_AST = (AST) currentAST.root;
            }
            else {
                throw new NoViableAltException(LT(1), getFilename());
            }
        }
        returnAST = quantified_factor_AST;
    }

    public final void row_status_clause() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST row_status_clause_AST = null;

        {
            {
                switch (LA(1)) {
                    case LITERAL_all:
                    case LITERAL_self:
                    case QUOTED_STRING:
                    case LITERAL_avg:
                    case LITERAL_count:
                    case LITERAL_max:
                    case LITERAL_min:
                    case LITERAL_stddev:
                    case LITERAL_sum:
                    case LITERAL_variance:
                    case LITERAL_filter:
                    case LITERAL_using:
                    case LITERAL_partition:
                    case LITERAL_by:
                    case IDENTIFIER:
                    case LITERAL_store:
                    case LITERAL_random:
                    case LITERAL_lowest:
                    case LITERAL_highest:
                    case LITERAL_with:
                    case LITERAL_group:
                    case LITERAL_pinned:
                    case LITERAL_geomean:
                    case LITERAL_kurtosis:
                    case LITERAL_median:
                    case LITERAL_skewness:
                    case LITERAL_sumsq:
                    case LITERAL_diff:
                    case LITERAL_custom:
                    case LITERAL_entrance:
                    case LITERAL_only:
                    case LITERAL_last:
                    case LITERAL_latest:
                    case LITERAL_milliseconds:
                    case LITERAL_seconds:
                    case LITERAL_minutes:
                    case LITERAL_hours:
                    case LITERAL_days:
                    case LITERAL_alert:
                    case LITERAL_correlate:
                    case LITERAL_present:
                    case LITERAL_is:
                    case LITERAL_row_status:
                    case LITERAL_new:
                    case LITERAL_dead:
                    case LITERAL_union:
                    case LITERAL_minus:
                    case LITERAL_except:
                    case LITERAL_intersect:
                    case LITERAL_limit:
                    case LITERAL_offset:
                    case LITERAL_current_timestamp:
                    case LITERAL_of: {
                        {
                            if ((_tokenSet_14.member(LA(1))) && (LA(2) == DOT)
                                    && (_tokenSet_14.member(LA(3)))) {
                                schema_name();
                                astFactory.addASTChild(currentAST, returnAST);
                                AST tmp215_AST = null;
                                tmp215_AST = astFactory.create(LT(1));
                                astFactory.addASTChild(currentAST, tmp215_AST);
                                match(DOT);
                            }
                            else if ((_tokenSet_14.member(LA(1))) && (LA(2) == DOT)
                                    && (LA(3) == DOLLAR)) {
                            }
                            else {
                                throw new NoViableAltException(LT(1), getFilename());
                            }

                        }
                        table_name();
                        astFactory.addASTChild(currentAST, returnAST);
                        AST tmp216_AST = null;
                        tmp216_AST = astFactory.create(LT(1));
                        astFactory.addASTChild(currentAST, tmp216_AST);
                        match(DOT);
                        break;
                    }
                    case DOLLAR: {
                        break;
                    }
                    default: {
                        throw new NoViableAltException(LT(1), getFilename());
                    }
                }
            }
            row_status_condition();
            astFactory.addASTChild(currentAST, returnAST);
        }
        if (inputState.guessing == 0) {
            row_status_clause_AST = (AST) currentAST.root;
            row_status_clause_AST = (AST) astFactory.make((new ASTArray(2)).add(
                    astFactory.create(ROW_STATUS_CLAUSE, "row_status_clause")).add(
                    row_status_clause_AST));
            currentAST.root = row_status_clause_AST;
            currentAST.child = row_status_clause_AST != null
                    && row_status_clause_AST.getFirstChild() != null ? row_status_clause_AST
                    .getFirstChild() : row_status_clause_AST;
            currentAST.advanceChildToEnd();
        }
        row_status_clause_AST = (AST) currentAST.root;
        returnAST = row_status_clause_AST;
    }

    public final void row_status_condition() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST row_status_condition_AST = null;

        {
            AST tmp217_AST = null;
            tmp217_AST = astFactory.create(LT(1));
            astFactory.addASTChild(currentAST, tmp217_AST);
            match(DOLLAR);
            AST tmp218_AST = null;
            tmp218_AST = astFactory.create(LT(1));
            astFactory.addASTChild(currentAST, tmp218_AST);
            match(LITERAL_row_status);
            AST tmp219_AST = null;
            tmp219_AST = astFactory.create(LT(1));
            astFactory.addASTChild(currentAST, tmp219_AST);
            match(LITERAL_is);
            {
                switch (LA(1)) {
                    case LITERAL_new: {
                        {
                            AST tmp220_AST = null;
                            tmp220_AST = astFactory.create(LT(1));
                            astFactory.addASTChild(currentAST, tmp220_AST);
                            match(LITERAL_new);
                        }
                        break;
                    }
                    case LITERAL_not:
                    case LITERAL_dead: {
                        {
                            {
                                switch (LA(1)) {
                                    case LITERAL_not: {
                                        AST tmp221_AST = null;
                                        tmp221_AST = astFactory.create(LT(1));
                                        astFactory.addASTChild(currentAST, tmp221_AST);
                                        match(LITERAL_not);
                                        break;
                                    }
                                    case LITERAL_dead: {
                                        break;
                                    }
                                    default: {
                                        throw new NoViableAltException(LT(1), getFilename());
                                    }
                                }
                            }
                            AST tmp222_AST = null;
                            tmp222_AST = astFactory.create(LT(1));
                            astFactory.addASTChild(currentAST, tmp222_AST);
                            match(LITERAL_dead);
                        }
                        break;
                    }
                    default: {
                        throw new NoViableAltException(LT(1), getFilename());
                    }
                }
            }
        }
        if (inputState.guessing == 0) {
            row_status_condition_AST = (AST) currentAST.root;
            row_status_condition_AST = (AST) astFactory.make((new ASTArray(2)).add(
                    astFactory.create(ROW_STATUS_CONDITION, "row_status_condition")).add(
                    row_status_condition_AST));
            currentAST.root = row_status_condition_AST;
            currentAST.child = row_status_condition_AST != null
                    && row_status_condition_AST.getFirstChild() != null ? row_status_condition_AST
                    .getFirstChild() : row_status_condition_AST;
            currentAST.advanceChildToEnd();
        }
        row_status_condition_AST = (AST) currentAST.root;
        returnAST = row_status_condition_AST;
    }

    public final void sorted_def() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST sorted_def_AST = null;

        {
            boolean synPredMatched292 = false;
            if (((_tokenSet_7.member(LA(1))) && (_tokenSet_8.member(LA(2)))
                    && (_tokenSet_76.member(LA(3))) && (_tokenSet_77.member(LA(4))))) {
                int _m292 = mark();
                synPredMatched292 = true;
                inputState.guessing++;
                try {
                    {
                        expression();
                    }
                }
                catch (RecognitionException pe) {
                    synPredMatched292 = false;
                }
                rewind(_m292);
                inputState.guessing--;
            }
            if (synPredMatched292) {
                expression();
                astFactory.addASTChild(currentAST, returnAST);
            }
            else {
                boolean synPredMatched294 = false;
                if (((LA(1) == NUMBER) && (_tokenSet_78.member(LA(2)))
                        && (_tokenSet_79.member(LA(3))) && (_tokenSet_80.member(LA(4))))) {
                    int _m294 = mark();
                    synPredMatched294 = true;
                    inputState.guessing++;
                    try {
                        {
                            match(NUMBER);
                        }
                    }
                    catch (RecognitionException pe) {
                        synPredMatched294 = false;
                    }
                    rewind(_m294);
                    inputState.guessing--;
                }
                if (synPredMatched294) {
                    AST tmp223_AST = null;
                    tmp223_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp223_AST);
                    match(NUMBER);
                }
                else {
                    throw new NoViableAltException(LT(1), getFilename());
                }
            }
        }
        {
            switch (LA(1)) {
                case LITERAL_asc: {
                    AST tmp224_AST = null;
                    tmp224_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp224_AST);
                    match(LITERAL_asc);
                    break;
                }
                case LITERAL_desc: {
                    AST tmp225_AST = null;
                    tmp225_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp225_AST);
                    match(LITERAL_desc);
                    break;
                }
                case SEMI:
                case COMMA:
                case CLOSE_PAREN:
                case LITERAL_order:
                case LITERAL_limit:
                case LITERAL_nulls: {
                    break;
                }
                default: {
                    throw new NoViableAltException(LT(1), getFilename());
                }
            }
        }
        {
            switch (LA(1)) {
                case LITERAL_nulls: {
                    AST tmp226_AST = null;
                    tmp226_AST = astFactory.create(LT(1));
                    astFactory.addASTChild(currentAST, tmp226_AST);
                    match(LITERAL_nulls);
                    {
                        switch (LA(1)) {
                            case LITERAL_first: {
                                AST tmp227_AST = null;
                                tmp227_AST = astFactory.create(LT(1));
                                astFactory.addASTChild(currentAST, tmp227_AST);
                                match(LITERAL_first);
                                break;
                            }
                            case LITERAL_last: {
                                AST tmp228_AST = null;
                                tmp228_AST = astFactory.create(LT(1));
                                astFactory.addASTChild(currentAST, tmp228_AST);
                                match(LITERAL_last);
                                break;
                            }
                            default: {
                                throw new NoViableAltException(LT(1), getFilename());
                            }
                        }
                    }
                    break;
                }
                case SEMI:
                case COMMA:
                case CLOSE_PAREN:
                case LITERAL_order:
                case LITERAL_limit: {
                    break;
                }
                default: {
                    throw new NoViableAltException(LT(1), getFilename());
                }
            }
        }
        sorted_def_AST = (AST) currentAST.root;
        returnAST = sorted_def_AST;
    }

    public final void keyword() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST keyword_AST = null;

        switch (LA(1)) {
            case LITERAL_filter: {
                AST tmp229_AST = null;
                tmp229_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp229_AST);
                match(LITERAL_filter);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_using: {
                AST tmp230_AST = null;
                tmp230_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp230_AST);
                match(LITERAL_using);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_with: {
                AST tmp231_AST = null;
                tmp231_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp231_AST);
                match(LITERAL_with);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_group: {
                AST tmp232_AST = null;
                tmp232_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp232_AST);
                match(LITERAL_group);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_by: {
                AST tmp233_AST = null;
                tmp233_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp233_AST);
                match(LITERAL_by);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_partition: {
                AST tmp234_AST = null;
                tmp234_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp234_AST);
                match(LITERAL_partition);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_store: {
                AST tmp235_AST = null;
                tmp235_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp235_AST);
                match(LITERAL_store);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_avg: {
                AST tmp236_AST = null;
                tmp236_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp236_AST);
                match(LITERAL_avg);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_count: {
                AST tmp237_AST = null;
                tmp237_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp237_AST);
                match(LITERAL_count);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_custom: {
                AST tmp238_AST = null;
                tmp238_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp238_AST);
                match(LITERAL_custom);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_geomean: {
                AST tmp239_AST = null;
                tmp239_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp239_AST);
                match(LITERAL_geomean);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_kurtosis: {
                AST tmp240_AST = null;
                tmp240_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp240_AST);
                match(LITERAL_kurtosis);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_max: {
                AST tmp241_AST = null;
                tmp241_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp241_AST);
                match(LITERAL_max);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_median: {
                AST tmp242_AST = null;
                tmp242_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp242_AST);
                match(LITERAL_median);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_min: {
                AST tmp243_AST = null;
                tmp243_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp243_AST);
                match(LITERAL_min);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_skewness: {
                AST tmp244_AST = null;
                tmp244_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp244_AST);
                match(LITERAL_skewness);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_stddev: {
                AST tmp245_AST = null;
                tmp245_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp245_AST);
                match(LITERAL_stddev);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_sum: {
                AST tmp246_AST = null;
                tmp246_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp246_AST);
                match(LITERAL_sum);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_sumsq: {
                AST tmp247_AST = null;
                tmp247_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp247_AST);
                match(LITERAL_sumsq);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_variance: {
                AST tmp248_AST = null;
                tmp248_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp248_AST);
                match(LITERAL_variance);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_diff: {
                AST tmp249_AST = null;
                tmp249_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp249_AST);
                match(LITERAL_diff);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_current_timestamp: {
                AST tmp250_AST = null;
                tmp250_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp250_AST);
                match(LITERAL_current_timestamp);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_pinned: {
                AST tmp251_AST = null;
                tmp251_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp251_AST);
                match(LITERAL_pinned);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_entrance: {
                AST tmp252_AST = null;
                tmp252_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp252_AST);
                match(LITERAL_entrance);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_only: {
                AST tmp253_AST = null;
                tmp253_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp253_AST);
                match(LITERAL_only);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_last: {
                AST tmp254_AST = null;
                tmp254_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp254_AST);
                match(LITERAL_last);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_latest: {
                AST tmp255_AST = null;
                tmp255_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp255_AST);
                match(LITERAL_latest);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_random: {
                AST tmp256_AST = null;
                tmp256_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp256_AST);
                match(LITERAL_random);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_lowest: {
                AST tmp257_AST = null;
                tmp257_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp257_AST);
                match(LITERAL_lowest);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_highest: {
                AST tmp258_AST = null;
                tmp258_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp258_AST);
                match(LITERAL_highest);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_milliseconds: {
                AST tmp259_AST = null;
                tmp259_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp259_AST);
                match(LITERAL_milliseconds);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_seconds: {
                AST tmp260_AST = null;
                tmp260_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp260_AST);
                match(LITERAL_seconds);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_minutes: {
                AST tmp261_AST = null;
                tmp261_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp261_AST);
                match(LITERAL_minutes);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_hours: {
                AST tmp262_AST = null;
                tmp262_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp262_AST);
                match(LITERAL_hours);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_days: {
                AST tmp263_AST = null;
                tmp263_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp263_AST);
                match(LITERAL_days);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_limit: {
                AST tmp264_AST = null;
                tmp264_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp264_AST);
                match(LITERAL_limit);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_offset: {
                AST tmp265_AST = null;
                tmp265_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp265_AST);
                match(LITERAL_offset);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_union: {
                AST tmp266_AST = null;
                tmp266_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp266_AST);
                match(LITERAL_union);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_all: {
                AST tmp267_AST = null;
                tmp267_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp267_AST);
                match(LITERAL_all);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_minus: {
                AST tmp268_AST = null;
                tmp268_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp268_AST);
                match(LITERAL_minus);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_except: {
                AST tmp269_AST = null;
                tmp269_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp269_AST);
                match(LITERAL_except);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_intersect: {
                AST tmp270_AST = null;
                tmp270_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp270_AST);
                match(LITERAL_intersect);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_of: {
                AST tmp271_AST = null;
                tmp271_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp271_AST);
                match(LITERAL_of);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_self: {
                AST tmp272_AST = null;
                tmp272_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp272_AST);
                match(LITERAL_self);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_row_status: {
                AST tmp273_AST = null;
                tmp273_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp273_AST);
                match(LITERAL_row_status);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_is: {
                AST tmp274_AST = null;
                tmp274_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp274_AST);
                match(LITERAL_is);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_new: {
                AST tmp275_AST = null;
                tmp275_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp275_AST);
                match(LITERAL_new);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_dead: {
                AST tmp276_AST = null;
                tmp276_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp276_AST);
                match(LITERAL_dead);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_alert: {
                AST tmp277_AST = null;
                tmp277_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp277_AST);
                match(LITERAL_alert);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_present: {
                AST tmp278_AST = null;
                tmp278_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp278_AST);
                match(LITERAL_present);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            case LITERAL_correlate: {
                AST tmp279_AST = null;
                tmp279_AST = astFactory.create(LT(1));
                astFactory.addASTChild(currentAST, tmp279_AST);
                match(LITERAL_correlate);
                keyword_AST = (AST) currentAST.root;
                break;
            }
            default: {
                throw new NoViableAltException(LT(1), getFilename());
            }
        }
        returnAST = keyword_AST;
    }

    public final void quoted_string() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST quoted_string_AST = null;

        AST tmp280_AST = null;
        tmp280_AST = astFactory.create(LT(1));
        astFactory.addASTChild(currentAST, tmp280_AST);
        match(QUOTED_STRING);
        quoted_string_AST = (AST) currentAST.root;
        returnAST = quoted_string_AST;
    }

    public final void match_string() throws RecognitionException, TokenStreamException {

        returnAST = null;
        ASTPair currentAST = new ASTPair();
        AST match_string_AST = null;

        AST tmp281_AST = null;
        tmp281_AST = astFactory.create(LT(1));
        astFactory.addASTChild(currentAST, tmp281_AST);
        match(QUOTED_STRING);
        match_string_AST = (AST) currentAST.root;
        returnAST = match_string_AST;
    }

    public static final String[] _tokenNames = { "<0>", "EOF", "<2>", "NULL_TREE_LOOKAHEAD",
            "SQL_STATEMENT", "SELECT_LIST", "SELECT_EXPRESSION", "DISPLAYED_COLUMN",
            "CASE_EXPRESSION", "EXP_SIMPLE", "TABLE_REFERENCE_LIST", "SELECTED_TABLE",
            "TABLE_SPEC", "ALIAS", "FILTER_SPEC", "PARTITION_SPEC_LIST", "PARTITION_SPEC",
            "ROW_PICK_SPEC", "WHERE_CONDITION", "FIRST_CLAUSE", "LIMIT_CLAUSE",
            "AGGREGATE_FUNCTION", "AGGREGATE_FUNCTION_SPEC", "MISC_FUNCTION", "WINDOW_FUNCTION",
            "TIME_UNIT_SPEC", "GROUP_FUNCTION", "ROW_STATUS_CLAUSE", "ROW_STATUS_CONDITION",
            "CLONE_PARTITION_CLAUSE", "POST_WHERE_CLAUSES", "MONITOR_EXPRESSION",
            "PARTITION_COLUMN_NAME_LIST", "USING_LIST", "USING_SPEC", "PRESENT_LIST",
            "PRESENT_SPEC", "SEMI", "\"select\"", "\"all\"", "\"distinct\"", "\"from\"",
            "\"where\"", "COMMA", "ASTERISK", "\"case\"", "\"else\"", "\"end\"", "\"when\"",
            "\"then\"", "\"left\"", "\"right\"", "\"outer\"", "\"inner\"", "\"join\"", "\"on\"",
            "DOT", "\"self\"", "POUND", "PLUS", "MINUS", "\"as\"", "DIVIDE", "MODULO", "VERTBAR",
            "OPEN_PAREN", "CLOSE_PAREN", "NUMBER", "QUOTED_STRING", "\"null\"", "\"avg\"",
            "\"count\"", "\"max\"", "\"min\"", "\"stddev\"", "\"sum\"", "\"variance\"",
            "\"filter\"", "\"using\"", "\"to\"", "\"partition\"", "\"by\"", "IDENTIFIER",
            "\"store\"", "\"random\"", "\"lowest\"", "\"highest\"", "\"with\"", "\"update\"",
            "\"group\"", "\"pinned\"", "\"geomean\"", "\"kurtosis\"", "\"median\"", "\"skewness\"",
            "\"sumsq\"", "DOLLAR", "\"diff\"", "\"custom\"", "\"entrance\"", "\"only\"",
            "\"last\"", "\"latest\"", "\"milliseconds\"", "\"seconds\"", "\"minutes\"",
            "\"hours\"", "\"days\"", "\"alert\"", "\"correlate\"", "\"or\"", "\"present\"",
            "\"and\"", "\"not\"", "\"in\"", "\"like\"", "\"escape\"", "\"between\"", "\"is\"",
            "\"row_status\"", "\"new\"", "\"dead\"", "\"any\"", "\"exists\"", "EQ", "LT", "GT",
            "NOT_EQ", "LE", "GE", "\"having\"", "\"union\"", "\"minus\"", "\"except\"",
            "\"intersect\"", "\"order\"", "\"first\"", "\"limit\"", "\"offset\"", "\"asc\"",
            "\"desc\"", "\"nulls\"", "\"current_timestamp\"", "\"of\"", "AT_SIGN", "N",
            "DOUBLE_QUOTE", "WS", "ML_COMMENT" };

    protected void buildTokenTypeASTClassMap() {
        tokenTypeToASTClassMap = null;
    };

    private static final long[] mk_tokenSet_0() {
        long[] data = { 1873550771300073472L, 270427079562985466L, 50808L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_0 = new BitSet(mk_tokenSet_0());

    private static final long[] mk_tokenSet_1() {
        long[] data = { -359977358154792960L, 270427079562985467L, 50808L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_1 = new BitSet(mk_tokenSet_1());

    private static final long[] mk_tokenSet_2() {
        long[] data = { -359941074271076352L, 847450786114797567L, 50808L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_2 = new BitSet(mk_tokenSet_2());

    private static final long[] mk_tokenSet_3() {
        long[] data = { -41306590271242240L, -293085819516780545L, 50939L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_3 = new BitSet(mk_tokenSet_3());

    private static final long[] mk_tokenSet_4() {
        long[] data = { 144115737831669760L, 270427079562985426L, 50808L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_4 = new BitSet(mk_tokenSet_4());

    private static final long[] mk_tokenSet_5() {
        long[] data = { 2840659621176147968L, 270427079562985430L, 50936L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_5 = new BitSet(mk_tokenSet_5());

    private static final long[] mk_tokenSet_6() {
        long[] data = { 4250903130566295554L, 847802629835685886L, 51192L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_6 = new BitSet(mk_tokenSet_6());

    private static final long[] mk_tokenSet_7() {
        long[] data = { 1873497994741940224L, 270427079562985466L, 50808L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_7 = new BitSet(mk_tokenSet_7());

    private static final long[] mk_tokenSet_8() {
        long[] data = { -2666103903929499648L, 270427079562985471L, 65272L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_8 = new BitSet(mk_tokenSet_8());

    private static final long[] mk_tokenSet_9() {
        long[] data = { 137438953472L, 4L, 640L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_9 = new BitSet(mk_tokenSet_9());

    private static final long[] mk_tokenSet_10() {
        long[] data = { 2516963123080986626L, 270778923283873756L, 50936L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_10 = new BitSet(mk_tokenSet_10());

    private static final long[] mk_tokenSet_11() {
        long[] data = { 4250849254496534530L, 847802629835718654L, 50936L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_11 = new BitSet(mk_tokenSet_11());

    private static final long[] mk_tokenSet_12() {
        long[] data = { -2336462209022L, -292733975795859457L, 65531L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_12 = new BitSet(mk_tokenSet_12());

    private static final long[] mk_tokenSet_13() {
        long[] data = { 137438953472L, 4L, 1664L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_13 = new BitSet(mk_tokenSet_13());

    private static final long[] mk_tokenSet_14() {
        long[] data = { 144115737831669760L, 270427079562985424L, 50808L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_14 = new BitSet(mk_tokenSet_14());

    private static final long[] mk_tokenSet_15() {
        long[] data = { 144133330017714176L, 270427079562985424L, 50808L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_15 = new BitSet(mk_tokenSet_15());

    private static final long[] mk_tokenSet_16() {
        long[] data = { -360258833131503616L, 270427079562985467L, 50808L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_16 = new BitSet(mk_tokenSet_16());

    private static final long[] mk_tokenSet_17() {
        long[] data = { -360222549247787008L, 270427079562985471L, 50808L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_17 = new BitSet(mk_tokenSet_17());

    private static final long[] mk_tokenSet_18() {
        long[] data = { -41306590271242240L, 270427079562985471L, 50936L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_18 = new BitSet(mk_tokenSet_18());

    private static final long[] mk_tokenSet_19() {
        long[] data = { 2588458042043400192L, 270427079562985428L, 50936L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_19 = new BitSet(mk_tokenSet_19());

    private static final long[] mk_tokenSet_20() {
        long[] data = { -211243671486462L, -292733975795859457L, 51195L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_20 = new BitSet(mk_tokenSet_20());

    private static final long[] mk_tokenSet_21() {
        long[] data = { 2449958747045363712L, 270427079562985424L, 50808L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_21 = new BitSet(mk_tokenSet_21());

    private static final long[] mk_tokenSet_22() {
        long[] data = { 210557163913871360L, 270427079562985428L, 50936L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_22 = new BitSet(mk_tokenSet_22());

    private static final long[] mk_tokenSet_23() {
        long[] data = { 4250849254496534530L, 847802629835685886L, 50936L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_23 = new BitSet(mk_tokenSet_23());

    private static final long[] mk_tokenSet_24() {
        long[] data = { -494917671452670L, -292733975795859457L, 51195L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_24 = new BitSet(mk_tokenSet_24());

    private static final long[] mk_tokenSet_25() {
        long[] data = { 66441426082201600L, 33554436L, 760L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_25 = new BitSet(mk_tokenSet_25());

    private static final long[] mk_tokenSet_26() {
        long[] data = { -137438953470L, -4503599644147713L, 65535L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_26 = new BitSet(mk_tokenSet_26());

    private static final long[] mk_tokenSet_27() {
        long[] data = { -359198766483374080L, -864691132766912513L, 65279L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_27 = new BitSet(mk_tokenSet_27());

    private static final long[] mk_tokenSet_28() {
        long[] data = { -292734113218035710L, -16777217L, 65535L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_28 = new BitSet(mk_tokenSet_28());

    private static final long[] mk_tokenSet_29() {
        long[] data = { 1873497994741940224L, 847450786114797562L, 50808L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_29 = new BitSet(mk_tokenSet_29());

    private static final long[] mk_tokenSet_30() {
        long[] data = { -2666112562583568384L, -293085819516780549L, 50811L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_30 = new BitSet(mk_tokenSet_30());

    private static final long[] mk_tokenSet_31() {
        long[] data = { -2666067482606829568L, -4855443365068801L, 51067L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_31 = new BitSet(mk_tokenSet_31());

    private static final long[] mk_tokenSet_32() {
        long[] data = { -431256360521302016L, -864691132766912555L, 65279L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_32 = new BitSet(mk_tokenSet_32());

    private static final long[] mk_tokenSet_33() {
        long[] data = { -364792806767591422L, -16777217L, 65535L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_33 = new BitSet(mk_tokenSet_33());

    private static final long[] mk_tokenSet_34() {
        long[] data = { -137438953470L, -16777217L, 65535L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_34 = new BitSet(mk_tokenSet_34());

    private static final long[] mk_tokenSet_35() {
        long[] data = { -2666104041368453120L, 270427079562985471L, 50808L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_35 = new BitSet(mk_tokenSet_35());

    private static final long[] mk_tokenSet_36() {
        long[] data = { 1873516686439612416L, 270427079562985470L, 50808L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_36 = new BitSet(mk_tokenSet_36());

    private static final long[] mk_tokenSet_37() {
        long[] data = { 1873497994741940224L, 270427079562985470L, 50808L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_37 = new BitSet(mk_tokenSet_37());

    private static final long[] mk_tokenSet_38() {
        long[] data = { -2666112837461475328L, 270427079562985471L, 50808L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_38 = new BitSet(mk_tokenSet_38());

    private static final long[] mk_tokenSet_39() {
        long[] data = { -359197666971746304L, -864691132766912513L, 65279L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_39 = new BitSet(mk_tokenSet_39());

    private static final long[] mk_tokenSet_40() {
        long[] data = { -359198766483374080L, -864691132766912553L, 65279L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_40 = new BitSet(mk_tokenSet_40());

    private static final long[] mk_tokenSet_41() {
        long[] data = { -2666104041368453120L, 270427079562985467L, 50808L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_41 = new BitSet(mk_tokenSet_41());

    private static final long[] mk_tokenSet_42() {
        long[] data = { -2666102941856825344L, 270427079562985471L, 50808L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_42 = new BitSet(mk_tokenSet_42());

    private static final long[] mk_tokenSet_43() {
        long[] data = { 720576490135093248L, 270427079562985424L, 50808L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_43 = new BitSet(mk_tokenSet_43());

    private static final long[] mk_tokenSet_44() {
        long[] data = { -359198766483374080L, -864691132766912555L, 65279L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_44 = new BitSet(mk_tokenSet_44());

    private static final long[] mk_tokenSet_45() {
        long[] data = { -431256360521302016L, -864691132766912553L, 65279L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_45 = new BitSet(mk_tokenSet_45());

    private static final long[] mk_tokenSet_46() {
        long[] data = { 2516400173127565312L, 270427079562985430L, 50936L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_46 = new BitSet(mk_tokenSet_46());

    private static final long[] mk_tokenSet_47() {
        long[] data = { 4398046511104L, 8388628L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_47 = new BitSet(mk_tokenSet_47());

    private static final long[] mk_tokenSet_48() {
        long[] data = { 4179345402002145280L, 847450786114830334L, 50808L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_48 = new BitSet(mk_tokenSet_48());

    private static final long[] mk_tokenSet_49() {
        long[] data = { -293828127287672832L, -293085819516747777L, 50939L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_49 = new BitSet(mk_tokenSet_49());

    private static final long[] mk_tokenSet_50() {
        long[] data = { -288725293823164414L, -4503599644180481L, 51195L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_50 = new BitSet(mk_tokenSet_50());

    private static final long[] mk_tokenSet_51() {
        long[] data = { 144128931971203072L, 270427079562985428L, 50808L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_51 = new BitSet(mk_tokenSet_51());

    private static final long[] mk_tokenSet_52() {
        long[] data = { 4179354198095167488L, 847450786114830334L, 50808L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_52 = new BitSet(mk_tokenSet_52());

    private static final long[] mk_tokenSet_53() {
        long[] data = { 2449958747045363714L, 270427079562985424L, 50808L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_53 = new BitSet(mk_tokenSet_53());

    private static final long[] mk_tokenSet_54() {
        long[] data = { -2666112837461475328L, -882494425043861509L, 50811L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_54 = new BitSet(mk_tokenSet_54());

    private static final long[] mk_tokenSet_55() {
        long[] data = { -2666102941856825344L, -882494425043861505L, 50811L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_55 = new BitSet(mk_tokenSet_55());

    private static final long[] mk_tokenSet_56() {
        long[] data = { -2665535456417939456L, -882142581322973185L, 50939L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_56 = new BitSet(mk_tokenSet_56());

    private static final long[] mk_tokenSet_57() {
        long[] data = { -2666112837461475328L, 272115929423249403L, 50808L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_57 = new BitSet(mk_tokenSet_57());

    private static final long[] mk_tokenSet_58() {
        long[] data = { -2666102941856825344L, 272115929423249407L, 50808L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_58 = new BitSet(mk_tokenSet_58());

    private static final long[] mk_tokenSet_59() {
        long[] data = { -2665535181540032512L, 272467773144137727L, 50936L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_59 = new BitSet(mk_tokenSet_59());

    private static final long[] mk_tokenSet_60() {
        long[] data = { -2666112837461475328L, 273241829330092027L, 50808L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_60 = new BitSet(mk_tokenSet_60());

    private static final long[] mk_tokenSet_61() {
        long[] data = { -2666102941856825344L, 273241829330092031L, 50808L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_61 = new BitSet(mk_tokenSet_61());

    private static final long[] mk_tokenSet_62() {
        long[] data = { -2665535456417939456L, 278097272678350847L, 50936L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_62 = new BitSet(mk_tokenSet_62());

    private static final long[] mk_tokenSet_63() {
        long[] data = { -2666112837461475328L, 279997228771147771L, 50808L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_63 = new BitSet(mk_tokenSet_63());

    private static final long[] mk_tokenSet_64() {
        long[] data = { -2666102941856825344L, 279997228771147775L, 50808L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_64 = new BitSet(mk_tokenSet_64());

    private static final long[] mk_tokenSet_65() {
        long[] data = { -2666102941856825344L, 280278703747858431L, 50808L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_65 = new BitSet(mk_tokenSet_65());

    private static final long[] mk_tokenSet_66() {
        long[] data = { -2666112837461475328L, 270427079562985467L, 50808L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_66 = new BitSet(mk_tokenSet_66());

    private static final long[] mk_tokenSet_67() {
        long[] data = { -2666102941856825344L, 270990029516406783L, 50808L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_67 = new BitSet(mk_tokenSet_67());

    private static final long[] mk_tokenSet_68() {
        long[] data = { -2665535456417939456L, 271341873237295103L, 50936L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_68 = new BitSet(mk_tokenSet_68());

    private static final long[] mk_tokenSet_69() {
        long[] data = { 1873497994741940224L, 847450781819830266L, 50808L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_69 = new BitSet(mk_tokenSet_69());

    private static final long[] mk_tokenSet_70() {
        long[] data = { -2666112562583568384L, -306033672740438021L, 50811L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_70 = new BitSet(mk_tokenSet_70());

    private static final long[] mk_tokenSet_71() {
        long[] data = { -2666067482606829568L, -594264048892149761L, 51067L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_71 = new BitSet(mk_tokenSet_71());

    private static final long[] mk_tokenSet_72() {
        long[] data = { -359940799393169408L, -594264048892149761L, 51067L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_72 = new BitSet(mk_tokenSet_72());

    private static final long[] mk_tokenSet_73() {
        long[] data = { 144115737831669760L, 270427083857952720L, 50808L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_73 = new BitSet(mk_tokenSet_73());

    private static final long[] mk_tokenSet_74() {
        long[] data = { 72057594037927936L, 252764529086169088L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_74 = new BitSet(mk_tokenSet_74());

    private static final long[] mk_tokenSet_75() {
        long[] data = { -2665545352022589440L, 270778923283873791L, 50936L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_75 = new BitSet(mk_tokenSet_75());

    private static final long[] mk_tokenSet_76() {
        long[] data = { -293264352700530686L, 270778923283873791L, 65528L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_76 = new BitSet(mk_tokenSet_76());

    private static final long[] mk_tokenSet_77() {
        long[] data = { -288760478195253246L, 847802629835718655L, 65528L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_77 = new BitSet(mk_tokenSet_77());

    private static final long[] mk_tokenSet_78() {
        long[] data = { 8933531975680L, 4L, 14976L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_78 = new BitSet(mk_tokenSet_78());

    private static final long[] mk_tokenSet_79() {
        long[] data = { 4246345379991257090L, 270778923283873790L, 59384L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_79 = new BitSet(mk_tokenSet_79());

    private static final long[] mk_tokenSet_80() {
        long[] data = { -288761577706881022L, 847802629835718655L, 65528L, 0L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_80 = new BitSet(mk_tokenSet_80());

}
