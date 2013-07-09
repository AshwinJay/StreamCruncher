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
header {
package streamcruncher.innards.impl.query;
}

/**
* Original file:
* <p>
* Oracle 7 SQL grammar (http://antlr.org/grammar/ORACLE-7-SQL)
*
* The BNF notation found at
* http://cui.unige.ch/db-research/Enseignement/analyseinfo/BNFweb.html
* may prove to be useful as a reference.
*
* Recent updates by bwiese@metasolv.com
*
* ToDo:
*   - Acknowledge all 7 parser ambiguities.
*   - Proper support of floating point numbers in lexer.  A floating point
*     number like '.9' will not be handled correctly.  I believe the way
*     java.g handles this should be more than sufficient, just has not
*     been an issue yet.
*   - Confirm that the lexer rules NOT_EQ and GT work and are even necessary.
*   - Deal with 3 lexer ambiguities, unless they were taken care of when
*     resolving the last to items.
*
* Changes:
*   5/17/2002 - Start version using above mentioned BNF as base
*   5/22/2002 - Changes include (any number is the line from pb.sql):
*      - Allow 'not' after 'or', not just at beginning of select.
*      - (2500) Added concat char_function.
*      - (1796) Moved "user" from other_function to new rule pseudo_column
*               and added pseudo_column to variable.
*      - (2624) Renamed 'constante_nonsigne' to literal, and in this rule
*               changed 'N' to NUMBER, where number was moved to the lexer
*               since 1e9 was being split into a number '1' and a literal
*               'e9', which was no good.  Also changed the "E" in number to
*               lowercase for case insensitivity.  The NUMBER in the lexer
*               needs work, see comment.
*      - (3327) The unary PLUS & MINUS is messed up, why would that be
*               mixed up with the binary addition and subtraction?  The
*               BNF above was helpful for the most part, but definitely
*               leaves a lot to be desired.  It would be nice to send them
*               all the fixes if I could manage it, but with all the changes
*               I am not sure that is pratical.
*      - (3414) Multiple "prior" conditions are allowed in a connect by,
*               so I just moved the optional "prior" qualifier to the
*               first alt of logical_factor.
*      - (3419) Add support for a list as an expression, needed to support
*               list comparisons.
*      - (3864) Add a bunch of keywords to identifiers, since "length" is
*               the name of a column and also a char_function.
*      - (4849) Allow in expression in exp_set, the 'simple_value' is not
*               sufficient.
*      - (5171) Make precedence of logical_not to be just lower then
*               parenthesises.
*      - (5221) Allow for an option "escape" followed by a QUOTED_STRING
*               after a like and match_string.
*      - (5252) Need to allow complex expression for match_string when used
*               with the "like" comparison operator.
*      - (5364) Make columns optional on update_clause.
*      - (2499) Allow for arbitrary parenthesises around select statements
*               by updating the select_statement rule to refer back to the
*               select_command rule, rather than going directly to the
*               select_expression rule.  The way it was only allow for
*               an optional set of parenthesises, not any number.
* </p>
*/

class RQLParser extends Parser;

options {
    exportVocab = RQL;
    k = 4;
    buildAST = true;
    defaultErrorHandler=false;
}

tokens {
    SQL_STATEMENT;
    SELECT_LIST;
    SELECT_EXPRESSION;
    DISPLAYED_COLUMN;
    CASE_EXPRESSION;
    EXP_SIMPLE;
    TABLE_REFERENCE_LIST;
    SELECTED_TABLE;
    TABLE_SPEC;
    ALIAS;
    FILTER_SPEC;
    PARTITION_SPEC_LIST;
    PARTITION_SPEC;
    ROW_PICK_SPEC;
    WHERE_CONDITION;
    FIRST_CLAUSE;
    LIMIT_CLAUSE;
    AGGREGATE_FUNCTION;
    AGGREGATE_FUNCTION_SPEC;
    MISC_FUNCTION;
    WINDOW_FUNCTION;
    TIME_UNIT_SPEC;
    GROUP_FUNCTION;
    ROW_STATUS_CLAUSE;
    ROW_STATUS_CONDITION;
    CLONE_PARTITION_CLAUSE;
    POST_WHERE_CLAUSES;
    
    MONITOR_EXPRESSION;
	PARTITION_COLUMN_NAME_LIST;
	USING_LIST;
	USING_SPEC;
	PRESENT_LIST;
	PRESENT_SPEC;
}

start_rule
    : 
      sql_statement EOF
    ;

sql_statement
    :
        select_expression SEMI
        { #sql_statement = #([SQL_STATEMENT, "sql_statement"], #sql_statement); }
    ;

select_expression
    :
      "select" (first_clause)? ( "all" | "distinct" )? select_list
      "from" data_source
      ( "where" where_condition )?
      post_where_clauses
      { #select_expression = #([SELECT_EXPRESSION, "select_expression"], #select_expression); }
    ;

data_source
	:	  
	  ( ( monitor_expression ) => monitor_expression | ( table_reference_list ) => table_reference_list )
	;

select_list
    :
      ( 
        ( displayed_column ) => displayed_column ( COMMA displayed_column )*
        | ASTERISK 
      )
      { #select_list = #([SELECT_LIST, "select_list"], #select_list); }
    ;

table_reference_list
    :
        selected_table ( COMMA selected_table )* (join_expression)?
        { #table_reference_list = #([TABLE_REFERENCE_LIST, "table_reference_list"], #table_reference_list); }
    ;

case_expression
    :
      ( "case" (expression)? (case_when_fragment)+  ("else" expression)? "end" )
      { #case_expression = #([CASE_EXPRESSION, "case_expression"], #case_expression); }
    ;

case_when_fragment
    :
      ("when" condition "then" expression)
    ;

join_expression
    :
        ( (("left" | "right") "outer") | "inner" )?
        "join" selected_table "on" condition
    ;

where_condition
    :
        condition
        { #where_condition = #([WHERE_CONDITION, "where_condition"], #where_condition); }
    ;

post_where_clauses
    :
      ( group_clause )?
      ( ( combine_clause ) => combine_clause )?
      ( ( order_clause ) => order_clause )?
      ( ( limit_clause ) => limit_clause )?
      { #post_where_clauses = #([POST_WHERE_CLAUSES, "post_where_clauses"], #post_where_clauses); }
    ;

displayed_column
    :
     (
      ( (schema_name DOT)? table_name DOT ASTERISK ) => ( ( schema_name DOT )? table_name DOT ASTERISK )
      | ( exp_simple ( alias )? )
      | ( case_expression ( alias )? )
     )
     { #displayed_column = #([DISPLAYED_COLUMN, "displayed_column"], #displayed_column); }      
    ;

clone_partition_clause
    :
    ( "self" POUND identifier alias ) => ( "self" POUND identifier alias )
    { #clone_partition_clause = #([CLONE_PARTITION_CLAUSE, "clone_partition_clause"], #clone_partition_clause); }
    ;

schema_name
    :
     identifier
    ;

table_name
    :
     identifier
    ;

exp_simple 
	: expression 
	{ #exp_simple = #([EXP_SIMPLE, "exp_simple"], #exp_simple); }
	;

expression
    :
     term ( ( PLUS | MINUS ) term )*
    ;

alias
    :
      ("as")? identifier
      { #alias = #([ALIAS, "alias"], #alias); }
    ;

term
    :
     factor ( ( multiply | DIVIDE | MODULO ) factor )*
    ;

multiply
    :
      ASTERISK
    ;

factor
    :
      factor2 ( VERTBAR VERTBAR factor2 )*
    ;

factor2
    :
      ( sql_literal ) => sql_literal
      | ( ( PLUS | MINUS ) expression ) => ( PLUS | MINUS ) expression
      | ( function ( OPEN_PAREN expression ( COMMA expression )* CLOSE_PAREN ) ) => function ( OPEN_PAREN expression ( COMMA expression )* CLOSE_PAREN )
      | ( group_function OPEN_PAREN ( ASTERISK | "all" | "distinct" )? (expression)? CLOSE_PAREN ) => group_function OPEN_PAREN ( ASTERISK | "all" | "distinct" )? (expression)? CLOSE_PAREN
      | ( OPEN_PAREN expression CLOSE_PAREN ) => OPEN_PAREN expression CLOSE_PAREN
      | ( variable ) => variable
      | expression_list
    ;

expression_list : OPEN_PAREN expression ( COMMA expression )+ CLOSE_PAREN ;

sql_literal
    :
        ( NUMBER | QUOTED_STRING | "null" )
    ;

variable
    :
        ( column_spec ( OPEN_PAREN PLUS CLOSE_PAREN ) ) => column_spec ( OPEN_PAREN PLUS CLOSE_PAREN )
        | column_spec
    ;

column_spec
    :
        ( ( schema_name DOT )? table_name DOT )? column_name
    ;

package_name : identifier ;

column_name : identifier ;

function
    :
      any_function
      | group_function
    ;

any_function
    :
     ( ( schema_name DOT )? package_name DOT )? identifier
    ;

group_function
    :
      ("avg" | "count" | "max" | "min" | "stddev" | "sum" | "variance")
      { #group_function = #([GROUP_FUNCTION, "group_function"], #group_function); }
    ;

selected_table
    :
        ( filter_spec alias )
        {  #selected_table = #([SELECTED_TABLE, "selected_table"], #selected_table); }
        |
        ( partition_spec_list alias )
        {  #selected_table = #([SELECTED_TABLE, "selected_table"], #selected_table); }
        |
        ( ( table_spec | subquery ) ( alias )? )
        {  #selected_table = #([SELECTED_TABLE, "selected_table"], #selected_table); }
        |
        ( clone_partition_clause )
        {  #selected_table = #([SELECTED_TABLE, "selected_table"], #selected_table); }
    ;

table_spec
    :
      ( schema_name DOT )? table_name
      { #table_spec = #([TABLE_SPEC, "table_spec"], #table_spec); }
    ;

filter_spec
    :
      (table_spec OPEN_PAREN "filter" "using") =>
      table_spec OPEN_PAREN "filter" "using" QUOTED_STRING CLOSE_PAREN
      { #filter_spec = #([FILTER_SPEC, "filter_spec"], #filter_spec); }
    ;

partition_spec_list
    :
      (table_spec partition_spec) => table_spec partition_spec ("to" partition_spec)*
      { #partition_spec_list = #([PARTITION_SPEC_LIST, "partition_spec_list"], #partition_spec_list); }
    ;

partition_spec
    :
      (OPEN_PAREN "partition") =>
      OPEN_PAREN "partition" ( "by" IDENTIFIER (COMMA IDENTIFIER)* )?
      "store" row_pick_spec
      ( "where" where_condition )? CLOSE_PAREN
      { #partition_spec = #([PARTITION_SPEC, "partition_spec"], #partition_spec); }
    ;

row_pick_spec
    :
     ((misc_function | window_function) (aggregate_function)?)
     { #row_pick_spec = #([ROW_PICK_SPEC, "row_pick_spec"], #row_pick_spec); }
    ;

misc_function
    :
      (
	      ( 
	        ( "random" NUMBER ) 
	        | 
	        ( ("lowest" | "highest") NUMBER "using" IDENTIFIER ("with" "update" "group" IDENTIFIER (COMMA IDENTIFIER)*)* ) 
	      )
	      ( QUOTED_STRING )?
	  )    
      { #misc_function  = #([MISC_FUNCTION , "misc_function"], #misc_function ); }
    ;

aggregate_function
    :
      ("with" ("pinned")? aggregate_function_spec (COMMA aggregate_function_spec)*)
      { #aggregate_function  = #([AGGREGATE_FUNCTION , "aggregate_function"], #aggregate_function ); }
    ;

aggregate_function_spec
    :
      (
          (
            (
             ( "avg" | "count" | "geomean" | "kurtosis" | "max" | "median" | "min" | "skewness" | "stddev" | "sum" | "sumsq" | "variance" )
              OPEN_PAREN IDENTIFIER ( DOLLAR "diff" ( QUOTED_STRING )? )? CLOSE_PAREN
            )
            |
            ( "custom" OPEN_PAREN IDENTIFIER (COMMA (IDENTIFIER | QUOTED_STRING) )* CLOSE_PAREN )
          ) 
          ("entrance" "only")? alias
      )
      { #aggregate_function_spec  = #([AGGREGATE_FUNCTION_SPEC , "aggregate_function_spec"], #aggregate_function_spec ); }
    ;

window_function
    :
     (
	     ( 
	      ( "last" NUMBER ( time_unit_spec ("max" NUMBER)? )? ( QUOTED_STRING )? )
	      | 
	      ( "latest" NUMBER )
	     )
	     ( QUOTED_STRING )?
     ) 
     { #window_function = #([WINDOW_FUNCTION, "window_function"], #window_function); }
    ;

time_unit_spec
    :
     ("milliseconds" | "seconds" | "minutes" | "hours" | "days")
     { #time_unit_spec = #([TIME_UNIT_SPEC, "time_unit_spec"], #time_unit_spec); }
    ;

table_alias
    :
      ( schema_name DOT )? table_name ( alias )?
    ;

//----------------

monitor_expression
    :
	  "alert" partition_column_name_list
	  using_list
	  present_list
      { #monitor_expression = #([MONITOR_EXPRESSION, "monitor_expression"], #monitor_expression); }
    ;

partition_column_name_list
	:
	  IDENTIFIER DOT IDENTIFIER "as" IDENTIFIER ( COMMA IDENTIFIER DOT IDENTIFIER "as" IDENTIFIER )*
      { #partition_column_name_list = #([PARTITION_COLUMN_NAME_LIST, "partition_column_name_list"], #partition_column_name_list); }      
	;

using_list
    :
      "using" ( using_spec ( COMMA using_spec )+ )      
      { #using_list = #([USING_LIST, "using_list"], #using_list); }      
    ;

using_spec
    :
      partition_spec_list alias "correlate" "on" identifier
      { #using_spec = #([USING_SPEC, "using_spec"], #using_spec); }
    ;

present_list
	:
	  "when" present_spec ( "or" present_spec )*
	  { #present_list = #([PRESENT_LIST, "present_list"], #present_list); }
	;

present_spec
	:
	  "present" OPEN_PAREN IDENTIFIER ( "and" ( "not" )? IDENTIFIER )+ CLOSE_PAREN
	  { #present_spec = #([PRESENT_SPEC, "present_spec"], #present_spec); }
	;

//----------------

condition
    :
      logical_term ( "or" logical_term )*
    ;

logical_term
    :
     logical_factor ( "and" logical_factor )*
    ;

logical_factor
    :
      ( exp_simple comparison_op exp_simple ) => ( exp_simple comparison_op exp_simple )
      | ( exp_simple ( "not" )? "in" ) => exp_simple ("not")? "in" exp_set
      | ( exp_simple ( "not" )? "like" ) => exp_simple ( "not" )? "like" expression ( "escape" QUOTED_STRING )?
      | ( exp_simple ( "not" )? "between" ) => exp_simple ( "not" )? "between" exp_simple "and" exp_simple
      | ( exp_simple "is" ( "not" )? "null" ) => exp_simple "is" ( "not" )? "null"
      | ( quantified_factor ) => quantified_factor
      | ( "not" condition ) => "not" condition
      | ( OPEN_PAREN condition CLOSE_PAREN )
      | row_status_clause
    ;

row_status_condition
    :
    ( DOLLAR "row_status" "is" ( ("new") | (("not")? "dead") ) )
    { #row_status_condition = #([ROW_STATUS_CONDITION, "row_status_condition"], #row_status_condition); }
    ;

row_status_clause
    :
    ( ((schema_name DOT)? table_name DOT)? row_status_condition ) => ( ((schema_name DOT)? table_name DOT)? row_status_condition )
    { #row_status_clause = #([ROW_STATUS_CLAUSE, "row_status_clause"], #row_status_clause); }
    ;

quantified_factor
    :
      ( exp_simple comparison_op ( "all" | "any" )? subquery ) => exp_simple comparison_op ( "all" | "any" )? subquery
      | ( ( "not" )? "exists" subquery ) => ( "not" )? "exists" subquery
      | subquery
    ;

comparison_op
    :
      EQ | LT | GT | NOT_EQ | LE | GE
    ;

exp_set
    : 
      ( exp_simple ) => exp_simple
      | subquery
    ;

subquery
    :
      ( OPEN_PAREN select_expression ) =>
      OPEN_PAREN select_expression CLOSE_PAREN
    ;

group_clause
    :
      "group" "by" expression ( COMMA expression )* ( "having" condition )?
    ;

// Would this really do what is necessary?  The following does not look
// right, but not that familiar with what is being refered to here.
combine_clause
      :
      ( ("union" ("all")?)
      | "minus"
      | "except"
      | "intersect" ) select_expression
    ;

order_clause
    :
      "order" "by" sorted_def ( COMMA sorted_def )*
    ;
    
first_clause
    :
      ( "first" NUMBER )
      { #first_clause = #([FIRST_CLAUSE, "first_clause"], #first_clause); }
    ;
    
limit_clause
    :
      ( "limit" NUMBER ("offset" NUMBER)? )
      { #limit_clause = #([LIMIT_CLAUSE, "limit_clause"], #limit_clause); }
    ;

sorted_def
    :
      (( expression ) => expression
      |
      ( NUMBER ) => NUMBER ) ( "asc" | "desc" )? ("nulls" ("first" | "last"))?
    ;

//
// Direct mappings to lexer.
//

identifier
    :
        ( IDENTIFIER | QUOTED_STRING | keyword )
    ;

quoted_string : QUOTED_STRING ;

match_string : QUOTED_STRING ;

//
// These are non reserve words that can be used as identifiers.  If it is
// a reserved word in Oracle but not ANSI, that is noted and commented out
// (can not be used). If it is a reserve word in ANSI and not in Oracle,
// that is noted but it is not commented out (can be used).
//

//todo Cleanup these keywords, functions above etc.

keyword
    :
        "filter"
        | "using"
        | "with"
        | "group"
        | "by"

        | "partition"
        | "store"

        | "avg"
        | "count"
        | "custom"
        | "geomean"
        | "kurtosis"
        | "max"
        | "median"
        | "min"
        | "skewness"
        | "stddev"
        | "sum"
        | "sumsq"
        | "variance"
        | "diff"
        | "current_timestamp"

		| "pinned"
		| "entrance"
		| "only"

        | "last"
        | "latest"
        | "random"
        | "lowest"
        | "highest"

        | "milliseconds"
        | "seconds"
        | "minutes"
        | "hours"
        | "days"

        | "limit"
        | "offset"

        | "union"
        | "all"
        | "minus"
        | "except"
        | "intersect"

        | "of"

		| "self"
        | "row_status"
        | "is"
        | "new"
        | "dead"
        
        | "alert"
        | "present"
        | "correlate"
    ;

//
// Lexer
//

class RQLLexer extends Lexer;

options {
    exportVocab = RQL;
    testLiterals = false;
    k = 2;
    caseSensitive = false;
    caseSensitiveLiterals = false;
    charVocabulary = '\3' .. '\177';
    defaultErrorHandler=false;
}

IDENTIFIER options { testLiterals=true; }
    :
        'a' .. 'z' ( 'a' .. 'z' | '0' .. '9' | '_' | '$' )*
    ;

QUOTED_STRING
      : '\'' ( ~'\'' )* '\''
    ;

SEMI : ';';

DOT : '.' ;
COMMA : ',' ;
ASTERISK : '*' ;
AT_SIGN : '@' ;
DOLLAR : '$';
POUND : '#';

OPEN_PAREN : '(' ;
CLOSE_PAREN : ')' ;

PLUS : '+' ;
MINUS : '-' ;
DIVIDE : '/' ;
MODULO : '%' ;

VERTBAR : '|' ;

EQ : '=' ;

// Why did I do this?  Isn't this handle by just increasing the look ahead?
NOT_EQ :
            '<' { _ttype = LT; }
                (       ( '>' { _ttype = NOT_EQ; } )
                    |   ( '=' { _ttype = LE; } ) )?
        | "!=" | "^="
    ;
GT : '>' ( '=' { _ttype = GE; } )? ;

// match_string
//    ::= "'" { "any_character" | "_" | "%" } "'"

//
// This is not right.  It will never pickup a leading PLUS, MINUS, or DOT.
// Should look at examples/java/java/java.g and follow that example.  Not
// a priority just yet though.

NUMBER
    :
        ( PLUS | MINUS )?
        ( ( N DOT N ) => N DOT N | DOT N | N )
        ( "e" ( PLUS | MINUS )? N )?
    ;

protected
N : '0' .. '9' ( '0' .. '9' )* ;

// Not sure exactly what the purpose of a double quote is.  It has cropped up
// around column names and aliases.  Maybe that means you could have
// table names, column names, or even aliases with spaces.  If so, they should
// no longer be skipped and added the rules.
DOUBLE_QUOTE : '"' { $setType(Token.SKIP); } ;

WS  :   (   ' '
        |   '\t'
        |   '\r' '\n' { newline(); }
        |   '\n'      { newline(); }
        |   '\r'      { newline(); }
        )
        {$setType(Token.SKIP);} //ignore this token
    ;

// Taken right from antlr-2.7.1/examples/java/java/java.g ...
// multiple-line comments
ML_COMMENT
    :   "/*"
        (   /*  '\r' '\n' can be matched in one alternative or by matching
                '\r' in one iteration and '\n' in another.  I am trying to
                handle any flavor of newline that comes in, but the language
                that allows both "\r\n" and "\r" and "\n" to all be valid
                newline is ambiguous.  Consequently, the resulting grammar
                must be ambiguous.  I'm shutting this warning off.
             */
            options {
                generateAmbigWarnings=false;
            }
        :
            { LA(2)!='/' }? '*'
        |   '\r' '\n'       {newline();}
        |   '\r'            {newline();}
        |   '\n'            {newline();}
        |   ~('*'|'\n'|'\r')
        )*
        "*/"
        {$setType(Token.SKIP);}
    ;