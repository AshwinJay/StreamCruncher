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

public interface RQLTokenTypes {
    int EOF = 1;

    int NULL_TREE_LOOKAHEAD = 3;

    int SQL_STATEMENT = 4;

    int SELECT_LIST = 5;

    int SELECT_EXPRESSION = 6;

    int DISPLAYED_COLUMN = 7;

    int CASE_EXPRESSION = 8;

    int EXP_SIMPLE = 9;

    int TABLE_REFERENCE_LIST = 10;

    int SELECTED_TABLE = 11;

    int TABLE_SPEC = 12;

    int ALIAS = 13;

    int FILTER_SPEC = 14;

    int PARTITION_SPEC_LIST = 15;

    int PARTITION_SPEC = 16;

    int ROW_PICK_SPEC = 17;

    int WHERE_CONDITION = 18;

    int FIRST_CLAUSE = 19;

    int LIMIT_CLAUSE = 20;

    int AGGREGATE_FUNCTION = 21;

    int AGGREGATE_FUNCTION_SPEC = 22;

    int MISC_FUNCTION = 23;

    int WINDOW_FUNCTION = 24;

    int TIME_UNIT_SPEC = 25;

    int GROUP_FUNCTION = 26;

    int ROW_STATUS_CLAUSE = 27;

    int ROW_STATUS_CONDITION = 28;

    int CLONE_PARTITION_CLAUSE = 29;

    int POST_WHERE_CLAUSES = 30;

    int MONITOR_EXPRESSION = 31;

    int PARTITION_COLUMN_NAME_LIST = 32;

    int USING_LIST = 33;

    int USING_SPEC = 34;

    int PRESENT_LIST = 35;

    int PRESENT_SPEC = 36;

    int SEMI = 37;

    int LITERAL_select = 38;

    int LITERAL_all = 39;

    int LITERAL_distinct = 40;

    int LITERAL_from = 41;

    int LITERAL_where = 42;

    int COMMA = 43;

    int ASTERISK = 44;

    int LITERAL_case = 45;

    int LITERAL_else = 46;

    int LITERAL_end = 47;

    int LITERAL_when = 48;

    int LITERAL_then = 49;

    int LITERAL_left = 50;

    int LITERAL_right = 51;

    int LITERAL_outer = 52;

    int LITERAL_inner = 53;

    int LITERAL_join = 54;

    int LITERAL_on = 55;

    int DOT = 56;

    int LITERAL_self = 57;

    int POUND = 58;

    int PLUS = 59;

    int MINUS = 60;

    int LITERAL_as = 61;

    int DIVIDE = 62;

    int MODULO = 63;

    int VERTBAR = 64;

    int OPEN_PAREN = 65;

    int CLOSE_PAREN = 66;

    int NUMBER = 67;

    int QUOTED_STRING = 68;

    int LITERAL_null = 69;

    int LITERAL_avg = 70;

    int LITERAL_count = 71;

    int LITERAL_max = 72;

    int LITERAL_min = 73;

    int LITERAL_stddev = 74;

    int LITERAL_sum = 75;

    int LITERAL_variance = 76;

    int LITERAL_filter = 77;

    int LITERAL_using = 78;

    int LITERAL_to = 79;

    int LITERAL_partition = 80;

    int LITERAL_by = 81;

    int IDENTIFIER = 82;

    int LITERAL_store = 83;

    int LITERAL_random = 84;

    int LITERAL_lowest = 85;

    int LITERAL_highest = 86;

    int LITERAL_with = 87;

    int LITERAL_update = 88;

    int LITERAL_group = 89;

    int LITERAL_pinned = 90;

    int LITERAL_geomean = 91;

    int LITERAL_kurtosis = 92;

    int LITERAL_median = 93;

    int LITERAL_skewness = 94;

    int LITERAL_sumsq = 95;

    int DOLLAR = 96;

    int LITERAL_diff = 97;

    int LITERAL_custom = 98;

    int LITERAL_entrance = 99;

    int LITERAL_only = 100;

    int LITERAL_last = 101;

    int LITERAL_latest = 102;

    int LITERAL_milliseconds = 103;

    int LITERAL_seconds = 104;

    int LITERAL_minutes = 105;

    int LITERAL_hours = 106;

    int LITERAL_days = 107;

    int LITERAL_alert = 108;

    int LITERAL_correlate = 109;

    int LITERAL_or = 110;

    int LITERAL_present = 111;

    int LITERAL_and = 112;

    int LITERAL_not = 113;

    int LITERAL_in = 114;

    int LITERAL_like = 115;

    int LITERAL_escape = 116;

    int LITERAL_between = 117;

    int LITERAL_is = 118;

    int LITERAL_row_status = 119;

    int LITERAL_new = 120;

    int LITERAL_dead = 121;

    int LITERAL_any = 122;

    int LITERAL_exists = 123;

    int EQ = 124;

    int LT = 125;

    int GT = 126;

    int NOT_EQ = 127;

    int LE = 128;

    int GE = 129;

    int LITERAL_having = 130;

    int LITERAL_union = 131;

    int LITERAL_minus = 132;

    int LITERAL_except = 133;

    int LITERAL_intersect = 134;

    int LITERAL_order = 135;

    int LITERAL_first = 136;

    int LITERAL_limit = 137;

    int LITERAL_offset = 138;

    int LITERAL_asc = 139;

    int LITERAL_desc = 140;

    int LITERAL_nulls = 141;

    int LITERAL_current_timestamp = 142;

    int LITERAL_of = 143;

    int AT_SIGN = 144;

    int N = 145;

    int DOUBLE_QUOTE = 146;

    int WS = 147;

    int ML_COMMENT = 148;
}
