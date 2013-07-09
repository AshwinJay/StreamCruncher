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
package streamcruncher.innards.query;

import java.io.ByteArrayInputStream;

import streamcruncher.api.ParserParameters;
import streamcruncher.api.artifact.RunningQuery;
import streamcruncher.innards.impl.query.RQLLexer;
import streamcruncher.innards.impl.query.RQLParser;
import antlr.RecognitionException;
import antlr.TokenStreamException;

/*
 * Author: Ashwin Jayaprakash Date: Feb 7, 2006 Time: 8:54:55 PM
 */

public abstract class Parser {
    protected final String query;

    protected final String queryName;

    protected final String[] resultColumnTypes;

    protected final RQLParser parser;

    /**
     * @param parserParameters
     * @throws QueryParseException
     */
    public Parser(ParserParameters parserParameters) throws QueryParseException {
        this.query = parserParameters.getQuery();
        this.queryName = parserParameters.getQueryName();
        this.resultColumnTypes = parserParameters.getResultColumnTypes();

        // -------------------------

        byte[] bytes = query.getBytes();
        RQLLexer lexer = new RQLLexer(new ByteArrayInputStream(bytes));

        this.parser = new RQLParser(lexer);

        try {
            parser.start_rule();
        }
        catch (RecognitionException e) {
            StringBuilder sb = new StringBuilder(this.query);

            int c = e.getColumn();
            c--;
            sb.append("\r\n");
            for (int i = 0; i < c; i++) {
                sb.append('-');
            }
            sb.append('^');

            sb.insert(0, "\r\n");

            String s = e.getMessage()
                    + " at column: "
                    + (c + 1)
                    + " [Error location might be inaccurate if query has \\r or \\n or \\t characters]";
            sb.insert(0, s);

            throw new QueryParseException(sb.toString());
        }
        catch (TokenStreamException e) {
            StringBuilder sb = new StringBuilder(e.getMessage());
            sb.append("\r\nQuery: ");
            sb.append(this.query);

            throw new QueryParseException(e);
        }
    }

    public abstract RunningQuery parse() throws QueryParseException;
}
