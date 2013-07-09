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

import java.io.InputStream;
import java.io.Reader;
import java.util.Hashtable;

import antlr.ANTLRHashString;
import antlr.ByteBuffer;
import antlr.CharBuffer;
import antlr.CharStreamException;
import antlr.CharStreamIOException;
import antlr.InputBuffer;
import antlr.LexerSharedInputState;
import antlr.NoViableAltForCharException;
import antlr.RecognitionException;
import antlr.Token;
import antlr.TokenStream;
import antlr.TokenStreamException;
import antlr.TokenStreamIOException;
import antlr.TokenStreamRecognitionException;
import antlr.collections.impl.BitSet;

public class RQLLexer extends antlr.CharScanner implements RQLTokenTypes, TokenStream {
    public RQLLexer(InputStream in) {
        this(new ByteBuffer(in));
    }

    public RQLLexer(Reader in) {
        this(new CharBuffer(in));
    }

    public RQLLexer(InputBuffer ib) {
        this(new LexerSharedInputState(ib));
    }

    public RQLLexer(LexerSharedInputState state) {
        super(state);
        caseSensitiveLiterals = false;
        setCaseSensitive(false);
        literals = new Hashtable();
        literals.put(new ANTLRHashString("between", this), new Integer(117));
        literals.put(new ANTLRHashString("diff", this), new Integer(97));
        literals.put(new ANTLRHashString("case", this), new Integer(45));
        literals.put(new ANTLRHashString("highest", this), new Integer(86));
        literals.put(new ANTLRHashString("new", this), new Integer(120));
        literals.put(new ANTLRHashString("end", this), new Integer(47));
        literals.put(new ANTLRHashString("alert", this), new Integer(108));
        literals.put(new ANTLRHashString("geomean", this), new Integer(91));
        literals.put(new ANTLRHashString("limit", this), new Integer(137));
        literals.put(new ANTLRHashString("distinct", this), new Integer(40));
        literals.put(new ANTLRHashString("seconds", this), new Integer(104));
        literals.put(new ANTLRHashString("where", this), new Integer(42));
        literals.put(new ANTLRHashString("minutes", this), new Integer(105));
        literals.put(new ANTLRHashString("then", this), new Integer(49));
        literals.put(new ANTLRHashString("select", this), new Integer(38));
        literals.put(new ANTLRHashString("present", this), new Integer(111));
        literals.put(new ANTLRHashString("to", this), new Integer(79));
        literals.put(new ANTLRHashString("and", this), new Integer(112));
        literals.put(new ANTLRHashString("outer", this), new Integer(52));
        literals.put(new ANTLRHashString("not", this), new Integer(113));
        literals.put(new ANTLRHashString("using", this), new Integer(78));
        literals.put(new ANTLRHashString("offset", this), new Integer(138));
        literals.put(new ANTLRHashString("from", this), new Integer(41));
        literals.put(new ANTLRHashString("null", this), new Integer(69));
        literals.put(new ANTLRHashString("count", this), new Integer(71));
        literals.put(new ANTLRHashString("skewness", this), new Integer(94));
        literals.put(new ANTLRHashString("last", this), new Integer(101));
        literals.put(new ANTLRHashString("variance", this), new Integer(76));
        literals.put(new ANTLRHashString("like", this), new Integer(115));
        literals.put(new ANTLRHashString("when", this), new Integer(48));
        literals.put(new ANTLRHashString("inner", this), new Integer(53));
        literals.put(new ANTLRHashString("except", this), new Integer(133));
        literals.put(new ANTLRHashString("custom", this), new Integer(98));
        literals.put(new ANTLRHashString("with", this), new Integer(87));
        literals.put(new ANTLRHashString("escape", this), new Integer(116));
        literals.put(new ANTLRHashString("only", this), new Integer(100));
        literals.put(new ANTLRHashString("intersect", this), new Integer(134));
        literals.put(new ANTLRHashString("join", this), new Integer(54));
        literals.put(new ANTLRHashString("of", this), new Integer(143));
        literals.put(new ANTLRHashString("is", this), new Integer(118));
        literals.put(new ANTLRHashString("kurtosis", this), new Integer(92));
        literals.put(new ANTLRHashString("or", this), new Integer(110));
        literals.put(new ANTLRHashString("any", this), new Integer(122));
        literals.put(new ANTLRHashString("correlate", this), new Integer(109));
        literals.put(new ANTLRHashString("min", this), new Integer(73));
        literals.put(new ANTLRHashString("as", this), new Integer(61));
        literals.put(new ANTLRHashString("first", this), new Integer(136));
        literals.put(new ANTLRHashString("by", this), new Integer(81));
        literals.put(new ANTLRHashString("minus", this), new Integer(132));
        literals.put(new ANTLRHashString("days", this), new Integer(107));
        literals.put(new ANTLRHashString("nulls", this), new Integer(141));
        literals.put(new ANTLRHashString("all", this), new Integer(39));
        literals.put(new ANTLRHashString("union", this), new Integer(131));
        literals.put(new ANTLRHashString("order", this), new Integer(135));
        literals.put(new ANTLRHashString("partition", this), new Integer(80));
        literals.put(new ANTLRHashString("hours", this), new Integer(106));
        literals.put(new ANTLRHashString("sumsq", this), new Integer(95));
        literals.put(new ANTLRHashString("lowest", this), new Integer(85));
        literals.put(new ANTLRHashString("exists", this), new Integer(123));
        literals.put(new ANTLRHashString("milliseconds", this), new Integer(103));
        literals.put(new ANTLRHashString("asc", this), new Integer(139));
        literals.put(new ANTLRHashString("row_status", this), new Integer(119));
        literals.put(new ANTLRHashString("left", this), new Integer(50));
        literals.put(new ANTLRHashString("desc", this), new Integer(140));
        literals.put(new ANTLRHashString("max", this), new Integer(72));
        literals.put(new ANTLRHashString("random", this), new Integer(84));
        literals.put(new ANTLRHashString("sum", this), new Integer(75));
        literals.put(new ANTLRHashString("on", this), new Integer(55));
        literals.put(new ANTLRHashString("else", this), new Integer(46));
        literals.put(new ANTLRHashString("right", this), new Integer(51));
        literals.put(new ANTLRHashString("filter", this), new Integer(77));
        literals.put(new ANTLRHashString("dead", this), new Integer(121));
        literals.put(new ANTLRHashString("store", this), new Integer(83));
        literals.put(new ANTLRHashString("in", this), new Integer(114));
        literals.put(new ANTLRHashString("self", this), new Integer(57));
        literals.put(new ANTLRHashString("avg", this), new Integer(70));
        literals.put(new ANTLRHashString("median", this), new Integer(93));
        literals.put(new ANTLRHashString("update", this), new Integer(88));
        literals.put(new ANTLRHashString("latest", this), new Integer(102));
        literals.put(new ANTLRHashString("stddev", this), new Integer(74));
        literals.put(new ANTLRHashString("pinned", this), new Integer(90));
        literals.put(new ANTLRHashString("entrance", this), new Integer(99));
        literals.put(new ANTLRHashString("group", this), new Integer(89));
        literals.put(new ANTLRHashString("having", this), new Integer(130));
        literals.put(new ANTLRHashString("current_timestamp", this), new Integer(142));
    }

    public Token nextToken() throws TokenStreamException {
        Token theRetToken = null;
        tryAgain: for (;;) {
            Token _token = null;
            int _ttype = Token.INVALID_TYPE;
            resetText();
            try { // for char stream error handling
                try { // for lexical error handling
                    switch (LA(1)) {
                        case 'a':
                        case 'b':
                        case 'c':
                        case 'd':
                        case 'e':
                        case 'f':
                        case 'g':
                        case 'h':
                        case 'i':
                        case 'j':
                        case 'k':
                        case 'l':
                        case 'm':
                        case 'n':
                        case 'o':
                        case 'p':
                        case 'q':
                        case 'r':
                        case 's':
                        case 't':
                        case 'u':
                        case 'v':
                        case 'w':
                        case 'x':
                        case 'y':
                        case 'z': {
                            mIDENTIFIER(true);
                            theRetToken = _returnToken;
                            break;
                        }
                        case '\'': {
                            mQUOTED_STRING(true);
                            theRetToken = _returnToken;
                            break;
                        }
                        case ';': {
                            mSEMI(true);
                            theRetToken = _returnToken;
                            break;
                        }
                        case ',': {
                            mCOMMA(true);
                            theRetToken = _returnToken;
                            break;
                        }
                        case '*': {
                            mASTERISK(true);
                            theRetToken = _returnToken;
                            break;
                        }
                        case '@': {
                            mAT_SIGN(true);
                            theRetToken = _returnToken;
                            break;
                        }
                        case '$': {
                            mDOLLAR(true);
                            theRetToken = _returnToken;
                            break;
                        }
                        case '#': {
                            mPOUND(true);
                            theRetToken = _returnToken;
                            break;
                        }
                        case '(': {
                            mOPEN_PAREN(true);
                            theRetToken = _returnToken;
                            break;
                        }
                        case ')': {
                            mCLOSE_PAREN(true);
                            theRetToken = _returnToken;
                            break;
                        }
                        case '%': {
                            mMODULO(true);
                            theRetToken = _returnToken;
                            break;
                        }
                        case '|': {
                            mVERTBAR(true);
                            theRetToken = _returnToken;
                            break;
                        }
                        case '=': {
                            mEQ(true);
                            theRetToken = _returnToken;
                            break;
                        }
                        case '!':
                        case '<':
                        case '^': {
                            mNOT_EQ(true);
                            theRetToken = _returnToken;
                            break;
                        }
                        case '>': {
                            mGT(true);
                            theRetToken = _returnToken;
                            break;
                        }
                        case '"': {
                            mDOUBLE_QUOTE(true);
                            theRetToken = _returnToken;
                            break;
                        }
                        case '\t':
                        case '\n':
                        case '\r':
                        case ' ': {
                            mWS(true);
                            theRetToken = _returnToken;
                            break;
                        }
                        default:
                            if ((LA(1) == '/') && (LA(2) == '*')) {
                                mML_COMMENT(true);
                                theRetToken = _returnToken;
                            }
                            else if ((LA(1) == '.') && (true)) {
                                mDOT(true);
                                theRetToken = _returnToken;
                            }
                            else if ((LA(1) == '+') && (true)) {
                                mPLUS(true);
                                theRetToken = _returnToken;
                            }
                            else if ((LA(1) == '-') && (true)) {
                                mMINUS(true);
                                theRetToken = _returnToken;
                            }
                            else if ((LA(1) == '/') && (true)) {
                                mDIVIDE(true);
                                theRetToken = _returnToken;
                            }
                            else if ((_tokenSet_0.member(LA(1))) && (true)) {
                                mNUMBER(true);
                                theRetToken = _returnToken;
                            }
                            else {
                                if (LA(1) == EOF_CHAR) {
                                    uponEOF();
                                    _returnToken = makeToken(Token.EOF_TYPE);
                                }
                                else {
                                    throw new NoViableAltForCharException((char) LA(1),
                                            getFilename(), getLine(), getColumn());
                                }
                            }
                    }
                    if (_returnToken == null)
                        continue tryAgain; // found SKIP token
                    _ttype = _returnToken.getType();
                    _returnToken.setType(_ttype);
                    return _returnToken;
                }
                catch (RecognitionException e) {
                    throw new TokenStreamRecognitionException(e);
                }
            }
            catch (CharStreamException cse) {
                if (cse instanceof CharStreamIOException) {
                    throw new TokenStreamIOException(((CharStreamIOException) cse).io);
                }
                else {
                    throw new TokenStreamException(cse.getMessage());
                }
            }
        }
    }

    public final void mIDENTIFIER(boolean _createToken) throws RecognitionException,
            CharStreamException, TokenStreamException {
        int _ttype;
        Token _token = null;
        int _begin = text.length();
        _ttype = IDENTIFIER;
        int _saveIndex;

        matchRange('a', 'z');
        {
            _loop305: do {
                switch (LA(1)) {
                    case 'a':
                    case 'b':
                    case 'c':
                    case 'd':
                    case 'e':
                    case 'f':
                    case 'g':
                    case 'h':
                    case 'i':
                    case 'j':
                    case 'k':
                    case 'l':
                    case 'm':
                    case 'n':
                    case 'o':
                    case 'p':
                    case 'q':
                    case 'r':
                    case 's':
                    case 't':
                    case 'u':
                    case 'v':
                    case 'w':
                    case 'x':
                    case 'y':
                    case 'z': {
                        matchRange('a', 'z');
                        break;
                    }
                    case '0':
                    case '1':
                    case '2':
                    case '3':
                    case '4':
                    case '5':
                    case '6':
                    case '7':
                    case '8':
                    case '9': {
                        matchRange('0', '9');
                        break;
                    }
                    case '_': {
                        match('_');
                        break;
                    }
                    case '$': {
                        match('$');
                        break;
                    }
                    default: {
                        break _loop305;
                    }
                }
            } while (true);
        }
        _ttype = testLiteralsTable(_ttype);
        if (_createToken && _token == null && _ttype != Token.SKIP) {
            _token = makeToken(_ttype);
            _token.setText(new String(text.getBuffer(), _begin, text.length() - _begin));
        }
        _returnToken = _token;
    }

    public final void mQUOTED_STRING(boolean _createToken) throws RecognitionException,
            CharStreamException, TokenStreamException {
        int _ttype;
        Token _token = null;
        int _begin = text.length();
        _ttype = QUOTED_STRING;
        int _saveIndex;

        match('\'');
        {
            _loop308: do {
                if ((_tokenSet_1.member(LA(1)))) {
                    matchNot('\'');
                }
                else {
                    break _loop308;
                }

            } while (true);
        }
        match('\'');
        if (_createToken && _token == null && _ttype != Token.SKIP) {
            _token = makeToken(_ttype);
            _token.setText(new String(text.getBuffer(), _begin, text.length() - _begin));
        }
        _returnToken = _token;
    }

    public final void mSEMI(boolean _createToken) throws RecognitionException, CharStreamException,
            TokenStreamException {
        int _ttype;
        Token _token = null;
        int _begin = text.length();
        _ttype = SEMI;
        int _saveIndex;

        match(';');
        if (_createToken && _token == null && _ttype != Token.SKIP) {
            _token = makeToken(_ttype);
            _token.setText(new String(text.getBuffer(), _begin, text.length() - _begin));
        }
        _returnToken = _token;
    }

    public final void mDOT(boolean _createToken) throws RecognitionException, CharStreamException,
            TokenStreamException {
        int _ttype;
        Token _token = null;
        int _begin = text.length();
        _ttype = DOT;
        int _saveIndex;

        match('.');
        if (_createToken && _token == null && _ttype != Token.SKIP) {
            _token = makeToken(_ttype);
            _token.setText(new String(text.getBuffer(), _begin, text.length() - _begin));
        }
        _returnToken = _token;
    }

    public final void mCOMMA(boolean _createToken) throws RecognitionException,
            CharStreamException, TokenStreamException {
        int _ttype;
        Token _token = null;
        int _begin = text.length();
        _ttype = COMMA;
        int _saveIndex;

        match(',');
        if (_createToken && _token == null && _ttype != Token.SKIP) {
            _token = makeToken(_ttype);
            _token.setText(new String(text.getBuffer(), _begin, text.length() - _begin));
        }
        _returnToken = _token;
    }

    public final void mASTERISK(boolean _createToken) throws RecognitionException,
            CharStreamException, TokenStreamException {
        int _ttype;
        Token _token = null;
        int _begin = text.length();
        _ttype = ASTERISK;
        int _saveIndex;

        match('*');
        if (_createToken && _token == null && _ttype != Token.SKIP) {
            _token = makeToken(_ttype);
            _token.setText(new String(text.getBuffer(), _begin, text.length() - _begin));
        }
        _returnToken = _token;
    }

    public final void mAT_SIGN(boolean _createToken) throws RecognitionException,
            CharStreamException, TokenStreamException {
        int _ttype;
        Token _token = null;
        int _begin = text.length();
        _ttype = AT_SIGN;
        int _saveIndex;

        match('@');
        if (_createToken && _token == null && _ttype != Token.SKIP) {
            _token = makeToken(_ttype);
            _token.setText(new String(text.getBuffer(), _begin, text.length() - _begin));
        }
        _returnToken = _token;
    }

    public final void mDOLLAR(boolean _createToken) throws RecognitionException,
            CharStreamException, TokenStreamException {
        int _ttype;
        Token _token = null;
        int _begin = text.length();
        _ttype = DOLLAR;
        int _saveIndex;

        match('$');
        if (_createToken && _token == null && _ttype != Token.SKIP) {
            _token = makeToken(_ttype);
            _token.setText(new String(text.getBuffer(), _begin, text.length() - _begin));
        }
        _returnToken = _token;
    }

    public final void mPOUND(boolean _createToken) throws RecognitionException,
            CharStreamException, TokenStreamException {
        int _ttype;
        Token _token = null;
        int _begin = text.length();
        _ttype = POUND;
        int _saveIndex;

        match('#');
        if (_createToken && _token == null && _ttype != Token.SKIP) {
            _token = makeToken(_ttype);
            _token.setText(new String(text.getBuffer(), _begin, text.length() - _begin));
        }
        _returnToken = _token;
    }

    public final void mOPEN_PAREN(boolean _createToken) throws RecognitionException,
            CharStreamException, TokenStreamException {
        int _ttype;
        Token _token = null;
        int _begin = text.length();
        _ttype = OPEN_PAREN;
        int _saveIndex;

        match('(');
        if (_createToken && _token == null && _ttype != Token.SKIP) {
            _token = makeToken(_ttype);
            _token.setText(new String(text.getBuffer(), _begin, text.length() - _begin));
        }
        _returnToken = _token;
    }

    public final void mCLOSE_PAREN(boolean _createToken) throws RecognitionException,
            CharStreamException, TokenStreamException {
        int _ttype;
        Token _token = null;
        int _begin = text.length();
        _ttype = CLOSE_PAREN;
        int _saveIndex;

        match(')');
        if (_createToken && _token == null && _ttype != Token.SKIP) {
            _token = makeToken(_ttype);
            _token.setText(new String(text.getBuffer(), _begin, text.length() - _begin));
        }
        _returnToken = _token;
    }

    public final void mPLUS(boolean _createToken) throws RecognitionException, CharStreamException,
            TokenStreamException {
        int _ttype;
        Token _token = null;
        int _begin = text.length();
        _ttype = PLUS;
        int _saveIndex;

        match('+');
        if (_createToken && _token == null && _ttype != Token.SKIP) {
            _token = makeToken(_ttype);
            _token.setText(new String(text.getBuffer(), _begin, text.length() - _begin));
        }
        _returnToken = _token;
    }

    public final void mMINUS(boolean _createToken) throws RecognitionException,
            CharStreamException, TokenStreamException {
        int _ttype;
        Token _token = null;
        int _begin = text.length();
        _ttype = MINUS;
        int _saveIndex;

        match('-');
        if (_createToken && _token == null && _ttype != Token.SKIP) {
            _token = makeToken(_ttype);
            _token.setText(new String(text.getBuffer(), _begin, text.length() - _begin));
        }
        _returnToken = _token;
    }

    public final void mDIVIDE(boolean _createToken) throws RecognitionException,
            CharStreamException, TokenStreamException {
        int _ttype;
        Token _token = null;
        int _begin = text.length();
        _ttype = DIVIDE;
        int _saveIndex;

        match('/');
        if (_createToken && _token == null && _ttype != Token.SKIP) {
            _token = makeToken(_ttype);
            _token.setText(new String(text.getBuffer(), _begin, text.length() - _begin));
        }
        _returnToken = _token;
    }

    public final void mMODULO(boolean _createToken) throws RecognitionException,
            CharStreamException, TokenStreamException {
        int _ttype;
        Token _token = null;
        int _begin = text.length();
        _ttype = MODULO;
        int _saveIndex;

        match('%');
        if (_createToken && _token == null && _ttype != Token.SKIP) {
            _token = makeToken(_ttype);
            _token.setText(new String(text.getBuffer(), _begin, text.length() - _begin));
        }
        _returnToken = _token;
    }

    public final void mVERTBAR(boolean _createToken) throws RecognitionException,
            CharStreamException, TokenStreamException {
        int _ttype;
        Token _token = null;
        int _begin = text.length();
        _ttype = VERTBAR;
        int _saveIndex;

        match('|');
        if (_createToken && _token == null && _ttype != Token.SKIP) {
            _token = makeToken(_ttype);
            _token.setText(new String(text.getBuffer(), _begin, text.length() - _begin));
        }
        _returnToken = _token;
    }

    public final void mEQ(boolean _createToken) throws RecognitionException, CharStreamException,
            TokenStreamException {
        int _ttype;
        Token _token = null;
        int _begin = text.length();
        _ttype = EQ;
        int _saveIndex;

        match('=');
        if (_createToken && _token == null && _ttype != Token.SKIP) {
            _token = makeToken(_ttype);
            _token.setText(new String(text.getBuffer(), _begin, text.length() - _begin));
        }
        _returnToken = _token;
    }

    public final void mNOT_EQ(boolean _createToken) throws RecognitionException,
            CharStreamException, TokenStreamException {
        int _ttype;
        Token _token = null;
        int _begin = text.length();
        _ttype = NOT_EQ;
        int _saveIndex;

        switch (LA(1)) {
            case '<': {
                match('<');
                if (inputState.guessing == 0) {
                    _ttype = LT;
                }
                {
                    switch (LA(1)) {
                        case '>': {
                            {
                                match('>');
                                if (inputState.guessing == 0) {
                                    _ttype = NOT_EQ;
                                }
                            }
                            break;
                        }
                        case '=': {
                            {
                                match('=');
                                if (inputState.guessing == 0) {
                                    _ttype = LE;
                                }
                            }
                            break;
                        }
                        default: {
                        }
                    }
                }
                break;
            }
            case '!': {
                match("!=");
                break;
            }
            case '^': {
                match("^=");
                break;
            }
            default: {
                throw new NoViableAltForCharException((char) LA(1), getFilename(), getLine(),
                        getColumn());
            }
        }
        if (_createToken && _token == null && _ttype != Token.SKIP) {
            _token = makeToken(_ttype);
            _token.setText(new String(text.getBuffer(), _begin, text.length() - _begin));
        }
        _returnToken = _token;
    }

    public final void mGT(boolean _createToken) throws RecognitionException, CharStreamException,
            TokenStreamException {
        int _ttype;
        Token _token = null;
        int _begin = text.length();
        _ttype = GT;
        int _saveIndex;

        match('>');
        {
            if ((LA(1) == '=')) {
                match('=');
                if (inputState.guessing == 0) {
                    _ttype = GE;
                }
            }
            else {
            }

        }
        if (_createToken && _token == null && _ttype != Token.SKIP) {
            _token = makeToken(_ttype);
            _token.setText(new String(text.getBuffer(), _begin, text.length() - _begin));
        }
        _returnToken = _token;
    }

    public final void mNUMBER(boolean _createToken) throws RecognitionException,
            CharStreamException, TokenStreamException {
        int _ttype;
        Token _token = null;
        int _begin = text.length();
        _ttype = NUMBER;
        int _saveIndex;

        {
            switch (LA(1)) {
                case '+': {
                    mPLUS(false);
                    break;
                }
                case '-': {
                    mMINUS(false);
                    break;
                }
                case '.':
                case '0':
                case '1':
                case '2':
                case '3':
                case '4':
                case '5':
                case '6':
                case '7':
                case '8':
                case '9': {
                    break;
                }
                default: {
                    throw new NoViableAltForCharException((char) LA(1), getFilename(), getLine(),
                            getColumn());
                }
            }
        }
        {
            boolean synPredMatched334 = false;
            if ((((LA(1) >= '0' && LA(1) <= '9')) && (_tokenSet_2.member(LA(2))))) {
                int _m334 = mark();
                synPredMatched334 = true;
                inputState.guessing++;
                try {
                    {
                        mN(false);
                        mDOT(false);
                        mN(false);
                    }
                }
                catch (RecognitionException pe) {
                    synPredMatched334 = false;
                }
                rewind(_m334);
                inputState.guessing--;
            }
            if (synPredMatched334) {
                mN(false);
                mDOT(false);
                mN(false);
            }
            else if ((LA(1) == '.')) {
                mDOT(false);
                mN(false);
            }
            else if (((LA(1) >= '0' && LA(1) <= '9')) && (true)) {
                mN(false);
            }
            else {
                throw new NoViableAltForCharException((char) LA(1), getFilename(), getLine(),
                        getColumn());
            }

        }
        {
            if ((LA(1) == 'e')) {
                match("e");
                {
                    switch (LA(1)) {
                        case '+': {
                            mPLUS(false);
                            break;
                        }
                        case '-': {
                            mMINUS(false);
                            break;
                        }
                        case '0':
                        case '1':
                        case '2':
                        case '3':
                        case '4':
                        case '5':
                        case '6':
                        case '7':
                        case '8':
                        case '9': {
                            break;
                        }
                        default: {
                            throw new NoViableAltForCharException((char) LA(1), getFilename(),
                                    getLine(), getColumn());
                        }
                    }
                }
                mN(false);
            }
            else {
            }

        }
        if (_createToken && _token == null && _ttype != Token.SKIP) {
            _token = makeToken(_ttype);
            _token.setText(new String(text.getBuffer(), _begin, text.length() - _begin));
        }
        _returnToken = _token;
    }

    protected final void mN(boolean _createToken) throws RecognitionException, CharStreamException,
            TokenStreamException {
        int _ttype;
        Token _token = null;
        int _begin = text.length();
        _ttype = N;
        int _saveIndex;

        matchRange('0', '9');
        {
            _loop339: do {
                if (((LA(1) >= '0' && LA(1) <= '9'))) {
                    matchRange('0', '9');
                }
                else {
                    break _loop339;
                }

            } while (true);
        }
        if (_createToken && _token == null && _ttype != Token.SKIP) {
            _token = makeToken(_ttype);
            _token.setText(new String(text.getBuffer(), _begin, text.length() - _begin));
        }
        _returnToken = _token;
    }

    public final void mDOUBLE_QUOTE(boolean _createToken) throws RecognitionException,
            CharStreamException, TokenStreamException {
        int _ttype;
        Token _token = null;
        int _begin = text.length();
        _ttype = DOUBLE_QUOTE;
        int _saveIndex;

        match('"');
        if (inputState.guessing == 0) {
            _ttype = Token.SKIP;
        }
        if (_createToken && _token == null && _ttype != Token.SKIP) {
            _token = makeToken(_ttype);
            _token.setText(new String(text.getBuffer(), _begin, text.length() - _begin));
        }
        _returnToken = _token;
    }

    public final void mWS(boolean _createToken) throws RecognitionException, CharStreamException,
            TokenStreamException {
        int _ttype;
        Token _token = null;
        int _begin = text.length();
        _ttype = WS;
        int _saveIndex;

        {
            switch (LA(1)) {
                case ' ': {
                    match(' ');
                    break;
                }
                case '\t': {
                    match('\t');
                    break;
                }
                case '\n': {
                    match('\n');
                    if (inputState.guessing == 0) {
                        newline();
                    }
                    break;
                }
                default:
                    if ((LA(1) == '\r') && (LA(2) == '\n')) {
                        match('\r');
                        match('\n');
                        if (inputState.guessing == 0) {
                            newline();
                        }
                    }
                    else if ((LA(1) == '\r') && (true)) {
                        match('\r');
                        if (inputState.guessing == 0) {
                            newline();
                        }
                    }
                    else {
                        throw new NoViableAltForCharException((char) LA(1), getFilename(),
                                getLine(), getColumn());
                    }
            }
        }
        if (inputState.guessing == 0) {
            _ttype = Token.SKIP;
        }
        if (_createToken && _token == null && _ttype != Token.SKIP) {
            _token = makeToken(_ttype);
            _token.setText(new String(text.getBuffer(), _begin, text.length() - _begin));
        }
        _returnToken = _token;
    }

    public final void mML_COMMENT(boolean _createToken) throws RecognitionException,
            CharStreamException, TokenStreamException {
        int _ttype;
        Token _token = null;
        int _begin = text.length();
        _ttype = ML_COMMENT;
        int _saveIndex;

        match("/*");
        {
            _loop346: do {
                switch (LA(1)) {
                    case '\n': {
                        match('\n');
                        if (inputState.guessing == 0) {
                            newline();
                        }
                        break;
                    }
                    case '\u0003':
                    case '\u0004':
                    case '\u0005':
                    case '\u0006':
                    case '\u0007':
                    case '\u0008':
                    case '\t':
                    case '\u000b':
                    case '\u000c':
                    case '\u000e':
                    case '\u000f':
                    case '\u0010':
                    case '\u0011':
                    case '\u0012':
                    case '\u0013':
                    case '\u0014':
                    case '\u0015':
                    case '\u0016':
                    case '\u0017':
                    case '\u0018':
                    case '\u0019':
                    case '\u001a':
                    case '\u001b':
                    case '\u001c':
                    case '\u001d':
                    case '\u001e':
                    case '\u001f':
                    case ' ':
                    case '!':
                    case '"':
                    case '#':
                    case '$':
                    case '%':
                    case '&':
                    case '\'':
                    case '(':
                    case ')':
                    case '+':
                    case ',':
                    case '-':
                    case '.':
                    case '/':
                    case '0':
                    case '1':
                    case '2':
                    case '3':
                    case '4':
                    case '5':
                    case '6':
                    case '7':
                    case '8':
                    case '9':
                    case ':':
                    case ';':
                    case '<':
                    case '=':
                    case '>':
                    case '?':
                    case '@':
                    case 'A':
                    case 'B':
                    case 'C':
                    case 'D':
                    case 'E':
                    case 'F':
                    case 'G':
                    case 'H':
                    case 'I':
                    case 'J':
                    case 'K':
                    case 'L':
                    case 'M':
                    case 'N':
                    case 'O':
                    case 'P':
                    case 'Q':
                    case 'R':
                    case 'S':
                    case 'T':
                    case 'U':
                    case 'V':
                    case 'W':
                    case 'X':
                    case 'Y':
                    case 'Z':
                    case '[':
                    case '\\':
                    case ']':
                    case '^':
                    case '_':
                    case '`':
                    case 'a':
                    case 'b':
                    case 'c':
                    case 'd':
                    case 'e':
                    case 'f':
                    case 'g':
                    case 'h':
                    case 'i':
                    case 'j':
                    case 'k':
                    case 'l':
                    case 'm':
                    case 'n':
                    case 'o':
                    case 'p':
                    case 'q':
                    case 'r':
                    case 's':
                    case 't':
                    case 'u':
                    case 'v':
                    case 'w':
                    case 'x':
                    case 'y':
                    case 'z':
                    case '{':
                    case '|':
                    case '}':
                    case '~':
                    case '\u007f': {
                        {
                            match(_tokenSet_3);
                        }
                        break;
                    }
                    default:
                        if (((LA(1) == '*') && ((LA(2) >= '\u0003' && LA(2) <= '\u007f')))
                                && (LA(2) != '/')) {
                            match('*');
                        }
                        else if ((LA(1) == '\r') && (LA(2) == '\n')) {
                            match('\r');
                            match('\n');
                            if (inputState.guessing == 0) {
                                newline();
                            }
                        }
                        else if ((LA(1) == '\r') && ((LA(2) >= '\u0003' && LA(2) <= '\u007f'))) {
                            match('\r');
                            if (inputState.guessing == 0) {
                                newline();
                            }
                        }
                        else {
                            break _loop346;
                        }
                }
            } while (true);
        }
        match("*/");
        if (inputState.guessing == 0) {
            _ttype = Token.SKIP;
        }
        if (_createToken && _token == null && _ttype != Token.SKIP) {
            _token = makeToken(_ttype);
            _token.setText(new String(text.getBuffer(), _begin, text.length() - _begin));
        }
        _returnToken = _token;
    }

    private static final long[] mk_tokenSet_0() {
        long[] data = { 288063250384289792L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_0 = new BitSet(mk_tokenSet_0());

    private static final long[] mk_tokenSet_1() {
        long[] data = { -549755813896L, -1L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_1 = new BitSet(mk_tokenSet_1());

    private static final long[] mk_tokenSet_2() {
        long[] data = { 288019269919178752L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_2 = new BitSet(mk_tokenSet_2());

    private static final long[] mk_tokenSet_3() {
        long[] data = { -4398046520328L, -1L, 0L, 0L };
        return data;
    }

    public static final BitSet _tokenSet_3 = new BitSet(mk_tokenSet_3());

}
