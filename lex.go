package expr

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
)

//go:generate go get golang.org/x/tools/cmd/goyacc
//go:generate goyacc parser.y

var keywords = map[string]int{
	"+":     ADD,
	"-":     MINUS,
	"*":     MUL,
	"/":     DIV,
	">":     GT,
	"gt":    GT,
	">=":    GTE,
	"gte":   GTE,
	"=":     EQ,
	"==":    EQ,
	"aeq":   EQ,
	"<":     LT,
	"<=":    LTE,
	"lte":   LTE,
	"!=":    NEQ,
	"!==":   NEQ,
	"ne":    NEQ,
	"and":   AND,
	"AND":   AND,
	"or":    OR,
	"OR":    OR,
	"!":     NOT,
	"not":   NOT,
	"NOT":   NOT,
	"like":  LIKE,
	"LIKE":  LIKE,
	"true":  BOOL,
	"TRUE":  BOOL,
	"FALSE": BOOL,
	"false": BOOL,
	",":     COMMA,
	"(":     LP,
	")":     RP,
	"null":  NULL,
	"$":     DOLLAR,
	":":     CAST,
	"|":     PIPE,
	"case":  CASE,
	"when":  WHEN,
	"then":  THEN,
	"end":   END,
	"in":    IN,
	"else":  ELSE,
	"is":    IS,
}

type Lexer struct {
	s           string
	state       int
	pos         int
	bufStartPos int
	parseResult *AstNode
}

type token struct {
	t   int
	s   string
	pos int
}

const EOF = 0

const (
	StateNone = iota
	StateToken
	StateQuote
	StateDoubleQuote
	StateRawQuote
	StateNumber

	StateCStyleEscape
)

var terminators = map[string]int{
	"+":   ADD,
	"-":   MINUS,
	"*":   MUL,
	"/":   DIV,
	">":   GT,
	">=":  GTE,
	"=":   EQ,
	"==":  EQ,
	"<":   LT,
	"<=":  LTE,
	"!=":  NEQ,
	"!==": NEQ,
	"<>":  NEQ,
	"!":   NOT,
	",":   COMMA,
	"(":   LP,
	")":   RP,
	"$":   DOLLAR,
	":":   CAST,
	"|":   PIPE,
}
var maxKeywordsLength = 3

func init() {
	yyErrorVerbose = true
	for k, _ := range keywords {
		if len(k) > maxKeywordsLength {
			maxKeywordsLength = len(k)
		}
	}
}

func NewLexer(s string) *Lexer {
	return &Lexer{
		s: s,
	}
}

func (l *Lexer) lookAheadKeyword() string {
	for i := l.pos + 3; i > l.pos; i-- {
		if i > len(l.s) {
			continue
		}
		k := strings.ToLower(l.s[l.pos:i])
		if terminators[k] > 0 {
			return k
		}
	}
	return ""
}

func (l *Lexer) lookAhead(n int) string {
	if l.pos+n > len(l.s) {
		return ""
	} else {
		return l.s[l.pos : l.pos+n]
	}
}

func (l *Lexer) Lex(lval *yySymType) (out int) {
	t, err := l.lex()
	if err != nil {
		l.Error(err.Error())
		return 0
	}
	//fmt.Println(t)
	lval.offset = t.pos
	lval.text = t.s
	if t.t == 0 {
		return 0
	}
	if t.t != STR && t.t != INT && t.t != FLOAT {
		if s, ok := keywords[strings.ToLower(t.s)]; ok {
			return s
		} else {
			return ID
		}
	} else {
		return t.t
	}
}

func (l *Lexer) Error(s string) {
	errInfo := fmt.Sprintf("\n%s\n%s\n", l.s, strings.Repeat(" ", l.pos)+"^")
	panic(errInfo + s)
}

func (l *Lexer) lex() (token, error) {
	for {
		switch l.state {
		case StateNone:
			if s := l.lookAheadKeyword(); s != "" {
				l.pos += len(s)
				l.bufStartPos = l.pos
				return token{
					t:   keywords[s],
					s:   s,
					pos: l.pos - len(s),
				}, nil
			}
			switch s := l.lookAhead(1); {
			case s == "e" || s == "E":
				s2 := l.lookAhead(2)
				if s2 == "e'" || s2 == "E'" {
					l.state = StateCStyleEscape
					l.bufStartPos = l.pos
					l.pos += 2
					continue
				}
				fallthrough
			case s == "'":
				l.state = StateQuote
				l.bufStartPos = l.pos
			case s == "\"":
				l.state = StateDoubleQuote
				l.bufStartPos = l.pos
			case s == "`":
				l.state = StateRawQuote
				l.bufStartPos = l.pos
			case s == "":
				return token{
					t:   EOF,
					s:   "",
					pos: l.pos + 1,
				}, nil
			case s[0] <= '9' && s[0] >= '0':
				l.state = StateNumber
				l.bufStartPos = l.pos
			case s == "\n" || s == " " || s == "\r":
				l.pos++
				continue
			default:
				s0 := s[0]
				if s0 >= 'a' && s0 <= 'z' || s0 >= 'A' && s0 <= 'Z' || s0 == '_' {
					l.state = StateToken
					l.bufStartPos = l.pos
				} else {
					return token{}, fmt.Errorf("\n"+l.s+"\n"+strings.Repeat(" ", l.pos)+"^\n"+
						strings.Repeat(" ", l.pos)+"unexpected token %s", s)
				}
			}
			l.pos++
		case StateCStyleEscape:
			switch s := l.lookAhead(1); {
			case s == "":
				panic("WTF?")
			case s == "\\":
				l.pos += 2
			case s == "'":
				switch s2 := l.lookAhead(2); {
				case s2 == "''":
					l.pos += 2
				case s2 == "":
					l.pos++
					l.state = StateNone
					str, err := SQLStyleUnquote(l.s[l.bufStartPos:l.pos])
					if err != nil {
						panic(err)
					}
					return token{
						t:   STR,
						s:   str,
						pos: l.bufStartPos,
					}, err

				}
			default:
				l.pos++
			}
		case StateQuote:
			switch s := l.lookAhead(1); {
			case s == "'":
				switch s2 := l.lookAhead(2); {
				case s2 == "" || s2[1] != '\'':
					l.pos++
					l.state = StateNone
					str, err := SQLStyleUnquote(l.s[l.bufStartPos:l.pos])
					if err != nil {
						err = fmt.Errorf("%v: %s", err, l.s[l.bufStartPos:l.pos])
					}
					return token{
						t:   STR,
						s:   str,
						pos: l.bufStartPos,
					}, err
				case s2 == "''":
					l.pos += 2
				}
			case s == "":
				return token{}, errors.New("unexpected EOF, incomplete string")
			case s == "\\":
				l.pos += 2
			default:
				l.pos++
			}
		case StateDoubleQuote:
			s := l.lookAhead(1)
			switch {
			case s == "\"":
				switch s2 := l.lookAhead(2); {
				case s2 == "" || s2[1] != '"':
					l.pos++
					l.state = StateNone
					str, err := unquoteDouble(l.s[l.bufStartPos:l.pos])
					if err != nil {
						err = fmt.Errorf("%v: %s", err, l.s[l.bufStartPos:l.pos])
					}
					return token{
						t:   STR,
						s:   str,
						pos: l.bufStartPos,
					}, err
				case s2 == "\"\"":
					l.pos += 2
				}
			case s == "":
				return token{}, errors.New("unexpected EOF, incomplete string")
			case s == "\\":
				l.pos += 2
			default:
				l.pos++
			}
		case StateRawQuote:
			s := l.lookAhead(1)
			switch {
			case s == "`":
				switch s2 := l.lookAhead(2); {
				case s2 == "" || s2[1] != '`':
					l.pos++
					l.state = StateNone

					return token{
						t:   STR,
						s:   strings.ReplaceAll(l.s[l.bufStartPos+1:l.pos-1], "``", "`"),
						pos: l.bufStartPos,
					}, nil
				case s2 == "``":
					l.pos += 2
				}
			case s == "":
				return token{}, errors.New("unexpected EOF, incomplete string")
			default:
				l.pos++
			}
		case StateToken:
			if s := l.lookAheadKeyword(); s != "" {
				l.state = StateNone
				return token{
					t:   ID,
					s:   l.s[l.bufStartPos:l.pos],
					pos: l.bufStartPos,
				}, nil
			}
			s := l.lookAhead(1)
			if s == "" || s == "'" || s == "\"" || s == "`" || s == " " || s == "\r" || s == "\n" {
				l.state = StateNone
				return token{
					t:   ID,
					s:   l.s[l.bufStartPos:l.pos],
					pos: l.bufStartPos,
				}, nil
			}
			l.pos++
		case StateNumber:
			if s := l.lookAheadKeyword(); s != "" {
				l.state = StateNone
				numberText := l.s[l.bufStartPos:l.pos]
				if strings.Contains(numberText, ".") {
					return token{
						t:   FLOAT,
						s:   numberText,
						pos: l.bufStartPos,
					}, nil
				} else {
					return token{
						t:   INT,
						s:   numberText,
						pos: l.bufStartPos,
					}, nil
				}
			}
			s := l.lookAhead(1)
			if s == "" || !(s[0] <= '9' && s[0] >= '0' || s[0] == '.') {
				l.state = StateNone
				numberText := l.s[l.bufStartPos:l.pos]
				if strings.Contains(numberText, ".") {
					return token{
						t:   FLOAT,
						s:   numberText,
						pos: l.bufStartPos,
					}, nil
				} else {
					return token{
						t:   INT,
						s:   numberText,
						pos: l.bufStartPos,
					}, nil
				}
			}
			l.pos++
		}
	}
}

func unquote(s string) (string, error) {
	if s == `''` {
		return "", nil
	}
	return _unquote(strings.ReplaceAll(s, "''", "\\'"))
}

func unquoteDouble(s string) (string, error) {
	if s == `""` {
		return "", nil
	}
	return _unquote(strings.ReplaceAll(s, `""`, `"`))

}

var ErrSyntax = errors.New("invalid quote syntax")

func contains(s string, c byte) bool {
	return strings.IndexByte(s, c) != -1
}

// SQLStyleUnquote
// Usage:
// SQL Style single quote string
// 'raw string quoted by '' excepted\n' => raw string quoted by ' excepted\n
// PostgreSQL Style Escape string
// e'string which needs escape characters like:\t\"' => string which needs escape characters like:	"
//
// this Function is kind of fork from strconv.unquote
// check out https://cs.opensource.google/go/go/+/refs/tags/go1.17.1:src/strconv/quote.go;l=391
func SQLStyleUnquote(in string) (out string, err error) {
	inLength := len(in)
	if inLength < 2 {
		return "", ErrSyntax
	}

	firstCharFromIn := in[0]
	lastCharFromIn := in[inLength-1]

	// last char MUST be '
	if lastCharFromIn != '\'' {
		fmt.Printf("We've got a piece of BS")
		return "", ErrSyntax
	}

	switch firstCharFromIn {
	case '\'':
		// in this case, we only need to replace `''` => `'`
		fmt.Println(`SQL Style RAW String`)

		// empty string
		if inLength == 2 {
			return "", nil
		}

		// inLength >= 3
		unquotedString := in[1 : inLength-1]
		escapedString := strings.ReplaceAll(unquotedString, `''`, `'`)
		out = escapedString
		return out, nil

	case 'E', 'e':
		fmt.Println(`PGSQL Style Escape String`)

		// We've already checked length of input >= 2, [1] is safe.
		if in[1] != '\'' {
			goto ErrorInput
		}
		// handle `e''` here
		if inLength < 3 {
			goto ErrorInput
		}
		if inLength == 3 {
			return "", nil
		}
		// inLength >=4
		unquotedString := in[2 : inLength-1]
		fmt.Println(unquotedString)

		// strconv validate UTF-8 here

		in = in[2 : len(in)-1]
		var runeTmp [utf8.UTFMax]byte
		buf := make([]byte, 0, 3*len(in)/2)
		for len(in) > 0 {
			// Kind of MAGIC !
			c, multibyte, ss, err := UnquoteCharForPGSQLEString(in)
			if err != nil {
				return "", err
			}
			in = ss
			if c < utf8.RuneSelf || !multibyte {
				buf = append(buf, byte(c))
			} else {
				n := utf8.EncodeRune(runeTmp[:], c)
				buf = append(buf, runeTmp[:n]...)
			}
		}
		if len(in) != 0 {
			return "", ErrSyntax
		}
		return string(buf), nil

	default:
		fmt.Println(`WTF?`)
	}

ErrorInput:
	return "", ErrSyntax
}

// UnquoteCharForPGSQLEString permits \' and '' and disallows unescaped '.
func UnquoteCharForPGSQLEString(s string) (value rune, multibyte bool, tail string, err error) {
	// easy cases
	if len(s) == 0 {
		err = ErrSyntax
		return
	}
	switch c := s[0]; {
	case c == '\'':
		if len(s) == 1 {
			return
		}
		// handle ''
		if s[1] == '\'' {
			value = '\''
			tail = s[2:]
		} else {
			err = ErrSyntax
		}
		return
	case c >= utf8.RuneSelf:
		r, size := utf8.DecodeRuneInString(s)
		return r, true, s[size:], nil
	case c != '\\':
		return rune(s[0]), false, s[1:], nil
	}

	if len(s) <= 1 {
		err = ErrSyntax
		return
	}

	c := s[1]
	s = s[2:]

	switch c {
	case 'a':
		value = '\a'
	case 'b':
		value = '\b'
	case 'f':
		value = '\f'
	case 'n':
		value = '\n'
	case 'r':
		value = '\r'
	case 't':
		value = '\t'
	case 'v':
		value = '\v'
	case 'x', 'u', 'U':
		n := 0
		switch c {
		case 'x':
			n = 2
		case 'u':
			n = 4
		case 'U':
			n = 8
		}
		var v rune
		if len(s) < n {
			err = ErrSyntax
			return
		}
		for j := 0; j < n; j++ {
			x, ok := unhex(s[j])
			if !ok {
				err = ErrSyntax
				return
			}
			v = v<<4 | x
		}
		s = s[n:]
		if c == 'x' {
			// single-byte string, possibly not UTF-8
			value = v
			break
		}
		if v > utf8.MaxRune {
			err = ErrSyntax
			return
		}
		value = v
		multibyte = true
	case '0', '1', '2', '3', '4', '5', '6', '7':
		v := rune(c) - '0'
		if len(s) < 2 {
			err = ErrSyntax
			return
		}
		for j := 0; j < 2; j++ { // one digit already; two more
			x := rune(s[j]) - '0'
			if x < 0 || x > 7 {
				err = ErrSyntax
				return
			}
			v = (v << 3) | x
		}
		s = s[2:]
		if v > 255 {
			err = ErrSyntax
			return
		}
		value = v
	case '\\':
		value = '\\'
	case '\'', '"':
		// we've already handled
		//if c != quote {
		//	err = ErrSyntax
		//	return
		//}
		value = rune(c)
	default:
		err = ErrSyntax
		return
	}
	tail = s
	return
}

func unhex(b byte) (v rune, ok bool) {
	c := rune(b)
	switch {
	case '0' <= c && c <= '9':
		return c - '0', true
	case 'a' <= c && c <= 'f':
		return c - 'a' + 10, true
	case 'A' <= c && c <= 'F':
		return c - 'A' + 10, true
	}
	return
}

func _unquote(s string) (string, error) {
	n := len(s)
	if n < 2 {
		return "", ErrSyntax
	}
	quote := s[0]
	if quote != s[n-1] {
		return "", ErrSyntax
	}
	s = s[1 : n-1]

	if quote == '`' {
		if contains(s, '`') {
			return "", ErrSyntax
		}
		if contains(s, '\r') {
			// -1 because we know there is at least one \r to remove.
			buf := make([]byte, 0, len(s)-1)
			for i := 0; i < len(s); i++ {
				if s[i] != '\r' {
					buf = append(buf, s[i])
				}
			}
			return string(buf), nil
		}
		return s, nil
	}
	if quote != '"' && quote != '\'' {
		return "", ErrSyntax
	}
	if contains(s, '\n') {
		return "", ErrSyntax
	}

	// Is it trivial? Avoid allocation.
	if !contains(s, '\\') && !contains(s, quote) {
		switch quote {
		case '"':
			if utf8.ValidString(s) {
				return s, nil
			}
		case '\'':
			r, size := utf8.DecodeRuneInString(s)
			if size == len(s) && (r != utf8.RuneError || size != 1) {
				return s, nil
			}
		}
	}

	var runeTmp [utf8.UTFMax]byte
	buf := make([]byte, 0, 3*len(s)/2) // Try to avoid more allocations.
	for len(s) > 0 {
		c, multibyte, ss, err := strconv.UnquoteChar(s, quote)
		if err != nil {
			return "", err
		}
		s = ss
		if c < utf8.RuneSelf || !multibyte {
			buf = append(buf, byte(c))
		} else {
			n := utf8.EncodeRune(runeTmp[:], c)
			buf = append(buf, runeTmp[:n]...)
		}
	}
	return string(buf), nil
}
