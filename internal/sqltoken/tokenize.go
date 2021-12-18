package sqltoken

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

//go:generate enumer -type=TokenType -json

type TokenType int

const (
	Comment TokenType = iota
	Whitespace
	QuestionMark // used in MySQL substitution
	AtSign       // used in sqlserver substitution
	DollarNumber // used in PostgreSQL substitution
	ColonWord    // used in sqlx substitution
	Literal      // strings
	Identifier   // used in SQL Server for many things
	AtWord       // used in SQL Server, subset of Identifier
	Number
	Semicolon
	Punctuation
	Word
	Other // control characters and other non-printables
)

func combineOkay(t TokenType) bool {
	// nolint:exhaustive
	switch t {
	case Number, QuestionMark, DollarNumber, ColonWord:
		return false
	}
	return true
}

type Token struct {
	Type TokenType
	Text string
}

// Config specifies the behavior of Tokenize as relates to behavior
// that differs between SQL implementations
type Config struct {
	// Tokenize ? as type Question (used by MySQL)
	NoticeQuestionMark bool

	// Tokenize $7 as type DollarNumber (PostgreSQL)
	NoticeDollarNumber bool

	// Tokenize :word as type ColonWord (sqlx, Oracle)
	NoticeColonWord bool

	// Tokenize :word with unicode as ColonWord (sqlx)
	ColonWordIncludesUnicode bool

	// Tokenize # as type comment (MySQL)
	NoticeHashComment bool

	// $q$ stuff $q$ and $$stuff$$ quoting (PostgreSQL)
	NoticeDollarQuotes bool

	// NoticeHexValues 0xa0 x'af' X'AF' (MySQL)
	NoticeHexNumbers bool

	// NoticeBinaryValues 0x01 b'01' B'01' (MySQL)
	NoticeBinaryNumbers bool

	// NoticeUAmpPrefix U& utf prefix U&"\0441\043B\043E\043D" (PostgreSQL)
	NoticeUAmpPrefix bool

	// NoticeCharsetLiteral _latin1'string' n'string' (MySQL)
	NoticeCharsetLiteral bool

	// NoticeNotionalStrings [nN]'...''...' (Oracle, SQL Server)
	NoticeNotionalStrings bool

	// NoticeDelimitedStrings [nN]?[qQ]'DELIM .... DELIM' (Oracle)
	NoticeDeliminatedStrings bool

	// NoticeTypedNumbers nn.nnEnn[fFdD] (Oracle)
	NoticeTypedNumbers bool

	// NoticeMoneyConstants $10 $10.32 (SQL Server)
	NoticeMoneyConstants bool

	// NoticeAtWord @foo (SQL Server)
	NoticeAtWord bool

	// NoticeAtIdentifiers _baz @fo$o @@b#ar #foo ##b@ar(SQL Server)
	NoticeIdentifiers bool
}

type Tokens []Token

type TokensList []Tokens

// OracleConfig returns a parsing configuration that is appropriate
// for parsing Oracle's SQL
func OracleConfig() Config {
	// https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Literals.html
	return Config{
		NoticeNotionalStrings:    true,
		NoticeDeliminatedStrings: true,
		NoticeTypedNumbers:       true,
		NoticeColonWord:          true,
	}
}

// SQLServerConfig returns a parsing configuration that is appropriate
// for parsing SQLServer's SQL
func SQLServerConfig() Config {
	return Config{
		NoticeNotionalStrings: true,
		NoticeHexNumbers:      true,
		NoticeMoneyConstants:  true,
		NoticeAtWord:          true,
		NoticeIdentifiers:     true,
	}
}

// MySQL returns a parsing configuration that is appropriate
// for parsing MySQL, MariaDB, and SingleStore SQL.
func MySQLConfig() Config {
	return Config{
		NoticeQuestionMark:   true,
		NoticeHashComment:    true,
		NoticeHexNumbers:     true,
		NoticeBinaryNumbers:  true,
		NoticeCharsetLiteral: true,
	}
}

// PostgreSQL returns a parsing configuration that is appropriate
// for parsing PostgreSQL and CockroachDB SQL.
func PostgreSQLConfig() Config {
	return Config{
		NoticeDollarNumber: true,
		NoticeDollarQuotes: true,
		NoticeUAmpPrefix:   true,
	}
}

// TokenizeMySQL breaks up MySQL / MariaDB / SingleStore SQL strings into
// Token objects.
func TokenizeMySQL(s string) Tokens {
	return Tokenize(s, MySQLConfig())
}

// TokenizePostgreSQL breaks up PostgreSQL / CockroachDB SQL strings into
// Token objects.
func TokenizePostgreSQL(s string) Tokens {
	return Tokenize(s, PostgreSQLConfig())
}

const debug = false

// Tokenize breaks up SQL strings into Token objects.  No attempt is made
// to break successive punctuation.
func Tokenize(s string, config Config) Tokens {
	if len(s) == 0 {
		return []Token{}
	}
	tokens := make([]Token, 0, len(s)/5)
	tokenStart := 0
	var i int
	var firstDollarEnd int
	var runeDelim rune
	var charDelim byte

	// Why is this written with Goto you might ask?  It's written
	// with goto because RE2 can't handle complex regex and PCRE
	// has external dependencies and thus isn't friendly for libraries.
	// So, it could have had a switch with a state variable, but that's
	// just a way to do goto that's lower performance.  Might as
	// well do goto the natural way.

	token := func(t TokenType) {
		if debug {
			fmt.Printf("> %s: {%s}\n", t, s[tokenStart:i])
		}
		if i-tokenStart == 0 {
			return
		}
		if len(tokens) > 0 && tokens[len(tokens)-1].Type == t && combineOkay(t) {
			tokens[len(tokens)-1].Text = s[tokenStart-len(tokens[len(tokens)-1].Text) : i]
		} else {
			tokens = append(tokens, Token{
				Type: t,
				Text: s[tokenStart:i],
			})
		}
		tokenStart = i
	}

BaseState:
	for i < len(s) {
		c := s[i]
		i++
		switch c {
		case '/':
			if i < len(s) && s[i] == '*' {
				goto CStyleComment
			}
			token(Punctuation)
		case '\'':
			goto SingleQuoteString
		case '"':
			goto DoubleQuoteString
		case '-':
			if i < len(s) && s[i] == '-' {
				goto SkipToEOL
			}
			token(Punctuation)
		case '#':
			if config.NoticeHashComment {
				goto SkipToEOL
			}
			if config.NoticeIdentifiers {
				goto Identifier
			}
			token(Punctuation)
		case '@':
			if config.NoticeAtWord {
				goto AtWordStart
			} else if config.NoticeIdentifiers {
				goto Identifier
			} else {
				token(Punctuation)
			}
		case ';':
			token(Semicolon)
		case '?':
			if config.NoticeQuestionMark {
				token(QuestionMark)
			} else {
				token(Punctuation)
			}
		case ' ', '\n', '\r', '\t', '\b', '\v', '\f':
			goto Whitespace
		case '.':
			goto PossibleNumber
		case ':':
			if config.NoticeColonWord {
				goto ColonWordStart
			}
			token(Punctuation)
		case '~', '`', '!', '%', '^', '&', '*', '(', ')', '+', '=', '{', '}', '[', ']',
			'|', '\\', '<', '>', ',':
			token(Punctuation)
		case '$':
			// $1
			// $seq$ stuff $seq$
			// $$stuff$$
			if config.NoticeDollarQuotes || config.NoticeDollarNumber {
				goto Dollar
			}
			token(Punctuation)
		case 'U':
			// U&'d\0061t\+000061'
			if config.NoticeUAmpPrefix && i+1 < len(s) && s[i] == '&' && s[i+1] == '\'' {
				i += 2
				goto SingleQuoteString
			}
			goto Word
		case 'x', 'X':
			// X'1f' x'1f'
			if config.NoticeHexNumbers && i < len(s) && s[i] == '\'' {
				i++
				goto QuotedHexNumber
			}
			goto Word
		case 'b', 'B':
			if config.NoticeBinaryNumbers && i < len(s) && s[i] == '\'' {
				i++
				goto QuotedBinaryNumber
			}
			goto Word
		case 'n', 'N':
			if config.NoticeNotionalStrings && i < len(s)-1 {
				switch s[i] {
				case 'q', 'Q':
					if config.NoticeDeliminatedStrings && i < len(s)-2 && s[i+1] == '\'' {
						i += 2
						goto DeliminatedString
					}
				case '\'':
					i++
					goto SingleQuoteString
				}
			}
			goto Word
		case 'q', 'Q':
			if config.NoticeDeliminatedStrings && i < len(s) && s[i] == '\'' {
				i++
				goto DeliminatedString
			}
			goto Word
		case 'a' /*b*/, 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
			/*n*/ 'o', 'p' /*q*/, 'r', 's', 't', 'u', 'v', 'w' /*x*/, 'y', 'z',
			'A' /*B*/, 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
			/*N*/ 'O', 'P' /*Q*/, 'R', 'S', 'T' /*U*/, 'V', 'W' /*X*/, 'Y', 'Z',
			'_':
			// This covers the entire alphabet except specific letters that have
			// been handled above.  This case is actually just a performance
			// hack: if there were a letter missing it would be caught below
			// by unicode.IsLetter()
			goto Word
		case '0':
			if config.NoticeHexNumbers && i < len(s) && s[i] == 'x' {
				i++
				goto HexNumber
			}
			if config.NoticeBinaryNumbers && i < len(s) && s[i] == 'b' {
				i++
				goto BinaryNumber
			}
			goto Number
		case /*0*/ '1', '2', '3', '4', '5', '6', '7', '8', '9':
			goto Number
		default:
			r, w := utf8.DecodeRuneInString(s[i-1:])
			switch {
			case r == '⎖':
				// "⎖" is the unicode decimal separator -- an alternative to "."
				i += w - 1
				goto NumberNoDot
			case unicode.IsDigit(r):
				i += w - 1
				goto Number
			case unicode.IsPunct(r) || unicode.IsSymbol(r) || unicode.IsMark(r):
				i += w - 1
				token(Punctuation)
			case unicode.IsLetter(r):
				i += w - 1
				goto Word
			case unicode.IsControl(r) || unicode.IsSpace(r):
				i += w - 1
				goto Whitespace
			default:
				i += w - 1
				token(Other)
			}
		}
	}
	goto Done

CStyleComment:
	for i < len(s) {
		c := s[i]
		i++
		switch c {
		case '*':
			if i < len(s) && s[i] == '/' {
				i++
				token(Comment)
				goto BaseState
			}
		}
	}
	token(Comment)
	goto Done

SingleQuoteString:
	for i < len(s) {
		c := s[i]
		i++
		switch c {
		case '\'':
			token(Literal)
			goto BaseState
		case '\\':
			if i < len(s) {
				i++
			} else {
				token(Literal)
				goto Done
			}
		}
	}
	token(Literal)
	goto Done

DoubleQuoteString:
	for i < len(s) {
		c := s[i]
		i++
		switch c {
		case '"':
			token(Literal)
			goto BaseState
		case '\\':
			if i < len(s) {
				i++
			} else {
				token(Literal)
				goto Done
			}
		}
	}
	token(Literal)
	goto Done

SkipToEOL:
	for i < len(s) {
		c := s[i]
		i++
		switch c {
		case '\n':
			token(Comment)
			goto BaseState
		}
	}
	token(Comment)
	goto Done

Word:
	for i < len(s) {
		c := s[i]
		switch c {
		case 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
			'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
			'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
			'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
			'_',
			'0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			// This covers the entire alphabet and numbers.
			// This case is actually just a performance
			// hack: if there were a letter missing it would be caught below
			// by unicode.IsLetter()
			i++
			continue
		case '#', '@', '$':
			if config.NoticeIdentifiers {
				goto Identifier
			}
			token(Word)
			goto BaseState
		case '\n', '\r', '\t', '\b', '\v', '\f', ' ',
			'!', '"' /*#*/ /*$*/, '%', '&' /*'*/, '(', ')', '*', '+', '-', '.', '/',
			':', ';', '<', '=', '>', '?', /*@*/
			'[', '\\', ']', '^' /*_*/, '`',
			'{', '|', '}', '~':
			// minor optimization to avoid DecodeRuneInString
			token(Word)
			goto BaseState
		case '\'':
			if config.NoticeCharsetLiteral {
				switch s[tokenStart] {
				case 'n', 'N':
					if i-tokenStart == 1 {
						i++
						goto SingleQuoteString
					}
				case '_':
					i++
					goto SingleQuoteString
				}
			}
		}
		r, w := utf8.DecodeRuneInString(s[i:])
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			i += w
			continue
		}
		token(Word)
		goto BaseState
	}
	token(Word)
	goto Done

ColonWordStart:
	if i < len(s) {
		c := s[i]
		switch c {
		case ':':
			// ::word is :: word, not : :word
			i++
			for i < len(s) && s[i] == ':' {
				i++
			}
			token(Punctuation)
			goto BaseState
		case 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
			'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
			'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
			'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z':
			i++
			goto ColonWord
		case '\n', '\r', '\t', '\b', '\v', '\f', ' ',
			'!', '"', '#', '$', '%', '&', '\'', '(', ')', '*', '+', '-', '.', '/',
			/*:*/ ';', '<', '=', '>', '?', '@',
			'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
			'[', '\\', ']', '^', '_', '`',
			'{', '|', '}', '~':
			// minor optimization to avoid DecodeRuneInString
			token(Punctuation)
			goto BaseState
		default:
			if config.ColonWordIncludesUnicode {
				r, w := utf8.DecodeRuneInString(s[i:])
				if unicode.IsLetter(r) {
					i += w
					goto ColonWord
				}
			}
			token(Punctuation)
			goto BaseState
		}
	}
	token(Punctuation)
	goto Done

ColonWord:
	for i < len(s) {
		c := s[i]
		switch c {
		case 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
			'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
			'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
			'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
			'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
			'_':
			i++
			continue
		case '.':
			if config.ColonWordIncludesUnicode {
				i++
				continue
			}
		case '\n', '\r', '\t', '\b', '\v', '\f', ' ',
			'!', '"', '#', '$', '%', '&', '\'', '(', ')', '*', '+', '-' /*.*/, '/',
			':', ';', '<', '=', '>', '?', '@',
			'[', '\\', ']', '^' /*_*/, '`',
			'{', '|', '}', '~':
			// minor optimization to avoid DecodeRuneInString
			token(ColonWord)
			goto BaseState
		default:
			if config.ColonWordIncludesUnicode {
				r, w := utf8.DecodeRuneInString(s[i:])
				if unicode.IsLetter(r) || unicode.IsDigit(r) {
					i += w
					continue
				}
			}
			token(ColonWord)
			goto BaseState
		}
	}
	token(ColonWord)
	goto Done

Identifier:
	for i < len(s) {
		c := s[i]
		switch c {
		case 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
			'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
			'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
			'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
			'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
			'#', '@', '$', '_':
			i++
			continue
		default:
			if i-tokenStart == 1 {
				// # @ $ or _
				token(Punctuation)
			} else {
				token(Identifier)
			}
			goto BaseState
		}
	}
	if i-tokenStart == 1 {
		// # @ $ or _
		token(Punctuation)
	} else {
		token(Identifier)
	}
	goto Done

AtWordStart:
	if i < len(s) {
		c := s[i]
		switch c {
		case 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
			'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
			'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
			'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z':
			i++
			goto AtWord
		default:
			if config.NoticeIdentifiers {
				goto Identifier
			}
			// @
			token(Punctuation)
			goto BaseState
		}
	}
	// @
	token(Punctuation)
	goto Done

AtWord:
	for i < len(s) {
		c := s[i]
		switch c {
		case 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
			'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
			'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
			'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
			'0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			i++
			continue
		case '#', '@', '$', '_':
			if config.NoticeIdentifiers {
				goto Identifier
			}
			token(AtWord)
			goto BaseState
		default:
			token(AtWord)
			goto BaseState
		}
	}
	token(AtWord)
	goto Done

PossibleNumber:
	if i < len(s) {
		c := s[i]
		switch c {
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			i++
			goto NumberNoDot
		case 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
			'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
			'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
			'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
			'\n', '\r', '\t', '\b', '\v', '\f', ' ',
			'!', '"', '#', '$', '%', '&', '\'', '(', ')', '*', '+', '-', '.', '/',
			':', ';', '<', '=', '>', '?', '@',
			'[', '\\', ']', '^', '_', '`',
			'{', '|', '}', '~':
			// minor optimization to avoid DecodeRuneInString
			token(Punctuation)
			goto BaseState
		default:
			r, w := utf8.DecodeRuneInString(s[i:])
			i += w
			if unicode.IsDigit(r) {
				goto NumberNoDot
			}
			// .
			token(Punctuation)
			goto BaseState
		}
	}
	// .
	token(Punctuation)
	goto Done

Number:
	for i < len(s) {
		c := s[i]
		i++
		switch c {
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			// okay
		case '.':
			goto NumberNoDot
		case 'e', 'E':
			if i < len(s) {
				switch s[i] {
				case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
					i++
					goto Exponent
				case 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
					'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
					'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
					'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
					'\n', '\r', '\t', '\b', '\v', '\f', ' ',
					'!', '"', '#', '$', '%', '&', '\'', '(', ')', '*', '+', '-', '.', '/',
					':', ';', '<', '=', '>', '?', '@',
					'[', '\\', ']', '^', '_', '`',
					'{', '|', '}', '~':
					// minor optimization to avoid DecodeRuneInString
				default:
					r, w := utf8.DecodeRuneInString(s[i:])
					if unicode.IsDigit(r) {
						i += w
						goto Exponent
					}
				}
			}
			i--
			token(Number)
			goto Word
		case 'd', 'D', 'f', 'F':
			if !config.NoticeTypedNumbers {
				i--
			}
			token(Number)
			goto BaseState
		case 'a', 'b', 'c' /*d*/ /*e*/ /*f*/, 'g', 'h', 'i', 'j', 'k', 'l', 'm',
			'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
			'A', 'B', 'C' /*D*/ /*E*/ /*F*/, 'G', 'H', 'I', 'J', 'K', 'L', 'M',
			'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
			'\n', '\r', '\t', '\b', '\v', '\f', ' ',
			'!', '"', '#', '$', '%', '&', '\'', '(', ')', '*', '+', '-' /*.*/, '/',
			':', ';', '<', '=', '>', '?', '@',
			'[', '\\', ']', '^', '_', '`',
			'{', '|', '}', '~':
			// minor optimization to avoid DecodeRuneInString
			i--
			token(Number)
			goto BaseState
		default:
			r, w := utf8.DecodeRuneInString(s[i-1:])
			if r == '⎖' {
				i += w - 1
				goto NumberNoDot
			}
			if !unicode.IsDigit(r) {
				i--
				token(Number)
				goto BaseState
			}
			i += w - 1
		}
	}
	token(Number)
	goto Done

NumberNoDot:
	for i < len(s) {
		c := s[i]
		i++
		switch c {
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			// okay
		case 'e', 'E':
			if i < len(s) {
				switch s[i] {
				case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
					i++
					goto Exponent
				}
			}
			i--
			token(Number)
			goto Word
		case 'd', 'D', 'f', 'F':
			if !config.NoticeTypedNumbers {
				i--
			}
			token(Number)
			goto BaseState
		case 'a', 'b', 'c' /*d*/ /*e*/ /*f*/, 'g', 'h', 'i', 'j', 'k', 'l', 'm',
			'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
			'A', 'B', 'C' /*D*/ /*E*/ /*F*/, 'G', 'H', 'I', 'J', 'K', 'L', 'M',
			'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
			'\n', '\r', '\t', '\b', '\v', '\f', ' ',
			'!', '"', '#', '$', '%', '&', '\'', '(', ')', '*', '+', '-', '.', '/',
			':', ';', '<', '=', '>', '?', '@',
			'[', '\\', ']', '^', '_', '`',
			'{', '|', '}', '~':
			// minor optimization to avoid DecodeRuneInString
			i--
			token(Number)
			goto BaseState
		default:
			r, w := utf8.DecodeRuneInString(s[i-1:])
			if !unicode.IsDigit(r) {
				i--
				token(Number)
				goto BaseState
			}
			i += w - 1
		}
	}
	token(Number)
	goto Done

Exponent:
	if i < len(s) {
		c := s[i]
		i++
		switch c {
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			goto ExponentConfirmed
		case 'd', 'D', 'f', 'F':
			if !config.NoticeTypedNumbers {
				i--
			}
			token(Number)
			goto BaseState
		case 'a', 'b', 'c' /*d*/, 'e' /*f*/, 'g', 'h', 'i', 'j', 'k', 'l', 'm',
			'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
			'A', 'B', 'C' /*D*/, 'E' /*F*/, 'G', 'H', 'I', 'J', 'K', 'L', 'M',
			'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
			'\n', '\r', '\t', '\b', '\v', '\f', ' ',
			'!', '"', '#', '$', '%', '&', '\'', '(', ')', '*', '+', '-', '.', '/',
			':', ';', '<', '=', '>', '?', '@',
			'[', '\\', ']', '^', '_', '`',
			'{', '|', '}', '~':
			// minor optimization to avoid DecodeRuneInString
			i--
			token(Number)
			goto BaseState
		default:
			r, w := utf8.DecodeRuneInString(s[i-1:])
			if !unicode.IsDigit(r) {
				i--
				token(Number)
				goto BaseState
			}
			i += w - 1
			goto ExponentConfirmed
		}
	}
	token(Number)
	goto BaseState

ExponentConfirmed:
	for i < len(s) {
		c := s[i]
		i++
		switch c {
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			// okay
		case 'd', 'D', 'f', 'F':
			if !config.NoticeTypedNumbers {
				i--
			}
			token(Number)
			goto BaseState
		case 'a', 'b', 'c' /*d*/, 'e' /*f*/, 'g', 'h', 'i', 'j', 'k', 'l', 'm',
			'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
			'A', 'B', 'C' /*D*/, 'E' /*F*/, 'G', 'H', 'I', 'J', 'K', 'L', 'M',
			'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
			'\n', '\r', '\t', '\b', '\v', '\f', ' ',
			'!', '"', '#', '$', '%', '&', '\'', '(', ')', '*', '+', '-', '.', '/',
			':', ';', '<', '=', '>', '?', '@',
			'[', '\\', ']', '^', '_', '`',
			'{', '|', '}', '~':
			// minor optimization to avoid DecodeRuneInString
			i--
			token(Number)
			goto BaseState
		default:
			r, w := utf8.DecodeRuneInString(s[i-1:])
			if !unicode.IsDigit(r) {
				i--
				token(Number)
				goto BaseState
			}
			i += w - 1
		}
	}
	token(Number)
	goto Done

HexNumber:
	for i < len(s) {
		c := s[i]
		i++
		switch c {
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
			'a', 'b', 'c', 'd', 'e', 'f',
			'A', 'B', 'C', 'D', 'E', 'F':
			// okay
		default:
			i--
			token(Number)
			goto BaseState
		}
	}
	token(Number)
	goto Done

BinaryNumber:
	for i < len(s) {
		c := s[i]
		i++
		switch c {
		case '0', '1':
			// okay
		default:
			i--
			token(Number)
			goto BaseState
		}
	}
	token(Number)
	goto Done

Whitespace:
	for i < len(s) {
		c := s[i]
		i++
		switch c {
		case ' ', '\n', '\r', '\t', '\b', '\v', '\f':
			// whitespace!
		case 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
			'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
			'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
			'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
			'!', '"', '#', '$', '%', '&', '\'', '(', ')', '*', '+', '-', '.', '/',
			':', ';', '<', '=', '>', '?', '@',
			'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
			'[', '\\', ']', '^', '_', '`',
			'{', '|', '}', '~':
			// minor optimization to avoid DecodeRuneInString
			i--
			token(Whitespace)
			goto BaseState
		default:
			r, w := utf8.DecodeRuneInString(s[i-1:])
			if !unicode.IsSpace(r) && !unicode.IsControl(r) {
				i--
				token(Whitespace)
				goto BaseState
			}
			i += w - 1
		}
	}
	token(Whitespace)
	goto Done

QuotedHexNumber:
	for i < len(s) {
		c := s[i]
		i++
		switch c {
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
			'a', 'b', 'c', 'd', 'e', 'f',
			'A', 'B', 'C', 'D', 'E', 'F':
			// okay
		case '\'':
			token(Number)
			goto BaseState
		default:
			i--
			token(Number)
			goto BaseState
		}
	}
	token(Number)
	goto Done

QuotedBinaryNumber:
	for i < len(s) {
		c := s[i]
		i++
		switch c {
		case '0', '1':
			// okay
		case '\'':
			token(Number)
			goto BaseState
		default:
			i--
			token(Number)
			goto BaseState
		}
	}
	token(Number)
	goto Done

DeliminatedString:
	// We arrive here with s[i] being on the opening delimiter
	// 'Foo''Bar'
	// n'Foo'
	// q'XlsXldsaX'
	// Q'(ls)(Xldsa)'
	// Nq'(ls)(Xldsa)'
	if i < len(s) {
		c := s[i]
		i++
		switch c {
		case '(':
			charDelim = ')'
			goto DeliminatedStringCharacter
		case '<':
			charDelim = '>'
			goto DeliminatedStringCharacter
		case '[':
			charDelim = ']'
			goto DeliminatedStringCharacter
		case '{':
			charDelim = '}'
			goto DeliminatedStringCharacter
		// [{<(
		case ')', '>', '}', ']',
			'\n', '\r', '\t', '\b', '\v', '\f', ' ':
			// not a valid delimiter
			i -= 2
			token(Word)
			i++
			goto SingleQuoteString
		case 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
			'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
			'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
			'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
			'!', '"', '#', '$', '%', '&', '\'' /*(*/ /*)*/, '*', '+', '-', '.', '/',
			':', ';' /*<*/, '=' /*>*/, '?', '@',
			/*[*/ '\\' /*]*/, '^', '_', '`',
			/*{*/ '|' /*}*/, '~':
			// minor optimzation to avoid DecodeRuneInString
			charDelim = c
			goto DeliminatedStringCharacter
		default:
			r, w := utf8.DecodeRuneInString(s[i-1:])
			if w == 1 {
				charDelim = s[i-1]
				goto DeliminatedStringCharacter
			}
			i += w - 1
			runeDelim = r
			goto DeliminatedStringRune
		}
	}
	token(Word)
	goto Done

DeliminatedStringCharacter:
	for i < len(s) {
		c := s[i]
		i++
		if c == charDelim && i < len(s) && s[i] == '\'' {
			i++
			token(Literal)
			goto BaseState
		}
	}
	token(Literal)
	goto Done

DeliminatedStringRune:
	for i < len(s) {
		r, w := utf8.DecodeRuneInString(s[i:])
		i += w
		if r == runeDelim {
			token(Literal)
			goto BaseState
		}
	}
	token(Literal)
	goto Done

Dollar:
	// $1
	// $seq$ stuff $seq$
	// $$stuff$$
	firstDollarEnd = i
	if i < len(s) {
		c := s[i]
		if config.NoticeDollarQuotes {
			if c == '$' {
				e := strings.Index(s[i+1:], "$$")
				if e == -1 {
					i = firstDollarEnd
					// $
					token(Punctuation)
					goto BaseState
				}
				i += 3 + e
				token(Literal)
				goto BaseState
			}
			r, w := utf8.DecodeRuneInString(s[i:])
			if unicode.IsLetter(r) {
				i += w
				for i < len(s) {
					// nolint:govet
					c := s[i]
					r, w := utf8.DecodeRuneInString(s[i:])
					i++
					if c == '$' {
						endToken := s[tokenStart:i]
						e := strings.Index(s[i:], endToken)
						if e == -1 {
							i = firstDollarEnd
							// $
							token(Punctuation)
							goto BaseState
						}
						i += e + len(endToken)
						token(Literal)
						goto BaseState
					} else if unicode.IsLetter(r) {
						i += w - 1
						continue
					} else {
						i = firstDollarEnd
						// $
						token(Punctuation)
						goto BaseState
					}
				}
			}
		}
		if config.NoticeDollarNumber {
			switch c {
			case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
				i++
				for i < len(s) {
					c := s[i]
					i++
					switch c {
					case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
						continue
					}
					i--
					break
				}
				token(DollarNumber)
				goto BaseState
			}
		}
		// $
		token(Punctuation)
		goto BaseState
	}
	// $
	token(Punctuation)
	goto Done

Done:
	return tokens
}

func (ts Tokens) String() string {
	if len(ts) == 0 {
		return ""
	}
	strs := make([]string, len(ts))
	for i, t := range ts {
		strs[i] = t.Text
	}
	return strings.Join(strs, "")
}

// Strip removes leading/trailing whitespace and semicolors
// and strips all internal comments.  Internal whitespace
// is changed to a single space.
func (ts Tokens) Strip() Tokens {
	i := 0
	for i < len(ts) {
		// nolint:exhaustive
		switch ts[i].Type {
		case Comment, Whitespace, Semicolon:
			i++
			continue
		}
		break
	}
	c := make(Tokens, 0, len(ts))
	var lastReal int
	for i < len(ts) {
		// nolint:exhaustive
		switch ts[i].Type {
		case Comment:
			continue
		case Whitespace:
			c = append(c, Token{
				Type: Whitespace,
				Text: " ",
			})
		case Semicolon:
			c = append(c, ts[i])
		default:
			c = append(c, ts[i])
			lastReal = len(c)
		}
		i++
	}
	c = c[:lastReal]
	return c
}

// CmdSplit breaks up the token array into multiple token arrays,
// one per command (splitting on ";")
func (ts Tokens) CmdSplit() TokensList {
	var r TokensList
	start := 0
	for i, t := range ts {
		if t.Type == Semicolon {
			r = append(r, Tokens(ts[start:i]).Strip())
			start = i + 1
		}
	}
	if start < len(ts) {
		r = append(r, Tokens(ts[start:]).Strip())
	}
	return r
}

func (tl TokensList) Strings() []string {
	r := make([]string, 0, len(tl))
	for _, ts := range tl {
		s := ts.String()
		if s != "" {
			r = append(r, s)
		}
	}
	return r
}
