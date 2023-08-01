package sqlgen

import (
	"errors"
	"regexp"
	"strings"

	"github.com/rs/zerolog/log"
)

type step int

const (
	stepType step = iota
	stepCreateTable
	stepIfNoExists
	stepSchemaTableName
	stepAs
	stepSelectStatement
	stepColumnDefsOpenBracket
	stepColumDefComma
	stepColumnDefTableConstraint
	stepColumnDefsCloseBracket
	stepColumnDefPrimaryKey
	stepColumnDefConstraints
)

type ColDef struct {
	Name       string
	Type       ColType
	PrimaryKey bool
	Array      bool
}

type ColType string

const (
	// TODO: this is not all the types, missing: datetime, etc
	PgColTypeText   ColType = "text"
	PgColTypeInt2   ColType = "int2"
	PgColTypeInt4   ColType = "int4"
	PgColTypeInt8   ColType = "int8"
	PgColTypeNum    ColType = "numeric"
	PgColTypeFloat4 ColType = "float4"
	PgColTypeFloat8 ColType = "float8"
	PgColTypeBytea  ColType = "bytea"
	PgColTypeJson   ColType = "json"
	PgColTypeJsonB  ColType = "jsonb"
	PgColTypeBool   ColType = "bool"

	SQLiteColTypeInteger ColType = "integer"
	SQLiteColTypeReal    ColType = "real"
	SQLiteColTypeText    ColType = "text"
	SQLiteColTypeBlob    ColType = "blob"
)

func (c ColType) PgType() (oid, size int) {
	switch c {
	case SQLiteColTypeText:
		return 25, -1
	case SQLiteColTypeInteger:
		return 23, 4
	case SQLiteColTypeReal:
		return 700, 4
	case SQLiteColTypeBlob:
		return 17, -1
	}

	return -1, -1
}

type Parser struct {
	sql   string
	i     int
	step  step
	table string
	cols  []ColDef
}

func NewParser(sql string) *Parser {
	sql = strings.NewReplacer("\n", " ", "\t", " ").Replace(sql)
	return &Parser{sql: strings.TrimSpace(sql)}
}

func (p *Parser) Parse() (string, []ColDef, error) {
	for {
		if p.i >= len(p.sql) {
			return p.table, p.cols, nil
		}

		switch p.step {
		case stepType:
			if peeked := p.peek(); peeked != "CREATE TABLE" {
				return p.table, p.cols, errors.New("invalid query, peeked: " + peeked)
			}
			p.pop()
			p.step = stepCreateTable
		case stepCreateTable:
			switch p.peek() {
			case "IF NOT EXISTS":
				p.pop()
				p.step = stepIfNoExists
			default:
				p.popWhitespace()
				p.step = stepSchemaTableName
			}
		case stepSchemaTableName:
			log.Trace().Msg("enter stepSchemaTableName")
			p.table = p.pop()
			peeked := strings.ToUpper(p.peek())
			switch peeked {
			case "AS":
				p.step = stepAs
				p.pop()
			case "(":
				p.step = stepColumnDefsOpenBracket
				p.pop()
			default:
				return p.table, p.cols, errors.New("unknown table schema step, peeked: " + peeked)
			}
		case stepAs:
			return p.table, p.cols, errors.New("AS tables not supported")
		case stepColumnDefsOpenBracket:
			log.Trace().Msg("enter stepColumnDefsOpenBracket")
			switch p.peek() {
			case "PRIMARY KEY":
				p.pop()
				p.step = stepColumnDefPrimaryKey
				continue
			}
			colName := p.pop()
			log.Trace().Msgf("col name: %q\n", colName)
			typeName := p.pop()
			log.Trace().Msgf("type name: %q\n", typeName)
			p.cols = append(p.cols, ColDef{
				Name: colName,
				Type: ColType(typeName),
			})

			switch p.peek() {
			case ",":
				p.pop()
				p.step = stepColumnDefsOpenBracket
			case ")":
				p.pop()
				p.step = stepColumnDefsCloseBracket
			default:
				p.step = stepColumnDefConstraints
			}
		case stepColumnDefConstraints:
			log.Trace().Msg("enter stepColumnDefConstraints")
			switch p.peek() {
			case "NOT NULL":
				p.pop()
			case "PRIMARY KEY":
				p.pop()
				p.cols[len(p.cols)-1].PrimaryKey = true
			case ",":
				p.pop()
				p.step = stepColumnDefsOpenBracket
			case ")":
				p.pop()
				p.step = stepColumnDefsCloseBracket
			}

		case stepColumnDefPrimaryKey:
			log.Trace().Msg("enter stepColumnDefPrimaryKey")
			switch p.peek() {
			case "(":
				p.pop()
				colName := p.pop()
				p.makeColPK(colName)
				// make col PK
			case ",":
				p.pop()
				colName := p.pop()
				p.makeColPK(colName)
			case ")":
				p.step = stepColumnDefConstraints
			default:
				return p.table, p.cols, errors.New("unknown column def PK")
			}
		case stepColumnDefsCloseBracket:
			return p.table, p.cols, nil
		}
	}
}

func (p *Parser) makeColPK(name string) {
	for i := range p.cols {
		if p.cols[i].Name == name {
			p.cols[i].PrimaryKey = true
			return
		}
	}
}

func (p *Parser) peek() string {
	token, _ := p.peekWithLength()
	return token
}

func (p *Parser) seekToNext(r rune) bool {
	idx := strings.IndexRune(p.sql[p.i:], r)
	if idx == -1 {
		return false
	}

	p.i += idx + 1
	p.popWhitespace()
	return true
}

func (p *Parser) pop() string {
	log.Trace().Msgf(">popping (%d) %q\n", p.i, p.sql[p.i:])
	token, l := p.peekWithLength()
	p.i = p.i + l
	p.popWhitespace()

	log.Trace().Msgf("<popped %q (%d/%d) %q\n", token, p.i, l, p.sql[p.i:])

	return token
}

var tokens = []string{
	"CREATE TABLE", "IF NOT EXISTS", "AS", "(", ")", ";",
	"PRIMARY KEY", ",",
}

func (p *Parser) peekWithLength() (string, int) {
	if p.i >= len(p.sql) {
		return "", 0
	}

	for _, token := range tokens {
		t := strings.ToUpper(p.sql[p.i:min(len(p.sql), p.i+len(token))])
		if token == t {
			return t, len(t)
		}
	}

	return p.peekIdentifierWithLength()
}

var pattern = regexp.MustCompile(`[a-zA-Z0-9\._*]`)

func (p *Parser) peekIdentifierWithLength() (string, int) {
	for i := p.i; i < len(p.sql); i++ {
		if !pattern.MatchString(string(p.sql[i])) {
			return p.sql[p.i:i], len(p.sql[p.i:i])
		}
	}
	return p.sql[p.i:], len(p.sql[p.i:])
}

func (p *Parser) popWhitespace() {
	for ; p.i < len(p.sql) && p.sql[p.i] == ' '; p.i++ {
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
