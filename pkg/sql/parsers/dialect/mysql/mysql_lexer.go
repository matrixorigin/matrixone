// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type MySQLParser struct {
	scanner Scanner
	lexer   Lexer
	// use this will save some memory allocation
	// parser  yyParserImpl
}

func (p *MySQLParser) Parse(ctx context.Context, sql string, lower int64) ([]tree.Statement, error) {
	p.scanner.setSql(sql)
	p.lexer.setScanner(&p.scanner, lower)

	/*
		The following can potentially save some memory allocation, but it exposes too much
		of yyParser internals, which is really yacc's business, not ours.
		I don't think it's worth it.

		p.parser.yySymType = &yySymType{}
		for i := 0; i < len(p.parser.stack); i++ {
			p.parser.stack[i] = yySymType{}
		}
		p.parser.char = 0
	*/

	if yyParse(&p.lexer) != 0 {
		for _, s := range p.lexer.stmts {
			s.Free()
		}
		return nil, p.lexer.scanner.LastError
	}
	if len(p.lexer.stmts) == 0 {
		/**
		For CORNER CASE like:

		mysql> -- MySQL dump 10.13  Distrib 8.1.0, for macos11.7 (arm64)

		the input will be stripped to empty string, and the parser will return 0 stmts.
		but, the mysql server responds ok to the client.
		so, we return an EmptyStmt that does nothing beside responding ok.
		*/
		return []tree.Statement{&tree.EmptyStmt{}}, nil
	}
	return p.lexer.stmts, nil
}

func Parse(ctx context.Context, sql string, lower int64) ([]tree.Statement, error) {
	lexer := NewLexer(dialect.MYSQL, sql, lower)
	defer PutScanner(lexer.scanner)
	if yyParse(lexer) != 0 {
		for _, s := range lexer.stmts {
			s.Free()
		}
		return nil, lexer.scanner.LastError
	}
	if len(lexer.stmts) == 0 {
		/**
		For CORNER CASE like:

		mysql> -- MySQL dump 10.13  Distrib 8.1.0, for macos11.7 (arm64)

		the input will be stripped to empty string, and the parser will return 0 stmts.
		but, the mysql server responds ok to the client.
		so, we return an EmptyStmt that does nothing beside responding ok.
		*/
		return []tree.Statement{&tree.EmptyStmt{}}, nil
	}
	return lexer.stmts, nil
}

func ParseOne(ctx context.Context, sql string, lower int64) (tree.Statement, error) {
	lexer := NewLexer(dialect.MYSQL, sql, lower)
	defer PutScanner(lexer.scanner)
	if yyParse(lexer) != 0 {
		for _, s := range lexer.stmts {
			s.Free()
		}
		return nil, lexer.scanner.LastError
	}
	if len(lexer.stmts) != 1 {
		return nil, moerr.NewParseError(ctx, "syntax error, or too many sql to parse")
	}
	return lexer.stmts[0], nil
}

type Lexer struct {
	scanner    *Scanner
	stmts      []tree.Statement
	paramIndex int
	lower      int64
}

func NewLexer(dialectType dialect.DialectType, sql string, lower int64) *Lexer {
	return &Lexer{
		scanner:    NewScanner(dialectType, sql),
		paramIndex: 0,
		lower:      lower,
	}
}

func (l *Lexer) setScanner(s *Scanner, lower int64) {
	l.scanner = s
	l.stmts = nil
	l.paramIndex = 0
	l.lower = lower
}

func (l *Lexer) GetParamIndex() int {
	l.paramIndex = l.paramIndex + 1
	return l.paramIndex
}

func (l *Lexer) Lex(lval *yySymType) int {
	typ, str := l.scanner.Scan()
	l.scanner.LastToken = str

	switch typ {
	case INTEGRAL:
		return l.toInt(lval, str)
	case FLOAT:
		return l.toFloat(lval, str)
	}

	lval.str = str
	return typ
}

func (l *Lexer) GetDbOrTblName(origin string) string {
	if l.lower == 1 {
		return strings.ToLower(origin)
	}
	return origin
}

func (l *Lexer) GetDbOrTblNameCStr(origin string) *tree.CStr {
	return tree.NewCStr(origin, l.lower)
}

var CaseInsensitiveDbs = []string{"information_schema", "mysql"}

func (l *Lexer) Error(err string) {
	errMsg := fmt.Sprintf("You have an error in your SQL syntax; check the manual that corresponds to your MatrixOne server version for the right syntax to use. %s", err)
	near := l.scanner.buf[l.scanner.PrePos:]
	var lenStr string
	if len(near) > 1024 {
		lenStr = " (total length " + strconv.Itoa(len(lenStr)) + ")"
		near = near[:1024]
	}
	l.scanner.LastError = PositionedErr{Err: errMsg, Line: l.scanner.Line, Col: l.scanner.Col, Near: near, LenStr: lenStr}
}

func (l *Lexer) AppendStmt(stmt tree.Statement) {
	l.stmts = append(l.stmts, stmt)
}

func (l *Lexer) toInt(lval *yySymType, str string) int {
	ival, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		// TODO: toDecimal()
		// l.scanner.LastError = err
		lval.str = str
		return DECIMAL_VALUE
	}
	switch {
	case ival <= math.MaxInt64:
		lval.item = int64(ival)
	default:
		lval.item = ival
	}
	lval.str = str
	return INTEGRAL
}

func (l *Lexer) toFloat(lval *yySymType, str string) int {
	fval, err := strconv.ParseFloat(str, 64)
	if err != nil {
		l.scanner.LastError = err
		return LEX_ERROR
	}
	lval.item = fval
	return FLOAT
}
