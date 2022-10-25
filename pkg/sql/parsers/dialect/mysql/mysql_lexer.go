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
	"fmt"
	"math"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func Parse(sql string) ([]tree.Statement, error) {
	lexer := NewLexer(dialect.MYSQL, sql)
	if yyParse(lexer) != 0 {
		return nil, lexer.scanner.LastError
	}
	return lexer.stmts, nil
}

func ParseOne(sql string) (tree.Statement, error) {
	lexer := NewLexer(dialect.MYSQL, sql)
	if yyParse(lexer) != 0 {
		return nil, lexer.scanner.LastError
	}
	if len(lexer.stmts) != 1 {
		return nil, moerr.NewInternalError("syntax error, or too many sql to parse")
	}
	return lexer.stmts[0], nil
}

type Lexer struct {
	scanner    *Scanner
	stmts      []tree.Statement
	paramIndex int
}

func NewLexer(dialectType dialect.DialectType, sql string) *Lexer {
	return &Lexer{
		scanner:    NewScanner(dialectType, sql),
		paramIndex: 0,
	}
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
	case HEX:
		return l.toHex(lval, str)
	case HEXNUM:
		return l.toHexNum(lval, str)
	case BIT_LITERAL:
		return l.toBit(lval, str)
	}

	lval.str = str
	return typ
}

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

func (l *Lexer) toHex(lval *yySymType, str string) int {
	return HEX
}

// don't transfer 0xXXX to int,uint, all string([]byte),for example
// 0x0616161 will be ' aaa'
func (l *Lexer) toHexNum(lval *yySymType, str string) int {
	// it won't be err, no need to process
	// ival, err := strconv.ParseUint(str[2:], 16, 64)
	// if err != nil {
	// 	// TODO: toDecimal()
	// 	//l.scanner.LastError = err
	// 	lval.item = str
	// 	return HEXNUM
	// }
	// switch {
	// case ival <= math.MaxInt64:
	// 	lval.item = int64(ival)
	// default:
	// 	lval.item = ival
	// }
	// lval.str = str
	lval.str = str
	return HEXNUM
}

func (l *Lexer) toBit(lval *yySymType, str string) int {
	ival, err := strconv.ParseUint(str[2:], 2, 64)
	if err != nil {
		// TODO: toDecimal()
		//l.scanner.LastError = err
		lval.item = str
		return BIT_LITERAL
	}
	switch {
	case ival <= math.MaxInt64:
		lval.item = int64(ival)
	default:
		lval.item = ival
	}
	lval.str = str
	return BIT_LITERAL
}
