// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cache

import (
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
)

func New() *Cache {
	return &Cache{
		pc: newParserCache(),
	}
}

func (c *Cache) Free() {
}

func (c *Cache) GetParser(dialectType dialect.DialectType, sql string, lower int64) *mysql.Lexer {
	p := c.pc.getParser()
	p.Reset(dialectType, sql, lower)
	return p
}

func (c *Cache) PutParser(p *mysql.Lexer) {
	c.pc.putParser(p)
}

func newParserCache() *parserCache {
	return &parserCache{}
}

func (pc *parserCache) getParser() *mysql.Lexer {
	if len(pc.parsers) == 0 {
		return mysql.NewLexer(dialect.MYSQL, "", 0)
	}
	parser := pc.parsers[0]
	pc.parsers = pc.parsers[1:]
	return parser
}

func (pc *parserCache) putParser(parser *mysql.Lexer) {
	pc.parsers = append(pc.parsers, parser)
}
