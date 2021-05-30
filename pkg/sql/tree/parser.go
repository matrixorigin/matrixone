package tree

import (
	"fmt"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
)

type Parser struct {
	p *parser.Parser
}

func NewParser() *Parser {
	return &Parser{p: parser.New()}
}

func (p *Parser) Parse(sql string) ([]Statement, error) {
	stmtNodes, _, err := p.p.Parse(sql, "", "")
	if err != nil {
		return nil, fmt.Errorf("parser parse failed.error:%v", err)
	}

	var tree_stmt []Statement = make([]Statement, len(stmtNodes))
	for i, stmt := range stmtNodes {
		if ss, ok := stmt.(*ast.SelectStmt); !ok {
			return nil, fmt.Errorf("parser parse failed.error:%v", err)
		} else {
			tree_stmt[i] = transformSelectStmtToSelect(ss)
		}
	}
	return tree_stmt, nil
}
