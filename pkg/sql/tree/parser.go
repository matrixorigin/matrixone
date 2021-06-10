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
		switch st := stmt.(type) {
		case *ast.SelectStmt:
			tree_stmt[i] = transformSelectStmtToSelect(st)
		case *ast.SetOprStmt:
			tree_stmt[i] = transformSetOprStmtToSelectStatement(st)
		case *ast.InsertStmt:
			tree_stmt[i] = transformInsertStmtToInsert(st)
		case *ast.CreateTableStmt:
			tree_stmt[i] = transformCreateTableStmtToCreateTable(st)
		default:
			return nil, fmt.Errorf("parser parse failed.error:%v", err)
		}
	}
	return tree_stmt, nil
}
