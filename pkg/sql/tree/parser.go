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

func (p *Parser) Parse(sql string) (tree_stmt []Statement,err error) {
	defer func() {
		if er := recover(); er != nil{
			tree_stmt = nil
			err = fmt.Errorf("parse() panic %v",er)
		}
	}()

	var stmtNodes []ast.StmtNode = nil

	stmtNodes, _, err = p.p.Parse(sql, "", "")
	if err != nil {
		return nil, fmt.Errorf("parser parse failed.error:%v", err)
	}

	tree_stmt = make([]Statement, len(stmtNodes))
	for i, stmt := range stmtNodes {
		switch st := stmt.(type) {
		//Data Definition Statement DDL
		case *ast.CreateTableStmt:
			tree_stmt[i] = transformCreateTableStmtToCreateTable(st)
		case *ast.DropTableStmt:
			tree_stmt[i] = transformDropTableStmtToDropTable(st)
		case *ast.CreateDatabaseStmt:
			tree_stmt[i] = transformCreateDatabaseStmtToCreateDatabase(st)
		case *ast.DropDatabaseStmt:
			tree_stmt[i] = transformDropDatabaseStmtToDropDatabase(st)
		//Data Manipulation Statement DML
		case *ast.SelectStmt:
			tree_stmt[i] = transformSelectStmtToSelect(st)
		case *ast.SetOprStmt:
			tree_stmt[i] = transformSetOprStmtToSelectStatement(st)
		case *ast.InsertStmt:
			tree_stmt[i] = transformInsertStmtToInsert(st)
		case *ast.DeleteStmt:
			tree_stmt[i] = transformDeleteStmtToDelete(st)
		case *ast.UpdateStmt:
			tree_stmt[i] = transformUpdateStmtToUpdate(st)
		case *ast.LoadDataStmt:
			tree_stmt[i] = transformLoadDataStmtToLoad(st)
		//Transactional / Locking Statement
		case *ast.BeginStmt:
			tree_stmt[i] = transformBeginStmtToBeginTransaction(st)
		case *ast.CommitStmt:
			tree_stmt[i] = transformCommitStmtToCommitTransaction(st)
		case *ast.RollbackStmt:
			tree_stmt[i] = transformRollbackStmtToRollbackTransaction(st)
		//Database Administration Statement
		case *ast.SetStmt:
			tree_stmt[i] = transformSetStmtToSetVar(st)
		case *ast.ShowStmt:
			tree_stmt[i] = transformShowStmtToShow(st)
		//Utility Statemnt
		case *ast.ExplainStmt:
			tree_stmt[i] = transformExplainStmtToExplain(st)
		case *ast.ExplainForStmt:
			tree_stmt[i] = transformExplainForStmtToExplain(st)
		case *ast.UseStmt:
			tree_stmt[i] = transformUseStmtToUse(st)
		default:
			return nil, fmt.Errorf("unsupported transformer for %s", sql)
		}
	}
	return tree_stmt, nil
}
