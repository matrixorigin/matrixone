package main

import (
	"github.com/pingcap/parser/ast"
	log "github.com/sirupsen/logrus"
)

type checker struct {
	Pos int
}

func getDBFromResultNode(pos int, resultNode ast.ResultSetNode) []string {
	var dbLables []string
	if resultNode == nil {
		return dbLables
	}
	switch node := resultNode.(type) {
	case *ast.TableSource:
		log.Infof("[%d] GRN-TableSource", pos)
		return getDBFromResultNode(pos, node.Source)
	case *ast.TableName:
		log.Infof("[%d] GRN-TableName: %v", pos, node.Name)
		// dbLables = append(dbLables, node.DBInfo.Name.O)
	case *ast.Join:
		iter := func(x ast.ResultSetNode) {
			if x != nil {
				dbs := getDBFromResultNode(pos, x)
				if dbs != nil {
					dbLables = append(dbLables, dbs...)
				}
			}
		}

		iter(node.Left)
		iter(node.Right)
	}
	return dbLables
}

func (v *checker) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch node := in.(type) {
	case *ast.TableName:
		log.Infof("[%d] TableName: %v", v.Pos, node.Name)
	case *ast.SelectStmt:
		log.Infof("[%d] SelectStmt", v.Pos)
		if node.From != nil {
			getDBFromResultNode(v.Pos, node.From.TableRefs)
		}
	case *ast.InsertStmt:
		log.Infof("[%d] InsertStmt", v.Pos)
	}
	return in, false
}

func (v *checker) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
}
