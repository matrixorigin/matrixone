package frontend

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// ComputationWrapper is the wrapper of the computation
type ComputationWrapper interface {
	GetAst() tree.Statement

	SetDatabaseName(db string) error

	GetColumns() ([]interface{},error)

	GetAffectedRows() uint64

	Compile(u interface{},
		fill func(interface{}, *batch.Batch) error) error

	Run(ts uint64) error
}