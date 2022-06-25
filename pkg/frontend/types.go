package frontend

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type ComputationRunner interface {
	Run(ts uint64) (err error)
}

// ComputationWrapper is the wrapper of the computation
type ComputationWrapper interface {
	ComputationRunner
	GetAst() tree.Statement

	SetDatabaseName(db string) error

	GetColumns() ([]interface{}, error)

	GetAffectedRows() uint64

	Compile(u interface{}, fill func(interface{}, *batch.Batch) error) (interface{}, error)
}
