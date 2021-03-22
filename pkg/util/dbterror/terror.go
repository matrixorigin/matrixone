package dbterror

import (
	"matrixbase/pkg/errno"
	"matrixbase/pkg/parser/terror"
)

// ErrClass represents a class of errors.
type ErrClass struct{ terror.ErrClass }

// Error classes.
var (
	ClassAutoid     = ErrClass{terror.ClassAutoid}
	ClassDDL        = ErrClass{terror.ClassDDL}
	ClassDomain     = ErrClass{terror.ClassDomain}
	ClassExecutor   = ErrClass{terror.ClassExecutor}
	ClassExpression = ErrClass{terror.ClassExpression}
	ClassAdmin      = ErrClass{terror.ClassAdmin}
	ClassKV         = ErrClass{terror.ClassKV}
	ClassMeta       = ErrClass{terror.ClassMeta}
	ClassOptimizer  = ErrClass{terror.ClassOptimizer}
	ClassPrivilege  = ErrClass{terror.ClassPrivilege}
	ClassSchema     = ErrClass{terror.ClassSchema}
	ClassServer     = ErrClass{terror.ClassServer}
	ClassStructure  = ErrClass{terror.ClassStructure}
	ClassVariable   = ErrClass{terror.ClassVariable}
	ClassXEval      = ErrClass{terror.ClassXEval}
	ClassTable      = ErrClass{terror.ClassTable}
	ClassTypes      = ErrClass{terror.ClassTypes}
	ClassJSON       = ErrClass{terror.ClassJSON}
	ClassTiKV       = ErrClass{terror.ClassTiKV}
	ClassSession    = ErrClass{terror.ClassSession}
	ClassPlugin     = ErrClass{terror.ClassPlugin}
	ClassUtil       = ErrClass{terror.ClassUtil}
)

// NewStd calls New using the standard message for the error code
// Attention:
// this method is not goroutine-safe and
// usually be used in global variable initializer
func (ec ErrClass) NewStd(code terror.ErrCode) *terror.Error {
	return ec.NewStdErr(code, errno.MySQLErrName[uint16(code)])
}
