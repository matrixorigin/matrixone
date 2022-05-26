package operator

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
)

var SelsType = types.Type{Oid: types.T_bool, Size: 1}

// ErrDivByZero is reported on a division by zero.
var ErrDivByZero = errors.New(errno.SyntaxErrororAccessRuleViolation, "division by zero")

// ErrModByZero is reported when computing the rest of a division by zero.
var ErrModByZero = errors.New(errno.SyntaxErrororAccessRuleViolation, "zero modulus")
