package operator

import (
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
)

var (
	// ErrDivByZero is reported on a division by zero.
	ErrDivByZero = errors.New(errno.SyntaxErrororAccessRuleViolation, "division by zero")
	// ErrModByZero is reported when computing the rest of a division by zero.
	ErrModByZero = errors.New(errno.SyntaxErrororAccessRuleViolation, "zero modulus")
)
