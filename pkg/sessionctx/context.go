package sessionctx

import (
	"fmt"

	"matrixone/pkg/sessionctx/variable"
	"matrixone/pkg/util"
)

// Context is an interface for transaction and executive args environment.
type Context interface {
	// SetValue saves a value associated with this context for key.
	SetValue(key fmt.Stringer, value interface{})

	// Value returns the value associated with this context for key.
	Value(key fmt.Stringer) interface{}

	// ClearValue clears the value associated with this context for key.
	ClearValue(key fmt.Stringer)

	GetSessionVars() *variable.SessionVars

	GetSessionManager() util.SessionManager

	// RefreshVars refreshes modified global variable to current session.
	// only used to daemon session like `statsHandle` to detect global variable change.
	// RefreshVars(context.Context) error
}

type basicCtxType int

func (t basicCtxType) String() string {
	switch t {
	case QueryString:
		return "query_string"
	case Initing:
		return "initing"
	case LastExecuteDDL:
		return "last_execute_ddl"
	}
	return "unknown"
}

// Context keys.
const (
	// QueryString is the key for original query string.
	QueryString basicCtxType = 1
	// Initing is the key for indicating if the server is running bootstrap or upgrade job.
	Initing basicCtxType = 2
	// LastExecuteDDL is the key for whether the session execute a ddl command last time.
	LastExecuteDDL basicCtxType = 3
)
