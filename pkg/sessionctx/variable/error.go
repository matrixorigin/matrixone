package variable

import (
	mysql "matrixbase/pkg/errno"
	"matrixbase/pkg/util/dbterror"
)

// Error instances.
var (
	errWarnDeprecatedSyntax        = dbterror.ClassVariable.NewStd(mysql.ErrWarnDeprecatedSyntax)
	ErrSnapshotTooOld              = dbterror.ClassVariable.NewStd(mysql.ErrSnapshotTooOld)
	ErrUnsupportedValueForVar      = dbterror.ClassVariable.NewStd(mysql.ErrUnsupportedValueForVar)
	ErrUnknownSystemVar            = dbterror.ClassVariable.NewStd(mysql.ErrUnknownSystemVariable)
	ErrIncorrectScope              = dbterror.ClassVariable.NewStd(mysql.ErrIncorrectGlobalLocalVar)
	ErrUnknownTimeZone             = dbterror.ClassVariable.NewStd(mysql.ErrUnknownTimeZone)
	ErrReadOnly                    = dbterror.ClassVariable.NewStd(mysql.ErrVariableIsReadonly)
	ErrWrongValueForVar            = dbterror.ClassVariable.NewStd(mysql.ErrWrongValueForVar)
	ErrWrongTypeForVar             = dbterror.ClassVariable.NewStd(mysql.ErrWrongTypeForVar)
	ErrTruncatedWrongValue         = dbterror.ClassVariable.NewStd(mysql.ErrTruncatedWrongValue)
	ErrMaxPreparedStmtCountReached = dbterror.ClassVariable.NewStd(mysql.ErrMaxPreparedStmtCountReached)
	ErrUnsupportedIsolationLevel   = dbterror.ClassVariable.NewStd(mysql.ErrUnsupportedIsolationLevel)
)
