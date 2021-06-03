package catalog

import (
	"github.com/pingcap/parser/mysql"
	"matrixone/pkg/util/dbterror"
)

var (
	// ErrDBExists is the error for db exists.
	ErrDBExists = dbterror.ClassMeta.NewStd(mysql.ErrDBCreateExists)
	// ErrDBNotExists is the error for db not exists.
	ErrDBNotExists = dbterror.ClassMeta.NewStd(mysql.ErrBadDB)
	// ErrTableExists is the error for table exists.
	ErrTableExists = dbterror.ClassMeta.NewStd(mysql.ErrTableExists)
	// ErrTableNotExists is the error for table not exists.
	ErrTableNotExists = dbterror.ClassMeta.NewStd(mysql.ErrNoSuchTable)
)
