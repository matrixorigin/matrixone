package catalog

import (
	"errors"
)

var (
	// ErrDBCreate is the err
	ErrDBCreate = errors.New("db create failed")
	// ErrDBCreateExists is the error for db exists.
	ErrDBCreateExists = errors.New("db already exists")
	// ErrDBNotExists is the error for db not exists.
	ErrDBNotExists = errors.New("db not exist")
	// ErrTableCreateExists is the error for table exists.
	ErrTableCreateExists = errors.New("table already exists")
	// ErrTableNotExists is the error for table not exists.
	ErrTableNotExists     = errors.New("table not exist")
	ErrTableCreateFailed  = errors.New("create table failed")
	ErrTooMuchTableExists = errors.New("the maximum limit of tables has been exceeded")
	ErrNoAvailableShard   = errors.New("no available raft group")
	ErrTableCreateTimeout = errors.New("create table timeout")
)
