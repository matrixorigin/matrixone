package server

import (
	"crypto/tls"
	"matrixbase/pkg/session"
	"matrixbase/pkg/sessionctx/stmtctx"
)

type DBDriver struct {
}

func NewDBDriver() *DBDriver {
	driver := &DBDriver{}
	return driver
}

type DBContext struct {
	session.Session
	currentDB string
	stmts     map[int]*DBStatement
}

// GetWarnings implements QueryCtx GetWarnings method.
func (tc *DBContext) GetWarnings() []stmtctx.SQLWarn {
	return tc.GetSessionVars().StmtCtx.GetWarnings()
}

// CurrentDB implements QueryCtx CurrentDB method.
func (tc *DBContext) CurrentDB() string {
	return tc.currentDB
}

// WarningCount implements QueryCtx WarningCount method.
func (tc *DBContext) WarningCount() uint16 {
	return tc.GetSessionVars().StmtCtx.WarningCount()
}

type DBStatement struct {
	id          uint32
	numParams   int
	boundParams [][]byte
	paramsType  []byte
	ctx         *DBContext
	rs          ResultSet
	sql         string
}

func (ds *DBStatement) ID() int {
	return int(ds.id)
}

// OpenCtx implements IDriver.
func (qd *DBDriver) OpenCtx(connID uint64, capability uint32, collation uint8, dbname string, tlsState *tls.ConnectionState) (*DBContext, error) {
	se, err := session.CreateSession()
	if err != nil {
		return nil, err
	}
	se.SetTLSState(tlsState)
	err = se.SetCollation(int(collation))
	if err != nil {
		return nil, err
	}
	se.SetClientCapability(capability)
	se.SetConnectionID(connID)
	tc := &DBContext{
		Session:   se,
		currentDB: dbname,
		stmts:     make(map[int]*DBStatement),
	}
	return tc, nil
}

// Close implements QueryCtx Close method.
func (tc *DBContext) Close() error {
	tc.Session.Close()
	return nil
}
