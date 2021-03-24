package server

import (
	"context"
	"encoding/binary"

	"github.com/pingcap/parser/mysql"
)

func (cc *clientConn) handleStmtPrepare(ctx context.Context, sql string) error {
	return mysql.NewErrf(mysql.ErrUnknown, "command %d not supported now", nil, mysql.ComStmtPrepare)
}

func (cc *clientConn) handleStmtExecute(ctx context.Context, data []byte) (err error) {
	return mysql.NewErrf(mysql.ErrUnknown, "command %d not supported now", nil, mysql.ComStmtExecute)
}

func (cc *clientConn) handleStmtFetch(ctx context.Context, data []byte) (err error) {
	return mysql.NewErrf(mysql.ErrUnknown, "command %d not supported now", nil, mysql.ComStmtFetch)
}

func (cc *clientConn) handleStmtSendLongData(data []byte) (err error) {
	return mysql.NewErrf(mysql.ErrUnknown, "command %d not supported now", nil, mysql.ComStmtSendLongData)
}

func (cc *clientConn) handleStmtReset(ctx context.Context, data []byte) (err error) {
	return mysql.NewErrf(mysql.ErrUnknown, "command %d not supported now", nil, mysql.ComStmtReset)
}

// handleSetOption refer to https://dev.mysql.com/doc/internals/en/com-set-option.html
func (cc *clientConn) handleSetOption(ctx context.Context, data []byte) (err error) {
	if len(data) < 2 {
		return mysql.ErrMalformPacket
	}

	switch binary.LittleEndian.Uint16(data[:2]) {
	case 0:
		cc.capability |= mysql.ClientMultiStatements
		cc.ctx.SetClientCapability(cc.capability)
	case 1:
		cc.capability &^= mysql.ClientMultiStatements
		cc.ctx.SetClientCapability(cc.capability)
	default:
		return mysql.ErrMalformPacket
	}
	if err = cc.writeEOF(0); err != nil {
		return err
	}

	return cc.flush(ctx)
}

func (cc *clientConn) handleStmtClose(data []byte) (err error) {
	return mysql.NewErrf(mysql.ErrUnknown, "command %d not supported now", nil, mysql.ComStmtClose)
}
