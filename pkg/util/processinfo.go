package util

import (
	"crypto/tls"
	"sync/atomic"
	"time"
)

type GlobalConnID struct {
	ServerID       uint64
	LocalConnID    uint64
	Is64bits       bool
	ServerIDGetter func() uint64
}

const (
	// MaxServerID is maximum serverID.
	MaxServerID = 1<<22 - 1
)

// ProcessInfo is a struct used for show processlist statement.
type ProcessInfo struct {
	ID              uint64
	User            string
	Host            string
	Port            string
	DB              string
	Digest          string
	Plan            interface{}
	PlanExplainRows [][]string
	// RuntimeStatsColl *execdetails.RuntimeStatsColl
	Time          time.Time
	Info          string
	CurTxnStartTS uint64
	// StmtCtx       *stmtctx.StatementContext
	// StatsInfo func(interface{}) map[string]uint64
	// MaxExecutionTime is the timeout for select statement, in milliseconds.
	// If the query takes too long, kill it.
	MaxExecutionTime uint64

	State                     uint16
	Command                   byte
	ExceedExpensiveTimeThresh bool
	RedactSQL                 bool
}

// SessionManager is an interface for session manage. Show processlist and
// kill statement rely on this interface.
type SessionManager interface {
	// ShowProcessList() map[uint64]*ProcessInfo
	// GetProcessInfo(id uint64) (*ProcessInfo, bool)
	Kill(connectionID uint64, query bool)
	KillAllConnections()
	UpdateTLSConfig(cfg *tls.Config)
	// ServerID() uint64
}

func (g *GlobalConnID) makeID(localConnID uint64) uint64 {
	var (
		id       uint64
		serverID uint64
	)
	if g.ServerIDGetter != nil {
		serverID = g.ServerIDGetter()
	} else {
		serverID = g.ServerID
	}
	if g.Is64bits {
		id |= 0x1
		id |= localConnID & 0xff_ffff_ffff << 1 // 40 bits local connID.
		id |= serverID & MaxServerID << 41      // 22 bits serverID.
	} else {
		// TODO: update after new design for 32 bits version.
		id |= localConnID & 0x7fff_ffff << 1 // 31 bits local connID.
	}
	return id
}

// ID returns the connection id
func (g *GlobalConnID) ID() uint64 {
	return g.makeID(g.LocalConnID)
}

// NextID returns next connection id
func (g *GlobalConnID) NextID() uint64 {
	localConnID := atomic.AddUint64(&g.LocalConnID, 1)
	return g.makeID(localConnID)
}
