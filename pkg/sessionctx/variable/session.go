package variable

import (
	"crypto/tls"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"matrixone/pkg/sessionctx/stmtctx"
	"strconv"
	"time"
)

type SessionVars struct {
	systems map[string]string

	// EnableWindowFunction enables the window function.
	EnableWindowFunction bool

	// EnableStrictDoubleTypeCheck enables table field double type check.
	EnableStrictDoubleTypeCheck bool

	// StmtCtx holds variables for current executing statement.
	StmtCtx *stmtctx.StatementContext

	//  TxnCtx Should be reset on transaction finished.
	TxnCtx *TransactionContext

	// Status stands for the session status. e.g. in transaction or not, auto commit is on or off, and so on.
	Status uint16

	SQLMode mysql.SQLMode

	// StrictSQLMode indicates if the session is in strict mode.
	StrictSQLMode bool

	// Per-connection time zones. Each client that connects has its own time zone setting, given by the session time_zone variable.
	// See https://dev.mysql.com/doc/refman/5.7/en/time-zone-support.html
	TimeZone *time.Location

	// AutoIncrementIncrement and AutoIncrementOffset indicates the autoID's start value and increment.
	AutoIncrementIncrement int

	AutoIncrementOffset int

	// GlobalVarsAccessor is used to set and get global variables.
	GlobalVarsAccessor GlobalVarAccessor

	// User is the user identity with which the session login.
	User *auth.UserIdentity

	// ClientCapability is client's capability.
	ClientCapability uint32

	// ConnectionID is the connection id of the current session.
	ConnectionID uint64

	// SelectLimit limits the max counts of select statement's output
	SelectLimit uint64

	// TLSConnectionState is the TLS connection state (nil if not using TLS).
	TLSConnectionState *tls.ConnectionState

	// CommandValue indicates which command current session is doing.
	CommandValue uint32

	// Port is the port of the connected socket
	Port string

	// Killed is a flag to indicate that this query is killed.
	Killed uint32
}

type ConnectionInfo struct {
	ConnectionID      uint64
	ConnectionType    string
	Host              string
	ClientIP          string
	ClientPort        string
	ServerID          int
	ServerPort        int
	Duration          float64
	User              string
	ServerOSLoginUser string
	OSVersion         string
	ClientVersion     string
	ServerVersion     string
	SSLVersion        string
	PID               int
	DB                string
}

// NewSessionVars creates a session vars object.
func NewSessionVars() *SessionVars {
	vars := &SessionVars{
		systems:       make(map[string]string),
		TxnCtx:        &TransactionContext{},
		StrictSQLMode: true,
		Status:        mysql.ServerStatusAutocommit,
		StmtCtx:       new(stmtctx.StatementContext),
	}
	return vars
}

// TransactionContext is used to store variables that has transaction scope.
type TransactionContext struct {
}

// special session variables.
const (
	SQLModeVar          = "sql_mode"
	MaxAllowedPacket    = "max_allowed_packet"
	TimeZone            = "time_zone"
	MaxExecutionTime    = "max_execution_time"
	CharacterSetResults = "character_set_results"
)

// GetCharsetInfo gets charset and collation for current context.
// What character set should the server translate a statement to after receiving it?
// For this, the server uses the character_set_connection and collation_connection system variables.
// It converts statements sent by the client from character_set_client to character_set_connection
// (except for string literals that have an introducer such as _latin1 or _utf8).
// collation_connection is important for comparisons of literal strings.
// For comparisons of strings with column values, collation_connection does not matter because columns
// have their own collation, which has a higher collation precedence.
// See https://dev.mysql.com/doc/refman/5.7/en/charset-connection.html
func (s *SessionVars) GetCharsetInfo() (charset, collation string) {
	charset = s.systems[CharacterSetConnection]
	collation = s.systems[CollationConnection]
	return
}

// BuildParserConfig generate parser.ParserConfig for initial parser
func (s *SessionVars) BuildParserConfig() parser.ParserConfig {
	return parser.ParserConfig{
		EnableWindowFunction:        s.EnableWindowFunction,
		EnableStrictDoubleTypeCheck: s.EnableStrictDoubleTypeCheck,
	}
}

func (s *SessionVars) SetSystemVar(name string, val string) error {
	switch name {
	case SQLModeVar:
		val = mysql.FormatSQLModeStr(val)
		// Modes is a list of different modes separated by commas.
		sqlMode, err2 := mysql.GetSQLMode(val)
		if err2 != nil {
			return errors.Trace(err2)
		}
		s.StrictSQLMode = sqlMode.HasStrictMode()
		s.SQLMode = sqlMode
		s.SetStatusFlag(mysql.ServerStatusNoBackslashEscaped, sqlMode.HasNoBackslashEscapesMode())
	case AutoCommit:
		isAutocommit := DBOptOn(val)
		s.SetStatusFlag(mysql.ServerStatusAutocommit, isAutocommit)
		if isAutocommit {
			s.SetInTxn(false)
		}
	case AutoIncrementIncrement:
		// AutoIncrementIncrement is valid in [1, 65535].
		s.AutoIncrementIncrement = dbOptPositiveInt32(val, DefAutoIncrementIncrement)
	case AutoIncrementOffset:
		// AutoIncrementOffset is valid in [1, 65535].
		s.AutoIncrementOffset = dbOptPositiveInt32(val, DefAutoIncrementOffset)
	case CharacterSetConnection, CharacterSetClient, CharacterSetResults,
		CharacterSetServer, CharsetDatabase, CharacterSetFilesystem:
		if val == "" {
			if name == CharacterSetResults {
				s.systems[CharacterSetResults] = ""
				return nil
			}
			return ErrWrongValueForVar.GenWithStackByArgs(name, "NULL")
		}
		cht, coll, err := charset.GetCharsetInfo(val)
		if err != nil {
			// logutil.BgLogger().Warn(err.Error())
			cht, coll = charset.GetDefaultCharsetAndCollate()
		}
		switch name {
		case CharacterSetConnection:
			s.systems[CollationConnection] = coll
			s.systems[CharacterSetConnection] = cht
		case CharsetDatabase:
			s.systems[CollationDatabase] = coll
			s.systems[CharsetDatabase] = cht
		case CharacterSetServer:
			s.systems[CollationServer] = coll
			s.systems[CharacterSetServer] = cht
		}
		val = cht
	case SQLSelectLimit:
		result, err := strconv.ParseUint(val, 10, 64)
		if err != nil {
			return errors.Trace(err)
		}
		s.SelectLimit = result
	}
	s.systems[name] = val
	return nil
}

// SetStatusFlag sets the session server status variable.
// If on is ture sets the flag in session status,
// otherwise removes the flag.
func (s *SessionVars) SetStatusFlag(flag uint16, on bool) {
	if on {
		s.Status |= flag
		return
	}
	s.Status &= ^flag
}

// SetInTxn sets whether the session is in transaction.
// It also updates the IsExplicit flag in TxnCtx if val is true.
func (s *SessionVars) SetInTxn(val bool) {
	s.SetStatusFlag(mysql.ServerStatusInTrans, val)
}
