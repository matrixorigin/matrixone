// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

// tuple (collation name, charset)
type charsetCollationName struct {
	charset       string
	collationName string
}

// mysql client capabilities
const (
	CLIENT_LONG_PASSWORD                  uint32 = 0x00000001
	CLIENT_FOUND_ROWS                     uint32 = 0x00000002
	CLIENT_LONG_FLAG                      uint32 = 0x00000004
	CLIENT_CONNECT_WITH_DB                uint32 = 0x00000008
	CLIENT_NO_SCHEMA                      uint32 = 0x00000010
	CLIENT_COMPRESS                       uint32 = 0x00000020
	CLIENT_LOCAL_FILES                    uint32 = 0x00000080
	CLIENT_IGNORE_SPACE                   uint32 = 0x00000100
	CLIENT_PROTOCOL_41                    uint32 = 0x00000200
	CLIENT_INTERACTIVE                    uint32 = 0x00000400
	CLIENT_SSL                            uint32 = 0x00000800
	CLIENT_IGNORE_SIGPIPE                 uint32 = 0x00001000
	CLIENT_TRANSACTIONS                   uint32 = 0x00002000
	CLIENT_RESERVED                       uint32 = 0x00004000
	CLIENT_SECURE_CONNECTION              uint32 = 0x00008000
	CLIENT_MULTI_STATEMENTS               uint32 = 0x00010000
	CLIENT_MULTI_RESULTS                  uint32 = 0x00020000
	CLIENT_PS_MULTI_RESULTS               uint32 = 0x00040000
	CLIENT_PLUGIN_AUTH                    uint32 = 0x00080000
	CLIENT_CONNECT_ATTRS                  uint32 = 0x00100000
	CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA uint32 = 0x00200000
	CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS   uint32 = 0x00400000
	CLIENT_SESSION_TRACK                  uint32 = 0x00800000
	CLIENT_DEPRECATE_EOF                  uint32 = 0x01000000
	CLIENT_ZSTD_COMPRESSION_ALGORITHM     uint32 = 0x04000000
)

// server status
const (
	SERVER_STATUS_IN_TRANS             uint16 = 0x0001 // A transaction is currently active
	SERVER_STATUS_AUTOCOMMIT           uint16 = 0x0002 // Autocommit mode is set
	SERVER_MORE_RESULTS_EXISTS         uint16 = 0x0008 // More results exists (more packet follow)
	SERVER_STATUS_NO_GOOD_INDEX_USED   uint16 = 0x0010
	SERVER_STATUS_NO_INDEX_USED        uint16 = 0x0020
	SERVER_STATUS_CURSOR_EXISTS        uint16 = 0x0040 // When using COM_STMT_FETCH, indicate that current cursor still has result
	SERVER_STATUS_LAST_ROW_SENT        uint16 = 0x0080 // When using COM_STMT_FETCH, indicate that current cursor has finished to send results
	SERVER_STATUS_DB_DROPPED           uint16 = 0x0100 // Database has been dropped
	SERVER_STATUS_NO_BACKSLASH_ESCAPES uint16 = 0x0200 // Current escape mode is "no backslash escape"
	SERVER_STATUS_METADATA_CHANGED     uint16 = 0x0400 // A DDL change did have an impact on an existing PREPARE (an automatic reprepare has been executed)
	SERVER_QUERY_WAS_SLOW              uint16 = 0x0800
	SERVER_PS_OUT_PARAMS               uint16 = 0x1000 // This resultset contain stored procedure output parameter
	SERVER_STATUS_IN_TRANS_READONLY    uint16 = 0x2000 // Current transaction is a read-only transaction
	SERVER_SESSION_STATE_CHANGED       uint16 = 0x4000 // Session state change. see Session change type for more information
)

type CommandType uint8

// text protocol in mysql client protocol
// iteration command
const (
	COM_SLEEP               CommandType = 0x00
	COM_QUIT                CommandType = 0x01
	COM_INIT_DB             CommandType = 0x02
	COM_QUERY               CommandType = 0x03
	COM_FIELD_LIST          CommandType = 0x04
	COM_CREATE_DB           CommandType = 0x05
	COM_DROP_DB             CommandType = 0x06
	COM_REFRESH             CommandType = 0x07
	COM_SHUTDOWN            CommandType = 0x08
	COM_STATISTICS          CommandType = 0x09
	COM_PROCESS_INFO        CommandType = 0x0a
	COM_CONNECT             CommandType = 0x0b
	COM_PROCESS_KILL        CommandType = 0x0c
	COM_DEBUG               CommandType = 0x0d
	COM_PING                CommandType = 0x0e
	COM_TIME                CommandType = 0x0f
	COM_DELAYED_INSERT      CommandType = 0x10
	COM_CHANGE_USER         CommandType = 0x11
	COM_REGISTER_SLAVE      CommandType = 0x15
	COM_STMT_PREPARE        CommandType = 0x16
	COM_STMT_EXECUTE        CommandType = 0x17
	COM_STMT_SEND_LONG_DATA CommandType = 0x18
	COM_STMT_CLOSE          CommandType = 0x19
	COM_STMT_RESET          CommandType = 0x1a
	COM_SET_OPTION          CommandType = 0x1b
	COM_STMT_FETCH          CommandType = 0x1c
	COM_DAEMON              CommandType = 0x1d
	COM_BINLOG_DUMP_GTID    CommandType = 0x1e
	COM_RESET_CONNECTION    CommandType = 0x1f
)

func (ct CommandType) String() string {
	switch ct {
	case COM_SLEEP:
		return "COM_SLEEP"
	case COM_QUIT:
		return "COM_QUIT"
	case COM_INIT_DB:
		return "COM_INIT_DB"
	case COM_QUERY:
		return "COM_QUERY"
	case COM_FIELD_LIST:
		return "COM_FIELD_LIST"
	case COM_CREATE_DB:
		return "COM_CREATE_DB"
	case COM_DROP_DB:
		return "COM_DROP_DB"
	case COM_REFRESH:
		return "COM_REFRESH"
	case COM_SHUTDOWN:
		return "COM_SHUTDOWN"
	case COM_STATISTICS:
		return "COM_STATISTICS"
	case COM_PROCESS_INFO:
		return "COM_PROCESS_INFO"
	case COM_CONNECT:
		return "COM_CONNECT"
	case COM_PROCESS_KILL:
		return "COM_PROCESS_KILL"
	case COM_DEBUG:
		return "COM_DEBUG"
	case COM_PING:
		return "COM_PING"
	case COM_TIME:
		return "COM_TIME"
	case COM_DELAYED_INSERT:
		return "COM_DELAYED_INSERT"
	case COM_CHANGE_USER:
		return "COM_CHANGE_USER"
	case COM_STMT_PREPARE:
		return "COM_STMT_PREPARE"
	case COM_STMT_EXECUTE:
		return "COM_STMT_EXECUTE"
	case COM_STMT_SEND_LONG_DATA:
		return "COM_STMT_SEND_LONG_DATA"
	case COM_STMT_CLOSE:
		return "COM_STMT_CLOSE"
	case COM_STMT_RESET:
		return "COM_STMT_RESET"
	case COM_SET_OPTION:
		return "COM_SET_OPTION"
	case COM_STMT_FETCH:
		return "COM_STMT_FETCH"
	case COM_DAEMON:
		return "COM_DAEMON"
	case COM_RESET_CONNECTION:
		return "COM_RESET_CONNECTION"
	default:
		return ""
	}
}

// reference to sql/query_options.h in mysql server 8.0.23
const (
	OPTION_AUTOCOMMIT                     uint32 = 1 << 8
	OPTION_BIG_SELECTS                    uint32 = 1 << 9
	OPTION_LOG_OFF                        uint32 = 1 << 10
	OPTION_QUOTE_SHOW_CREATE              uint32 = 1 << 11
	TMP_TABLE_ALL_COLUMNS                 uint32 = 1 << 12
	OPTION_WARNINGS                       uint32 = 1 << 13
	OPTION_AUTO_IS_NULL                   uint32 = 1 << 14
	OPTION_FOUND_COMMENT                  uint32 = 1 << 15
	OPTION_SAFE_UPDATES                   uint32 = 1 << 16
	OPTION_BUFFER_RESULT                  uint32 = 1 << 17
	OPTION_BIN_LOG                        uint32 = 1 << 18
	OPTION_NOT_AUTOCOMMIT                 uint32 = 1 << 19
	OPTION_BEGIN                          uint32 = 1 << 20
	OPTION_TABLE_LOCK                     uint32 = 1 << 21
	OPTION_QUICK                          uint32 = 1 << 22
	OPTION_NO_CONST_TABLES                uint32 = 1 << 23
	OPTION_ATTACH_ABORT_TRANSACTION_ERROR uint32 = 1 << 24 //defined in mo
)
