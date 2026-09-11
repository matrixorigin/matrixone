// Copyright 2026 Matrix Origin
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
//
// Portions derived from github.com/rashiq/mysql-digest; see ../LICENSE.

package internal

// Matches MySQL's my_lex_states enum in strings/sql_chars.h
type LexState int

const (
	MY_LEX_START LexState = iota
	MY_LEX_CHAR
	MY_LEX_IDENT
	MY_LEX_IDENT_SEP
	MY_LEX_IDENT_START
	MY_LEX_REAL
	MY_LEX_HEX_NUMBER
	MY_LEX_BIN_NUMBER
	MY_LEX_CMP_OP
	MY_LEX_LONG_CMP_OP
	MY_LEX_STRING
	MY_LEX_COMMENT
	MY_LEX_END
	MY_LEX_NUMBER_IDENT
	MY_LEX_INT_OR_REAL
	MY_LEX_REAL_OR_POINT
	MY_LEX_BOOL
	MY_LEX_EOL
	MY_LEX_LONG_COMMENT
	MY_LEX_END_LONG_COMMENT
	MY_LEX_SEMICOLON
	MY_LEX_SET_VAR
	MY_LEX_USER_END
	MY_LEX_HOSTNAME
	MY_LEX_SKIP
	MY_LEX_USER_VARIABLE_DELIMITER
	MY_LEX_SYSTEM_VAR
	MY_LEX_IDENT_OR_KEYWORD
	MY_LEX_IDENT_OR_HEX
	MY_LEX_IDENT_OR_BIN
	MY_LEX_IDENT_OR_NCHAR
	MY_LEX_IDENT_OR_DOLLAR_QUOTED_TEXT
	MY_LEX_STRING_OR_DELIMITER
)

// Matches MySQL's init_state_maps() from strings/sql_chars.cc
var stateMap [256]LexState

func init() {
	// Default: all characters start as MY_LEX_CHAR
	for i := 0; i < 256; i++ {
		stateMap[i] = MY_LEX_CHAR
	}

	// Alphabetic characters -> MY_LEX_IDENT
	for c := 'a'; c <= 'z'; c++ {
		stateMap[c] = MY_LEX_IDENT
	}
	for c := 'A'; c <= 'Z'; c++ {
		stateMap[c] = MY_LEX_IDENT
	}

	// High-bit bytes (0x80-0xFF) are treated as alpha for UTF-8 support
	for i := 0x80; i <= 0xFF; i++ {
		stateMap[i] = MY_LEX_IDENT
	}

	// Digits -> MY_LEX_NUMBER_IDENT
	for c := '0'; c <= '9'; c++ {
		stateMap[c] = MY_LEX_NUMBER_IDENT
	}

	// Whitespace -> MY_LEX_SKIP
	stateMap[' '] = MY_LEX_SKIP
	stateMap['\t'] = MY_LEX_SKIP
	stateMap['\n'] = MY_LEX_SKIP
	stateMap['\r'] = MY_LEX_SKIP
	stateMap['\v'] = MY_LEX_SKIP
	stateMap['\f'] = MY_LEX_SKIP

	stateMap['_'] = MY_LEX_IDENT

	stateMap['\''] = MY_LEX_STRING

	stateMap['.'] = MY_LEX_REAL_OR_POINT

	stateMap['>'] = MY_LEX_CMP_OP
	stateMap['='] = MY_LEX_CMP_OP
	stateMap['!'] = MY_LEX_CMP_OP
	stateMap['<'] = MY_LEX_LONG_CMP_OP

	stateMap['&'] = MY_LEX_BOOL
	stateMap['|'] = MY_LEX_BOOL

	// Comment starters
	stateMap['#'] = MY_LEX_COMMENT
	stateMap['/'] = MY_LEX_LONG_COMMENT
	stateMap['*'] = MY_LEX_END_LONG_COMMENT

	stateMap[';'] = MY_LEX_SEMICOLON
	stateMap[':'] = MY_LEX_SET_VAR
	stateMap['@'] = MY_LEX_USER_END
	stateMap['`'] = MY_LEX_USER_VARIABLE_DELIMITER
	stateMap['"'] = MY_LEX_STRING_OR_DELIMITER

	// End of input
	stateMap[0] = MY_LEX_EOL

	// Special handling for hex/bin/nchar string prefixes
	// These override the default MY_LEX_IDENT for these letters
	stateMap['x'] = MY_LEX_IDENT_OR_HEX
	stateMap['X'] = MY_LEX_IDENT_OR_HEX
	stateMap['b'] = MY_LEX_IDENT_OR_BIN
	stateMap['B'] = MY_LEX_IDENT_OR_BIN
	stateMap['n'] = MY_LEX_IDENT_OR_NCHAR
	stateMap['N'] = MY_LEX_IDENT_OR_NCHAR

	stateMap['$'] = MY_LEX_IDENT_OR_DOLLAR_QUOTED_TEXT
}

func getStateMap(c byte) LexState {
	return stateMap[c]
}
