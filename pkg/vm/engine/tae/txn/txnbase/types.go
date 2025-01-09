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

package txnbase

import (
	"bytes"
)

type TxnFlag uint64

const (
	TxnFlag_Normal TxnFlag = 1 << iota
	TxnFlag_Replay
	TxnFlag_Heartbeat
)

type TxnSkipFlag = TxnFlag

const (
	TxnSkipFlag_Normal    = TxnFlag_Normal
	TxnSkipFlag_Replay    = TxnFlag_Replay
	TxnSkipFlag_Heartbeat = TxnFlag_Heartbeat
)

const (
	TxnSkipFlag_None TxnFlag = 0
	TxnSkipFlag_All  TxnFlag = TxnFlag_Normal | TxnFlag_Replay | TxnFlag_Heartbeat
)

func (m TxnFlag) String() string {
	var (
		first = true
		w     bytes.Buffer
	)

	w.WriteString("Flag[")

	if m&TxnFlag_Normal == TxnFlag_Normal {
		w.WriteByte('N')
	}
	if m&TxnFlag_Replay == TxnFlag_Replay {
		if first {
			first = false
			w.WriteByte('|')
		}
		w.WriteByte('R')
	}
	if m&TxnFlag_Heartbeat == TxnFlag_Heartbeat {
		if first {
			w.WriteByte('|')
		}
		w.WriteByte('H')
	}
	w.WriteByte(']')
	return w.String()
}

// flag should only contain one bit
func (m TxnSkipFlag) Skip(flag TxnFlag) bool {
	return m&flag == flag
}
