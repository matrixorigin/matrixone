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

package entry

import (
	"io"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

type Type = uint16

const (
	ETInvalid Type = iota
	ETNoop
	ETFlush
	ETCheckpoint
	ETUncommitted
	ETTxn
	ETCustomizedStart
)

const (
	GTInvalid uint32 = iota
	GTNoop
	GTCKp
	GTUncommit
	GTCustomizedStart
)

type Desc interface {
	GetType() Type
	SetType(Type)
	GetPayloadSize() int
	SetPayloadSize(int)
	GetInfoSize() int
	SetInfoSize(int)
	TotalSize() int
	GetMetaBuf() []byte
	IsFlush() bool
	IsCheckpoint() bool
}

type Entry interface {
	Desc
	GetPayload() []byte
	SetInfo(interface{})
	GetInfo() interface{}
	GetInfoBuf() []byte
	SetInfoBuf(buf []byte)

	Unmarshal([]byte) error
	UnmarshalFromNode(*common.MemNode, bool) error
	ReadFrom(io.Reader) (int64, error)
	WriteTo(io.Writer) (int64, error)

	WaitDone() error
	DoneWithErr(error)
	GetError() error

	Free()

	Duration() time.Duration
	StartTime()
	PrintTime()
	IsPrintTime() bool
}
