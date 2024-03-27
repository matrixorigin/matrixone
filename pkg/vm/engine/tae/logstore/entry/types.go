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
	"os"
	"time"
)

type Type = uint16

const (
	GTInvalid uint32 = iota
	GTNoop
	GTCKp
	GTInternal
	GTCustomizedStart
)

type Desc interface {
	GetType() Type
	SetType(Type)
	GetPayloadSize() int
	SetPayloadSize(int)
	GetInfoSize() int
	SetInfoSize(int)
	GetMetaSize() int
	TotalSize() int
	GetMetaBuf() []byte
}

type Entry interface {
	Desc
	GetPayload() []byte
	SetInfo(any)
	GetInfo() any
	GetInfoBuf() []byte
	SetInfoBuf(buf []byte)

	RegisterPreCallback(func() error)
	ExecutePreCallbacks() error

	SetPayload([]byte) error
	UnmarshalFromNode([]byte, bool) error

	Unmarshal(buf []byte) error
	Marshal() (buf []byte, err error)
	ReadFrom(io.Reader) (int64, error)
	UnmarshalBinary(buf []byte) (n int64, err error)
	ReadAt(r *os.File, offset int) (int, error)
	WriteTo(io.Writer) (int64, error)
	PrepareWrite()

	GetLsn() (gid uint32, lsn uint64)
	WaitDone() error
	DoneWithErr(error)
	GetError() error

	Free()

	Duration() time.Duration
	StartTime()
	PrintTime()
	IsPrintTime() bool
}
