// Copyright 2022 Matrix Origin
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

package cdc

import (
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
)

/*
Cdc process
	logtail replayer
=>  Queue[DecoderInput]
=>  Decoder.Decode
=>  Queue[DecoderOutput]
=>  Sinker.Sink
=>  Sink.Send

*/

type TableCtx struct {
	db, table     string
	dbId, tableId uint64
}

type DecoderInput struct {
	ts    timestamp.Timestamp
	state *logtailreplay.PartitionState
}

type DecoderOutput struct {
	ts timestamp.Timestamp
}

// Decoder convert binary data into sql parts
type Decoder interface {
	Decode(
		cdcCtx *TableCtx,
		input *DecoderInput,
	) *DecoderOutput
}

// Sinker manages and drains the sql parts
type Sinker interface {
	Sink(
		cdcCtx *TableCtx,
		data *DecoderOutput,
	) error
}

// Sink represents the destination mysql or matrixone
type Sink interface {
	Send(
		data *DecoderOutput,
	) error
}

// Queue
// Two features:
//
//	persistence
//	concurrent safe
type Queue[T any] interface {
	//Push saves the value before it returns
	Push(T)
	Pop()
	Front() T
	Back() T
	Size() int
	Empty() bool
}
