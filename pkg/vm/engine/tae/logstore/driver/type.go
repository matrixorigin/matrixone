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

package driver

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
)

type Driver interface {
	Append(*entry.Entry) error
	Truncate(lsn uint64) error
	GetTruncated() (lsn uint64, err error)
	Close() error
	Replay(ctx context.Context, h ApplyHandle) error
	GetDSN() uint64
}

type ReplayEntryState int8

const (
	RE_Truncate ReplayEntryState = iota
	RE_Internal
	RE_Nomal
	RE_Invalid
)

type ApplyHandle = func(*entry.Entry) (replayEntryState ReplayEntryState)
