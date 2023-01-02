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

package deletion

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type Argument struct {
	Ts           uint64
	DeleteCtxs   []*DeleteCtx
	AffectedRows uint64
}

type DeleteCtx struct {
	TableName          string
	DbName             string
	TableSource        engine.Relation
	UseDeleteKey       string // The column used when deletion(dml), Currently, it is based on '__row_id' column
	CanTruncate        bool
	ColIndex           int32
	IsIndexTableDelete bool
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool) {
}
