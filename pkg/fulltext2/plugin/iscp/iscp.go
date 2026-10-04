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

// Package iscp is fulltext2's ISCP hook layer. A fulltext2 index is
// model-building: its CDC sinker builds tag=1 CdcTail delta frames (the writer /
// consumer loop in pkg/iscp), NOT SQL text. Mirrors bm25's iscp hooks.
//
// Registered from pkg/indexplugin/iscp/import.go via iscp.Register.
package iscp

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	iscppkg "github.com/matrixorigin/matrixone/pkg/iscp"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func init() {
	iscppkg.Register(catalog.MoIndexFullText2Algo.ToString(), Hooks{})
}

// Hooks implements iscp.Hooks for fulltext2.
type Hooks struct{}

var _ iscppkg.Hooks = Hooks{}

func (Hooks) NewSqlWriter(jobID iscppkg.JobID, info *iscppkg.ConsumerInfo,
	tabledef *plan.TableDef, indexdefs []*plan.IndexDef) (iscppkg.IndexSqlWriter, error) {
	return iscppkg.NewFulltext2SqlWriter(catalog.MoIndexFullText2Algo.ToString(), jobID, info, tabledef, indexdefs)
}

func (Hooks) Run(c *iscppkg.IndexConsumer, ctx context.Context, errch chan error, r iscppkg.DataRetriever) {
	iscppkg.RunFulltext2(c, ctx, errch, r)
}
