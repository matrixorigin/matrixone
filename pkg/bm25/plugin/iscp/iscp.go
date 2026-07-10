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

// Package iscp is bm25's ISCP hook layer. A bm25 index is model-building —
// its CDC sinker builds tag=1 CdcTail delta frames (the WAND writer /
// consumer loop in pkg/iscp), NOT SQL text. Unlike the classic fulltext
// plugin there is no parser branch: a bm25 index is always WAND.
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
	iscppkg.Register(catalog.MoIndexBm25Algo.ToString(), Hooks{})
}

// Hooks implements iscp.Hooks for bm25.
type Hooks struct{}

var _ iscppkg.Hooks = Hooks{}

func (Hooks) NewSqlWriter(jobID iscppkg.JobID, info *iscppkg.ConsumerInfo,
	tabledef *plan.TableDef, indexdefs []*plan.IndexDef) (iscppkg.IndexSqlWriter, error) {
	return iscppkg.NewWandSqlWriter(catalog.MoIndexBm25Algo.ToString(), jobID, info, tabledef, indexdefs)
}

func (Hooks) Run(c *iscppkg.IndexConsumer, ctx context.Context, errch chan error, r iscppkg.DataRetriever) {
	iscppkg.RunWand(c, ctx, errch, r)
}
