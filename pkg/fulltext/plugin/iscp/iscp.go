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

// Package iscp provides fulltext's ISCP hook layer.
//
// The writer body lives in pkg/iscp (FulltextSqlWriter); this package
// is a thin adapter that satisfies iscp.Hooks by delegating to that
// surface. The consumer loop reuses the generic SQL-execution runner
// iscp.RunIndex.
//
// Registered from pkg/indexplugin/all/all.go via iscp.Register.
package iscp

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	iscppkg "github.com/matrixorigin/matrixone/pkg/iscp"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func init() {
	iscppkg.Register(catalog.MOIndexFullTextAlgo.ToString(), Hooks{})
}

// Hooks implements iscp.Hooks for fulltext.
type Hooks struct{}

var _ iscppkg.Hooks = Hooks{}

func (Hooks) NewSqlWriter(jobID iscppkg.JobID, info *iscppkg.ConsumerInfo,
	tabledef *plan.TableDef, indexdefs []*plan.IndexDef) (iscppkg.IndexSqlWriter, error) {
	// A retrieval (WAND) index is model-building — build tag=1 CdcTail delta
	// frames — whereas postings/ngram fulltext emits SQL. Branch on the parser.
	if len(indexdefs) > 0 && catalog.GetIndexParser(indexdefs[0].IndexAlgoParams) == "retrieval" {
		return iscppkg.NewWandSqlWriter("fulltext", jobID, info, tabledef, indexdefs)
	}
	return iscppkg.NewFulltextSqlWriter("fulltext", jobID, info, tabledef, indexdefs)
}

func (Hooks) Run(c *iscppkg.IndexConsumer, ctx context.Context, errch chan error, r iscppkg.DataRetriever) {
	// Retrieval uses the WAND consumer loop; postings/ngram use the generic SQL runner.
	if _, ok := c.SqlWriter().(*iscppkg.WandSqlWriter); ok {
		iscppkg.RunWand(c, ctx, errch, r)
		return
	}
	iscppkg.RunIndex(c, ctx, errch, r)
}
