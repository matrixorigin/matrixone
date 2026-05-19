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

// Package iscp provides HNSW's ISCP hook layer: writer construction and
// consumer-loop dispatch.
//
// The writer and runner bodies live in pkg/iscp itself (HnswSqlWriter,
// runHnsw / RunHnsw[T]) — code motion would force exporting a long list
// of BaseIndexSqlWriter fields and bring no functional benefit, so this
// package is a thin adapter that satisfies iscp.Hooks by delegating to
// the existing pkg/iscp surface.
//
// Registered from pkg/indexplugin/all/all.go via iscp.Register.
package iscp

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	iscppkg "github.com/matrixorigin/matrixone/pkg/iscp"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func init() {
	iscppkg.Register(catalog.MoIndexHnswAlgo.ToString(), Hooks{})
}

// Hooks implements iscp.Hooks for HNSW.
type Hooks struct{}

var _ iscppkg.Hooks = Hooks{}

// NewSqlWriter delegates to iscp.NewHnswSqlWriter — the existing factory
// already does the float32/float64 dispatch based on the source table's
// vector column type.
func (Hooks) NewSqlWriter(jobID iscppkg.JobID, info *iscppkg.ConsumerInfo,
	tabledef *plan.TableDef, indexdefs []*plan.IndexDef) (iscppkg.IndexSqlWriter, error) {
	return iscppkg.NewHnswSqlWriter("hnsw", jobID, info, tabledef, indexdefs)
}

// Run dispatches to the right RunHnsw[T] specialization based on the
// writer's element type. Equivalent to the type-switch that previously
// lived inside IndexConsumer.run.
func (Hooks) Run(c *iscppkg.IndexConsumer, ctx context.Context, errch chan error, r iscppkg.DataRetriever) {
	switch c.SqlWriter().(type) {
	case *iscppkg.HnswSqlWriter[float32]:
		iscppkg.RunHnsw[float32](c, ctx, errch, r)
	case *iscppkg.HnswSqlWriter[float64]:
		iscppkg.RunHnsw[float64](c, ctx, errch, r)
	default:
		errch <- moerr.NewInternalError(ctx, "hnsw iscp Run: unexpected writer type")
	}
}
