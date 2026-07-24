//go:build gpu

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

// Package iscp provides CAGRA's ISCP hook layer.
//
// The CDC writer body is shared with IVF-PQ via iscp.CuvsCdcWriter
// (binary EncodeEventRecord stream, upsert-as-insert, INCLUDE column
// support). This package supplies only the CAGRA-specific bits:
//
//   - The algo string the iscp registry uses ("cagra")
//   - The sync constructor (cagra.NewCagraSync) the runner builds
//     inside its txn
//
// CAGRA is GPU-only — this package is //go:build gpu so cuvs cgo
// imports stay out of CPU build graphs.
//
// Registered from pkg/indexplugin/iscp/import_gpu.go.
package iscp

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	iscppkg "github.com/matrixorigin/matrixone/pkg/iscp"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/cagra"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

func init() {
	iscppkg.Register(catalog.MoIndexCagraAlgo.ToString(), Hooks{})
}

// Hooks implements iscp.Hooks for CAGRA.
type Hooks struct{}

var _ iscppkg.Hooks = Hooks{}

func (Hooks) NewSqlWriter(jobID iscppkg.JobID, info *iscppkg.ConsumerInfo,
	tabledef *plan.TableDef, indexdefs []*plan.IndexDef) (iscppkg.IndexSqlWriter, error) {
	return iscppkg.NewCuvsCdcWriter(catalog.MoIndexCagraAlgo.ToString(), info.DBName, info.TableName, info.IndexName,
		tabledef, indexdefs)
}

// Run delegates to the shared cuvs runner — raw event-record bytes
// flow from the writer's channel into CagraSync.AppendRecords, no
// JSON round-trip.
func (Hooks) Run(c *iscppkg.IndexConsumer, ctx context.Context, errch chan error, r iscppkg.DataRetriever) {
	iscppkg.RunCuvs(c, ctx, errch, r, func(sqlproc *sqlexec.SqlProcess) (iscppkg.CuvsSync, error) {
		w := c.SqlWriter().(*iscppkg.CuvsCdcWriter)
		return cagra.NewCagraSync(sqlproc, w.DbName(), w.TblName(), w.IndexName(),
			w.IndexDef(), w.Dimension(), w.BaseVectorType(), w.ColMetaJSON())
	})
}
