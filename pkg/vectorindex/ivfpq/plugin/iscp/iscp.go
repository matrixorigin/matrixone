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

// Package iscp provides IVF-PQ's ISCP hook layer. Mirrors CAGRA — see
// pkg/vectorindex/cagra/plugin/iscp/iscp.go for the architectural notes.
//
// Registered from pkg/indexplugin/iscp/import_gpu.go.
package iscp

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	iscppkg "github.com/matrixorigin/matrixone/pkg/iscp"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/ivfpq"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

func init() {
	iscppkg.Register(catalog.MoIndexIvfpqAlgo.ToString(), Hooks{})
}

// Hooks implements iscp.Hooks for IVF-PQ.
type Hooks struct{}

var _ iscppkg.Hooks = Hooks{}

func (Hooks) NewSqlWriter(jobID iscppkg.JobID, info *iscppkg.ConsumerInfo,
	tabledef *plan.TableDef, indexdefs []*plan.IndexDef) (iscppkg.IndexSqlWriter, error) {
	return iscppkg.NewCuvsCdcWriter(catalog.MoIndexIvfpqAlgo.ToString(), info.DBName, info.TableName, info.IndexName,
		tabledef, indexdefs)
}

func (Hooks) Run(c *iscppkg.IndexConsumer, ctx context.Context, errch chan error, r iscppkg.DataRetriever) {
	iscppkg.RunCuvs(c, ctx, errch, r, func(sqlproc *sqlexec.SqlProcess) (iscppkg.CuvsSync, error) {
		w := c.SqlWriter().(*iscppkg.CuvsCdcWriter)
		return ivfpq.NewIvfpqSync(sqlproc, w.DbName(), w.TblName(), w.IndexName(),
			w.IndexDef(), w.Dimension(), w.BaseVectorType(), w.ColMetaJSON())
	})
}
