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

package iscp

// In production, the iscp Hooks registry is populated from
// pkg/sql/compile/iscp_register.go, which blank-imports each
// algorithm's plugin/iscp/ sub-package. pkg/iscp's own tests can't
// import those sub-packages (they themselves import pkg/iscp, which
// would cycle), so this file registers inline stubs that delegate
// back into pkg/iscp's still-exported factories and runners.
//
// The behaviour is byte-identical to the production hooks — same
// factories, same runners, just registered from here instead of from
// pkg/sql/compile.

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

type testHnswHooks struct{}

func (testHnswHooks) NewSqlWriter(jobID JobID, info *ConsumerInfo, tabledef *plan.TableDef, indexdefs []*plan.IndexDef) (IndexSqlWriter, error) {
	return NewHnswSqlWriter("hnsw", jobID, info, tabledef, indexdefs)
}

func (testHnswHooks) Run(c *IndexConsumer, ctx context.Context, errch chan error, r DataRetriever) {
	switch c.SqlWriter().(type) {
	case *HnswSqlWriter[float32]:
		runHnsw[float32](c, ctx, errch, r)
	case *HnswSqlWriter[float64]:
		runHnsw[float64](c, ctx, errch, r)
	default:
		errch <- moerr.NewInternalError(ctx, "test hnsw hook: unexpected writer type")
	}
}

type testIvfflatHooks struct{}

func (testIvfflatHooks) NewSqlWriter(jobID JobID, info *ConsumerInfo, tabledef *plan.TableDef, indexdefs []*plan.IndexDef) (IndexSqlWriter, error) {
	return NewIvfflatSqlWriter("ivfflat", jobID, info, tabledef, indexdefs)
}

func (testIvfflatHooks) Run(c *IndexConsumer, ctx context.Context, errch chan error, r DataRetriever) {
	runIndex(c, ctx, errch, r)
}

type testFulltextHooks struct{}

func (testFulltextHooks) NewSqlWriter(jobID JobID, info *ConsumerInfo, tabledef *plan.TableDef, indexdefs []*plan.IndexDef) (IndexSqlWriter, error) {
	return NewFulltextSqlWriter("fulltext", jobID, info, tabledef, indexdefs)
}

func (testFulltextHooks) Run(c *IndexConsumer, ctx context.Context, errch chan error, r DataRetriever) {
	runIndex(c, ctx, errch, r)
}

func init() {
	Register(catalog.MoIndexHnswAlgo.ToString(), testHnswHooks{})
	Register(catalog.MoIndexIvfFlatAlgo.ToString(), testIvfflatHooks{})
	Register(catalog.MOIndexFullTextAlgo.ToString(), testFulltextHooks{})
}
