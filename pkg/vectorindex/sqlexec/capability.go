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

package sqlexec

import (
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
)

// ClusterHasIndexProvenance reports whether every live service understands the widened index
// metadata shape. It is the write-side counterpart of the plan-time gate in
// pkg/indexplugin/plan, and answers a DIFFERENT question from HasProvenanceColumns:
//
//	this        -- is any un-upgraded CN still out there?          (cluster, live)
//	HasProvenance -- does THIS table carry the columns right now?  (one table)
//
// Both are needed and neither implies the other. A tail frame row is only safe to WRITE once no
// old reader can meet it: an un-upgraded CN reads the metadata table with SELECT * and would take
// 'cdc_tail:7' for a base sub-index and try to load it as one. The table's own shape cannot answer
// that -- a table is widened either by the v4_0_7 migration or, once activated, at CREATE INDEX,
// and the latter says nothing about the rest of the deployment.
//
// MOProtocolVersion tracks the OLDEST live service and is lowered again for a rollback, so it is
// read live at every decision. Unavailable answers false, which withholds the row.
func ClusterHasIndexProvenance(sqlproc *SqlProcess) bool {
	if sqlproc == nil {
		return false
	}
	// A SqlProcess carries EITHER a Proc or a SqlCtx, and the CDC path -- the only writer of
	// tail frame rows -- carries the SqlCtx one (RunTxnWithSqlContext builds it with Proc nil).
	// Reading only Proc answered false for every CDC flush, which silently withheld every tail
	// frame row and left sizing permanently on the chunk-count bound. GetService already
	// resolves either shape, which is what RunSql and HasProvenanceColumns use.
	if sqlproc.Proc == nil && sqlproc.SqlCtx == nil {
		return false
	}
	service := sqlproc.GetService()
	// ServiceRuntime returns nil for a service it has never seen (an unregistered service id,
	// which internal and test callers do reach), so this must be checked before the read.
	rt := moruntime.ServiceRuntime(service)
	if rt == nil {
		return false
	}
	value, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	if !ok {
		return false
	}
	version, ok := value.(int64)
	return ok && version >= defines.MORPCVersion58
}
